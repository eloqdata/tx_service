/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#include "cc/non_blocking_lock.h"

#include <butil/logging.h>
#include <local_cc_shards.h>

#include <cassert>

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "cc/cc_shard.h"
#include "error_messages.h"
#include "remote_cc_request.h"

namespace txservice
{
/**
 * @brief Upgrade write lock or write intent.
 * 1. release low level lock for write lock or write intent for the same
 * tx_number.
 * 2. acquire the desired write lock/intent.
 * 3. read lock and read intent have no upgrade logic since they don't have low
 * level locks.
 *
 */
void NonBlockingLock::UpgradeLock(TxNumber tx_number, LockType lock_type)
{
    assert(write_lk_type_ == WriteLockType::NoWritelock ||
           write_txn_ == tx_number);
    if (lock_type == LockType::WriteLock)
    {
        write_txn_ = tx_number;
        write_lk_type_ = WriteLockType::WriteLock;
    }
    else if (lock_type == LockType::WriteIntent)
    {
        write_txn_ = tx_number;
        write_lk_type_ = WriteLockType::WriteIntent;
    }

    // The upgrade removes the read lock or intent, if there is any.
    if (read_intentions_.size() > 0)
    {
        read_intentions_.erase(tx_number);
    }
    if (read_locks_.size() > 0)
    {
        read_locks_.erase(tx_number);
    }
}

/**
 * @brief Re-execute the queued request when some locks are released.
 */
void NonBlockingLock::ExecuteQueuedRequest(const LockQueueEntry &queue_head,
                                           CcShard *ccs)
{
    // Before call this method, "queue_head.req_" already acquired the lock,
    // should pop it from blocking queue before execute it. If not, the request
    // may be executed repeatedly.
    ccs->Enqueue(ccs->LocalCoreId(), queue_head.req_);
    blocking_queue_.Dequeue();
}

/**
 * @brief Try to pop the requests from queue and re-execute the requests if
 * there is no conflict.
 */
void NonBlockingLock::TryPopBlockingQueue(CcShard *ccs)
{
    while (blocking_queue_.Size() > 0)
    {
        const LockQueueEntry &queue_head = blocking_queue_.Peek();
        TxNumber queued_txn = queue_head.req_->Txn();

        if (queue_head.lk_type_ == LockType::WriteLock)
        {
            if (NoWriteConflict(queued_txn) && NoReadLockConflict(queued_txn))
            {
                UpgradeLock(queued_txn, LockType::WriteLock);

                // re-execute the head request in the queue.
                ExecuteQueuedRequest(queue_head, ccs);
            }
            else
            {
                // stop poping request from queue when hitting conflict.
                return;
            }
        }
        else if (queue_head.lk_type_ == LockType::WriteIntent)
        {
            if (NoWriteConflict(queued_txn))
            {
                UpgradeLock(queued_txn, LockType::WriteIntent);

                // re-execute the head request in the queue.
                ExecuteQueuedRequest(queue_head, ccs);
            }
            else
            {
                // stop poping request from queue when hitting conflict.
                return;
            }
        }
        else if (queue_head.lk_type_ == LockType::ReadLock)
        {
            if (NoWriteLockConflict(queued_txn))
            {
                read_locks_.emplace(queue_head.req_->Txn());
                read_intentions_.erase(queue_head.req_->Txn());
                // re-execute the head request in the queue.
                ExecuteQueuedRequest(queue_head, ccs);
            }
            else
            {
                // stop poping request from queue when hitting conflict.
                return;
            }
        }
        else
        {
            assert(queue_head.lk_type_ == LockType::NoLock);
            ExecuteQueuedRequest(queue_head, ccs);
        }
    }
}

LockOpStatus NonBlockingLock::AcquireLock(CcRequestBase *cc_req,
                                          CcProtocol cc_protocol,
                                          LockType lock_type)
{
    TxNumber txn = cc_req->Txn();
    LockOpStatus lock_status = LockOpStatus::Successful;

    switch (lock_type)
    {
    case LockType::NoLock:
    {
        lock_status = LockOpStatus::Successful;
        break;
    }
    case LockType::ReadIntent:
    {
        AcquireReadIntent(txn);
        lock_status = LockOpStatus::Successful;
        break;
    }
    case LockType::ReadLock:
    {
        bool success = AcquireReadLock(cc_req);
        lock_status =
            success ? LockOpStatus::Successful : LockOpStatus::Blocked;
        break;
    }
    case LockType::WriteIntent:
    {
        if (cc_protocol == CcProtocol::OCC)
        {
            bool success = AcquireWriteIntent(cc_req, cc_protocol);
            lock_status =
                success ? LockOpStatus::Successful : LockOpStatus::Failed;
        }
        else
        {
            bool success = AcquireWriteIntent(cc_req, cc_protocol);
            lock_status =
                success ? LockOpStatus::Successful : LockOpStatus::Blocked;
        }
        break;
    }
    case LockType::WriteLock:
    {
        bool success = AcquireWriteLock(cc_req, cc_protocol);
        if (cc_protocol == CcProtocol::OCC)
        {
            lock_status =
                success ? LockOpStatus::Successful : LockOpStatus::Failed;
        }
        else
        {
            lock_status =
                success ? LockOpStatus::Successful : LockOpStatus::Blocked;
        }
        break;
    }
    }

    return lock_status;
}

/**
 * @brief Acquire the write lock on this object (i.e. ccentry). The algorithm
 * is as follows:
 * 1. fast path if the lock is already held.
 * 1. list non conflict case: no write lock conflict, no write intent conflict
 * and no read lock conflict.
 * 2. upgrade low-level locks/intents if lock succeeds.
 * 3. put the request into blocking queue under LOCKING/OccRead protocol.
 *
 * @param cc_req: lock request.
 * @param protocol: OCC, OccRead, LOCKING.
 * @return true: lock succeeds.
 * @return false: lock failed.
 */
bool NonBlockingLock::AcquireWriteLock(CcRequestBase *cc_req,
                                       CcProtocol protocol)
{
    TxNumber tx_number = cc_req->Txn();
    // fast path for lock is already held.
    if (write_lk_type_ == WriteLockType::WriteLock && write_txn_ == tx_number)
    {
        return true;
    }

    // lock succeeds if there is no conflict.
    if (NoWriteConflict(tx_number) && NoReadLockConflict(tx_number))
    {
        UpgradeLock(tx_number, LockType::WriteLock);

        return true;
    }
    else
    {
        // lock fails.
        if (protocol == CcProtocol::Locking || protocol == CcProtocol::OccRead)
        {
            // block the request by putting it into the blocking queue.
            blocking_queue_.Enqueue(
                LockQueueEntry(cc_req, LockType::WriteLock));
        }
        // OCC doesn't enqueue request.
        return false;
    }
}

bool NonBlockingLock::AcquireReadLockFast(TxNumber tx_number)
{
    // read lock doesn't conflict with write intent in blocking queue.
    bool no_blocking_queue_conflict =
        blocking_queue_.Size() == 0 ||
        blocking_queue_.Peek().lk_type_ == LockType::WriteIntent;

    if (NoWriteLockConflict(tx_number) && no_blocking_queue_conflict)
    {
        assert(read_cnt_ >= 0);
        // Acquire read lock succeeds.
        // For fast path catalog read, only increment the read_cnt_.
        read_cnt_++;
        return true;
    }

    return false;
}

bool NonBlockingLock::ReleaseReadLockFast(CcShard *ccs)
{
    // For fast path catalog realese read lock, only decrement the read_cnt_.
    assert(read_cnt_ > 0);
    read_cnt_--;
    // If releasing the current read lock may unblock anything, it may be the
    // write lock who is the head of the blocking queue, or a no lock pk read
    // directed from a sk scan.
    TryPopBlockingQueue(ccs);
    return true;
}

/**
 * @brief Acquire the read lock on this object (i.e. ccentry). The algorithm
 * is as follows:
 * 1. fast path is that the lock is already held.
 * 2. acquire succeeds if write_lock is empty and (a. the blocking queue is
 * empty or b. the head of queue is write intent since write intent is not
 * conflict with read lock).
 * 3. put the request into blocking queue if lock fails.
 *
 * @param cc_req: lock request.
 * @return true: lock succeeds.
 * @return false: lock failed, push request into blocking.
 */
bool NonBlockingLock::AcquireReadLock(CcRequestBase *cc_req)
{
    TxNumber tx_number = cc_req->Txn();
    // fast path for lock is already held.
    if (read_locks_.find(tx_number) != read_locks_.end() ||
        (write_lk_type_ != WriteLockType::NoWritelock &&
         write_txn_ == tx_number))
    {
        return true;
    }

    // read lock doesn't conflict with write intent in blocking queue.
    bool no_blocking_queue_conflict =
        blocking_queue_.Size() == 0 ||
        blocking_queue_.Peek().lk_type_ == LockType::WriteIntent;

    if (NoWriteLockConflict(tx_number) && no_blocking_queue_conflict)
    {
        // acquire read lock succeeds
        read_locks_.emplace(tx_number);
        read_intentions_.erase(tx_number);
        return true;
    }
    else
    {
        // protocol must be LOCKING, since tx under OCC never acquires read
        // locks.
        blocking_queue_.Enqueue(LockQueueEntry(cc_req, LockType::ReadLock));
        return false;
    }
}

/**
 * @brief Release the read lock on this object (i.e. ccentry).
 *
 * @param tx_number
 * @param ccs
 */
bool NonBlockingLock::ReleaseReadLock(TxNumber tx_number, CcShard *ccs)
{
    if (read_locks_.empty())
    {
        return false;
    }

    size_t removed_cnt = read_locks_.erase(tx_number);
    if (removed_cnt == 0)
    {
        return false;
    }

    // If releasing the current read lock may unblock anything, it may be the
    // write lock who is the head of the blocking queue, or a no lock pk read
    // directed from a sk scan.
    TryPopBlockingQueue(ccs);
    return true;
}

/**
 * @brief Release the write lock on this object (i.e. ccentry).
 *
 * @param tx_number
 * @param ccs
 */
bool NonBlockingLock::ReleaseWriteLock(TxNumber tx_number,
                                       CcShard *ccs,
                                       TxObject *object)
{
    if (write_lk_type_ != WriteLockType::WriteLock || write_txn_ != tx_number)
    {
        return false;
    }

    write_lk_type_ = WriteLockType::NoWritelock;
    write_txn_ = 0;

    if (ccs == nullptr)
    {
        return true;  // warning: just for unit-tests.
    }

    if (object != nullptr && PopBlockCmdRequest(ccs, object))
    {
        return true;
    }

    TryPopBlockingQueue(ccs);
    return true;
}

/**
 * @brief Acquire the write intent on this object (i.e. ccentry). The
 * algorithm is as follows:
 * 1. fast path if lock is already held.
 * 2. acquire succeeds if no conflict write intent or write lock on this
 * object and lock owner is not the current tx and blocking queue is empty.
 * 3. upgrade low-level locks/intents if succeeds.
 * 4. return true if acquire succeeds. return false if acquire fails. For
 * LOCKING protocol, put the request into blocking queue.
 *
 * @param cc_req: lock request.
 * @param protocol: OCC, OccRead or LOCKING.
 * @return true: lock succeeds.
 * @return false: lock failed, push request into blocking for OccRead or LOCKING
 * protocol. return directly for OCC protocol.
 */
bool NonBlockingLock::AcquireWriteIntent(CcRequestBase *cc_req,
                                         CcProtocol protocol)
{
    TxNumber tx_number = cc_req->Txn();
    // fast path for lock is already held.
    if (write_lk_type_ != WriteLockType::NoWritelock && write_txn_ == tx_number)
    {
        return true;
    }

    // lock succeeds if:
    // 1. no conflict write intent or write locks
    // 2. blocking queue is empty which is used to avoid the starvation of
    // queued write lock.
    else if (NoWriteConflict(tx_number) && blocking_queue_.Size() == 0)
    {
        UpgradeLock(tx_number, LockType::WriteIntent);

        return true;
    }
    // lock fails case.
    else
    {
        if (protocol != CcProtocol::OCC)
        {
            // block the request by putting it into the blocking queue.
            blocking_queue_.Enqueue(
                LockQueueEntry(cc_req, LockType::WriteIntent));
        }
        // OccRead doesn't enqueue request.
        return false;
    }
}

void NonBlockingLock::DowngradeWriteLock(TxNumber tx_number, CcShard *ccs)
{
    if (write_lk_type_ != WriteLockType::WriteLock || write_txn_ != tx_number)
    {
        return;
    }

    write_lk_type_ = WriteLockType::WriteIntent;
    assert(write_txn_ == tx_number);

    TryPopBlockingQueue(ccs);
}

/**
 * @brief Release the write intent on this object (i.e. ccentry).
 *
 * @param tx_number
 * @param ccs
 */
bool NonBlockingLock::ReleaseWriteIntent(TxNumber tx_number, CcShard *ccs)
{
    if (write_lk_type_ != WriteLockType::WriteIntent || write_txn_ != tx_number)
    {
        return false;
    }

    write_lk_type_ = WriteLockType::NoWritelock;
    write_txn_ = 0;

    TryPopBlockingQueue(ccs);
    return true;
}

bool NonBlockingLock::AcquireReadIntent(TxNumber tx_number)
{
    if (read_locks_.find(tx_number) != read_locks_.end() ||
        read_intentions_.find(tx_number) != read_intentions_.end() ||
        (write_lk_type_ != WriteLockType::NoWritelock &&
         write_txn_ == tx_number))
    {
        return false;
    }

    read_intentions_.emplace(tx_number);
    return true;
}

bool NonBlockingLock::ReleaseReadIntent(TxNumber tx_number)
{
    if (read_intentions_.empty())
    {
        return false;
    }

    size_t cnt = read_intentions_.erase(tx_number);
    return cnt > 0;
}

void NonBlockingLock::InsertBlockingQueue(CcRequestBase *cc_req,
                                          LockType lock_type)
{
    blocking_queue_.EnqueueAsFirst(LockQueueEntry(cc_req, lock_type));
}

bool NonBlockingLock::IsEmpty() const
{
    return read_intentions_.empty() && read_locks_.empty() && read_cnt_ == 0 &&
           write_lk_type_ == WriteLockType::NoWritelock &&
           blocking_queue_.Size() == 0 && queue_block_cmds_.Size() == 0;
}

TxNumber NonBlockingLock::WriteLockTx() const
{
    return write_txn_;
}

bool NonBlockingLock::HasWriteLock() const
{
    return write_lk_type_ == WriteLockType::WriteLock;
}

LockType NonBlockingLock::ClearTx(TxNumber tx_number,
                                  CcShard *ccs,
                                  TxObject *object)
{
    if (ReleaseReadLock(tx_number, ccs))
    {
        return LockType::ReadLock;
    }
    else if (ReleaseReadIntent(tx_number))
    {
        return LockType::ReadIntent;
    }
    else if (write_lk_type_ != WriteLockType::NoWritelock &&
             write_txn_ == tx_number)
    {
        auto return_type = write_lk_type_ == WriteLockType::WriteLock
                               ? LockType::WriteLock
                               : LockType::WriteIntent;

        write_lk_type_ = WriteLockType::NoWritelock;
        write_txn_ = 0;
        if (object != nullptr && return_type == LockType::WriteLock &&
            PopBlockCmdRequest(ccs, object))
        {
            return return_type;
        }

        TryPopBlockingQueue(ccs);

        return return_type;
    }
    else
    {
        return LockType::NoLock;
    }
}

const absl::flat_hash_set<TxNumber> &NonBlockingLock::ReadLocks() const
{
    return read_locks_;
}

const absl::flat_hash_set<TxNumber> &NonBlockingLock::ReadIntents() const
{
    return read_intentions_;
}

std::vector<TxNumber> NonBlockingLock::GetBlockTxIds(TxNumber exclude_id)
{
    std::vector<uint64_t> vct;
    for (size_t i = 0; i < blocking_queue_.Size(); i++)
    {
        LockQueueEntry &lqe = blocking_queue_.Get(i);
        if (lqe.req_->Txn() == exclude_id)
        {
            continue;
        }

        vct.push_back(lqe.req_->Txn());
    }

    return vct;
}

void NonBlockingLock::AbortAllQueuedRequests(CcErrorCode err)
{
    for (int64_t i = 0; i < (int64_t) blocking_queue_.Size(); i++)
    {
        blocking_queue_.Get(i).req_->AbortCcRequest(err);
    }
    blocking_queue_.Reset();
}

void NonBlockingLock::AbortQueueRequest(TxNumber txid, CcErrorCode err)
{
    for (int64_t i = 0; i < (int64_t) blocking_queue_.Size(); i++)
    {
        const LockQueueEntry &ety = blocking_queue_.Get(i);
        if (ety.req_->Txn() == txid)
        {
            ety.req_->AbortCcRequest(err);
            blocking_queue_.Erase(i);
            i--;
        }
    }
}

bool NonBlockingLock::FindQueueRequest(TxNumber txid)
{
    for (int64_t i = 0; i < (int64_t) blocking_queue_.Size(); i++)
    {
        const LockQueueEntry &ety = blocking_queue_.Get(i);
        if (ety.req_->Txn() == txid)
        {
            return true;
        }
    }

    return false;
}

LockType NonBlockingLock::SearchLock(TxNumber txn)
{
    if (write_lk_type_ != WriteLockType::NoWritelock && write_txn_ == txn)
    {
        return write_lk_type_ == WriteLockType::WriteLock
                   ? LockType::WriteLock
                   : LockType::WriteIntent;
    }
    else if (read_locks_.find(txn) != read_locks_.end())
    {
        return LockType::ReadLock;
    }
    else if (read_intentions_.find(txn) != read_intentions_.end())
    {
        return LockType::ReadIntent;
    }
    else
    {
        return LockType::NoLock;
    }
}

bool NonBlockingLock::PopBlockCmdRequest(CcShard *ccs, TxObject *object)
{
    for (size_t i = 0; i < queue_block_cmds_.Size(); i++)
    {
        ApplyCc *req = dynamic_cast<ApplyCc *>(queue_block_cmds_.Get(i));
        assert(req != nullptr);
        TxCommand *cmd = nullptr;
        if (req->IsLocal())
        {
            cmd = req->CommandPtr();
        }
        else
        {
            cmd = req->remote_input_.cmd_;
        }

        assert(cmd != nullptr);

        if (cmd->AblePopBlockRequest(object))
        {
            // Upgrade lock for blocked command request so that no one can be
            // executed before it.
            UpgradeLock(req->Txn(), LockType::WriteLock);
            ccs->Enqueue(ccs->LocalCoreId(), req);
            queue_block_cmds_.Erase(i);
            return true;
        }
    }

    return false;
}

void NonBlockingLock::AbortBlockCmdRequest(TxNumber txid, CcErrorCode err)
{
    for (size_t i = 0; i < queue_block_cmds_.Size(); i++)
    {
        if (queue_block_cmds_.Get(i)->Txn() == txid)
        {
            queue_block_cmds_.Get(i)->AbortCcRequest(err);
            queue_block_cmds_.Erase(i);
            return;
        }
    }
}

void KeyGapLockAndExtraData::SetUsedStatus(bool is_used)
{
    in_use_ = is_used;
    if (!in_use_)
    {
        last_used_ts_ = Sharder::Instance().GetLocalCcShards()->TsBase();
    }
}

bool KeyGapLockAndExtraData::SafeToRecycle() const
{
    return !in_use_ &&
           Sharder::Instance().GetLocalCcShards()->TsBase() - last_used_ts_ >=
               recycle_interval_us_;
}

void KeyGapLockAndExtraData::SetForwardEntry(StandbyForwardEntry *entry)
{
    forward_entry_ = entry;
}

StandbyForwardEntry *KeyGapLockAndExtraData::ForwardEntry()
{
    return forward_entry_;
}
}  // namespace txservice
