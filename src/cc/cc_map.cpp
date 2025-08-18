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

#include "cc/cc_map.h"

#include <utility>  // std::pair

#include "cc/local_cc_shards.h"
#include "cc_entry.h"
#include "tx_trace.h"

namespace txservice
{
void CcMap::MoveRequest(CcRequestBase *cc_req, uint32_t target_core_id)
{
    shard_->local_shards_.EnqueueCcRequest(
        shard_->core_id_, target_core_id, cc_req);
}

std::pair<LockType, CcErrorCode> CcMap::AcquireCceKeyLock(
    LruEntry *cce,
    uint64_t commit_ts,
    LruPage *page,
    RecordStatus cce_payload_status,
    CcRequestBase *req,
    uint32_t ng_id,
    int64_t ng_term,
    int64_t tx_term,
    LockType lock_type,
    CcOperation cc_op,
    IsolationLevel iso_level,
    CcProtocol protocol,
    uint64_t read_ts,
    CcMap *ccm)
{
    if (iso_level == IsolationLevel::Snapshot)
    {
        if (cc_op == CcOperation::ReadForWrite && read_ts < commit_ts)
        {
            LOG(WARNING) << "SI ReadForWrite, latest version not fits the read "
                            "timestamp. tx:"
                         << req->Txn();
            // For ReadForWrite under Snapshot Isolation,  we will return the
            // latest version, only if the latest version fits the read's
            // timestamp. Otherwise, we will return an error to abort the tx.
            // Because, snapshot isolation is a guarantee that all reads made in
            // a transaction will see a consistent snapshot of the database, and
            // the transaction itself will successfully commit only if no
            // updates it has made conflict with any concurrent updates made
            // since that snapshot.
            return std::pair<LockType, CcErrorCode>(
                LockType::NoLock, CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
        }
        else if (cc_op == CcOperation::Read ||
                 cc_op == CcOperation::ReadSkIndex)
        {
            NonBlockingLock *lock = cce->GetKeyLock();

            if (lock != nullptr && lock->HasWriteLock() &&
                lock->WLockTs() < read_ts)
            {
                // Having a write lock means the entry will be updated soon. If
                // wlock_ts_ is less than the read timestamp, this read may be
                // toward a future version, given that the tx may commit before
                // the read timestamp. Puts the request to the head of the
                // key's blocking queue to acquire the read lock. The read lock
                // ensures that the cc entry is not kicked out between when the
                // write lock is released and when this request is re-enqueued
                // and processed. The read lock is released when the request is
                // re-processed.
                lock->InsertBlockingQueue(req, LockType::ReadLock);
                shard_->CheckRecoverTx(lock->WriteLockTx(), ng_id, ng_term);

                return std::pair<LockType, CcErrorCode>(
                    LockType::NoLock, CcErrorCode::MVCC_READ_MUST_WAIT_WRITE);
            }
        }
    }

    TxNumber tx_number = req->Txn();
    LockOpStatus lock_op_status = LockOpStatus::Successful;
    CcErrorCode err_code = CcErrorCode::NO_ERROR;
    NonBlockingLock *lock = nullptr;

    if (lock_type != LockType::NoLock)
    {
        if (lock_type == LockType::WriteLock ||
            lock_type == LockType::WriteIntent ||
            cce_payload_status != RecordStatus::Deleted)
        {
            // When a range is locked, the bucket to which the range is mapped
            // is also locked. The request of locking the bucket comes from the
            // range cc map, so "this" does not refer to the bucket cc map. We
            // need to pass the bucket cc map via the input parameter.
            CcMap *lock_ccm = ccm == nullptr ? this : ccm;
            lock = &cce->GetOrCreateKeyLock(shard_, lock_ccm, page);
            lock_op_status = lock->AcquireLock(req, protocol, lock_type);
            if (lock_ccm->table_name_.StringView() ==
                    "tpcc.DISTRICT*$$D_W_ID_1_D_ID_1_D_NEXT_O_ID_1_D_TAX_1" &&
                lock_type != LockType::ReadLock &&
                lock_type != LockType::ReadIntent)
            {
                LOG(INFO) << ">> txn: " << req->Txn() << ", cce: " << cce
                          << ", lock_op_status: " << (int) lock_op_status;
            }
        }
        else
        {
            lock_type = LockType::NoLock;
            lock = cce->GetKeyLock();
        }
    }

    if (lock_op_status == LockOpStatus::Successful)
    {
        if (lock_type != LockType::NoLock)
        {
            shard_->UpsertLockHoldingTx(tx_number,
                                        tx_term,
                                        cce,
                                        lock_type == LockType::WriteLock,
                                        ng_id,
                                        table_name_.Type());
        }

        if (lock != nullptr && lock->HasWriteLock() &&
            lock->WriteLockTx() != tx_number)
        {
            shard_->CheckRecoverTx(lock->WriteLockTx(), ng_id, ng_term);
        }
        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Successful",
            cce,
            (
                [&req, &cce, &lock_type, &cce_payload_status]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"LockType\":")
                        .append(std::to_string((uint8_t) lock_type))
                        .append(",\"cce_payload_status\":")
                        .append(std::to_string((uint8_t) cce_payload_status))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(cce->GetKeyLock().DebugInfo());
                }));
    }
    else if (lock_op_status == LockOpStatus::Failed)
    {
        // check and recover conflicted transactions.
        assert(lock != nullptr);
        RecoverTxForLockConfilct(*lock, lock_type, ng_id, ng_term);
        auto [w_tx, w_lk_type] = lock->WriteTx();
        if (lock_type == LockType::WriteLock &&
            w_lk_type == NonBlockingLock::WriteLockType::NoWritelock)
        {
            err_code = CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT;
        }
        else
        {
            err_code = CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT;
        }

        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Failed",
            cce,
            (
                [&req, &cce, &lock_type, &cce_payload_status]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"LockType\":")
                        .append(std::to_string((uint8_t) lock_type))
                        .append(",\"cce_payload_status\":")
                        .append(std::to_string((uint8_t) cce_payload_status))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(cce->GetKeyLock().DebugInfo());
                }));
    }
    else
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Blocked",
            cce,
            (
                [&req, &cce, &lock_type, &cce_payload_status]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"LockType\":")
                        .append(std::to_string((uint8_t) lock_type))
                        .append(",\"cce_payload_status\":")
                        .append(std::to_string((uint8_t) cce_payload_status))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(cce->GetKeyLock().DebugInfo());
                }));

        // check and recover conflicted transactions.
        assert(lock != nullptr);
        RecoverTxForLockConfilct(*lock, lock_type, ng_id, ng_term);
        err_code = CcErrorCode::ACQUIRE_LOCK_BLOCKED;
    }

    return std::pair<LockType, CcErrorCode>(lock_type, err_code);
}

std::pair<LockType, CcErrorCode> CcMap::LockHandleForResumedRequest(
    LruEntry *cce,
    uint64_t commit_ts,
    RecordStatus cce_payload_status,
    CcRequestBase *req,
    uint32_t ng_id,
    int64_t ng_term,
    int64_t tx_term,
    CcOperation cc_op,
    IsolationLevel iso_level,
    CcProtocol protocol,
    uint64_t read_ts,
    bool is_covering_keys)
{
    TxNumber tx_number = req->Txn();
    LockType acquired_lock = LockTypeUtil::DeduceLockType(
        cc_op, iso_level, protocol, is_covering_keys);
    CcErrorCode err_code = CcErrorCode::NO_ERROR;
    NonBlockingLock *lock = cce->GetKeyLock();
    assert(lock != nullptr);

    if (acquired_lock == LockType::ReadLock &&
        cce_payload_status == RecordStatus::Deleted)
    {
        // The read lock has been acquired. But if the key has been deleted by
        // the prior tx, there is no point of keeping the lock.
        lock->ReleaseReadLock(tx_number, shard_);
        cce->RecycleKeyLock(*shard_);
        acquired_lock = LockType::NoLock;

        // DeleteLockHoldingTx is required. Because this may be a retried
        // request and the prior blocked request may has upsert the tx's lock
        // info in the shard.
        shard_->DeleteLockHoldingTx(tx_number, cce, ng_id);
    }
    else if (acquired_lock == LockType::WriteIntent &&
             iso_level == IsolationLevel::Snapshot && read_ts < commit_ts)
    {
        // The write intent has been acquired. Does not keep the write intent if
        // this tx under Snapshot Isolation (SI) is destined to fail. For
        // ReadForWrite under SI, if the tx' snapshot sees a key's old version,
        // but the tx also intends to update the key, the tx is destined to
        // fail. The transaction will successfully commit only if its updates do
        // not conflict with any concurrent updates made since its snapshot.
        LOG(WARNING) << "SI ReadForWrite, latest version not fits the read "
                        "timestamp. tx:"
                     << req->Txn();

        err_code = CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT;
        lock->ReleaseWriteIntent(tx_number, shard_);
        cce->RecycleKeyLock(*shard_);
        acquired_lock = LockType::NoLock;

        // DeleteLockHoldingTx is required. Because this may be a retried
        // request and the prior blocked request may has upsert the tx's lock
        // info in the shard.
        shard_->DeleteLockHoldingTx(tx_number, cce, ng_id);
    }
    else
    {
        shard_->UpsertLockHoldingTx(tx_number,
                                    tx_term,
                                    cce,
                                    acquired_lock == LockType::WriteLock,
                                    ng_id,
                                    table_name_.Type());
    }

    return std::pair<LockType, CcErrorCode>(acquired_lock, err_code);
}

void CcMap::RecoverTxForLockConfilct(NonBlockingLock &lock,
                                     LockType lock_type,
                                     uint32_t ng_id,
                                     int64_t ng_term)
{
    // check and recover conflicted transactions.
    switch (lock_type)
    {
    case LockType::WriteLock:
    {
        auto [write_tx, write_type] = lock.WriteTx();

        if (write_type != NonBlockingLock::WriteLockType::NoWritelock)
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "RecoverTxForLockConfilct",
                &lock,
                [&lock]() -> std::string
                {
                    return std::string("\"Lock\":")
                        .append(FMT_POINTER_TO_UINT64T(&lock))
                        .append(",\"associate\":\"key_lock_.write_lock\"");
                });

            shard_->CheckRecoverTx(write_tx, ng_id, ng_term);
        }
        else
        {
            // If the request fails to acquire the write lock
            // because of read locks, checks each read lock and
            // recovers if needed.
            for (const auto &read_tx : lock.ReadLocks())
            {
                shard_->CheckRecoverTx(read_tx, ng_id, ng_term);
            }
        }
        break;
    }
    case LockType::WriteIntent:
    {
        auto [write_tx, write_type] = lock.WriteTx();
        if (write_type != NonBlockingLock::WriteLockType::NoWritelock)
        {
            shard_->CheckRecoverTx(write_tx, ng_id, ng_term);
        }
        break;
    }
    case LockType::ReadLock:
    {
        if (lock.HasWriteLock())
        {
            shard_->CheckRecoverTx(lock.WriteLockTx(), ng_id, ng_term);
        }
        break;
    }
    default:
        break;
    }  // switch
}

void CcMap::DowngradeCceKeyWriteLock(LruEntry *cce, TxNumber tx_number)
{
    NonBlockingLock *lock = cce->GetKeyLock();
    if (lock != nullptr)
    {
        lock->DowngradeWriteLock(tx_number, shard_);
    }
}

void CcMap::ReleaseCceLock(NonBlockingLock *lock,
                           LruEntry *cce,
                           TxNumber tx_number,
                           uint32_t ng_id,
                           LockType lk_type,
                           bool recycle_lock,
                           TxObject *object) const
{
    if (lock == nullptr)
    {
        return;
    }

    LockType unlock_type = LockType::NoLock;
    switch (lk_type)
    {
    case LockType::ReadLock:
    {
        bool success = lock->ReleaseReadLock(tx_number, shard_);
        if (success)
        {
            unlock_type = LockType::ReadLock;
        }
        break;
    }
    case LockType::ReadIntent:
    {
        bool success = lock->ReleaseReadIntent(tx_number);
        if (success)
        {
            unlock_type = LockType::ReadIntent;
        }
        break;
    }
    case LockType::WriteLock:
    {
        bool success = lock->ReleaseWriteLock(tx_number, shard_, object);
        if (success)
        {
            unlock_type = LockType::WriteLock;
        }
        break;
    }
    default:
        unlock_type = lock->ClearTx(tx_number, shard_, object);
        break;
    }

    if (unlock_type != LockType::NoLock)
    {
        if (unlock_type == LockType::WriteLock)
        {
            lock->SetWLockTs(0);
        }

        shard_->DeleteLockHoldingTx(tx_number, cce, ng_id);
        if (recycle_lock)
        {
            cce->RecycleKeyLock(*shard_);
        }
    }
    else
    {
        // Otherwise, the lock should have been recycled.
        // assert(!lock->IsEmpty());
        // If its owner KeyGapLockAndExtraData has blocked commands, the lock
        // will not be recycled even if it is empty.
    }
}
}  // namespace txservice
