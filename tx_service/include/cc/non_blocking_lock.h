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
#pragma once

#include <glog/logging.h>

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "cc_protocol.h"
#include "cc_req_base.h"
#include "circular_queue.h"
#include "error_messages.h"
#include "standby.h"
#include "tx_command.h"
#include "tx_id.h"
#include "tx_object.h"

namespace txservice
{
template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
struct CcEntry;

class CcMap;
struct LruPage;
struct LruEntry;

class NonBlockingLock
{
public:
    using Uptr = std::unique_ptr<NonBlockingLock>;

    enum struct WriteLockType
    {
        NoWritelock = 0,
        WriteLock,
        WriteIntent
    };

    NonBlockingLock()
    {
    }

    ~NonBlockingLock()
    {
    }

    NonBlockingLock(const NonBlockingLock &rhs) = delete;
    NonBlockingLock(NonBlockingLock &&rhs) = delete;
    NonBlockingLock &operator=(NonBlockingLock &&rhs) = delete;

    void Reset()
    {
        read_intentions_.clear();
        read_locks_.clear();
        read_cnt_ = 0;
        write_lk_type_ = WriteLockType::NoWritelock;
        write_txn_ = 0;
        blocking_queue_.Reset();
        queue_block_cmds_.Reset();
        wlock_ts_ = 0;
    }

    /**
     * @brief Tries to acqurie the write lock. The operation succeeds, if no one
     * is holding the read lock, the write lock or the write intent. Note that
     * the write lock does not conflict with read intentions. The net effect of
     * the failed operation varies by concurrency control (cc) protocols: for
     * 2PL, the request is put into a waiting queue; for OCC/MVCC protocols, the
     * request returns without blocking.
     *
     * @param cc_req The cc request that tries to acquire the write lock.
     * @param protocol The cc protocol control the tx uses.
     * @return true, if the request acquires the write lock successfully; false,
     * if the request is blocked and put into the waiting queue.
     */
    bool AcquireWriteLock(CcRequestBase *cc_req, CcProtocol protocol);

    bool ReleaseWriteLock(TxNumber tx_number,
                          CcShard *ccs,
                          TxObject *object = nullptr);

    /**
     *  @brief Release the write lock and add the write intent, take effect only
     * when write lock is owned by the tx_number
     */
    void DowngradeWriteLock(TxNumber tx_number, CcShard *ccs);

    bool AcquireWriteIntent(CcRequestBase *cc_req, CcProtocol protocol);

    bool ReleaseWriteIntent(TxNumber tx_number, CcShard *ccs);

    /**
     * @brief Fast path to add read lock. Only used by Catalog read.
     *
     * @param tx_number
     * @return
     */
    bool AcquireReadLockFast(TxNumber tx_number);

    /**
     * @brief Fast path to release read lock. Only used by Catalog read.
     *
     * @param ccs
     * @return
     */
    bool ReleaseReadLockFast(CcShard *ccs, TxNumber tx_number);

    /**
     * @brief Tries to acquire the read lock. Only tx's under 2PL acquire read
     * locks. The operation succeeds, if no one is holding the write lock and no
     * write lock request is blocked. The operation is blocked and put into the
     * waiting queue, if the write lock is held by someone else or someone is
     * blocked and waiting for the write lock.
     *
     * @param cc_req The cc request that tries to acquire the write lock.
     * @return true, if the request acquires the read lock successfully; false,
     * if the request is blocked and put into the blocking queue.
     */
    bool AcquireReadLock(CcRequestBase *cc_req);

    bool ReleaseReadLock(TxNumber tx_number, CcShard *ccs);

    /**
     * @brief Acquires a read intent. Tx's under OCC/MVCC acquire read intents
     * for read operations. Read intents do not block writes. Their goal is to
     * prevent the cache replacement algorithm from kicking out the cc entry
     * from the cc map. Hence, acquiring read intent always succeeds.
     *
     * @param tx_number The tx who acquires the read intention
     * @return true, if the tx adds a read intent to the lock.
     * @return false, if the tx already holds a lock/intent higher than read
     * intent and no read intent is added.
     */
    bool AcquireReadIntent(TxNumber tx_number);

    bool ReleaseReadIntent(TxNumber tx_number, bool release_all = true);

    LockOpStatus AcquireLock(CcRequestBase *cc_req,
                             CcProtocol protocol,
                             LockType lock_type);

    /**
     * @brief Answers what AcquireLock() WOULD return for this request,
     * without acquiring anything and without enqueuing the requester.
     *
     * This exists for the paged-object Yield protocol (eloqkv
     * docs/08-paged-objects.md §6), which defers lock acquisition until
     * ExecuteOn is known to complete: a command that will yield must leave
     * behind neither a lock nor a blocking-queue entry.
     *
     * It must mirror AcquireLock EXACTLY — blocking-queue fairness, the
     * already-held fast paths, and the per-protocol Failed/Blocked
     * distinction included. The scheme's correctness rests on
     * test-then-acquire being atomic on the shard core, so any divergence
     * between this and the real acquisition is a correctness bug;
     * LockGrantability-Test pins the two together across the lock lattice.
     *
     * Grantability itself is protocol-independent — the protocol only
     * selects how a failure is reported (OCC fails, Locking/OccRead blocks)
     * — but it is reproduced here so the two are directly comparable.
     */
    LockOpStatus WouldAcquireLock(TxNumber tx_number,
                                  CcProtocol protocol,
                                  LockType lock_type) const;

    void InsertBlockingQueue(CcRequestBase *cc_req, LockType lock_type);

    bool IsEmpty() const;

    /**
     * @brief The number of requests parked in the blocking queue. Read-only;
     * exposed for the grantability test's side-effect check (a prediction
     * must never enqueue) and for lock-contention observability.
     */
    size_t BlockingQueueSize() const
    {
        return blocking_queue_.Size();
    }

    TxNumber WriteLockTx() const;

    bool HasWriteLock() const;

    std::pair<TxNumber, WriteLockType> WriteTx() const
    {
        return {write_txn_, write_lk_type_};
    }

    bool HasWriteLockOrWriteIntent(TxNumber txn) const
    {
        return write_lk_type_ != WriteLockType::NoWritelock &&
               write_txn_ == txn;
    }

    bool HasWriteLock(TxNumber txn) const
    {
        return write_lk_type_ == WriteLockType::WriteLock && write_txn_ == txn;
    }

    LockType ClearTx(TxNumber tx_number,
                     CcShard *ccs,
                     TxObject *object = nullptr);

    const absl::flat_hash_set<TxNumber> &ReadLocks() const;
    const absl::flat_hash_map<TxNumber, uint32_t> &ReadIntents() const;

    uint64_t WLockTs() const
    {
        return wlock_ts_;
    }
    void SetWLockTs(uint64_t ts)
    {
        wlock_ts_ = ts;
    }

    std::string DebugInfo()
    {
        std::string debug_string = "read_intentions: ";
        for (auto it = read_intentions_.begin(); it != read_intentions_.end();
             it++)
        {
            debug_string.append(std::to_string(it->first));
            debug_string.append(",");
        }

        debug_string.append(" ,read_locks: ");
        for (auto it = read_locks_.begin(); it != read_locks_.end(); it++)
        {
            debug_string.append(std::to_string(*it));
            debug_string.append(",");
        }

        debug_string.append(", read_cnt_(for catalog lock): ");
        debug_string.append(std::to_string(read_cnt_));

        debug_string.append(" ,write_lock: ");
        if (write_lk_type_ != WriteLockType::NoWritelock)
        {
            debug_string.append(std::to_string(write_txn_));
            debug_string.append(":");
            if (write_lk_type_ == WriteLockType::WriteLock)
            {
                debug_string.append("lock");
            }
            else
            {
                debug_string.append("intent");
            }
        }
        else
        {
            debug_string.append("empty");
        }

        return debug_string;
    }
    std::vector<TxNumber> GetBlockTxIds(TxNumber exclude_id);
    void AbortQueueRequest(TxNumber txid,
                           CcErrorCode err = CcErrorCode::DEAD_LOCK_ABORT);
    bool FindQueueRequest(TxNumber txid);

    void AbortAllQueuedRequests(CcErrorCode err = CcErrorCode::DEAD_LOCK_ABORT);

    LockType SearchLock(TxNumber txn);

    void PushBlockCmdRequest(CcRequestBase *req)
    {
        queue_block_cmds_.Enqueue(std::move(req));
    }

    bool AbortBlockCmdRequest(TxNumber txid, CcErrorCode err, CcShard *ccs);

private:
    struct LockQueueEntry
    {
        LockQueueEntry() = default;

        LockQueueEntry(CcRequestBase *req, LockType type)
            : req_(req), lk_type_(type)
        {
        }

        LockQueueEntry(const LockQueueEntry &rhs)
        {
            req_ = rhs.req_;
            lk_type_ = rhs.lk_type_;
        }

        LockQueueEntry(LockQueueEntry &&rhs)
        {
            req_ = rhs.req_;
            lk_type_ = rhs.lk_type_;
        }

        LockQueueEntry &operator=(const LockQueueEntry &rhs)
        {
            if (this != &rhs)
            {
                req_ = rhs.req_;
                lk_type_ = rhs.lk_type_;
            }

            return *this;
        }

        LockQueueEntry &operator=(LockQueueEntry &&rhs)
        {
            if (this != &rhs)
            {
                req_ = rhs.req_;
                lk_type_ = rhs.lk_type_;
            }

            return *this;
        }

        CcRequestBase *req_{nullptr};
        LockType lk_type_{LockType::ReadLock};
    };

    void ExecuteQueuedRequest(const LockQueueEntry &queue_head, CcShard *ccs);
    void UpgradeLock(TxNumber tx_number, LockType lock_type);
    void TryPopBlockingQueue(CcShard *ccs);

    bool NoReadLockConflict(TxNumber tx_number) const
    {
        if (read_cnt_ > 0)
        {
            DLOG(INFO) << "ReadLockConflict, read_cnt_: " << read_cnt_;
            return false;
        }

        return read_locks_.empty() ||
               (read_locks_.size() == 1 && *read_locks_.begin() == tx_number);
    }

    bool NoWriteLockConflict(TxNumber tx_number) const
    {
        return write_lk_type_ != WriteLockType::WriteLock ||
               write_txn_ == tx_number;
    }

    bool NoWriteConflict(TxNumber txn) const
    {
        return write_lk_type_ == WriteLockType::NoWritelock ||
               write_txn_ == txn;
    }

    // Pop a blocked command request from the waiting queue and reenqueue it to
    // cc request queue. Returns true if a command request is popped.
    bool PopBlockCmdRequest(CcShard *ccs, TxObject *object);

    // Read intentions do not block writes. They are used by a tx under
    // OCC/MVCC protocols to mark that the tx is accessing the data item and
    // to prevent the cache replacement algorithm from kicking out the
    // item's concurrency control (cc) entry from the cc map before the tx
    // finishes.
    //
    // ScanSliceCc might scan records batch-by-batch. After scan previous batch,
    // scanner INCR a read intent on the last record of the batch, and DECR it
    // when scan next batch.
    absl::flat_hash_map<TxNumber, uint32_t> read_intentions_;
    // Tx's who have acquired read locks
    absl::flat_hash_set<TxNumber> read_locks_;
    // How many readers has acquired read locks. Only used for Catalog read fast
    // path.
    int read_cnt_{};

    // Default-initialized, not merely set by Reset(): a pooled lock is always
    // Reset() before use, but a default-constructed one (tests, or any future
    // direct use) would otherwise start with an indeterminate write lock type
    // and take conflict decisions on garbage.
    WriteLockType write_lk_type_{WriteLockType::NoWritelock};
    TxNumber write_txn_{0};

    // The time when a write tx acquires the write lock on this lock.
    uint64_t wlock_ts_;

    // blocking_queue_ stores the requests that 1) want to acquire
    // lock/intent but failed due to conflict, or 2) want to read a pk
    // record whose commit ts is less than the commit ts of the
    // corresponding secondary index the transaction just read, under which
    // circumstance the pk read should wait until the pk is updated to
    // maintain the consistency of pk-sk mapping. There are four types of
    // lock requests can be in blocking queue: Write Lock(WL), Write
    // Intent(WI), Read Lock(RL) and No Lock(NL). The conflict map is that
    // WL conflicts with WL/WI/RL, WI conflicts with WL/WI and RL conflicts
    // with WL. NL denotes a pk read request under read committed isolation
    // level. NL requests are always inserted to the beginning of
    // blocking_queue_ because it will not introduce any conflict with other
    // tansactions.
    CircularQueue<LockQueueEntry> blocking_queue_;

    // blocked commands that wait to pop and execute after the conditions are
    // satisfied.
    CircularQueue<CcRequestBase *> queue_block_cmds_;

    template <typename KeyT,
              typename ValueT,
              bool VersionedRecord,
              bool RangePartitioned>
    friend struct CcEntry;
};

// TODO(liunyl): Lock structure needs to be separated for versioned and
// non-versioned payloads.
/**
 * The lock structure and extra data fields that are accessed with lock
 * acquired. This structure is assigned on-demand to reduce CcEntry's memory
 * overhead.
 */
struct FetchHub;

class KeyGapLockAndExtraData
{
public:
    using uptr = std::unique_ptr<KeyGapLockAndExtraData>;

    KeyGapLockAndExtraData() = default;

    // Out of line: fetch_hub_ is a unique_ptr over the forward-declared
    // FetchHub (cc/page_fetch.h).
    ~KeyGapLockAndExtraData();

    void Reset(CcMap *ccm, LruPage *page, LruEntry *entry)
    {
        key_lock_.Reset();
        ccm_ = ccm;
        page_ = page;
        entry_ = entry;

        dirty_payload_ = nullptr;
        dirty_payload_status_ = RecordStatus::NonExistent;
        pending_cmd_ = nullptr;
        buffered_cmd_list_.Clear();
        forward_entry_.reset();
        entry_pin_count_ = 0;
        // Recycle precedes reuse, and IsEmpty() forbids recycling with
        // outstanding page fetches — so the hub must already be empty here
        // (docs/08-paged-objects.md §7).
        ResetFetchHub();
    }

    void SetUsedStatus(bool is_used);

    bool GetUsedStatus() const
    {
        return in_use_;
    }

    bool SafeToRecycle() const;

    NonBlockingLock *KeyLock()
    {
        return &key_lock_;
    }

    CcMap *GetCcMap() const
    {
        return ccm_;
    }

    LruPage *GetCcPage() const
    {
        return page_;
    }

    LruEntry *GetCcEntry() const
    {
        return entry_;
    }

    // TODO: allow CcEntry to be moved to another memory space, needs to update
    // the lock's entry field.
    void UpdateCcEntry(LruEntry *entry)
    {
        entry_ = entry;
    }

    void UpdateCcPage(LruPage *new_page)
    {
        page_ = new_page;
    }

    bool IsEmpty()
    {
        if (key_lock_.IsEmpty())
        {
            // There must be no pending command and dirty payload if lock is
            // empty.
            assert((std::holds_alternative<TxCommand *>(pending_cmd_)
                        ? std::get<TxCommand *>(pending_cmd_) == nullptr
                        : std::get<std::unique_ptr<TxCommand>>(pending_cmd_) ==
                              nullptr) &&
                   dirty_payload_ == nullptr &&
                   (dirty_payload_status_ == RecordStatus::NonExistent ||
                    dirty_payload_status_ == RecordStatus::Deleted));
            // The fetch-hub clause is the recycle gate for paged objects
            // (docs/08-paged-objects.md §7): recycling this structure while
            // the store handler still holds pointers into an outstanding
            // PageFetch would be a use-after-free. Every page fetch also
            // holds an entry pin from issue to completion, so a non-empty
            // hub already implies entry_pin_count_ > 0 — the explicit test
            // is defense in depth against the pin discipline drifting.
            return !HasBufferedCommandList() && entry_pin_count_ == 0 &&
                   !HasFetchHubWork();
        }
        else
        {
            return false;
        }
    }

    std::variant<TxCommand *, std::unique_ptr<TxCommand>> PendingCmd()
    {
        return std::move(pending_cmd_);
    }

    TxCommand *GetPendingCmd() const
    {
        TxCommand *pending_cmd = nullptr;
        if (std::holds_alternative<TxCommand *>(pending_cmd_))
        {
            pending_cmd = std::get<TxCommand *>(pending_cmd_);
        }
        else
        {
            assert(std::holds_alternative<std::unique_ptr<TxCommand>>(
                pending_cmd_));
            pending_cmd =
                std::get<std::unique_ptr<TxCommand>>(pending_cmd_).get();
        }
        return pending_cmd;
    }

    void SetPendingCmd(
        std::variant<TxCommand *, std::unique_ptr<TxCommand>> cmd)
    {
        pending_cmd_ = std::move(cmd);
    }
    bool IsNullPendingCmd()
    {
        return std::holds_alternative<TxCommand *>(pending_cmd_)
                   ? std::get<TxCommand *>(pending_cmd_) == nullptr
                   : std::get<std::unique_ptr<TxCommand>>(pending_cmd_) ==
                         nullptr;
    }
    void AddPin()
    {
        entry_pin_count_++;
    }
    void ReleasePin()
    {
        assert(entry_pin_count_ > 0);
        entry_pin_count_--;
    }

    /**
     * @brief The page-fetch hub for paged large objects
     * (docs/08-paged-objects.md §7; cc/page_fetch.h). Lazily allocated on
     * the first page fetch; the accessors are out of line because FetchHub
     * is forward-declared here.
     */
    FetchHub &GetOrCreateFetchHub();

    FetchHub *FetchHubPtr()
    {
        return fetch_hub_.get();
    }

    bool HasFetchHubWork() const;
    std::unique_ptr<TxObject> DirtyPayload()
    {
        return std::move(dirty_payload_);
    }

    /**
     * @brief Non-owning look at the dirty payload. DirtyPayload() above MOVES
     * it out, which a caller that only needs to inspect the object must not do
     * (docs/08 §7: a page-fetch completion has to reach the dirty payload's
     * page state without taking it away from the transaction that owns it).
     */
    TxObject *PeekDirtyPayload() const
    {
        return dirty_payload_.get();
    }

    void SetDirtyPayload(std::unique_ptr<TxObject> dirty_payload)
    {
        dirty_payload_ = std::move(dirty_payload);
    }

    RecordStatus DirtyPayloadStatus() const
    {
        return dirty_payload_status_;
    }

    void SetDirtyPayloadStatus(RecordStatus status)
    {
        dirty_payload_status_ = status;
    }

    BufferedTxnCmdList &BufferedCommandList()
    {
        return buffered_cmd_list_;
    }

    bool HasBufferedCommandList()
    {
        if (!buffered_cmd_list_.Empty())
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    StandbyForwardEntry *ForwardEntry();
    void SetForwardEntry(std::unique_ptr<StandbyForwardEntry> entry);
    std::unique_ptr<StandbyForwardEntry> ReleaseForwardEntry();

    void ClearTx()
    {
        pending_cmd_ = nullptr;
        dirty_payload_ = nullptr;
        dirty_payload_status_ = RecordStatus::NonExistent;
        forward_entry_ = nullptr;
    }

private:
    NonBlockingLock key_lock_;
    bool in_use_{false};
    uint64_t last_used_ts_{0};
    // the lock object can be recycled (released in memory) in 30 seconds since
    // it was last used
    static inline uint64_t recycle_interval_us_ = 30000000;
    CcMap *ccm_{nullptr};
    LruPage *page_{nullptr};
    LruEntry *entry_{nullptr};

    std::variant<TxCommand *, std::unique_ptr<TxCommand>> pending_cmd_{nullptr};
    // temporary object to process subsequent commands in the same txn
    std::unique_ptr<TxObject> dirty_payload_;
    // status of temporary object
    RecordStatus dirty_payload_status_{RecordStatus::NonExistent};

    void ResetFetchHub();

    BufferedTxnCmdList buffered_cmd_list_;
    std::unique_ptr<StandbyForwardEntry> forward_entry_;
    // The ENTRY pins on the cce (as distinct from the per-page pin counts a
    // paged payload keeps, docs/08-paged-objects.md §4). For cases that we
    // need to keep the cce in memory for a while and we are not in a
    // transaction context, we use pins to prevent the cce from being
    // recycled.
    uint32_t entry_pin_count_{0};
    // Outstanding page fetches for a paged payload (docs/08 §7): null for
    // every entry that has never page-faulted.
    std::unique_ptr<FetchHub> fetch_hub_;
};

}  // namespace txservice
