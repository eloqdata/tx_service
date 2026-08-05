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
#include "cc/cc_entry.h"

#include "cc/cc_shard.h"
#include "cc/page_fetch.h"
#include "error_messages.h"
#include "tx_record.h"

namespace txservice
{
TxObject *TxCommand::CommitOn(TxObject *obj_ptr, const PagedCommitContext &ctx)
{
    TxObject *out = CommitOn(obj_ptr);
    if (out == nullptr &&
        RetirePagedPayloadOnDelete(obj_ptr, ctx.lke_, ctx.shard_))
    {
        // The deleted paged block is RETAINED (tagged deletion-retained)
        // so the checkpoint can read its page-id list; returning the same
        // pointer keeps every caller's "pointer changed" logic inert.
        return obj_ptr;
    }
    return out;
}

bool RetirePagedPayloadOnDelete(TxObject *obj,
                                KeyGapLockAndExtraData *lke,
                                CcShard *shard)
{
    PagedTxObject *dead = obj != nullptr ? obj->AsPaged() : nullptr;
    if (dead == nullptr)
    {
        return false;
    }
    // Orphan first: a successor incarnation restarts page ids at 0, so a
    // fetch issued for this block must never install into whatever a
    // replayed recreate builds next (docs/08 §7).
    if (lke != nullptr)
    {
        FetchHub *hub = lke->FetchHubPtr();
        if (hub != nullptr)
        {
            hub->SpliceAllToOrphans();
            std::vector<CcRequestBase *> parked = hub->TakeAllParked();
            if (shard != nullptr)
            {
                for (CcRequestBase *req : parked)
                {
                    shard->Enqueue(shard->LocalCoreId(), req);
                }
            }
            else
            {
                assert(parked.empty() &&
                       "parked requests with no shard to wake them");
            }
        }
    }
    // The complete volatile teardown: frames, §8 admission reservations,
    // pending faults, tx contexts, LRU. Only the page-id metadata the
    // deletion flush reads survives. AbandonAllTxContexts alone was NOT
    // enough — it clears contexts without decrementing per-slot pins, so a
    // pin-respecting release left pinned frames and every reservation
    // charged to the shard heap until the deletion checkpoint (a reported
    // defect: a stalled checkpoint held that memory indefinitely).
    dead->TeardownForDeletion();
    dead->MarkDeletionRetained();
    return true;
}

template <bool Versioned, bool RangePartitioned>
RecordStatus VersionedLruEntry<Versioned, RangePartitioned>::PayloadStatus()
    const
{
    // The lowest 4 bits encode the record status.
    RecordStatus status =
        static_cast<RecordStatus>(entry_info_.commit_ts_and_status_ & 0x0F);
    return status;
}

template <bool Versioned, bool RangePartitioned>
void VersionedLruEntry<Versioned, RangePartitioned>::SetCommitTsPayloadStatus(
    uint64_t ts, RecordStatus status)
{
    uint8_t stat = static_cast<uint8_t>(status);
    uint64_t curr_ts = entry_info_.commit_ts_and_status_ >> 8;

    if (curr_ts < ts)
    {
        entry_info_.commit_ts_and_status_ = (ts << 8) | stat;
    }

    if (!Versioned && txservice_skip_kv && status == RecordStatus::Deleted)
    {
        // Mark entry as flushed on skip_kv mode.
        entry_info_.commit_ts_and_status_ |= 0x10;
    }
}

template <bool Versioned, bool RangePartitioned>
bool VersionedLruEntry<Versioned, RangePartitioned>::IsPersistent() const
{
    if (!txservice_skip_kv && Sharder::Instance().StandbyNodeTerm() >= 0 &&
        Sharder::Instance().GetDataStoreHandler()->IsSharedStorage())
    {
        // If this is a follower with shared kv, check the ng leader's ckpt_ts.
        return CommitTs() <= Sharder::Instance().NativeNodeGroupCkptTs();
    }

    if (Versioned)
    {
        return CommitTs() <= CkptTs();
    }
    else
    {
        // The fifth bit represents if the latest version has been flushed.
        return entry_info_.commit_ts_and_status_ & 0x10;
    }
}

template <bool Versioned, bool RangePartitioned>
bool VersionedLruEntry<Versioned, RangePartitioned>::IsDirty() const
{
    // Only check CommitTs > 1 to exclude initial entries
    if (CommitTs() <= 1)
    {
        return false;
    }

    if (Versioned)
    {
        // For versioned records, dirty means CommitTs > CkptTs
        return CommitTs() > CkptTs();
    }
    else
    {
        // For non-versioned records, dirty means the flush bit (5th bit) is not
        // set
        return !(entry_info_.commit_ts_and_status_ & 0x10);
    }
}

template <bool Versioned, bool RangePartitioned>
bool VersionedLruEntry<Versioned, RangePartitioned>::IsFree() const
{
    // As long as all locks are released, the lock associated with this cc entry
    // should be recycled.
    assert(cc_lock_and_extra_ == nullptr || !cc_lock_and_extra_->IsEmpty());

    return cc_lock_and_extra_ == nullptr && IsPersistent();
}

NonBlockingLock &LruEntry::GetOrCreateKeyLock(CcShard *ccs,
                                              CcMap *ccm,
                                              LruPage *page)
{
    if (cc_lock_and_extra_ == nullptr)
    {
        cc_lock_and_extra_ = ccs->NewLock(ccm, page, this);
    }

    assert(cc_lock_and_extra_->GetCcMap() == ccm);
    // For cc entries of the bucket cc map, the input page may be null.
    assert(page == nullptr || cc_lock_and_extra_->GetCcPage() == nullptr ||
           cc_lock_and_extra_->GetCcPage() == page);
    return *cc_lock_and_extra_->KeyLock();
}

NonBlockingLock *LruEntry::GetKeyLock() const
{
    return cc_lock_and_extra_ == nullptr ? nullptr
                                         : cc_lock_and_extra_->KeyLock();
}

NonBlockingLock *LruEntry::GetGapLock() const
{
    assert("Gap lock unsupported.");
    return nullptr;
}

KeyGapLockAndExtraData *LruEntry::GetLockAddr() const
{
    return cc_lock_and_extra_;
}

bool LruEntry::RecycleKeyLock(CcShard &ccs)
{
    if (cc_lock_and_extra_ != nullptr && cc_lock_and_extra_->IsEmpty())
    {
        // recycle key lock if all the locks in lock entry are released.
        cc_lock_and_extra_->SetUsedStatus(false);
        ccs.DecreaseLockCount();
        cc_lock_and_extra_ = nullptr;
        return true;
    }

    return false;
}

void LruEntry::ClearLocks(CcShard &ccs,
                          NodeGroupId ng_id,
                          bool invalidate_owner_term)
{
    if (cc_lock_and_extra_ == nullptr)
    {
        return;
    }

    NonBlockingLock *key_lock = cc_lock_and_extra_->KeyLock();

    // Deletes the write lock/intent.
    auto [w_tx, w_type] = key_lock->WriteTx();
    if (w_type != NonBlockingLock::WriteLockType::NoWritelock)
    {
        ccs.DeleteLockHoldingTx(w_tx, this, ng_id);
    }

    // Deletes key read locks.
    for (const TxNumber &txn : key_lock->ReadLocks())
    {
        ccs.DeleteLockHoldingTx(txn, this, ng_id);
    }

    for (const auto &[txn, cnt] : key_lock->ReadIntents())
    {
        ccs.DeleteLockHoldingTx(txn, this, ng_id);
    }

    // clean up blocked cc reqs
    key_lock->AbortAllQueuedRequests(CcErrorCode::REQUESTED_NODE_NOT_LEADER);

    int64_t buffered_cmd_cnt_decr =
        cc_lock_and_extra_->BufferedCommandList().Size();
    ccs.UpdateBufferedCommandCnt(-buffered_cmd_cnt_decr);
    cc_lock_and_extra_->Reset(nullptr, nullptr, nullptr);
    // reset lock entry in ccshard lock array to make it reusable.
    cc_lock_and_extra_->SetUsedStatus(false);
    cc_lock_and_extra_ = nullptr;
    ccs.DecreaseLockCount();
}

void LruEntry::UpdateBufferedCommandCnt(CcShard *shard, int64_t delta)
{
    shard->UpdateBufferedCommandCnt(delta);
}

template <bool Versioned, bool RangePartitioned>
void VersionedLruEntry<Versioned, RangePartitioned>::SetBeingCkpt()
{
    entry_info_.commit_ts_and_status_ =
        entry_info_.commit_ts_and_status_ | 0x20;
}

template <bool Versioned, bool RangePartitioned>
void VersionedLruEntry<Versioned, RangePartitioned>::ClearBeingCkpt()
{
    uint64_t mask = UINT64_MAX;  // All bits set to 1
    mask &= ~(1ULL << 5);        // Clear the 6th bit
    entry_info_.commit_ts_and_status_ =
        entry_info_.commit_ts_and_status_ & mask;
}

template <bool Versioned, bool RangePartitioned>
bool VersionedLruEntry<Versioned, RangePartitioned>::GetBeingCkpt() const
{
    return entry_info_.commit_ts_and_status_ & 0x20;
}

TxKey FlushRecord::Key() const
{
    if (std::holds_alternative<TxKey>(flush_key_))
    {
        return std::get<TxKey>(flush_key_).GetShallowCopy();
    }
    assert(false &&
           "The flush key is of type KeyIndex and cannot return the key "
           "pointer.");
    return TxKey();
}

template struct VersionedLruEntry<true, true>;
template struct VersionedLruEntry<true, false>;
template struct VersionedLruEntry<false, true>;
template struct VersionedLruEntry<false, false>;

}  // namespace txservice
