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
#include "error_messages.h"
#include "tx_record.h"

namespace txservice
{

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
    if (Sharder::Instance().StandbyNodeTerm() >= 0 &&
        Sharder::Instance().GetDataStoreHandler()->IsSharedStorage())
    {
        // If this is a follower with shared kv, all cce is treated as persisted
        // since primary node will write them to kv.
        return true;
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

    for (const TxNumber &txn : key_lock->ReadIntents())
    {
        ccs.DeleteLockHoldingTx(txn, this, ng_id);
    }

    LOG(INFO) << "== not leader: clear locks";
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
