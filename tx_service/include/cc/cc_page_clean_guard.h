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

#include <algorithm>
#include <bitset>
#include <memory>
#include <utility>
#include <vector>

#include "cc_entry.h"
#include "cc_map.h"
#include "cc_req_misc.h"
#include "cc_request.h"
#include "cc_shard.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
/**
 * Procedure of cleaning one page is divided into two subprocedure: Mark and
 * Compact. Mark marks those to-be-cleaned entries by assigning them in
 * clean_set_. Compact erase those to-be-cleaned key/entries. Note that when a
 * page's first key can be cleanned, the ccmp_ has to erase/update its node key.
 *
 * Mark is done by LocalCcShards::KickoutPage.
 */
template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
struct CcPageCleanGuard
{
public:
    explicit CcPageCleanGuard(
        CcShard *cc_shard,
        NodeGroupId cc_ng_id,
        const TableName &table_name,
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page)
        : cc_shard_(cc_shard),
          cc_ng_id_(cc_ng_id),
          table_name_(table_name),
          page_(page)
    {
    }

    virtual ~CcPageCleanGuard() = default;

    virtual bool CleanSuccess() const = 0;

    virtual void Compact()
    {
        // Those to-be-cleaned entries have been marked. Erase
        // those keys/entries now.

        auto key_it = page_->keys_.begin();
        auto entry_it = page_->entries_.begin();
        uint64_t smallest_ttl = UINT64_MAX;

        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
            SoftwarePrefetch(
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                    PREFETCH_FLAG_PAYLOAD,
                page_->entries_.begin(),
                page_->entries_.end());  // Prefetch for TTL
        for (size_t idx = 0; idx < page_->Size(); ++idx)
        {
            if (clean_set_[idx] == false)
            {
                if (page_->entries_[idx]->PayloadStatus() ==
                    RecordStatus::Normal)
                {
                    if (page_->entries_[idx]->payload_.cur_payload_ &&
                        page_->entries_[idx]->payload_.cur_payload_->HasTTL())
                    {
                        uint64_t ttl = page_->entries_[idx]
                                           ->payload_.cur_payload_->GetTTL();
                        smallest_ttl = ttl < smallest_ttl ? ttl : smallest_ttl;
                    }
                }
                else if (page_->entries_[idx]->PayloadStatus() ==
                         RecordStatus::Deleted)
                {
                    smallest_ttl = 0;
                }
                *key_it = std::move(page_->keys_[idx]);
                *entry_it = std::move(page_->entries_[idx]);
                ++key_it;
                ++entry_it;
            }
        }

        page_->keys_.erase(key_it, page_->keys_.end());

        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
            SoftwarePrefetch(
                CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                    PREFETCH_FLAG_BLOB,
                page_->entries_.begin(),
                page_->entries_.end());  // Prefetch for mi_free
        page_->entries_.erase(entry_it, page_->entries_.end());

        page_->smallest_ttl_ = smallest_ttl;
    }

    bool ToCleanPageHeadKey() const
    {
        return clean_set_[0];
    }

    // Number of keys freed.
    size_t FreedCount() const
    {
        return free_cnt_;
    }

    // Number of dirty keys freed.
    size_t DirtyFreedCount() const
    {
        return dirty_freed_cnt_;
    }

    // If any valid key is evicted. This is used to update if ccm is still fully
    // cached. Note that this does not include
    // 1. Deleted/expired keys. Freeing deleted keys does not affect cache
    // completeness.
    // 2. Keys that are migrated away. Since keys are removed since the
    // ownership changed, it does not affect ccm completeness.
    bool EvictedValidKeys() const
    {
        return evicted_valid_key_;
    }

    // Number of normal keys freed.
    size_t CleanObjectCount() const
    {
        return clean_obj_cnt_;
    }

protected:
    struct CanBeCleanedResult
    {
        bool can_be_cleaned_;
        bool delay_free_;
    };
    virtual CanBeCleanedResult CanBeCleaned(
        const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
        const uint64_t *const dirty_range_ts = nullptr,
        const uint64_t *const range_last_sync_ts = nullptr) const = 0;

    virtual bool IsCleanTarget(
        const KeyT &key,
        const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce)
        const = 0;

    virtual void Reserve(uint8_t idx, bool is_clean_target) = 0;

    virtual bool NeedInvalidateLockTerm() const = 0;

    virtual bool RemoveFromKeyCache() const = 0;

    // If cleaning normarl keys will affect ccm_has_full_entries_.
    // For some cleaning operations, we're removing keys that are no
    // longer owned by this ccm, in which case it won't affect cache
    // completeness.
    virtual bool AffectCacheCompleteness() const = 0;

    size_t MarkCleanInRange(TemplateStoreRange<KeyT> *store_range,
                            size_t idx_in_page,
                            bool &kickout_any,
                            const uint64_t *const dirty_range_ts,
                            const uint64_t &range_last_sync_ts)
    {
        assert(RangePartitioned);
        kickout_any = false;
        bool remove_from_key_cache = RemoveFromKeyCache();

        size_t range_end_idx;
        const KeyT *range_end_key = store_range->RangeEndKey();
        assert(range_end_key);
        assert(page_->keys_[0] <= *range_end_key);
        range_end_idx = page_->LowerBound(*range_end_key);

        while (idx_in_page < range_end_idx)
        {
            const KeyT &start_key = page_->keys_[idx_in_page];

            // Clean next slice.
            TemplateStoreSlice<KeyT> *store_slice =
                store_range->FindSlice(start_key);
            assert(store_slice);

            size_t slice_end_idx;
            const KeyT *slice_end_key = store_slice->EndKey();
            assert(slice_end_key);
            assert(page_->keys_[0] <= *slice_end_key);
            slice_end_idx = page_->LowerBound(*slice_end_key);

            assert(slice_end_idx <= range_end_idx);

            bool slice_kicked = false;
            bool tried_slice_kick = false;

            CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                SoftwarePrefetch(
                    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                        PREFETCH_FLAG_CCENTRY,
                    page_->entries_.begin() + idx_in_page,
                    page_->entries_.begin() + slice_end_idx);
            for (size_t idx = idx_in_page; idx < slice_end_idx; ++idx)
            {
                KeyT &key = page_->keys_[idx];
                auto &cce = page_->entries_[idx];

                bool is_clean_target = IsCleanTarget(key, cce.get());
                auto [can_be_cleaned, delay_free] = CanBeCleaned(
                    cce.get(), dirty_range_ts, &range_last_sync_ts);

                if (is_clean_target && can_be_cleaned)
                {
                    if (!tried_slice_kick)
                    {
                        slice_kicked = store_slice->Kickout();
                        tried_slice_kick = true;
                    }

                    if (slice_kicked)
                    {
                        // The key cache contains all keys in this range, but
                        // when we delete a key from the range, the update is
                        // delayed until the cce is removed from ccmap. This is
                        // because we always search for key in ccmap first
                        // before trying to query the key cache. Remove the key
                        // from key cache if the key is in deleted status.
                        // In certain special cases like cleaning up cces that
                        // no longer belong to this range, we remove keys from
                        // key cache as long as it is not in unknown status.
                        // This is because cces in unknown status is not added
                        // to the key cache yet.
                        if (txservice_enable_key_cache &&
                            table_name_.IsBase() &&
                            ((remove_from_key_cache &&
                              cce->PayloadStatus() != RecordStatus::Unknown) ||
                             cce->PayloadStatus() == RecordStatus::Deleted))
                        {
                            store_range->DeleteKey(
                                key, cc_shard_->core_id_, store_slice);
                        }

                        MarkClean(cc_ng_id_, idx, delay_free);
                        continue;
                    }
                }

                Reserve(idx, is_clean_target);
            }

            idx_in_page = slice_end_idx;
            kickout_any = kickout_any || slice_kicked;
        }

        return idx_in_page;
    }

    /**
     * @brief Mark a key if it can be cleanned.
     *
     * - Under range partition, OrphanKey is the key whose StoreRange metadata
     * has been kicked out.
     * - Under hash partition, regards every key as OrphanKey.
     */
    void MarkCleanForOrphanKey(uint8_t idx)
    {
        const KeyT &key = page_->keys_[idx];
        std::unique_ptr<
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>> &cce =
            page_->entries_[idx];

        bool is_clean_target = IsCleanTarget(key, cce.get());
        auto [can_be_cleaned, delay_free] = CanBeCleaned(cce.get());

        if (is_clean_target && can_be_cleaned)
        {
            MarkClean(cc_ng_id_, idx, delay_free);
        }
        else
        {
            Reserve(idx, is_clean_target);
        }
    }

    void MarkClean(NodeGroupId cc_ng_id, uint8_t idx, bool delay_free)
    {
        std::unique_ptr<
            CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>> &cce =
            page_->entries_[idx];

        if (cce->PayloadStatus() == RecordStatus::Normal)
        {
            ++clean_obj_cnt_;
            if (AffectCacheCompleteness())
            {
                evicted_valid_key_ = true;
            }
        }
        if (cce->IsDirty())
        {
            ++dirty_freed_cnt_;
        }

        cce->ClearLocks(*cc_shard_, cc_ng_id);
        clean_set_.set(idx, true);
        ++free_cnt_;
    }

protected:
    CcShard *cc_shard_{nullptr};
    NodeGroupId cc_ng_id_{0};
    const TableName &table_name_;
    CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page_{nullptr};
    uint64_t last_commit_ts_{0};
    uint64_t free_cnt_{0};
    uint64_t dirty_freed_cnt_{0};
    bool evicted_valid_key_{false};
    uint64_t clean_obj_cnt_{0};

private:
    std::bitset<CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                    split_threshold_>
        clean_set_;

    friend class LocalCcShards;
};

template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
struct CcPageCleanGuardWithoutKickoutCc
    : public CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>
{
public:
    explicit CcPageCleanGuardWithoutKickoutCc(
        CcShard *cc_shard,
        NodeGroupId cc_ng_id,
        const TableName &table_name_,
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page)
        : CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>(
              cc_shard, cc_ng_id, table_name_, page)
    {
    }

    bool CleanSuccess() const override
    {
        // If we're just doing regular page clean, clean_succecss is always
        // true.
        return true;
    }

private:
    typename CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>::
        CanBeCleanedResult
        CanBeCleaned(
            const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
            const uint64_t *const dirty_range_ts = nullptr,
            const uint64_t *const range_last_sync_ts = nullptr) const override
    {
        if (RangePartitioned && dirty_range_ts &&
            cce->CkptTs() > *range_last_sync_ts &&
            cce->CkptTs() <= *dirty_range_ts)
        {
            // During the split range, if the ckpt ts of cce is greater than
            // the range last sync ts and not greater than the dirty range
            // version ts, it means that cce is only flushed into the new range,
            // but the new range entry has not been installed into the range cc
            // map. At this time, we cannot evict this cce because the read
            // request will still access the old range.
            assert(*dirty_range_ts > 0);
            // LOG(INFO) << "CanBeCleaned: cce: 0x" << std::hex << (void *) cce
            //           << ", dirty range ts: " << std::dec << *dirty_range_ts
            //           << ", range last sync ts: " << *range_last_sync_ts
            //           << ", ckpt ts: " << cce->CkptTs()
            //           << ", is free: " << std::boolalpha << cce->IsFree()
            //           << ", being ckpt: " << std::boolalpha
            //           << cce->GetBeingCkpt() << ", can not be cleaned.";
            return {false, false};
        }

        // if (!cce->IsFree() || cce->GetBeingCkpt())
        // {
        //     LOG(INFO) << "CanBeCleaned: cce: 0x" << std::hex << (void *) cce
        //               << ", is free: " << std::boolalpha << cce->IsFree()
        //               << ", being ckpt: " << std::boolalpha
        //               << cce->GetBeingCkpt() << ", can not be cleaned.";
        // }
        // else if (cce->IsFree() && !cce->GetBeingCkpt())
        // {
        //     LOG(INFO) << "CanBeCleaned: cce: 0x" << std::hex << (void *) cce
        //               << ", is free: " << std::boolalpha << cce->IsFree()
        //               << ", being ckpt: " << std::boolalpha
        //               << cce->GetBeingCkpt() << ", can be cleaned.";
        // }

        return {(cce->IsFree() && !cce->GetBeingCkpt()), false};
    }

    bool IsCleanTarget(
        const KeyT &key,
        const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce)
        const override
    {
        // If we're just doing regular page clean, all cce is specific clean
        // target.
        return true;
    }

    void Reserve(uint8_t idx, bool is_clean_target) override
    {
        assert(is_clean_target);
    }

    bool NeedInvalidateLockTerm() const override
    {
        return false;
    }

    bool AffectCacheCompleteness() const override
    {
        return true;
    }

    // Returns true if the key should be removed from key cache regardless
    // of rec status.
    bool RemoveFromKeyCache() const override
    {
        return false;
    }
};

template <typename KeyT,
          typename ValueT,
          bool VersionedRecord,
          bool RangePartitioned>
struct CcPageCleanGuardWithKickoutCc
    : public CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>
{
public:
    CcPageCleanGuardWithKickoutCc(
        CcShard *cc_shard,
        NodeGroupId cc_ng_id,
        const TableName &table_name,
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page,
        const KickoutCcEntryCc *kickout_cc)
        : CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>(
              cc_shard, cc_ng_id, table_name, page),
          kickout_cc_(kickout_cc),
          need_invalidate_lock_term_(
              DeduceNeedInvalidateLockTerm(kickout_cc->GetCleanType())),
          clean_success_(true)
    {
        assert(kickout_cc);
    }

    bool CleanSuccess() const override
    {
        return clean_success_;
    }

    void Compact() override
    {
        CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>::
            Compact();
        CleanType type = kickout_cc_->GetCleanType();
        if (type == CleanType::CleanRangeData ||
            type == CleanType::CleanRangeDataForMigration ||
            type == CleanType::CleanBucketData)
        {
            // Only in these clean types will we kickout dirty data.
            UpdatePageDirtyCommitTs();
        }
    }

private:
    static bool DeduceNeedInvalidateLockTerm(CleanType type)
    {
        if (type == CleanType::CleanRangeData ||
            type == CleanType::CleanRangeDataForMigration ||
            type == CleanType::CleanBucketData)
        {
            // If the ccentry that expect to clean still has lock on it,
            // it must be that the owner of this lock has failed. The
            // reason is that the lock owner must have acquired
            // range/bucket read lock before accessing data in
            // range/bucket. And if we're doing clean data on the
            // range/bucket, that means the DDL has acquired write lock
            // on this range/bucket on all ngs. So it must be that the
            // data lock owner ng has failed and the read lock has
            // expired. In this case invalidate the lock term so that if
            // the failed node tries to access data with the deleted cce
            // addr, we can reject the request.
            return true;
        }
        else
        {
            return false;
        }
    }

private:
    typename CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>::
        CanBeCleanedResult
        CanBeCleaned(
            const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce,
            const uint64_t *const dirty_range_ts = nullptr,
            const uint64_t *const range_last_sync_ts = nullptr) const override
    {
        // Check if the cce has any locks on it. If so recycle the lock entry
        // before deleting cce.
        bool can_be_cleaned =
            kickout_cc_->CanBeCleaned(cce, VersionedRecord, RangePartitioned);
        bool delay_free = can_be_cleaned && cce->GetKeyLock() &&
                          !cce->GetKeyLock()->IsEmpty();
        return {can_be_cleaned, delay_free};
    }

    bool IsCleanTarget(
        const KeyT &key,
        const CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned> *cce)
        const override
    {
        return kickout_cc_->IsCleanTarget(key, cce, this->cc_shard_);
    }

    void Reserve(uint8_t idx, bool is_clean_target) override
    {
        if (is_clean_target)
        {
            clean_success_ = false;
        }
    }

    bool AffectCacheCompleteness() const override
    {
        return kickout_cc_->GetCleanType() == CleanType::CleanForAlterTable;
    }

    bool NeedInvalidateLockTerm() const override
    {
        return need_invalidate_lock_term_;
    }

    bool RemoveFromKeyCache() const override
    {
        // When kicking data that no longer belongs to this range,
        // we should remove the key regardless of its rec status.
        return kickout_cc_->GetCleanType() == CleanType::CleanRangeData;
    }

    void UpdatePageDirtyCommitTs()
    {
        CcPage<KeyT, ValueT, VersionedRecord, RangePartitioned> *page =
            CcPageCleanGuard<KeyT, ValueT, VersionedRecord, RangePartitioned>::
                page_;

        if (!page->Empty())
        {
            // During range split kickout, we might clean cc entries that
            // are still dirty from page. So the max dirty ts might
            // decrease.
            auto commit_ts_less =
                [](const std::unique_ptr<
                       CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>
                       &left,
                   const std::unique_ptr<
                       CcEntry<KeyT, ValueT, VersionedRecord, RangePartitioned>>
                       &right) { return left->CommitTs() < right->CommitTs(); };

            uint64_t page_max_commit_ts =
                (*std::max_element(page->entries_.begin(),
                                   page->entries_.end(),
                                   commit_ts_less))
                    ->CommitTs();
            page->last_dirty_commit_ts_ =
                std::min(page_max_commit_ts, page->last_dirty_commit_ts_);
        }
        else
        {
            page->last_dirty_commit_ts_ = 0;
        }
    }

private:
    const KickoutCcEntryCc *kickout_cc_;

    bool need_invalidate_lock_term_;

    bool clean_success_;
};

}  // namespace txservice
