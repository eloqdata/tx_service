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

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

// #include "cc_req_misc.h"
#include "data_sync_task.h"
#include "range_bucket_key_record.h"
#include "range_slice.h"
#include "sharder.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_serialize.h"

namespace txservice
{
struct DataSyncTask;
struct FetchRangeSlicesReq;
struct LruEntry;
template <typename KeyT, typename ValueT>
struct CcEntry;

// struct that stores range related info that we read from
// KV storage during table range initialization.
struct InitRangeEntry
{
    InitRangeEntry() : key_(), partition_id_(-1), version_ts_(0)
    {
    }

    InitRangeEntry(const InitRangeEntry &rhs) = delete;
    InitRangeEntry &operator=(const InitRangeEntry &rhs) = delete;

    InitRangeEntry(TxKey start_key, int32_t partition_id, uint64_t version_ts)
        : key_(std::move(start_key)),
          partition_id_(partition_id),
          version_ts_(version_ts)
    {
    }

    InitRangeEntry(InitRangeEntry &&rhs)
        : key_(std::move(rhs.key_)),
          partition_id_(rhs.partition_id_),
          version_ts_(rhs.version_ts_)
    {
    }

    TxKey key_;
    int32_t partition_id_{0};
    uint64_t version_ts_{0};
};

struct RangeInfo
{
public:
    RangeInfo() = delete;
    RangeInfo(uint64_t version_ts, uint32_t partition_id, bool is_dirty = false)
        : partition_id_(partition_id),
          is_dirty_(is_dirty),
          version_ts_(version_ts),
          dirty_ts_(0)
    {
    }

    virtual ~RangeInfo() = default;

    RangeInfo(const RangeInfo &other) = delete;

    // RangeInfo(RangeInfo &&other)
    //     : end_key_(other.end_key_),
    //       partition_id_(other.partition_id_),
    //       version_ts_(other.version_ts_),
    //       new_partition_id_(other.new_partition_id_),
    //       dirty_ts_(other.dirty_ts_),
    //       is_dirty_(other.is_dirty_)
    // {
    //     if (!other.start_key_)
    //     {
    //         start_key_ = nullptr;
    //     }
    //     else
    //     {
    //         start_key_ = other.start_key_->Clone();
    //     }
    //     for (auto &key : other.new_key_)
    //     {
    //         new_key_.push_back(key->Clone());
    //     }
    // }

    // RangeInfo &operator=(const RangeInfo &other)
    // {
    //     if (this != &other)
    //     {
    //         end_key_ = other.end_key_;
    //         partition_id_ = other.partition_id_;
    //         version_ts_ = other.version_ts_;
    //         new_partition_id_ = other.new_partition_id_;
    //         dirty_ts_ = other.dirty_ts_;
    //         is_dirty_ = other.is_dirty_;

    //         if (!other.start_key_)
    //         {
    //             start_key_ = nullptr;
    //         }
    //         else
    //         {
    //             start_key_ = other.start_key_->Clone();
    //         }

    //         new_key_.clear();
    //         for (const auto &key : other.new_key_)
    //         {
    //             new_key_.push_back(key->Clone());
    //         }

    //         assert(new_partition_id_.size() == new_key_.size());
    //     }
    //     return *this;
    // }

    virtual std::unique_ptr<RangeInfo> Clone() const = 0;

    virtual void SetNewRanges(
        const std::vector<std::pair<TxKey, int32_t>> &new_ranges) = 0;

    // void SetDirty(const std::vector<TxKey> &new_key,
    //               const std::vector<int32_t> &new_partition_id,
    //               uint64_t dirty_ts)
    // {
    //     if (dirty_ts >= version_ts_ && dirty_ts >= dirty_ts_)
    //     {
    //         new_key_.clear();
    //         for (const auto &key : new_key)
    //         {
    //             new_key_.emplace_back(key.Clone());
    //         }
    //         new_partition_id_ = new_partition_id;
    //         assert(new_key_.size() == new_partition_id_.size());
    //         dirty_ts_ = dirty_ts;
    //         is_dirty_ = true;
    //     }
    // }

    void SetDirty(std::vector<TxKey> new_key,
                  std::vector<int32_t> new_partition_id,
                  uint64_t dirty_ts)
    {
        if (dirty_ts >= version_ts_ && dirty_ts >= dirty_ts_)
        {
            new_key_ = std::move(new_key);
            new_partition_id_ = std::move(new_partition_id);
            assert(new_key_.size() == new_partition_id_.size());
            dirty_ts_ = dirty_ts;
            is_dirty_ = true;
        }
    }

    void CommitDirty()
    {
        if (dirty_ts_ >= version_ts_)
        {
            version_ts_ = dirty_ts_;
            is_dirty_ = false;
        }
    }

    void ClearDirty(uint64_t commit_ts = 0)
    {
        new_key_.clear();
        new_partition_id_.clear();
        dirty_ts_ = 0;
        if (commit_ts != 0 && commit_ts > version_ts_)
        {
            version_ts_ = commit_ts;
        }
        is_dirty_ = false;
    }

    bool IsDirty() const
    {
        return is_dirty_;
    }

    int32_t GetKeyNewRangeId(const TxKey &key) const
    {
        if (!IsDirty())
        {
            return -1;
        }

        assert(!new_key_.empty());
        // Does not belong to any of the new ranges
        if (key < new_key_.front())
        {
            return -1;
        }

        uint idx = 1;
        for (; idx < new_key_.size(); idx++)
        {
            if (key < new_key_.at(idx))
            {
                break;
            }
        }

        return new_partition_id_.at(idx - 1);
    }

    virtual TxKey StartTxKey() const = 0;
    virtual TxKey EndTxKey() const = 0;

    int32_t PartitionId() const
    {
        return partition_id_;
    }

    uint64_t VersionTs() const
    {
        return version_ts_;
    }

    const std::vector<TxKey> *NewKey() const
    {
        return is_dirty_ ? &new_key_ : nullptr;
    }

    const std::vector<int32_t> *NewPartitionId() const
    {
        return is_dirty_ ? &new_partition_id_ : nullptr;
    }

    const std::vector<int32_t> &NewPartitionIdUncheckDirty() const
    {
        return new_partition_id_;
    }

    uint64_t DirtyTs() const
    {
        return is_dirty_ ? dirty_ts_ : 0;
    }

    virtual size_t MemUsage() const = 0;

protected:
    int32_t partition_id_{0};
    // is_dirty_ means if the new key and partition ids are visible to regular
    // requests. During post commit phase of range split, we have a short period
    // where we need to keep the new partition info but make them invisible to
    // regular range read request.
    bool is_dirty_{false};

    uint64_t version_ts_{1};

    std::vector<int32_t> new_partition_id_;
    std::vector<TxKey> new_key_;
    uint64_t dirty_ts_{0};

    template <typename KeyT>
    friend class RangeCcMap;
    friend struct RangeRecord;
    friend struct TableRangeEntry;
    template <typename KeyT>
    friend struct TemplateTableRangeEntry;
    friend struct SplitFlushRangeOp;
};

template <typename KeyT>
struct TemplateRangeInfo : public RangeInfo
{
public:
    TemplateRangeInfo() = delete;

    TemplateRangeInfo(const KeyT *start_key,
                      uint64_t version_ts,
                      uint32_t partition_id,
                      const KeyT *end_key = nullptr,
                      bool is_dirty = false)
        : RangeInfo(version_ts, partition_id, is_dirty), end_key_(end_key)
    {
        if (start_key == KeyT::NegativeInfinity())
        {
            is_start_neg_inf_ = true;
        }
        else
        {
            start_key_ = KeyT(*start_key);
            is_start_neg_inf_ = false;
        }
    }

    std::unique_ptr<RangeInfo> Clone() const override
    {
        std::unique_ptr<TemplateRangeInfo<KeyT>> that =
            std::make_unique<TemplateRangeInfo<KeyT>>(
                is_start_neg_inf_ ? KeyT::NegativeInfinity() : &start_key_,
                version_ts_,
                partition_id_,
                end_key_,
                is_dirty_);

        for (const auto &key : new_key_)
        {
            that->new_key_.emplace_back(key.Clone());
        }
        that->new_partition_id_ = new_partition_id_;
        that->dirty_ts_ = dirty_ts_;
        assert(that->new_partition_id_.size() == that->new_key_.size());

        return that;
    }

    void SetEndKey(const KeyT *end_key)
    {
        end_key_ = end_key;
    }

    TxKey StartTxKey() const override
    {
        return TxKey(StartKey());
    }

    TxKey EndTxKey() const override
    {
        return TxKey(EndKey());
    }

    const KeyT *StartKey() const
    {
        return is_start_neg_inf_ ? KeyT::NegativeInfinity() : &start_key_;
    }

    const KeyT *EndKey() const
    {
        return end_key_;
    }

    size_t MemUsage() const override
    {
        size_t mem_usage = sizeof(TemplateRangeInfo<KeyT>);
        for (const auto &key : new_key_)
        {
            mem_usage += key.MemUsage();
        }
        mem_usage += sizeof(int32_t) * new_partition_id_.size();
        return mem_usage;
    }

    void SetNewRanges(
        const std::vector<std::pair<TxKey, int32_t>> &new_ranges) override
    {
        new_partition_id_.clear();
        new_partition_id_.reserve(new_ranges.size());
        new_key_.clear();
        new_key_.reserve(new_ranges.size());

        for (const auto &[range_start, range_id] : new_ranges)
        {
            assert(range_start.Type() == KeyType::Normal);
            new_key_.emplace_back(
                std::make_unique<KeyT>(*range_start.template GetKey<KeyT>()));
            new_partition_id_.emplace_back(range_id);
        }
    }

private:
    // For the first range starting from negative infinity, the field start_key_
    // is meaningless. For the last range ending with positive infinity, the end
    // key points to KeyT::PositiveInfinity();
    KeyT start_key_;
    bool is_start_neg_inf_{false};
    const KeyT *end_key_{nullptr};
};

struct TableRangeEntry
{
public:
    using uptr = std::unique_ptr<TableRangeEntry>;

    TableRangeEntry() = default;
    TableRangeEntry(const TableRangeEntry &) = delete;
    TableRangeEntry &operator=(const TableRangeEntry &) = delete;

    TableRangeEntry(uint64_t version_ts, int64_t partition_id)
        : mux_(), fetch_range_slices_req_(nullptr)
    {
    }

    virtual ~TableRangeEntry();

    virtual const RangeInfo *GetRangeInfo() const = 0;

    virtual uint64_t Version() const = 0;

    virtual uint64_t DirtyVersion() const = 0;

    StoreRange *PinStoreRange()
    {
        std::shared_lock<std::shared_mutex> lk(mux_);
        StoreRange *store_range = RangeSlices();
        if (store_range != nullptr)
        {
            store_range->pins_.fetch_add(1, std::memory_order_release);
            return store_range;
        }
        return nullptr;
    }

    void UnPinStoreRange()
    {
        std::shared_lock<std::shared_mutex> lk(mux_);
        StoreRange *store_range = RangeSlices();
        if (store_range != nullptr)
        {
            store_range->pins_.fetch_sub(1, std::memory_order_release);
        }
    }

    virtual void SetRangeEndTxKey(TxKey end_tx_key) = 0;

    virtual void SetVersion(uint64_t version) = 0;

    virtual void InitRangeSlices(std::vector<SliceInitInfo> &&slices,
                                 NodeGroupId ng_id,
                                 bool init_key_cache,
                                 bool empty_range = false,
                                 size_t estimate_rec_size = UINT64_MAX,
                                 bool has_dml_since_ddl = true) = 0;

    /**
     * @brief Check whether the store range is free or not.
     * NOTE: Holding the table range entry unique lock during invoke this
     * function.
     */
    virtual bool IsStoreRangeFree(bool sync_info = false) = 0;
    /**
     * @brief Drop store range. Be sure the store range is free, and holding the
     * table range entry unique lock during invoke this function.
     */
    virtual void DropStoreRange() = 0;

    uint64_t GetLastSyncTs()
    {
        std::shared_lock<std::shared_mutex> lk(mux_);
        return last_sync_ts_;
    }

    void UpdateLastDataSyncTS(uint64_t last_sync_ts)
    {
        std::unique_lock<std::shared_mutex> lk(mux_);

        if (last_sync_ts > last_sync_ts_)
        {
            // data sync succeeded, update last sync ts
            last_sync_ts_ = last_sync_ts;
        }
    }

    void FetchRangeSlices(const TableName &range_tbl_name,
                          CcRequestBase *requester,
                          NodeGroupId ng_id,
                          int64_t ng_term,
                          CcShard *cc_shard);

    virtual StoreRange *RangeSlices() = 0;
    virtual TxKey RangeStartTxKey() = 0;

protected:
    uint64_t last_sync_ts_{0};

    // Protects range_slices_, fetch_range_slices_cc_
    // Any update on these pointers requres unique lock on mux. But updating
    // the object that these pointers point to only requires shared lock.
    std::shared_mutex mux_;

    std::unique_ptr<FetchRangeSlicesReq> fetch_range_slices_req_{nullptr};

    template <typename KeyT>
    friend class RangeCcMap;
    friend class LocalCcShards;
    friend struct FetchRangeSlicesReq;
};

template <typename KeyT>
struct TemplateTableRangeEntry : public TableRangeEntry
{
public:
    TemplateTableRangeEntry(
        const KeyT *start_key,
        uint64_t version_ts,
        int64_t partition_id,
        std::unique_ptr<TemplateStoreRange<KeyT>> slices = nullptr)
        : range_info_(start_key, version_ts, partition_id),
          range_slices_(std::move(slices))
    {
    }

    void UpdateRangeEntry(uint64_t version_ts,
                          std::unique_ptr<TemplateStoreRange<KeyT>> slices)
    {
        range_info_.version_ts_ = version_ts;
        std::lock_guard<std::shared_mutex> lk(mux_);
        range_slices_ = std::move(slices);
    }

    void SetRangeEndTxKey(TxKey end_tx_key) override
    {
        const KeyT *typed_end = end_tx_key.template GetKey<KeyT>();
        SetRangeEndKey(typed_end);
    }

    void SetRangeEndKey(const KeyT *end_key)
    {
        range_info_.SetEndKey(end_key);
        if (range_slices_)
        {
            range_slices_->SetRangeEndKey(end_key);
        }
    }

    StoreRange *RangeSlices() override
    {
        return range_slices_.get();
    }

    TemplateStoreRange<KeyT> *TypedStoreRange() const
    {
        return range_slices_.get();
    }

    void InitRangeSlices(std::vector<SliceInitInfo> &&slices,
                         NodeGroupId ng_id,
                         bool init_key_cache,
                         bool empty_range = false,
                         size_t estimate_rec_size = UINT64_MAX,
                         bool has_dml_since_ddl = true) override
    {
        std::unique_ptr<TemplateStoreRange<KeyT>> range_slices =
            std::make_unique<TemplateStoreRange<KeyT>>(
                range_info_.StartKey(),
                range_info_.EndKey(),
                range_info_.PartitionId(),
                ng_id,
                *Sharder::Instance().GetLocalCcShards(),
                init_key_cache,
                empty_range,
                estimate_rec_size,
                has_dml_since_ddl);

        if (!empty_range)
        {
            range_slices->InitSlices(std::move(slices));
        }
        range_slices_ = std::move(range_slices);
    }

    /**
     * @brief Check whether the store range is free or not.
     * NOTE: Holding the table range entry unique lock during invoke this
     * function.
     */
    bool IsStoreRangeFree(bool sync_info = false) override
    {
        if (!sync_info)
        {
            return !range_slices_ || range_slices_->Pins() == 0;
        }

        // If the sync_info flag is true, it means this function is called
        // during bucket migration and we're cleaning up range slices that are
        // migrated away. In this case we should make sure that no range slices
        // in this bucket is loaded into memory after this function returns
        // true. So we need to wait til the current fetch req is finished.
        return range_slices_ != nullptr ? range_slices_->Pins() == 0
                                        : fetch_range_slices_req_ == nullptr;
    }

    /**
     * @brief Drop store range. Be sure the store range is free, and holding the
     * table range entry unique lock during invoke this function.
     *
     */
    void DropStoreRange() override
    {
        // We need to make sure that there's no one accesing StoreRange before
        // dropping store range.
        if (range_slices_)
        {
            assert(range_slices_->Pins() == 0);
            range_slices_ = nullptr;
        }
    }

    const RangeInfo *GetRangeInfo() const override
    {
        return &range_info_;
    }

    const TemplateRangeInfo<KeyT> *TypedRangeInfo() const
    {
        return &range_info_;
    }

    TemplateRangeInfo<KeyT> *TypedRangeInfo()
    {
        return &range_info_;
    }

    /**
     * @brief Set new table range info in range_info_.
     */
    void UploadNewRangeInfo(std::vector<TxKey> new_key,
                            std::vector<int32_t> new_partition_id,
                            uint64_t commit_ts)
    {
        assert(commit_ts >= range_info_.DirtyTs());

        range_info_.SetDirty(
            std::move(new_key), std::move(new_partition_id), commit_ts);
    }

    uint64_t Version() const override
    {
        return range_info_.VersionTs();
    }

    uint64_t DirtyVersion() const override
    {
        return range_info_.DirtyTs();
    }

    void SetVersion(uint64_t version) override
    {
        range_info_.version_ts_ = version;
    }

    const KeyT *RangeStartKey() const
    {
        return range_info_.StartKey();
    }

    TxKey RangeStartTxKey() override
    {
        return TxKey(RangeStartKey());
    }

    bool UploadDirtyRangeSlices(int32_t new_partition_id,
                                uint64_t dirty_ts,
                                bool has_dml_since_ddl,
                                std::vector<SliceInitInfo> &&new_slices)
    {
        std::lock_guard<std::shared_mutex> entry_lk(mux_);

        if (range_info_.IsDirty() && dirty_ts == range_info_.DirtyTs())
        {
            if (dirty_range_slices_ == nullptr)
            {
                dirty_range_slices_ = std::make_unique<std::tuple<
                    bool,
                    bool,
                    std::unordered_map<int32_t, std::vector<SliceInitInfo>>>>();
                std::get<0>(*dirty_range_slices_) = true;
                std::get<1>(*dirty_range_slices_) = has_dml_since_ddl;
            }
            DLOG(INFO) << "Received new range slices info, range_id:"
                       << range_info_.PartitionId()
                       << ", new_range_id:" << new_partition_id;
            auto ins_pair =
                std::get<2>(*dirty_range_slices_).try_emplace(new_partition_id);
            // overwrite old slice specs with the new one. If a range split
            // fails during create sk, we might get different sk slice specs on
            // recover.
            ins_pair.first->second = std::move(new_slices);
            return true;
        }
        else
        {
            return false;
        }
    }

    void UpdateDirtyRangeSlice(int32_t new_partition_id,
                               const std::vector<uint32_t> &slice_idxs,
                               uint64_t dirty_version,
                               SliceStatus status)
    {
        assert(status == SliceStatus::FullyCached);
        std::shared_lock<std::shared_mutex> entry_lk(mux_);
        if (range_info_.IsDirty() && DirtyVersion() == dirty_version &&
            dirty_range_slices_ != nullptr)
        {
            assert(range_info_.IsDirty());
            if (!std::get<0>(*dirty_range_slices_))
            {
                // this range data has been kicked, don't update slices
                // status.
                DLOG(INFO) << "Skip update dirty range slice status for range "
                              "keys was ever kicked.";

                return;
            }

            assert(new_partition_id >= 0);

            std::unordered_map<int32_t, std::vector<SliceInitInfo>>
                &range_slices = std::get<2>(*dirty_range_slices_);
            assert(range_slices.find(new_partition_id) != range_slices.end());

            std::vector<SliceInitInfo> &slices =
                range_slices.at(new_partition_id);

            for (size_t i = 0; i < slice_idxs.size(); i++)
            {
                slices[slice_idxs[i]].status_ = status;
            }
            return;
        }
        else
        {
            LOG(WARNING) << "UpdateDirtyRangeSlice: dirty range#"
                         << new_partition_id << " has been installed.";
        }
    }

    bool AcceptsDirtyRangeData(uint64_t dirty_version)
    {
        std::shared_lock<std::shared_mutex> entry_lk(mux_);
        if (dirty_range_slices_ != nullptr && DirtyVersion() == dirty_version)
        {
            return std::get<0>(*dirty_range_slices_);
        }
        return false;
    }

    void SetAcceptsDirtyRangeData(bool accept)
    {
        std::lock_guard<std::shared_mutex> entry_lk(mux_);

        if (dirty_range_slices_ != nullptr)
        {
            std::get<0>(*dirty_range_slices_) = accept;
        }
    }

    std::unique_ptr<
        std::tuple<bool,
                   bool,
                   std::unordered_map<int32_t, std::vector<SliceInitInfo>>>>
    ReleaseDirtyRangeSlices()
    {
        std::lock_guard<std::shared_mutex> entry_lk(mux_);
        if (dirty_range_slices_ != nullptr &&
            !std::get<0>(*dirty_range_slices_))
        {
            auto &map_ref = std::get<2>(*dirty_range_slices_);
            for (auto it = map_ref.begin(); it != map_ref.end(); it++)
            {
                auto &slices = it->second;
                for (auto &slice : slices)
                {
                    slice.status_ = SliceStatus::PartiallyCached;
                }
            }
        }

        return std::move(dirty_range_slices_);
    }

private:
    TemplateRangeInfo<KeyT> range_info_;

    // range_slices_ stores the slice info in this range. This is only
    // initialized on the node group that owns this range, and it is initialized
    // lazily when needed. range_slices_ is only safe to accessed in the
    // following cases:
    // 1. StoreRange is pinned.
    // 2. mutex lock is acquried on TableRangeEntry.mux_.
    std::unique_ptr<TemplateStoreRange<KeyT>> range_slices_{nullptr};

    // (accept_data, has_dml_since_ddl, {new_range_id->[slices_info,...], ...})
    std::unique_ptr<
        std::tuple<bool,
                   bool,
                   std::unordered_map<int32_t, std::vector<SliceInitInfo>>>>
        dirty_range_slices_{nullptr};
};

struct RangeRecord : public TxRecord
{
public:
    RangeRecord()
        : range_info_{nullptr},
          is_info_owner_(false),
          range_owner_rec_(nullptr),
          new_range_owner_rec_(nullptr),
          is_read_result_(false)
    {
    }

    RangeRecord(const RangeRecord &rhs)
        : range_info_(nullptr),
          is_info_owner_(rhs.is_info_owner_),
          is_read_result_(rhs.is_read_result_)
    {
        if (rhs.is_info_owner_)
        {
            assert(range_info_uptr_ == nullptr);
            range_info_uptr_ = rhs.range_info_uptr_->Clone();
        }
        else
        {
            range_info_ = rhs.range_info_;
        }
        if (rhs.is_read_result_)
        {
            range_owner_bucket_ = rhs.range_owner_bucket_;
            if (rhs.new_range_owner_bucket_)
            {
                new_range_owner_bucket_ =
                    std::make_unique<std::vector<const BucketInfo *>>();
                for (auto &info : *rhs.new_range_owner_bucket_)
                {
                    new_range_owner_bucket_->push_back(info);
                }
            }
            else
            {
                new_range_owner_bucket_ = nullptr;
            }
        }
        else
        {
            // Copy new_range_owner_rec_
            range_owner_rec_ = rhs.range_owner_rec_;
            if (rhs.new_range_owner_rec_)
            {
                new_range_owner_rec_ =
                    std::make_unique<std::vector<LruEntry *>>();
                for (auto &entry : *rhs.new_range_owner_rec_)
                {
                    new_range_owner_rec_->push_back(entry);
                }
            }
            else
            {
                new_range_owner_rec_ = nullptr;
            }
        }
    }

    RangeRecord(const RangeInfo *info, LruEntry *range_owner)
        : range_info_(info),
          is_info_owner_(false),
          range_owner_rec_(range_owner),
          new_range_owner_rec_(nullptr),
          is_read_result_(false)
    {
    }

    RangeRecord(std::unique_ptr<RangeInfo> info, LruEntry *range_owner)
        : range_info_uptr_(std::move(info)),
          is_info_owner_(true),
          range_owner_rec_(range_owner),
          new_range_owner_rec_(nullptr),
          is_read_result_(false)
    {
    }

    ~RangeRecord()
    {
        if (is_info_owner_ && range_info_uptr_)
        {
            range_info_uptr_.reset();
        }
        if (is_read_result_ && new_range_owner_bucket_)
        {
            new_range_owner_bucket_.reset();
        }
        else if (!is_read_result_ && new_range_owner_rec_)
        {
            new_range_owner_rec_.reset();
        }
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        assert(false);
    }

    void Serialize(std::string &str) const override
    {
        assert(!is_read_result_);
        // handle neg inf key
        TxKey start_tx_key = range_info_->StartTxKey();
        bool is_normal = start_tx_key.Type() == KeyType::Normal;
        SerializeToStr(&is_normal, str);
        if (is_normal)
        {
            start_tx_key.Serialize(str);
        }
        SerializeToStr(&range_info_->partition_id_, str);
        SerializeToStr(&range_info_->version_ts_, str);

        // serialize dirty range info
        uint16_t new_part_size = range_info_->new_key_.size();
        SerializeToStr(&new_part_size, str);
        for (const TxKey &new_key : range_info_->new_key_)
        {
            new_key.Serialize(str);
        }
        for (const int32_t &new_id : range_info_->new_partition_id_)
        {
            SerializeToStr(&new_id, str);
        }
        SerializeToStr(&range_info_->dirty_ts_, str);
        // we do not care about end key
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
        // This should not be called, use DeserializeRangeRecord
        // in range_cc_map. We cannot create KeyT object since
        // it is not passed into RangeRecord.
        assert(false);
    }

    size_t SerializedLength() const override
    {
        assert(!is_read_result_);
        size_t size = 0;
        size += sizeof(bool);
        TxKey start_tx_key = range_info_->StartTxKey();
        if (start_tx_key.Type() == KeyType::Normal)
        {
            size += start_tx_key.SerializedLength();
        }
        // version_ts, dirty_ts, partition_id, new_key_cnt
        size += (2 * sizeof(uint64_t) + sizeof(int32_t) + sizeof(uint16_t));
        for (const TxKey &new_key : range_info_->new_key_)
        {
            size += new_key.SerializedLength();
        }
        size += (sizeof(int32_t) * range_info_->new_partition_id_.size());
        return size;
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<RangeRecord>(*this);
    }

    void Copy(const TxRecord &rhs) override
    {
        auto &rhs_range_record = static_cast<const RangeRecord &>(rhs);
        *this = rhs_range_record;
    }

    std::string ToString() const override
    {
        return "";
    }

    RangeRecord &operator=(const RangeRecord &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        // Release RangeInfo ownership
        if (is_info_owner_)
        {
            range_info_uptr_.reset();
            is_info_owner_ = false;
        }
        else
        {
            range_info_ = nullptr;
        }

        assert(is_info_owner_ == false);

        if (rhs.is_info_owner_)
        {
            range_info_uptr_ = rhs.range_info_uptr_->Clone();
        }
        else
        {
            range_info_ = rhs.range_info_;
        }

        is_info_owner_ = rhs.is_info_owner_;

        // Free own unique ptr.
        if (!is_read_result_ && new_range_owner_rec_)
        {
            new_range_owner_rec_.reset();
        }
        else if (is_read_result_ && new_range_owner_bucket_)
        {
            new_range_owner_bucket_.reset();
        }
        is_read_result_ = rhs.is_read_result_;

        if (rhs.is_read_result_)
        {
            range_owner_bucket_ = rhs.range_owner_bucket_;
            if (rhs.new_range_owner_bucket_)
            {
                new_range_owner_bucket_ =
                    std::make_unique<std::vector<const BucketInfo *>>();
                for (auto &bucket : *rhs.new_range_owner_bucket_)
                {
                    new_range_owner_bucket_->push_back(bucket);
                }
            }
            else
            {
                new_range_owner_bucket_ = nullptr;
            }
        }
        else
        {
            range_owner_rec_ = rhs.range_owner_rec_;
            if (rhs.new_range_owner_rec_)
            {
                new_range_owner_rec_ =
                    std::make_unique<std::vector<LruEntry *>>();
                for (auto &entry : *rhs.new_range_owner_rec_)
                {
                    new_range_owner_rec_->push_back(entry);
                }
            }
            else
            {
                new_range_owner_rec_ = nullptr;
            }
        }
        return *this;
    }

    const RangeInfo *GetRangeInfo() const
    {
        return is_info_owner_ ? range_info_uptr_.get() : range_info_;
    }

    void SetRangeInfo(std::unique_ptr<RangeInfo> range_info)
    {
        if (!is_info_owner_)
        {
            range_info_ = nullptr;
            is_info_owner_ = true;
        }
        range_info_uptr_ = std::move(range_info);
    }

    void SetRangeInfo(const RangeInfo *range_info)
    {
        if (is_info_owner_)
        {
            range_info_uptr_.reset();
            is_info_owner_ = false;
        }
        range_info_ = range_info;
    }

    void CopyForReadResult(const RangeRecord &other);

    /**
     * @brief Get range owner node group. Should only be called if
     * this record is from ReadKeyResult returned by ReadCc.
     */
    const BucketInfo *GetRangeOwnerNg() const
    {
        assert(is_read_result_);
        return range_owner_bucket_;
    }

    /**
     * @brief Get splitting range owner node group. Should only be called if
     * this record is from ReadKeyResult returned by ReadCc.
     */
    const std::vector<const BucketInfo *> *GetNewRangeOwnerNgs() const
    {
        assert(is_read_result_);
        return new_range_owner_bucket_.get();
    }

    void SetNewRangeOwnerRec(
        std::unique_ptr<std::vector<LruEntry *>> new_range_rec)
    {
        if (new_range_owner_rec_)
        {
            new_range_owner_rec_.reset();
        }
        new_range_owner_rec_ = std::move(new_range_rec);
    }

    size_t Size() const override
    {
        return 5 * 8 + 2;
    }

    size_t MemUsage() const override
    {
        size_t mem_usage = sizeof(RangeRecord);
        if (is_info_owner_)
        {
            mem_usage += range_info_uptr_->MemUsage();
        }
        return mem_usage;
    }

    bool NeedsDefrag(mi_heap_t *heap) override
    {
        assert(!is_info_owner_);
        assert(!is_read_result_);
        return false;
    }

    // Usually range_info_ is a raw pointer that points to range info in
    // TableRangeEntry stored in local cc shards. But when it is used in
    // post write all to pass the value to cc request, it will be the owner
    // of a temp range info object.
    union
    {
        const RangeInfo *range_info_;
        std::unique_ptr<RangeInfo> range_info_uptr_;
    };
    bool is_info_owner_{false};

    /**
     * @brief The bucket record that owns this range.
     */
    union
    {
        // We use range_owner_rec_ to store pointer to range_bucket_ccm in
        // range cc map cc entries. But the range bucket cc entry should not
        // be exposed when we copy range record into ReadKeyResult. In that case
        // we will only copy the BucketInfo pointer owned by local cc shards.
        LruEntry *range_owner_rec_{nullptr};
        const BucketInfo *range_owner_bucket_;
    };

    /**
     * @brief The bucket record for new splitted ranges. This is only used
     * during range split, and reset back to nullptr once range split is done.
     */
    union
    {
        std::unique_ptr<std::vector<LruEntry *>> new_range_owner_rec_{nullptr};
        std::unique_ptr<std::vector<const BucketInfo *>>
            new_range_owner_bucket_;
    };

    // If this is record copied into ReadKeyResult during read result.
    bool is_read_result_{false};
};
}  // namespace txservice
