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
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "cc_req_misc.h"
#include "cuckoofilter/cuckoofilter.h"
#include "error_messages.h"
#include "fault_inject.h"
#include "range_slice_type.h"
#include "sharder.h"
#include "tx_key.h"
#include "type.h"

namespace txservice
{
class CcShard;
class CcMap;
struct FillStoreSliceCc;
class StoreRange;
class StoreSlice;
template <typename KeyT>
class TemplateStoreRange;
class LocalCcShards;
struct KVCatalogInfo;
struct CcRequestBase;
struct TableSchema;
struct FlushRecord;

// whether use key cache to skip kv read.
inline bool txservice_enable_key_cache = false;

namespace store
{
class DataStoreHandler;
}

enum struct RangeSliceOpStatus
{
    /**
     * The slice is fully cached in memory and has been pinned.
     */
    Successful = 0,
    /**
     * @brief The demanding cc request either performs an async data store read
     * or awaits for an on-the-fly data store read. The cc request will be
     * re-enqueued for execution once the async read finishes and fills the
     * slice into memory (though filling may fail due to out of memory).
     *
     */
    BlockedOnLoad,
    /**
     * @brief The request is temporarily pushed back due to concurrent
     * modifications of the slice. The request is re-enqueued and re-tries
     * immediately.
     *
     */
    Retry,
    /**
     * @brief The cc request demands a slice that was loaded shortly (less than
     * 4 seconds). Given that the demanding slice becomes partially cached in
     * such a short period, the cache is likely experiencing thrashing. To
     * mitigate the issue, loading is not performed and the request is pushed
     * back for retry. If the range in which the slice resides is experiencing
     * splitting, the request is aborted on the OOM error.
     *
     */
    Delay,
    /**
     * @brief The node group is not the owner of the slice. This should only
     * happen if ng failover after a range split just finished  but before
     * checkpointer is able to truncate the log. In this case the log records of
     * the data that now falls on another ng will still be replayed on the old
     * ng on recover.
     */
    NotOwner,
    /**
     * @brief The slice is not fully cached but after querying the key cache,
     * the key is not found in the slice.
     */
    KeyNotExists,
    /**
     * @brief Only will be used if skip_load_on_miss is true. This means the
     * slice is partially cached.
     */
    NotPinned,
    Error,
};

struct SliceChangeInfo
{
    SliceChangeInfo() = default;
    SliceChangeInfo(const SliceChangeInfo &rhs) = delete;
    SliceChangeInfo &operator=(const SliceChangeInfo &rhs) = delete;

    SliceChangeInfo(TxKey key, uint32_t cur_size, uint32_t post_update_size)
        : key_(std::move(key)),
          cur_size_(cur_size),
          post_update_size_(post_update_size)
    {
    }

    SliceChangeInfo &operator=(SliceChangeInfo &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        key_ = std::move(rhs.key_);
        cur_size_ = rhs.cur_size_;
        post_update_size_ = rhs.post_update_size_;
        return *this;
    }

    SliceChangeInfo(SliceChangeInfo &&rhs)
        : key_(std::move(rhs.key_)),
          cur_size_(rhs.cur_size_),
          post_update_size_(rhs.post_update_size_)
    {
    }

    void SetKey(TxKey key)
    {
        key_ = std::move(key);
    }

    ~SliceChangeInfo() = default;

    TxKey key_;
    uint32_t cur_size_{0};
    uint32_t post_update_size_{0};
};

struct SplitRangeInfo
{
    SplitRangeInfo() = delete;

    SplitRangeInfo(TxKey start_key,
                   int32_t partition_id,
                   std::vector<const StoreSlice *> slices)
        : start_key_(std::move(start_key)),
          partition_id_(partition_id),
          slices_(std::move(slices))
    {
    }

    SplitRangeInfo(SplitRangeInfo &&rhs)
        : start_key_(std::move(rhs.start_key_)),
          partition_id_(rhs.partition_id_),
          slices_(std::move(rhs.slices_))
    {
    }

    SplitRangeInfo(const SplitRangeInfo &rhs)
    {
        start_key_ = rhs.start_key_.GetShallowCopy();
        partition_id_ = rhs.partition_id_;
        slices_ = rhs.slices_;
    }

    TxKey start_key_;
    int32_t partition_id_;
    std::vector<const StoreSlice *> slices_;
};

/**
 * @brief A wrapper that wraps an internal pointer to a range slice. It is
 * passed to external users, e.g., tx state machines, who later use it to
 * re-access the slice, e.g., unpin the slice in memory.
 *
 */
struct RangeSliceId
{
public:
    RangeSliceId() : range_ptr_(nullptr), slice_ptr_(nullptr)
    {
    }

    explicit RangeSliceId(StoreRange *range, StoreSlice *slice)
        : range_ptr_(range), slice_ptr_(slice)
    {
    }

    RangeSliceId(const RangeSliceId &rhs)
        : range_ptr_(rhs.range_ptr_), slice_ptr_(rhs.slice_ptr_)
    {
    }

    RangeSliceId(RangeSliceId &&rhs)
        : range_ptr_(rhs.range_ptr_), slice_ptr_(rhs.slice_ptr_)
    {
    }

    RangeSliceId &operator=(const RangeSliceId &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        range_ptr_ = rhs.range_ptr_;
        slice_ptr_ = rhs.slice_ptr_;

        return *this;
    }

    void Unpin();

    StoreRange *Range()
    {
        return range_ptr_;
    }

    const StoreRange *Range() const
    {
        return range_ptr_;
    }

    StoreSlice *Slice()
    {
        return slice_ptr_;
    }

    const StoreSlice *Slice() const
    {
        return slice_ptr_;
    }

    TxKey RangeStartTxKey() const;
    TxKey RangeEndTxKey() const;

    void Reset()
    {
        range_ptr_ = nullptr;
        slice_ptr_ = nullptr;
    }

    friend bool operator==(const RangeSliceId &lhs, const RangeSliceId &rhs)
    {
        if (&lhs != &rhs)
        {
            return lhs.range_ptr_ == rhs.range_ptr_ &&
                   lhs.slice_ptr_ == rhs.slice_ptr_;
        }
        else
        {
            return true;
        }
    }

    friend bool operator!=(const RangeSliceId &lhs, const RangeSliceId &rhs)
    {
        return !(lhs == rhs);
    }

private:
    StoreRange *range_ptr_;
    StoreSlice *slice_ptr_;

    friend class StoreRange;
};

class StoreSlice
{
public:
    using uptr = std::unique_ptr<StoreSlice>;

    /**
     * @brief The size of a slice.
     *
     */
    static constexpr uint32_t slice_upper_bound = 16 * 1024;

    StoreSlice(size_t size,
               SliceStatus status,
               bool init_key_cache,
               bool empty_slice)
        : size_(size),
          status_(status),
          fetch_slice_cc_(nullptr),
          cache_validity_((txservice_enable_key_cache && init_key_cache)
                              ? Sharder::Instance().GetLocalCcShardsCount()
                              : 0)
    {
        if (empty_slice && !cache_validity_.empty())
        {
            // If slice is empty, set the key cache as valid at the start.
            for (uint16_t i = 0;
                 i < Sharder::Instance().GetLocalCcShardsCount();
                 i++)
            {
                SetKeyCacheValidity(i, true);
            }
        }
    }

    virtual ~StoreSlice();

    StoreSlice(const StoreSlice &) = delete;

    void StartLoading(FillStoreSliceCc *fill_req, LocalCcShards &cc_shards);
    void CommitLoading(StoreRange &range, uint32_t slice_size);

    virtual TxKey StartTxKey() const = 0;
    virtual TxKey EndTxKey() const = 0;

    FillStoreSliceCc *FillCcRequest();

    void SetLoadingError(StoreRange &range, CcErrorCode err_code);

    bool NeedSplitOrMerge() const
    {
        return size_ > StoreSlice::slice_upper_bound ||
               size_ < (StoreSlice::slice_upper_bound >> 2);
    }

    /**
     * @brief Marks the slice to be incomplete in memory. The method is called
     * when a cached key falling in the slice is to be kicked out from memory.
     *
     * @return true, if the slice is allowed to be kicked out and the slice
     * status is marked as PartiallyCached.
     * @return false, if the slice is pinned or being loaded, and thus the
     * caller cannot kickout any keys in the slice.
     */
    bool Kickout()
    {
        std::unique_lock<std::mutex> lk(slice_mux_);
        if (pins_ > 0 || status_ == SliceStatus::BeingLoaded)
        {
            return false;
        }
        else
        {
            status_ = SliceStatus::PartiallyCached;
            return true;
        }
    }

    uint64_t Size() const
    {
        return size_;
    }

    void UpdateSize(uint64_t slice_size)
    {
        size_ = slice_size;
    }

    bool UpdateSize()
    {
        if (post_ckpt_size_ != UINT64_MAX)
        {
            size_ = post_ckpt_size_;
            post_ckpt_size_ = UINT64_MAX;
            return true;
        }
        return false;
    }

    uint64_t PostCkptSize() const
    {
        return post_ckpt_size_;
    }

    void SetPostCkptSize(uint64_t size)
    {
        post_ckpt_size_ = size;
    }

    uint16_t PinCount()
    {
        std::unique_lock<std::mutex> lk(slice_mux_);
        return pins_;
    }

    bool ChangeAllowed()
    {
        std::unique_lock<std::mutex> lk(slice_mux_);
        return pins_ == 0 && status_ != SliceStatus::BeingLoaded;
    }

    size_t MemUsage() const
    {
        // The start and end key pointers are stored in the templated subclass.
        return sizeof(StoreSlice) + 16;
    }

    void UpdateLastLoadTs(uint64_t load_ts)
    {
        last_load_ts_ = load_ts;
    }

    bool IsValidInKeyCache(uint16_t core_id) const
    {
        assert(!cache_validity_.empty());
        return cache_validity_[core_id] & 1;
    }

    void SetKeyCacheValidity(uint16_t core_id, bool valid)
    {
        assert(!cache_validity_.empty());
        if (valid)
        {
            cache_validity_[core_id] |= 1;
        }
        else
        {
            cache_validity_[core_id] &= ~(1);
        }
    }

    void SetLoadingKeyCache(uint16_t core_id, bool status)
    {
        assert(!cache_validity_.empty());
        if (status)
        {
            cache_validity_[core_id] |= (1 << 1);
        }
        else
        {
            cache_validity_[core_id] &= ~(1 << 1);
        }
    }

    bool IsLoadingKeyCache(uint16_t core_id)
    {
        assert(!cache_validity_.empty());
        return cache_validity_[core_id] & (1 << 1);
    }

    void InitKeyCache(CcShard *cc_shard,
                      StoreRange *range,
                      const TableName *tbl_name,
                      NodeGroupId ng_id,
                      int64_t term);

    InitKeyCacheCc *InitKeyCacheRequest()
    {
        return init_key_cache_cc_;
    }

protected:
    bool IsRecentLoad() const;

    uint64_t size_{0};
    // Use UINT64_MAX to indicate invalid post checkpoint slice size
    uint64_t post_ckpt_size_{UINT64_MAX};

    SliceStatus status_{SliceStatus::PartiallyCached};

    /**
     * @brief The number of times a range slice has been pinned by online
     * tx's. A pin is put by a tx when it intends to read/scan the slice and
     * prevents the cache replacement algorithm from kicking out any keys in
     * the slice.
     *
     */
    uint16_t pins_{0};
    bool to_alter_{false};

    FillStoreSliceCc *fetch_slice_cc_{nullptr};
    InitKeyCacheCc *init_key_cache_cc_{nullptr};

    /**
     * @brief A queue of cc requests waiting for the slice to be loaded into
     * memory.
     *
     */
    std::vector<std::pair<CcRequestBase *, CcShard *>> cc_queue_;

    /**
     * @brief The timestamp when the slice was last loaded into memory. The last
     * load may fail, due to failures in the data store. When failures happen,
     * the timestamp is still updated and the slice's status is set to an error
     * state.
     *
     */
    uint64_t last_load_ts_{1};

    std::mutex slice_mux_;

    // If this slice is included in the range key filter. Each core should only
    // access its own bitset, so we do not need mutex protection.
    // Note that byte is the smallest unit c++ sync across threads. To avoid
    // data corruption we need at least 1 byte for each core mask.
    // The first bit implies if the key cache is valid on this core, the second
    // bit implies if the key cache is being loaded on this core.
    std::vector<uint8_t> cache_validity_;

    friend class StoreRange;
    template <typename KeyT>
    friend class TemplateStoreRange;
    friend struct InitKeyCacheCc;
};

template <typename KeyT>
class TemplateStoreSlice : public StoreSlice
{
public:
    TemplateStoreSlice(const KeyT *start,
                       const KeyT *end,
                       size_t size = 0,
                       SliceStatus status = SliceStatus::PartiallyCached,
                       bool init_key_cache = false,
                       bool empty_slice = false)
        : StoreSlice(size, status, init_key_cache, empty_slice),
          start_key_(start),
          end_key_(end)
    {
        assert(start != nullptr && end != nullptr);
    }

    TxKey StartTxKey() const override
    {
        return TxKey(start_key_);
    }

    TxKey EndTxKey() const override
    {
        return TxKey(end_key_);
    }

    const KeyT *StartKey() const
    {
        return start_key_;
    }

    const KeyT *EndKey() const
    {
        return end_key_;
    }

    void SetEndKey(const KeyT *end)
    {
        end_key_ = end;
    }

private:
    const KeyT *start_key_{nullptr};
    const KeyT *end_key_{nullptr};
};

struct RangeSliceId;

class StoreRange
{
public:
    /**
     * @brief Max number of slices in range. The total size of a range
     * is (16*1024) * (16*1024) = 256MB
     *
     */
#ifdef SMALL_RANGE
    static constexpr uint32_t range_max_size =
        1024 * 1024;  // 1MB range size for testing range split
#else
    static constexpr uint32_t range_max_size =
        268435456;  // 256 * 1024 * 1024 = 256MB
#endif

    static constexpr float_t key_cache_default_load_factor = 0.8;

    static constexpr float_t new_range_load_factor = 0.1;

    StoreRange(uint32_t partition_id,
               NodeGroupId range_owner,
               LocalCcShards &cc_shards,
               bool init_key_cache,
               size_t estimate_rec_size,
               bool has_dml_since_ddl = true);

    StoreRange(const StoreRange &) = delete;

    virtual ~StoreRange() = default;

    RangeSliceOpStatus PinSlice(const TableName &tbl_name,
                                int64_t ng_term,
                                StoreSlice *slice,
                                const KeySchema *key_schema,
                                const RecordSchema *rec_schema,
                                uint64_t schema_ts,
                                uint64_t snapshot_ts,
                                const KVCatalogInfo *kv_info,
                                CcRequestBase *cc_request,
                                CcShard *cc_shard,
                                store::DataStoreHandler *store_hd,
                                bool force_load = false,
                                uint32_t prefetch_size = 0);

    void UnpinSlice(StoreSlice *slice, bool need_lock_range);

    void BatchUnpinSlices(StoreSlice *start_slice,
                          const StoreSlice *end_slice,
                          bool forward_dir);

    bool UpdateRangeSlicesInStore(const TableName &table_name,
                                  uint64_t ckpt_ts,
                                  uint64_t range_version,
                                  store::DataStoreHandler *store_hd);

    bool SampleSubRangeKeys(StoreSlice *slice,
                            const TableName &table_name,
                            NodeGroupId ng_id,
                            int64_t ng_term,
                            uint64_t data_sync_ts,
                            size_t key_cnt,
                            std::vector<TxKey> &new_range_keys);

    void UpdateSliceSpec(StoreSlice *slice,
                         const std::vector<TxKey> &new_range_keys,
                         size_t first_idx,
                         size_t subslice_cnt);

    void UpdateSliceSpec(StoreSlice *slice,
                         std::vector<SliceChangeInfo> &split_keys);

    /**
     * This function is NOT THREAD SAFE. Only checkpointer should be calling
     * this function and update range specs.
     */
    virtual bool CalculateRangeSplitKeys(
        const TableName &table_name,
        NodeGroupId ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        size_t post_ckpt_size,
        std::vector<TxKey> &new_range_keys) = 0;

    // Update slice size after data flush. If flush is successful,
    // update slice size to precalculated post ckpt size. Otherwise,
    // reset post ckpt size.
    virtual bool UpdateSliceSizeAfterFlush(const TxKey *start_key,
                                           const TxKey *end_key,
                                           bool flush_res) = 0;

    virtual TxKey RangeStartTxKey() const = 0;
    virtual TxKey RangeEndTxKey() const = 0;

    uint32_t PartitionId() const
    {
        return partition_id_;
    }

    virtual StoreSlice *FindSlice(const TxKey &key) = 0;
    virtual StoreSlice *FindSlice(size_t idx) = 0;

    virtual std::vector<const StoreSlice *> Slices() const = 0;

    /**
     * @brief Get the StoreSlice between the @@start_key and @@end_key.
     *
     * @param inclusive - True if need return the slice to which the @@end_key
     * belong.
     */
    virtual std::vector<const StoreSlice *> SubSlices(
        const TxKey &start_key,
        const TxKey &end_key,
        bool inclusive = false) = 0;

    virtual size_t PostCkptSize() = 0;

    virtual size_t SlicesCount() const = 0;

    uint32_t Pins()
    {
        return pins_.load(std::memory_order_acquire);
    }

    void Lock()
    {
        has_write_lock_.store(true, std::memory_order_relaxed);
    }

    bool HasLock()
    {
        return has_write_lock_.load(std::memory_order_relaxed);
    }

    void Unlock()
    {
        return has_write_lock_.store(false, std::memory_order_relaxed);
    }

    // virtual void SetRangeEndTxKey(TxKey end_key) = 0;

    void UpdateLastAccessedTs(uint64_t ts)
    {
        last_accessed_ts_.store(ts, std::memory_order_relaxed);
    }

    uint64_t LastAccessedTs() const
    {
        return last_accessed_ts_.load(std::memory_order_relaxed);
    }

    std::string KeyCacheInfo(uint16_t core_id) const
    {
        assert(core_id < key_cache_.size());
        return key_cache_[core_id]->Info();
    }

    void SetHasDmlSinceDdl()
    {
        has_dml_since_ddl_ = true;
    }

    bool HasDmlSinceDdl() const
    {
        return has_dml_since_ddl_;
    }

protected:
    class LoadSliceController
    {
    public:
        static StoreRange::LoadSliceController ForceLoadConotroller();

        static StoreRange::LoadSliceController NonForceLoadController(
            bool async_emit, CcShard *submitter);

        bool ForceLoad() const
        {
            return force_load_;
        }

        bool AsyncEmit() const
        {
            return async_emit_;
        }

        // Round-robin load balance.
        uint16_t NextExecutor() const;

    private:
        bool force_load_;

        // When async_emit is set, dispatch the loading task to other
        // TxProcessor. It is mainly used for concurrency emitting.
        // StoreHandler Driver may cost a lot of cpu to emit a request. e.g,
        // Cassandra prepare-stmt and execute-stmt are relatively slow.
        bool async_emit_;

        CcShard *submitter_;
        mutable uint16_t executor_;
    };

    enum struct LoadSliceStatus
    {
        Success,
        Delay,
        Retry,
        Error
    };

    LoadSliceStatus LoadSlice(const TableName &tbl_name,
                              int64_t cc_ng_term,
                              StoreSlice &slice,
                              const KeySchema *key_schema,
                              const RecordSchema *rec_schema,
                              uint64_t schema_ts,
                              uint64_t snapshot_ts,
                              const KVCatalogInfo *kv_info,
                              CcRequestBase *cc_request,
                              CcShard *cc_shard,
                              store::DataStoreHandler *store_hd,
                              const LoadSliceController &ctrl,
                              std::unique_lock<std::mutex> &&slice_lk);

    virtual std::pair<size_t, size_t> SearchSlice(
        const StoreSlice *slice) const = 0;
    virtual StoreSlice *GetSlice(size_t slice_idx) const = 0;
    virtual void UpdateSlice(StoreSlice *slice,
                             std::vector<SliceChangeInfo> &split_info) = 0;

    bool SetLastInitKeyCacheTs();

    /**
     * @brief The partition ID of the range.
     *
     */
    uint32_t partition_id_;

    NodeGroupId cc_ng_id_;

    /**
     * @brief A collection of slices the checkpointer intends to alter. The
     * checkpointer adds a slice to this collection, when it detects that the
     * slice's size in the data store exceeds the pre-defined threshold after
     * flushing changed data items in this slice. Slices in the collection
     * cannot be pinned by online tx's, until the checkpointer finishes
     * splitting or merging the slice. The checkpointer is single-threaded and
     * only splits one or merges two slices at a time. We use the vector rather
     * than the hash map, because for very small collections searching in the
     * vector is fast enough.
     *
     */

    mutable std::shared_mutex mux_;

    std::condition_variable_any wait_cv_;

    LocalCcShards &local_cc_shards_;

    std::atomic<bool> has_write_lock_{false};

    std::atomic_uint64_t last_accessed_ts_;
    // pins_ will increase in these cases:
    // 1. Child store slice is pinned
    // 2. Child store slice is being loaded
    // 3. TableRangeEntry.PinStoreRange() is called.
    // This is the value we rely on to decide if a StoreRange can be
    // safely evicted from memory.
    std::atomic_uint32_t pins_{0};

    // A key cache that caches 1) All keys in ccm (including deleted keys) 2)
    // All visible keys in kv store (does not include deleted keys). It is used
    // if we cannot find a key in cc map. We will query for the key in this
    // cache to make sure that the key exists in kv before loading the slice
    // from kv. We maintain a cache for each core to reduce contention. We only
    // maintain key cache for primary key for now. A 12 bits long fingerprint
    // for each key leads to a 0.1% false positive rate for the key cache.

    // The cache is updated when 1) whenever a new key is inserted into ccm, we
    // will add the key to key cache. 2) when a deleted key is removed from ccm,
    // this means this key has been inserted into key cache when its inserted
    // into ccm, but does not exist in kv, so we need to remove it from key
    // cache. Removing keys from cache when they are evicted reduces the number
    // of look ups to find the slice of the key since we can evict the keys in
    // batch.
    std::vector<std::unique_ptr<cuckoofilter::CuckooFilter<size_t, 12>>>
        key_cache_;
    std::atomic<uint64_t> last_init_key_cache_time_{0};

    // This variable is used during the upsert table scheme transaction(such as,
    // add index), it represents that whether there are keys whose version is
    // larger than the dirty table version in this range, that is to say, there
    // are concurrent dml transaction during the ddl transaction.
    bool has_dml_since_ddl_{true};

    friend class StoreSlice;
    friend struct TableRangeEntry;
    friend struct RangeSliceId;
};

template <typename KeyT>
class TemplateStoreRange : public StoreRange
{
public:
    TemplateStoreRange(const KeyT *start_key,
                       const KeyT *end_key,
                       uint32_t partition_id,
                       NodeGroupId range_owner,
                       LocalCcShards &cc_shards,
                       bool init_key_cache,
                       bool empty_range = false,
                       size_t estimate_rec_size = UINT64_MAX,
                       bool has_dml_since_ddl = true)
        : StoreRange(partition_id,
                     range_owner,
                     cc_shards,
                     init_key_cache,
                     estimate_rec_size,
                     has_dml_since_ddl),
          range_start_key_(start_key),
          range_end_key_(end_key)
    {
        std::unique_ptr<TemplateStoreSlice<KeyT>> slice =
            std::make_unique<TemplateStoreSlice<KeyT>>(
                start_key,
                end_key,
                0,
                empty_range ? SliceStatus::FullyCached
                            : SliceStatus::PartiallyCached,
                init_key_cache,
                empty_range ? init_key_cache : false);
        slices_.emplace_back(std::move(slice));
    }

    ~TemplateStoreRange() = default;

    void SetRangeEndKey(const KeyT *end_key)
    {
        range_end_key_ = end_key;
        assert(slices_.size() > 0);
        slices_.back()->SetEndKey(end_key);
    }

    TxKey RangeStartTxKey() const override
    {
        return TxKey(range_start_key_);
    }

    TxKey RangeEndTxKey() const override
    {
        return TxKey(range_end_key_);
    }

    const KeyT *RangeStartKey() const
    {
        return range_start_key_;
    }

    const KeyT *RangeEndKey() const
    {
        return range_end_key_;
    }

    void InitSlices(std::vector<SliceInitInfo> &&slice_keys)
    {
        slices_.clear();
        boundary_keys_.clear();

        const KeyT *slice_start = range_start_key_;
        const KeyT *slice_end = slice_keys.size() > 1
                                    ? slice_keys[1].key_.GetKey<KeyT>()
                                    : range_end_key_;
        // The start of the first slice of the first range points to
        // KeyT::NegativeInfinity().
        assert(slice_start != nullptr);
        // The end of the last slice of the last range points to positive
        // infinity.
        assert(slice_end != nullptr);
        size_t slice_size = slice_keys.size() > 0 ? slice_keys[0].size_ : 0;
        SliceStatus slice_status = slice_keys[0].status_;

        std::unique_ptr<TemplateStoreSlice<KeyT>> slice =
            std::make_unique<TemplateStoreSlice<KeyT>>(slice_start,
                                                       slice_end,
                                                       slice_size,
                                                       slice_status,
                                                       !key_cache_.empty());

        slices_.emplace_back(std::move(slice));

        for (size_t idx = 1; idx < slice_keys.size(); ++idx)
        {
            slice_start = slice_keys[idx].key_.GetKey<KeyT>();
            slice_end = idx == slice_keys.size() - 1
                            ? range_end_key_
                            : slice_keys[idx + 1].key_.GetKey<KeyT>();
            slice_size = slice_keys[idx].size_;
            slice_status = slice_keys[idx].status_;

            slice =
                std::make_unique<TemplateStoreSlice<KeyT>>(slice_start,
                                                           slice_end,
                                                           slice_size,
                                                           slice_status,
                                                           !key_cache_.empty());

            slices_.emplace_back(std::move(slice));

            assert(slice_keys[idx].key_.IsOwner());
            std::unique_ptr<KeyT> boundary_key =
                slice_keys[idx].key_.MoveKey<KeyT>();
            boundary_keys_.emplace_back(std::move(boundary_key));
        }

        assert(slices_.size() == boundary_keys_.size() + 1);
    }

    StoreSlice *FindSlice(const TxKey &key) override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        const KeyT *typed_key = key.GetKey<KeyT>();
        size_t slice_idx = SearchSlice(*typed_key, true);
        return slices_[slice_idx].get();
    }

    TemplateStoreSlice<KeyT> *FindSlice(const KeyT &key)
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        size_t slice_idx = SearchSlice(key, true);
        return slices_[slice_idx].get();
    }

    StoreSlice *FindSlice(size_t idx) override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        return slices_.at(idx).get();
    }

    std::vector<const StoreSlice *> Slices() const override
    {
        std::vector<const StoreSlice *> slice_vec;
        slice_vec.reserve(slices_.size());

        for (const auto &slice : slices_)
        {
            slice_vec.emplace_back(slice.get());
        }

        return slice_vec;
    }

    /**
     * @brief Get the StoreSlice between the @@start_key and @@end_key.
     *
     * @param inclusive - True if need return the slice to which the @@end_key
     * belong.
     */
    std::vector<const StoreSlice *> SubSlices(const TxKey &start_key,
                                              const TxKey &end_key,
                                              bool inclusive = false) override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        std::vector<const StoreSlice *> slice_vec;

        const KeyT *typed_key = start_key.GetKey<KeyT>();
        size_t start_slice_idx = SearchSlice(*typed_key, true);

        typed_key = end_key.GetKey<KeyT>();
        bool inclusive_slice = inclusive;
        size_t end_slice_idx = SearchSlice(*typed_key, inclusive_slice);

        slice_vec.reserve(end_slice_idx - start_slice_idx + 1);
        for (size_t idx = start_slice_idx; idx <= end_slice_idx; ++idx)
        {
            slice_vec.emplace_back(slices_[idx].get());
        }

        return slice_vec;
    }

    size_t SlicesCount() const override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        return slices_.size();
    }

    const std::vector<std::unique_ptr<TemplateStoreSlice<KeyT>>> &TypedSlices()
        const
    {
        return slices_;
    }

    void InvalidateKeyCache(uint16_t core_id)
    {
        if (key_cache_.empty())
        {
            return;
        }
        LOG(INFO) << "Invalidate key cache of range " << partition_id_
                  << " on core " << core_id << " due to collision";
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        // shared lock to avoid slice split
        for (auto &slice : slices_)
        {
            slice->SetKeyCacheValidity(core_id, false);
        }
        // Create a larger key cache if the old one cannot hold enough keys.
        size_t last_key_cache_size = key_cache_[core_id]->Size();
        key_cache_[core_id] =
            std::make_unique<cuckoofilter::CuckooFilter<size_t, 12>>(
                last_key_cache_size * 1.2);
    }
    /**
     * @brief Split the range with new_end. new_end will be the new
     * end key of this range, and every slice after new_end will be removed
     * from this range and returned to the caller.
     *
     * @param new_end
     * @return std::vector<std::pair<TxKey::Uptr, uint32_t, SliceStatus>>
     */
    bool SplitRange(const KeyT *new_end,
                    std::vector<SliceInitInfo> &removed_slices)
    {
        std::unique_lock<std::shared_mutex> range_lk(mux_);
        size_t remove_offset = SearchSlice(*new_end, true);
        if (remove_offset == 0)
        {
            // All slices are smaller than new end
            return true;
        }
        auto boundary = boundary_keys_.begin() + remove_offset - 1;
        for (auto slice = slices_.begin(); slice != slices_.end(); slice++)
        {
            // first check if any of the slices in range is pinned. We should
            // not update StoreRange if it is used by any cc req. Normally this
            // should not happen since we've already acquired range write lock.
            // But if a node acuired range read lock then failed over, we might
            // have cc request accessing this range even if range split tx has
            // acquired range write lock. In this case, we should wait for them
            // to complete before continuing.
            std::unique_lock<std::mutex> slice_lk((*slice)->slice_mux_);
            if ((*slice)->pins_ > 0)
            {
                DLOG(INFO) << "slice pinned when trying to split range";
                return false;
            }
            else if ((*slice)->status_ == SliceStatus::BeingLoaded)
            {
                DLOG(INFO) << "slice filling into memory when trying to "
                              "split range";
                return false;
            }
            else if ((*slice)->FillCcRequest() != nullptr)
            {
                DLOG(INFO) << "slice loading from data store when trying to "
                              "split range";
                return false;
            }
            else if ((*slice)->InitKeyCacheRequest() != nullptr)
            {
                DLOG(INFO) << "slice initing key cache when trying to "
                              "split range";
                return false;
            }
        }
        auto slice = slices_.begin() + remove_offset;
        while (boundary != boundary_keys_.end())
        {
            // Remove boundary keys >= new end key
            removed_slices.emplace_back(TxKey(std::move(*boundary)),
                                        (*slice)->Size(),
                                        (*slice)->status_);
            boundary++;
            slice++;
        }
        slices_.erase(slices_.begin() + remove_offset, slices_.end());
        boundary_keys_.erase(boundary_keys_.begin() + remove_offset - 1,
                             boundary_keys_.end());
        assert(!removed_slices.empty());
        // We set the range's end key to nullptr for now. It will be updated
        // when new ranges are inserted into the table range table in
        // LocalCcShards, and set to the start of the first splitted range.
        range_end_key_ = nullptr;

        return true;
    }

    RangeSliceId PinSlices(
        const TableName &tbl_name,
        int64_t ng_term,
        const KeyT &search_key,
        bool inclusive,
        const KeyT *end_key,
        bool end_inclusive,
        const KeySchema *key_schema,
        const RecordSchema *rec_schema,
        uint64_t schema_ts,
        uint64_t snapshot_ts,
        const KVCatalogInfo *kv_info,
        CcRequestBase *cc_request,
        CcShard *cc_shard,
        store::DataStoreHandler *store_hd,
        bool force_load,
        uint32_t prefetch_size,
        uint32_t max_pin_cnt,
        bool forward_pin,
        RangeSliceOpStatus &pin_status,
        const StoreSlice *&last_pinned_slice,
        bool check_key_cache = false,
        uint16_t shard_id = 0,  // only used if check_key_cache = true
        bool no_load_on_miss = false,
        bool prefetch_force_load = false,
        const std::function<int32_t(int32_t, bool)> &next_prefetch_slice =
            [](int32_t idx, bool forward)
        { return forward ? (idx + 1) : (idx - 1); })
    {
        // A shared lock on the range to prevent concurrent splitting or merging
        // of slices.
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        bool inclusive_slice = forward_pin || inclusive;
        size_t slice_idx = SearchSlice(search_key, inclusive_slice);
        StoreSlice *slice = slices_[slice_idx].get();
        std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

        uint32_t pin_slice_cnt = 0;
        bool to_prefetch = prefetch_size > 0;

        last_pinned_slice = nullptr;

        if (slice->to_alter_)
        {
            // The checkpointer is waiting to alter this slice. The calling tx
            // is pushed back for re-execution, if the request is processed for
            // the first time. If the slice has been pinned, forcing the
            // checkpointer to wait, the request must be allowed to proceed to
            // finish and unpin the slice.
            pin_status = RangeSliceOpStatus::Retry;
            return RangeSliceId(this, slice);
        }
        CODE_FAULT_INJECTOR("PinSlices_Fail", {
            LOG(INFO) << "FaultInject  PinSlices_Fail, " << check_key_cache
                      << ", is valid " << slice->IsValidInKeyCache(shard_id);
            if (slice->status_ == SliceStatus::FullyCached)
            {
                slice->status_ = SliceStatus::PartiallyCached;
            }
        });
        if (slice->status_ == SliceStatus::FullyCached)
        {
            ++slice->pins_;
            ++pin_slice_cnt;
            pin_status = RangeSliceOpStatus::Successful;
            last_pinned_slice = slice;

            slice_lk.unlock();

            if (forward_pin)
            {
                for (size_t s_idx = slice_idx + 1;
                     s_idx < slices_.size() && pin_slice_cnt < max_pin_cnt;
                     ++s_idx)
                {
                    TemplateStoreSlice<KeyT> *prepin_slice =
                        slices_[s_idx].get();

                    if (end_key != nullptr)
                    {
                        // If the request (e.g., a scan) specifies the end key,
                        // does not pin slices beyond the end key.
                        const KeyT *slice_start = prepin_slice->StartKey();
                        if (!(*slice_start < *end_key ||
                              (end_inclusive && *slice_start == *end_key)))
                        {
                            to_prefetch = false;
                            break;
                        }
                    }

                    std::unique_lock<std::mutex> s_lk(prepin_slice->slice_mux_);
                    if (!prepin_slice->to_alter_ &&
                        prepin_slice->status_ == SliceStatus::FullyCached)
                    {
                        last_pinned_slice = prepin_slice;
                        ++prepin_slice->pins_;
                        ++pin_slice_cnt;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            else if (slice_idx > 0)
            {
                for (int32_t s_idx = slice_idx - 1;
                     s_idx >= 0 && pin_slice_cnt < max_pin_cnt;
                     --s_idx)
                {
                    TemplateStoreSlice<KeyT> *prepin_slice =
                        slices_[s_idx].get();

                    if (end_key != nullptr)
                    {
                        // If the request (e.g., a scan) specifies the end key,
                        // does not pin slices beyond the end key.
                        const KeyT *slice_end = prepin_slice->EndKey();
                        if (!(*end_key < *slice_end))
                        {
                            to_prefetch = false;
                            break;
                        }
                    }

                    std::unique_lock<std::mutex> s_lk(prepin_slice->slice_mux_);
                    if (!prepin_slice->to_alter_ &&
                        prepin_slice->status_ == SliceStatus::FullyCached)
                    {
                        last_pinned_slice = prepin_slice;
                        ++prepin_slice->pins_;
                        ++pin_slice_cnt;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            slice_lk.lock();
            pins_.fetch_add(pin_slice_cnt, std::memory_order_release);
        }
        else if (check_key_cache)
        {
            assert(to_prefetch == false);
            if (slice->IsValidInKeyCache(shard_id))
            {
                bool found = ContainsKey(search_key, shard_id);
                if (!found)
                {
                    // If the key is not found in range, directly return and
                    // skip loading slice from kv
                    pin_status = RangeSliceOpStatus::KeyNotExists;
                    return RangeSliceId(this, slice);
                }
                // If key is found in range key cache, the key must exist in kv
                // store. Load slice from kv to get the value.
            }
            else if (!slice->IsLoadingKeyCache(shard_id))
            {
                // If this slice can use key cache but the key cache is not
                // intialized, always load slice from kv to initialize the key
                // cache.
                no_load_on_miss = false;
            }
            else
            {
                // Someone is loading this key cache into memory, no need to
                // load slice to init key cache, just wait and retry.
                pin_status = RangeSliceOpStatus::Retry;
                return RangeSliceId(this, slice);
            }
        }

        assert(slice_lk.owns_lock());

        if (slice->status_ != SliceStatus::FullyCached)
        {
            if (!no_load_on_miss)
            {
                LoadSliceController load_ctrl =
                    force_load ? LoadSliceController::ForceLoadConotroller()
                               : LoadSliceController::NonForceLoadController(
                                     false, cc_shard);

                LoadSliceStatus load_ret = LoadSlice(tbl_name,
                                                     ng_term,
                                                     *slice,
                                                     key_schema,
                                                     rec_schema,
                                                     schema_ts,
                                                     snapshot_ts,
                                                     kv_info,
                                                     cc_request,
                                                     cc_shard,
                                                     store_hd,
                                                     load_ctrl,
                                                     std::move(slice_lk));
                switch (load_ret)
                {
                case LoadSliceStatus::Success:
                    pin_status = RangeSliceOpStatus::BlockedOnLoad;
                    break;
                case LoadSliceStatus::Delay:
                    pin_status = RangeSliceOpStatus::Delay;
                    break;
                case LoadSliceStatus::Retry:
                    pin_status = RangeSliceOpStatus::Retry;
                    break;
                default:
                    pin_status = RangeSliceOpStatus::Error;
                    break;
                }
            }
            else
            {
                pin_status = RangeSliceOpStatus::NotPinned;
                assert(prefetch_size == 0);
            }
        }

        if (slice_lk.owns_lock())
        {
            slice_lk.unlock();
        }

        if (to_prefetch)
        {
            LoadSliceController load_ctrl =
                prefetch_force_load
                    ? LoadSliceController::ForceLoadConotroller()
                    : LoadSliceController::NonForceLoadController(true,
                                                                  cc_shard);

            if (forward_pin)
            {
                int32_t curr_idx =
                    slice_idx + (pin_slice_cnt > 1 ? (pin_slice_cnt - 1) : 0);
                int32_t sid = next_prefetch_slice(curr_idx, true);

                for (uint32_t k = 0;
                     static_cast<size_t>(sid) < slices_.size() &&
                     k < prefetch_size;
                     ++k)
                {
                    TemplateStoreSlice<KeyT> *prefetch_slice =
                        slices_[sid].get();
                    if (end_key != nullptr)
                    {
                        // If the request (e.g., a scan) specifies the
                        // end key, does not prefetch slices beyond the
                        // end key.
                        const KeyT *slice_start = prefetch_slice->StartKey();
                        if (!(*slice_start < *end_key ||
                              (end_inclusive && *slice_start == *end_key)))
                        {
                            break;
                        }
                    }

                    std::unique_lock<std::mutex> prefetch_lk(
                        prefetch_slice->slice_mux_, std::try_to_lock);

                    if (prefetch_lk.owns_lock())
                    {
                        if (prefetch_slice->status_ ==
                            SliceStatus::PartiallyCached)
                        {
                            LoadSlice(tbl_name,
                                      ng_term,
                                      *prefetch_slice,
                                      key_schema,
                                      rec_schema,
                                      schema_ts,
                                      snapshot_ts,
                                      kv_info,
                                      nullptr,
                                      cc_shard,
                                      store_hd,
                                      load_ctrl,
                                      std::move(prefetch_lk));
                        }
                    }

                    sid = next_prefetch_slice(sid, true);
                }
            }
            else if (slice_idx > 0)
            {
                uint32_t curr_idx =
                    slice_idx - (pin_slice_cnt > 1 ? (pin_slice_cnt - 1) : 0);
                int32_t sid = next_prefetch_slice(curr_idx, false);

                for (uint32_t k = 0; sid > -1 && k < prefetch_size; ++k)
                {
                    TemplateStoreSlice<KeyT> *prefetch_slice =
                        slices_[sid].get();
                    if (end_key != nullptr)
                    {
                        // If the request (e.g., a scan) specifies the
                        // end key, does not prefetch slices beyond the
                        // end key.
                        const KeyT *slice_end = prefetch_slice->EndKey();
                        if (!(*end_key < *slice_end))
                        {
                            break;
                        }
                    }

                    std::unique_lock<std::mutex> prefetch_lk(
                        prefetch_slice->slice_mux_);

                    if (prefetch_slice->status_ == SliceStatus::PartiallyCached)
                    {
                        LoadSlice(tbl_name,
                                  ng_term,
                                  *prefetch_slice,
                                  key_schema,
                                  rec_schema,
                                  schema_ts,
                                  snapshot_ts,
                                  kv_info,
                                  nullptr,
                                  cc_shard,
                                  store_hd,
                                  load_ctrl,
                                  std::move(prefetch_lk));
                    }

                    sid = next_prefetch_slice(sid, false);
                }
            }
        }

        return RangeSliceId(this, slice);
    }

    bool CalculateRangeSplitKeys(const TableName &table_name,
                                 NodeGroupId ng_id,
                                 int64_t ng_term,
                                 uint64_t data_sync_ts,
                                 size_t post_ckpt_size,
                                 std::vector<TxKey> &new_range_keys) override
    {
        uint32_t slice_idx = 0;
        uint32_t subrange_slice_idx = 0;
        size_t subrange_cnt =
            std::ceil(post_ckpt_size / (StoreRange::range_max_size *
                                        StoreRange::new_range_load_factor));
        size_t avg_subrange_size = post_ckpt_size / subrange_cnt;

        new_range_keys.reserve(subrange_cnt);

        while (slice_idx < slices_.size())
        {
            size_t curr_subrange_size = 0;
            bool sample_keys = false;
            StoreSlice *curr_slice = nullptr;
            for (; curr_subrange_size < avg_subrange_size &&
                   slice_idx < slices_.size();
                 ++slice_idx)
            {
                curr_slice = slices_[slice_idx].get();
                if (curr_slice->PostCkptSize() != UINT64_MAX)
                {
                    if (curr_slice->PostCkptSize() > avg_subrange_size)
                    {
                        // The current slice need to split into multiple
                        // sub-ranges, then to sample subrange keys from the
                        // current slice.
                        sample_keys = true;
                        break;
                    }

                    curr_subrange_size += curr_slice->PostCkptSize();
                }
                else
                {
                    curr_subrange_size += curr_slice->Size();
                }
            }

            if (sample_keys)
            {
                // The post ckpt size of the current slice is large than average
                // sub-range size, so need to split into several subrange.
                // The start key of the first sub-range is the start key of the
                // current slice, and start keys of the remaining sub-ranges are
                // obtained by sampling.
                size_t slice_subranges_cnt =
                    std::ceil(curr_slice->PostCkptSize() / avg_subrange_size);

                slice_subranges_cnt =
                    (slice_subranges_cnt < 2) ? 2 : slice_subranges_cnt;
                /**
                 * If curr_subrange_size == 0, then subrange_slice_index ==
                 * slice_index, otherwise, subrange_slice_index < slice_index.
                 * Anyway, should use the start key of the slice with
                 * subrange_slice_index as one subrange key.
                 */
                if (subrange_slice_idx != 0)
                {
                    // Skip the first subrange.
                    const KeyT *slice_start =
                        slices_[subrange_slice_idx]->StartKey();
                    assert(slice_start->Type() == KeyType::Normal);
                    new_range_keys.emplace_back(
                        std::make_unique<KeyT>(*slice_start));
                }

                size_t first_key_idx = new_range_keys.size();
                if (!SampleSubRangeKeys(curr_slice,
                                        table_name,
                                        ng_id,
                                        ng_term,
                                        data_sync_ts,
                                        (slice_subranges_cnt - 1),
                                        new_range_keys))
                {
                    return false;
                }
                assert(first_key_idx <= new_range_keys.size());

                size_t actual_subranges_cnt =
                    new_range_keys.size() - first_key_idx + 1;
                if (actual_subranges_cnt > 1)
                {
                    // Update the current slice into multiple slices. To avoid
                    // more than one subrange task access one slice
                    // simultaneously during handling the FlushRecords. NOTE:
                    // The new slice spec is not the final status, the new
                    // subslices boundary is equal to the boundary of the
                    // subrange which the new subslice belong to.
                    UpdateSliceSpec(curr_slice,
                                    new_range_keys,
                                    first_key_idx,
                                    actual_subranges_cnt);
                }

                // Update the slice index:
                slice_idx += actual_subranges_cnt;
            }
            // Skip the first subrange since it will reuse the current range
            // entry.
            else if (subrange_slice_idx != 0)
            {
                const KeyT *slice_start =
                    slices_[subrange_slice_idx]->StartKey();
                assert(slice_start->Type() == KeyType::Normal);
                new_range_keys.emplace_back(
                    std::make_unique<KeyT>(*slice_start));
            }

            subrange_slice_idx = slice_idx;
        }
        assert(
            [&]()
            {
                bool sorted = std::is_sorted(new_range_keys.begin(),
                                             new_range_keys.end());
                auto it = std::adjacent_find(new_range_keys.begin(),
                                             new_range_keys.end());
                bool unique = it == new_range_keys.end();
                return sorted && unique;
            }());
        return true;
    }

    void DeleteKey(const KeyT &key, uint16_t core_id, StoreSlice *slice)
    {
        if (slice == nullptr)
        {
            TxKey search_key(&key);
            slice = FindSlice(search_key);
        }
        if (slice->IsValidInKeyCache(core_id))
        {
            cuckoofilter::Status status =
                key_cache_[core_id]->Delete(key.Hash());
            // We should not try to delete a non-existing key.
            if (status == cuckoofilter::Status::NotFound)
            {
                LOG(ERROR) << "Deleting a non-existing key from key cache.";
            }
            assert(status != cuckoofilter::Status::NotFound);
        }
        // Delete key is not going to be called if slice is being loaded, so
        // we don't need to worry about concurrent key cache initialization.
    }

    // NOTE: The slice to which the @@key belong must be valid in key cache.
    void DeleteKey(const KeyT &key, uint16_t core_id)
    {
        cuckoofilter::Status status = key_cache_[core_id]->Delete(key.Hash());
        // We should not try to delete a non-existing key.
        if (status == cuckoofilter::Status::NotFound)
        {
            LOG(ERROR) << "Deleting a non-existing key from key cache.";
        }
        assert(status != cuckoofilter::Status::NotFound);
    }

    RangeSliceOpStatus AddKey(const KeyT &key,
                              uint16_t core_id,
                              StoreSlice *slice = nullptr,
                              bool init = false)
    {
        assert(txservice_enable_key_cache);
        if (slice == nullptr)
        {
            TxKey search_key(&key);
            slice = FindSlice(search_key);
        }
        if (init || slice->IsValidInKeyCache(core_id))
        {
            assert(init || !slice->IsLoadingKeyCache(core_id));
            cuckoofilter::Status status = key_cache_[core_id]->Add(key.Hash());
            if (status == cuckoofilter::Status::Ok)
            {
                return RangeSliceOpStatus::Successful;
            }
            else
            {
                assert(status == cuckoofilter::Status::NotEnoughSpace);
                // Add failed, we need to invalidate the filter.
                InvalidateKeyCache(core_id);
                return RangeSliceOpStatus::Error;
            }
        }
        else if (slice->IsLoadingKeyCache(core_id))
        {
            // Retry later when key cache is initialized.
            return RangeSliceOpStatus::Retry;
        }
        else
        {
            // cache is not valid, no op
            return RangeSliceOpStatus::Successful;
        }
    }

    void InitKeyCache(CcShard *cc_shard,
                      const TableName *tbl_name,
                      NodeGroupId ng_id,
                      int64_t term,
                      bool force = false)
    {
        if (!SetLastInitKeyCacheTs() && !force)
        {
            // Just initialized recently but already invalidated. The range in
            // memory might not fit into the key cache.
            return;
        }
        std::shared_lock<std::shared_mutex> lk(mux_);
        for (auto &slice : slices_)
        {
            slice->InitKeyCache(cc_shard, this, tbl_name, ng_id, term);
        }
    }

    bool ContainsKey(const KeyT &key, uint16_t core_id)
    {
        return key_cache_[core_id]->Contain(key.Hash()) ==
               cuckoofilter::Status::Ok;
    }

    size_t PostCkptSize() override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        size_t size = 0;
        for (size_t idx = 0; idx < slices_.size(); idx++)
        {
            if (slices_.at(idx)->post_ckpt_size_ != UINT64_MAX)
            {
                size += slices_.at(idx)->PostCkptSize();
            }
            else
            {
                size += slices_.at(idx)->Size();
            }
        }
        return size;
    }

    bool UpdateSliceSizeAfterFlush(const TxKey *start_key,
                                   const TxKey *end_key,
                                   bool flush_res) override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        bool updated = false;
        size_t slice_start_idx = 0;
        size_t slice_end_idx = slices_.size();
        if (start_key)
        {
            assert(end_key);
            const KeyT *typed_key = start_key->GetKey<KeyT>();
            slice_start_idx = SearchSlice(*typed_key, true);
            slice_end_idx = slice_start_idx;
            while (++slice_end_idx < slices_.size() &&
                   slices_[slice_end_idx]->StartTxKey() < *end_key)
            {
                ;
            }
        }

        // Iterate over all slices
        for (size_t idx = slice_start_idx; idx < slice_end_idx; ++idx)
        {
            if (flush_res)
            {
                updated |= slices_[idx]->UpdateSize();
            }
            else
            {
                slices_[idx]->SetPostCkptSize(UINT64_MAX);
            }
        }

        return updated;
    }

private:
    static size_t LowerBound(
        const std::vector<std::unique_ptr<KeyT>> &slice_keys,
        const KeyT &search_key);

    // ------------|----------|----------|----------|----------|----------->
    // -00/BK     K0         K1         K2         K3         K4       +00/EK
    // [    S0    )[   S1    )[    S2   )[    S3   )[    S4   )[    S5     )
    //
    // Suppose there are SIX slices, with FIVE boundary keys. Also Ki is both
    // slice[i]'s end_key and slice[i+1]'s start_key.
    //
    // - Case search_key is above K4, certainly lower_bound_idx = 5:
    //   For scan [search_key, +00): return slice[5]
    //   For scan (search_key, +00): return slice[5]
    //   For scan (-00, search_key]: return slice[5]
    //   For scan (-00, search_key): return slice[5]
    //
    // - Case search_key is between (K1, K2), certainly lower_bound_idx = 2:
    //   For scan [search_key, +00): return slice[2]
    //   For scan (search_key, +00): return slice[2]
    //   For scan (-00, search_key]: return slice[2]
    //   For scan (-00, search_key): return slice[2]
    //
    // - Case search_key is equal to K2, certainly lower_bound_idx = 2:
    //   For scan [search_key, +00): return slice[3]
    //   For scan (search_key, +00): return slice[3]
    //   For scan (-00, search_key]: return slice[3]
    //   For scan (-00, search_key): return slice[2] <<---- Special Case
    //
    //  From above analyze, the last case is special. For an index scan with
    //  `where i < K2`, the method should return the slice just before the
    //  located slice. The bool flag `inclusive_slice` is used to distinguish it
    //  from common cases.
    //
    //  WARNING: the semantic of `inclusive_slice` is quite different from
    //  inclusive scan, which is passed from ScanSliceCc.
    //
    size_t SearchSlice(const KeyT &search_key, bool inclusive_slice) const
    {
        size_t slice_idx = 0;
        size_t lower_bound_idx = LowerBound(boundary_keys_, search_key);

        if (lower_bound_idx == boundary_keys_.size())
        {
            // The search key is greater than or equal to the last slice's
            // starting key.
            slice_idx = lower_bound_idx;
        }
        else
        {
            // The search key equals to or is less than
            // slice_key_[lower_bound_idx].

            if (*boundary_keys_[lower_bound_idx] == search_key &&
                inclusive_slice)
            {
                // If the search key equals to slice_key_[lower_bound_idx], the
                // containing slice is [lower_bound_idx, lower_bound_idx + 1),
                // if the inclusive flag is true.
                slice_idx = lower_bound_idx + 1;
            }
            else
            {
                // If the search key is less than slice_key_[lower_bound_idx],
                // the containing slice is [lower_bound_idx - 1,
                // lower_bound_idx).
                slice_idx = lower_bound_idx;
            }
        }

        return slice_idx;
    }

    std::pair<size_t, size_t> SearchSlice(
        const StoreSlice *slice) const override
    {
        const TemplateStoreSlice<KeyT> *typed_slice =
            static_cast<const TemplateStoreSlice<KeyT> *>(slice);
        const KeyT *slice_start = typed_slice->StartKey();
        assert(slice_start != nullptr);
        size_t slice_idx = SearchSlice(*slice_start, true);
        return {slice_idx, slices_.size()};
    }

    StoreSlice *GetSlice(size_t slice_idx) const override
    {
        return slice_idx < slices_.size() ? slices_[slice_idx].get() : nullptr;
    }

    void UpdateSlice(StoreSlice *slice,
                     std::vector<SliceChangeInfo> &split_keys) override
    {
        assert(split_keys.size() > 1);

        TemplateStoreSlice<KeyT> *typed_slice =
            static_cast<TemplateStoreSlice<KeyT> *>(slice);

        // A slice's start is never null. It points to negative infinity, if
        // this is the first slice of the first range.
        assert(typed_slice->StartKey() != nullptr);
        size_t slice_idx = typed_slice->StartKey() == KeyT::NegativeInfinity()
                               ? 0
                               : SearchSlice(*typed_slice->StartKey(), true);
        const KeyT *slice_end_key = typed_slice->EndKey();

        std::unique_ptr<KeyT> next_slice_start_key = nullptr;
        if (split_keys[1].key_.IsOwner())
        {
            next_slice_start_key = split_keys[1].key_.MoveKey<KeyT>();
        }
        else
        {
            next_slice_start_key =
                std::make_unique<KeyT>(*split_keys[1].key_.GetKey<KeyT>());
        }

        typed_slice->SetEndKey(next_slice_start_key.get());
        slice->size_ = split_keys[0].cur_size_;
        slice->post_ckpt_size_ = split_keys[0].post_update_size_;

        for (size_t idx = 1; idx < split_keys.size(); ++idx)
        {
            const KeyT *sub_slice_start = next_slice_start_key.get();
            const KeyT *sub_slice_end = nullptr;

            size_t boundary_keys_idx = slice_idx + idx - 1;
            // Inserts the new boundary keys.
            boundary_keys_.emplace(boundary_keys_.begin() + boundary_keys_idx,
                                   std::move(next_slice_start_key));

            if (idx < split_keys.size() - 1)
            {
                if (split_keys[idx + 1].key_.IsOwner())
                {
                    next_slice_start_key =
                        split_keys[idx + 1].key_.MoveKey<KeyT>();
                }
                else
                {
                    next_slice_start_key = std::make_unique<KeyT>(
                        *split_keys[idx + 1].key_.GetKey<KeyT>());
                }
                sub_slice_end = next_slice_start_key.get();
            }
            else
            {
                // The last sub-slice's end key points to that of the
                // original slice.
                next_slice_start_key = nullptr;
                sub_slice_end = slice_end_key;
            }

            std::unique_ptr<TemplateStoreSlice<KeyT>> sub_slice =
                std::make_unique<TemplateStoreSlice<KeyT>>(
                    sub_slice_start,
                    sub_slice_end,
                    split_keys[idx].cur_size_,
                    SliceStatus::PartiallyCached,
                    !slice->cache_validity_.empty());

            sub_slice->post_ckpt_size_ = split_keys[idx].post_update_size_;
            sub_slice->status_ = slice->status_;
            sub_slice->last_load_ts_ = slice->last_load_ts_;
            sub_slice->cache_validity_ = slice->cache_validity_;

            // Inserts the new sub-slices following the first sub-slice.
            slices_.emplace(slices_.begin() + slice_idx + idx,
                            std::move(sub_slice));
        }
    }

    /**
     * @brief The start and end keys of the range. The two boundary keys are
     * raw pointers and point to external keys in the table's range entries
     * at LocalCcShards.
     *
     */
    const KeyT *range_start_key_, *range_end_key_;

    /**
     * @brief A sorted vector of keys that split the range into slices.
     *
     */
    std::vector<std::unique_ptr<KeyT>> boundary_keys_;

    std::vector<std::unique_ptr<TemplateStoreSlice<KeyT>>> slices_;
};  // namespace txservice

template <typename KeyT>
size_t TemplateStoreRange<KeyT>::LowerBound(
    const std::vector<std::unique_ptr<KeyT>> &middle_keys,
    const KeyT &search_key)
{
    size_t len = middle_keys.size();
    size_t first_idx = 0;

    while (len > 0)
    {
        size_t half_len = len >> 1;
        size_t middle_idx = first_idx + half_len;
        if (*middle_keys[middle_idx] < search_key)
        {
            first_idx = middle_idx + 1;
            len = len - half_len - 1;
        }
        else
        {
            len = half_len;
        }
    }

    return first_idx;
}

}  // namespace txservice
