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
#include "range_slice.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "catalog_factory.h"
#include "cc_map.h"
#include "cc_req_misc.h"
#include "cc_shard.h"
#include "error_messages.h"
#include "local_cc_shards.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_service_metrics.h"
#include "tx_start_ts_collector.h"
#include "util.h"

namespace txservice
{
void RangeSliceId::Unpin()
{
    range_ptr_->UnpinSlice(slice_ptr_, true);
}

TxKey RangeSliceId::RangeStartTxKey() const
{
    return range_ptr_->RangeStartTxKey();
}

TxKey RangeSliceId::RangeEndTxKey() const
{
    return range_ptr_->RangeEndTxKey();
}

StoreSlice::~StoreSlice() = default;

void StoreSlice::StartLoading(FillStoreSliceCc *fill_req,
                              LocalCcShards &cc_shards)
{
    std::unique_lock<std::mutex> lk(slice_mux_);

    assert(pins_ == 0);
    status_ = SliceStatus::BeingLoaded;

    for (uint16_t core_id = 0; core_id < cc_shards.Count(); ++core_id)
    {
        cc_shards.EnqueueCcRequest(core_id, fill_req);
    }
}

void StoreSlice::CommitLoading(StoreRange &range, uint32_t slice_size)
{
    std::unique_lock<std::mutex> slice_lk(slice_mux_);
    assert(pins_ == 0);
    assert(status_ == SliceStatus::BeingLoaded);

    status_ = SliceStatus::FullyCached;
    size_ = slice_size;

    for (auto &[cc_req, cc_shard] : cc_queue_)
    {
        cc_shard->Enqueue(cc_req);
    }

    if (cc_queue_.size() > 128)
    {
        cc_queue_.resize(8);
        cc_queue_.shrink_to_fit();
    }
    cc_queue_.clear();

    fetch_slice_cc_ = nullptr;
    range.pins_.fetch_sub(1, std::memory_order_release);

    if (to_alter_)
    {
        slice_lk.unlock();
        std::lock_guard<std::shared_mutex> range_lk(range.mux_);
        range.wait_cv_.notify_all();
    }
}

FillStoreSliceCc *StoreSlice::FillCcRequest()
{
    return fetch_slice_cc_;
}

void StoreSlice::SetLoadingError(StoreRange &range, CcErrorCode err_code)
{
    std::unique_lock<std::mutex> slice_lk(slice_mux_);

    assert(pins_ == 0);
    status_ = SliceStatus::PartiallyCached;

    // We need to make sure that the CcMap::Execute(CcRequest ) and
    // CcRequest::ABortCcRequest(...) functions occur on the same thread.
    // Otherwise, AbortCcRequest is not safe behavior.
    std::unordered_map<CcShard *, std::vector<CcRequestBase *>> waiting_reqs;
    for (auto &[cc_req, cc_shard] : cc_queue_)
    {
        waiting_reqs[cc_shard].push_back(cc_req);
    }

    for (auto &[cc_shard, reqs] : waiting_reqs)
    {
        cc_shard->AbortCcRequests(std::move(reqs), err_code);
    }

    if (cc_queue_.size() > 8)
    {
        cc_queue_.resize(8);
        cc_queue_.shrink_to_fit();
    }
    cc_queue_.clear();

    fetch_slice_cc_ = nullptr;
    range.pins_.fetch_sub(1, std::memory_order_release);

    if (to_alter_)
    {
        slice_lk.unlock();
        std::lock_guard<std::shared_mutex> range_lk(range.mux_);
        range.wait_cv_.notify_all();
    }
}

bool StoreSlice::IsRecentLoad() const
{
    int64_t delta = LocalCcShards::ClockTs() - last_load_ts_;
    return delta < 4000000;
}

void StoreSlice::InitKeyCache(CcShard *cc_shard,
                              StoreRange *range,
                              const TableName *tbl_name,
                              NodeGroupId ng_id,
                              int64_t term)
{
    std::lock_guard<std::mutex> lk(slice_mux_);
    if (status_ == SliceStatus::FullyCached && !init_key_cache_cc_)
    {
        // Pin the range so that the StoreRange won't be evicted.
        range->pins_.fetch_add(1, std::memory_order_acquire);
        // Pin the slice so that it won't be kicked out during key cache init.
        pins_++;

        init_key_cache_cc_ = cc_shard->NewInitKeyCacheCc();
        init_key_cache_cc_->Reset(range,
                                  this,
                                  range->local_cc_shards_.Count(),
                                  *tbl_name,
                                  term,
                                  ng_id);

        uint16_t core_cnt = range->local_cc_shards_.Count();
        for (uint16_t core_id = 0; core_id < core_cnt; core_id++)
        {
            Sharder::Instance().GetLocalCcShards()->EnqueueToCcShard(
                core_id, init_key_cache_cc_);
        }
    }
}

StoreRange::LoadSliceController
StoreRange::LoadSliceController::ForceLoadConotroller()
{
    StoreRange::LoadSliceController ctrl;
    ctrl.force_load_ = true;
    ctrl.async_emit_ = false;
    ctrl.submitter_ = nullptr;
    ctrl.executor_ = UINT16_MAX;
    return ctrl;
}

StoreRange::LoadSliceController
StoreRange::LoadSliceController::NonForceLoadController(bool async_emit,
                                                        CcShard *submitter)
{
    StoreRange::LoadSliceController ctrl;
    ctrl.force_load_ = false;
    ctrl.async_emit_ = async_emit;
    ctrl.submitter_ = submitter;
    ctrl.executor_ =
        (async_emit && submitter) ? submitter->core_id_ : UINT16_MAX;
    return ctrl;
}

uint16_t StoreRange::LoadSliceController::NextExecutor() const
{
    assert(force_load_ == false && submitter_ != nullptr);
    if (executor_++ == submitter_->core_cnt_)
    {
        executor_ = 0;
    }
    return executor_;
}

StoreRange::StoreRange(uint32_t partition_id,
                       NodeGroupId range_owner,
                       LocalCcShards &cc_shards,
                       bool init_key_cache,
                       size_t estimate_rec_size,
                       bool has_dml_since_ddl)
    : partition_id_(partition_id),
      cc_ng_id_(range_owner),
      local_cc_shards_(cc_shards),
      last_accessed_ts_(local_cc_shards_.ClockTs()),
      has_dml_since_ddl_(has_dml_since_ddl)
{
    if (init_key_cache && txservice_enable_key_cache)
    {
        size_t key_cache_size;
        if (estimate_rec_size == UINT64_MAX)
        {
            // Assume each record is 200 bytes, calculate the size of the key
            // cache.
            key_cache_size = StoreRange::range_max_size /
                             StoreRange::key_cache_default_load_factor / 200;
        }
        else
        {
            key_cache_size =
                std::max((size_t) 1000,
                         (size_t) (StoreRange::range_max_size /
                                   StoreRange::key_cache_default_load_factor /
                                   estimate_rec_size));
        }

        uint16_t core_cnt = Sharder::Instance().GetLocalCcShardsCount();
        for (uint16_t id = 0; id < core_cnt; id++)
        {
            key_cache_.push_back(
                std::make_unique<cuckoofilter::CuckooFilter<size_t, 12>>(
                    key_cache_size / core_cnt));
        }
    }
    else
    {
        key_cache_.resize(0);
    }
}

RangeSliceOpStatus StoreRange::PinSlice(const TableName &tbl_name,
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
                                        bool force_load,
                                        uint32_t prefetch_size)
{
    // A shared lock on the range to prevent concurrent splitting or merging of
    // slices.
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

    // Only the checkpointer calls this function to pin a slice given the slice
    // Id. The to_alter_ flag is only set by the checkpointer, so this flag must
    // be false here.
    assert(!slice->to_alter_);

    if (slice->status_ == SliceStatus::FullyCached)
    {
        ++slice->pins_;
        pins_.fetch_add(1, std::memory_order_release);
        return RangeSliceOpStatus::Successful;
    }
    else
    {
        RangeSliceOpStatus pin_status;

        LoadSliceController load_ctrl =
            force_load
                ? LoadSliceController::ForceLoadConotroller()
                : LoadSliceController::NonForceLoadController(false, cc_shard);

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
        case LoadSliceStatus::Retry:
            // When load slice from data store, may using the prepapre
            // statement, if the PS is being built, it will return Retry.
            pin_status = RangeSliceOpStatus::Retry;
            break;
        default:
            // This method is only called by the checkpointer, who sets the
            // force_load flag to true. So, LoadSlice() in this method always
            // reads the slice from the data store, even if there is thrashing.
            assert(load_ret == LoadSliceStatus::Error);
            pin_status = RangeSliceOpStatus::Error;
            break;
        }

        if (prefetch_size > 0)
        {
            load_ctrl =
                LoadSliceController::NonForceLoadController(true, cc_shard);

            auto [slice_idx, slice_cnt] = SearchSlice(slice);
            size_t sid = slice_idx + 1;
            for (size_t fid = 0; fid < prefetch_size && sid < slice_cnt;
                 ++fid, ++sid)
            {
                StoreSlice *prefetch_slice = GetSlice(sid);
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
            }
        }

        return pin_status;
    }
}

void StoreRange::UnpinSlice(StoreSlice *slice, bool need_lock_range)
{
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);
    if (slice->pins_ > 0)
    {
        --slice->pins_;
        pins_.fetch_sub(1, std::memory_order_release);
    }

    // The slice is unpinned. If the data sync worker has requested to alter the
    // slice, wakes up the worker thread.
    if (slice->pins_ == 0 && slice->to_alter_)
    {
        // Wake up all waiting threads since there could be multiple slices
        // waiting on the same range wait_cv_.
        if (need_lock_range)
        {
            slice_lk.unlock();
            std::unique_lock<std::shared_mutex> range_lk(mux_);
            wait_cv_.notify_all();
        }
        else
        {
            wait_cv_.notify_all();
        }
    }
}

void StoreRange::BatchUnpinSlices(StoreSlice *start_slice,
                                  const StoreSlice *end_slice,
                                  bool forward_dir)
{
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    auto [slice_idx, slice_cnt] = SearchSlice(start_slice);
    assert(GetSlice(slice_idx) == start_slice);
    StoreSlice *slice = start_slice;
    while (slice != end_slice)
    {
        UnpinSlice(slice, false);
        if (forward_dir)
        {
            slice_idx++;
        }
        else
        {
            slice_idx--;
        }
        assert(slice_idx < slice_cnt);
        slice = GetSlice(slice_idx);
    }
    UnpinSlice(slice, false);
}

bool StoreRange::SampleSubRangeKeys(StoreSlice *slice,
                                    const TableName &table_name,
                                    NodeGroupId ng_id,
                                    int64_t ng_term,
                                    uint64_t data_sync_ts,
                                    size_t key_cnt,
                                    std::vector<TxKey> &new_range_keys)
{
    TxKey start_key = slice->StartTxKey();
    TxKey end_key = slice->EndTxKey();

    SampleSubRangeKeysCc sample_keys_cc(table_name,
                                        ng_id,
                                        ng_term,
                                        data_sync_ts,
                                        &start_key,
                                        &end_key,
                                        key_cnt);

    // Send the request to one shard randomly.
    uint64_t core_rand = butil::fast_rand();
    local_cc_shards_.EnqueueLowPriorityCcRequestToShard(
        core_rand % local_cc_shards_.Count(), &sample_keys_cc);
    DLOG(INFO) << "Send the sample range keys request to shard#"
               << core_rand % local_cc_shards_.Count();

    sample_keys_cc.Wait();
    CcErrorCode res = sample_keys_cc.ErrorCode();
    if (res != CcErrorCode::NO_ERROR)
    {
        LOG(ERROR) << "SampleSlice failed on table: " << table_name.StringView()
                   << " with error code: " << static_cast<uint32_t>(res);
        return false;
    }
    else
    {
        // Get sub-range keys.
        std::vector<TxKey> &target_keys = sample_keys_cc.TargetTxKeys();
        assert(target_keys.size() <= key_cnt);
        // The Txkey in the target_keys vector is just the key pointer.
        for (auto &key : target_keys)
        {
            new_range_keys.emplace_back(key.Clone());
        }
    }

    return true;
}

void StoreRange::UpdateSliceSpec(StoreSlice *slice,
                                 const std::vector<TxKey> &new_range_keys,
                                 size_t first_idx,
                                 size_t subslice_cnt)
{
    uint64_t sub_slice_size = slice->Size() / subslice_cnt;
    uint64_t sub_post_ckpt_size = slice->PostCkptSize() / subslice_cnt;
    std::vector<SliceChangeInfo> split_keys;
    split_keys.reserve(subslice_cnt);

    // The first sub-slice's start key re-uses the old slice's start key.
    split_keys.emplace_back(
        slice->StartTxKey(), sub_slice_size, sub_post_ckpt_size);
    for (size_t i = 0; i < subslice_cnt - 1; ++i)
    {
        split_keys.emplace_back(new_range_keys[first_idx + i].GetShallowCopy(),
                                sub_slice_size,
                                sub_post_ckpt_size);
    }

    // Split this StoreSlice in memory.
    UpdateSliceSpec(slice, split_keys);
}

void StoreRange::UpdateSliceSpec(StoreSlice *slice,
                                 std::vector<SliceChangeInfo> &split_keys)
{
    assert(split_keys.size() > 1);
    std::unique_lock<std::shared_mutex> range_lk(mux_);
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

    assert(!slice->to_alter_);
    slice->to_alter_ = true;

    // Unlocks the slice before checking the slice's pin count. If some tx's are
    // pinning the slice, the calling thread, i.e., the checkpointer, is put
    // into sleep on the condition variable.
    slice_lk.unlock();
    wait_cv_.wait(range_lk,
                  [slice_ptr = slice] { return slice_ptr->ChangeAllowed(); });

    slice_lk.lock();

    std::unique_lock<std::mutex> heap_lk(
        local_cc_shards_.table_ranges_heap_mux_);
    bool is_override_thd = mi_is_override_thread();
    mi_threadid_t prev_thd =
        mi_override_thread(local_cc_shards_.GetTableRangesHeapThreadId());
    mi_heap_t *prev_heap =
        mi_heap_set_default(local_cc_shards_.GetTableRangesHeap());

#if defined(WITH_JEMALLOC)
    uint32_t prev_arena;
    JemallocArenaSwitcher::ReadCurrentArena(prev_arena);
    // override arena id
    auto table_range_arena_id = local_cc_shards_.GetTableRangesArenaId();
    JemallocArenaSwitcher::SwitchToArena(table_range_arena_id);
#endif

    UpdateSlice(slice, split_keys);

    bool range_slice_mem_full = local_cc_shards_.TableRangesMemoryFull();
    mi_heap_set_default(prev_heap);
    if (is_override_thd)
    {
        mi_override_thread(prev_thd);
    }
    else
    {
        mi_restore_default_thread_id();
    }
#if defined(WITH_JEMALLOC)
    JemallocArenaSwitcher::SwitchToArena(prev_arena);
#endif

    heap_lk.unlock();

    slice->to_alter_ = false;
    if (range_slice_mem_full)
    {
        range_lk.unlock();
        slice_lk.unlock();
        local_cc_shards_.KickoutRangeSlices();
    }
}

bool StoreRange::UpdateRangeSlicesInStore(const TableName &table_name,
                                          uint64_t ckpt_ts,
                                          uint64_t range_version,
                                          store::DataStoreHandler *store_hd)
{
    // no range lock is needed since it is updated only by checkpointer.

    return store_hd->UpdateRangeSlices(table_name,
                                       ckpt_ts,
                                       RangeStartTxKey(),
                                       Slices(),
                                       partition_id_,
                                       range_version);
}

bool StoreRange::SetLastInitKeyCacheTs()
{
    // If the range key cache is just initialized in the last 10s, do not try
    // to reinitialize it. The in memory range might be too big to fit into the
    // key cache, in which case we should avoid spamming InitializeKeyCache.
    uint64_t last_ts =
        last_init_key_cache_time_.load(std::memory_order_acquire);
    if (last_ts + 10000000 < local_cc_shards_.ClockTs() &&
        last_init_key_cache_time_.compare_exchange_strong(
            last_ts, local_cc_shards_.ClockTs(), std::memory_order_acq_rel))
    {
        return true;
    }

    // last ts just updated by another thread
    return false;
}

StoreRange::LoadSliceStatus StoreRange::LoadSlice(
    const TableName &tbl_name,
    int64_t ng_term,
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
    std::unique_lock<std::mutex> &&slice_lk)
{
    // The caller of this method has acquired the slice lock on the input
    // mutex.

    if (slice.fetch_slice_cc_ == nullptr)
    {
        if (!ctrl.ForceLoad() && slice.IsRecentLoad())
        {
            return LoadSliceStatus::Delay;
        }
        pins_.fetch_add(1, std::memory_order_release);

        // Calls the data store's async API to load the slice
        // [slice_start, slice_end) into memory.
        slice.fetch_slice_cc_ = cc_shard->NewFillStoreSliceCc();
        slice.fetch_slice_cc_->Reset(tbl_name,
                                     cc_ng_id_,
                                     ng_term,
                                     key_schema,
                                     rec_schema,
                                     schema_ts,
                                     &slice,
                                     this,
                                     ctrl.ForceLoad(),
                                     snapshot_ts,
                                     local_cc_shards_);

        if (cc_request != nullptr)
        {
            slice.cc_queue_.emplace_back(cc_request, cc_shard);
        }

        slice_lk.unlock();

        if (ctrl.AsyncEmit())
        {
            // Prefetching.
            assert(ctrl.ForceLoad() == false);
            assert(cc_request == nullptr);

            auto task = [this, store_hd, &tbl_name, &slice, kv_info](CcShard &)
            {
                slice.fetch_slice_cc_->start_ = metrics::Clock::now();

                // fetch_slice_cc_.load_slice_req_ holds a copy of slice's
                // start_key and slice's end_key. Thus it is safe to execute in
                // a seperate TxProcessor.
                store::DataStoreHandler::DataStoreOpStatus kv_load_status =
                    store_hd->LoadRangeSlice(tbl_name,
                                             kv_info,
                                             partition_id_,
                                             slice.fetch_slice_cc_);

                std::unique_lock<std::mutex> lk(slice.slice_mux_,
                                                std::defer_lock);
                switch (kv_load_status)
                {
                case store::DataStoreHandler::DataStoreOpStatus::Success:
                    break;
                case store::DataStoreHandler::DataStoreOpStatus::Retry:
                    // Put the ccrequests back to txprocessor queue.
                    lk.lock();
                    for (auto [cc_req, cc_shard] : slice.cc_queue_)
                    {
                        cc_shard->Enqueue(cc_req);
                    }
                    slice.cc_queue_.clear();
                    slice.fetch_slice_cc_->Free();
                    slice.fetch_slice_cc_ = nullptr;
                    pins_.fetch_sub(1, std::memory_order_release);
                    break;
                default:
                    // Abort those ccrequests.
                    //
                    // We need to make sure that the CcMap::Execute(CcRequest )
                    // and CcRequest::ABortCcRequest(...) functions occur on the
                    // same thread. Otherwise, AbortCcRequest is not safe
                    // behavior.
                    std::unordered_map<CcShard *, std::vector<CcRequestBase *>>
                        waiting_reqs;
                    lk.lock();
                    for (auto [cc_req, cc_shard] : slice.cc_queue_)
                    {
                        waiting_reqs[cc_shard].push_back(cc_req);
                    }

                    for (auto &[cc_shard, reqs] : waiting_reqs)
                    {
                        cc_shard->AbortCcRequests(std::move(reqs),
                                                  CcErrorCode::DATA_STORE_ERR);
                    }
                    slice.cc_queue_.clear();
                    slice.fetch_slice_cc_->Free();
                    slice.fetch_slice_cc_ = nullptr;
                    pins_.fetch_sub(1, std::memory_order_release);
                    break;
                }

                return true;
            };

            cc_shard->DispatchTask(ctrl.NextExecutor(), std::move(task));

            return LoadSliceStatus::Success;
        }
        else
        {
            slice.fetch_slice_cc_->start_ = metrics::Clock::now();

            store::DataStoreHandler::DataStoreOpStatus kv_load_status =
                store_hd->LoadRangeSlice(
                    tbl_name, kv_info, partition_id_, slice.fetch_slice_cc_);

            // By the time LoadRangeSlice() returns, the slice's status is
            // either BeingLoaded or PartiallyCached. This is because this
            // method is called by PinSlice(), which is called by one of tx
            // processors who will process the fill slice cc request. Hence,
            // loading slice into memory cannot complete at this point.

            switch (kv_load_status)
            {
            case store::DataStoreHandler::DataStoreOpStatus::Success:
                return LoadSliceStatus::Success;
            case store::DataStoreHandler::DataStoreOpStatus::Retry:
                // Put the ccrequests back to txprocessor queue except the first
                // one which will be put back to txprocessor queue by the
                // caller.
                slice_lk.lock();
                for (size_t i = 1; i < slice.cc_queue_.size(); ++i)
                {
                    auto *cc_req = std::get<0>(slice.cc_queue_[i]);
                    auto *cc_shard = std::get<1>(slice.cc_queue_[i]);
                    cc_shard->Enqueue(cc_req);
                }
                slice.cc_queue_.clear();
                slice.fetch_slice_cc_->Free();
                slice.fetch_slice_cc_ = nullptr;
                pins_.fetch_sub(1, std::memory_order_release);
                return LoadSliceStatus::Retry;
            default:
                // Abort those ccrequests except the first one which will be
                // aborted by the caller. We need to make sure that the
                // CcMap::Execute(CcRequest ) and CcRequest::ABortCcRequest(...)
                // functions occur on the same thread. Otherwise, AbortCcRequest
                // is not safe behavior.
                std::unordered_map<CcShard *, std::vector<CcRequestBase *>>
                    waiting_reqs;
                slice_lk.lock();
                for (size_t i = 1; i < slice.cc_queue_.size(); ++i)
                {
                    auto *cc_req = std::get<0>(slice.cc_queue_[i]);
                    auto *cc_shard = std::get<1>(slice.cc_queue_[i]);
                    waiting_reqs[cc_shard].push_back(cc_req);
                }

                for (auto &[cc_shard, reqs] : waiting_reqs)
                {
                    cc_shard->AbortCcRequests(std::move(reqs),
                                              CcErrorCode::DATA_STORE_ERR);
                }
                slice.cc_queue_.clear();
                slice.fetch_slice_cc_->Free();
                slice.fetch_slice_cc_ = nullptr;
                pins_.fetch_sub(1, std::memory_order_release);
                return LoadSliceStatus::Error;
            }
        }
    }
    else
    {
        if (ctrl.ForceLoad() && !slice.fetch_slice_cc_->ForceLoad() &&
            slice.status_ != SliceStatus::BeingLoaded)
        {
            // If the demanding request sets the force_load flag and the
            // fetching request does not, the demanding request is allowed
            // to change the flag if filling into memory has not started.
            slice.fetch_slice_cc_->SetForceLoad(true);
        }
        else if (ctrl.ForceLoad() && !slice.fetch_slice_cc_->ForceLoad())
        {
            // Retry this request whose force_load flag is true, rather than put
            // it into ccrequest queue. If OOM, the queued request will be
            // aborted. Setting force load to true means that requests will
            // ignore OOM errors, such as DataSyncScanCc.
            return LoadSliceStatus::Retry;
        }

        if (cc_request != nullptr)
        {
            slice.cc_queue_.emplace_back(cc_request, cc_shard);
        }

        return LoadSliceStatus::Success;
    }
}

}  // namespace txservice
