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

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog_factory.h"  //TableSchema
#include "cc/cc_entry.h"      // LruEntry
#include "cc_req_base.h"
#include "error_messages.h"
// #include "range_slice.h"
#include "range_slice_type.h"
#include "slice_data_item.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service_metrics.h"
#include "type.h"

namespace txservice
{
class CcMap;
class CcShard;
class LocalCcShards;
class StoreSlice;
class StoreRange;
struct RangeSliceId;
struct InitRangeEntry;
struct TableRangeEntry;
struct SliceChangeInfo;
namespace store
{
class DataStoreHandler;
};

struct FetchCc : public CcRequestBase
{
public:
    virtual ~FetchCc() = default;
    void AddRequester(CcRequestBase *requester);
    size_t RequesterCount() const;
    NodeGroupId GetNodeGroupId() const;
    int64_t LeaderTerm() const;
    metrics::TimePoint start_;

protected:
    FetchCc(CcShard &ccs, NodeGroupId cc_ng_id, int64_t cc_ng_term);

    std::vector<CcRequestBase *> requesters_;
    CcShard &ccs_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
};

struct FetchCatalogCc : public FetchCc
{
public:
    FetchCatalogCc() = delete;
    FetchCatalogCc(const TableName &table_name,
                   CcShard &ccs,
                   NodeGroupId cc_ng_id,
                   int64_t cc_ng_term,
                   bool fetch_from_primary = false);
    ~FetchCatalogCc() = default;

    bool ValidTermCheck();

    bool Execute(CcShard &ccs) override;

    std::string &CatalogImage()
    {
        return catalog_image_;
    }

    void SetCommitTs(uint64_t commit_ts)
    {
        commit_ts_ = commit_ts;
    }

    uint64_t &CommitTs()
    {
        return commit_ts_;
    }

    const TableName &CatalogName() const
    {
        return table_name_;
    }

    void SetFinish(RecordStatus status, int err);

private:
    const TableName table_name_;
    std::string catalog_image_;
    uint64_t commit_ts_;
    RecordStatus status_;
    int error_code_{0};
    bool fetch_from_primary_{false};
};

struct FetchTableStatisticsCc : public FetchCc
{
public:
    FetchTableStatisticsCc() = delete;
    FetchTableStatisticsCc(const TableName &table_name,
                           CcShard &ccs,
                           NodeGroupId cc_ng_id,
                           int64_t cc_ng_term);
    ~FetchTableStatisticsCc() = default;

    bool Execute(CcShard &ccs) override;

    const TableName &CatalogName() const
    {
        return table_name_;
    }

    void SetCurrentVersion(uint64_t current_version)
    {
        current_version_ = current_version;
    }

    uint64_t CurrentVersion() const
    {
        return current_version_;
    }

    void SamplePoolMergeFrom(const TableName &table_or_index_name,
                             std::vector<TxKey> &&samplekeys)
    {
        for (TxKey &samplekey : samplekeys)
        {
            sample_pool_map_[table_or_index_name].second.emplace_back(
                std::move(samplekey));
        }
    }

    void SetRecords(const TableName &table_or_index_name, uint64_t records)
    {
        sample_pool_map_[table_or_index_name].first = records;
    }

    void SetStoreHandler(store::DataStoreHandler *store_hd)
    {
        store_hd_ = store_hd;
    }

    store::DataStoreHandler *StoreHandler()
    {
        return store_hd_;
    }

    void SetFinish(int err);

private:
    const TableName table_name_;
    store::DataStoreHandler *store_hd_{nullptr};
    uint64_t current_version_{0};
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
        sample_pool_map_;
    int error_code_{0};
};

struct FetchTableRangesCc : public FetchCc
{
public:
    FetchTableRangesCc(const TableName &table_name,
                       CcShard &ccs,
                       NodeGroupId cc_ng_id,
                       int64_t cc_ng_term);

    bool Execute(CcShard &ccs) override;
    void AppendTableRanges(std::vector<InitRangeEntry> &&ranges);
    void AppendTableRange(InitRangeEntry &&range);

    bool EmptyRanges() const;
    void SetFinish(int err);

public:
    const TableName table_name_;
    int error_code_{0};
    std::vector<InitRangeEntry> ranges_vec_;
};

struct FetchRangeSlicesReq
{
public:
    FetchRangeSlicesReq(const TableName &table_name,
                        TableRangeEntry *range_entry,
                        NodeGroupId ng_id,
                        int64_t cc_ng_term)
        : table_name_(table_name),
          cc_ng_id_(ng_id),
          cc_ng_term_(cc_ng_term),
          range_entry_(range_entry)
    {
    }

    void SetFinish(CcErrorCode err);
    void AddRequester(CcRequestBase *requester, CcShard *ccs)
    {
        requesters_.emplace_back(requester, ccs);
    }
    size_t RequesterCount() const
    {
        return requesters_.size();
    }

    void SetSliceVersion(uint64_t slice_version)
    {
        slice_version_ = slice_version;
    }

    uint64_t SliceVersion()
    {
        return slice_version_;
    }

    void SetSegmentCnt(uint64_t segment_cnt)
    {
        segment_cnt_ = segment_cnt;
    }

    uint64_t SegmentCnt()
    {
        return segment_cnt_;
    }

    void SetCurrentSegmentId(uint64_t segment_id)
    {
        segment_id_ = segment_id;
    }

    uint64_t CurrentSegmentId() const
    {
        return segment_id_;
    }

    const TableName table_name_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    TableRangeEntry *range_entry_;
    std::vector<std::pair<CcRequestBase *, CcShard *>> requesters_;
    std::vector<SliceInitInfo> slice_info_;
    uint64_t slice_version_{0};
    uint64_t segment_cnt_{0};
    uint64_t segment_id_{0};
};

/**
 * @brief The request sent by a cc node when the cc node steps down as the
 * leader of its node group, so as to clear cc maps associated with the cc node
 * group at this node.
 *
 */
struct ClearCcNodeGroup : public CcRequestBase
{
public:
    ClearCcNodeGroup(uint32_t cc_ng_id, uint16_t core_cnt)
        : cc_ng_id_(cc_ng_id), core_cnt_(core_cnt)
    {
    }

    ClearCcNodeGroup() = delete;
    ClearCcNodeGroup(const ClearCcNodeGroup &) = delete;

    bool Execute(CcShard &ccs) override;

    void Wait()
    {
        std::unique_lock lk(mux_);
        while (finish_cnt_ != core_cnt_)
        {
            wait_cv_.wait(lk);
        }
    }

private:
    const uint32_t cc_ng_id_;
    const uint16_t core_cnt_;
    uint16_t finish_cnt_{0};
    // ClearCcNodeGroup is issued and Waited on RPC handler bthread, use bthread
    // mutex and condition variable.
    bthread::Mutex mux_;
    bthread::ConditionVariable wait_cv_;
};

struct FillStoreSliceCc;

struct LoadRangeSliceRequest

{
public:
    LoadRangeSliceRequest() = delete;

    LoadRangeSliceRequest(const TableName &tbl_name,
                          const KeySchema *key_schema,
                          const RecordSchema *rec_schema,
                          uint64_t schema_ts,
                          TxKey start_key,
                          TxKey end_key,
                          uint64_t snapshot_ts,
                          NodeGroupId cc_ng_id,
                          int64_t cc_ng_term)
        : table_name_(&tbl_name),
          key_schema_(key_schema),
          rec_schema_(rec_schema),
          schema_ts_(schema_ts),
          start_key_(std::move(start_key)),
          end_key_(std::move(end_key)),
          snapshot_ts_(snapshot_ts),
          slice_size_(0),
          rec_cnt_(0),
          cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term)
    {
    }

    void Reset()
    {
        failed_ = false;
        slice_size_ = 0;
        rec_cnt_ = 0;
        slice_data_.clear();
    }

    void AddDataItem(txservice::TxKey key,
                     std::unique_ptr<txservice::TxRecord> &&record,
                     uint64_t version_ts,
                     bool is_deleted)
    {
        slice_size_ += key.Size();
        slice_size_ += record->Size();
        slice_data_.emplace_back(
            std::move(key), std::move(record), version_ts, is_deleted);
        if (!is_deleted)
        {
            rec_cnt_++;
        }
    }

    std::deque<SliceDataItem> &SliceData()
    {
        return slice_data_;
    }

    void SetFinish();
    void SetError();

    const TableName &TblName() const
    {
        return *table_name_;
    }

    const KeySchema *GetKeySchema() const
    {
        return key_schema_;
    }

    const RecordSchema *GetRecordSchema() const
    {
        return rec_schema_;
    }

    uint64_t SchemaTs() const
    {
        return schema_ts_;
    }

    const TxKey &StartKey() const
    {
        return start_key_;
    }

    const TxKey &EndKey() const
    {
        return end_key_;
    }

    uint32_t SliceSize() const
    {
        return slice_size_;
    }

    uint64_t SnapshotTs() const
    {
        return snapshot_ts_;
    }

    NodeGroupId GetNodeGroupId() const
    {
        return cc_ng_id_;
    }

    int64_t LeaderTerm() const
    {
        return cc_ng_term_;
    }

    bool IsError() const
    {
        return failed_;
    }

    size_t RecordCnt() const
    {
        return rec_cnt_;
    }

    std::function<void(LoadRangeSliceRequest *)> post_lambda_;
    metrics::TimePoint start_;

private:
    const TableName *table_name_;

    std::deque<SliceDataItem> slice_data_;
    const KeySchema *key_schema_;
    const RecordSchema *rec_schema_;
    const uint64_t schema_ts_;
    TxKey start_key_;
    TxKey end_key_;
    uint64_t snapshot_ts_;
    uint32_t slice_size_;
    uint32_t rec_cnt_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    bool failed_{false};
};

struct InitKeyCacheCc : public CcRequestBase
{
public:
    static constexpr size_t MaxScanBatchSize = 64;
    InitKeyCacheCc() = delete;
    InitKeyCacheCc(StoreRange *range,
                   StoreSlice *slice,
                   uint16_t core_cnt,
                   const TableName *tbl_name,
                   int64_t term,
                   NodeGroupId ng_id)
        : tbl_name_(
              tbl_name->String(),
              TableType::Primary),  // key cache is only used on primary table
          term_(term),
          ng_id_(ng_id),
          range_(range),
          slice_(slice),
          unfinished_cnt_(core_cnt)
    {
        assert(tbl_name->IsBase());
        pause_pos_.resize(core_cnt);
    }
    bool Execute(CcShard &ccs) override;
    void SetFinish(uint16_t core, bool succ);
    StoreSlice &Slice();
    StoreRange &Range();
    void SetPauseKey(TxKey &key, uint16_t core_id);
    TxKey &PauseKey(uint16_t core_id);

private:
    const TableName tbl_name_;
    int64_t term_;
    NodeGroupId ng_id_;
    StoreRange *range_;
    StoreSlice *slice_;
    std::atomic<uint16_t> unfinished_cnt_{0};
    std::vector<TxKey> pause_pos_;
};

struct FillStoreSliceCc : public CcRequestBase
{
public:
    static constexpr size_t MaxScanBatchSize = 64;

    FillStoreSliceCc(const TableName &table_name,
                     NodeGroupId cc_ng_id,
                     int64_t cc_ng_term,
                     const KeySchema *key_schema,
                     const RecordSchema *rec_schema,
                     uint64_t schema_ts,
                     StoreSlice &slice,
                     StoreRange &range,
                     bool force_load,
                     uint64_t snapshot_ts,
                     LocalCcShards &cc_shards);

    ~FillStoreSliceCc() = default;

    bool Execute(CcShard &ccs) override;

    std::deque<SliceDataItem> &SliceData(uint16_t core_id)
    {
        assert(core_id < partitioned_slice_data_.size());
        return partitioned_slice_data_[core_id];
    }

    void AddDataItem(TxKey key,
                     std::unique_ptr<txservice::TxRecord> &&record,
                     uint64_t version_ts,
                     bool is_deleted);

    void SetFinish();
    void SetError(CcErrorCode err_code);

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        DLOG(ERROR) << "Abort this FillStoreSliceCc request with error: "
                    << CcErrorMessage(err_code);
        SetError(err_code);
    }

    const TableName &TblName() const
    {
        return *table_name_;
    }

    void StartFilling();
    void TerminateFilling();

    StoreRange &Range()
    {
        return range_;
    }

    StoreSlice &Slice()
    {
        return range_slice_;
    }

    LoadRangeSliceRequest *LoadRequest()
    {
        return &load_slice_req_;
    }

    bool ForceLoad()
    {
        std::unique_lock<std::mutex> lk(mux_);
        return force_load_;
    }

    void SetForceLoad(bool force_load)
    {
        std::unique_lock<std::mutex> lk(mux_);
        force_load_ = force_load;
    }

    size_t NextIndex(size_t core_idx) const
    {
        size_t next_idx = next_idxs_[core_idx];
        assert(next_idx <= partitioned_slice_data_[core_idx].size());
        return next_idx;
    }

    void SetNextIndex(size_t core_idx, size_t index)
    {
        assert(index <= partitioned_slice_data_[core_idx].size());
        next_idxs_[core_idx] = index;
    }

    NodeGroupId NodeGroup() const
    {
        return cc_ng_id_;
    }

    int64_t Term() const
    {
        return cc_ng_term_;
    }

private:
    const TableName *table_name_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    bool force_load_;
    uint16_t finish_cnt_;
    std::mutex mux_;
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};

    std::vector<size_t> next_idxs_;
    std::vector<std::deque<SliceDataItem>> partitioned_slice_data_;
    LoadRangeSliceRequest load_slice_req_;

    StoreSlice &range_slice_;
    StoreRange &range_;
    LocalCcShards &local_cc_shards_;
};

struct FetchRecordCc : public FetchCc
{
public:
    FetchRecordCc() = delete;
    FetchRecordCc(const TableName *tbl_name,
                  const TableSchema *tbl_schema,
                  TxKey tx_key,
                  LruEntry *cce,
                  CcMap *ccm,
                  CcShard &ccs,
                  NodeGroupId cc_ng_id,
                  int64_t cc_ng_term,
                  int32_t range_id_ = -1,
                  bool fetch_from_primary = false);
    ~FetchRecordCc() = default;

    bool ValidTermCheck();

    bool Execute(CcShard &ccs) override;

    void SetFinish(int err);

    static uint64_t GetFetchRecordTxNumber(uint32_t node_id)
    {
        // Use a special local tx num for fetch record read intent lock
        return ((uint64_t) node_id << 10) << 32L | UINT32_MAX;
    }

    uint16_t &RetryCnt()
    {
        return retry_cnt_;
    }

    const TableName *table_name_{nullptr};
    const TableSchema *table_schema_{nullptr};
    TxKey tx_key_;
    LruEntry *cce_{nullptr};
    CcMap *ccm_;
    uint64_t rec_ts_{0};
    RecordStatus rec_status_{RecordStatus::Unknown};
    std::string rec_str_;
    int error_code_{0};
    // Only used in range partition
    int range_id_;
    bool fetch_from_primary_{false};
    // Only used for fetch record from primary
    int64_t standby_term_;
    std::function<void(CcShard &)> handle_resp_;
    uint16_t retry_cnt_{0};
};

struct RunOnTxProcessorCc : public CcRequestBase
{
public:
    explicit RunOnTxProcessorCc(std::function<bool(CcShard &ccs)> task = {})
        : task_(std::move(task))
    {
    }

    void Reset(std::function<bool(CcShard &ccs)> task)
    {
        task_ = std::move(task);
    }

    bool Execute(CcShard &ccs) override;

private:
    std::function<bool(CcShard &ccs)> task_;
};

struct WaitableCc : public RunOnTxProcessorCc
{
public:
    explicit WaitableCc(std::function<bool(CcShard &ccs)> task = {},
                        uint32_t core_cnt = 1)
        : RunOnTxProcessorCc(std::move(task)),
          unfinished_cnt_(core_cnt),
          error_code_(CcErrorCode::NO_ERROR)
    {
    }

    void Reset(std::function<bool(CcShard &ccs)> task = {},
               uint16_t core_cnt = 1)
    {
        std::lock_guard<bthread::Mutex> lk(mux_);
        RunOnTxProcessorCc::Reset(std::move(task));

        unfinished_cnt_ = core_cnt;
        error_code_ = CcErrorCode::NO_ERROR;
    }

    void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        while (unfinished_cnt_)
        {
            cv_.wait(lk);
        }
    }

    bool IsFinished() const
    {
        std::lock_guard<bthread::Mutex> lk(mux_);
        return unfinished_cnt_ == 0;
    }

    bool IsError() const
    {
        std::lock_guard<bthread::Mutex> lk(mux_);
        return error_code_ != CcErrorCode::NO_ERROR;
    }

    CcErrorCode ErrorCode() const
    {
        std::lock_guard<bthread::Mutex> lk(mux_);
        return error_code_;
    }

    void AbortCcRequest(CcErrorCode error_code) override
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        unfinished_cnt_--;
        error_code_ = error_code;
        if (unfinished_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    bool Execute(CcShard &ccs) override
    {
        if (RunOnTxProcessorCc::Execute(ccs))
        {
            std::unique_lock<bthread::Mutex> lk(mux_);
            error_code_ = CcErrorCode::NO_ERROR;
            if (--unfinished_cnt_ == 0)
            {
                cv_.notify_one();
            }
        }
        return false;
    }

private:
    void *operator new(size_t) noexcept
    {
        return nullptr;
    }

    void operator delete(void *)
    {
    }

private:
    mutable bthread::Mutex mux_;
    bthread::ConditionVariable cv_;

    uint32_t unfinished_cnt_{0};
    CcErrorCode error_code_;
};
struct UpdateCceCkptTsCc : public CcRequestBase
{
public:
#ifdef RANGE_PARTITION_ENABLED
    static constexpr size_t SCAN_BATCH_SIZE = 1024;
#else
    static constexpr size_t SCAN_BATCH_SIZE = 64;
#endif

#ifdef RANGE_PARTITION_ENABLED
    UpdateCceCkptTsCc(
        std::vector<std::vector<FlushRecord *>> &&flush_records_per_core,
        size_t core_cnt,
        NodeGroupId node_group,
        int64_t term)
        : flush_records_per_core_(std::move(flush_records_per_core)),
          unfinished_core_cnt_(core_cnt),
          node_group_(node_group),
          term_(term)
    {
        idxs_.resize(core_cnt, 0);
    }
#else
    UpdateCceCkptTsCc(std::vector<FlushRecord> *flush_records,
                      size_t core_cnt,
                      NodeGroupId node_group,
                      int64_t term)
        : flush_records_(flush_records),
          idx_(0),
          unfinished_core_cnt_(core_cnt),
          node_group_(node_group),
          term_(term)
    {
    }

#endif

    UpdateCceCkptTsCc(const UpdateCceCkptTsCc &) = delete;
    UpdateCceCkptTsCc &operator=(const UpdateCceCkptTsCc &) = delete;

    bool Execute(CcShard &ccs) override;

    void SetFinished(CcErrorCode error_code)
    {
        std::lock_guard<std::mutex> lk(mux_);
        unfinished_core_cnt_--;
        if (unfinished_core_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    bool IsError()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return error_code_ != CcErrorCode::NO_ERROR;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [&]() { return unfinished_core_cnt_ == 0; });
    }

private:
#ifdef RANGE_PARTITION_ENABLED
    std::vector<std::vector<FlushRecord *>> flush_records_per_core_;
    std::vector<size_t> idxs_;
#else
    std::vector<FlushRecord> *flush_records_{nullptr};
    size_t idx_{0};
#endif
    size_t unfinished_core_cnt_;
    NodeGroupId node_group_;
    int64_t term_;
    std::mutex mux_;
    std::condition_variable cv_;
    CcErrorCode error_code_{CcErrorCode::NO_ERROR};
};

struct WaitNoNakedBucketRefCc : public CcRequestBase
{
public:
    WaitNoNakedBucketRefCc() : mutex_(), cv_(), finish_(false)
    {
    }

    WaitNoNakedBucketRefCc(const WaitNoNakedBucketRefCc &) = delete;
    WaitNoNakedBucketRefCc(WaitNoNakedBucketRefCc &&) = delete;

    bool Execute(CcShard &ccs) override;

    void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(mutex_);
        while (!finish_)
        {
            cv_.wait(lk);
        }
    }

private:
    bthread::Mutex mutex_;
    bthread::ConditionVariable cv_;
    bool finish_{false};
};

/**
 * Restore CcMap with data from KV
 */
struct RestoreCcMapCc : public CcRequestBase
{
public:
    RestoreCcMapCc();

    void Reset(const TableName *table_name,
               uint32_t cc_group_id,
               int64_t cc_group_term,
               const uint16_t core_cnt,
               std::atomic<CcErrorCode> *cancel_data_loading_on_error);

    bool Execute(CcShard &ccs) override;

    std::deque<SliceDataItem> &DecodedSliceData(uint16_t core_id);
    std::deque<RawSliceDataItem> &SliceData(uint16_t core_id);

    void AddDataItem(uint16_t core_id,
                     std::string &&key_str,
                     std::string &&rec_str,
                     uint64_t version_ts,
                     bool is_deleted);

    void DecodedDataItem(uint16_t core_id,
                         TxKey &&key,
                         std::unique_ptr<txservice::TxRecord> &&record,
                         uint64_t version_ts,
                         bool is_deleted);

    void SetFinished(CcErrorCode error_code = CcErrorCode::NO_ERROR);

    size_t NextIndex(size_t core_idx) const
    {
        size_t next_idx = next_idxs_[core_idx];
        if (data_item_decoded_[core_idx] == 0)
        {
            assert(next_idx <= slice_data_[core_idx].size());
        }
        else
        {
            assert(next_idx <= decoded_slice_data_[core_idx].size());
        }
        return next_idx;
    }

    void SetNextIndex(size_t core_idx, size_t index)
    {
        if (data_item_decoded_[core_idx] == 0)
        {
            assert(index <= slice_data_[core_idx].size());
        }
        else
        {
            assert(index <= decoded_slice_data_[core_idx].size());
        }

        next_idxs_[core_idx] = index;
    }

    const TableName *table_name_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    uint16_t core_cnt_{0};
    uint16_t finished_cnt_{0};

    std::vector<std::deque<RawSliceDataItem>> slice_data_;
    std::vector<std::deque<SliceDataItem>> decoded_slice_data_;
    std::vector<size_t> next_idxs_;
    std::atomic<CcErrorCode> *cancel_data_loading_on_error_;

    std::vector<size_t> data_item_decoded_{};
    CcErrorCode error_code_{CcErrorCode::NO_ERROR};
    size_t total_cnt_{0};
    bthread::Mutex req_mux_{};
};

}  // namespace txservice
