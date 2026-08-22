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

#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "catalog_factory.h"  //TableSchema
#include "cc/cc_entry.h"      // LruEntry
#include "cc_req_base.h"
#include "error_messages.h"
// #include "range_slice.h"
#include <absl/container/flat_hash_map.h>

#include "metrics.h"
#include "range_slice_type.h"
#include "schema.h"
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
    FetchCc() = default;
    FetchCc(CcShard &ccs, NodeGroupId cc_ng_id, int64_t cc_ng_term);

    std::vector<CcRequestBase *> requesters_;
    CcShard *ccs_{nullptr};
    NodeGroupId cc_ng_id_{0};
    int64_t cc_ng_term_{-1};
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

public:
    // Table name with engine prefix, only used in DataStoreHandler.
    std::string kv_key_;

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

public:
    // These variables only be used in DataStoreHandler
    std::string kv_start_key_;
    std::string kv_end_key_;
    std::string kv_session_id_;
    int32_t kv_partition_id_{0};
};

struct FetchTableRangesCc : public FetchCc
{
public:
    FetchTableRangesCc(const TableName &table_name,
                       CcShard &ccs,
                       NodeGroupId cc_ng_id,
                       int64_t cc_ng_term);

    bool Execute(CcShard &ccs) override;
    void AppendTableRanges(int32_t kv_part_id,
                           std::vector<InitRangeEntry> &&ranges);
    void AppendTableRange(int32_t kv_part_id, InitRangeEntry &&range);

    bool EmptyRanges() const;
    void SetFinish(int err);
    void Merge();

public:
    struct PartitionScanState
    {
        std::string kv_start_key_;
        std::string kv_end_key_;
        std::string kv_session_id_;
    };

    const TableName table_name_;
    int error_code_{0};
    std::vector<InitRangeEntry> ranges_vec_;
    std::vector<std::vector<InitRangeEntry>> partition_ranges_vec_;

    // These variables only be used in DataStoreHandler
    // Per-partition scan states used for concurrent FetchTableRanges
    std::vector<PartitionScanState> partition_scan_states_;
    int32_t remaining_partitions_{0};
    // Protects error_code_ and remaining_partitions_ when completing from
    // callbacks
    mutable bthread::Mutex finish_mux_;
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

    // These variables only be used in DataStoreHandler
    std::string kv_start_key_;
    int32_t kv_partition_id_{0};
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

    // ClearCcNodeGroup is issued and Waited on RPC handler bthread, while
    // Execute() runs on tx processors (the brpc worker main stack), so the
    // two sides must not share a bthread::Mutex. Wait() polls an atomic
    // flag instead.
    void Wait();

private:
    const uint32_t cc_ng_id_;
    const uint16_t core_cnt_;
    std::atomic<uint16_t> finish_cnt_{0};
    // Set by the last core after it has dropped the node group's table
    // statistics, catalogs, ranges and bucket info.
    std::atomic<bool> done_{false};
};

struct FillStoreSliceCc;

struct InitKeyCacheCc : public CcRequestBase
{
public:
    static constexpr size_t MaxScanBatchSize = 64;

    InitKeyCacheCc() = default;

    void Reset(StoreRange *range,
               StoreSlice *slice,
               const TableName &tbl_name,
               int64_t term,
               NodeGroupId ng_id)
    {
        assert(tbl_name.IsBase());
        // key cache is only used on primary table
        tbl_name_ =
            TableName(tbl_name.String(), TableType::Primary, tbl_name.Engine());
        term_ = term;
        ng_id_ = ng_id;
        range_ = range;
        slice_ = slice;
        pause_pos_ = TxKey();
    }

    bool Execute(CcShard &ccs) override;
    void SetFinish(bool succ);
    StoreSlice &Slice();
    StoreRange &Range();
    void SetPauseKey(TxKey &key);
    TxKey &PauseKey();

private:
    TableName tbl_name_{std::string(""), TableType::Primary, TableEngine::None};
    int64_t term_;
    NodeGroupId ng_id_;
    StoreRange *range_;
    StoreSlice *slice_;
    TxKey pause_pos_;
};

struct FillStoreSliceCc : public CcRequestBase
{
public:
    static constexpr size_t MaxScanBatchSize = 64;

    FillStoreSliceCc() = default;

    void Reset(const TableName &table_name,
               NodeGroupId cc_ng_id,
               int64_t cc_ng_term,
               const KeySchema *key_schema,
               const RecordSchema *rec_schema,
               uint64_t schema_ts,
               StoreSlice *slice,
               StoreRange *range,
               bool force_load,
               uint64_t snapshot_ts,
               LocalCcShards &cc_shards);

    ~FillStoreSliceCc() = default;

    bool Execute(CcShard &ccs) override;

    std::deque<SliceDataItem> &SliceData()
    {
        return slice_data_;
    }

    void AddDataItem(TxKey key,
                     std::unique_ptr<txservice::TxRecord> &&record,
                     uint64_t version_ts,
                     bool is_deleted);

    void SetFinish(CcShard *cc_shard);
    void SetError(CcErrorCode err_code);

    void SetKvFinish(bool success);

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        DLOG(ERROR) << "Abort this FillStoreSliceCc request with error: "
                    << CcErrorMessage(err_code);
        SetError(err_code);
        // Recycle request
        Free();
    }

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

    void StartFilling();
    void TerminateFilling();

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

    size_t NextIndex() const
    {
        assert(next_idx_ <= slice_data_.size());
        return next_idx_;
    }

    void SetNextIndex(size_t index)
    {
        assert(index <= slice_data_.size());
        next_idx_ = index;
    }

    NodeGroupId NodeGroup() const
    {
        return cc_ng_id_;
    }

    int64_t Term() const
    {
        return cc_ng_term_;
    }

    uint64_t SnapshotTs() const
    {
        return snapshot_ts_;
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

    bool AbortIfOom() const override
    {
        return true;
    }

    int32_t PartitionId() const;

    metrics::TimePoint start_;

private:
    const TableName *table_name_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    bool force_load_;
    std::mutex mux_;
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};

    size_t next_idx_;
    std::deque<SliceDataItem> slice_data_;

    StoreSlice *range_slice_ = nullptr;
    StoreRange *range_ = nullptr;

    const KeySchema *key_schema_;
    const RecordSchema *rec_schema_;
    TxKey start_key_;
    TxKey end_key_;
    uint64_t schema_ts_;
    uint64_t snapshot_ts_;
    uint32_t slice_size_{0};
    uint32_t rec_cnt_{0};

public:
    // These variables only be used in DataStoreHandler
    const std::string *kv_table_name_{nullptr};
    TxKey kv_start_key_owner_;
    std::string_view kv_start_key_;
    std::string_view kv_end_key_;
    std::string kv_session_id_;
    int32_t kv_partition_id_{0};
};

struct FetchRecordCc : public FetchCc
{
public:
    FetchRecordCc() = default;
    FetchRecordCc(const TableName *tbl_name,
                  const TableSchema *tbl_schema,
                  TxKey tx_key,
                  LruEntry *cce,
                  CcShard &ccs,
                  NodeGroupId cc_ng_id,
                  int64_t cc_ng_term,
                  int32_t partition_id,
                  bool fetch_from_primary = false,
                  uint64_t snapshot_read_ts = 0,
                  bool only_fetch_archives = false,
                  bool reopen = false);
    ~FetchRecordCc() = default;

    void Reset(const TableName *tbl_name,
               const TableSchema *tbl_schema,
               TxKey tx_key,
               LruEntry *cce,
               CcShard &ccs,
               NodeGroupId cc_ng_id,
               int64_t cc_ng_term,
               int32_t partition_id,
               bool fetch_from_primary = false,
               uint64_t snapshot_read_ts = 0,
               bool only_fetch_archives = false,
               bool reopen = false);

    bool ValidTermCheck();

    bool Execute(CcShard &ccs) override;

    void SetFinish(int err);

    // table_name is a string view, cannot access it outside TxProcessor.
    TableName table_name_{
        std::string(""), TableType::Primary, TableEngine::None};
    const TableSchema *table_schema_{nullptr};
    std::string kv_table_name_;
    TxKey tx_key_;
    LruEntry *cce_{nullptr};
    KeyGapLockAndExtraData *lock_{nullptr};
    uint64_t rec_ts_{0};
    // If set snapshot_read_ts_ (not equal 0), the snapshot_read_ts_ will be
    // used to fetch record from archives table.
    uint64_t snapshot_read_ts_{0};
    std::unique_ptr<
        std::vector<std::tuple<uint64_t, RecordStatus, std::string>>>
        archive_records_{nullptr};

    std::string rec_str_;

    // These variables only be used in DataStoreHandler
    std::string kv_session_id_;
    std::string kv_start_key_;
    std::string kv_end_key_;

    int error_code_{0};
    int partition_id_{0};
    RecordStatus rec_status_{RecordStatus::Unknown};
    bool fetch_from_primary_{false};
    // If set only_fetch_archives_ (true), don't fetch record from base table.
    bool only_fetch_archives_{false};
    bool reopen_{false};
};

struct FetchBucketDataCc;
typedef void (*OnFetchedBucketData)(FetchBucketDataCc *fetch_cc,
                                    CcRequestBase *requester);

struct FetchBucketDataCc : public CcRequestBase
{
public:
    FetchBucketDataCc() = default;

    ~FetchBucketDataCc() = default;

    void Reset(const TableName *table_name,
               const TableSchema *table_schema,
               NodeGroupId node_group_id,
               int64_t node_group_term,
               CcShard *ccs,
               bool is_local,
               uint16_t bucket_id,
               const std::vector<DataStoreSearchCond> *pushdown_cond,
               std::string_view start_key,
               KeyType start_key_type,
               bool start_key_inclusive,
               std::string_view end_key,
               KeyType end_key_type,
               bool end_key_inclusive,
               size_t batch_size,
               CcRequestBase *requester,
               OnFetchedBucketData backfill_func);

    bool ValidTermCheck();

    bool Execute(CcShard &ccs) override;

    void SetFinish(int32_t err);

    void AddDataItem(std::string &&key_str,
                     std::string &&rec_str,
                     uint64_t version,
                     bool is_deleted);

    std::string_view StartKey()
    {
        if (std::holds_alternative<std::string>(start_key_))
        {
            return std::string_view(std::get<0>(start_key_).data(),
                                    std::get<0>(start_key_).size());
        }
        else
        {
            return std::get<1>(start_key_);
        }
    }

    std::string_view EndKey()
    {
        if (std::holds_alternative<std::string>(end_key_))
        {
            return std::string_view(std::get<0>(end_key_).data(),
                                    std::get<0>(end_key_).size());
        }
        else
        {
            return std::get<1>(end_key_);
        }
    }

    // table_name is a string view, cannot access it outside TxProcessor.
    TableName table_name_{
        std::string(""), TableType::Primary, TableEngine::None};
    std::string kv_table_name_;
    NodeGroupId node_group_id_;
    int64_t node_group_term_;
    CcShard *ccs_;
    uint16_t bucket_id_;
    const std::vector<DataStoreSearchCond> *pushdown_cond_{nullptr};
    std::variant<std::string, std::string_view> start_key_;
    KeyType start_key_type_{KeyType::NegativeInf};
    bool start_key_inclusive_{false};
    std::variant<std::string, std::string_view> end_key_;
    KeyType end_key_type_{KeyType::PositiveInf};
    bool end_key_inclusive_{false};
    size_t batch_size_{0};
    CcRequestBase *requester_{nullptr};
    int32_t err_code_{0};

    bool is_local_{true};
    std::deque<RawSliceDataItem> bucket_data_items_;
    bool is_drained_{false};

    OnFetchedBucketData backfill_func_;

    std::string kv_start_key_;
    std::string kv_end_key_;
};

struct UpdateRangeSlicesReq
{
    UpdateRangeSlicesReq(const TableName *table_name,
                         uint64_t ckpt_ts,
                         TxKey &&start_key,
                         std::vector<const txservice::StoreSlice *> &&slices,
                         int32_t partition_id,
                         uint64_t range_version)
        : table_name_(table_name),
          ckpt_ts_(ckpt_ts),
          start_key_(std::move(start_key)),
          range_slices_(std::move(slices)),
          partition_id_(partition_id),
          range_version_(range_version)
    {
    }

    UpdateRangeSlicesReq(const UpdateRangeSlicesReq &) = delete;
    UpdateRangeSlicesReq &operator=(const UpdateRangeSlicesReq &) = delete;

    UpdateRangeSlicesReq(UpdateRangeSlicesReq &&other) noexcept
        : table_name_(std::move(other.table_name_)),
          ckpt_ts_(std::move(other.ckpt_ts_)),
          start_key_(std::move(other.start_key_)),
          range_slices_(std::move(other.range_slices_)),
          partition_id_(std::move(other.partition_id_)),
          range_version_(std::move(other.range_version_))
    {
    }

    UpdateRangeSlicesReq &operator=(UpdateRangeSlicesReq &&other) noexcept
    {
        if (this != &other)
        {
            table_name_ = std::move(other.table_name_);
            ckpt_ts_ = std::move(other.ckpt_ts_);
            start_key_ = std::move(other.start_key_);
            range_slices_ = std::move(other.range_slices_);
            partition_id_ = std::move(other.partition_id_);
            range_version_ = std::move(other.range_version_);
        }

        return *this;
    }

    const TableName *table_name_{nullptr};
    uint64_t ckpt_ts_{UINT64_MAX};
    TxKey start_key_;
    std::vector<const StoreSlice *> range_slices_;
    uint32_t partition_id_{UINT32_MAX};
    uint64_t range_version_{UINT64_MAX};
};

struct FetchSnapshotCc;
typedef void (*OnFetchedSnapshot)(FetchSnapshotCc *fetch_cc,
                                  CcRequestBase *requester);

struct FetchSnapshotCc : public CcRequestBase
{
public:
    FetchSnapshotCc()
    {
    }
    ~FetchSnapshotCc() = default;

    void Reset(const TableName *tbl_name,
               const TableSchema *tbl_schema,
               TxKey tx_key,
               CcShard &ccs,
               NodeGroupId cc_ng_id,
               int64_t cc_ng_term,
               uint64_t snapshot_read_ts,
               bool only_fetch_archive,
               CcRequestBase *requester,
               size_t tuple_idx,
               OnFetchedSnapshot backfill_func,
               int32_t partition_id);

    bool ValidTermCheck();

    bool Execute(CcShard &ccs) override;

    void SetFinish(int err);

    NodeGroupId GetNodeGroupId() const
    {
        return cc_ng_id_;
    }

    int64_t LeaderTerm() const
    {
        return cc_ng_term_;
    }

    metrics::TimePoint start_;

    CcShard *ccs_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;

    // table_name is a string view, cannot access it outside TxProcessor.
    TableName table_name_{
        std::string(""), TableType::Primary, TableEngine::None};
    const TableSchema *table_schema_{nullptr};
    std::string kv_table_name_;
    TxKey tx_key_;
    std::string rec_str_;
    uint64_t rec_ts_{0};
    RecordStatus rec_status_{RecordStatus::Unknown};
    int error_code_{0};
    int partition_id_;

    // Used to fetch record from archives table.
    uint64_t snapshot_read_ts_{0};
    // If set only_fetch_archives_ (true), don't fetch record from base table.
    bool only_fetch_archives_{false};

    CcRequestBase *requester_{nullptr};
    // Now only used by Scan.
    size_t tuple_idx_{UINT64_MAX};
    // On fetched archive record, call backfill_func_ to backfill the record (to
    // request).
    OnFetchedSnapshot backfill_func_{nullptr};

    // These variables only be used in DataStoreHandler
    std::string kv_start_key_;
    std::string kv_end_key_;
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
        RunOnTxProcessorCc::Reset(std::move(task));

        assert(active_finishers_.load(std::memory_order_seq_cst) == 0);
        error_code_.store(CcErrorCode::NO_ERROR, std::memory_order_relaxed);
        unfinished_cnt_.store(core_cnt, std::memory_order_seq_cst);
    }

    void SetCoroCallbacks(const std::function<void()> *yield_fn,
                          const std::function<void()> *resume_fn)
    {
        yield_fn_ = yield_fn;
        resume_fn_ = resume_fn;
    }

    // The owner of a WaitableCc may be a bthread while Execute() runs on tx
    // processors (the brpc worker main stack). The two sides must not share
    // a bthread::Mutex, so Wait() polls atomics. mux_ is only used for the
    // yield/resume handshake whose waiter is a dedicated worker thread.
    void Wait();

    void Wait(const std::function<void()> *yield_fn,
              const std::function<void()> *resume_fn)
    {
        if (yield_fn == nullptr || resume_fn == nullptr)
        {
            Wait();
            return;
        }
        std::unique_lock<bthread::Mutex> lk(mux_);
        while (!IsFinished())
        {
            if (unfinished_cnt_.load(std::memory_order_seq_cst) == 0)
            {
                lk.unlock();
                bthread_usleep(100);
                lk.lock();
                continue;
            }
            waiting_.store(true, std::memory_order_release);
            lk.unlock();
            (*yield_fn)();
            lk.lock();
            waiting_.store(false, std::memory_order_release);
        }
    }

    bool IsFinished() const
    {
        return unfinished_cnt_.load(std::memory_order_seq_cst) == 0 &&
               active_finishers_.load(std::memory_order_seq_cst) == 0;
    }

    bool IsError() const
    {
        return error_code_.load(std::memory_order_acquire) !=
               CcErrorCode::NO_ERROR;
    }

    CcErrorCode ErrorCode() const
    {
        return error_code_.load(std::memory_order_acquire);
    }

    // Record an error without completing this request. Use this from a task
    // body that will return true and let Execute() perform the single
    // FinishOne() call for that shard.
    void SetErrorCode(CcErrorCode error_code)
    {
        // Latch the first error; a later success on another core must not
        // erase it.
        CcErrorCode expected = CcErrorCode::NO_ERROR;
        error_code_.compare_exchange_strong(expected,
                                            error_code,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed);
    }

    void AbortCcRequest(CcErrorCode error_code) override
    {
        SetErrorCode(error_code);
        FinishOne();
    }

    bool Execute(CcShard &ccs) override
    {
        if (RunOnTxProcessorCc::Execute(ccs))
        {
            FinishOne();
        }
        return false;
    }

private:
    void FinishOne()
    {
        active_finishers_.fetch_add(1, std::memory_order_seq_cst);

        uint32_t unfinished = unfinished_cnt_.load(std::memory_order_seq_cst);
        while (unfinished > 0)
        {
            if (unfinished_cnt_.compare_exchange_weak(
                    unfinished,
                    unfinished - 1,
                    std::memory_order_seq_cst,
                    std::memory_order_seq_cst))
            {
                break;
            }
        }

        if (unfinished == 0)
        {
            LOG(ERROR) << "WaitableCc::FinishOne called after completion";
            assert(false);
            active_finishers_.fetch_sub(1, std::memory_order_seq_cst);
            return;
        }

        if (unfinished == 1)
        {
            if (resume_fn_ != nullptr)
            {
                // The coroutine waiter checks unfinished_cnt_ and sets
                // waiting_ under mux_, so the last finisher either observes
                // waiting_ here, or the waiter re-checks the count and never
                // yields.
                std::unique_lock<bthread::Mutex> lk(mux_);
                if (waiting_.load(std::memory_order_acquire))
                {
                    waiting_.store(false, std::memory_order_release);
                    auto *fn = resume_fn_;
                    lk.unlock();
                    (*fn)();
                }
            }
        }

        active_finishers_.fetch_sub(1, std::memory_order_seq_cst);
    }

    void *operator new(size_t) noexcept
    {
        return nullptr;
    }

    void operator delete(void *)
    {
    }

private:
    // Only guards the coroutine yield/resume handshake. Never taken by
    // bthread owners.
    mutable bthread::Mutex mux_;

    std::atomic<uint32_t> unfinished_cnt_{0};
    std::atomic<CcErrorCode> error_code_;

    // Coroutine yield/resume support
    const std::function<void()> *yield_fn_{nullptr};
    const std::function<void()> *resume_fn_{nullptr};
    std::atomic<bool> waiting_{false};

    // Counts threads currently inside FinishOne(). unfinished_cnt_ may reach
    // zero before the last finisher completes the resume handshake and stops
    // touching this stack-allocated request, so waiters treat this as a
    // lifetime fence before leaving the request's scope.
    std::atomic<uint32_t> active_finishers_{0};
};

/**
 * @brief Self-driving orphan-lock recovery for a locally-coordinated tx. No
 * thread ever waits on this request; it hops between shards:
 *
 * 1. Probe (on the tx's owner shard): reads the tx's liveness inline from
 *    the shard's tx array (LocateTx): Dead when the TEntry slot has been
 *    reused or carries Aborted/Finished status, Committed when resident
 *    with post-processing possibly in flight — then re-enqueues itself to
 *    the origin shard.
 * 2. Resolve (on the shard holding the locks): re-validates the registry
 *    entry and repairs it via CcShard::RecoverDeadTxLocks. All lock/registry
 *    mutation happens here, on the shard that owns the data, against
 *    freshly-read state; the cross-hop verdict is only a hint.
 *
 * Launched by CcShard::CheckRecoverTx from a per-shard pool; recycled to the
 * pool when Resolve finishes. The only stale-verdict hazard is the tx ident
 * being reused between the two hops, which requires 2^32 transactions on one
 * core within the hop window — negligible.
 */
struct RecoverDeadTxCc : public CcRequestBase
{
public:
    enum struct Phase
    {
        Probe,
        Resolve
    };

    RecoverDeadTxCc() = default;
    RecoverDeadTxCc(const RecoverDeadTxCc &) = delete;
    RecoverDeadTxCc &operator=(const RecoverDeadTxCc &) = delete;

    /**
     * @brief The probe's liveness verdict. Alive covers Ongoing and any
     * non-terminal state. Committed means the TEntry is resident with the
     * commit decided but post-processing possibly in flight: read
     * locks/intents are releasable (the owner's own release is an
     * idempotent no-op), while write locks must not be abort-cleared — the
     * pending value may not be installed yet.
     */
    enum struct Verdict
    {
        Alive,
        Committed,
        Dead
    };

    void Reset(TxNumber txn,
               NodeGroupId cc_ng_id,
               int64_t cc_ng_term,
               int64_t tx_coord_term,
               uint64_t wlock_ts,
               uint16_t origin_core)
    {
        tx_number_ = txn;
        cc_ng_id_ = cc_ng_id;
        cc_ng_term_ = cc_ng_term;
        tx_coord_term_ = tx_coord_term;
        wlock_ts_ = wlock_ts;
        origin_core_ = origin_core;
        phase_ = Phase::Probe;
        verdict_ = Verdict::Alive;
    }

    bool Execute(CcShard &ccs) override;

private:
    NodeGroupId cc_ng_id_{0};
    int64_t cc_ng_term_{-1};
    int64_t tx_coord_term_{-1};
    uint64_t wlock_ts_{0};
    uint16_t origin_core_{0};
    Phase phase_{Phase::Probe};
    Verdict verdict_{Verdict::Alive};
};

struct UpdateCceCkptTsCc : public CcRequestBase
{
public:
    static constexpr size_t SCAN_BATCH_SIZE = 128;

    struct CkptTsEntry
    {
        CkptTsEntry() = default;
        CkptTsEntry(LruEntry *cce, uint64_t commit_ts, size_t post_flush_size)
            : cce_(cce),
              commit_ts_(commit_ts),
              post_flush_size_(post_flush_size)

        {
        }

        LruEntry *cce_;
        uint64_t commit_ts_;
        size_t post_flush_size_;
    };

    UpdateCceCkptTsCc(
        NodeGroupId node_group_id,
        int64_t term,
        const TableName &table_name,
        absl::flat_hash_map<size_t, std::vector<CkptTsEntry>> &cce_entries)
        : cce_entries_(cce_entries),
          node_group_id_(node_group_id),
          term_(term),
          table_name_(table_name)
    {
        assert(cce_entries_.size() > 0 && cce_entries_.size() <= UINT32_MAX);
        state_.store(
            CompletionState{static_cast<uint32_t>(cce_entries_.size()), 0},
            std::memory_order_relaxed);

        for (const auto &entry : cce_entries_)
        {
            indices_[entry.first] = 0;
        }
    }

    UpdateCceCkptTsCc(const UpdateCceCkptTsCc &) = delete;
    UpdateCceCkptTsCc &operator=(const UpdateCceCkptTsCc &) = delete;

    bool Execute(CcShard &ccs) override;

    void SetCoroCallbacks(const std::function<void()> *yield_fn,
                          const std::function<void()> *resume_fn)
    {
        // The coroutine mode (SetCoroCallbacks + Wait) and the continuation
        // mode (SetOnFinished) are mutually exclusive; see SetFinished().
        assert(on_finished_ == nullptr);
        yield_fn_ = yield_fn;
        resume_fn_ = resume_fn;
    }

    /**
     * @brief Continues execution on the last core to apply its slice, instead
     * of waking a thread parked in Wait().
     *
     * For callers that cannot block: the flush path installs a continuation
     * that reports its partition complete, which keeps "PutAll has returned"
     * meaning "every ckpt ts is published" even though nothing waits for it.
     *
     * @p on_finished runs on a cc shard and may destroy this request's owner,
     * so it must be the last thing that touches the request. Consumed on
     * invocation.
     */
    void SetOnFinished(std::function<void()> on_finished)
    {
        // Mutually exclusive with the coroutine mode; see SetFinished().
        assert(yield_fn_ == nullptr && resume_fn_ == nullptr);
        on_finished_ = std::move(on_finished);
    }

    /**
     * @brief Marks the calling core's slice applied. The core that brings the
     * count to zero performs the completion action: the on-finished
     * continuation, the coroutine resume, or the condition-variable notify.
     *
     * The three actions are mutually exclusive completion modes, not stages:
     * exactly one fires, selected by what the creator installed. The
     * continuation mode (SetOnFinished; the per-partition flush) returns
     * without touching the wake machinery -- its consumer reports the
     * partition complete and wakes the PutAll waiter itself. The resume and
     * notify modes serve a caller blocked in this request's own Wait() (the
     * deferred publication path). A request never has more than one waiter.
     *
     * Mutex-free except in condition-variable mode. In coroutine mode, count
     * and waiter flag share one atomic word, so the decrement that reaches zero
     * atomically collects whether a waiter has committed to suspending. The
     * resume callback is captured before that decrement: once zero is visible,
     * the terminal shard either uses only that local pointer or returns without
     * touching the request, allowing a waiter that never suspended to destroy
     * it safely. acquire/release suffices because all fan-in operations are
     * RMWs on one release sequence.
     *
     * In condition-variable mode the decrement itself is protected by mux_. A
     * waiter therefore cannot observe zero, return, and destroy the request
     * before the terminal shard has finished notifying through cv_.
     */
    void SetFinished()
    {
        // Both modes are immutable once requests are published to the shards.
        // Capture them before a coroutine-mode terminal decrement publishes
        // zero, after which an unarmed waiter may return and destroy `this`.
        const bool continuation_mode = static_cast<bool>(on_finished_);
        const std::function<void()> *resume_fn = resume_fn_;

        if (continuation_mode || resume_fn != nullptr)
        {
            CompletionState prev = state_.load(std::memory_order_relaxed);
            CompletionState next;
            do
            {
                assert(prev.unfinished_core_cnt_ >= 1);
                next = prev;
                --next.unfinished_core_cnt_;
            } while (!state_.compare_exchange_weak(prev,
                                                   next,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_relaxed));
            if (prev.unfinished_core_cnt_ != 1)
            {
                return;
            }

            if (continuation_mode)
            {
                // May destroy this request's owner; nothing touches members
                // afterwards.
                auto fn = std::move(on_finished_);
                on_finished_ = nullptr;
                fn();
            }
            else if (prev.waiter_suspended_ != 0)
            {
                // The waiter committed to yielding before publishing the flag.
                // resume_fn may queue that wake before yield_fn runs.
                (*resume_fn)();
            }
        }
        else
        {
            // Publish zero while holding the same mutex used by Wait(). Wait
            // cannot return and destroy the request until notification is done
            // and this critical section has released the mutex.
            std::lock_guard<bthread::Mutex> lk(mux_);
            CompletionState prev = state_.load(std::memory_order_relaxed);
            assert(prev.waiter_suspended_ == 0);
            assert(prev.unfinished_core_cnt_ >= 1);
            --prev.unfinished_core_cnt_;
            state_.store(prev, std::memory_order_release);
            if (prev.unfinished_core_cnt_ == 0)
            {
                cv_.notify_one();
            }
        }
    }

    /**
     * @brief Blocks the caller until every core has applied its slice.
     *
     * With coroutine callbacks installed (SetCoroCallbacks), suspends through
     * them and never touches the mutex; otherwise waits on the condition
     * variable under mux_.
     */
    void Wait()
    {
        assert((yield_fn_ == nullptr) == (resume_fn_ == nullptr));
        // The continuation mode (SetOnFinished) completes asynchronously and
        // never waits.
        assert(on_finished_ == nullptr);
        if (yield_fn_ != nullptr)
        {
            CompletionState cur = state_.load(std::memory_order_acquire);
            while (cur.unfinished_core_cnt_ != 0)
            {
                assert(cur.waiter_suspended_ == 0);
                // Publish the waiter and the count > 0 condition it depends
                // on in one RMW: the flag can only be set while cores remain,
                // so the terminal decrement either sees it (and resumes the
                // suspension entered unconditionally below) or the CAS fails
                // and the reloaded count exits the loop without suspending.
                CompletionState suspended = cur;
                suspended.waiter_suspended_ = 1;
                if (state_.compare_exchange_weak(cur,
                                                 suspended,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire))
                {
                    (*yield_fn_)();
                    cur = state_.load(std::memory_order_acquire);
                }
            }
        }
        else
        {
            std::unique_lock<bthread::Mutex> lk(mux_);
            while (state_.load(std::memory_order_acquire).unfinished_core_cnt_ >
                   0)
            {
                // timeout_us, preserve original value
                cv_.wait_for(lk, 10000L);
            }
        }
    }

    const absl::flat_hash_map<size_t, std::vector<CkptTsEntry>> &EntriesRef()
        const
    {
        return cce_entries_;
    }

    bool IsFinished() const
    {
        return state_.load(std::memory_order_acquire).unfinished_core_cnt_ == 0;
    }

private:
    absl::flat_hash_map<size_t, std::vector<CkptTsEntry>> &cce_entries_;
    // key: core_idx, value: entry_index
    absl::flat_hash_map<size_t, size_t> indices_;

    /**
     * @brief The fan-in count and the waiter's suspension flag, bundled in
     * one 8-byte lock-free atomic so their consistency is maintained by
     * single RMWs; see SetFinished() and Wait().
     */
    struct CompletionState
    {
        // Cores that have not yet applied their slice.
        uint32_t unfinished_core_cnt_{0};
        // 1 while the coroutine waiter is suspended. uint32_t rather than
        // bool keeps the struct padding-free, so compare_exchange only ever
        // compares meaningful bytes.
        uint32_t waiter_suspended_{0};
    };
    static_assert(sizeof(CompletionState) == 8);

    std::atomic<CompletionState> state_{CompletionState{}};
    static_assert(std::atomic<CompletionState>::is_always_lock_free);
    NodeGroupId node_group_id_;
    int64_t term_;
    TableName table_name_;
    // Guards the count transition and notification in condition-variable
    // completion mode; the coroutine and continuation modes never touch it.
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;

    // What to run once every core has applied its slice, instead of waking a
    // thread parked in Wait(). The flush path installs a continuation that
    // reports the partition complete to its PutAll coordinator, so a partition
    // is only counted as done after the cc entries it wrote are marked clean --
    // preserving the guarantee that PutAll returns with every ckpt ts already
    // published, without anyone blocking to get it. It exists because both ends
    // sit on threads that must not park: SetFinished() runs on a cc shard, and
    // the flush path is driven from a data store completion callback.
    //
    // Runs on whichever shard finishes last. It may destroy this request's
    // owner (the request lives inside the pooled partition state that the
    // continuation can free), so SetFinished() moves it out, drops the lock,
    // and touches no member afterwards. Consumed on invocation, so a pooled
    // request cannot fire a stale continuation from an earlier flush.
    std::function<void()> on_finished_{nullptr};

    // Coroutine yield/resume support
    const std::function<void()> *yield_fn_{nullptr};
    const std::function<void()> *resume_fn_{nullptr};
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

struct ShardCleanCc : public CcRequestBase
{
public:
    ShardCleanCc() : free_count_(0)
    {
    }

    ShardCleanCc(ShardCleanCc &&rhs) = delete;

    bool Execute(CcShard &ccs) override;

private:
    size_t free_count_{0};
};

struct FetchTableRangeSizeCc : public CcRequestBase
{
public:
    FetchTableRangeSizeCc() = default;
    ~FetchTableRangeSizeCc() = default;

    void Reset(const TableName &table_name,
               int32_t partition_id,
               const TxKey &start_key,
               CcShard *ccs,
               NodeGroupId ng_id,
               int64_t ng_term);

    bool ValidTermCheck();
    bool Execute(CcShard &ccs) override;
    void SetFinish(uint32_t error);

    const TableName *table_name_;
    int32_t partition_id_{0};
    TxKey start_key_{};
    NodeGroupId node_group_id_{0};
    int64_t node_group_term_{-1};
    CcShard *ccs_{nullptr};

    uint32_t error_code_{0};
    int32_t store_range_size_{0};

    // Only used in DataStoreHandler
    std::string kv_start_key_;
};
}  // namespace txservice
