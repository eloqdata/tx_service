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
    void AppendTableRanges(std::vector<InitRangeEntry> &&ranges);
    void AppendTableRange(InitRangeEntry &&range);

    bool EmptyRanges() const;
    void SetFinish(int err);

public:
    const TableName table_name_;
    int error_code_{0};
    std::vector<InitRangeEntry> ranges_vec_;

    // These variables only be used in DataStoreHandler
    std::string kv_start_key_;
    std::string kv_end_key_;
    std::string kv_session_id_;
    int32_t kv_partition_id_{0};
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

struct InitKeyCacheCc : public CcRequestBase
{
public:
    static constexpr size_t MaxScanBatchSize = 64;

    InitKeyCacheCc() = default;

    void Reset(StoreRange *range,
               StoreSlice *slice,
               uint16_t core_cnt,
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
        unfinished_cnt_ = core_cnt;

        pause_pos_.clear();
        pause_pos_.resize(core_cnt);
    }

    bool Execute(CcShard &ccs) override;
    bool SetFinish(uint16_t core, bool succ);
    StoreSlice &Slice();
    StoreRange &Range();
    void SetPauseKey(TxKey &key, uint16_t core_id);
    TxKey &PauseKey(uint16_t core_id);

private:
    TableName tbl_name_{std::string(""), TableType::Primary, TableEngine::None};
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

    std::deque<SliceDataItem> &SliceData(uint16_t core_id)
    {
        assert(core_id < partitioned_slice_data_.size());
        return partitioned_slice_data_[core_id];
    }

    void AddDataItem(TxKey key,
                     std::unique_ptr<txservice::TxRecord> &&record,
                     uint64_t version_ts,
                     bool is_deleted);

    bool SetFinish(CcShard *cc_shard);
    bool SetError(CcErrorCode err_code);

    void SetKvFinish(bool success);

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        DLOG(ERROR) << "Abort this FillStoreSliceCc request with error: "
                    << CcErrorMessage(err_code);
        bool finish_all = SetError(err_code);
        // Recycle request
        if (finish_all)
        {
            Free();
        }
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

    metrics::TimePoint start_;

private:
    const TableName *table_name_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    bool force_load_;
    uint16_t finish_cnt_;
    uint16_t core_cnt_;
    std::mutex mux_;
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};

    std::vector<size_t> next_idxs_;
    std::vector<std::deque<SliceDataItem>> partitioned_slice_data_;

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
    std::string_view kv_start_key_;
    std::string_view kv_end_key_;
    std::string kv_session_id_;
    int32_t kv_partition_id_{0};
};

struct FetchRecordCc : public FetchCc
{
public:
    FetchRecordCc() = delete;
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
                  bool only_fetch_archives = false);
    ~FetchRecordCc() = default;

    bool ValidTermCheck();

    bool Execute(CcShard &ccs) override;

    void SetFinish(int err);

    // table_name is a string view, cannot access it outside TxProcessor.
    TableName table_name_;
    const TableSchema *table_schema_{nullptr};
    std::string kv_table_name_;
    TxKey tx_key_;
    LruEntry *cce_{nullptr};
    KeyGapLockAndExtraData *lock_{nullptr};
    uint64_t rec_ts_{0};
    RecordStatus rec_status_{RecordStatus::Unknown};
    std::string rec_str_;
    int error_code_{0};
    int partition_id_;
    bool fetch_from_primary_{false};

    // If set snapshot_read_ts_ (not equal 0), the snapshot_read_ts_ will be
    // used to fetch record from archives table.
    uint64_t snapshot_read_ts_{0};
    // If set only_fetch_archives_ (true), don't fetch record from base table.
    bool only_fetch_archives_{false};
    std::unique_ptr<
        std::vector<std::tuple<uint64_t, RecordStatus, std::string>>>
        archive_records_{nullptr};

    // These variables only be used in DataStoreHandler
    std::string kv_session_id_;
    std::string kv_start_key_;
    std::string kv_end_key_;
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
               uint16_t bucket_id,
               TxKey start_key,
               bool start_key_inclusive,
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

    // table_name is a string view, cannot access it outside TxProcessor.
    TableName table_name_{
        std::string(""), TableType::Primary, TableEngine::None};
    std::string kv_table_name_;
    NodeGroupId node_group_id_;
    int64_t node_group_term_;
    CcShard *ccs_;
    uint16_t bucket_id_;
    TxKey start_key_;
    bool start_key_inclusive_;
    size_t batch_size_{0};
    CcRequestBase *requester_{nullptr};
    int32_t err_code_{0};

    std::deque<RawSliceDataItem> bucket_data_items_;
    bool is_drained_{false};

    OnFetchedBucketData backfill_func_;
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
        absl::flat_hash_map<size_t, std::vector<CkptTsEntry>> &cce_entries,
        bool range_partitioned,
        bool versioned_payload)
        : cce_entries_(cce_entries),
          node_group_id_(node_group_id),
          term_(term),
          range_partitioned_(range_partitioned),
          versioned_payload_(versioned_payload)
    {
        unfinished_core_cnt_ = cce_entries_.size();
        assert(unfinished_core_cnt_ > 0);

        for (const auto &entry : cce_entries_)
        {
            indices_[entry.first] = 0;
        }
    }

    UpdateCceCkptTsCc(const UpdateCceCkptTsCc &) = delete;
    UpdateCceCkptTsCc &operator=(const UpdateCceCkptTsCc &) = delete;

    bool Execute(CcShard &ccs) override;

    void SetFinished()
    {
        std::lock_guard<bthread::Mutex> lk(mux_);
        unfinished_core_cnt_--;
        if (unfinished_core_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        while (unfinished_core_cnt_ > 0)
        {
            cv_.wait_for(lk, 10000);
        }
    }

    const absl::flat_hash_map<size_t, std::vector<CkptTsEntry>> &EntriesRef()
        const
    {
        return cce_entries_;
    }

private:
    absl::flat_hash_map<size_t, std::vector<CkptTsEntry>> &cce_entries_;
    // key: core_idx, value: entry_index
    absl::flat_hash_map<size_t, size_t> indices_;

    size_t unfinished_core_cnt_;
    NodeGroupId node_group_id_;
    int64_t term_;
    bool range_partitioned_{false};
    bool versioned_payload_{false};
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;
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
