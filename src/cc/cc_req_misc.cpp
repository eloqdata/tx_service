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
#include "cc/cc_req_misc.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <mutex>
#include <optional>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <variant>
#include <vector>

#include "cc/cc_map.h"
#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "error_messages.h"
#include "range_record.h"
#include "range_slice.h"
#include "sharder.h"
#include "statistics.h"
#include "tx_id.h"
#include "tx_record.h"
#include "tx_service.h"
#include "type.h"

namespace txservice
{
FetchCc::FetchCc(CcShard &ccs, NodeGroupId cc_ng_id, int64_t cc_ng_term)
    : ccs_(ccs), cc_ng_id_(cc_ng_id), cc_ng_term_(cc_ng_term)
{
}

void FetchCc::AddRequester(CcRequestBase *requester)
{
    requesters_.emplace_back(requester);
}

size_t FetchCc::RequesterCount() const
{
    return requesters_.size();
}

NodeGroupId FetchCc::GetNodeGroupId() const
{
    return cc_ng_id_;
}

int64_t FetchCc::LeaderTerm() const
{
    return cc_ng_term_;
}

FetchCatalogCc::FetchCatalogCc(const TableName &table_name,
                               CcShard &ccs,
                               uint32_t cc_ng_id,
                               int64_t cc_ng_term,
                               bool fetch_from_primary)
    : FetchCc(ccs, cc_ng_id, cc_ng_term),
      table_name_(table_name.StringView().data(),
                  table_name.StringView().size(),
                  table_name.Type(),
                  table_name.Engine()),
      fetch_from_primary_(fetch_from_primary)
{
}

bool FetchCatalogCc::ValidTermCheck()
{
    if (fetch_from_primary_)
    {
        int64_t standby_term = Sharder::Instance().StandbyNodeTerm();
        if (standby_term < 0)
        {
            standby_term = Sharder::Instance().CandidateStandbyNodeTerm();
        }

        if (standby_term != cc_ng_term_)
        {
            return false;
        }
    }
    else
    {
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
        if (cc_ng_term < 0)
        {
            cc_ng_term = Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
        }

        if (cc_ng_term != cc_ng_term_)
        {
            return false;
        }
    }

    return true;
}

bool FetchCatalogCc::Execute(CcShard &ccs)
{
    if (error_code_ == 0)
    {
        if (ValidTermCheck())
        {
            // If on_leader_stop and Enqueue(ClearCcNodeGroup) happens at this
            // time, the creating catalog will be cleaned by ClearCcNodeGroup,
            // and the running cc_requests will check term invalid.

            if (status_ == RecordStatus::Normal)
            {
                assert(commit_ts_ > 0);
                ccs.CreateCatalog(
                    table_name_, cc_ng_id_, catalog_image_, commit_ts_);
            }
            else
            {
                assert(status_ == RecordStatus::Deleted);
                assert(catalog_image_.empty());
                // The catalog of the specified table does not exists. The
                // version of the non-existent catalog starts from the beginning
                // of history, i.e., ts=1.
                ccs.CreateCatalog(table_name_, cc_ng_id_, catalog_image_, 1);
            }

            for (CcRequestBase *req : requesters_)
            {
                ccs.Enqueue(ccs.core_id_, req);
            }
        }
        else
        {
            for (CcRequestBase *req : requesters_)
            {
                req->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
            }
        }
    }
    else
    {
        for (CcRequestBase *req : requesters_)
        {
            req->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
        }
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchCatalogCc::SetFinish(RecordStatus status, int err)
{
    status_ = status;
    error_code_ = err;

    CODE_FAULT_INJECTOR("FetchCatalogCc_SetFinish_Error", {
        status_ = RecordStatus::Unknown;
        error_code_ = static_cast<int>(CcErrorCode::DATA_STORE_ERR);
        commit_ts_ = 0;
        catalog_image_.clear();
    });
    ccs_.Enqueue(this);
}

FetchTableStatisticsCc::FetchTableStatisticsCc(const TableName &table_name,
                                               CcShard &ccs,
                                               uint32_t cc_ng_id,
                                               int64_t cc_ng_term)
    : FetchCc(ccs, cc_ng_id, cc_ng_term),
      table_name_(table_name.StringView().data(),
                  table_name.StringView().size(),
                  table_name.Type(),
                  table_name.Engine())
{
}

bool FetchTableStatisticsCc::Execute(CcShard &ccs)
{
    if (error_code_ == 0)
    {
        int64_t cc_ng_candid_term =
            Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

        if (std::max(cc_ng_candid_term, cc_ng_term) == cc_ng_term_)
        {
            // If on_leader_stop and Enqueue(ClearCcNodeGroup) happens at this
            // time, the creating catalog will be cleaned by ClearCcNodeGroup,
            // and the running cc_requests will check term invalid.

            CatalogEntry *catalog_entry =
                ccs.GetCatalog(table_name_, cc_ng_id_);
            ccs.InitTableStatistics(catalog_entry->schema_.get(),
                                    catalog_entry->dirty_schema_.get(),
                                    cc_ng_id_,
                                    std::move(sample_pool_map_));
            for (CcRequestBase *req : requesters_)
            {
                ccs.Enqueue(ccs.core_id_, req);
            }
        }
        else
        {
            for (CcRequestBase *req : requesters_)
            {
                req->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
            }
        }
    }
    else
    {
        for (CcRequestBase *req : requesters_)
        {
            req->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
        }
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchTableStatisticsCc::SetFinish(int err)
{
    error_code_ = err;

    CODE_FAULT_INJECTOR("FetchTableStatisticsCc_SetFinish_Error", {
        error_code_ = static_cast<int>(CcErrorCode::DATA_STORE_ERR);
        current_version_ = 0;
        sample_pool_map_.clear();
    });
    ccs_.Enqueue(this);
}

FetchTableRangesCc::FetchTableRangesCc(const TableName &table_name,
                                       CcShard &ccs,
                                       NodeGroupId cc_ng_id,
                                       int64_t cc_ng_term)
    : FetchCc(ccs, cc_ng_id, cc_ng_term), table_name_(table_name)
{
}

bool FetchTableRangesCc::Execute(CcShard &ccs)
{
    if (error_code_ == 0)
    {
        int64_t cc_ng_candid_term =
            Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

        if (std::max(cc_ng_candid_term, cc_ng_term) == cc_ng_term_)
        {
            // If on_leader_stop and Enqueue(ClearCcNodeGroup) happens at this
            // time, the creating catalog will be cleaned by ClearCcNodeGroup,
            // and the running cc_requests will check term invalid.

            ccs.InitTableRanges(table_name_, ranges_vec_, cc_ng_id_);
            for (CcRequestBase *req : requesters_)
            {
                ccs.Enqueue(ccs.core_id_, req);
            }
        }
        else
        {
            for (CcRequestBase *req : requesters_)
            {
                req->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
            }
        }
    }
    else
    {
        for (CcRequestBase *req : requesters_)
        {
            req->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
        }
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchTableRangesCc::AppendTableRanges(std::vector<InitRangeEntry> &&ranges)
{
    for (auto &range : ranges)
    {
        ranges_vec_.push_back(std::move(range));
    }
}

void FetchTableRangesCc::AppendTableRange(InitRangeEntry &&range)
{
    ranges_vec_.push_back(std::move(range));
}

bool FetchTableRangesCc::EmptyRanges() const
{
    return ranges_vec_.empty();
}

void FetchTableRangesCc::SetFinish(int err)
{
    error_code_ = err;
    CODE_FAULT_INJECTOR("FetchTableRangesCc_SetFinish_Error", {
        error_code_ = static_cast<int>(CcErrorCode::DATA_STORE_ERR);
        ranges_vec_.clear();
    });
    ccs_.Enqueue(this);
}

void FetchRangeSlicesReq::SetFinish(CcErrorCode err)
{
    if (err == CcErrorCode::NO_ERROR)
    {
        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        size_t estimate_rec_size = UINT64_MAX;
        if (table_name_.IsBase() && txservice_enable_key_cache)
        {
            // Get estiamte record size for key cache
            auto schema = shards->GetSharedTableSchema(
                TableName(table_name_.GetBaseTableNameSV(),
                          TableType::Primary,
                          table_name_.Engine()),
                cc_ng_id_);
            auto stats = schema->StatisticsObject();
            if (stats)
            {
                estimate_rec_size = stats->EstimateRecordSize();
            }
        }
        std::unique_lock<std::shared_mutex> lk(range_entry_->mux_);
        assert(range_entry_->RangeSlices() == nullptr);

        std::unique_lock<std::mutex> heap_lk(shards->table_ranges_heap_mux_);
        bool is_override_thd = mi_is_override_thread();
        mi_threadid_t prev_thd =
            mi_override_thread(shards->GetTableRangesHeapThreadId());
        mi_heap_t *prev_heap =
            mi_heap_set_default(shards->GetTableRangesHeap());

        range_entry_->InitRangeSlices(std::move(slice_info_),
                                      cc_ng_id_,
                                      table_name_.IsBase(),
                                      false,
                                      estimate_rec_size);
        bool range_slice_mem_full = shards->TableRangesMemoryFull();

        mi_heap_set_default(prev_heap);
        if (is_override_thd)
        {
            mi_override_thread(prev_thd);
        }
        else
        {
            mi_restore_default_thread_id();
        }
        heap_lk.unlock();

        for (auto [req, ccs] : requesters_)
        {
            ccs->Enqueue(req);
        }
        range_entry_->fetch_range_slices_req_ = nullptr;
        if (range_slice_mem_full)
        {
            lk.unlock();
            shards->KickoutRangeSlices();
        }
    }
    else
    {
        // We need to make sure that the CcMap::Execute(CcRequest ) and
        // CcRequest::ABortCcRequest(...) functions occur on the same thread.
        // Otherwise, AbortCcRequest is not safe behavior.
        std::unique_lock<std::shared_mutex> lk(range_entry_->mux_);
        std::unordered_map<CcShard *, std::vector<CcRequestBase *>>
            waiting_reqs;

        for (auto [req, ccs] : requesters_)
        {
            waiting_reqs[ccs].push_back(req);
        }

        for (auto &[ccs, reqs] : waiting_reqs)
        {
            ccs->AbortCcRequests(std::move(reqs), err);
        }
        range_entry_->fetch_range_slices_req_ = nullptr;
    }
}

bool ClearCcNodeGroup::Execute(CcShard &ccs)
{
    ccs.DropLockHoldingTxs(cc_ng_id_);
    ccs.DropCcms(cc_ng_id_);
    ccs.ResetStandbySequence();

    if (ccs.IsNative(cc_ng_id_))
    {
        ccs.ClearActvieSiTxs();
        ccs.ClearNativeSchemaCntl();
        ccs.ClearActiveBlockingTxs();
    }

    std::unique_lock lk(mux_);
    ++finish_cnt_;
    if (finish_cnt_ == core_cnt_)
    {
        ccs.local_shards_.DropTableStatistics(cc_ng_id_);
        ccs.local_shards_.DropCatalogs(cc_ng_id_);
        ccs.local_shards_.DropTableRanges(cc_ng_id_);
        ccs.local_shards_.DropBucketInfo(cc_ng_id_);
        LOG(INFO) << "ccshard: " << ccs.core_id_
                  << "; clear ccmaps and catalogs of node group: " << cc_ng_id_;
        wait_cv_.notify_one();
    }

    // The owner of this request is the raft thread that downgrades the cc
    // ng leader to a non-leader node. The request is not in a resource pool
    // and re-used. So, always returns false.
    return false;
}

bool InitKeyCacheCc::SetFinish(uint16_t core, bool succ)
{
    if (succ)
    {
        slice_->SetKeyCacheValidity(core, succ);
    }
    slice_->SetLoadingKeyCache(core, false);

    if (unfinished_cnt_.fetch_sub(1, std::memory_order_relaxed) == 1)
    {
        pause_pos_.clear();

        // Unpin the slice.
        range_->UnpinSlice(slice_, true);
        std::unique_lock<std::mutex> slice_lk(slice_->slice_mux_);
        slice_->init_key_cache_cc_ = nullptr;

        return true;
    }

    return false;
}

bool InitKeyCacheCc::Execute(CcShard &ccs)
{
    int64_t cc_ng_candid_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(ng_id_);
    if (std::max(cc_ng_candid_term, cc_ng_term) != term_)
    {
        return SetFinish(ccs.core_id_, false);
    }

    CcMap *ccm = ccs.GetCcm(tbl_name_, ng_id_);
    if (ccm == nullptr)
    {
        // ccm is empty when slice is fully cached. That means this slice is
        // empty on this core.
        return SetFinish(ccs.core_id_, true);
    }

    return ccm->Execute(*this);
}
StoreRange &InitKeyCacheCc::Range()
{
    return *range_;
}

StoreSlice &InitKeyCacheCc::Slice()
{
    return *slice_;
}

void InitKeyCacheCc::SetPauseKey(TxKey &key, uint16_t core_id)
{
    pause_pos_[core_id] = key.Clone();
}

TxKey &InitKeyCacheCc::PauseKey(uint16_t core_id)
{
    return pause_pos_[core_id];
}

void FillStoreSliceCc::Reset(const TableName &table_name,
                             NodeGroupId cc_ng_id,
                             int64_t cc_ng_term,
                             const KeySchema *key_schema,
                             const RecordSchema *rec_schema,
                             uint64_t schema_ts,
                             StoreSlice *slice,
                             StoreRange *range,
                             bool force_load,
                             uint64_t snapshot_ts,
                             LocalCcShards &cc_shards)
{
    assert(slice != nullptr);
    assert(range != nullptr);

    table_name_ = &table_name;
    cc_ng_id_ = cc_ng_id;
    cc_ng_term_ = cc_ng_term;
    force_load_ = force_load;
    finish_cnt_ = 0;
    core_cnt_ = cc_shards.Count();

    next_idxs_.clear();
    next_idxs_.resize(cc_shards.Count(), 0);

    partitioned_slice_data_.clear();
    partitioned_slice_data_.resize(cc_shards.Count());

    range_slice_ = slice;
    range_ = range;

    key_schema_ = key_schema;
    rec_schema_ = rec_schema;
    start_key_ = slice->StartTxKey();
    end_key_ = slice->EndTxKey();
    schema_ts_ = schema_ts;
    snapshot_ts_ = snapshot_ts;
    cc_ng_id_ = cc_ng_id;
    cc_ng_term_ = cc_ng_term;
    slice_size_ = 0;
    rec_cnt_ = 0;
}

void FillStoreSliceCc::SetKvFinish(bool success)
{
    CODE_FAULT_INJECTOR("LoadRangeSliceRequest_SetFinish_Error", {
        success = false;
        partitioned_slice_data_.clear();
        slice_size_ = 0;
        snapshot_ts_ = 0;
    });

    if (metrics::enable_kv_metrics)
    {
        metrics::kv_meter->Collect(metrics::NAME_KV_LOAD_SLICE_TOTAL, 1);
        metrics::kv_meter->CollectDuration(metrics::NAME_KV_LOAD_SLICE_DURATION,
                                           start_);
    }

    // Update the slice's last load ts.
    uint64_t cur_ts = LocalCcShards::ClockTs();
    range_slice_->UpdateLastLoadTs(cur_ts);
    if (success)
    {
        StartFilling();
    }
    else
    {
        // We need to abort and recycle this request explicitly if
        // `FillStoreSliceCc::SetKvFinished` called by
        // `DataStoreHandler::OnLoadRangeSlice()`. Because this request already
        // resides in the `slice.cc_queue` at this point.
        TerminateFilling();
        Free();
    }
}

bool FillStoreSliceCc::Execute(CcShard &ccs)
{
    int64_t cc_ng_candid_term =
        Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
    if (std::max(cc_ng_candid_term, cc_ng_term) != cc_ng_term_)
    {
        return SetError(CcErrorCode::NG_TERM_CHANGED);
    }

    CcMap *ccm = ccs.GetCcm(*table_name_, cc_ng_id_);

    if (ccm == nullptr)
    {
        std::optional<const TableSchema *> init_res =
            ccs.InitCcm(*table_name_,
                        cc_ng_id_,
                        std::max(cc_ng_term, cc_ng_candid_term),
                        this);

        if (!init_res.has_value())
        {
            // InitCcm failure.
            // the catalog may need to be fetched from the
            // KV store, may not exist (payload status = Deleted),
            // or is currently being modified (write lock acquired).
            //
            // In the first case, the requester will be re-enqueued
            // after FetchCatalog() completes fetching the catalog
            // from the data store. In the latter cases, the request
            // is marked as errored.
            return false;
        }
        const TableSchema *table_schema = init_res.value();
        // Successfully load table catalog from data store.
        assert(table_schema != nullptr);
        assert(table_schema->Version() > 0);
        // For a filling range slice request, there must be a prior
        // request reading and locking the table's schema, to prevent
        // others from dropping the table. Hence, the table's schema
        // must be avaliable.
        ccm = ccs.GetCcm(*table_name_, cc_ng_id_);
        assert(ccm != nullptr);
    }

    return ccm->Execute(*this);
}

void FillStoreSliceCc::AddDataItem(
    TxKey key,
    std::unique_ptr<txservice::TxRecord> &&record,
    uint64_t version_ts,
    bool is_deleted)
{
    slice_size_ += key.Size();
    slice_size_ += record->Size();

    if (!is_deleted)
    {
        rec_cnt_++;
    }

    size_t hash = key.Hash();
    // Uses the lower 10 bits of the hash code to shard the key across
    // CPU cores at this node.
    uint16_t core_code = hash & 0x3FF;
    uint16_t core_id = core_code % core_cnt_;

    partitioned_slice_data_[core_id].emplace_back(
        std::move(key), std::move(record), version_ts, is_deleted);
}

bool FillStoreSliceCc::SetFinish(CcShard *cc_shard)
{
    bool finish_all = false;
    CcErrorCode err_code;
    {
        std::lock_guard<std::mutex> lk(mux_);
        ++finish_cnt_;

        if (finish_cnt_ == core_cnt_)
        {
            finish_all = true;
            err_code = err_code_;
        }
    }

    if (finish_all)
    {
        if (err_code == CcErrorCode::NO_ERROR)
        {
            bool init_key_cache =
                txservice_enable_key_cache && table_name_->IsBase();
            // Cache  the pointer since FillStoreSliceCc will be freed after
            // CommitLoading.

            const TableName *tbl_name = table_name_;
            auto cc_ng_id = cc_ng_id_;
            auto cc_ng_term = cc_ng_term_;
            if (init_key_cache && rec_cnt_ > 0)
            {
                LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
                size_t estimate_rec_size = UINT64_MAX;

                // Get estiamte record size for key cache
                auto schema = shards->GetSharedTableSchema(
                    TableName(table_name_->GetBaseTableNameSV(),
                              TableType::Primary,
                              table_name_->Engine()),
                    cc_ng_id_);
                auto stats = schema->StatisticsObject();
                assert(slice_size_ > 0);
                estimate_rec_size = slice_size_ / rec_cnt_;
                if (stats)
                {
                    // Update estimate size in table stats with the loaded
                    // slice.
                    stats->SetEstimateRecordSize(estimate_rec_size);
                }
            }
            range_slice_->CommitLoading(*range_, slice_size_);
            if (init_key_cache)
            {
                range_slice_->InitKeyCache(
                    cc_shard, range_, tbl_name, cc_ng_id, cc_ng_term);
            }
        }
        else
        {
            range_slice_->SetLoadingError(*range_, err_code);
        }

        next_idxs_.clear();
        partitioned_slice_data_.clear();
    }

    return finish_all;
}

bool FillStoreSliceCc::SetError(CcErrorCode err_code)
{
    bool finish_all = false;
    {
        std::lock_guard<std::mutex> lk(mux_);
        ++finish_cnt_;
        err_code_ = err_code;

        if (finish_cnt_ == core_cnt_)
        {
            finish_all = true;
        }
    }

    if (finish_all)
    {
        range_slice_->SetLoadingError(*range_, err_code_);

        next_idxs_.clear();
        partitioned_slice_data_.clear();
    }

    return finish_all;
}

void FillStoreSliceCc::StartFilling()
{
    range_slice_->StartLoading(this, *Sharder::Instance().GetLocalCcShards());
}

void FillStoreSliceCc::TerminateFilling()
{
    // The method is called when there is an error of reading the data store.
    // The slice has not been filled into memory. So, the out-of-memory flag is
    // false.
    range_slice_->SetLoadingError(*range_, CcErrorCode::DATA_STORE_ERR);
    next_idxs_.clear();
    partitioned_slice_data_.clear();
}

FetchRecordCc::FetchRecordCc(const TableName *tbl_name,
                             const TableSchema *tbl_schema,
                             TxKey tx_key,
                             LruEntry *cce,
                             CcShard &ccs,
                             NodeGroupId cc_ng_id,
                             int64_t cc_ng_term,
                             int32_t partition_id,
                             bool fetch_from_primary,
                             uint64_t snapshot_read_ts,
                             bool only_fetch_archives)
    : FetchCc(ccs, cc_ng_id, cc_ng_term),
      table_name_(tbl_name->StringView(), tbl_name->Type(), tbl_name->Engine()),
      table_schema_(tbl_schema),
      kv_table_name_(
          table_schema_->GetKVCatalogInfo()->GetKvTableName(table_name_)),
      tx_key_(std::move(tx_key)),
      cce_(cce),
      lock_(cce->GetKeyGapLockAndExtraData()),
      partition_id_(partition_id),
      fetch_from_primary_(fetch_from_primary),
      snapshot_read_ts_(snapshot_read_ts),
      only_fetch_archives_(only_fetch_archives)
{
}

bool FetchRecordCc::ValidTermCheck()
{
    if (fetch_from_primary_)
    {
        if (Sharder::Instance().StandbyNodeTerm() != cc_ng_term_ &&
            Sharder::Instance().CandidateStandbyNodeTerm() != cc_ng_term_)
        {
            return false;
        }
    }
    else
    {
        int64_t cc_ng_candid_term =
            Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
        int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();

        if (std::max({cc_ng_candid_term, cc_ng_term, standby_node_term}) !=
            cc_ng_term_)
        {
            return false;
        }
    }

    return true;
}

bool FetchRecordCc::Execute(CcShard &ccs)
{
    if (!ValidTermCheck())
    {
        // term has changed and the ccm has been erased already. It is no
        // longer safe to access cce. Just abort all the reqs.
        for (CcRequestBase *req : requesters_)
        {
            if (req)
            {
                req->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
            }
        }
        ccs.RemoveFetchRecordRequest(cce_);
        return false;
    }

    if (lock_->GetCcEntry() != nullptr)
    {
        assert(lock_->GetCcMap() != nullptr);
        assert(lock_->GetCcEntry() == cce_);
        // if the referenced cce is already invalid, we do not need to care
        // about the fetch result and pending reqs since they are all
        // invalid.
        bool succ;
        if (only_fetch_archives_)
        {
            succ = lock_->GetCcMap()->BackFillArchives(
                cce_, *archive_records_, true);
        }
        else
        {
            if (snapshot_read_ts_ > 0 && archive_records_ != nullptr &&
                archive_records_->size() > 0)
            {
                succ = lock_->GetCcMap()->BackFillArchives(
                    cce_, *archive_records_, false);
            }

            succ = lock_->GetCcMap()->BackFill(
                cce_, rec_ts_, rec_status_, rec_str_);
        }

        if (!succ)
        {
            // Retry if backfill failed.
            ccs.Enqueue(ccs.core_id_, this);
            return false;
        }
        if (error_code_ == 0)
        {
            for (CcRequestBase *req : requesters_)
            {
                if (req)
                {
                    ccs.Enqueue(ccs.core_id_, req);
                }
            }
        }
        else
        {
            for (CcRequestBase *req : requesters_)
            {
                // TODO(liunyl): key object forward req can only be aborted if
                // term changes. retry if data store op failed.
                if (req)
                {
                    // Release the pin added by the ccrequest.
                    cce_->GetKeyGapLockAndExtraData()->ReleasePin();
                    cce_->RecycleKeyLock(ccs);

                    req->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
                }
            }
        }
    }

    ccs.RemoveFetchRecordRequest(cce_);
    return false;
}

void FetchRecordCc::SetFinish(int err)
{
    error_code_ = err;
    ccs_.Enqueue(this);
}

void FetchBucketDataCc::Reset(
    const TableName *table_name,
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
    OnFetchedBucketData backfill_func)
{
    table_name_ = TableName(
        table_name->StringView(), table_name->Type(), table_name->Engine());
    kv_table_name_ =
        table_schema->GetKVCatalogInfo()->GetKvTableName(table_name_);
    node_group_id_ = node_group_id;
    node_group_term_ = node_group_term;
    ccs_ = ccs;
    is_local_ = is_local;
    bucket_id_ = bucket_id;
    pushdown_cond_ = pushdown_cond;
    start_key_ = start_key;
    start_key_type_ = start_key_type;
    start_key_inclusive_ = start_key_inclusive;
    end_key_ = end_key;
    end_key_type_ = end_key_type;
    end_key_inclusive_ = end_key_inclusive;
    assert(std::holds_alternative<std::string_view>(start_key_));
    assert(std::holds_alternative<std::string_view>(end_key_));

    batch_size_ = batch_size;
    requester_ = requester;
    err_code_ = 0;

    bucket_data_items_.clear();
    is_drained_ = false;
    backfill_func_ = backfill_func;

    kv_start_key_.clear();
    kv_end_key_.clear();
}

bool FetchBucketDataCc::ValidTermCheck()
{
    int64_t ng_leader_term = Sharder::Instance().LeaderTerm(node_group_id_);
    int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();

    if (std::max(ng_leader_term, standby_node_term) != node_group_term_)
    {
        return false;
    }

    return true;
}

void FetchBucketDataCc::AddDataItem(std::string &&key_str,
                                    std::string &&rec_str,
                                    uint64_t version,
                                    bool is_deleted)
{
    bucket_data_items_.emplace_back(
        std::move(key_str), std::move(rec_str), version, is_deleted);
}

bool FetchBucketDataCc::Execute(CcShard &ccs)
{
    if (!ValidTermCheck())
    {
        err_code_ = static_cast<int32_t>(CcErrorCode::NG_TERM_CHANGED);
    }

    if (err_code_ != 0)
    {
        if (is_local_)
        {
            ScanNextBatchCc *req = static_cast<ScanNextBatchCc *>(requester_);
            req->DecreaseWaitForFetchBucketCnt(ccs.core_id_);
            req->SetErrorCode(static_cast<CcErrorCode>(err_code_));
            if (req->IsWaitForFetchBucket(ccs.core_id_) &&
                req->WaitForFetchBucketCnt(ccs.core_id_) == 0)
            {
                ccs_->Enqueue(requester_);
            }
        }
        else
        {
            remote::RemoteScanNextBatch *req =
                static_cast<remote::RemoteScanNextBatch *>(requester_);
            req->DecreaseWaitForFetchBucketCnt(ccs.core_id_);
            req->SetErrorCode(static_cast<CcErrorCode>(err_code_));
            if (req->IsWaitForFetchBucket(ccs.core_id_) &&
                req->WaitForFetchBucketCnt(ccs.core_id_) == 0)
            {
                ccs_->Enqueue(requester_);
            }
        }
    }
    else
    {
        (*backfill_func_)(this, requester_);
    }

    return true;
}

void FetchBucketDataCc::SetFinish(int32_t err)
{
    err_code_ = err;
    ccs_->Enqueue(this);
}

void FetchSnapshotCc::Reset(const TableName *tbl_name,
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
                            int32_t partition_id)

{
    ccs_ = &ccs;
    cc_ng_id_ = cc_ng_id;
    cc_ng_term_ = cc_ng_term;
    table_name_ =
        TableName(tbl_name->StringView(), tbl_name->Type(), tbl_name->Engine());
    table_schema_ = tbl_schema;
    kv_table_name_ =
        table_schema_->GetKVCatalogInfo()->GetKvTableName(table_name_);
    tx_key_ = std::move(tx_key);
    partition_id_ = partition_id;
    snapshot_read_ts_ = snapshot_read_ts;
    only_fetch_archives_ = only_fetch_archive;
    requester_ = requester;
    tuple_idx_ = tuple_idx;
    backfill_func_ = backfill_func;
    rec_str_.clear();
}

bool FetchSnapshotCc::ValidTermCheck()
{
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

    if (cc_ng_term != cc_ng_term_)
    {
        return false;
    }

    return true;
}

bool FetchSnapshotCc::Execute(CcShard &ccs)
{
    if (!ValidTermCheck())
    {
        // term has changed and the ccm has been erased already. It is no
        // longer safe to access cce. Just abort all the reqs.
        requester_->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
        error_code_ = static_cast<int>(CcErrorCode::NG_TERM_CHANGED);
    }
    else if (error_code_ != 0)
    {
        requester_->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
    }
    else
    {
        assert(backfill_func_ != nullptr && requester_ != nullptr);
        (*backfill_func_)(this, requester_);
    }

    return true;
}

void FetchSnapshotCc::SetFinish(int err)
{
    error_code_ = err;
    ccs_->Enqueue(this);
}

bool RunOnTxProcessorCc::Execute(CcShard &ccs)
{
    if (task_)
    {
        bool done = task_(ccs);
        if (!done)
        {
            ccs.Enqueue(this);
            return false;
        }
    }
    return true;
}

bool UpdateCceCkptTsCc::Execute(CcShard &ccs)
{
    assert(indices_.count(ccs.core_id_) > 0);

    auto &index = indices_[ccs.core_id_];
    auto &records = cce_entries_[ccs.core_id_];
    assert(index < records.size());

    if (index >= records.size())
    {
        // Set finished. We don't care error code.
        SetFinished();
        return false;
    }

    int64_t ng_leader_term = Sharder::Instance().LeaderTerm(node_group_id_);
    int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
    int64_t current_term = std::max(ng_leader_term, standby_node_term);

    if (current_term < 0 || current_term != term_)
    {
        SetFinished();
        return false;
    }

    size_t last_index = std::min(index + SCAN_BATCH_SIZE, records.size());

    for (; index < last_index; ++index)
    {
        const CkptTsEntry &ref = records[index];
        if (range_partitioned_)
        {
            if (versioned_payload_)
            {
                VersionedLruEntry<true, true> *v_entry =
                    static_cast<VersionedLruEntry<true, true> *>(ref.cce_);
                v_entry->entry_info_.SetDataStoreSize(ref.post_flush_size_);

                v_entry->SetCkptTs(ref.commit_ts_);
                v_entry->ClearBeingCkpt();
            }
            else
            {
                VersionedLruEntry<false, true> *v_entry =
                    static_cast<VersionedLruEntry<false, true> *>(ref.cce_);
                v_entry->entry_info_.SetDataStoreSize(ref.post_flush_size_);

                v_entry->SetCkptTs(ref.commit_ts_);
                v_entry->ClearBeingCkpt();
            }
        }
        else
        {
            if (versioned_payload_)
            {
                VersionedLruEntry<true, false> *v_entry =
                    static_cast<VersionedLruEntry<true, false> *>(ref.cce_);

                v_entry->SetCkptTs(ref.commit_ts_);
                v_entry->ClearBeingCkpt();
            }
            else
            {
                VersionedLruEntry<false, false> *v_entry =
                    static_cast<VersionedLruEntry<false, false> *>(ref.cce_);

                v_entry->SetCkptTs(ref.commit_ts_);
                v_entry->ClearBeingCkpt();
            }
        }
    }

    if (index == records.size())
    {
        SetFinished();
    }
    else
    {
        ccs.Enqueue(ccs.core_id_, this);
    }
    return false;
}

bool WaitNoNakedBucketRefCc::Execute(CcShard &ccs)
{
    std::unique_lock<bthread::Mutex> lk(mutex_);

    if (ccs.NakedBucketsRefCnt() != 0)
    {
        // re-enqueue until NakedBucketsRefCnt() is zero.
        ccs.Enqueue(this);
        return false;
    }

    if (ccs.core_id_ < ccs.core_cnt_ - 1)
    {
        // move to next core.
        ccs.local_shards_.EnqueueCcRequest(
            ccs.core_id_, ccs.core_id_ + 1, this);
        return false;
    }

    // at last core, set finish.
    finish_ = true;
    cv_.notify_one();

    return false;
}

RestoreCcMapCc::RestoreCcMapCc()
    : table_name_(nullptr),
      cc_ng_id_(0),
      cc_ng_term_(0),
      core_cnt_(0),
      finished_cnt_(0),
      slice_data_(),
      decoded_slice_data_(),
      next_idxs_(),
      cancel_data_loading_on_error_(nullptr),
      data_item_decoded_(),
      error_code_(CcErrorCode::NO_ERROR),
      total_cnt_(0)
{
}

void RestoreCcMapCc::Reset(
    const TableName *table_name,
    uint32_t cc_group_id,
    int64_t cc_group_term,
    const uint16_t core_cnt,
    std::atomic<CcErrorCode> *cancel_data_loading_on_error)
{
    table_name_ = table_name;
    cc_ng_id_ = cc_group_id;
    cc_ng_term_ = cc_group_term;
    core_cnt_ = core_cnt;
    cancel_data_loading_on_error_ = cancel_data_loading_on_error;
    error_code_ = CcErrorCode::NO_ERROR;
    finished_cnt_ = 0;
    slice_data_.clear();
    slice_data_.resize(core_cnt_);
    decoded_slice_data_.clear();
    decoded_slice_data_.resize(core_cnt_);
    next_idxs_.clear();
    next_idxs_.resize(core_cnt_);
    data_item_decoded_.clear();
    data_item_decoded_.resize(core_cnt_);
    std::fill(data_item_decoded_.begin(), data_item_decoded_.end(), 0);
    next_idxs_.clear();
    next_idxs_.resize(core_cnt_);
    std::fill(next_idxs_.begin(), next_idxs_.end(), 0);
    total_cnt_ = 0;
}

bool RestoreCcMapCc::Execute(CcShard &ccs)
{
    int64_t cc_ng_candid_term =
        Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
    int64_t standby_candid_term =
        Sharder::Instance().CandidateStandbyNodeTerm();
    int64_t standby_term = Sharder::Instance().StandbyNodeTerm();

    // what ever this is a primary or standby node, either term matched is ok
    if (std::max(cc_ng_candid_term, cc_ng_term) != cc_ng_term_ &&
        std::max(standby_candid_term, standby_term) != cc_ng_term_)
    {
        cancel_data_loading_on_error_->store(CcErrorCode::NG_TERM_CHANGED,
                                             std::memory_order_release);
        SetFinished(CcErrorCode::NG_TERM_CHANGED);
        return false;
    }

    if (cancel_data_loading_on_error_->load(std::memory_order_acquire) !=
        CcErrorCode::NO_ERROR)
    {
        SetFinished(CcErrorCode::FORCE_FAIL);
        return false;
    }

    CcMap *ccm = ccs.GetCcm(*table_name_, cc_ng_id_);

    if (ccm == nullptr)
    {
        std::optional<const TableSchema *> init_res =
            ccs.InitCcm(*table_name_,
                        cc_ng_id_,
                        std::max(cc_ng_term, cc_ng_candid_term),
                        this);

        if (!init_res.has_value())
        {
            // InitCcm failure.
            // the catalog may need to be fetched from the
            // KV store, may not exist (payload status = Deleted),
            // or is currently being modified (write lock acquired).
            //
            // In the first case, the requester will be re-enqueued
            // after FetchCatalog() completes fetching the catalog
            // from the data store. In the latter cases, the request
            // is marked as errored.
            return false;
        }
        const TableSchema *table_schema = init_res.value();
        // Successfully load table catalog from data store.
        assert(table_schema != nullptr);
        assert(table_schema->Version() > 0);
        // For a filling range slice request, there must be a prior
        // request reading and locking the table's schema, to prevent
        // others from dropping the table. Hence, the table's schema
        // must be avaliable.
        ccm = ccs.GetCcm(*table_name_, cc_ng_id_);
        assert(ccm != nullptr);
    }

    ccm->Execute(*this);

    return false;
}

void RestoreCcMapCc::SetFinished(CcErrorCode error_code)
{
    std::unique_lock<bthread::Mutex> lk(req_mux_);

    if (error_code != CcErrorCode::NO_ERROR &&
        error_code_ == CcErrorCode::NO_ERROR)
    {
        error_code_ = error_code;

        if (cancel_data_loading_on_error_->load(std::memory_order_acquire) ==
            CcErrorCode::NO_ERROR)
        {
            CcErrorCode expected = CcErrorCode::NO_ERROR;
            cancel_data_loading_on_error_->compare_exchange_strong(expected,
                                                                   error_code_);
        }
        DLOG(INFO) << "RestoreCcMapCc " << this
                   << " error: " << static_cast<int>(error_code_);
    }

    if (++finished_cnt_ == core_cnt_)
    {
        Free();
    }
}

std::deque<SliceDataItem> &RestoreCcMapCc::DecodedSliceData(uint16_t core_id)
{
    assert(core_id < decoded_slice_data_.size());
    return decoded_slice_data_[core_id];
}

std::deque<RawSliceDataItem> &RestoreCcMapCc::SliceData(uint16_t core_id)
{
    assert(core_id < slice_data_.size());
    return slice_data_[core_id];
}

void RestoreCcMapCc::AddDataItem(uint16_t core_id,
                                 std::string &&key_str,
                                 std::string &&rec_str,
                                 uint64_t version_ts,
                                 bool is_deleted)
{
    assert(core_id < slice_data_.size());
    slice_data_[core_id].emplace_back(
        std::move(key_str), std::move(rec_str), version_ts, is_deleted);
}

void RestoreCcMapCc::DecodedDataItem(
    uint16_t core_id,
    TxKey &&key,
    std::unique_ptr<txservice::TxRecord> &&record,
    uint64_t version_ts,
    bool is_deleted)
{
    assert(core_id < decoded_slice_data_.size());
    decoded_slice_data_[core_id].emplace_back(
        std::move(key), std::move(record), version_ts, is_deleted);
}

}  // namespace txservice
