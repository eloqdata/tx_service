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
#include "data_sync_task.h"

#include <absl/container/flat_hash_map.h>

#include <mutex>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "cc_req_misc.h"
#include "cc_shard.h"
#include "checkpointer.h"
#include "error_messages.h"
#include "local_cc_shards.h"
#include "sharder.h"
#include "standby.h"
#include "store/data_store_handler.h"
#include "type.h"

namespace txservice
{

#ifdef RANGE_PARTITION_ENABLED
DataSyncTask::DataSyncTask(const TableName &table_name,
                           uint32_t ng_id,
                           int64_t ng_term,
                           std::shared_ptr<const TableSchema> table_schema,
                           TableRangeEntry *range_entry,
                           const TxKey &start_key,
                           const TxKey &end_key,
                           uint64_t data_sync_ts,
                           bool is_dirty,
                           bool export_base_table_items,
                           uint64_t txn,
                           std::shared_ptr<DataSyncStatus> status,
                           CcHandlerResult<Void> *hres)
    : table_name_(table_name),
      node_group_id_(ng_id),
      node_group_term_(ng_term),
      data_sync_ts_(data_sync_ts),
      status_(status),
      is_dirty_(is_dirty),
      task_res_(hres),
      start_key_(start_key.GetShallowCopy()),
      end_key_(end_key.GetShallowCopy()),
      table_schema_(table_schema),
      range_entry_(range_entry),
      during_split_range_(true),
      export_base_table_items_(export_base_table_items),
      tx_number_(txn)
{
    if (start_key_.KeyPtr() ==
        range_entry->GetRangeInfo()->StartTxKey().KeyPtr())
    {
        range_id_ = range_entry->GetRangeInfo()->PartitionId();
    }
    else
    {
        range_id_ = range_entry_->GetRangeInfo()->GetKeyNewRangeId(start_key_);
    }
}
#endif

void DataSyncTask::SetFinish()
{
    std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
    status_->unfinished_tasks_--;
    // The default value of `truncate_log_ts_` is `0`.
    if (status_->truncate_log_ts_ == 0)
    {
        status_->truncate_log_ts_ = data_sync_ts_;
    }
    else
    {
        // Update minimum checkpoint timestamp. We use this timestamp to
        // truncate log at the end.
        status_->truncate_log_ts_ =
            std::min(status_->truncate_log_ts_, data_sync_ts_);
    }

    if (status_->unfinished_tasks_ == 0 && status_->all_task_started_)
    {
        status_->PersistKV();

        if (status_->need_truncate_log_)
        {
            if (status_->err_code_ == CcErrorCode::NO_ERROR)
            {
                // Truncate redo log
                LOG(INFO) << "Checkpoint of node group #" << node_group_id_
                          << " succeeded with timestamp: "
                          << status_->truncate_log_ts_;
                if (status_->truncate_log_ts_ != UINT64_MAX)
                {
                    Sharder::Instance().UpdateNodeGroupCkptTs(
                        node_group_id_, status_->truncate_log_ts_);

                    if (!txservice_skip_wal)
                    {
#ifndef RANGE_PARTITION_ENABLED
                        if (!is_standby_node_ckpt_)
#endif
                        {
                            Sharder::Instance()
                                .GetLogAgent()
                                ->UpdateCheckpointTs(node_group_id_,
                                                     node_group_term_,
                                                     status_->truncate_log_ts_);
                        }
                    }

#ifndef RANGE_PARTITION_ENABLED
                    if (!is_standby_node_ckpt_ && Sharder::Instance()
                                                      .GetDataStoreHandler()
                                                      ->IsSharedStorage())
                    {
                        BrocastPrimaryCkptTs(node_group_id_,
                                             node_group_term_,
                                             status_->truncate_log_ts_);
                    }
#endif
                }
            }
            else
            {
                LOG(INFO) << "Checkpoint of node group #" << node_group_id_
                          << " finished with timestamp: " << data_sync_ts_
                          << " with result code: "
                          << static_cast<uint32_t>(status_->err_code_);
            }
        }

        Sharder::Instance().GetCheckpointer()->CollectCkptMetric(
            status_->err_code_ == CcErrorCode::NO_ERROR);

        if (task_res_)
        {
            if (status_->err_code_ == CcErrorCode::NO_ERROR)
            {
                task_res_->SetFinished();
            }
            else
            {
                task_res_->SetError(status_->err_code_);
            }
        }
        status_->cv_.notify_all();
    }
}

void DataSyncTask::SetError(CcErrorCode err_code)
{
    std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
    status_->unfinished_tasks_--;
    status_->err_code_ = err_code;
    // The default value of `truncate_log_ts_` is `0`.
    if (status_->truncate_log_ts_ == 0)
    {
        status_->truncate_log_ts_ = data_sync_ts_;
    }
    else
    {
        // Update minimum checkpoint timestamp. We use this timestamp to
        // truncate log at the end.
        status_->truncate_log_ts_ =
            std::min(status_->truncate_log_ts_, data_sync_ts_);
    }

    if (status_->unfinished_tasks_ == 0 && status_->all_task_started_)
    {
        status_->PersistKV();

        if (task_res_)
        {
            task_res_->SetError(status_->err_code_);
        }
        status_->cv_.notify_all();
    }
}

void DataSyncStatus::PersistKV()
{
    store::DataStoreHandler *store_hd =
        Sharder::Instance().GetDataStoreHandler();
    if (store_hd != nullptr && store_hd->NeedPersistKV())
    {
        // set `force` flag.
        bool res = PersistKV("", {}, nullptr, true);
        if (!res && err_code_ == CcErrorCode::NO_ERROR)
        {
            err_code_ = CcErrorCode::DATA_STORE_ERR;
        }
    }
}

bool DataSyncStatus::PersistKV(
    const std::string &kv_table_name,
    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        &&cce_entries,
    std::shared_ptr<DataSyncTask> task,
    bool all_task_finished)
{
    static constexpr uint64_t CKPT_END_BATCH_SIZE = 1000000;

    bool flush_ret = true;

    store::DataStoreHandler *store_hd =
        Sharder::Instance().GetDataStoreHandler();

    if (store_hd && store_hd->NeedPersistKV())
    {
        std::unique_lock<std::mutex> task_lk(task_mux_);

        if (task != nullptr)
        {
            tasks_.push_back(std::move(task));
        }

        if (!kv_table_name.empty())
        {
            dedup_kv_table_names_.insert(kv_table_name);
        }

        for (auto &[core_idx, entry] : cce_entries)
        {
            total_entry_cnt_ += entry.size();
            cce_entries_[core_idx].push_back(std::move(entry));
        }

        if (total_entry_cnt_ >= CKPT_END_BATCH_SIZE || all_task_finished)
        {
            auto tmp_flush_task_entries = std::move(cce_entries_);
            auto tmp_tasks = std::move(tasks_);
            std::vector<std::string> kv_table_names(
                dedup_kv_table_names_.begin(), dedup_kv_table_names_.end());

            size_t entry_cnt = total_entry_cnt_;
            total_entry_cnt_ = 0;
            cce_entries_.clear();
            dedup_kv_table_names_.clear();
            assert(tasks_.empty());
            assert(cce_entries_.empty());
            assert(dedup_kv_table_names_.empty());

            task_lk.unlock();

            if (entry_cnt != 0 && !tmp_flush_task_entries.empty())
            {
                flush_ret = store_hd->PersistKV(kv_table_names);
                DLOG(INFO) << "ckpt_end, res:" << static_cast<int>(flush_ret);

                if (flush_ret)
                {
                    UpdateCceCkptTsCc update_cce_ckpt_req(
                        node_group_id_,
                        node_group_term_,
                        std::move(tmp_flush_task_entries));

                    LocalCcShards *local_shards =
                        Sharder::Instance().GetLocalCcShards();

                    for (const auto &entry : update_cce_ckpt_req.EntriesRef())
                    {
                        local_shards->EnqueueToCcShard(entry.first,
                                                       &update_cce_ckpt_req);
                    }
                    update_cce_ckpt_req.Wait();

                    // Reset waiting ckpt flag. Shards should be able to request
                    // ckpt
                    // again if no cc entries can be kicked out.
                    local_shards->SetWaitingCkpt(false);
                }
            }

            LocalCcShards *local_shards =
                Sharder::Instance().GetLocalCcShards();

            for (auto &task : tmp_tasks)
            {
                local_shards->PostProcessCkpt(task, flush_ret);
            }
        }
    }

    return flush_ret;
}

}  // namespace txservice