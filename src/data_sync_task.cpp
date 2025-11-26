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

DataSyncStatus::DataSyncStatus(NodeGroupId node_group_id,
                               int64_t node_group_term,
                               bool need_truncate_log)
    : node_group_id_(node_group_id),
      node_group_term_(node_group_term),
      need_truncate_log_(need_truncate_log)
{
    Sharder::Instance().GetCheckpointer()->IncrementOngoingDataSyncCnt();
}

DataSyncStatus::~DataSyncStatus()
{
    Sharder::Instance().GetCheckpointer()->DecrementOngoingDataSyncCnt();
}

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
    assert(!table_name_.IsHashPartitioned());
    if (start_key_.KeyPtr() ==
        range_entry->GetRangeInfo()->StartTxKey().KeyPtr())
    {
        id_ = range_entry->GetRangeInfo()->PartitionId();
    }
    else
    {
        id_ = range_entry_->GetRangeInfo()->GetKeyNewRangeId(start_key_);
    }

    // For a data sync task during range split, we only need to update the ckpt
    // ts if the new range owner is the current node group.
    NodeGroupId range_owner = Sharder::Instance()
                                  .GetLocalCcShards()
                                  ->GetRangeOwner(id_, ng_id)
                                  ->BucketOwner();
    need_update_ckpt_ts_ = range_owner == ng_id;
}

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
                        if (!is_standby_node_ckpt_)
                        {
                            Sharder::Instance()
                                .GetLogAgent()
                                ->UpdateCheckpointTs(node_group_id_,
                                                     node_group_term_,
                                                     status_->truncate_log_ts_);
                        }
                    }

                    if (!is_standby_node_ckpt_ && Sharder::Instance()
                                                      .GetDataStoreHandler()
                                                      ->IsSharedStorage())
                    {
                        BrocastPrimaryCkptTs(node_group_id_,
                                             node_group_term_,
                                             status_->truncate_log_ts_);
                    }
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
        if (task_res_)
        {
            task_res_->SetError(status_->err_code_);
        }
        status_->cv_.notify_all();
    }
}

void DataSyncTask::SetScanTaskFinished()
{
    std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
    status_->unfinished_scan_tasks_--;
    if (status_->unfinished_scan_tasks_ == 0 && status_->all_task_started_ &&
        status_->unfinished_tasks_ != 0)
    {
        // If all scan tasks are finished, but there are still unfinished data
        // sync task due to pending flush, flush the current flush buffer.
        DLOG(INFO) << "Flushing current flush buffer after all scan tasks are "
                      "finished";
        Sharder::Instance().GetLocalCcShards()->FlushCurrentFlushBuffer();
    }
}

}  // namespace txservice