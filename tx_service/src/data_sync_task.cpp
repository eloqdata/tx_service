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

#include <algorithm>
#include <cassert>
#include <mutex>
#include <unordered_map>

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
namespace
{
bool IsCheckpointCancellation(CcErrorCode err_code)
{
    return err_code == CcErrorCode::NG_TERM_CHANGED ||
           err_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER;
}

void ReportCheckpointResult(
    const DataSyncStatus &status,
    const std::optional<DataSyncStatus::CheckpointResult> &result)
{
    if (result.has_value())
    {
        Sharder::Instance().GetCheckpointer()->ReportCheckpointOutcome(
            status.node_group_id_,
            status.node_group_term_,
            result->outcome_,
            result->failure_reason_);
    }
}
}  // namespace

NewestTermByNodeGroup FindNewestTerms(const FlushTaskEntryMap &flush_task)
{
    NewestTermByNodeGroup newest_terms;
    for (const auto &[_, entries] : flush_task)
    {
        for (const auto &entry : entries)
        {
            const DataSyncTask *task = entry->data_sync_task_.get();
            assert(task != nullptr);
            auto [term_it, inserted] = newest_terms.try_emplace(
                task->node_group_id_, task->node_group_term_);
            if (!inserted)
            {
                term_it->second =
                    std::max(term_it->second, task->node_group_term_);
            }
        }
    }
    return newest_terms;
}

bool IsNewestTerm(const FlushTaskEntry &entry,
                  const NewestTermByNodeGroup &newest_terms)
{
    const DataSyncTask *task = entry.data_sync_task_.get();
    assert(task != nullptr);
    auto term_it = newest_terms.find(task->node_group_id_);
    assert(term_it != newest_terms.end());
    return task->node_group_term_ == term_it->second;
}

std::vector<CkptTsUpdateGroup> CollectCkptTsUpdateGroups(
    const std::vector<std::unique_ptr<FlushTaskEntry>> &table_entries,
    const NewestTermByNodeGroup &newest_terms,
    size_t cc_shard_count)
{
    std::vector<CkptTsUpdateGroup> update_groups;
    if (table_entries.empty())
    {
        return update_groups;
    }

    assert(cc_shard_count > 0);
    const DataSyncTask *first_task =
        table_entries.front()->data_sync_task_.get();
    assert(first_task != nullptr);
    std::unordered_map<NodeGroupId, size_t> group_indices;

    for (const auto &entry : table_entries)
    {
        const DataSyncTask *task = entry->data_sync_task_.get();
        assert(task != nullptr);
        assert(task->table_name_ == first_task->table_name_);
        if (!IsNewestTerm(*entry, newest_terms) ||
            entry->data_sync_vec_ == nullptr)
        {
            continue;
        }

        const size_t core_idx = task->CheckpointCceOwnerCore(cc_shard_count);
        assert(core_idx < cc_shard_count);

        for (const FlushRecord &record : *entry->data_sync_vec_)
        {
            if (record.cce_ == nullptr)
            {
                continue;
            }

            auto [group_it, inserted] = group_indices.try_emplace(
                task->node_group_id_, update_groups.size());
            if (inserted)
            {
                update_groups.emplace_back(CkptTsUpdateGroup{
                    task->node_group_id_, task->node_group_term_, {}});
            }

            CkptTsUpdateGroup &group = update_groups[group_it->second];
            assert(group.node_group_id_ == task->node_group_id_);
            assert(group.node_group_term_ == task->node_group_term_);
            group.cce_entries_[core_idx].emplace_back(
                record.cce_, record.commit_ts_, record.post_flush_size_);
        }
    }

    return update_groups;
}

DataSyncStatus::DataSyncStatus(NodeGroupId node_group_id,
                               int64_t node_group_term,
                               bool need_truncate_log,
                               Origin origin)
    : node_group_id_(node_group_id),
      node_group_term_(node_group_term),
      origin_(origin),
      need_truncate_log_(need_truncate_log)
{
    Sharder::Instance().GetCheckpointer()->IncrementOngoingDataSyncCnt();
}

DataSyncStatus::~DataSyncStatus()
{
    Sharder::Instance().GetCheckpointer()->DecrementOngoingDataSyncCnt();
}

void DataSyncStatus::RecordCheckpointFailure(CheckpointFailureReason reason)
{
    if (origin_ != Origin::Checkpoint ||
        reason == CheckpointFailureReason::None)
    {
        return;
    }

    std::lock_guard<std::mutex> lk(mux_);
    if (checkpoint_failure_reason_ == CheckpointFailureReason::None)
    {
        checkpoint_failure_reason_ = reason;
    }
}

bool CkptCycle::Release()
{
    const uint32_t before = open_count_.fetch_sub(1, std::memory_order_acq_rel);
    assert(before > 0);
    if (before != 1)
    {
        return false;
    }
    Sharder::Instance().GetCheckpointer()->OnCkptCycleConcluded();
    return true;
}

void DataSyncStatus::JoinCkptCycle(std::shared_ptr<CkptCycle> cycle)
{
    assert(ckpt_cycle_ == nullptr);
    cycle->BeginNodeGroup();
    ckpt_cycle_ = std::move(cycle);
}

void DataSyncStatus::LeaveCkptCycle()
{
    assert(ckpt_cycle_ != nullptr);
    std::shared_ptr<CkptCycle> cycle = std::move(ckpt_cycle_);
    cycle->ConcludeNodeGroup();
}

std::optional<DataSyncStatus::CheckpointResult>
DataSyncStatus::TryFinalizeCheckpointLocked()
{
    if (origin_ != Origin::Checkpoint || checkpoint_finalized_ ||
        unfinished_tasks_ != 0)
    {
        return std::nullopt;
    }

    checkpoint_finalized_ = true;
    if (ckpt_cycle_ != nullptr)
    {
        // Every outcome (success, failure, cancellation, skipped entries,
        // merged) concludes this node group's flush in the cycle: the cycle
        // has its verdict on the node group's data. A merged data sync
        // operation gets here only once the task it was merged into has ended,
        // since that task counts as one of its unfinished tasks
        // (DataSyncTask::AbsorbStatus()).
        std::shared_ptr<CkptCycle> cycle = std::move(ckpt_cycle_);
        cycle->ConcludeNodeGroup();
    }
    if (IsCheckpointCancellation(err_code_))
    {
        return CheckpointResult{CheckpointOutcome::Canceled,
                                CheckpointFailureReason::None};
    }
    if (err_code_ != CcErrorCode::NO_ERROR)
    {
        return CheckpointResult{
            CheckpointOutcome::Failure,
            checkpoint_failure_reason_ == CheckpointFailureReason::None
                ? CheckpointFailureReason::Unknown
                : checkpoint_failure_reason_};
    }
    if (no_truncate_reason_ == NoTruncateReason::Deduplicated ||
        no_truncate_reason_ == NoTruncateReason::EntriesSkipped)
    {
        return CheckpointResult{CheckpointOutcome::Neutral,
                                CheckpointFailureReason::None};
    }
    return CheckpointResult{CheckpointOutcome::Success,
                            CheckpointFailureReason::None};
}

bool DataSyncStatus::EndTaskLocked()
{
    // Registered before any worker could run it (RegisterTasks), counted by
    // DataSyncTask::AbsorbStatus(), or the coordinator's count.
    assert(unfinished_tasks_ > 0);
    unfinished_tasks_--;
    return unfinished_tasks_ == 0;
}

bool DataSyncStatus::EndEnqueuingLocked()
{
    assert(unfinished_scan_tasks_ > 0);
    unfinished_scan_tasks_--;
    return EndTaskLocked();
}

std::optional<DataSyncStatus::CheckpointResult> DataSyncStatus::FinishLocked(
    CcHandlerResult<Void> *task_res)
{
    std::optional<CheckpointResult> checkpoint_result =
        TryFinalizeCheckpointLocked();
    if (task_res != nullptr)
    {
        if (err_code_ == CcErrorCode::NO_ERROR)
        {
            task_res->SetFinished();
        }
        else
        {
            task_res->SetError(err_code_);
        }
    }
    cv_.notify_all();
    return checkpoint_result;
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
      tx_number_(txn),
      high_priority_(true)
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
}

size_t DataSyncTask::CheckpointCceOwnerCore(size_t cc_shard_count) const
{
    assert(cc_shard_count > 0);
    assert(id_ >= 0);

    if (table_name_.IsHashPartitioned())
    {
        // Hash-partition checkpoint tasks are created per local core.
        return static_cast<size_t>(id_) % cc_shard_count;
    }

    int32_t range_id = id_;
    if (during_split_range_)
    {
        assert(range_entry_ != nullptr);
        // id_ is the child range ID, but the scanned CCEs are still owned by
        // the parent range's CC shard.
        range_id = range_entry_->GetRangeInfo()->PartitionId();
    }

    return static_cast<size_t>((range_id & 0x3FF) % cc_shard_count);
}

void DataSyncTask::SetFinish()
{
    std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
    std::optional<DataSyncStatus::CheckpointResult> checkpoint_result;
    const bool last_task = status_->EndTaskLocked();
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

    if (last_task)
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
                    bool advanced = Sharder::Instance().UpdateNodeGroupCkptTs(
                        node_group_id_, status_->truncate_log_ts_);
                    if (advanced &&
                        status_->origin_ == DataSyncStatus::Origin::Checkpoint)
                    {
                        Sharder::Instance()
                            .GetCheckpointer()
                            ->RecordCheckpointAdvance(node_group_id_,
                                                      node_group_term_);
                    }

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

#ifndef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
                    if (!is_standby_node_ckpt_ && Sharder::Instance()
                                                      .GetDataStoreHandler()
                                                      ->IsSharedStorage())
                    {
#else
                    if (!is_standby_node_ckpt_)
                    {
#endif
                        BrocastPrimaryCkptTs(node_group_id_,
                                             node_group_term_,
                                             status_->truncate_log_ts_,
                                             status_->HasDataStoreWrite());
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

        checkpoint_result = status_->FinishLocked(task_res_);
    }
    task_sender_lk.unlock();
    ReportCheckpointResult(*status_, checkpoint_result);
    ReleaseAbsorbedStatuses();
}

void DataSyncTask::AbsorbStatus(std::shared_ptr<DataSyncStatus> status)
{
    // A task that has released its attached statuses is no longer any
    // limiter's ckpt_flush_task_; attaching now would never be released.
    assert(!absorbed_released_.load(std::memory_order_acquire));
    {
        std::lock_guard<std::mutex> lk(status->mux_);
        // The data sync operation's coordinator is still enqueuing, so its
        // count is held.
        assert(status->unfinished_tasks_ > 0);
        status->unfinished_tasks_++;
        // Counted apart as well: this task is not one of the data sync
        // operation's own, so it does not make it wait for the flush buffer
        // (DataSyncStatus::WaitsForFlushBufferLocked()).
        status->absorbed_tasks_++;
    }
    absorbed_statuses_.push_back(std::move(status));
}

void DataSyncTask::ReleaseAbsorbedStatuses()
{
    absorbed_released_.store(true, std::memory_order_release);
    for (std::shared_ptr<DataSyncStatus> &absorbed : absorbed_statuses_)
    {
        std::unique_lock<std::mutex> lk(absorbed->mux_);
        // A merged data sync operation neither truncates the log nor has a
        // result handle, so its last task needs no work of its own. It keeps
        // no error: this task's own data sync operation reports a failure
        // once.
        assert(absorbed->absorbed_tasks_ > 0);
        absorbed->absorbed_tasks_--;
        if (absorbed->EndTaskLocked())
        {
            std::optional<DataSyncStatus::CheckpointResult> checkpoint_result =
                absorbed->FinishLocked(nullptr);
            // Reported outside the data sync operation's mutex: the report
            // takes the checkpointer's metrics mutex.
            lk.unlock();
            ReportCheckpointResult(*absorbed, checkpoint_result);
        }
    }
    absorbed_statuses_.clear();
}

void DataSyncTask::SetError(
    CcErrorCode err_code,
    DataSyncStatus::CheckpointFailureReason checkpoint_failure_reason)
{
    std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
    std::optional<DataSyncStatus::CheckpointResult> checkpoint_result;
    const bool last_task = status_->EndTaskLocked();
    status_->err_code_ = err_code;
    if (status_->origin_ == DataSyncStatus::Origin::Checkpoint &&
        !IsCheckpointCancellation(err_code) &&
        status_->checkpoint_failure_reason_ ==
            DataSyncStatus::CheckpointFailureReason::None)
    {
        status_->checkpoint_failure_reason_ = checkpoint_failure_reason;
    }
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

    if (last_task)
    {
        checkpoint_result = status_->FinishLocked(task_res_);
    }
    task_sender_lk.unlock();
    ReportCheckpointResult(*status_, checkpoint_result);
    ReleaseAbsorbedStatuses();
}

void DataSyncTask::SetScanTaskFinished()
{
    std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
    status_->unfinished_scan_tasks_--;
    if (status_->WaitsForFlushBufferLocked())
    {
        // If all scan tasks are finished, but there are still unfinished data
        // sync task due to pending flush, flush the current flush buffer.
        DLOG(INFO) << "Flushing current flush buffer after all scan tasks are "
                      "finished";
        task_sender_lk.unlock();
        Sharder::Instance().GetLocalCcShards()->FlushCurrentFlushBuffer();
    }
}

void DataSyncTask::ResetRangeSplittingStatus()
{
    if (!high_priority_ || during_split_range_)
    {
        return;
    }

    WaitableCc reset_cc(
        [&](CcShard &ccs)
        {
            ccs.ResetRangeSplittingStatus(table_name_, node_group_id_, id_);
            return true;
        });

    LocalCcShards *local_cc_shards = Sharder::Instance().GetLocalCcShards();
    uint16_t dest_core = (id_ & 0x3FF) % local_cc_shards->Count();
    local_cc_shards->EnqueueToCcShard(dest_core, &reset_cc);
    reset_cc.Wait();
}

}  // namespace txservice
