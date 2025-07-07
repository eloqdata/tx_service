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

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "catalog_factory.h"
#include "cc_handler_result.h"
#include "cc_req_misc.h"
#include "sharder.h"
#include "tx_key.h"
#include "type.h"

namespace txservice
{
extern bool txservice_skip_wal;

struct DataSyncTask;

struct DataSyncStatus
{
    explicit DataSyncStatus(NodeGroupId node_group_id,
                            int64_t node_group_term,
                            bool need_truncate_log)
        : node_group_id_(node_group_id),
          node_group_term_(node_group_term),
          need_truncate_log_(need_truncate_log)
    {
    }

    void SetNoTruncateLog()
    {
        std::lock_guard<std::mutex> lk(mux_);
        need_truncate_log_ = false;
    }

    void PersistKV();

    bool PersistKV(
        const std::string &kv_table_name,
        absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
            &&cce_entries,
        std::shared_ptr<DataSyncTask> task,
        bool all_task_finished);

    NodeGroupId node_group_id_;
    int64_t node_group_term_;
    int32_t unfinished_tasks_{0};
    bool all_task_started_{false};
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};
    // True if need to truncate redo log when all tasks succeed.
    bool need_truncate_log_{true};
    uint64_t truncate_log_ts_{0};
    // Collect from each data sync task.
    size_t total_entry_cnt_{0};

    // If kvstore need do PersistKV after PutAll, we must update ccentry's
    // ckpt_ts after PersistKV done. Since we do data sync in small baches and
    // if PersistKV is called every time PutAll is done, it will cause excessive
    // write pressure on KVStore. So, we can accumulate a large batch of data
    // and then call PersistKV at once.
    std::mutex task_mux_;
    std::vector<std::shared_ptr<DataSyncTask>> tasks_;
    absl::flat_hash_map<
        size_t,
        std::vector<std::vector<UpdateCceCkptTsCc::CkptTsEntry>>>
        cce_entries_;
    std::unordered_set<std::string> dedup_kv_table_names_;

    std::mutex mux_;
    std::condition_variable cv_;
};

struct TableRangeEntry;

// On HashPartition, we handle DataSyncTask separatelly by core.
// On RangePartition, we handle DataSyncTask separatelly by range.
struct DataSyncTask
{
public:
    DataSyncTask(const TableName &table_name,
                 int32_t range_id,
                 uint64_t range_version,
                 uint32_t ng_id,
                 int64_t ng_term,
                 uint64_t data_sync_ts,
                 std::shared_ptr<DataSyncStatus> status,
                 bool is_dirty,
                 bool need_adjust_ts,
                 CcHandlerResult<Void> *hres
#ifndef RANGE_PARTITION_ENABLED
                 ,
                 std::function<bool(size_t)> filter_lambda,
                 bool forward_cache,
                 bool is_standby_node_ckpt,
                 size_t worker_idx
#endif
                 )
        : table_name_(table_name),
          range_id_(range_id),
          range_version_(range_version),
          node_group_id_(ng_id),
          node_group_term_(ng_term),
          data_sync_ts_(data_sync_ts),
#ifndef RANGE_PARTITION_ENABLED
          filter_lambda_(filter_lambda),
          forward_cache_(forward_cache),
          is_standby_node_ckpt_(is_standby_node_ckpt),
          worker_idx_(worker_idx),
#endif
          status_(status),
          is_dirty_(is_dirty),
          sync_ts_adjustable_(need_adjust_ts),
          task_res_(hres)
    {
    }

#ifdef RANGE_PARTITION_ENABLED
    DataSyncTask(const TableName &table_name,
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
                 CcHandlerResult<Void> *hres);
#endif

    void SetFinish();

    void SetError(CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR);

    void SetErrorCode(CcErrorCode err_code)
    {
        std::unique_lock<std::mutex> lk(status_->mux_);
        status_->err_code_ = err_code;
    }

    bool SyncTsAdjustable() const
    {
        return sync_ts_adjustable_;
    }

    void UnsetSyncTsAdjustable()
    {
        sync_ts_adjustable_ = false;
    }

    const TableName table_name_;
    int32_t range_id_;
    uint64_t range_version_;
    uint32_t node_group_id_;
    int64_t node_group_term_{-1};
    uint64_t data_sync_ts_{0};

    enum class CkptErrorCode
    {
        NO_ERROR = 0,
        // Failed on data sync scan
        SCAN_ERROR,
        // Failed on flush data
        FLUSH_ERROR,
        // Term mismatch
        TERM_MISMATCH,
        // Failed to persist data
        PERSIST_KV_ERROR,
    };

    bthread::Mutex flight_task_mux_;
    // Flush data task cnt + 1 (Data sync task)
    int64_t flight_task_cnt_{0};
    CkptErrorCode ckpt_err_{CkptErrorCode::NO_ERROR};
#ifndef RANGE_PARTITION_ENABLED
    std::function<bool(size_t)> filter_lambda_;
    bool forward_cache_{false};
    bool is_standby_node_ckpt_{false};
    size_t worker_idx_;
#endif

    std::shared_ptr<DataSyncStatus> status_{nullptr};
    // True if need to use the dirty schema.
    bool is_dirty_{false};
    // The PendingTaskQueue allows only one normal checkpoint task. Subsequent
    // tasks with larger timestamps will not be added to the PendingTaskqueue.
    // Instead, it will only update the latest_pending_ts_. When the task in the
    // queue is executed, the data_sync_ts_ of task will be updated using
    // latest_pending_ts_.
    bool sync_ts_adjustable_{true};
    // Indicate the single task result.
    CcHandlerResult<Void> *task_res_{nullptr};

#ifdef RANGE_PARTITION_ENABLED
    const TxKey start_key_;
    const TxKey end_key_;
    std::shared_ptr<const TableSchema> table_schema_{nullptr};
    TableRangeEntry *range_entry_{nullptr};
    bool during_split_range_{false};
    bool export_base_table_items_{false};
    uint64_t tx_number_{0};
#endif

    bthread::Mutex update_cce_mux_;
    std::string kv_table_name_;
    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        cce_entries_;
    // std::vector<UpdateCceCkptTsCc::FlushTaskEntry> flush_task_entries_;
};

}  // namespace txservice