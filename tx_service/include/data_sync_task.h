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
#include <utility>

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
                            bool need_truncate_log);

    ~DataSyncStatus();

    void SetEntriesSkippedAndNoTruncateLog()
    {
        std::lock_guard<std::mutex> lk(mux_);
        has_skipped_entries_ = true;
        need_truncate_log_ = false;
    }

    NodeGroupId node_group_id_;
    int64_t node_group_term_;
    // Number of unfinished scan tasks. We keep track of this separately since
    // we need to flush the flush data buffer when all scan tasks are finished.
    int32_t unfinished_scan_tasks_{0};
    // Number of unfinished data sync tasks. A data sync task is finished when
    // the data are scanned and flushed to kvstore.
    int32_t unfinished_tasks_{0};
    bool all_task_started_{false};

    CcErrorCode err_code_{CcErrorCode::NO_ERROR};
    // True if need to truncate redo log when all tasks succeed.
    bool need_truncate_log_{true};
    uint64_t truncate_log_ts_{0};
    // Collect from each data sync task.
    size_t total_entry_cnt_{0};

    // Whether there are entries being skipped by DataSyncScan. For EloqKV,
    // entries with buffer commands might be skipped.
    bool has_skipped_entries_{false};

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
    DataSyncTask(
        const TableName &table_name,
        int32_t id,  // range_id on RangePartition, core_idx on HashPartition
        uint64_t range_version,  // only used on RangePartition table
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        std::shared_ptr<DataSyncStatus> status,
        bool is_dirty,
        bool need_adjust_ts,
        CcHandlerResult<Void> *hres,
        std::function<bool(size_t)> filter_lambda = nullptr,
        bool forward_cache = false,
        bool is_standby_node_ckpt = false)
        : table_name_(table_name),
          id_(id),
          range_version_(range_version),
          node_group_id_(ng_id),
          node_group_term_(ng_term),
          data_sync_ts_(data_sync_ts),
          filter_lambda_(std::move(filter_lambda)),
          forward_cache_(forward_cache),
          is_standby_node_ckpt_(is_standby_node_ckpt),
          status_(std::move(status)),
          is_dirty_(is_dirty),
          sync_ts_adjustable_(need_adjust_ts),
          task_res_(hres),
          need_update_ckpt_ts_(true)
    {
    }

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
                 CcHandlerResult<Void> *hress);

    void SetFinish();

    void SetError(CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR);

    // Decrease unfinished_scan_tasks_ by 1. If all scan tasks are finished,
    // and there are still unfinished data sync tasks, that means there might
    // be in flight flush task in the flush data buffer. Manually flush the
    // flush data buffer.
    void SetScanTaskFinished();

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
    int32_t id_;
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
    std::function<bool(size_t)> filter_lambda_;
    bool forward_cache_{false};
    bool is_standby_node_ckpt_{false};

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

    // Used for data sync task generated by range split op. It only syncs a
    // subrange of an existing range.
    const TxKey start_key_;
    const TxKey end_key_;
    std::shared_ptr<const TableSchema> table_schema_{nullptr};
    TableRangeEntry *range_entry_{nullptr};
    bool during_split_range_{false};
    bool export_base_table_items_{false};
    uint64_t tx_number_{0};

    bthread::Mutex update_cce_mux_;
    std::string kv_table_name_;
    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        cce_entries_;

    bool need_update_ckpt_ts_{true};
};

struct FlushTaskEntry
{
public:
    FlushTaskEntry(
        std::unique_ptr<std::vector<FlushRecord>> &&data_sync_vec,
        std::unique_ptr<std::vector<FlushRecord>> &&archive_vec,
        std::unique_ptr<std::vector<std::pair<TxKey, int32_t>>> &&mv_base_vec,
        TransactionExecution *data_sync_txm,
        std::shared_ptr<DataSyncTask> data_sync_task,
        std::shared_ptr<const TableSchema> table_schema,
        size_t size)
        : data_sync_vec_(std::move(data_sync_vec)),
          archive_vec_(std::move(archive_vec)),
          mv_base_vec_(std::move(mv_base_vec)),
          data_sync_txm_(data_sync_txm),
          data_sync_task_(std::move(data_sync_task)),
          table_schema_(std::move(table_schema)),
          size_(size)
    {
    }

    ~FlushTaskEntry() = default;

    std::unique_ptr<std::vector<FlushRecord>> data_sync_vec_;
    std::unique_ptr<std::vector<FlushRecord>> archive_vec_;
    std::unique_ptr<std::vector<std::pair<TxKey, int32_t>>> mv_base_vec_;
    TransactionExecution *data_sync_txm_{nullptr};
    std::shared_ptr<DataSyncTask> data_sync_task_{nullptr};
    std::shared_ptr<const TableSchema> table_schema_{nullptr};
    size_t size_{0};
};

struct FlushDataTask
{
public:
    FlushDataTask(size_t max_pending_flush_size = 100 * 1024 * 1024)
        : max_pending_flush_size_(max_pending_flush_size)
    {
    }
    ~FlushDataTask() = default;

    /**
     * @brief Add a flush task entry to the flush task.
     * @param entry The flush task entry to add.
     */
    void AddFlushTaskEntry(std::unique_ptr<FlushTaskEntry> &&entry)
    {
        std::lock_guard<bthread::Mutex> lk(flush_task_entries_mux_);
        pending_flush_size_ += entry->size_;
        auto table_flush_entries_it = flush_task_entries_.try_emplace(
            entry->table_schema_->GetKVCatalogInfo()->GetKvTableName(
                entry->data_sync_task_->table_name_));
        table_flush_entries_it.first->second.emplace_back(std::move(entry));
    }

    bool IsFull()
    {
        std::lock_guard<bthread::Mutex> lk(flush_task_entries_mux_);
        return pending_flush_size_ > max_pending_flush_size_;
    }

    bool IsEmpty()
    {
        std::lock_guard<bthread::Mutex> lk(flush_task_entries_mux_);
        return pending_flush_size_ == 0;
    }

    size_t GetPendingFlushSize()
    {
        std::lock_guard<bthread::Mutex> lk(flush_task_entries_mux_);
        return pending_flush_size_;
    }

    size_t GetFlushBufferSize() const
    {
        return max_pending_flush_size_;
    }

    /**
     * @brief Merge all entries from another FlushDataTask into this one.
     * @param other The FlushDataTask to merge from. It will be emptied after
     * merging.
     * @return true if merge was successful, false if merge would exceed
     * max_pending_flush_size_
     */
    bool MergeFrom(std::unique_ptr<FlushDataTask> &&other)
    {
        // Lock both mutexes in consistent order (by address) to avoid deadlock
        bthread::Mutex *m1 = &flush_task_entries_mux_;
        bthread::Mutex *m2 = &other->flush_task_entries_mux_;
        if (m1 > m2)
        {
            std::swap(m1, m2);
        }

        std::lock_guard<bthread::Mutex> lk1(*m1);
        std::lock_guard<bthread::Mutex> lk2(*m2);

        // Check if merge would exceed max size
        if (pending_flush_size_ + other->pending_flush_size_ >
            max_pending_flush_size_)
        {
            return false;
        }

        // Merge entries by table name
        for (auto &[table_name, entries] : other->flush_task_entries_)
        {
            auto table_flush_entries_it =
                flush_task_entries_.try_emplace(table_name);
            auto &target_entries = table_flush_entries_it.first->second;
            target_entries.insert(target_entries.end(),
                                  std::make_move_iterator(entries.begin()),
                                  std::make_move_iterator(entries.end()));
        }

        // Update size
        pending_flush_size_ += other->pending_flush_size_;

        // Clear the other task
        other->pending_flush_size_ = 0;
        other->flush_task_entries_.clear();

        return true;
    }

    std::unique_ptr<FlushDataTask> MoveFlushData(bool force)
    {
        std::lock_guard<bthread::Mutex> lk(flush_task_entries_mux_);
        if ((force && pending_flush_size_ > 0) ||
            pending_flush_size_ > max_pending_flush_size_)
        {
            std::unique_ptr<FlushDataTask> ret =
                std::make_unique<FlushDataTask>(max_pending_flush_size_);
            ret->pending_flush_size_ = pending_flush_size_;
            ret->flush_task_entries_ = std::move(flush_task_entries_);
            pending_flush_size_ = 0;
            flush_task_entries_.clear();
            return ret;
        }
        return nullptr;
    }

    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        flush_task_entries_;
    size_t pending_flush_size_{0};
    size_t max_pending_flush_size_{0};
    bthread::Mutex flush_task_entries_mux_;
};

}  // namespace txservice
