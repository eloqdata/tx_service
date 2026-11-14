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

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

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

/** Returns whether ckpt-ts publication must wait beyond base-partition writes.
 */
inline bool DeferCkptTsUpdate(bool need_persist_kv, bool enable_mvcc)
{
    return need_persist_kv || enable_mvcc;
}

struct DataSyncTask;

/**
 * @brief One checkpoint cycle: the checkpointer (Checkpointer::Ckpt()) starts
 * a flush of every node group this node leads or is recovering, and the cycle
 * concludes when all of those flushes have concluded.
 *
 * A node group's flush is one data sync operation (DataSyncStatus). It
 * concludes when the data sync operation finalizes, or when the checkpointer
 * abandons it because the node group's leader term changed. It stays open while
 * any of its data sync tasks is unfinished, including an earlier cycle's
 * checkpoint flush task it was merged into (DataSyncTask::AbsorbStatus()). A
 * node group whose checkpoint timestamp is pinned by a write lock is skipped.
 *
 * Each concluded cycle advances the node's count
 * (Checkpointer::OnCkptCycleConcluded()), which ages requests parked for
 * shard memory.
 */
struct CkptCycle
{
    /** @brief Adds a node group's flush that must conclude first. */
    void BeginNodeGroup()
    {
        open_count_.fetch_add(1, std::memory_order_acq_rel);
    }

    /**
     * @brief Concludes a node group's flush added by BeginNodeGroup().
     * @return True when this call concluded the cycle.
     */
    bool ConcludeNodeGroup()
    {
        return Release();
    }

    /**
     * @brief Releases the checkpointer's hold once it has started every node
     * group's flush (see open_count_).
     * @return True when this call concluded the cycle.
     */
    bool ReleaseCheckpointerHold()
    {
        return Release();
    }

private:
    /**
     * @brief Lowers open_count_. The call that brings it to zero concludes the
     * cycle and advances the node's concluded-cycle count.
     * @return True when this call concluded the cycle.
     */
    bool Release();

    /**
     * Node group flushes not yet concluded, plus one hold the checkpointer
     * keeps until it has started every node group's flush. Without the hold, a
     * node group whose flush concludes before the next one starts would
     * conclude the cycle early.
     */
    std::atomic<uint32_t> open_count_{1};
};

/**
 * @brief The shared state of one data sync operation: a flush of one node
 * group's data to the data store, carried out by data sync tasks, one per
 * range or per core's share of a hash-partitioned table.
 *
 * The data sync operation's coordinator creates the tasks, enqueues them for
 * the data sync workers, and then signals that all of them are enqueued
 * (EndEnqueuingLocked()). Coordinators, by origin:
 * - Checkpoint: Checkpointer::Ckpt(), one data sync operation per node group
 *   in a checkpoint cycle (CkptCycle spans the cycle's data sync operations).
 * - Snapshot: SnapshotManager::RunOneRoundCheckpoint(), before a snapshot for
 *   a standby or a backup.
 * - FlushData: CcNodeService::FlushDataAll(), one table, for a remote node.
 * - CreateIndex: LocalCcShards::EnqueueDataSyncTaskForTable(), a new index
 *   table.
 * - RangeSplit: LocalCcShards::EnqueueDataSyncTaskForSplittingRange(), a
 *   splitting range and its new ranges.
 * - Migration: LocalCcShards::EnqueueDataSyncTaskForBucket(), the data of
 *   migrating buckets.
 */
struct DataSyncStatus
{
    /** The kind of coordinator that started the data sync operation. */
    enum class Origin
    {
        Checkpoint,
        Snapshot,
        FlushData,
        CreateIndex,
        RangeSplit,
        Migration
    };

    /** Explains why successful work cannot advance the checkpoint watermark. */
    enum class NoTruncateReason
    {
        None,
        Deduplicated,
        EntriesSkipped,
        NotCheckpoint
    };

    /** Terminal checkpoint classification consumed by observability state. */
    enum class CheckpointOutcome
    {
        Success,
        Failure,
        Neutral,
        Canceled
    };

    /** Stable low-cardinality stage attribution for checkpoint failures. */
    enum class CheckpointFailureReason
    {
        None,
        Scan,
        CopyBase,
        PutBase,
        PutArchive,
        Persist,
        Metadata,
        Unknown
    };

    /** Exactly-once terminal result returned by checkpoint finalization. */
    struct CheckpointResult
    {
        CheckpointOutcome outcome_;
        CheckpointFailureReason failure_reason_;
    };

    explicit DataSyncStatus(NodeGroupId node_group_id,
                            int64_t node_group_term,
                            bool need_truncate_log,
                            Origin origin);

    ~DataSyncStatus();

    /**
     * @brief Counts new tasks of the data sync operation, as unfinished and
     * as unfinished scans, before they are put where a worker can run them.
     *
     * The coordinator calls this before it puts a task on a worker queue or on
     * a task limiter's waiting queue, so that the counts never drop below the
     * number of tasks still running. It does so only before
     * EndEnqueuingLocked().
     *
     * @param count How many tasks.
     */
    void RegisterTasks(uint32_t count = 1)
    {
        std::lock_guard<std::mutex> lk(mux_);
        // A count that reached zero finished the data sync operation.
        assert(unfinished_tasks_ > 0);
        unfinished_tasks_ += count;
        unfinished_scan_tasks_ += count;
    }

    void SetNoTruncateLog(
        NoTruncateReason reason = NoTruncateReason::Deduplicated)
    {
        std::lock_guard<std::mutex> lk(mux_);
        need_truncate_log_ = false;
        if (no_truncate_reason_ == NoTruncateReason::None)
        {
            no_truncate_reason_ = reason;
        }
    }

    void SetEntriesSkippedAndNoTruncateLog()
    {
        std::lock_guard<std::mutex> lk(mux_);
        has_skipped_entries_ = true;
        need_truncate_log_ = false;
        no_truncate_reason_ = NoTruncateReason::EntriesSkipped;
    }

    /**
     * Records the first checkpoint failure stage observed by the data sync
     * operation.
     */
    void RecordCheckpointFailure(CheckpointFailureReason reason);

    /**
     * Finalizes checkpoint observability exactly once after all tasks finish.
     * The caller must hold mux_. Data sync operations of other origins return
     * nullopt.
     * Finalizing also concludes this node group's flush in its checkpoint
     * cycle, whatever the outcome.
     */
    std::optional<CheckpointResult> TryFinalizeCheckpointLocked();

    /**
     * @brief Counts one task of the data sync operation as ended: a task of
     * its own finished or failed (DataSyncTask::SetFinish(),
     * DataSyncTask::SetError()), or the earlier checkpoint flush task it was
     * merged into did (DataSyncTask::ReleaseAbsorbedStatuses()). The caller
     * holds mux_.
     * @return True when the count reached zero: that task was the last, and
     * the coordinator has signalled that all tasks are enqueued. The caller
     * then does its own last-task work and calls FinishLocked().
     */
    bool EndTaskLocked();

    /**
     * @brief Signals that the coordinator has created and enqueued every task
     * of the data sync operation, by dropping the count the coordinator has
     * held since construction. The coordinator calls it once, holding mux_.
     * @return True when the count reached zero: every task has already ended,
     * or none was created. The coordinator then does the last-task work and
     * calls FinishLocked(), as the last task would have.
     */
    bool EndEnqueuingLocked();

    /**
     * @brief Finishes the data sync operation once the count reached zero:
     * finalizes the checkpoint outcome (TryFinalizeCheckpointLocked()),
     * signals the result handle with err_code_, and wakes the threads waiting
     * on cv_. The caller holds mux_, and reports the returned outcome after
     * releasing it.
     * @param task_res The result handle the data sync operation's requester
     * waits on, or nullptr.
     * @return The checkpoint outcome, when this call finalized it.
     */
    std::optional<CheckpointResult> FinishLocked(
        CcHandlerResult<Void> *task_res);

    /**
     * @brief Adds this data sync operation, a node group's flush, to its
     * checkpoint cycle. Only the checkpointer calls it, once, before it
     * creates any task, so no other thread can reach ckpt_cycle_ yet and no
     * lock is taken.
     * @param cycle The cycle; the flush concludes in it when the data sync
     * operation finalizes or is abandoned.
     */
    void JoinCkptCycle(std::shared_ptr<CkptCycle> cycle);

    /**
     * @brief Concludes this node group's flush in its cycle without
     * finalizing the data sync operation, for the checkpointer's exits that
     * never signal that all tasks are enqueued, such as a leadership change
     * after some were.
     *
     * Only the checkpointer calls it, once, after JoinCkptCycle(). On those
     * exits the count never reaches zero, so TryFinalizeCheckpointLocked(),
     * the only other code that touches ckpt_cycle_, never runs, and no lock
     * is taken.
     */
    void LeaveCkptCycle();

    void MarkDataStoreWrite()
    {
        has_data_store_write_.store(true, std::memory_order_release);
    }

    bool HasDataStoreWrite() const
    {
        return has_data_store_write_.load(std::memory_order_acquire);
    }

    /**
     * @brief Whether the current flush buffer must be flushed now on this data
     * sync operation's account: every scan has finished and the coordinator
     * has signalled that all tasks are enqueued, while a task of its own is
     * still unfinished, so that task only waits for the buffer to be flushed.
     *
     * A task this data sync operation was merged into
     * (DataSyncTask::AbsorbStatus()) is not one of its own. This data sync
     * operation put nothing in the buffer for it, and that task's own data
     * sync operation asks for the flush when its scans finish; flushing here
     * would only cut short the batching of the tasks still scanning. The
     * caller holds mux_.
     * @return True when the caller should flush the current flush buffer
     * (LocalCcShards::FlushCurrentFlushBuffer()).
     */
    bool WaitsForFlushBufferLocked() const
    {
        return unfinished_scan_tasks_ == 0 &&
               unfinished_tasks_ > absorbed_tasks_;
    }

    NodeGroupId node_group_id_;
    int64_t node_group_term_;
    const Origin origin_;
    /**
     * @brief Tasks of the data sync operation whose scan has not finished,
     * plus one held by the coordinator until EndEnqueuingLocked(). Once it is
     * zero, its own tasks still unfinished only wait for the current flush
     * buffer, which is then flushed (WaitsForFlushBufferLocked()).
     */
    int32_t unfinished_scan_tasks_{1};
    /**
     * @brief Unfinished tasks of the data sync operation, plus one held by
     * the coordinator until EndEnqueuingLocked(). A task finishes once its
     * data are scanned and flushed to the data store.
     *
     * Tasks are added only while the coordinator holds its count, so zero
     * means every task has ended. Whoever brings the count to zero, the last
     * task or the coordinator, calls FinishLocked().
     */
    int32_t unfinished_tasks_{1};
    /**
     * @brief How many of unfinished_tasks_ are not tasks of this data sync
     * operation but earlier checkpoint flush tasks it was merged into
     * (DataSyncTask::AbsorbStatus()), until those tasks end
     * (DataSyncTask::ReleaseAbsorbedStatuses()).
     */
    int32_t absorbed_tasks_{0};

    CcErrorCode err_code_{CcErrorCode::NO_ERROR};
    // True if need to truncate redo log when all tasks succeed.
    bool need_truncate_log_{true};
    NoTruncateReason no_truncate_reason_{NoTruncateReason::None};
    uint64_t truncate_log_ts_{0};
    // Collect from each data sync task.
    size_t total_entry_cnt_{0};

    // Whether there are entries being skipped by DataSyncScan. For EloqKV,
    // entries with buffer commands might be skipped.
    bool has_skipped_entries_{false};

    CheckpointFailureReason checkpoint_failure_reason_{
        CheckpointFailureReason::None};
    bool checkpoint_finalized_{false};
    /**
     * The checkpoint cycle of this node group's flush; null once concluded.
     * Set by JoinCkptCycle(), and cleared by either LeaveCkptCycle() or
     * TryFinalizeCheckpointLocked(), never both.
     */
    std::shared_ptr<CkptCycle> ckpt_cycle_;

    // Whether there is any data written to datastore in this DataSync round.
    std::atomic<bool> has_data_store_write_{false};

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
        bool is_standby_node_ckpt = false,
        bool high_priority = false)
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
          high_priority_(high_priority)
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

    void SetError(
        CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR,
        DataSyncStatus::CheckpointFailureReason checkpoint_failure_reason =
            DataSyncStatus::CheckpointFailureReason::Unknown);

    /**
     * @brief Merges a later checkpoint cycle's flush of this task's partition
     * into this task.
     *
     * This task is a checkpoint flush task that has not started yet (the task
     * limiter's ckpt_flush_task_). When it starts, it flushes the partition up
     * to the later cycle's timestamp as well, so the later cycle creates no
     * task of its own for the partition. Instead, the later cycle's data sync
     * operation, @p status, counts this task as one of its unfinished tasks.
     * When this task finishes or fails, ReleaseAbsorbedStatuses() removes
     * that count again.
     *
     * The caller holds LocalCcShards::task_limiter_mux_, which this task also
     * takes to start (DataSyncTaskLimiter::TakeLatestCkptFlushTs()), so it
     * cannot start, let alone finish, before this call returns.
     *
     * @param status The later checkpoint cycle's data sync operation for this
     * task's node group.
     */
    void AbsorbStatus(std::shared_ptr<DataSyncStatus> status);

    /**
     * @brief Removes this task from the unfinished-task count of every data
     * sync operation merged into it by AbsorbStatus(), once this task has
     * finished or failed.
     *
     * A data sync operation whose count reaches zero finishes, and its node
     * group's flush concludes in its checkpoint cycle.
     */
    void ReleaseAbsorbedStatuses();

    // Decrease unfinished_scan_tasks_ by 1. If all scan tasks are finished,
    // and there are still unfinished data sync tasks, that means there might
    // be in flight flush task in the flush data buffer. Manually flush the
    // flush data buffer.
    void SetScanTaskFinished();

    // Once the range size reaches the threshold, a DataSyncTask is created to
    // trigger the split range operation, and a flag is set indicating that the
    // range has been split. This flag needs to be reset after the DataSyncTask
    // completes.
    void ResetRangeSplittingStatus();

    void SetErrorCode(
        CcErrorCode err_code,
        DataSyncStatus::CheckpointFailureReason checkpoint_failure_reason =
            DataSyncStatus::CheckpointFailureReason::Unknown)
    {
        std::unique_lock<std::mutex> lk(status_->mux_);
        status_->err_code_ = err_code;
        if (status_->origin_ == DataSyncStatus::Origin::Checkpoint &&
            err_code != CcErrorCode::NG_TERM_CHANGED &&
            err_code != CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
            status_->checkpoint_failure_reason_ ==
                DataSyncStatus::CheckpointFailureReason::None)
        {
            status_->checkpoint_failure_reason_ = checkpoint_failure_reason;
        }
    }

    bool SyncTsAdjustable() const
    {
        return sync_ts_adjustable_;
    }

    /**
     * @brief The cc shard that owns the CCEs this task's flush collected.
     *
     * Range data is sharded by range id. A split-child task scans CCEs that
     * remain in the source range's CcMap until split cleanup completes, so
     * during a split the owning core derives from the parent range's id, not
     * the child's.
     */
    size_t CheckpointCceOwnerCore(size_t cc_shard_count) const;

    void UnsetSyncTsAdjustable()
    {
        sync_ts_adjustable_ = false;
    }

    const TableName table_name_;
    // What this identifies depends on the table's partitioning scheme. Hash
    // partition: the cc shard / core index whose scan produces this task's
    // records (a round creates one task per core). Range partition: the range
    // id; the owning core is derived as (id_ & 0x3FF) % core_cnt, and one
    // task covers one range.
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
    /**
     * Data sync operations of later checkpoint cycles merged into this task
     * (AbsorbStatus()), released when this task finishes or fails
     * (ReleaseAbsorbedStatuses()). Written only while this task is the task
     * limiter's ckpt_flush_task_, under LocalCcShards::task_limiter_mux_.
     * Read only after this task started, which takes the same mutex
     * (DataSyncTaskLimiter::TakeLatestCkptFlushTs()), so the writes are
     * visible.
     */
    std::vector<std::shared_ptr<DataSyncStatus>> absorbed_statuses_;
    /**
     * True once ReleaseAbsorbedStatuses() has run. AbsorbStatus() asserts it is
     * false: a task stops being ckpt_flush_task_ before it can finish.
     */
    std::atomic<bool> absorbed_released_{false};
    // True if need to use the dirty schema.
    bool is_dirty_{false};
    /**
     * True for a checkpoint flush task until it starts. Until then, later
     * checkpoint rounds raise the timestamp it flushes up to through the task
     * limiter's latest_ckpt_flush_ts_, which the task adopts when it starts
     * (DataSyncTaskLimiter::TakeLatestCkptFlushTs()).
     */
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

    bool high_priority_{false};
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

using FlushTaskEntryMap =
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>;
using NewestTermByNodeGroup = std::unordered_map<NodeGroupId, int64_t>;

/**
 * @brief Finds the highest task term represented for each node group.
 * @param flush_task A merged flush batch, potentially spanning tables and
 * node-group terms.
 * @return The highest term in the whole batch for every represented node
 * group.
 */
NewestTermByNodeGroup FindNewestTerms(const FlushTaskEntryMap &flush_task);

/**
 * @brief Returns whether an entry belongs to its node group's newest term.
 * @param entry The entry to test.
 * @param newest_terms Batch-wide newest terms returned by FindNewestTerms().
 */
bool IsNewestTerm(const FlushTaskEntry &entry,
                  const NewestTermByNodeGroup &newest_terms);

/** Deferred publication metadata for one table and node group. */
struct CkptTsUpdateGroup
{
    NodeGroupId node_group_id_;
    int64_t node_group_term_;
    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        cce_entries_;
};

/**
 * @brief Collects deferred ckpt-ts updates for one table bucket.
 * @param table_entries All entries grouped under one physical kv table.
 * @param newest_terms Batch-wide newest terms returned by FindNewestTerms().
 * @param cc_shard_count Number of local cc shards, used to map range ids.
 * @return One update group per represented node group. Lower-term entries are
 * omitted because their datastore writes are discarded from the same batch.
 */
std::vector<CkptTsUpdateGroup> CollectCkptTsUpdateGroups(
    const std::vector<std::unique_ptr<FlushTaskEntry>> &table_entries,
    const NewestTermByNodeGroup &newest_terms,
    size_t cc_shard_count);

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

    FlushTaskEntryMap flush_task_entries_;
    size_t pending_flush_size_{0};
    size_t max_pending_flush_size_{0};
    bthread::Mutex flush_task_entries_mux_;
};

}  // namespace txservice
