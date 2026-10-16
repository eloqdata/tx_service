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

#include <thread>
#include <vector>

#include "tx_operation.h"
#include "type.h"

namespace txservice
{
struct UpsertTableIndexOp : public SchemaOp
{
    UpsertTableIndexOp() = delete;
    UpsertTableIndexOp(const std::string_view table_name_sv,
                       TableEngine table_engine,
                       const std::string &current_image,
                       uint64_t curr_schema_ts,
                       const std::string &dirty_image,
                       const std::string &alter_table_image,
                       OperationType op_type,
                       PackSkError *store_pack_sk_err,
                       TransactionExecution *txm);

    ~UpsertTableIndexOp()
    {
        // Make sure background workers exit before the op is destroyed.
        for (auto &worker : local_task_workers_)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }
        local_task_workers_.clear();

        if (!is_last_finished_key_str_)
        {
            last_finished_end_key_.~TxKey();
        }
    }

    void Forward(TransactionExecution *txm) override;

    void Reset(const std::string_view table_name_str,
               TableEngine table_engine,
               const std::string &current_image,
               uint64_t curr_schema_ts,
               const std::string &dirty_image,
               const std::string &alter_table_image,
               OperationType op_type,
               PackSkError *store_pack_sk_err,
               TransactionExecution *txm);

    /**
     * @brief The current stage of this multi-stage schema operation.
     */
    TransactionOperation *op_{nullptr};
    /**
     * @brief Acquire read lock on local cluster config ccmap to block cluster
     * config update during upsert table op. We cannot allow config update
     * between acquire write all and post write all. The op's first step; it
     * holds the lock until its last post write all
     * (unlock_cluster_config_op_).
     */
    ReadLocalOperation lock_cluster_config_op_;
    /**
     * @brief Acquires write intents on the table's catalog in all nodes to
     * prevent concurrent schema modifications.
     */
    AcquireAllOp acquire_all_intent_op_;
    /**
     * @brief Upgrade write intent to write lock on all nodes. To prevent
     * concurrent DDL and DML on the table. Then get a boundary between new
     * and old tuples.
     */
    AcquireAllOp upgrade_all_intent_to_lock_op_;
    /**
     * @brief For add index, creates the kv table of each index being added
     * (DataStoreHandler::CreateKvTable), before the prepare log and under the
     * prepare-stage write lock.
     *
     * Recovery returns a node to the state right after the prepare log, in
     * which the dirty schema is installed and DML writes the new index, so its
     * kv table must exist in every state recovery can reach. The kv catalog is
     * written only after the commit log (kv_update_schema_image_op_).
     */
    AsyncOp<Void> create_kv_table_op_;
    /**
     * @brief Drops the kv tables create_kv_table_op_ created, when the add
     * index fails before its prepare log is flushed. A coordinator failure
     * before it finishes leaves them orphaned, which is harmless: no schema
     * refers to them.
     */
    AsyncOp<Void> drop_kv_table_op_;
    /**
     * @brief Flushes the prepare log to the log service. The schema operation
     * is guaranteed to succeed after this stage.
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Installs the dirty schema in the tx service and returns a local
     * view (pointer) of it, and downgrade all write lock to write intent.
     */
    PostWriteAllOp downgrade_all_lock_to_intent_op_;
    /**
     * @brief Release cluster config read lock after post write all: after the
     * op's last one, post_all_lock_op_, before the clean log.
     */
    PostReadOperation unlock_cluster_config_op_;
    /**
     * @brief Update the sequence table for range id info.
     */
    AsyncOp<Void> update_sequence_table_op_;
    /**
     * @brief Generate sk record from pk record parallelly. The parallel
     * granularity of the operation is range.
     */
    AsyncOp<GenerateSkParallelResult> generate_sk_parallel_op_;
    /**
     * @brief Flush the index data of the old tuples into data store. Consist of
     * scan, flush.
     *
     * NOTE: table_name is the index table name.
     */
    AsyncOp<Void> flush_all_old_tuples_sk_op_;
    /**
     * @brief Flushes the log to the log service. This log confirms that the
     * index data operation of the old tuples succeeds. In term of recovery,
     * this log ensure that write intent is hold before this log, rather than
     * write lock which will block checkpointer(acquire read lock).
     */
    WriteToLogOp prepare_data_log_op_;
    /**
     * @brief Broadcast the dirty schema image to all node groups if it is
     * changed.
     */
    PostWriteAllOp broadcast_dirty_schema_image_op_;
    /**
     * @brief Upgrades acquired write intents to write locks in all nodes.
     */
    AcquireAllOp acquire_all_lock_op_;
    /**
     * @brief Flushes the commit log to the log service. The commit log confirms
     * that the data store operation succeeds and does not need redo upon
     * failures.
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Drop index from kv storage.
     */
    DsUpsertTableOp kv_drop_index_op_;
    /**
     * @brief For creating index, rollback the create index transaction if any
     * constraints violated.
     */
    DsUpsertTableOp kv_rollback_create_index_op_;
    /**
     * @brief For creating index, writes the new schema (with the indexes'
     * multikey attributes) to the kv catalog at the commit stage, after the
     * commit log and under the commit-stage write lock.
     *
     * The index's kv table exists since before the prepare log
     * (create_kv_table_op_), and until the commit log the prepare log carries
     * the dirty schema for any node that recovers. Written at the op's
     * timestamp, so the kv catalog version matches the in-memory schema
     * version; retried until it succeeds, since after the commit log the op
     * only rolls forward. The write lock keeps any later DDL on the table from
     * writing its catalog row first: the DSS RocksDB backends apply writes in
     * arrival order, not by version, and a later drop table deletes the row,
     * leaving nothing a late write could be compared against.
     */
    DsUpsertTableOp kv_update_schema_image_op_;
    /**
     * @brief Clean ccmap on all node groups
     *
     */
    KickoutDataAllOp clean_ccm_op_;

    /**
     * @brief Removes write locks in all nodes. If the schema operation
     * succeeds, also installs the new schema in all nodes.
     */
    PostWriteAllOp post_all_lock_op_;
    /**
     * @brief The last log operation that removes the schema record from the log
     * state machine.
     */
    WriteToLogOp clean_log_op_;

    CcHandlerResult<ReadKeyResult> read_cluster_result_;
    ClusterConfigRecord cluster_conf_rec_;

    // The last finished end key.
    union
    {
        TxKey last_finished_end_key_;
        const std::string *last_finished_end_key_str_;
    };
    bool is_last_finished_key_str_;

    /**
     * @brief Whether the ADD INDEX was rolled back: its build broke a
     * constraint after the prepare log.
     *
     * The op still commits, at its commit-stage timestamp, and the commit log
     * carries this mark instead of the zero commit timestamp that used to mean
     * the same: the index's kv tables are dropped, the catalog row keeps the
     * table's current schema at the commit timestamp, and the second phase, a
     * Rollback at that timestamp, drops the index's cc maps and keeps the
     * current schema instead of promoting the dirty one. Recovery reads it
     * from the commit log.
     */
    bool rolled_back_{false};

public:
    void RecoveryIndexesMultiKeyAttr(const TableSchema *dirty_schema);

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillPrepareDataLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);

    /**
     * @brief Starts create_kv_table_op_, which creates the kv table of each
     * index being added.
     *
     * Called once `commit_ts_` is chosen under the prepare-stage write lock,
     * before the prepare log.
     *
     * @param txm The transaction running this operation.
     */
    void CreateKvTables(TransactionExecution *txm);

    /**
     * @brief Starts drop_kv_table_op_, which drops the kv tables of the
     * indexes being added, and then releases the locks.
     *
     * Called when the add index fails before its prepare log is flushed.
     *
     * @param txm The transaction running this operation.
     */
    void DropKvTables(TransactionExecution *txm);

    /**
     * @brief Writes the prepare log, once `commit_ts_` is chosen and, for add
     * index, the kv tables exist.
     *
     * @param txm The transaction running this operation.
     */
    void WritePrepareLog(TransactionExecution *txm);

    /**
     * @brief Marks the op failed: commit timestamp 0, and a Rollback second
     * phase that discards whatever dirty schema the op installed and leaves
     * the catalog entry's version alone.
     *
     * @param txm The coordinating tx.
     */
    void MarkFailed(TransactionExecution *txm);

    /**
     * @brief Runs a data store step that failed after the prepare log again,
     * KvRetryInterval() later (ArmKvRetryWait); ends the op if the node is no
     * longer the leader.
     *
     * A failure before the prepare log (create_kv_table_op_) is not retried:
     * the op fails.
     *
     * @param txm The coordinating tx.
     * @param step The step to run again: update_sequence_table_op_,
     * flush_all_old_tuples_sk_op_, kv_drop_index_op_,
     * kv_rollback_create_index_op_ or kv_update_schema_image_op_.
     */
    void RetryKvStep(TransactionExecution *txm, TransactionOperation *step);

    /**
     * @brief Fault-injection hold (alter_table_index_hold_before_commit_lock):
     * keeps an add index whose build has finished, after its last prepare data
     * log, from taking its commit-stage write locks until the test disarms the
     * hold, or for hold_s seconds (the fault's parameter "hold_s=N", 60 by
     * default), still holding the cluster config read lock it took first.
     * Arms the alter_table_index_held_before_commit_lock marker while it
     * holds, so a test can tell the op got there.
     *
     * The wait is kv_retry_wait_op_'s blocking work on a worker thread, and
     * the op's kv_retry_wait_op_ stage then runs acquire_all_lock_op_, so the
     * TxProcessor keeps serving other requests meanwhile. Only fault-injection
     * builds call it.
     *
     * @warning While the fault is armed it holds every add index of any table
     * that finishes its build. A test arms it only around the one build it
     * means to hold and disarms it in a finally.
     *
     * @param txm The coordinating tx.
     */
    void HoldBeforeCommitLock(TransactionExecution *txm);

    // Flush pk or sk data from ccmap into data store.
    void FlushDataIntoDataStore(const TableName &table_name,
                                NodeGroupId ng_id,
                                uint64_t data_sync_ts,
                                bool is_dirty,
                                CcHandlerResult<Void> &hres,
                                int64_t ng_term = INIT_TERM);
    // Reset node group leader term
    void ResetLeaderTerms();
    bool NeedTriggerFlushSkOp()
    {
        return (scanned_pk_range_count_ % 6000 == 0) ||
               (last_scanned_end_key_.Type() == KeyType::PositiveInf);
    }
    void DispatchRangeTask(TransactionExecution *upsert_index_txm,
                           CcHandlerResult<GenerateSkParallelResult> &hd_res);
    void HandleRangeTask(
        const TableName &base_table_name,
        int32_t partition_id,
        TxKey range_start_key,
        TxKey range_end_key,
        NodeGroupId range_owner,
        uint64_t scan_ts,
        uint64_t tx_number,
        int64_t tx_term,
        std::mutex &task_mux,
        std::condition_variable &task_cv,
        uint32_t &unfinished_task_cnt,
        bool &all_task_started,
        uint32_t &total_pk_items_count,
        uint32_t &dispatched_task_count,
        CcErrorCode &task_res,
        GenerateSkParallelResult &task_value,
        std::function<void(TxKey batch_range_start_key,
                           TxKey batch_range_end_key,
                           const std::string *batch_range_start_key_str,
                           const std::string *batch_range_end_key_str,
                           TxKey &last_scanned_end_key,
                           bool &is_last_scanned_key_str,
                           size_t batch_range_cnt,
                           uint32_t &actual_task_cnt)> &dispatch_func);

    static std::vector<MultiKeyAttr> InitIndexesMultiKeyAttr(
        const std::vector<TableName> &new_indexes_name);

    static bool IndexesMergeMultiKeyAttr(std::vector<MultiKeyAttr> &from,
                                         std::vector<MultiKeyAttr> &to);

    static std::string RebuildSchemaImage(
        const TableSchema *dirty_schema,
        const std::vector<MultiKeyAttr> &indexes_multikey_attr);

    static std::vector<const KeySchema *> GetIndexesSchema(
        const TableSchema *dirty_schema,
        const std::vector<TableName> &new_indexes_name);

private:
    // This variable have two roles:
    // 1) deserialize as AlterTableInfo object. 2) save into log.
    std::string alter_table_info_image_str_{""};
    AlterTableInfo alter_table_info_;

    // Store the node group leader terms after acquired them.
    std::vector<int64_t> leader_terms_;

    // Due to term or other error, called ForceToFinish to terminate this
    // operation
    bool is_force_finished_{false};

    /**
     * The cluster gate refused the op at its first step: it holds nothing but
     * the cluster config read lock, releases it and ends without a log.
     */
    bool gated_{false};

#ifdef NDEBUG
    static constexpr uint8_t scan_batch_range_size_{10};
#else
    static constexpr uint8_t scan_batch_range_size_{3};
#endif

    union
    {
        TxKey last_scanned_end_key_;
        const std::string *last_scanned_end_key_str_;
    };
    bool is_last_scanned_key_str_;
    std::vector<TableName> new_indexes_name_;
    std::vector<std::thread> local_task_workers_;
    size_t scanned_pk_range_count_{0};
    size_t finished_pk_range_count_{0};
    size_t total_scanned_pk_items_count_{0};

    // - Points to UpsertTableTxRequest::pack_sk_err.
    // - Points to nullptr when recovering.
    PackSkError *store_pack_sk_err_{nullptr};

    // Within one [generate sk -> flush -> prepare data log] round, does index
    // schema changed due to meeting multikey records? Reset to false before
    // each round start.
    bool indexes_changed_within_round_;

    // - Merge multikey attribute to indexes_multikey_attr_ round by round.
    // - Overwrite multikey attribute in dirty schema.
    std::vector<MultiKeyAttr> indexes_multikey_attr_;
};

}  // namespace txservice
