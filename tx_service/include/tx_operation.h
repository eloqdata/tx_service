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

#include <boost/context/continuation.hpp>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog_key_record.h"
#include "cc_entry.h"
#include "cc_map.h"
#include "cluster_config_record.h"
#include "log.pb.h"
#include "log_closure.h"
#include "range_record.h"
#include "read_write_set.h"
#include "schema.h"
#include "tx_command.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "tx_req_result.h"
#include "type.h"

namespace txservice
{
class TransactionExecution;
struct ReadTxRequest;
struct ReadOutsideTxRequest;
struct ScanOpenTxRequest;
struct ScanBatchTxRequest;
struct ScanBatchTuple;
struct AnalyzeTableTxRequest;
struct BroadcastStatisticsTxRequest;
struct BatchReadTxRequest;
struct DataMigrationStatus;
struct ObjectCommandTxRequest;
struct BackfillRec;
struct MultiObjectCommandTxRequest;

#define RETRY_NUM 3

enum class TxLogType
{
    DATA,
    PREPARE,
    COMMIT,
    CLEAN
};

struct TransactionOperation
{
    TransactionOperation()
    {
    }
    virtual ~TransactionOperation() = default;
    /**
     * @brief Called by txm->Forward() for determining the how
     * this operation will be processed, e.g. calling txm->Process(this) if this
     * operation has not been processed, or calling txm->PostProcess(this) if
     * the result has been set finished, or rerun this operation if the result
     * is set error.
     */
    virtual void Forward(TransactionExecution *txm) = 0;

    /**
     * @brief If operation fails since remote node dies, auto-failover will
     * elect a new leader and recover the dead node group. Re-run the operator
     * automatically to avoid client to re-run the whole query.
     *
     */
    void ReRunOp(TransactionExecution *txm);
    /**
     * @brief If the operation is running block command and the command is
     * blocking. True: the operation need to call Forward frequently to know if
     * the commmand has expired.
     */
    virtual bool IsBlockCommand()
    {
        return false;
    }

    int retry_num_{RETRY_NUM};
    bool is_running_{false};
    static constexpr uint64_t tx_op_failed_ts_ = 0;
    metrics::TimePoint op_start_{metrics::TimePoint::max()};
};

struct CompositeTransactionOperation : TransactionOperation
{
    CompositeTransactionOperation();

    virtual ~CompositeTransactionOperation() = default;

    template <typename Op>
    void ForwardToSubOperation(TransactionExecution *txm, Op *next_op);

    template <typename Op>
    void RetrySubOperation(TransactionExecution *txm,
                           Op *next_op,
                           bool retry_immediately = true);

    /**
     * @brief The current stage of this multi-stage schema operation
     */
    TransactionOperation *op_{nullptr};
};

struct ReadLocalOperation : TransactionOperation
{
public:
    void Reset();
    void Reset(TableName tbl_name,
               const TxKey *key,
               TxRecord *rec,
               CcHandlerResult<ReadKeyResult> *hd_res)
    {
        table_name_ = std::move(tbl_name);
        key_ = key;
        rec_ = rec;
        hd_result_ = hd_res;
        execute_immediately_ = true;
    }

    void Forward(TransactionExecution *txm) override;

    // in-parameters
    const TxKey *key_{};
    TableName table_name_{
        empty_sv, TableType::RangePartition, TableEngine::None};
    TxRecord *rec_{};
    bool execute_immediately_{true};

    // out-parameters, to pass result to caller operation
    CcHandlerResult<ReadKeyResult> *hd_result_{};
};

struct ReadOperation : TransactionOperation
{
public:
    explicit ReadOperation(
        TransactionExecution *txm,
        CcHandlerResult<ReadKeyResult> *lock_range_bucket_result = nullptr);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    ReadType read_type_{ReadType::Inside};
    CcProtocol protocol_{CcProtocol::OCC};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    ReadTxRequest *read_tx_req_{nullptr};
    ReadOutsideTxRequest *read_outside_tx_req_{nullptr};
    CcHandlerResult<ReadKeyResult> hd_result_;
    bool local_cache_miss_{false};

    CcHandlerResult<ReadKeyResult> *lock_range_bucket_result_{nullptr};
};

struct PostReadOperation : TransactionOperation
{
public:
    explicit PostReadOperation(TransactionExecution *txm);
    void ResetHandlerTxm(TransactionExecution *txm);

    void Reset(const CcEntryAddr *cce_addr = nullptr,
               const ReadSetEntry *read_set_entry = nullptr);

    void Forward(TransactionExecution *txm) override;

    const CcEntryAddr *cce_addr_;
    const ReadSetEntry *read_set_entry_;
    CcHandlerResult<PostProcessResult> hd_result_;
};

struct SetCommitTsOperation : TransactionOperation
{
public:
    explicit SetCommitTsOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<uint64_t> hd_result_;
};

struct ValidateOperation : TransactionOperation
{
public:
    static const uint32_t default_read_set_capacity = 16;

    explicit ValidateOperation(TransactionExecution *txm);
    void Reset(size_t read_cnt);
    bool IsError();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
};

/**
 * @brief
 * Acquire write lock operation.
 * Write are cached in local rwset for each transaction, hence no write lock is
 * held at write operation. This operation is called right after Commit request.
 */
struct AcquireWriteOperation : TransactionOperation
{
public:
    explicit AcquireWriteOperation(TransactionExecution *txm);
    void Reset(size_t acquire_write_cnt, size_t wentry_cnt);
    void AggregateAcquiredKeys(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<std::vector<AcquireKeyResult>> hd_result_;
    std::vector<WriteSetEntry *> acquire_write_entries_{16};

    // Number of remote keys on which the acquire write operation needs to
    // acquire write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};
    // Identify whether any keys in rset are expired (may be updated by other
    // tx) under the RepeatableRead or Serializable isolation level.
    bool rset_has_expired_{false};
};

struct LockWriteRangeBucketsOp : public TransactionOperation
{
public:
    explicit LockWriteRangeBucketsOp(
        CcHandlerResult<ReadKeyResult> *lock_result)
        : lock_result_(lock_result)
    {
    }

    void Forward(TransactionExecution *txm) override;

    void Reset()
    {
        init_ = false;
        is_running_ = false;
        execute_immediately_ = true;
    }

    /**
     * @brief Advances the internal iterator to the next write key to acquire a
     * write lock. Also Check if current write key needs to be forward written
     * (bucket is migrating).
     */
    void Advance(TransactionExecution *txm);

    TableName range_table_name_{
        empty_sv, TableType::RangePartition, TableEngine::None};
    CcHandlerResult<ReadKeyResult> *lock_result_{nullptr};

    std::unordered_map<TableName, std::pair<uint64_t, TableWriteSet>>::iterator
        table_it_;
    std::unordered_map<TableName, std::pair<uint64_t, TableWriteSet>>::iterator
        table_end_;
    TableWriteSet::iterator write_key_it_;
    TableWriteSet::iterator write_key_end_;
    bool init_;
    bool execute_immediately_{true};
};

struct ReloadCacheOperation : TransactionOperation
{
    ReloadCacheOperation(TransactionExecution *txm);
    void Reset(uint32_t hres_ref_cnt);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> hd_result_;
};

struct FaultInjectOp : TransactionOperation
{
public:
    FaultInjectOp(TransactionExecution *txm);

    void Set(const std::string &fault_name,
             const std::string &fault_paras,
             std::vector<int> vct_node_id)
    {
        fault_name_ = fault_name;
        fault_paras_ = fault_paras;
        vct_node_id_ = vct_node_id;
        succeed_ = false;
    }

    void Reset();
    void Forward(TransactionExecution *txm) override;

    std::string fault_name_;
    std::string fault_paras_;
    std::vector<int> vct_node_id_;
    bool succeed_;
    CcHandlerResult<bool> hd_result_;
};

struct WriteToLogOp : TransactionOperation
{
    WriteToLogOp(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();
    void ResetHandlerTxm(TransactionExecution *txm);

    TxLogType log_type_{TxLogType::DATA};
    uint32_t log_group_id_{0};
    CcHandlerResult<Void> hd_result_;
    LogClosure log_closure_{&hd_result_};
};

struct UpdateTxnStatus : TransactionOperation
{
    UpdateTxnStatus(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> hd_result_;
};

struct PostProcessOp : TransactionOperation
{
    explicit PostProcessOp(TransactionExecution *txm);
    void Reset(size_t write_cnt,
               size_t data_read_cnt,
               size_t meta_data_read_cnt,
               size_t catalog_write_all_cnt,
               bool forward_to_update_txn_op);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> *stage_;

    bool forward_to_update_txn_op_{true};

    // Release data read lock.
    CcHandlerResult<PostProcessResult> hd_result_;

    // Release meta data read lock. AcquireAll/PostAcquireAll should be nested
    // inside cluster config lock.
    CcHandlerResult<PostProcessResult> meta_data_hd_result_;

    // Release catalog all write lock (Alter table inside a DML transaction).
    size_t catalog_write_all_cnt_{0};
    CcHandlerResult<PostProcessResult> catalog_post_all_hd_result_;
};

struct InitTxnOperation : TransactionOperation
{
    explicit InitTxnOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    uint32_t log_group_id_{UINT32_MAX};
    uint32_t tx_ng_id_;
    CcHandlerResult<InitTxResult> hd_result_;
};

struct ScanOpenOperation : TransactionOperation
{
    explicit ScanOpenOperation(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    void Set(const TableName *table_name,
             ScanIndexType index_type,
             const TxKey *key,
             bool inclusive,
             ScanDirection direction,
             bool is_ckpt_delta)
    {
        table_name_ = table_name;
        index_type_ = index_type;
        start_key_ = key;
        inclusive_ = inclusive;
        direction_ = direction;
        is_ckpt_delta_ = is_ckpt_delta;
    }

    void Reset();

    ClusterConfigRecord cluster_config_rec_;
    CcHandlerResult<ReadKeyResult> lock_cluster_config_result_;
    CcHandlerResult<ScanOpenResult> hd_result_;

    const TableName *table_name_{nullptr};
    ScanIndexType index_type_{ScanIndexType::Primary};
    const TxKey *start_key_{nullptr};
    bool inclusive_{true};
    ScanDirection direction_{ScanDirection::Forward};
    bool is_ckpt_delta_{false};
    ScanOpenTxRequest *tx_req_{nullptr};
};

struct ScanState
{
    ScanState() = delete;
    ScanState(std::unique_ptr<CcScanner> scanner,
              std::vector<DataStoreSearchCond> pushdown_cond,
              const TxKey *start_key,
              bool start_inclusive,
              const TxKey *end_key,
              bool end_inclusive)
        : scanner_(std::move(scanner)),
          pushdown_condition_(std::move(pushdown_cond)),
          scan_start_key_(start_key),
          scan_start_inclusive_(start_inclusive),
          scan_end_key_(end_key),
          scan_end_inclusive_(end_inclusive)
    {
    }

    size_t current_plan_index_{SIZE_MAX};
    std::unique_ptr<CcScanner> scanner_;
    std::vector<DataStoreSearchCond> pushdown_condition_;
    const TxKey *scan_start_key_{nullptr};
    bool scan_start_inclusive_{false};
    const TxKey *scan_end_key_{nullptr};
    bool scan_end_inclusive_{false};

    ScanState(std::unique_ptr<CcScanner> scanner,
              uint64_t schema_version,
              const TxKey *end_key,
              bool end_inclusive,
              uint32_t range_id,
              NodeGroupId range_ng,
              TxKey last_key,
              bool inclusive,
              SlicePosition position)
        : scanner_(std::move(scanner)),
          scan_end_key_(end_key),
          scan_end_inclusive_(end_inclusive),
          schema_version_(schema_version),
          range_id_(range_id),
          range_ng_(range_ng),
          slice_last_key_(std::move(last_key)),
          inclusive_(inclusive),
          slice_position_(position)
    {
    }

    ~ScanState() = default;

    void SetSliceLastKey(TxKey slice_last_key)
    {
        slice_last_key_ = std::move(slice_last_key);
    }

    const TxKey *SliceLastKey() const
    {
        return &slice_last_key_;
    }

    uint64_t schema_version_;
    uint32_t range_id_;
    NodeGroupId range_ng_;
    TxKey slice_last_key_;
    bool inclusive_;
    SlicePosition slice_position_;
    CcEntryAddr range_cce_addr_;
};

struct ScanNextOperation : TransactionOperation
{
    explicit ScanNextOperation(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();
    void ResetResult();
    void ResetResultForHashPart(size_t ng_cnt);

    ScanDirection Direction() const
    {
        return scan_state_->scanner_ != nullptr
                   ? scan_state_->scanner_->Direction()
                   : ScanDirection::Forward;
    }

    void UpdateScanState(ScanState *scan_state)
    {
        scan_state_ = scan_state;
        slice_hd_result_.Value().ccm_scanner_ = scan_state->scanner_.get();
        hd_result_.Value().ccm_scanner_ = scan_state->scanner_.get();
    }

    ScanState *scan_state_;
    CcHandlerResult<ScanNextResult> hd_result_;

    int64_t RangeNgTerm() const
    {
        return slice_hd_result_.Value().ccm_scanner_->PartitionNgTerm();
    }

    SlicePosition LastScannedSlicePosition() const
    {
        return scan_state_->slice_position_;
    }

    CcHandlerResult<RangeScanSliceResult> slice_hd_result_;
    TableName range_table_name_{
        empty_sv, TableType::RangePartition, TableEngine::None};
    RangeRecord range_rec_;
    CcHandlerResult<ReadKeyResult> lock_range_result_;
    CcHandlerResult<PostProcessResult> unlock_range_result_;

    size_t alias_{0};
    ScanBatchTxRequest *tx_req_{nullptr};
};

struct AcquireAllOp : public TransactionOperation
{
    AcquireAllOp(TransactionExecution *txm);
    void Resize(size_t new_size);
    void Reset(size_t node_cnt);
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    /**
     * @brief Get the max commit/validate ts of the all result
     */
    uint64_t MaxTs() const;

    bool IsDeadlock() const;

    /**
     * @brief Returns a concrete error code for a failed AcquireAllOp.
     *
     * fail_cnt_ alone cannot supply one: it is also raised on the dedup
     * read-version mismatch path, where every handler result is successful.
     * A failed AcquireAll must never be reported as NO_ERROR, or the caller
     * aborts the transaction while telling the client it succeeded.
     */
    CcErrorCode RepresentativeError() const;

    std::vector<CcHandlerResult<AcquireAllResult>> hd_results_;
    uint32_t upload_cnt_{0};
    std::atomic<uint32_t> finish_cnt_{0};
    std::atomic<uint32_t> fail_cnt_{0};
    // Number of remote keys on which the upload operation needs to acquire
    // write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};

    const TableName *table_name_{nullptr};
    std::vector<TxKey> keys_;

    CcOperation cc_op_{CcOperation::ReadForWrite};
    CcProtocol protocol_{CcProtocol::OCC};
};

struct PostWriteAllOp : public TransactionOperation
{
    PostWriteAllOp(TransactionExecution *txm);
    void Reset(uint32_t ng_cnt);
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    bool IsFailed();

    CcHandlerResult<PostProcessResult> hd_result_;

    const TableName *table_name_{nullptr};
    std::vector<TxKey> keys_;
    std::vector<TxRecord *> recs_;
    OperationType op_type_{OperationType::Upsert};
    PostWriteType write_type_{PostWriteType::PrepareCommit};
};

/**
 * Used in DML commit context. Combine {lock cluster, acquire all intent,
 * acquire all lock} together.
 *
 * Here acquire all intent serves two purposes: 1) Prevent dead lock on single
 * catalog key; 2) Prepare for post all intent. Post all intent is used to build
 * TableSchema object.
 */
struct CatalogAcquireAllOp : public TransactionOperation
{
    explicit CatalogAcquireAllOp(TransactionExecution *txm);
    void Reset();
    void SetCatalogWriteSet(const std::map<TxKey, ReplicaWriteSetEntry> &wset);
    uint64_t MaxTs() const;
    void Forward(TransactionExecution *txm) override;

    bool succeed_;
    TransactionOperation *op_;

    ReadLocalOperation lock_cluster_config_op_;
    AcquireAllOp acquire_all_intent_op_;
    AcquireAllOp acquire_all_lock_op_;

private:
    CcHandlerResult<ReadKeyResult> read_cluster_result_;
    ClusterConfigRecord cluster_conf_rec_;
};

struct KickoutDataOp : public TransactionOperation
{
    KickoutDataOp(TransactionExecution *txm);
    void Reset();
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    const TableName *table_name_{nullptr};
    NodeGroupId node_group_;
    TxKey start_key_;
    TxKey end_key_;

    int32_t range_id_{INT32_MAX};
    uint64_t range_version_{UINT64_MAX};
    CleanType clean_type_{CleanType::CleanRangeData};
    // Clean ts for the kickout cc. Only valid if clean type is
    // CleanForAlterTable.
    uint64_t clean_ts_{0};
    // Target buckets for kickout cc. Only valid if clean type is
    // CleanBucketData.
    std::vector<uint16_t> *bucket_ids_{nullptr};
    CcHandlerResult<Void> hd_result_;
};

struct KickoutDataAllOp : public TransactionOperation
{
    explicit KickoutDataAllOp(TransactionExecution *txm);
    void Reset(uint32_t ng_cnt, size_t table_cnt);
    void Clear();
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    // To handle multi tables.
    std::vector<TableName> table_names_;
    uint64_t commit_ts_{0};
    CleanType clean_type_{CleanType::CleanForAlterTable};
    CcHandlerResult<Void> hd_result_;
};

struct DsUpsertTableOp : public TransactionOperation
{
    DsUpsertTableOp() = delete;
    DsUpsertTableOp(const TableName *table_name,
                    OperationType op_type,
                    TransactionExecution *txm);

    void Reset();
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    const TableName *table_name_{nullptr};
    const TableSchema *table_schema_old_{nullptr};
    const TableSchema *table_schema_{nullptr};
    OperationType op_type_{OperationType::Upsert};
    CcHandlerResult<Void> hd_result_;
    txservice::AlterTableInfo *alter_table_info_{nullptr};

    // write_time_ is corresponding to kv-storage's write_time. Generally,
    // write_time_ is assigned to commit_ts and commit_ts is regarded as new
    // schema version. But for rollback create-index, write_time_ is assigned to
    // `max(commit_ts, latest commit_ts_bound_) + 1` to overwrite catalog image
    // in kv-storage.
    uint64_t write_time_{0};
};

template <typename ResultType>
struct AsyncOp : public TransactionOperation
{
    AsyncOp() = delete;
    ~AsyncOp();
    explicit AsyncOp(TransactionExecution *txm);
    void ResetHandlerTxm(TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;
    void Reset();

    /**
     * @brief Sets op_func_ to run blocking work on worker_thread_ and finish
     * hd_result_ with the code it returns.
     *
     * For work that must not run on the tx processor thread, which calls
     * op_func_: a synchronous store call, an RPC, a sleep. Each run starts a
     * new thread that calls the stored work; Forward() joins it once
     * hd_result_ is finished. Work with a result value sets it on hd_result_
     * before returning.
     *
     * @param work Runs on the worker thread; returns CcErrorCode::NO_ERROR on
     * success, else the error hd_result_ gets.
     * @warning A retry runs the same work again, so the work must leave what
     * it captured intact: pass captured data to callees by reference, never
     * by std::move.
     */
    void SetBlockingWork(std::function<CcErrorCode()> work);

    std::function<void(AsyncOp<ResultType> &async_op)> op_func_;

    // hd_result_ represents the async result, and op_func_ should finish it
    // after completes its work.
    CcHandlerResult<ResultType> hd_result_;

    // An optional executor.
    std::thread worker_thread_;
};

struct SchemaOp : public TransactionOperation
{
    SchemaOp() = delete;
    SchemaOp(const std::string_view table_name_sv,
             txservice::TableEngine table_engine,
             const std::string &current_image,
             const std::string &dirty_image,
             uint64_t schema_ts,
             OperationType op_type,
             TransactionExecution *txm);

    void FillPrepareLogRequestCommon(TransactionExecution *txm,
                                     WriteToLogOp &prepare_log_op);
    void FillCommitLogRequestCommon(TransactionExecution *txm,
                                    WriteToLogOp &commit_log_op);
    void FillCleanLogRequestCommon(TransactionExecution *txm,
                                   WriteToLogOp &clean_log_op);

    /**
     * @brief Replaces the op's schema images, as the engine built them, with
     * the images of schema objects built at known timestamps.
     *
     * An engine builds a DDL's new image before the DDL has a timestamp, so it
     * may record a table the DDL creates without one (0, the unknown
     * timestamp). Once the prepare stage has chosen the op's timestamp, and
     * before anything is logged or installed, the op builds the new schema at
     * that timestamp and the current schema at its version and keeps their
     * images, in which the engine records every base and index table's real
     * timestamp. Every log
     * record, catalog row and copy sent to another node then carries each
     * table's own version, whatever version a node later loads it at.
     * CatalogFactory::CreateTableSchema states what each engine must do for
     * this.
     *
     * @param dirty_version The op's prepare-stage timestamp: the version of a
     * table the op creates.
     */
    void ResolveSchemaImages(uint64_t dirty_version);

    CatalogKey table_key_;  // string owner
    CatalogRecord catalog_rec_;
    std::string image_str_{""};
    std::string dirty_image_str_{""};
    uint64_t curr_schema_ts_;
    OperationType op_type_{OperationType::Insert};

    /**
     * @brief Sets the pause before a failed data store step that must succeed
     * runs again: the ddl_kv_retry_interval_seconds setting. Called at
     * start-up, before any schema op runs.
     *
     * @param interval The pause, at least one second.
     */
    static void SetKvRetryInterval(std::chrono::seconds interval)
    {
        assert(interval.count() > 0);
        kv_retry_interval_ = interval;
    }

    /**
     * @return The pause before a failed data store step that must succeed runs
     * again.
     */
    static std::chrono::seconds KvRetryInterval()
    {
        return kv_retry_interval_;
    }

    /**
     * @brief Waits KvRetryInterval() before a failed data store step runs
     * again (ArmKvRetryWait).
     */
    AsyncOp<Void> kv_retry_wait_op_;
    /** @brief The step to run again once kv_retry_wait_op_ ends. */
    TransactionOperation *kv_retry_step_{nullptr};
    /** @brief Failed attempts of kv_retry_step_, for the log. */
    uint32_t kv_retry_failures_{0};

protected:
    /**
     * @brief Starts a data store step as a single attempt.
     *
     * A DsUpsertTableOp retries on its own unless it is pushed with retry
     * count 0; the schema op alone decides whether a failed step runs again.
     *
     * @param txm The coordinating tx.
     * @param step The step.
     */
    void StartKvStep(TransactionExecution *txm, DsUpsertTableOp &step);

    /**
     * @brief Arms kv_retry_wait_op_ to wait KvRetryInterval() before a failed
     * data store step runs again, and logs a warning.
     *
     * A data store step that fails before the op's point of no return, the
     * commit log of UpsertTableOp or the prepare log of UpsertTableIndexOp, is
     * not retried: the op rolls back. TRUNCATE TABLE, which never rolls back
     * after its prepare log, retries the steps it runs before its commit log.
     * A step that fails after the point of no return must succeed:
     * the op retries it indefinitely, KvRetryInterval() apart, while its node
     * is the leader, holding the table's catalog lock and keeping the client
     * waiting. A node that loses leadership stops, and whoever recovers the op
     * retries the step the same way. The caller makes kv_retry_wait_op_ its
     * current stage, and once the wait ends runs the step again
     * (RunKvStepAgain).
     *
     * @param txm The coordinating tx.
     * @param step The failed step.
     * @param step_name The step, for the log.
     * @param error The step's error message, for the log.
     */
    void ArmKvRetryWait(TransactionExecution *txm,
                        TransactionOperation *step,
                        std::string_view step_name,
                        const std::string &error);

    /**
     * @brief Runs kv_retry_step_ again, as a single attempt, once
     * kv_retry_wait_op_ has ended.
     *
     * @param txm The coordinating tx.
     */
    void RunKvStepAgain(TransactionExecution *txm);

    /**
     * @brief Clears the retry state, when a pooled op is reused.
     *
     * @param txm The tx the op now runs in.
     */
    void ResetKvRetry(TransactionExecution *txm);

    /**
     * @brief Releases the cluster config read lock, which the op took at its
     * first step, or which recovery took before resuming it
     * (LocalCcShards::CreateSchemaRecoveryTx).
     *
     * A schema op holds the lock until its last post write all, so that each
     * step it sends to all node groups reaches the same node groups: a cluster
     * scale waits for it.
     *
     * @param txm The coordinating tx.
     * @param unlock_op The op's unlock step, which the caller made its current
     * stage.
     */
    void StartUnlockClusterConfig(TransactionExecution *txm,
                                  PostReadOperation &unlock_op);

private:
    /**
     * @brief The pause before a failed data store step that must succeed runs
     * again (KvRetryInterval()); 30 seconds unless the
     * ddl_kv_retry_interval_seconds setting says otherwise.
     */
    static inline std::chrono::seconds kv_retry_interval_{30};
};

/**
 * @brief The schema op of CREATE, DROP, TRUNCATE and ALTER TABLE
 * (tx_upsert_table_operation.cpp).
 *
 * The op runs as a stackful coroutine (boost::context) on a stack of its own:
 * the first Forward() starts it and every later one resumes it, once the step
 * it awaits has finished. Its path is written out flat, in three segments cut
 * at the two log records, which are where recovery resumes an op:
 * RunUntilPrepareLog (the cluster config read lock, the write intents, the
 * prepare-stage timestamp, the prepare log), RunUntilCommitLog (the dirty
 * schema's install, the op's own steps before its commit log, the write
 * locks, the commit log) and RunAfterCommitLog (the op's own steps after its
 * commit log, then RunEnding: the post write all, the release of the cluster
 * config lock, which the op holds until then, and the clean log).
 *
 * Return values. A function on the coroutine's path that returns bool says
 * whether the op proceeds to its following steps:
 * - true: the function's steps are done, and the caller proceeds. That is not
 *   always success: a create that gave up, or an ALTER whose catalog row write
 *   failed, proceeds once its commit log at timestamp 0 is written, and its
 *   ending rolls it back.
 * - false: the op has ended and must not proceed: it failed at or before its
 *   prepare log and was rolled back, or the node lost its leadership
 *   (ForceFinish). result_ is set. Every caller returns at once, up to Run(),
 *   and Forward() calls Finish().
 *
 * KvWriteFailed and PrepareSequenceStep run no step and are outside this rule:
 * they return whether the write failed and whether the table has sequences.
 *
 * @warning The coroutine may be resumed on another thread than the one that
 * suspended it, so its body must not access thread_local state directly: an
 * address computed before a suspension would be the old thread's.
 */
struct UpsertTableOp : public SchemaOp
{
    UpsertTableOp() = delete;
    UpsertTableOp(const std::string_view table_name_str,
                  txservice::TableEngine table_engine,
                  const std::string &current_image,
                  uint64_t curr_schema_ts,
                  const std::string &dirty_image,
                  OperationType op_type,
                  TransactionExecution *txm);

    void Reset(const std::string_view table_name_str,
               txservice::TableEngine table_engine,
               const std::string &current_image,
               uint64_t curr_schema_ts,
               const std::string &dirty_image,
               OperationType op_type,
               TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    /**
     * @brief Where a recovered op resumes: after the log record it was
     * recovered from.
     */
    enum class Entry
    {
        kStart,            ///< A new op, from its first step.
        kAfterPrepareLog,  ///< Recovered from its prepare log.
        kAfterCommitLog    ///< Recovered from its commit log.
    };

    /**
     * @brief The step the coroutine awaits, for tracing.
     */
    TransactionOperation *op_{nullptr};
    /** @brief Where Run() starts; recovery sets it. */
    Entry entry_{Entry::kStart};
    /**
     * @brief Acquire read lock on local cluster config ccmap to block cluster
     * config update during upsert table op. We cannot allow config update
     * between acquire write all and post write all. The first step of every
     * op, which holds the lock until its last post write all
     * (unlock_cluster_config_op_).
     */
    ReadLocalOperation lock_cluster_config_op_;
    /**
     * @brief Acquires write intents on the table's catalog in all nodes to
     * prevent concurrent schema modifications.
     *
     */
    AcquireAllOp acquire_all_intent_op_;
    /**
     * @brief Flushes the prepare log to the log service. The schema operation
     * is guaranteed to succeed after this stage.
     *
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Installs the dirty schema in the tx service and returns a local
     * view (pointer) of it.
     *
     */
    PostWriteAllOp post_all_intent_op_;
    /**
     * @brief Release cluster config read lock after post write all: after the
     * op's last one, post_all_lock_op_, before the clean log.
     */
    PostReadOperation unlock_cluster_config_op_;
    /**
     * @brief CREATE TABLE only: creates the kv tables of the new table and of
     * its indexes, after the prepare log and under the write intent.
     *
     * On the data store service that is each range-partitioned table's
     * initial range record, at the prepare-stage timestamp, which the
     * in-memory ranges match (PostCommit). It precedes the commit log, so a
     * failure is not retried: the create gives up (AbandonCreate). Every state
     * recovery can reach from a commit log that commits the create has the kv
     * tables.
     */
    AsyncOp<Void> create_kv_table_op_;
    /**
     * @brief CREATE TABLE only: once the create gives up, removes what
     * create_kv_table_op_ created, before the commit log that fails the op.
     *
     * The store's drop-table path, over the dirty schema at the prepare-stage
     * timestamp: the kv tables, their ranges and the catalog key, which the
     * create has not written. One attempt: a failure leaves them orphaned,
     * which is accepted: they hold no data, no schema refers to them, and a
     * later create of the same name writes them again (CreateKvTable is
     * idempotent).
     */
    DsUpsertTableOp drop_kv_table_op_;
    /**
     * @brief TRUNCATE TABLE on shared storage: drops the base table's kv table
     * (DataStoreHandler::DropKvTable), under the write locks and before the
     * commit log, retried until it succeeds; the catalog row follows after the
     * commit log (upsert_kv_table_op_).
     *
     * That is all the store's own truncate (UpsertTable, TruncateTable) does to
     * an EloqKV table's data, the only kind a truncate runs on: EloqKV tables
     * are hash partitioned, have no indexes and keep no statistics, so there
     * are no ranges or statistics to reset. The kv table keeps its name: the
     * store keeps no per-table state.
     */
    AsyncOp<Void> truncate_kv_table_op_;
    /**
     * @brief Writes the data store side of the op; each run names its store
     * operation (AwaitUpsertKvTable).
     *
     * CREATE TABLE and, on shared storage, TRUNCATE TABLE: the catalog row
     * only, after the commit log, at the commit-stage timestamp (op type
     * Update). DROP TABLE: deletes the kv tables, after the commit log. Those
     * are retried until they succeed (AwaitKvRetry). ALTER TABLE (Update): the
     * new catalog row, before the commit log, rolled back on failure.
     */
    DsUpsertTableOp upsert_kv_table_op_;
    /**
     * @brief Update the sequence table for range id info and the auto increment
     * value if this table has auto_increment column.
     *
     * CREATE TABLE runs it before the commit log, and gives up on a failure
     * (AbandonCreate). TRUNCATE TABLE on shared storage runs it before the
     * commit log and DROP TABLE after it, both retrying it until it succeeds
     * (AwaitKvRetry).
     */
    AsyncOp<Void> update_sequence_table_op_;
    /**
     * @brief Upgrades acquired write intents to write locks in all nodes.
     *
     */
    AcquireAllOp acquire_all_lock_op_;
    /**
     * @brief Downgrades the write locks to write intents when
     * acquire_all_lock_op_ deadlocks, so that transactions waiting to read the
     * catalog go ahead; the op then acquires the write locks again.
     */
    PostWriteAllOp downgrade_lock_op_;
    /**
     * @brief Flushes the commit log to the log service. The commit log confirms
     * that the data store operation succeeds and does not need redo upon
     * failures.
     *
     */
    WriteToLogOp commit_log_op_;

    /**
     * @brief Empties the table's cc maps on all node groups (CleanCcm).
     */
    KickoutDataAllOp clean_ccm_op_;

    /**
     * @brief Removes write locks in all nodes. If the schema operation
     * succeeds, also installs the new schema in all nodes.
     *
     */
    PostWriteAllOp post_all_lock_op_;
    /**
     * @brief The last log operation that removes the schema record from the log
     * state machine.
     *
     */
    WriteToLogOp clean_log_op_;

    CcHandlerResult<ReadKeyResult> read_cluster_result_;
    ClusterConfigRecord cluster_conf_rec_;

private:
    /** @brief The state of the op's coroutine. */
    enum class CoroStatus
    {
        kEmpty,    ///< Not started.
        kRunning,  ///< Started, suspended while a step runs.
        kFinished  ///< Its body has returned; Forward() ends the op.
    };

    /**
     * @brief The coroutine's stack size: sub-operations can run cc requests
     * inline and send remote requests on it.
     */
    static constexpr size_t kCoroStackSize = 128 * 1024;

    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);

    /**
     * @brief Suspends the coroutine, back to Forward(), until a step has
     * finished; returns at once if it already has.
     *
     * A step has finished once it has left the tx's stack: its PostProcess
     * pops it, and the tx then forwards the op again, which resumes the
     * coroutine. A step can finish inside its Process() already, and then the
     * coroutine goes on without a suspension.
     *
     * @param txm The coordinating tx.
     * @param op The step, which the caller pushed and started.
     */
    void SuspendUntilDone(TransactionExecution *txm,
                          const TransactionOperation &op);

    /**
     * @brief Runs a sub-operation to its end: pushes and processes it, then
     * suspends until it has finished and left the tx's stack
     * (SuspendUntilDone).
     *
     * @note A retry awaits the same sub-operation again without resetting it:
     * the tx's Process() for every sub-operation this op runs resets its
     * result (and, for an AcquireAllOp, each result and the counters) before
     * sending anything. The exception is the unlock step, a PostReadOperation,
     * which StartUnlockClusterConfig resets and which never runs twice.
     *
     * @param txm The coordinating tx.
     * @param op The sub-operation.
     */
    template <typename OpT>
    void Await(TransactionExecution *txm, OpT &op);

    /**
     * @brief Await(txm, op) with a fault point: while the fault point is
     * armed, the sub-operation fails with fault_error without running.
     *
     * A sub-operation failed this way is neither pushed nor awaited: its
     * hd_result_ holds the error when the call returns, and the coroutine goes
     * on without a suspension.
     *
     * For a sub-operation whose result is a single hd_result_; one without
     * such a result does not compile with it.
     *
     * @param txm The coordinating tx.
     * @param op The sub-operation.
     * @param fault_point The fault point's name.
     * @param fault_error The error the sub-operation reports when it fires.
     */
    template <typename OpT>
    void Await(TransactionExecution *txm,
               OpT &op,
               const char *fault_point,
               CcErrorCode fault_error);

    /**
     * @brief Runs a data store step to its end as a single attempt
     * (StartKvStep).
     *
     * @param txm The coordinating tx.
     * @param step The step.
     */
    void AwaitKvStep(TransactionExecution *txm, DsUpsertTableOp &step);

    /**
     * @brief The coroutine's body: the op's path, entered at entry_.
     *
     * @param txm The coordinating tx.
     */
    void Run(TransactionExecution *txm);

    /**
     * @brief The first segment, the same for every op: the cluster config read
     * lock, the write intents, the prepare-stage timestamp D, the prepare log.
     * A failure rolls the op back.
     *
     * @param txm The coordinating tx.
     * @return true if the prepare log is written: the op proceeds to
     * RunUntilCommitLog. false if the op has ended and must not proceed: it
     * failed at or before the prepare log and was rolled back, or the node
     * lost its leadership.
     */
    bool RunUntilPrepareLog(TransactionExecution *txm);

    /**
     * @brief The second segment: the dirty schema's install, then the op's own
     * steps up to and including its commit log.
     *
     * @param txm The coordinating tx.
     * @return true if the commit log is written: the op proceeds to
     * RunAfterCommitLog, also when the commit log, at timestamp 0, rolls a
     * failed op back. false if the op has ended and must not proceed: the node
     * lost its leadership.
     */
    bool RunUntilCommitLog(TransactionExecution *txm);

    /**
     * @brief CREATE TABLE before its commit log: the kv tables, the sequence
     * table, the write locks at the commit-stage timestamp P, the commit log.
     * A data store failure gives the create up (AbandonCreate).
     *
     * @param txm The coordinating tx.
     * @return true if the commit log is written, at the commit timestamp or,
     * for a create that gave up, at timestamp 0: the op proceeds either way.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool CreateUntilCommitLog(TransactionExecution *txm);

    /**
     * @brief DROP TABLE before its commit log: the write locks at the
     * commit-stage timestamp P, the commit log.
     *
     * @param txm The coordinating tx.
     * @return true if the commit log is written: the op proceeds.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool DropUntilCommitLog(TransactionExecution *txm);

    /**
     * @brief TRUNCATE TABLE before its commit log: the write locks at the
     * commit-stage timestamp P, on shared storage the table's data, each step
     * retried until it succeeds, then the commit log.
     *
     * On shared storage the data is the cc maps (CleanCcm), then the kv table
     * (truncate_kv_table_op_), then the sequences' reset; the catalog row waits
     * for the commit log (TruncateAfterCommitLog). A participant that recovers
     * from the commit log, and takes writes into the emptied table, then finds
     * no later step that wipes them. Without shared storage every step comes
     * after the commit log, as each node group truncates its own store: one
     * that recovers from the commit log truncates its store in the replay,
     * before it takes writes, and the coordinator's later request skips it.
     *
     * @param txm The coordinating tx.
     * @return true if the commit log is written: the op proceeds.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool TruncateUntilCommitLog(TransactionExecution *txm);

    /**
     * @brief ALTER TABLE before its commit log: the new catalog row, the write
     * locks, the commit log. A failed catalog row write rolls the op back with
     * a commit log at timestamp 0.
     *
     * @param txm The coordinating tx.
     * @return true if the commit log is written, at the commit timestamp or,
     * after a failed catalog row write, at timestamp 0: the op proceeds either
     * way. false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool AlterUntilCommitLog(TransactionExecution *txm);

    /**
     * @brief The third segment: the op's own steps after its commit log, then
     * RunEnding.
     *
     * @param txm The coordinating tx.
     */
    void RunAfterCommitLog(TransactionExecution *txm);

    /**
     * @brief CREATE TABLE after its commit log: the catalog row, retried until
     * it is written; nothing for a create that gave up.
     *
     * @param txm The coordinating tx.
     * @return true if the catalog row is written, or the create gave up and
     * has none to write: the op proceeds to RunEnding.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool CreateAfterCommitLog(TransactionExecution *txm);

    /**
     * @brief DROP TABLE after its commit log: the kv table's deletion and the
     * sequences' deletion, each retried until it succeeds, then the cc maps'
     * emptying (CleanCcm), which PostCommit follows by dropping them.
     *
     * @param txm The coordinating tx.
     * @return true if the kv table and the sequences are gone and the cc maps
     * are empty: the op proceeds to RunEnding. false if the op has ended and
     * must not proceed: the node lost its leadership.
     */
    bool DropAfterCommitLog(TransactionExecution *txm);

    /**
     * @brief TRUNCATE TABLE after its commit log: on shared storage, the
     * catalog row at the commit timestamp, retried until it is written; without
     * shared storage, each node group empties its cc maps and truncates its own
     * store, catalog row included (CleanCcm).
     *
     * @param txm The coordinating tx.
     * @return true if the catalog row is written, or the node groups truncated
     * their stores: the op proceeds to RunEnding.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool TruncateAfterCommitLog(TransactionExecution *txm);

    /**
     * @brief The ending every op shares: the post write all (PostCommit, or
     * Rollback for an op that failed), the release of the cluster config lock,
     * the clean log; then sets result_.
     *
     * @param txm The coordinating tx.
     */
    void RunEnding(TransactionExecution *txm);

    /**
     * @brief Upgrades the write intents to write locks on every node group,
     * retried until held; a deadlock first downgrades the locks
     * (downgrade_lock_op_).
     *
     * @param txm The coordinating tx.
     * @return true if the write locks are held: the op proceeds.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool AcquireWriteLocks(TransactionExecution *txm);

    /**
     * @brief Writes the commit log at the op's commit timestamp, 0 for an op
     * that failed, retried until it is written.
     *
     * @param txm The coordinating tx.
     * @return true if the commit log is written: the op proceeds.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool WriteCommitLog(TransactionExecution *txm);

    /**
     * @brief Runs post_all_lock_op_: PostCommit, or Rollback for an op that
     * failed.
     *
     * @param txm The coordinating tx.
     * @param retry Whether to retry a failed attempt until one succeeds; if
     * false, one attempt, whatever its outcome.
     * @return true if the post write all has run: it succeeded, or, with
     * retry false, it was attempted; the op proceeds.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool RunPostWriteAll(TransactionExecution *txm, bool retry);

    /**
     * @brief Releases the cluster config read lock, after the op's last post
     * write all.
     *
     * @param txm The coordinating tx.
     * @return true if the lock is released: the op proceeds.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool ReleaseClusterConfig(TransactionExecution *txm);

    /**
     * @brief Runs upsert_kv_table_op_ once over the current and dirty schema,
     * at the op's commit timestamp.
     *
     * @param txm The coordinating tx.
     * @param op_type What the store does: Update writes the catalog row only
     * (CREATE and TRUNCATE TABLE after their commit log, ALTER TABLE);
     * DropTable deletes the kv tables, their ranges and statistics and the
     * catalog row.
     */
    void AwaitUpsertKvTable(TransactionExecution *txm, OperationType op_type);

    /**
     * @brief Whether upsert_kv_table_op_ failed, counting the
     * upsert_table_kv_write_fail fault point.
     *
     * @param txm The coordinating tx, for the log.
     * @return true if the write failed.
     */
    bool KvWriteFailed(TransactionExecution *txm);

    /**
     * @brief Waits KvRetryInterval() before a data store step that failed
     * after the commit log runs again, and logs a warning (ArmKvRetryWait).
     *
     * @param txm The coordinating tx.
     * @param step The failed step.
     * @param step_name The step, for the log.
     * @param error The step's error message, for the log.
     * @return true if the pause is over: the caller runs the step again.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool AwaitKvRetry(TransactionExecution *txm,
                      TransactionOperation *step,
                      std::string_view step_name,
                      const std::string &error);

    /**
     * @brief Sets update_sequence_table_op_'s work if the table has sequences.
     *
     * @return false if the table has none, so the step is skipped.
     */
    bool PrepareSequenceStep();

    /**
     * @brief Sets truncate_kv_table_op_'s work: drops the base table's kv
     * table, which keeps its name.
     *
     * @param txm The coordinating tx, for the log.
     */
    void PrepareTruncateKvTable(TransactionExecution *txm);

    /**
     * @brief Sets create_kv_table_op_'s work: creates the kv tables of the new
     * table and of its indexes, at the op's prepare-stage timestamp.
     *
     * @param txm The coordinating tx.
     */
    void PrepareCreateKvTables(TransactionExecution *txm);

    /**
     * @brief Empties the table's cc maps on every node group
     * (clean_ccm_op_), retried until it succeeds.
     *
     * Each core frees a map's pages 32 at a time and yields between batches
     * (CcShard::CleanCcmPages). For DROP TABLE, PostCommit then drops the
     * emptied maps: a full map would free all its pages inside that one
     * post-write request, which cannot yield, stalling the core. For TRUNCATE
     * TABLE, whose maps stay, this is what empties them; without shared
     * storage, core 0 of each node group also truncates its own kv table in
     * the same request.
     *
     * @param txm The coordinating tx.
     * @param base_table_only Whether to clean only the base table: a truncate
     * without shared storage, whose node groups truncate their kv tables as
     * part of it.
     * @return true if the cc maps are cleaned: the op proceeds.
     * false if the op has ended and must not proceed: the node lost its
     * leadership.
     */
    bool CleanCcm(TransactionExecution *txm, bool base_table_only);

    /**
     * @brief Re-assigns the commit timestamp after the commit-stage write lock:
     * P = max(the prepare-stage timestamp, the lock results' last_vali_ts + 1
     * and commit_ts + 1).
     *
     * For CREATE, DROP and TRUNCATE TABLE, whose prepare stage holds only a
     * write intent: DML of the old table can commit after the prepare stage,
     * and the checkpoint can pass its timestamp. Under the lock, P is above
     * every DML the lock drained and above the checkpoint, so the commit log,
     * the DDL record and PostCommit all carry a timestamp that orders the op
     * after the old table's writes and before the new table's.
     *
     * @param txm The coordinating tx.
     */
    void AssignCommitStageTs(TransactionExecution *txm);

    /**
     * @brief Gives up a CREATE TABLE whose kv tables or sequence table update
     * failed, before its commit log: marks the op failed and removes the kv
     * tables it created.
     *
     * The op has only a prepare log, so it may still fail, as it did before
     * the prepare log: drop_kv_table_op_ removes whatever was created, once,
     * then the caller writes a commit log with commit timestamp 0, which fails
     * the op; the ending rejects the dirty schema and releases the write
     * intents, and the client gets DATA_STORE_ERROR. The failed op appends no
     * DDL record. Sequence entries it wrote stay, which is harmless: a later
     * create of the same name writes them again. A node that recovers the op
     * from its prepare log tries its data store steps again, once.
     *
     * @param txm The coordinating tx.
     * @return true if the kv tables' removal has run, whatever its outcome:
     * the op proceeds to its commit log at timestamp 0. false if the op has
     * ended and must not proceed: the node lost its leadership.
     */
    bool AbandonCreate(TransactionExecution *txm);

    /**
     * @brief Marks the op failed: commit timestamp 0, and a Rollback second
     * phase that discards whatever dirty schema the op installed and leaves
     * the catalog entry's version alone.
     *
     * @param txm The coordinating tx.
     */
    void MarkFailed(TransactionExecution *txm);

    /**
     * @brief Ends the op on the node's loss of leadership: whoever recovers it
     * takes over.
     *
     * @param txm The coordinating tx.
     */
    void ForceFinish(TransactionExecution *txm);

    /**
     * @brief The result of an op that got to its clean log, or that ended on a
     * loss of leadership: Failed for a failed op, Unverified for one forced
     * to finish, Succeeded otherwise.
     *
     * @param txm The coordinating tx.
     * @return The result.
     */
    UpsertResult CleanLogResult(TransactionExecution *txm);

    /**
     * @brief Ends the op once its coroutine has finished: answers the client
     * with result_, pops the op and returns it to the pool. Runs on the tx's
     * own stack, not the coroutine's, which the pooled op owns.
     *
     * @param txm The coordinating tx.
     */
    void Finish(TransactionExecution *txm);

    /** @brief The coroutine, while it is suspended. */
    boost::context::continuation coro_;
    /**
     * @brief Forward()'s side of the switch, which lives on the coroutine's
     * stack: set from the coroutine's start until its body returns.
     */
    boost::context::continuation *caller_{nullptr};
    CoroStatus coro_status_{CoroStatus::kEmpty};  ///< The coroutine's state.
    /** @brief The op's result, which Finish() reports. */
    UpsertResult result_{UpsertResult::Failed};

    // Due to term or other error, called ForceFinish to terminate this
    // operation
    bool is_force_finished;
};

/**
 * Used in DML commit context to flush updated schema images to storage, and
 * clean schema log. Its function is similar to Checkpoint. However, since there
 * is not a periodic retry thread, the commit thread should **keep retry** until
 * succeed or leader changed.
 */
struct FlushUpdateTableOp : public TransactionOperation
{
    explicit FlushUpdateTableOp(TransactionExecution *txm);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    TransactionOperation *op_;

    // Build and install dirty table schemas.
    PostWriteAllOp post_all_intent_op_;

    AsyncOp<Void> update_kv_table_op_;

    bool need_write_log_;  // Disable wal log to speedup jstests.

    WriteToLogOp commit_log_op_;

    WriteToLogOp clean_log_op_;
};

struct SleepOperation : TransactionOperation
{
public:
    explicit SleepOperation(TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    int sleep_secs_{0};
};

struct CleanCcEntryForTestOp : TransactionOperation
{
public:
    explicit CleanCcEntryForTestOp(TransactionExecution *txm);

    void Set(const TableName *tn,
             const TxKey *key,
             bool only_archives,
             bool flush)
    {
        tab_name_ = tn;
        key_ = key;
        only_archives_ = only_archives;
        flush_ = flush;
        succeed_ = false;
    }

    void Reset()
    {
        succeed_ = false;
        hd_result_.Reset();
    }
    void Forward(TransactionExecution *txm) override;

    const TableName *tab_name_{nullptr};
    const TxKey *key_{nullptr};
    bool only_archives_{false};
    bool flush_{true};

    bool succeed_{false};
    CcHandlerResult<bool> hd_result_;
};

struct NoOp : public TransactionOperation
{
    NoOp(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    CcHandlerResult<Void> hd_result_;
};

struct SplitFlushRangeOp : public CompositeTransactionOperation
{
    SplitFlushRangeOp() = delete;

    SplitFlushRangeOp(const TableName &table_name,
                      std::shared_ptr<const TableSchema> &&table_schema,
                      TableRangeEntry *range_entry,
                      std::vector<std::pair<TxKey, int32_t>> &&new_range_info,
                      TransactionExecution *txm,
                      bool is_dirty);

    void Reset(const TableName &table_name,
               std::shared_ptr<const TableSchema> &&table_schema,
               TableRangeEntry *range_entry,
               std::vector<std::pair<TxKey, int32_t>> &&new_range_info,
               TransactionExecution *txm,
               bool is_dirty);

    void Forward(TransactionExecution *txm) override;

    std::shared_ptr<const TableSchema> table_schema_{nullptr};
    TableName table_name_;        // TableName owner.
    TableName range_table_name_;  // References table_name_.
    CcHandlerResult<ReadKeyResult> read_cluster_result_;
    ClusterConfigRecord cluster_conf_rec_;

    std::unique_ptr<RangeInfo> range_info_;
    std::unique_ptr<RangeRecord> range_record_;
    // TODO{liunyl}: change these to Uptr after we update inf key instance.
    // Now we need to use raw pointers to accomadate with inf key instance.
    // Now we make them point to the Uptr in range_info_ if they are normal key,
    // or raw pointers to inf key instances otherwise.
    TxKey old_start_key_;
    TxKey old_end_key_;
    TableRangeEntry *range_entry_;
    // vector< new start key, new partition id >
    std::vector<std::pair<TxKey, int32_t>> new_range_info_;

    bool is_dirty_{false};

    std::vector<std::pair<TxKey, int32_t>>::const_iterator kickout_data_it_;
    bool cleaning_old_range_dirty_owner_{false};

    /**
     * @brief Acquire read lock on local cluster config ccmap to block cluster
     * config update during upsert table op. We cannot allow config update
     * between acquire write all and post write all.
     */
    ReadLocalOperation lock_cluster_config_op_;
    /**
     * @brief Acquire write lock on all node groups. Since split-flush op is
     * the only operation that would try to acquire write lock on range
     * table entry, and each range is only sharded to one node group, we
     * should be the only one trying to acquire write lock on this range
     * entry. So we can directly try to acquire write lock and not worrying
     * about dead locks. The last valid ts returned by this op will be used
     * to calculate commit ts of the split-flush tx.
     */
    AcquireAllOp prepare_acquire_all_write_op_;
    /**
     * @brief Write prepare log. Prepare log should have
     * 1. The old range id.
     * 2. The new range id.
     * 3. The start key of new range id.
     * 4. The commit ts of this split-flush tx.
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Add new partition id to range entry on each node group. Now
     * write operation will write to both new and old range partition.
     * Downgrade write lock to write intent lock.
     */
    PostWriteAllOp install_new_range_op_;
    /**
     * @brief Release cluster config lock after post write all.
     */
    PostReadOperation unlock_cluster_config_op_;
    /**
     * @brief Scan and Flush data in memory before commit_ts to KV storage.
     * These data will be flushed into new partitions. We can't safely update
     * ckpt_ts of CcEntry for now.
     */
    AsyncOp<Void> data_sync_op_;
    /**
     * @brief Acquire write lock on all node group on the old partition and
     * new partition.
     */
    AcquireAllOp commit_acquire_all_write_op_;

    /**
     * @brief Update key cache if key cache is enabled.
     */
    AsyncOp<Void> update_key_cache_op_;

    /**
     * @brief Write commit log.
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Upsert new ranges into range table in KV store.
     */
    AsyncOp<Void> ds_upsert_range_op_;
    /**
     * @brief Kickout old range data from cc map if the data now
     * falls on a new node group.
     */
    KickoutDataOp kickout_old_range_data_op_;
    /**
     * @brief 1. Insert new range into range tables on all nodes
     * 2. Remove new range info from old range entry.
     * 3. Release locks on all nodes.
     */
    PostWriteAllOp post_all_lock_op_;
    /**
     * @brief Remove obselete data from old range in KV store. We don't need
     * any lock here since these data will not be visible to anyone after the
     * new range info has been comitted.
     */
    AsyncOp<Void> ds_clean_old_range_op_;
    /**
     * @brief Remove split-flush log.
     */
    WriteToLogOp clean_log_op_;

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void FillCleanLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
    void ClearInfos();
    bool ForwardKickoutIterator(TransactionExecution *txm);
    std::vector<SplitRangeInfo> GenSplittedRangeInfos();
};

struct AnalyzeTableAllOp : TransactionOperation
{
private:
    constexpr static uint32_t range_sample_pool_capacity_{128};

public:
    explicit AnalyzeTableAllOp(TransactionExecution *txm);
    void Reset(uint32_t hres_ref_cnt);
    void Forward(TransactionExecution *txm) override;

    AnalyzeTableTxRequest *analyze_tx_req_{nullptr};
    CcHandlerResult<Void> hd_result_;
};

struct BroadcastStatisticsOp : TransactionOperation
{
    explicit BroadcastStatisticsOp(TransactionExecution *txm);
    void Reset(uint32_t hres_ref_cnt);
    void Forward(TransactionExecution *txm) override;

    BroadcastStatisticsTxRequest *broadcast_tx_req_{nullptr};
    CcHandlerResult<Void> hd_result_;
};

struct ObjectCommandOp : TransactionOperation
{
    explicit ObjectCommandOp(
        TransactionExecution *txm,
        CcHandlerResult<ReadKeyResult> *lock_range_bucket_result = nullptr);
    // Reset to empty state
    void Reset();
    void Reset(const TableName *table_name,
               const TxKey *key,
               TxCommand *command,
               bool auto_commit = false,
               bool always_redirect = false);
    void Forward(TransactionExecution *txm) override;

    const TableName *table_name_{};
    const TxKey *key_{};
    TxCommand *command_{};

    CcHandlerResult<ObjectCommandResult> hd_result_;

    bool auto_commit_{};
    bool always_redirect_{};
    CcHandlerResult<ReadKeyResult> *lock_bucket_result_;
    uint32_t forward_key_shard_{UINT32_MAX};
    bool catalog_read_success_{};
};

struct MultiObjectCommandOp : TransactionOperation
{
    explicit MultiObjectCommandOp(
        TransactionExecution *txm,
        CcHandlerResult<ReadKeyResult> *lock_range_bucket_result = nullptr);
    // Reset to empty state
    void Reset();
    void Reset(MultiObjectCommandTxRequest *tx_req);

    void Forward(TransactionExecution *txm) override;
    bool IsBlockCommand() override;

    TransactionExecution *txm_{};
    MultiObjectCommandTxRequest *tx_req_{};

    std::vector<CcHandlerResult<ObjectCommandResult>> vct_hd_result_;
    std::atomic_int32_t atm_cnt_{0};
    // For blocked commands, it is not need to wait all commands to finished, it
    // will wait one or more of them finished and send abort requests for other
    // block commands.
    std::atomic_int32_t atm_block_cnt_{0};
    // The count of commands executed on local node. This variable is used to
    // ensure all local command executed to avoid that the local commands
    // execute after released and visit released memory to make process crash.
    std::atomic_int32_t atm_local_cnt_{0};
    // Fast path to know whether error occurs instead of scan "vct_hd_result_"
    std::atomic<CcErrorCode> atm_err_code_{CcErrorCode::NO_ERROR};
    // Used to abort blocked commands
    std::vector<CcHandlerResult<ObjectCommandResult>> vct_abort_hd_result_;
    // The op is blocking when running block commands
    bool is_block_command_;

    size_t bucket_lock_cur_{0};
    CcHandlerResult<ReadKeyResult> *lock_bucket_result_;
    // [{key shard code, forward key shard code}, ...]
    std::vector<std::pair<uint32_t, uint32_t>> vct_key_shard_code_;
    bool catalog_read_success_{};
};

// Only acquire key write lock on forward NodeGroup (dirty owner of bucket)
// without execute tx command when the bucket is in migration.
struct CmdForwardAcquireWriteOp : TransactionOperation
{
public:
    explicit CmdForwardAcquireWriteOp(TransactionExecution *txm);
    void Reset(size_t acquire_write_cnt);
    void AggregateAcquiredKeys(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<std::vector<AcquireKeyResult>> hd_result_;
    std::vector<CmdForwardEntry *> acquire_write_entries_{16};

    // Number of remote keys on which the acquire write operation needs to
    // acquire write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};

    TransactionExecution *txm_{nullptr};
};

class NotifyMigrationClosure : public google::protobuf::Closure
{
public:
    NotifyMigrationClosure(
        std::atomic<size_t> *unfinished_cnt,
        std::unordered_map<NodeGroupId, BucketMigrateInfo> &migrate_plans)
        : cntl_(),
          unfinished_cnt_(unfinished_cnt),
          migrate_plans_(migrate_plans)
    {
    }

    ~NotifyMigrationClosure() = default;

    void Run() override
    {
        if (cntl_.Failed())
        {
            LOG(INFO)
                << "Cluster scale tx notify migration response, node group: "
                << request_.orig_owner() << ", success: false";
            migrate_plans_[request_.orig_owner()].has_migration_tx_ = false;
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
        }
        else
        {
            LOG(INFO)
                << "Cluster scale tx notify migration response, node group: "
                << request_.orig_owner()
                << ", success: " << response_.success();
            migrate_plans_[request_.orig_owner()].has_migration_tx_ =
                response_.success();
        }
        channel_ = nullptr;

        unfinished_cnt_->fetch_sub(1, std::memory_order_release);
    }

    remote::InitMigrationRequest &Request()
    {
        return request_;
    }

    remote::InitMigrationResponse &Response()
    {
        return response_;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    void Reset()
    {
        cntl_.Reset();
        response_.Clear();
        channel_ = nullptr;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

private:
    brpc::Controller cntl_;
    remote::InitMigrationRequest request_;
    remote::InitMigrationResponse response_;
    std::atomic<size_t> *unfinished_cnt_;
    std::unordered_map<NodeGroupId, BucketMigrateInfo> &migrate_plans_;
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;
};

struct NotifyStartMigrateOp : public TransactionOperation
{
    explicit NotifyStartMigrateOp(TransactionExecution *txm);

    void Clear();
    void Reset(size_t node_group_count);
    void Forward(TransactionExecution *txm) override;

    void InitDataMigration(TxNumber tx_number, NodeGroupId old_owner);

    std::unordered_map<NodeGroupId, BucketMigrateInfo> migrate_plans_;

    std::atomic<size_t> unfinished_req_cnt_{0};
    std::vector<std::unique_ptr<NotifyMigrationClosure>> closures_;
};

struct CheckMigrationIsFinishedOp : public TransactionOperation
{
    explicit CheckMigrationIsFinishedOp(TransactionExecution *txm);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    bool migration_is_finished_{false};
    std::atomic<bool> rpc_is_finished_{false};

    CheckMigrationIsFinishedClosure closure_;
};

/**
 * Cluster scale op consists of 2 parts, changing cluster config and migrating
 * data. Changing cluster config will only modify peers and cc node group
 * configs, but it does not rebalance data among node groups. Data migration
 * will rebalance data among node groups after the cluster config change.
 */
struct ClusterScaleOp : public CompositeTransactionOperation
{
public:
    ClusterScaleOp() = delete;
    ClusterScaleOp(const std::string &id,
                   ClusterScaleOpType event_type,
                   std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
                       &&new_ng_config,
                   TransactionExecution *txm);
    void Reset(const std::string &id,
               ClusterScaleOpType event_type,
               std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
                   &&new_ng_config,
               TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    /**
     * Publish is_migrating to all node groups.
     * And finish handler_result after receiving all rpc responses.
     **/
    static void SendBucketsMigratingRpc(bool is_migrating,
                                        bool &result,
                                        bool &rpc_failed);

    std::unordered_map<NodeGroupId, BucketMigrateInfo> bucket_migrate_infos_;
    /**
     * Cluster scale tx has different op processing order based on the event
     * type. For add node, the order is
     * 1. prepare_log_op_
     * == pub_buckets_migrate_begin_op_
     * 2. acquire_cluster_config_write_op_
     * 3. update_cluster_config_log_op_
     * 4. install_cluster_config_op_
     * 5. notify_migration_op_
     * 6. check_migration_is_finished_op_
     * ==  pub_buckets_migrate_end_op_
     * 7. clean_log_op_
     *
     * For remove node, the order is
     * 1. prepare_log_op_
     * == pub_buckets_migrate_begin_op_
     * 2. notify_migration_op_
     * 3. check_migration_is_finished_op_
     * 4. acquire_cluster_config_write_op_
     * 5. update_cluster_config_log_op_
     * 6. install_cluster_config_op_
     * ==  pub_buckets_migrate_end_op_
     * 7. clean_log_op_
     *
     * Basically for add node we're adding new nodes into the cluster first,
     * then migrate data to the new nodes. For remove node we're migrating data
     * from the to be removed nodes, then actually removing them from the
     * cluster.
     * When a new node is just added into the cluster, we can start tx
     * on it but it does not hold any data yet. We need to migrate data
     * ownership to the new ngs separately through data migration.
     * When a node is removed from the cluster, we need to make sure that all
     * data owned by that node group is migrated to other node groups.
     */

    /**
     * Write prepare log for the scale event. This log should contain the new
     * cluster config and data migration plan.
     */
    WriteToLogOp prepare_log_op_;

    /**
     * Upgrade the cluster config lock to write lock as we're now going to
     * update the cluster config.
     */
    AcquireAllOp acquire_cluster_config_write_op_;

    /**
     * Write log for cluster config update. The cluster config update is a 1
     * phase commit.
     */
    WriteToLogOp update_cluster_config_log_op_;

    /**
     * Release the cluster config lock. Install the new cluster config on all
     * ngs. This will establish new cc streams and raft nodes. After this stage,
     * the CP will be able to start new nodes or remove old nodes.
     */
    PostWriteAllOp install_cluster_config_op_;

    /**
     * @brief Notify all node group to start bucket migration.
     */
    NotifyStartMigrateOp notify_migration_op_;

    /**
     * @brief Check whether bucket migration is finished.
     */
    CheckMigrationIsFinishedOp check_migration_is_finished_op_;

    WriteToLogOp clean_log_op_;

    /**
     * @brief Before buckets migrating, notify all nodes that reading bucket
     * info from RangeBucketCcMap instead of no-lock reading
     * LocalCcShards::buckets_infos_.
     * @result Identify that there is no tx reading buckets_infos_ directly on
     * all nodes.
     */
    AsyncOp<Void> pub_buckets_migrate_begin_op_;

    /**
     * @brief Notify all nodes that buckets migration finshed and can read
     * bucket info from buckets_infos directly.
     */
    AsyncOp<Void> pub_buckets_migrate_end_op_;

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillUpdateClusterConfigLogRequest(TransactionExecution *txm);
    void FillCleanLogRequest(TransactionExecution *txm);

    void ForceToFinish(TransactionExecution *txm);

    void ClearContainer();

    ClusterConfigRecord cluster_config_rec_;

    std::string id_;
    ClusterScaleOpType event_type_;
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> new_ng_config_;
    // mutex protects txn_ and finished_, which are used when control plane
    // queries for current cluster scale event status for a specific txn.
    std::mutex mux_;
    TxNumber txn_;
};

struct DataMigrationOp : public CompositeTransactionOperation
{
    /**
     * @brief Data migration op will migrate buckets to their new leader in
     * batches. We will have multilpe migrate worker txs to do the migrate, and
     * each worker will try to fetch some new buckets to work on from the
     * pending work list when it is done' with the previous batch. For hash
     * partition, a worker will migrate all buckets on a core in a single run.
     * This is to speed up the flush and kickout op and avoid repeatedly
     * scanning the same core with each bucket.
     */
public:
    DataMigrationOp() = delete;

    DataMigrationOp(TransactionExecution *txm,
                    std::shared_ptr<DataMigrationStatus> status);

    void Reset(TransactionExecution *txm,
               std::shared_ptr<DataMigrationStatus> status);

    void Forward(TransactionExecution *txm) override;

    void PrepareNextRoundBuckets();

    /**
     * @brief Write the first prepare log. This log request will check if the
     * ClusterScaleTx log is exsit. If not, log service will reject this write
     * log request. This migration transaction will be aborted. Since a finished
     * ClusterScaleTx notify request will arrive late due network delay. It will
     * break idempotent. This log request will also insert an empty log for
     * DataMigration Tx. Empty log just used by RecoverTx.
     */
    WriteToLogOp write_first_prepare_log_op_;
    /**
     * @brief Write a log to indicate that this data migrate tx is going to be
     * migrating this bucket. This is to make sure the write lock acquired by
     * prepare_bucket_lock_op_ can be correctly recovered.
     */
    WriteToLogOp write_before_locking_log_op_;
    /**
     * @brief Acquire bucket write lock on all node groups.
     */
    AcquireAllOp prepare_bucket_lock_op_;
    /**
     * @brief Write prepare log for bucket migration
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Install dirty bucket record on CcMap and downgrade write lock to
     * write intent lock.
     */
    PostWriteAllOp install_dirty_bucket_op_;
    /**
     * @brief Flush data in this bucket into data store so that after bucket
     * is migrated the new bucket owner can access latest data from data store.
     */
    AsyncOp<Void> data_sync_op_;
    /**
     * @brief Upgrade write intent lock to write lock on all node group.
     */
    AcquireAllOp acquire_bucket_lock_op_;
    /**
     * @brief Write commit log for bucket migration
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Kick out data in this bucket from memory before switching bucket
     * owner.
     */
    KickoutDataOp kickout_data_op_;
    /**
     * @brief Commit dirty bucket record and release bucket write lock
     */
    PostWriteAllOp post_all_bucket_lock_op_;
    /**
     * @brief Write clean log for bucket migration
     */
    WriteToLogOp clean_log_op_;
    /**
     * @brief When all bucket migration are finished, we will write the last
     * clean log to log service. This op will also erase empty log of
     * DataMigration. once this operation is finished, we will set finished flag
     * to true on MigrateStatus.
     * Note: Whether write_last_clean_log_op_ is needed needs more
     * consideration.
     */
    WriteToLogOp write_last_clean_log_op_;

    size_t migrate_bucket_idx_{0};
    std::vector<RangeBucketKey> bucket_keys_;
    std::vector<RangeBucketRecord> bucket_records_;
    std::vector<BucketInfo> bucket_info_;

private:
    void FillLogRequest(TransactionExecution *txm,
                        WriteToLogOp *log_op,
                        TxLogType log_type,
                        txlog::BucketMigrateStage migrate_stage);

    void FillFirstLogRequest(TransactionExecution *txm,
                             std::vector<uint64_t> &migration_txns);
    void FillLastLogRequest(TransactionExecution *txm);

    void ForceToFinish(TransactionExecution *txm);
    void Clear();

    // the snapshot of the ranges that we need to migrate for current bucket.
    // Note that the table name here is of type range partition.
    std::unordered_map<TableName, std::unordered_set<int32_t>>
        ranges_in_bucket_snapshot_;

    std::unordered_map<TableName, std::unordered_set<int32_t>>::const_iterator
        kickout_range_tbl_it_;
    std::unordered_set<int32_t>::const_iterator kickout_range_it_;
    TableName kickout_range_table_{
        std::string(""), TableType::Primary, TableEngine::None};

    std::unordered_set<TableName> hash_partitioned_tables_snapshot_;
    std::unordered_set<TableName>::const_iterator
        kickout_hash_partitioned_tbl_it_;

    std::shared_ptr<DataMigrationStatus> status_;
};

struct BatchReadOperation : TransactionOperation
{
public:
    explicit BatchReadOperation(
        TransactionExecution *txm,
        CcHandlerResult<ReadKeyResult> *lock_range_result = nullptr);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    bool IsFinished() const
    {
        return unfinished_cnt_.load(std::memory_order_relaxed) == 0;
    }

    BatchReadTxRequest *batch_read_tx_req_{nullptr};
    std::vector<CcHandlerResult<ReadKeyResult>> hd_result_vec_;
    bool local_cache_checked_;  // If checked local cache for this op
    std::atomic<uint32_t> unfinished_cnt_{0};

    CcHandlerResult<ReadKeyResult> *lock_range_bucket_result_{nullptr};
    size_t lock_index_{0};
    std::vector<int32_t> range_ids_;
};

struct InvalidateTableCacheOp : TransactionOperation
{
public:
    explicit InvalidateTableCacheOp(TransactionExecution *txm);

    void Reset(uint32_t hres_ref_cnt);

    void ResetHandlerTxm(TransactionExecution *txm);

    void Forward(TransactionExecution *txm);

    const TableName *table_name_;

    CcHandlerResult<Void> hd_result_;
};

struct InvalidateTableCacheCompositeOp : CompositeTransactionOperation
{
    InvalidateTableCacheCompositeOp() = delete;

    InvalidateTableCacheCompositeOp(const TableName *table_name,
                                    TransactionExecution *txm);

    void Reset(const TableName *table_name, TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    const TableName *table_name_;

    CatalogKey catalog_key_;
    CatalogRecord catalog_rec_;

    AcquireAllOp acquire_all_lock_op_;

    InvalidateTableCacheOp invalidate_table_cache_op_;

    PostWriteAllOp post_all_lock_op_;
};

}  // namespace txservice
