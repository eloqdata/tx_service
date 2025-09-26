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
             OperationType op_type);

    void FillPrepareLogRequestCommon(TransactionExecution *txm,
                                     WriteToLogOp &prepare_log_op);
    void FillCommitLogRequestCommon(TransactionExecution *txm,
                                    WriteToLogOp &commit_log_op);
    void FillCleanLogRequestCommon(TransactionExecution *txm,
                                   WriteToLogOp &clean_log_op);

    CatalogKey table_key_;  // string owner
    CatalogRecord catalog_rec_;
    std::string image_str_{""};
    std::string dirty_image_str_{""};
    uint64_t curr_schema_ts_;
    OperationType op_type_{OperationType::Insert};
};

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
     * @brief The current stage of this multi-stage schema operation.
     *
     */
    TransactionOperation *op_{nullptr};
    /**
     * @brief Acquire read lock on local cluster config ccmap to block cluster
     * config update during upsert table op. We cannot allow config update
     * between acquire write all and post write all.
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
     * @brief Release cluster config read lock after post write all.
     */
    PostReadOperation unlock_cluster_config_op_;
    /**
     * @brief Creates/deletes the data store table and persists/removes the
     * binary representation of the catalog in the data store.
     *
     */
    DsUpsertTableOp upsert_kv_table_op_;
    /**
     * @brief Update the sequence table for range id info and the auto increment
     * value if this table has auto_increment column.
     */
    AsyncOp<Void> update_sequence_table_op_;
    /**
     * @brief Upgrades acquired write intents to write locks in all nodes.
     *
     */
    AcquireAllOp acquire_all_lock_op_;
    /**
     * @brief Flushes the commit log to the log service. The commit log confirms
     * that the data store operation succeeds and does not need redo upon
     * failures.
     *
     */
    WriteToLogOp commit_log_op_;

    /**
     * @brief Clean ccmap on all node groups
     *
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
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
    // Due to term or other error, called ForceToFinish to terminate this
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

// To remove remainder records' lock when scan close
struct ReleaseScanExtraLockOp : TransactionOperation
{
    explicit ReleaseScanExtraLockOp(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
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
