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

#include <memory>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc/cc_handler.h"
#include "cc/ccm_scanner.h"
#include "cc_protocol.h"
#include "command_set.h"
#include "metrics.h"
#include "read_write_set.h"
#include "spinlock.h"
#include "tx_index_operation.h"
#include "tx_operation.h"
#include "tx_req_result.h"
#include "txlog.h"

namespace txservice
{
struct TxRequest;
struct InitTxRequest;
struct ReadTxRequest;
struct ReadOutsideTxRequest;
struct ScanOpenTxRequest;
struct ScanBatchTxRequest;
struct ScanCloseTxRequest;
struct UpsertTxRequest;
struct CommitTxRequest;
struct AbortTxRequest;
struct UpsertTableTxRequest;
struct ObjectCommandTxRequest;
struct MultiObjectCommandTxRequest;
struct PublishTxRequest;
struct ReloadCacheTxRequest;
struct FaultInjectTxRequest;
struct CleanCcEntryForTestTxRequest;
struct CleanArchivesTxRequest;
struct ScanBatchTuple;
struct SplitFlushTxRequest;
struct AnalyzeTableTxRequest;
struct BroadcastStatisticsTxRequest;
struct ClusterScaleTxRequest;
struct SchemaRecoveryTxRequest;
struct RangeSplitRecoveryTxRequest;
struct UnlockTuple;
struct BatchReadTxRequest;
struct DataMigrationTxRequest;
struct InvalidateTableCacheTxRequest;

class TxProcessor;

enum struct TxmStatus
{
    Idle = 0,
    Busy,
    Finished,
    ForwardFailed
};

static const uint64_t RedisDBCnt = 16;

class LinkedTransaction
{
public:
    LinkedTransaction *prev_{nullptr};
    LinkedTransaction *next_{nullptr};
};

class TransactionExecution : public LinkedTransaction
{
public:
    using uptr = std::unique_ptr<TransactionExecution>;

    // The number of read/write/scan keys when the tx is considered to be
    // "large"
    static const uint32_t LargeTxKeySize = 1000;
    static const uint32_t LoopCnt = 10000;

    TransactionExecution(CcHandler *handler,
                         TxLog *tx_log,
                         TxProcessor *tx_processor,
                         bool bind_to_ext_proc = false);

    TransactionExecution(const TransactionExecution &) = delete;

    /**
     * Interface for TxProcessor.
     */
    /**
     * @brief Resets the internal states of the tx state machine. Called when
     * the tx finishes.
     *
     */
    void Reset();

    /**
     * @brief Restarts the tx state machine when it is reused for a new
     * user-level tx, allowing it to receive tx requests.
     *
     */
    void Restart(CcHandler *handler,
                 TxLog *tx_log,
                 TxProcessor *tx_processor,
                 bool bind_to_ext_proc = false);

    /**
     * @brief Check whether transction is idle and waiting for new TxRequest
     * from runtime.
     */
    bool IsIdle();

    /**
     * @brief Process different kinds of TxRequests based on the request state
     * machine by TxProcessor.
     */

    /**
     * @brief BeginRequest specifies the isolation level and cc protocol.
     */
    void ProcessTxRequest(InitTxRequest &begin_req);
    void ProcessTxRequest(ReadTxRequest &read_req);
    void ProcessTxRequest(ReadOutsideTxRequest &read_outside_req);
    void ProcessTxRequest(ScanOpenTxRequest &scan_open_req);
    void ProcessTxRequest(ScanBatchTxRequest &scan_batch_req);
    void ProcessTxRequest(ScanCloseTxRequest &scan_close_req);
    void ProcessTxRequest(UpsertTxRequest &upsert_req);
    void ProcessTxRequest(CommitTxRequest &commit_req);
    void ProcessTxRequest(AbortTxRequest &abort_req);
    void ProcessTxRequest(UpsertTableTxRequest &req);
    void ProcessTxRequest(ObjectCommandTxRequest &req);
    void ProcessTxRequest(MultiObjectCommandTxRequest &req);
    void ProcessTxRequest(PublishTxRequest &req);
    void ProcessTxRequest(ReloadCacheTxRequest &req);
    void ProcessTxRequest(FaultInjectTxRequest &fi_req);
    void ProcessTxRequest(CleanCcEntryForTestTxRequest &clean_req);
    void ProcessTxRequest(CleanArchivesTxRequest &clean_req);
    void ProcessTxRequest(SplitFlushTxRequest &split_flush_req);
    void ProcessTxRequest(AnalyzeTableTxRequest &analyze_req);
    void ProcessTxRequest(BroadcastStatisticsTxRequest &analyze_req);
    void ProcessTxRequest(ClusterScaleTxRequest &scale_req);
    void ProcessTxRequest(SchemaRecoveryTxRequest &recover_req);
    void ProcessTxRequest(RangeSplitRecoveryTxRequest &recover_req);
    void ProcessTxRequest(BatchReadTxRequest &batch_read_req);
    void ProcessTxRequest(DataMigrationTxRequest &data_migration_req);
    void ProcessTxRequest(InvalidateTableCacheTxRequest &req);

    /**
     * Interface for storage engine runtime.
     */
    /**
     * Put request into next_req_ and wait to be processed.
     * The hypothesis is that a client can only execute one request at a time,
     * and needs to call request.Wait() to wait for finish signal.
     */
    int Execute(TxRequest *tx_req);

    void InitTx(IsolationLevel iso_level,
                CcProtocol protocol,
                NodeGroupId tx_ng_id = UINT32_MAX,
                bool start_now = false,
                const std::function<void()> *yield_func = nullptr,
                const std::function<void()> *resume_func = nullptr);
    std::unique_ptr<InitTxRequest> init_tx_req_;
    bool CommitTx(CommitTxRequest &commit_req);
    std::unique_ptr<CommitTxRequest> commit_tx_req_;
    size_t OpenTxScan(ScanOpenTxRequest &scan_open_tx_req);
    void CloseTxScan(uint64_t alias,
                     const TableName &table_name,
                     std::vector<UnlockTuple> &unlock_vec);

    TxErrorCode Insert(const TableName &table_name,
                       TxKey tx_key,
                       TxRecord::Uptr rec);

    TxErrorCode TxUpsert(const TableName &table_name,
                         uint64_t schema_version,
                         TxKey key,
                         TxRecord::Uptr rec,
                         OperationType op,
                         bool check_unique = false);

    void TxRevert(const TableName &table_name, const TxKey &key);

    /**
     * General Interface
     */
    uint64_t TxNumber() const;

    int64_t TxTerm() const;

    uint16_t CommandId() const;

    uint64_t CommitTs() const;

    uint32_t TxCcNodeId() const;

    TxnStatus TxStatus() const;

    void SetRecoverTxState(uint64_t txn, int64_t tx_term, uint64_t commit_ts);

    CcProtocol GetCcProtocol() const
    {
        return protocol_;
    }

    IsolationLevel GetIsolationLevel() const
    {
        return iso_level_;
    }

    void SetIsolationLevel(IsolationLevel iso_level)
    {
        iso_level_ = iso_level;
    }

    uint64_t GetStartTs() const
    {
        return start_ts_;
    }

    void SetStartTs(uint64_t ts)
    {
        start_ts_ = ts;
    }

    /**
     * @brief EloqDoc use data write_set size to decide whether it is inside a
     * DML transaction.
     */
    size_t DataWriteSetSize() const
    {
        return rw_set_.WriteSetSize();
    }

    TxProcessor *GetTxProcessor()
    {
        return tx_processor_;
    }

    /**
     * @brief Check if current node is still leader of the transaction owner
     * node group.
     */
    bool CheckLeaderTerm() const
    {
        NodeGroupId ng_id = TxCcNodeId();
        if (Sharder::Instance().CheckLeaderTerm(ng_id, TxTerm()) ||
            (TxStatus() == TxnStatus::Recovering &&
             Sharder::Instance().CandidateLeaderTerm(ng_id) >= 0))
        {
            return true;
        }

        return false;
    }

    bool CheckStandbyTerm() const
    {
        return TxCcNodeId() == Sharder::Instance().NativeNodeGroup() &&
               Sharder::Instance().StandbyNodeTerm() == TxTerm();
    }

#ifdef EXT_TX_PROC_ENABLED
    void Enlist();
    /**
     *
     * @param enlist_txm_if_fails
     * @return Whether successfully forward the txm.
     */
    bool ExternalForward(bool enlist_txm_if_fails = true);
#endif
    void RecoverDataMigration(const ::txlog::BucketMigrateMessage *migrate_msg,
                              size_t cur_idx,
                              std::shared_ptr<DataMigrationStatus> status);

    void RecoverClusterScale(const ::txlog::ClusterScaleOpMessage &scale_msg,
                             bool dm_started,
                             bool dm_finished);

    /**
     * @brief Acquires the exclusive forward latch.
     *
     * This should only be called by txm.Forward() to avoid hd_res being freed
     * or timed out when CcStreamReceiver is accessing hd_res in txm.
     */
    bool AcquireExclusiveForwardLatch()
    {
        int32_t expect = 0;
        return forward_latch_.compare_exchange_strong(
            expect, -1, std::memory_order_acquire, std::memory_order_relaxed);
    }

    void ReleaseExclusiveForwardLatch()
    {
        forward_latch_.store(0, std::memory_order_release);
    }

    /**
     * @brief Acquires the shared forward latch.
     *
     * This should only be called by CcStreamReceiver to avoid hd_res being
     * freed or timed out by txm.Forward().
     */
    void AcquireSharedForwardLatch()
    {
        int32_t expect = 0;
        while (
            !forward_latch_.compare_exchange_strong(expect,
                                                    expect + 1,
                                                    std::memory_order_acquire,
                                                    std::memory_order_relaxed))
        {
            if (expect == -1)
            {
                // txm is being forwarded.
                expect = 0;
                // yeild this bthread for now since txm forward might take a
                // while
                bthread_usleep(1000);
            }
        }
    }

    void ReleaseSharedForwardLatch()
    {
        int32_t res = forward_latch_.fetch_sub(1, std::memory_order_release);
        assert(res > 0);
        // This silences the -Wunused-but-set-variable warning without any
        // runtime overhead.
        (void) res;
    }

private:
    /**
     * @brief Moves forward the tx state machine and transitions the machine to
     * next state if not blocked on the current state. A user's tx request
     * causes the tx state machine to transition. A single tx requests may lead
     * to a chain of transitions to different states. For example, a commit
     * request consists of acquiring write intentions, setting the commit
     * timestamp, writing the tx log and installing committed values. Some
     * transitions may not return immediately, causing the tx state machine to
     * be blocked on the current state. The tx state machine employs async
     * programming. Forward() checks if the current transition finishes, and if
     * so, moves to the next state, until reaching the final state of the
     * current tx request and notifying the user the tx request's result. When
     * being blocked, Forward() returns the control to the processing thread,
     * allowing it to switch to another tx state machine or process concurrency
     * control requests directed to the binding shard.
     *
     */
    TxmStatus Forward();

    void PushOperation(TransactionOperation *op, int retry_num = RETRY_NUM);

    /**
     * Process Operations.
     * The TxRequest is responsible for putting the corresponding operations
     * into state_stack. The first Forward call will start to process these
     * operations.
     */

    void Process(InitTxnOperation &init_txn);
    void PostProcess(InitTxnOperation &init_txn);
    void Process(ReadOperation &read);
    void PostProcess(ReadOperation &read);
    void Process(ReadLocalOperation &lock_local);
    void PostProcess(ReadLocalOperation &lock_local);
    void Process(ScanOpenOperation &scan_open);
    void PostProcess(ScanOpenOperation &scan_open);
    void Process(ScanNextOperation &scan_next);
    void PostProcess(ScanNextOperation &scan_next);
    void Process(LockWriteRangeBucketsOp &lock_write_range_buckets);
    void PostProcess(LockWriteRangeBucketsOp &lock_write_range_buckets);
    void Process(AcquireWriteOperation &acquire_write);
    void PostProcess(AcquireWriteOperation &acquire_write);
    void Process(CatalogAcquireAllOp &acquire_catalog_write);
    void PostProcess(CatalogAcquireAllOp &acquire_catalog_write);
    void Process(SetCommitTsOperation &set_ts);
    void PostProcess(SetCommitTsOperation &set_ts);
    void Process(ValidateOperation &validate);
    void PostProcess(ValidateOperation &validate);
    void Process(UpdateTxnStatus &update_txn);
    void PostProcess(UpdateTxnStatus &update_txn);
    void Process(PostProcessOp &post_process);
    void PostProcess(PostProcessOp &post_process);
    void Process(WriteToLogOp &write_log);
    void PostProcess(WriteToLogOp &write_log);
    void Process(ReloadCacheOperation &reload_cache_op);
    void PostProcess(ReloadCacheOperation &reload_cache_op);
    void Process(FaultInjectOp &fault_inject_op);
    void PostProcess(FaultInjectOp &fault_inject_op);
    void Process(CleanCcEntryForTestOp &clean_entry_op);
    void PostProcess(CleanCcEntryForTestOp &clean_entry_op);
    void Process(AnalyzeTableAllOp &analyze_table_op);
    void PostProcess(AnalyzeTableAllOp &analyze_table_op);
    void Process(BroadcastStatisticsOp &broadcast_stat_op);
    void PostProcess(BroadcastStatisticsOp &broadcast_stat_op);
    void Process(KickoutDataOp &kickout_data_all_op);
    void PostProcess(KickoutDataOp &kickout_data_all_op);

    void Process(AcquireAllOp &acq_all_op);
    void PostProcess(AcquireAllOp &acq_all_op);
    void Process(PostWriteAllOp &post_write_all_op);
    void PostProcess(PostWriteAllOp &post_write_all_op);

    void Process(DsUpsertTableOp &ds_upsert_table_op);
    void PostProcess(DsUpsertTableOp &ds_upsert_table_op);

    void Process(PostReadOperation &post_read_operation);
    void PostProcess(PostReadOperation &post_read_operation);

    void Process(NoOp &no_op);
    void PostProcess(NoOp &no_op);

    template <typename ResultType>
    void Process(AsyncOp<ResultType> &ds_op);
    template <typename ResultType>
    void PostProcess(AsyncOp<ResultType> &ds_op);

    void Process(ReleaseScanExtraLockOp &unlock_op);
    void PostProcess(ReleaseScanExtraLockOp &unlock_op);

    void Process(ObjectCommandOp &obj_cmd_op);
    void PostProcess(ObjectCommandOp &obj_cmd_op);

    void Process(MultiObjectCommandOp &obj_cmd_op);
    void PostProcess(MultiObjectCommandOp &obj_cmd_op);

    void Process(CmdForwardAcquireWriteOp &forward_write_op);
    void PostProcess(CmdForwardAcquireWriteOp &forward_write_op);

    void Process(KickoutDataAllOp &kickout_data_all_op);
    void PostProcess(KickoutDataAllOp &kickout_data_all_op);

    void Process(NotifyStartMigrateOp &notify_migration_op);
    void PostProcess(NotifyStartMigrateOp &notify_migration_op);

    void Process(CheckMigrationIsFinishedOp &notify_migration_finished_op);
    void PostProcess(CheckMigrationIsFinishedOp &notify_migration_finished_op);

    void Process(InvalidateTableCacheOp &invalidate_table_cache_op);
    void PostProcess(InvalidateTableCacheOp &invalidate_table_cache_op);

    // Process TxRequests without Operations. These TxRequests can be executed
    // immediately without using CcRequests.
    void ScanClose(const std::vector<UnlockTuple> &unlock_batch,
                   size_t alias,
                   const TableName &table_name);

    void Upsert(const TableName &table_name,
                uint64_t schema_version,
                TxKey key,
                TxRecord::Uptr rec,
                OperationType op);

    void Commit();
    void Abort();

    bool FillDataLogRequest(WriteToLogOp &write_log);
    bool FillCommitCatalogsLogRequest(WriteToLogOp &write_log);
    bool FillCleanCatalogsLogRequest(WriteToLogOp &write_log);

    bool IsTimeOut(int wait_secs = 10);
    void StartTiming();

    void ReleaseMetaDataReadLock(CcHandlerResult<PostProcessResult> &hd_result);
    void ReleaseCatalogWriteAll(CcHandlerResult<PostProcessResult> &hd_result);
    void DrainScanner(CcScanner *scanner, const TableName &table_name);

    static TxErrorCode ConvertCcError(CcErrorCode error);

    ScanCloseTxRequest *NextScanCloseTxReq(size_t alias,
                                           const TableName &table_name);

    void Process(BatchReadOperation &batch_read_op);
    void PostProcess(BatchReadOperation &batch_read_op);

    TxRequest *DequeueTxRequest()
    {
        TxRequest *req = nullptr;
        if (bind_to_ext_proc_)
        {
            if (tx_req_queue_.Size() > 0)
            {
                req = tx_req_queue_.Peek();
                tx_req_queue_.Dequeue();
            }
        }
        else
        {
            req_queue_lock_.Lock();
            if (tx_req_queue_.Size() > 0)
            {
                req = tx_req_queue_.Peek();
                tx_req_queue_.Dequeue();
            }
            req_queue_lock_.Unlock();
        }

        return req;
    }

    size_t TxRequestCount()
    {
        if (bind_to_ext_proc_)
        {
            return tx_req_queue_.Size();
        }
        else
        {
            req_queue_lock_.Lock();
            size_t cnt = tx_req_queue_.Size();
            req_queue_lock_.Unlock();
            return cnt;
        }
    }

    LockType DeduceReadLockType(TableType tbl_type,
                                bool read_for_write,
                                IsolationLevel iso_level,
                                bool is_covering_key,
                                RecordStatus rec = RecordStatus::Normal);

    void AdvanceCommand()
    {
        command_id_.fetch_add(1, std::memory_order_relaxed);
    }

    const BucketInfo *FastToGetBucket(uint16_t bucket_id);

    void ClearCachedBucketInfos();

    void ReleaseCatalogsRead();

    enum struct TxType
    {
        Data = 0,
        Schema,
        PartitionFunction
    };

    CcHandler *cc_handler_;
    TxLog *txlog_;
    TxProcessor *tx_processor_;

    TxId txid_;
    // The tx number is a global identifier of the tx in the cluster. It differs
    // from TxId in that TxId includes additional information to physically
    // locate the tx entry without additional lookups. The tx number is wrapped
    // by std::atomic so as to allow a remote cc request's response to match
    // against the tx number before setting the cc handler result. Matching tx
    // number is necessary because the tx may abort proactively, after not
    // receiving the response of the cc request for an extended period of time.
    std::atomic<uint64_t> tx_number_;
    int64_t tx_term_;
    uint64_t start_ts_;
    uint64_t commit_ts_;
    uint64_t commit_ts_bound_;
    std::atomic<TxnStatus> tx_status_;

    // The command id is a identifier to distinguish whether the cc request's
    // response is expired. For remote cc request's responses do not
    // guarantee the sequence and we re-run tx operations after timeout, that
    // is, same one operation's cc requests maybe sent repeatedly even if the
    // previous one is being handled, the result is chaos once some one of
    // the expired cc request's responses reaches before transaction finishing.
    // The {command_id_} increases when TxExectuion handle a new TxRequest or
    // send remote cc requests.
    std::atomic<uint16_t> command_id_;

    // This latch is used to avoid concurrent update of txm status between
    // txm.Forward() and CcStreamReceiver. CcStreamReceiver will update hd_res
    // in txm, when hd_res is being modified, txm.Forward() will be blocked to
    // avoid hd_res being freed or timed out.
    // Positive numbers(1,2,3...) denotes how many responses are being handled
    // by stream thread, and txm can not timeout when result_status_>0. Negative
    // number(-1) denotes txm is being forwarded.
    std::atomic<int32_t> forward_latch_{0};

    // The number of calls to Forward() at a given state.
    uint32_t state_forward_cnt_;

    // The local time when the tx machine first moves to its current state.
    uint64_t state_clock_;

    std::vector<TransactionOperation *> state_stack_;
    size_t idle_rep_;

    // local cache of read/write entries.
    ReadWriteSet rw_set_;
    // local cache of command entries.
    CommandSet cmd_set_;
    // when read an entry, it may not exist in ccmap. In this case, we create a
    // empty record in ccmap and add read intention for it. Then we read the
    // entry from data store and backfill the ccmap. cache_miss_read_cce_addr_
    // can help us to locate the previous empty cc entry quickly.
    CcEntryAddr cache_miss_read_cce_addr_;

    std::unique_ptr<UpsertTableOp> schema_op_;

    std::unique_ptr<SplitFlushRangeOp> split_flush_op_;

    std::unique_ptr<UpsertTableIndexOp> index_op_;

    std::unique_ptr<ClusterScaleOp> cluster_scale_op_;
    std::unique_ptr<DataMigrationOp> migration_op_;

    std::unique_ptr<InvalidateTableCacheCompositeOp>
        invalidate_table_cache_composite_op_;

    std::unordered_map<
        uint64_t,
        std::pair<TableWriteSet::const_iterator, TableWriteSet::const_iterator>>
        wset_iters_;
    std::unordered_map<uint64_t,
                       std::pair<TableWriteSet::const_reverse_iterator,
                                 TableWriteSet::const_reverse_iterator>>
        wset_reverse_iters_;

    /**
     * @brief A collection of open primary/secondary index scans. Each scan is
     * identified by a scan alias (of type uint64_t) generated when the scan is
     * opened.
     *
     */
    std::unordered_map<uint64_t, ScanState> scans_;
    uint64_t scan_alias_cnt_{0};

    // Response whose returned result is void
    TxResult<Void> *void_resp_;
    // Response whose returned result is record, used by ObjectCommandTxRequest
    TxResult<RecordStatus> *rec_resp_;
    // Response whose returned vector of RecordStatus, used by
    // MultiObjectCommandTxRequest
    TxResult<std::vector<RecordStatus>> *vct_rec_resp_;
    // Response whose returned result is record-ts-pair, used by ReadTxRequest
    TxResult<std::pair<RecordStatus, uint64_t>> *rtp_resp_;
    // Response whose returned result is bool
    TxResult<bool> *bool_resp_;
    // Scan result
    TxResult<
        std::tuple<const TxKey *, const TxRecord *, RecordStatus, uint64_t>>
        *kvp_resp_;
    // Scan open result
    TxResult<size_t> *uint64_resp_;
    // Response whose returned result is vector<int64_t>
    TxResult<std::vector<int64_t>> *int64_vec_resp_;
    // Response for UpsertTableTxRequest or UpsertTableIndexOp
    TxResult<UpsertResult> *upsert_resp_;
    // Response for ClusterScaleTxRequest
    TxResult<std::string> *string_resp_;

    // tx_req_queue_ is used to exchange request between runtime and
    // TxProcessor.
    CircularQueue<TxRequest *> tx_req_queue_;
    SimpleSpinlock req_queue_lock_;

    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};

    bool bind_to_ext_proc_{false};

    // Initialization phase.
    InitTxnOperation init_txn_;

    ReadLocalOperation lock_cluster_config_op_;
    // Execution phase.
    CcHandlerResult<ReadKeyResult> lock_range_bucket_result_;
    ReadLocalOperation lock_range_op_;
    RangeRecord range_rec_;
    // TODO(lzx): decrease these member fields.
    ReadLocalOperation lock_bucket_op_;
    RangeBucketKey bucket_key_;
    // Used to wrap "bucket_key_" when read bucket through "lock_bucket_op_",
    // because ReadLocalOperation::key_ is a TxKey pointer.
    TxKey bucket_tx_key_;
    RangeBucketRecord bucket_rec_;
    // Cache locked bucket infos: {bucket_id->BucketInfo*}
    std::unordered_map<uint16_t, const BucketInfo *> locked_buckets_;
    // Fast path to fetch bucket if no bucket is migrating.
    const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>>
        *all_bucket_infos_{nullptr};
    ReadOperation read_;
    ScanOpenOperation scan_open_;
    ScanNextOperation scan_next_;
    // Temporarily save scan tuple when drain out the remainder scan tuples.
    // To avoid ccentry address to be saved in stack
    std::vector<std::pair<CcEntryAddr, uint64_t>> drain_batch_;

    std::unique_ptr<CircularQueue<std::unique_ptr<ScanCloseTxRequest>>>
        scan_close_req_pool_{nullptr};

    // The dbs (for EloqKV) this txm has read.
    // All elements are value-initialized to {nullptr,0(schema_version)}
    // TODO: accommodate for EloqSql
    std::array<std::pair<NonBlockingLock *, uint64_t>, RedisDBCnt> locked_db_{};

    // TODO(zkl): allocate these fields on heap since they are rarely used.
    ReadLocalOperation read_catalog_op_;
    CatalogKey read_catalog_key_;
    // not used, only need to load the catalog and add read lock
    CatalogRecord read_catalog_record_;
    TxKey catalog_tx_key_;
    CcHandlerResult<ReadKeyResult> read_catalog_result_;

    ObjectCommandOp obj_cmd_;
    MultiObjectCommandOp multi_obj_cmd_;

    // Committing phase.
    LockWriteRangeBucketsOp lock_write_range_buckets_;
    CmdForwardAcquireWriteOp cmd_forward_write_;
    AcquireWriteOperation acquire_write_;
    CatalogAcquireAllOp catalog_acquire_all_;
    SetCommitTsOperation set_ts_;
    ValidateOperation validate_;
    UpdateTxnStatus update_txn_;
    PostProcessOp post_process_;
    WriteToLogOp write_log_;
    FlushUpdateTableOp flush_update_table_;
    SleepOperation sleep_op_;

    // analyze table
    AnalyzeTableAllOp analyze_table_all_op_;

    // broadcast statistics
    BroadcastStatisticsOp broadcast_stat_op_;

    // reload acl and cache
    ReloadCacheOperation reload_cache_op_;

    // fault inject
    FaultInjectOp fault_inject_op_;

    // clean archives
    CleanCcEntryForTestOp clean_entry_op_;

    ReleaseScanExtraLockOp abundant_lock_op_;
    BatchReadOperation batch_read_op_;

    metrics::TimePoint tx_duration_start_;
    bool is_collecting_duration_round_{false};

    friend struct TransactionOperation;
    friend struct CompositeTransactionOperation;
    friend struct ReadOperation;
    friend struct ReadLocalOperation;
    friend struct UnlockReadRangeOperation;
    friend struct LockReadRangesOp;
    friend struct ReadOutsideOperation;
    friend struct LockWriteRangeBucketsOp;
    friend struct AcquireWriteOperation;
    friend struct SetCommitTsOperation;
    friend struct WriteToLogOp;
    friend struct UpdateTxnStatus;
    friend struct ValidateOperation;
    friend struct InitTxnOperation;
    friend struct PostProcessOp;
    friend struct ScanOpenOperation;
    friend struct ScanNextOperation;
    friend struct ReleaseScanExtraLockOp;
    friend struct ReloadCacheOperation;
    friend struct FaultInjectOp;
    friend struct AcquireAllOp;
    friend struct PostWriteAllOp;
    friend struct CatalogAcquireAllOp;
    friend struct UpsertTableOp;
    friend struct DsUpsertTableOp;
    friend struct FlushUpdateTableOp;
    friend struct SleepOperation;
    friend struct CleanCcEntryForTestOp;
    friend struct CleanArchivesOp;
    friend struct SplitFlushRangeOp;
    friend struct DsSplitOp;
    friend struct AnalyzeTableAllOp;
    friend struct BroadcastStatisticsOp;
    friend struct KickoutDataOp;
    friend struct ClusterScaleOp;
    friend struct NoOp;
    template <typename ResultType>
    friend struct AsyncOp;
    friend struct PostReadOperation;
    friend struct ObjectCommandOp;
    friend struct MultiObjectCommandOp;
    friend struct CmdForwardAcquireWriteOp;
    friend struct KickoutDataAllOp;
    friend struct UpsertTableIndexOp;
    friend struct DataMigrationOp;
    friend struct NotifyStartMigrateOp;
    friend struct CheckMigrationIsFinishedOp;
    friend struct BatchReadOperation;
    friend struct InvalidateTableCacheOp;
    friend struct InvalidateTableCacheCompositeOp;
    friend class TxProcessor;
};
}  // namespace txservice
