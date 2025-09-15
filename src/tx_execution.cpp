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
#include "tx_execution.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "cc_protocol.h"
#include "error_messages.h"  //CcErrorCode
#include "local_cc_shards.h"
#include "log.pb.h"
#include "log_type.h"
#include "scan.h"
#include "sharder.h"
#include "tx_command.h"
#include "tx_key.h"
#include "tx_operation.h"
#include "tx_operation_result.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_trace.h"
#include "tx_util.h"
#include "type.h"

DEFINE_bool(cmd_read_catalog,
            true,
            "First read catalog when executing commands");

namespace txservice
{
TransactionExecution::TransactionExecution(CcHandler *handler,
                                           TxLog *txlog,
                                           TxProcessor *tx_processor,
                                           bool bind_to_ext_proc)
    : cc_handler_(handler),
      txlog_(txlog),
      tx_processor_(tx_processor),
      txid_(UINT32_MAX),
      tx_number_(UINT64_MAX),
      tx_term_(-1),
      commit_ts_(UINT64_MAX),
      commit_ts_bound_(0),
      tx_status_(TxnStatus::Ongoing),
      command_id_{0},
      forward_latch_(0),
      rw_set_(),
      cache_miss_read_cce_addr_(),
      scans_(),
      void_resp_(nullptr),
      rec_resp_(nullptr),
      vct_rec_resp_(nullptr),
      rtp_resp_(nullptr),
      bool_resp_(nullptr),
      kvp_resp_(nullptr),
      uint64_resp_(nullptr),
      bind_to_ext_proc_(bind_to_ext_proc),
      init_txn_(this),
      lock_range_bucket_result_(this),
      lock_bucket_op_(),
      bucket_key_(),
      bucket_tx_key_(&bucket_key_),
      read_(this, &lock_range_bucket_result_),
      scan_open_(this),
      scan_next_(this),
      read_catalog_op_(),
      catalog_tx_key_(&read_catalog_key_),
      read_catalog_result_(this),
      obj_cmd_(this, &lock_range_bucket_result_),
      multi_obj_cmd_(this, &lock_range_bucket_result_),
      lock_write_range_buckets_(&lock_range_bucket_result_),
      cmd_forward_write_(this),
      acquire_write_(this),
      catalog_acquire_all_(this),
      set_ts_(this),
      validate_(this),
      update_txn_(this),
      post_process_(this),
      write_log_(this),
      flush_update_table_(this),
      sleep_op_(this),
      analyze_table_all_op_(this),
      broadcast_stat_op_(this),
      reload_cache_op_(this),
      fault_inject_op_(this),
      clean_entry_op_(this),
      abundant_lock_op_(this),
      batch_read_op_(this, &lock_range_bucket_result_)
{
    TX_TRACE_ASSOCIATE(this, cc_handler_);

    init_tx_req_ = std::make_unique<InitTxRequest>();
    commit_tx_req_ = std::make_unique<CommitTxRequest>();
    scan_close_req_pool_ =
        std::make_unique<CircularQueue<std::unique_ptr<ScanCloseTxRequest>>>(8);
}

void TransactionExecution::Reset()
{
    // Skip releasing catalogs read if the tx is not started.
    if (FLAGS_cmd_read_catalog && TxNumber() != UINT64_MAX)
    {
        ReleaseCatalogsRead();
    }
    cache_miss_read_cce_addr_.SetCceLock(0, -1, 0, 0);
    state_stack_.clear();
    txid_.Reset();
    commit_ts_ = UINT64_MAX;
    commit_ts_bound_ = 0;
    rw_set_.Reset();
    cmd_set_.Reset();
    wset_iters_.clear();
    wset_reverse_iters_.clear();
    scans_.clear();
    tx_number_.store(UINT64_MAX, std::memory_order_release);
    command_id_.store(0, std::memory_order_release);
    void_resp_ = nullptr;
    rec_resp_ = nullptr;
    vct_rec_resp_ = nullptr;
    rtp_resp_ = nullptr;
    bool_resp_ = nullptr;
    kvp_resp_ = nullptr;
    uint64_resp_ = nullptr;
    schema_op_ = nullptr;
    split_flush_op_ = nullptr;
    index_op_ = nullptr;
    invalidate_table_cache_composite_op_ = nullptr;

    if (drain_batch_.capacity() > 32)
    {
        drain_batch_.resize(32);
        drain_batch_.shrink_to_fit();
    }
    drain_batch_.clear();

    scan_alias_cnt_ = 0;

    // drain out tx_req_queue_ (if any request left)
    if (bind_to_ext_proc_)
    {
        tx_req_queue_.Reset();
        bind_to_ext_proc_ = false;
#ifdef EXT_TX_PROC_ENABLED
        // Decrement the external_txm_cnt_ of this TxProcessor.
        tx_processor_->coordi_->external_txm_cnt_.fetch_sub(
            1, std::memory_order_relaxed);
#endif
    }
    else
    {
        req_queue_lock_.Lock();
        tx_req_queue_.Reset();
        req_queue_lock_.Unlock();
    }

    ClearCachedBucketInfos();
    lock_cluster_config_op_.Reset();
    lock_range_bucket_result_.Reset();
    lock_range_op_.Reset();
    lock_bucket_op_.Reset();
    lock_write_range_buckets_.Reset();

    tx_term_ = -1;
}

void TransactionExecution::Restart(CcHandler *handler,
                                   TxLog *txlog,
                                   TxProcessor *tx_processor,
                                   bool bind_to_ext_proc)
{
    cc_handler_ = handler;
    txlog_ = txlog;
    tx_processor_ = tx_processor;
    bind_to_ext_proc_ = bind_to_ext_proc;
    tx_status_.store(TxnStatus::Ongoing, std::memory_order_relaxed);
}

bool TransactionExecution::IsIdle()
{
    return state_stack_.empty() && TxRequestCount() == 0;
}

uint64_t TransactionExecution::TxNumber() const
{
    return tx_number_.load(std::memory_order_relaxed);
}

int64_t TransactionExecution::TxTerm() const
{
    return tx_term_;
}

uint16_t TransactionExecution::CommandId() const
{
    return command_id_.load(std::memory_order_relaxed);
}

uint64_t TransactionExecution::CommitTs() const
{
    return commit_ts_;
}

uint32_t TransactionExecution::TxCcNodeId() const
{
    return (tx_number_.load(std::memory_order_relaxed) >> 32L) >> 10;
}

TxnStatus TransactionExecution::TxStatus() const
{
    return tx_status_.load(std::memory_order_relaxed);
}

void TransactionExecution::SetRecoverTxState(uint64_t txn,
                                             int64_t tx_term,
                                             uint64_t commit_ts)
{
    tx_number_.store(txn, std::memory_order_relaxed);
    tx_term_ = tx_term;
    commit_ts_ = commit_ts;
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
}

#ifdef EXT_TX_PROC_ENABLED
void TransactionExecution::Enlist()
{
    if (bind_to_ext_proc_)
    {
        tx_processor_->EnlistTx(this);
    }
}

bool TransactionExecution::ExternalForward(bool enlist_txm_if_fails)
{
    if (bind_to_ext_proc_)
    {
        bool success = tx_processor_->ForwardTx(this);
        if (!success && enlist_txm_if_fails)
        {
            tx_processor_->EnlistTx(this);
        }
        return success;
    }
    return true;
}
#endif

const BucketInfo *TransactionExecution::FastToGetBucket(uint16_t bucket_id)
{
    auto bucket_it = locked_buckets_.find(bucket_id);
    if (bucket_it != locked_buckets_.end())
    {
        return bucket_it->second;
    }

    if (all_bucket_infos_ != nullptr)
    {
        return all_bucket_infos_->at(bucket_id).get();
    }
    else
    {
        CcShard &ccs =
            *tx_processor_->local_cc_shards_.GetCcShard(tx_processor_->thd_id_);
        if (!ccs.IsBucketsMigrating())
        {
            all_bucket_infos_ = ccs.GetAllBucketInfos(TxCcNodeId());
            assert(all_bucket_infos_ != nullptr);
            ccs.IncrNakedBucketReader();
            return all_bucket_infos_->at(bucket_id).get();
        }
    }

    return nullptr;
}

void TransactionExecution::ClearCachedBucketInfos()
{
    locked_buckets_.clear();
    if (all_bucket_infos_ != nullptr)
    {
        CcShard &ccs =
            *tx_processor_->local_cc_shards_.GetCcShard(tx_processor_->thd_id_);
        ccs.DecrNakedBucketReader();
        all_bucket_infos_ = nullptr;
    }
}

void TransactionExecution::ReleaseCatalogsRead()
{
    NodeGroupId ng_id = TxCcNodeId();
    if (BAIDU_UNLIKELY(!Sharder::Instance().CheckLeaderTerm(ng_id, TxTerm()) &&
                       Sharder::Instance().StandbyNodeTerm() != TxTerm()))
    {
        locked_db_ = {};
        return;
    }

    for (auto &db_idx : locked_db_)
    {
        if (db_idx.first != nullptr)
        {
            LocalCcHandler *local_hd =
                static_cast<LocalCcHandler *>(cc_handler_);
            local_hd->ReleaseCatalogRead(db_idx.first);
        }
    }
    locked_db_ = {};
}

TxErrorCode TransactionExecution::ConvertCcError(CcErrorCode error)
{
    switch (error)
    {
    case CcErrorCode::NO_ERROR:
        return TxErrorCode::NO_ERROR;

    case CcErrorCode::FORCE_FAIL:
        return TxErrorCode::INTERNAL_ERR_TIMEOUT;

    case CcErrorCode::REQUESTED_NODE_NOT_LEADER:
        return TxErrorCode::CC_REQ_FOLLOWER;

    case CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH:
    case CcErrorCode::VALIDATION_FAILED_FOR_CONFILICTED_TXS:
        return TxErrorCode::OCC_BREAK_REPEATABLE_READ;

    case CcErrorCode::DEAD_LOCK_ABORT:
        return TxErrorCode::DEAD_LOCK_ABORT;

    case CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT:
        return TxErrorCode::READ_WRITE_CONFLICT;

    case CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT:
    case CcErrorCode::ACQUIRE_GAP_LOCK_FAILED:
        return TxErrorCode::WRITE_WRITE_CONFLICT;

    case CcErrorCode::DUPLICATE_INSERT_ERR:
        return TxErrorCode::DUPLICATE_KEY;

    case CcErrorCode::NG_TERM_CHANGED:
        return TxErrorCode::NG_TERM_CHANGED;

    case CcErrorCode::REQUEST_LOST:
        return TxErrorCode::REQUEST_LOST;

    case CcErrorCode::PIN_RANGE_SLICE_FAILED:
        return TxErrorCode::CKPT_PIN_RANGE_SLICE_FAIL;

    case CcErrorCode::DATA_STORE_ERR:
        return TxErrorCode::DATA_STORE_ERROR;

    case CcErrorCode::OUT_OF_MEMORY:
        return TxErrorCode::OUT_OF_MEMORY;

    case CcErrorCode::GET_RANGE_ID_ERR:
        return TxErrorCode::GET_RANGE_ID_ERROR;

    case CcErrorCode::DATA_NOT_ON_LOCAL_NODE:
        return TxErrorCode::DATA_NOT_ON_LOCAL_NODE;

    case CcErrorCode::READ_CATALOG_FAIL:
        return TxErrorCode::READ_CATALOG_FAIL;

    case CcErrorCode::READ_CATALOG_CONFLICT:
        return TxErrorCode::READ_CATALOG_CONFLICT;
    case CcErrorCode::UNIQUE_CONSTRAINT:
        return TxErrorCode::UNIQUE_CONSTRAINT;
    case CcErrorCode::PACK_SK_ERR:
        return TxErrorCode::CAL_ENGINE_DEFINED_CONSTRAINT;
    case CcErrorCode::INVALID_CURSOR:
        return TxErrorCode::INVALID_CURSOR;

    case CcErrorCode::REQUESTED_TABLE_NOT_EXISTS:
        return TxErrorCode::REQUESTD_TABLE_NOT_EXISTS;
    case CcErrorCode::REQUESTED_INDEX_TABLE_NOT_EXISTS:
        return TxErrorCode::REQUESTD_INDEX_TABLE_NOT_EXISTS;
    case CcErrorCode::REQUESTED_TABLE_SCHEMA_MISMATCH:
        return TxErrorCode::REQUESTD_TABLE_SCHEMA_MISMATCH;

    case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
        return TxErrorCode::ACQUIRE_LOCK_BLOCKED;

    case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
        return TxErrorCode::MVCC_READ_MUST_WAIT_WRITE;
    case CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT:
        return TxErrorCode::SI_R4W_ERR_KEY_WAS_UPDATED;

    case CcErrorCode::TX_NODE_NOT_LEADER:
        return TxErrorCode::TX_NODE_NOT_LEADER;

    case CcErrorCode::NEGOTIATED_TX_UNKNOWN:
        return TxErrorCode::NEGOTIATED_TX_UNKNOWN;
    case CcErrorCode::NEGOTIATE_TX_ERR:
        return TxErrorCode::NEGOTIATE_TX_ERR;

    case CcErrorCode::CREATE_CCM_SCANNER_FAILED:
        return TxErrorCode::CREATE_CCM_SCANNER_FAILED;

    case CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR:
        return TxErrorCode::LOG_SERVICE_UNREACHABLE;
    case CcErrorCode::WRITE_LOG_FAILED:
        return TxErrorCode::WRITE_LOG_FAIL;
    case CcErrorCode::LOG_NODE_NOT_LEADER:
        return TxErrorCode::LOG_NODE_NOT_LEADER;
    case CcErrorCode::DUPLICATE_MIGRATION_TX_ERR:
        return TxErrorCode::DUPLICATE_MIGRATION_TX_ERROR;
    case CcErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERR:
        return TxErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERROR;
    case CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED:
        return TxErrorCode::ESTABLISH_NODE_CHANNEL_FAILED;

    case CcErrorCode::SYSTEM_HANDLER_ERR:
        return TxErrorCode::SYSTEM_HANDLER_ERR;
    case CcErrorCode::TASK_EXPIRED:
        return TxErrorCode::TASK_EXPIRED;
    case CcErrorCode::LOG_NOT_TRUNCATABLE:
        return TxErrorCode::LOG_NOT_TRUNCATABLE;

    case CcErrorCode::UPLOAD_BATCH_REJECTED:
        return TxErrorCode::UPLOAD_BATCH_REJECTED;

    case CcErrorCode::RPC_CALL_ERR:
        return TxErrorCode::RPC_CALL_ERR;

    case CcErrorCode::UPDATE_SEQUENCE_TABLE_FAIL:
        return TxErrorCode::UPDATE_SEQUENCE_TABLE_FAIL;

    case CcErrorCode::UNDEFINED_ERR:
    default:
        return TxErrorCode::UNDEFINED_ERR;
    }
}

TxmStatus TransactionExecution::Forward()
{
    TransactionOperation *prev_op = nullptr;
    uint16_t cmd_id = 0;

    if (!AcquireExclusiveForwardLatch())
    {
        return TxmStatus::ForwardFailed;
    }

    while (true)
    {
        if (!state_stack_.empty())
        {
            TransactionOperation *curr_op = state_stack_.back();
            if (curr_op == prev_op && cmd_id == CommandId())
            {
                break;
            }

            prev_op = curr_op;
            cmd_id = CommandId();
            curr_op->Forward(this);
        }
        else
        {
            prev_op = nullptr;
            cmd_id = 0;

            TxRequest *req = DequeueTxRequest();
            if (req == nullptr)
            {
                break;
            }

            req->Process(this);
        }
    }

    ReleaseExclusiveForwardLatch();

    TxnStatus status = TxStatus();
    if (status == TxnStatus::Finished)
    {
        return TxmStatus::Finished;
    }
    else if (state_stack_.empty() && TxRequestCount() == 0)
    {
        return TxmStatus::Idle;
    }
    else
    {
        return TxmStatus::Busy;
    }
}

int TransactionExecution::Execute(TxRequest *tx_req)
{
    TxnStatus status = tx_status_.load(std::memory_order_relaxed);

    if (status == TxnStatus::Ongoing || status == TxnStatus::Recovering)
    {
        if (bind_to_ext_proc_)
        {
            tx_req_queue_.Enqueue(tx_req);
        }
        else
        {
            req_queue_lock_.Lock();
            tx_req_queue_.Enqueue(tx_req);
            req_queue_lock_.Unlock();
        }
        return 0;
    }
    else
    {
        // The tx has started committing/aborting or has committed/aborted. Does
        // not accept new requests.
        return 1;
    }
}

void TransactionExecution::InitTx(IsolationLevel iso_level,
                                  CcProtocol protocol,
                                  NodeGroupId tx_ng_id,
                                  bool start_now,
                                  const std::function<void()> *yield_func,
                                  const std::function<void()> *resume_func)
{
    if (start_now)
    {
        InitTxRequest init_tx_req{
            iso_level, protocol, yield_func, resume_func, this, tx_ng_id};
        Execute(&init_tx_req);
        init_tx_req.Wait();
    }
    else
    {
        init_tx_req_->Reset();
        init_tx_req_->iso_level_ = iso_level;
        init_tx_req_->protocol_ = protocol;
        init_tx_req_->tx_ng_id_ = tx_ng_id;
        init_tx_req_->txm_ = this;
        Execute(init_tx_req_.get());
    }
}

bool TransactionExecution::CommitTx(CommitTxRequest &commit_req)
{
    if (rw_set_.DataReadSetSize() == 0 && rw_set_.WriteSetSize() == 0 &&
        rw_set_.CatalogWriteSetSize() == 0 &&
        cmd_set_.ObjectCntWithWriteLock() == 0)
    {
        commit_tx_req_->Reset();
        commit_tx_req_->to_commit_ = commit_req.to_commit_;
        Execute(commit_tx_req_.get());
#ifdef EXT_TX_PROC_ENABLED
        Enlist();
#endif
        return true;
    }
    else
    {
        int err = Execute(&commit_req);
        if (err != 0)
        {
            commit_req.SetError(
                TxErrorCode::TX_REQUEST_TO_COMMITTED_ABORTED_TX);
            return false;
        }
        commit_req.Wait();
        bool success = commit_req.Result();
        return success;
    }
}

size_t TransactionExecution::OpenTxScan(ScanOpenTxRequest &scan_open_tx_req)
{
    scan_open_tx_req.scan_alias_ = scan_alias_cnt_;
    scan_alias_cnt_++;
    Execute(&scan_open_tx_req);
    return scan_open_tx_req.scan_alias_;
}

void TransactionExecution::CloseTxScan(uint64_t alias,
                                       const TableName &table_name,
                                       std::vector<UnlockTuple> &unlock_vec)
{
    ScanCloseTxRequest *scan_close_req = NextScanCloseTxReq(alias, table_name);
    assert(scan_close_req->unlock_batch_.empty());

    if (!unlock_vec.empty())
    {
        scan_close_req->unlock_batch_.swap(unlock_vec);
    }

    Execute(scan_close_req);
#ifdef EXT_TX_PROC_ENABLED
// Note that for scan open, we don't enlist the tx for execution, and only
// enlist for scan close. This is because scan open is always followed by
// scan next or scan close, which will enlist the tx and executes scan open.
// ExternalForward();
#endif
}

TxErrorCode TransactionExecution::TxUpsert(const TableName &table_name,
                                           uint64_t schema_version,
                                           TxKey key,
                                           TxRecord::Uptr rec,
                                           OperationType op,
                                           bool check_unique)
{
    if (table_name.Type() == TableType::Primary ||
        table_name.Type() == TableType::Secondary ||
        table_name.Type() == TableType::UniqueSecondary)
    {
        return rw_set_.AddWrite(table_name,
                                schema_version,
                                std::move(key),
                                std::move(rec),
                                op,
                                check_unique);
    }
    else
    {
        assert(table_name.Type() == TableType::Catalog &&
               op == OperationType::Update);
        rw_set_.AddCatalogWrite(std::move(key), std::move(rec));
        return TxErrorCode::NO_ERROR;
    }
}

void TransactionExecution::TxRevert(const TableName &table_name,
                                    const TxKey &key)
{
    rw_set_.DeleteWrite(table_name, key);
}

bool TransactionExecution::IsTimeOut(int wait_secs)
{
    ++state_forward_cnt_;
    uint32_t step = bind_to_ext_proc_ ? 1 : LoopCnt;
    if (state_forward_cnt_ == step)
    {
        state_forward_cnt_ = 0;
        uint64_t now_ts = LocalCcShards::ClockTs();
        using namespace std::chrono_literals;
        // TODO remove this hard code 10 seconds
        uint64_t duration =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::seconds(wait_secs))
                .count();
        if (now_ts - state_clock_ > duration)
        {
            // The local clock is advanced in roughly 2 seconds. So, if the
            // current time is greater than the prior one by at least 4
            // seconds(local clock advances at least two times), then we can
            // confirm the tx machine has been stuck in this state for at least
            // 2 seconds.
            //
            // local clock(s):      0          2          4
            //                |----------|----------|----------|
            //                          ^            ^
            // current time:          prior         now
            //
            state_clock_ = now_ts;
            return true;
        }
    }

    return false;
}

void TransactionExecution::StartTiming()
{
    state_forward_cnt_ = 0;
    state_clock_ = LocalCcShards::ClockTs();

#ifdef EXT_TX_PROC_ENABLED
    if (bind_to_ext_proc_)
    {
        tx_processor_->EnlistWaitingTx(this);
    }
#endif
}

void TransactionExecution::PushOperation(TransactionOperation *op,
                                         int retry_num)
{
    command_id_.fetch_add(1, std::memory_order_relaxed);
    state_stack_.push_back(op);
    op->retry_num_ = retry_num;
    op->is_running_ = false;
}

void TransactionExecution::ProcessTxRequest(InitTxRequest &init_txn_req)
{
    TX_TRACE_ACTION(this, &init_txn_req);
    // If the input request is the internal request of the tx state machine,
    // there is no external requester waiting for the response/result. Sets the
    // response pointer to nullptr.
    uint64_resp_ = &init_txn_req == init_tx_req_.get()
                       ? nullptr
                       : &init_txn_req.tx_result_;
    iso_level_ = init_txn_req.iso_level_;
    protocol_ = init_txn_req.protocol_;
    init_txn_.tx_ng_id_ = init_txn_req.tx_ng_id_ == UINT32_MAX
                              ? Sharder::Instance().NativeNodeGroup()
                              : init_txn_req.tx_ng_id_;

    init_txn_.log_group_id_ = init_txn_req.log_group_id_;
    PushOperation(&init_txn_);
    Process(init_txn_);
}

void TransactionExecution::ProcessTxRequest(ReadTxRequest &read_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    if (tx_term_ < 0)
    {
        read_req.SetError(TxErrorCode::TX_INIT_FAIL);
        return;
    }

    rtp_resp_ = &read_req.tx_result_;

    read_.read_type_ = ReadType::Inside;
    read_.read_tx_req_ = &read_req;
    read_.Reset();

    PushOperation(&read_);
    Process(read_);
}

void TransactionExecution::ProcessTxRequest(
    ReadOutsideTxRequest &read_outside_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read_outside_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    rec_resp_ = &read_outside_req.tx_result_;

    read_.read_type_ = read_outside_req.is_deleted_ ? ReadType::OutsideDeleted
                                                    : ReadType::OutsideNormal;
    read_.read_outside_tx_req_ = &read_outside_req;
    read_.Reset();
    PushOperation(&read_);
    Process(read_);
}

void TransactionExecution::ProcessTxRequest(ScanOpenTxRequest &scan_open_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_open_req,
        [&]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name:\":")
                .append(scan_open_req.tab_name_->String());
        });

    if (scan_open_req.scan_alias_ == UINT64_MAX)
    {
        scan_open_req.scan_alias_ = scan_alias_cnt_;
        ++scan_alias_cnt_;
    }
    uint64_resp_ = &scan_open_req.tx_result_;

    scan_open_.Reset();
    scan_open_.Set(scan_open_req.tab_name_,
                   scan_open_req.indx_type_,
                   scan_open_req.StartKey(),
                   scan_open_req.start_inclusive_,
                   scan_open_req.direct_,
                   scan_open_req.is_ckpt_delta_);
    scan_open_.tx_req_ = &scan_open_req;
    PushOperation(&scan_open_);
    Process(scan_open_);
}

void TransactionExecution::ProcessTxRequest(ScanBatchTxRequest &scan_batch_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_batch_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    bool_resp_ = &scan_batch_req.tx_result_;

    scan_next_.Reset();
    scan_next_.tx_req_ = &scan_batch_req;
    scan_next_.alias_ = scan_batch_req.alias_;
    if (!scan_batch_req.table_name_.IsHashPartitioned())
    {
        scan_next_.range_table_name_ =
            TableName(scan_batch_req.table_name_.StringView(),
                      TableType::RangePartition,
                      scan_batch_req.table_name_.Engine());
    }
    PushOperation(&scan_next_);
    Process(scan_next_);
}

void TransactionExecution::ProcessTxRequest(ScanCloseTxRequest &scan_close_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_close_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    void_resp_ = nullptr;
    ScanClose(scan_close_req.unlock_batch_,
              scan_close_req.alias_,
              scan_close_req.table_name_);

    scan_close_req.unlock_batch_.clear();
    scan_close_req.in_use_.store(false, std::memory_order_relaxed);
    scan_close_req.tx_result_.Finish(void_);
}

void TransactionExecution::ProcessTxRequest(UpsertTxRequest &upsert_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &upsert_req,
        [&]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name:\":")
                .append(upsert_req.tab_name_->String());
        });
    void_resp_ = &upsert_req.tx_result_;
    Upsert(*upsert_req.tab_name_,
           0,
           std::move(upsert_req.tx_key_),
           std::move(upsert_req.rec_),
           upsert_req.operation_type_);
}

void TransactionExecution::ProcessTxRequest(CommitTxRequest &commit_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &commit_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    bool_resp_ = &commit_req.tx_result_;
    if (commit_req.to_commit_)
    {
        Commit();
    }
    else
    {
        // When the tx is aborted/rolled back by the user, write locks must have
        // not acquired. Clear the write set before entering post-processing.
        rw_set_.ClearWriteSet();
        rw_set_.ClearCatalogWriteSet();
        Abort();
    }
}

void TransactionExecution::ProcessTxRequest(AbortTxRequest &abort_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &abort_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    bool_resp_ = &abort_req.tx_result_;
    // When the tx is aborted/rolled back by the user, write locks must have not
    // acquired. Clear the write set before entering post-processing.
    rw_set_.ClearWriteSet();
    rw_set_.ClearCatalogWriteSet();
    Abort();
}

void TransactionExecution::ProcessTxRequest(UpsertTableTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &req,
        [&]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name\":")
                .append(req.table_name_->String());
        });
    upsert_resp_ = &req.tx_result_;

    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    if (req.op_type_ == OperationType::CreateTable ||
        req.op_type_ == OperationType::Update ||
        req.op_type_ == OperationType::DropTable ||
        req.op_type_ == OperationType::TruncateTable)
    {
        std::unique_lock<std::mutex> lk(
            local_shards->table_schema_op_pool_mux_);
        if (local_shards->table_schema_op_pool_.empty())
        {
            std::unique_ptr<UpsertTableOp> table_op = nullptr;
            table_op =
                std::make_unique<UpsertTableOp>(req.table_name_->StringView(),
                                                req.table_name_->Engine(),
                                                *req.curr_image_,
                                                req.curr_schema_ts_,
                                                *req.dirty_image_,
                                                req.op_type_,
                                                this);
            schema_op_ = std::move(table_op);
        }
        else
        {
            assert(local_shards->table_schema_op_pool_.back() != nullptr);
            schema_op_ = std::move(local_shards->table_schema_op_pool_.back());
            local_shards->table_schema_op_pool_.pop_back();

            schema_op_->Reset(req.table_name_->StringView(),
                              req.table_name_->Engine(),
                              *req.curr_image_,
                              req.curr_schema_ts_,
                              *req.dirty_image_,
                              req.op_type_,
                              this);
        }
        lk.unlock();

        PushOperation(schema_op_.get());
    }
    else if (req.op_type_ == OperationType::AddIndex ||
             req.op_type_ == OperationType::DropIndex)
    {
        std::unique_lock<std::mutex> lk(local_shards->table_index_op_pool_mux_);
        if (local_shards->table_index_op_pool_.empty())
        {
            std::unique_ptr<UpsertTableIndexOp> index_op =
                std::make_unique<UpsertTableIndexOp>(
                    req.table_name_->StringView(),
                    req.table_name_->Engine(),
                    *req.curr_image_,
                    req.curr_schema_ts_,
                    *req.dirty_image_,
                    *req.alter_table_info_image_,
                    req.op_type_,
                    req.pack_sk_err_.get(),
                    this);

            index_op_ = std::move(index_op);
        }
        else
        {
            assert(local_shards->table_index_op_pool_.back() != nullptr);
            index_op_ = std::move(local_shards->table_index_op_pool_.back());
            local_shards->table_index_op_pool_.pop_back();

            index_op_->Reset(req.table_name_->StringView(),
                             req.table_name_->Engine(),
                             *req.curr_image_,
                             req.curr_schema_ts_,
                             *req.dirty_image_,
                             *req.alter_table_info_image_,
                             req.op_type_,
                             req.pack_sk_err_.get(),
                             this);
        }
        lk.unlock();

        PushOperation(index_op_.get());
    }
    else
    {
        // Currently, no implementation for other table schema operation, such
        // as add/drop columns.
        assert(false);
    }
}

void TransactionExecution::ProcessTxRequest(ObjectCommandTxRequest &req)
{
    rec_resp_ = &req.tx_result_;
    TxCommand *command = req.Command();
    const TxKey *key = req.Key();
    obj_cmd_.Reset(
        req.table_name_, key, command, req.auto_commit_, req.always_redirect_);

    PushOperation(&obj_cmd_);
    Process(obj_cmd_);
}

void TransactionExecution::ProcessTxRequest(MultiObjectCommandTxRequest &req)
{
    vct_rec_resp_ = &req.tx_result_;
    multi_obj_cmd_.Reset(&req);

    PushOperation(&multi_obj_cmd_);
    Process(multi_obj_cmd_);
}

void TransactionExecution::ProcessTxRequest(PublishTxRequest &req)
{
    auto all_node_groups = Sharder::Instance().AllNodeGroups();

    for (uint32_t ng_id : *all_node_groups)
    {
        cc_handler_->PublishMessage(ng_id, tx_term_, req.chan_, req.message_);
    }

    req.tx_result_.Finish(Void{});
}

void TransactionExecution::ProcessTxRequest(ReloadCacheTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &fi_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    void_resp_ = &req.tx_result_;

    uint32_t hres_ref_cnt = Sharder::Instance().NodeGroupCount();
    reload_cache_op_.Reset(hres_ref_cnt);

    PushOperation(&reload_cache_op_);
    Process(reload_cache_op_);
}

void TransactionExecution::ProcessTxRequest(FaultInjectTxRequest &fi_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &fi_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    bool_resp_ = &fi_req.tx_result_;

    fault_inject_op_.Set(
        fi_req.fault_name_, fi_req.fault_paras_, fi_req.vct_node_id_);
    PushOperation(&fault_inject_op_);
    Process(fault_inject_op_);
}

void TransactionExecution::ProcessTxRequest(InvalidateTableCacheTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    void_resp_ = &req.tx_result_;
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();

    std::unique_lock<std::mutex> lk(
        local_shards->invalidate_table_cache_op_mux_);
    if (local_shards->invalidate_table_cache_op_pool_.empty())
    {
        invalidate_table_cache_composite_op_ =
            std::make_unique<InvalidateTableCacheCompositeOp>(req.table_name_,
                                                              this);
    }
    else
    {
        invalidate_table_cache_composite_op_ =
            std::move(local_shards->invalidate_table_cache_op_pool_.back());
        local_shards->invalidate_table_cache_op_pool_.pop_back();
        invalidate_table_cache_composite_op_->Reset(req.table_name_, this);
    }
    lk.unlock();

    PushOperation(invalidate_table_cache_composite_op_.get());
}

void TransactionExecution::ProcessTxRequest(SplitFlushTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    bool_resp_ = &req.tx_result_;

    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    std::unique_lock<std::mutex> lk(
        local_shards->split_flush_range_op_pool_mux_);
    if (local_shards->split_flush_range_op_pool_.empty())
    {
        split_flush_op_ =
            std::make_unique<SplitFlushRangeOp>(*req.table_name_,
                                                std::move(req.schema_),
                                                req.range_entry_,
                                                std::move(req.new_range_info_),
                                                this,
                                                req.is_dirty_);
    }
    else
    {
        split_flush_op_ =
            std::move(local_shards->split_flush_range_op_pool_.back());
        local_shards->split_flush_range_op_pool_.pop_back();
        assert(split_flush_op_ != nullptr);
        split_flush_op_->Reset(*req.table_name_,
                               std::move(req.schema_),
                               req.range_entry_,
                               std::move(req.new_range_info_),
                               this,
                               req.is_dirty_);
    }
    lk.unlock();

    PushOperation(split_flush_op_.get());
}

void TransactionExecution::ProcessTxRequest(
    DataMigrationTxRequest &data_migration_req)
{
    void_resp_ = &data_migration_req.tx_result_;
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();

    std::lock_guard<std::mutex> lk(local_shards->data_migration_op_pool_mux_);

    if (local_shards->migration_op_pool_.empty())
    {
        migration_op_ =
            std::make_unique<DataMigrationOp>(this, data_migration_req.status_);
    }
    else
    {
        migration_op_ = std::move(local_shards->migration_op_pool_.back());
        local_shards->migration_op_pool_.pop_back();
        migration_op_->Reset(this, data_migration_req.status_);
    }

    if (data_migration_req.status_->next_bucket_idx_ != 0)
    {
        // If this is not the first worker, we can destruct the
        // tx req after migrate status is passed into migration op.
        void_resp_->Finish(void_);
    }

    PushOperation(migration_op_.get());
    Forward();
}

void TransactionExecution::ProcessTxRequest(AnalyzeTableTxRequest &analyze_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &analyze_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name\":")
                .append(analyze_req.table_name_->String());
        });

    void_resp_ = &analyze_req.tx_result_;
    analyze_table_all_op_.analyze_tx_req_ = &analyze_req;

    uint32_t hres_ref_cnt = Sharder::Instance().NodeGroupCount();
    analyze_table_all_op_.Reset(hres_ref_cnt);

    PushOperation(&analyze_table_all_op_);
    Process(analyze_table_all_op_);
}

void TransactionExecution::ProcessTxRequest(
    BroadcastStatisticsTxRequest &broadcast_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &broadcast_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name\":")
                .append(broadcast_req.table_name_->String());
        });

    void_resp_ = &broadcast_req.tx_result_;
    broadcast_stat_op_.broadcast_tx_req_ = &broadcast_req;

    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    assert(ng_cnt > 0);
    uint32_t hres_ref_cnt = ng_cnt - 1;
    broadcast_stat_op_.Reset(hres_ref_cnt);

    PushOperation(&broadcast_stat_op_);
    Process(broadcast_stat_op_);
}

void TransactionExecution::ProcessTxRequest(ClusterScaleTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    string_resp_ = &req.tx_result_;
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> new_ng_config;
    if (req.scale_type_ == ClusterScaleOpType::AddNodeGroup)
    {
        new_ng_config =
            Sharder::Instance().AddNodeGroupToCluster(*req.delta_nodes_);
    }
    else if (req.scale_type_ == ClusterScaleOpType::RemoveNode)
    {
        assert(req.delta_nodes_ != nullptr && !req.delta_nodes_->empty());
        new_ng_config =
            Sharder::Instance().RemoveNodeFromCluster(*req.delta_nodes_);
    }
    else if (req.scale_type_ == ClusterScaleOpType::AddNodeGroupPeers)
    {
        new_ng_config = Sharder::Instance().AddNodeGroupPeersToCluster(
            req.ng_id_, *req.delta_nodes_, *req.is_candidate_);
    }
    else
    {
        assert(false);
    }

    if (new_ng_config.empty())
    {
        // The cluster scale request is invalid, for example, the new added
        // node is already in the cluster.
        req.SetError(TxErrorCode::INVALID_CLUSTER_SCALE_REQUEST);
        return;
    }

    std::unique_lock<std::mutex> lk(local_shards->cluster_scale_op_mux_);
    if (local_shards->cluster_scale_op_pool_.empty())
    {
        cluster_scale_op_ = std::make_unique<ClusterScaleOp>(
            req.id_, req.scale_type_, std::move(new_ng_config), this);
    }
    else
    {
        cluster_scale_op_ =
            std::move(local_shards->cluster_scale_op_pool_.back());
        local_shards->cluster_scale_op_pool_.pop_back();
        cluster_scale_op_->Reset(
            req.id_, req.scale_type_, std::move(new_ng_config), this);
    }
    lk.unlock();

    PushOperation(cluster_scale_op_.get());
}

void TransactionExecution::ProcessTxRequest(
    SchemaRecoveryTxRequest &recover_req)
{
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
    upsert_resp_ = &recover_req.tx_result_;
    auto &schema_op_msg = recover_req.schema_op_msg_;
    switch (schema_op_msg.schema_op_case())
    {
    case ::txlog::SchemaOpMessage::kTableOp:
    {
        const ::txlog::UpsertTableMessage &table_msg = schema_op_msg.table_op();
        OperationType operation_type =
            static_cast<OperationType>(table_msg.op_type());

        LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
        switch (operation_type)
        {
        case OperationType::CreateTable:
        case OperationType::DropTable:
        case OperationType::TruncateTable:
        case OperationType::Update:
        {
            std::unique_lock<std::mutex> lk(
                local_shards->table_schema_op_pool_mux_);
            if (Sharder::Instance()
                    .GetLocalCcShards()
                    ->table_schema_op_pool_.empty())
            {
                TableEngine table_engine =
                    ::txlog::ToLocalType::ConvertTableEngine(
                        schema_op_msg.table_engine());
                std::unique_ptr<UpsertTableOp> table_op = nullptr;
                table_op = std::make_unique<UpsertTableOp>(
                    schema_op_msg.table_name_str(),
                    table_engine,
                    schema_op_msg.old_catalog_blob(),
                    schema_op_msg.catalog_ts(),
                    schema_op_msg.new_catalog_blob(),
                    operation_type,
                    this);
                schema_op_ = std::move(table_op);
            }
            else
            {
                assert(Sharder::Instance()
                           .GetLocalCcShards()
                           ->table_schema_op_pool_.back() != nullptr);
                schema_op_ = std::move(Sharder::Instance()
                                           .GetLocalCcShards()
                                           ->table_schema_op_pool_.back());
                Sharder::Instance()
                    .GetLocalCcShards()
                    ->table_schema_op_pool_.pop_back();

                TableEngine table_engine =
                    ::txlog::ToLocalType::ConvertTableEngine(
                        schema_op_msg.table_engine());
                schema_op_->Reset(schema_op_msg.table_name_str(),
                                  table_engine,
                                  schema_op_msg.old_catalog_blob(),
                                  schema_op_msg.catalog_ts(),
                                  schema_op_msg.new_catalog_blob(),
                                  operation_type,
                                  this);
            }
            lk.unlock();

            if (schema_op_msg.stage() ==
                ::txlog::SchemaOpMessage::Stage::
                    SchemaOpMessage_Stage_PrepareSchema)
            {
                schema_op_->prepare_log_op_.hd_result_.SetFinished();
                schema_op_->op_ = &schema_op_->prepare_log_op_;
            }
            else
            {
                assert(schema_op_msg.stage() ==
                       ::txlog::SchemaOpMessage::Stage::
                           SchemaOpMessage_Stage_CommitSchema);

                // extract table schema and dirty schema from schema_op_msg
                TableType table_type = ::txlog::ToLocalType::ConvertCcTableType(
                    schema_op_msg.table_type());
                TableEngine table_engine =
                    ::txlog::ToLocalType::ConvertTableEngine(
                        schema_op_msg.table_engine());
                std::string_view table_name_sv{schema_op_msg.table_name_str()};
                TableName table_name{table_name_sv, table_type, table_engine};
                uint64_t schema_ts = schema_op_msg.catalog_ts();
                std::shared_ptr<TableSchema> schema_ptr =
                    Sharder::Instance()
                        .GetLocalCcShards()
                        ->CreateTableSchemaFromImage(
                            table_name,
                            schema_op_msg.old_catalog_blob(),
                            schema_ts);
                std::shared_ptr<TableSchema> dirty_schema_ptr =
                    Sharder::Instance()
                        .GetLocalCcShards()
                        ->CreateTableSchemaFromImage(
                            table_name,
                            schema_op_msg.new_catalog_blob(),
                            commit_ts_);

                schema_op_->catalog_rec_.Set(
                    schema_ptr, dirty_schema_ptr, schema_ts);
                schema_op_->commit_log_op_.hd_result_.SetFinished();
                schema_op_->op_ = &schema_op_->commit_log_op_;
            }

            PushOperation(schema_op_.get());
            break;
        }
        case OperationType::AddIndex:
        case OperationType::DropIndex:
        {
            std::unique_lock<std::mutex> lk(
                local_shards->table_index_op_pool_mux_);

            if (local_shards->table_index_op_pool_.empty())
            {
                TableEngine table_engine =
                    ::txlog::ToLocalType::ConvertTableEngine(
                        schema_op_msg.table_engine());
                std::unique_ptr<UpsertTableIndexOp> index_op =
                    std::make_unique<UpsertTableIndexOp>(
                        schema_op_msg.table_name_str(),
                        table_engine,
                        schema_op_msg.old_catalog_blob(),
                        schema_op_msg.catalog_ts(),
                        schema_op_msg.new_catalog_blob(),
                        schema_op_msg.alter_table_info_blob(),
                        operation_type,
                        nullptr,
                        this);

                index_op_ = std::move(index_op);
            }
            else
            {
                assert(local_shards->table_index_op_pool_.back() != nullptr);
                index_op_ =
                    std::move(local_shards->table_index_op_pool_.back());
                local_shards->table_index_op_pool_.pop_back();
                TableEngine table_engine =
                    ::txlog::ToLocalType::ConvertTableEngine(
                        schema_op_msg.table_engine());

                index_op_->Reset(schema_op_msg.table_name_str(),
                                 table_engine,
                                 schema_op_msg.old_catalog_blob(),
                                 schema_op_msg.catalog_ts(),
                                 schema_op_msg.new_catalog_blob(),
                                 schema_op_msg.alter_table_info_blob(),
                                 operation_type,
                                 nullptr,
                                 this);
            }
            lk.unlock();

            if (schema_op_msg.stage() ==
                ::txlog::SchemaOpMessage::Stage::
                    SchemaOpMessage_Stage_PrepareSchema)
            {
                index_op_->prepare_log_op_.hd_result_.SetFinished();
                index_op_->op_ = &index_op_->prepare_log_op_;
            }
            else if (schema_op_msg.stage() ==
                     ::txlog::SchemaOpMessage::Stage::
                         SchemaOpMessage_Stage_PrepareData)
            {
                if (schema_op_msg.last_key_type() ==
                    ::txlog::SchemaOpMessage::LastKeyType::
                        SchemaOpMessage_LastKeyType_PosInfKey)
                {
                    TableEngine table_engine =
                        ::txlog::ToLocalType::ConvertTableEngine(
                            schema_op_msg.table_engine());
                    // The positive inf key
                    index_op_->last_finished_end_key_ =
                        Sharder::Instance()
                            .GetLocalCcShards()
                            ->GetCatalogFactory(table_engine)
                            ->PositiveInfKey();
                    index_op_->is_last_finished_key_str_ = false;
                }
                else
                {
                    index_op_->last_finished_end_key_str_ =
                        &schema_op_msg.last_key_value();
                    index_op_->is_last_finished_key_str_ = true;
                }

                // - Recovery catalog_rec_ to rebuild dirty schema image.
                // - Recovery catalog_rec_ to update image inside kv-storage.
                TableType table_type = ::txlog::ToLocalType::ConvertCcTableType(
                    schema_op_msg.table_type());
                TableEngine table_engine =
                    ::txlog::ToLocalType::ConvertTableEngine(
                        schema_op_msg.table_engine());
                std::string_view table_name_sv{schema_op_msg.table_name_str()};
                TableName table_name{table_name_sv, table_type, table_engine};
                CatalogEntry *catalog_entry =
                    local_shards->GetCatalog(table_name, TxCcNodeId());
                index_op_->catalog_rec_.Set(catalog_entry->schema_,
                                            catalog_entry->dirty_schema_,
                                            catalog_entry->schema_version_);
                index_op_->catalog_rec_.SetSchemaImage(
                    catalog_entry->schema_->SchemaImage());
                index_op_->catalog_rec_.SetDirtySchemaImage(
                    catalog_entry->dirty_schema_->SchemaImage());

                // Recovery indexes_multikey_attr_.
                index_op_->RecoveryIndexesMultiKeyAttr(
                    catalog_entry->dirty_schema_.get());

                index_op_->op_ = &index_op_->prepare_data_log_op_;
                index_op_->prepare_data_log_op_.hd_result_.SetFinished();
            }
            else
            {
                assert(schema_op_msg.stage() ==
                       ::txlog::SchemaOpMessage::Stage::
                           SchemaOpMessage_Stage_CommitSchema);

                // - Recovery catalog_rec_ to rollback kv-storage
                // - Recovery catalog_rec_ to drop index from kv-storage.
                // - Recovery catalog_rec_ to update image inside kv-storage.
                TableType table_type = ::txlog::ToLocalType::ConvertCcTableType(
                    schema_op_msg.table_type());
                TableEngine table_engine =
                    ::txlog::ToLocalType::ConvertTableEngine(
                        schema_op_msg.table_engine());
                std::string_view table_name_sv{schema_op_msg.table_name_str()};
                TableName table_name{table_name_sv, table_type, table_engine};
                uint64_t schema_ts = schema_op_msg.catalog_ts();
                std::shared_ptr<TableSchema> schema_ptr =
                    Sharder::Instance()
                        .GetLocalCcShards()
                        ->CreateTableSchemaFromImage(
                            table_name,
                            schema_op_msg.old_catalog_blob(),
                            schema_ts);
                std::shared_ptr<TableSchema> dirty_schema_ptr =
                    Sharder::Instance()
                        .GetLocalCcShards()
                        ->CreateTableSchemaFromImage(
                            table_name,
                            schema_op_msg.new_catalog_blob(),
                            commit_ts_);

                index_op_->catalog_rec_.Set(
                    schema_ptr, dirty_schema_ptr, schema_ts);
                index_op_->catalog_rec_.SetSchemaImage(
                    schema_ptr->SchemaImage());
                index_op_->catalog_rec_.SetDirtySchemaImage(
                    dirty_schema_ptr->SchemaImage());

                // Recovery indexes_multikey_attr_.
                index_op_->RecoveryIndexesMultiKeyAttr(dirty_schema_ptr.get());

                index_op_->commit_log_op_.hd_result_.SetFinished();
                index_op_->op_ = &index_op_->commit_log_op_;
            }

            PushOperation(index_op_.get());
            break;
        }
        default:
            assert(false);
            break;
        }

        break;
    }
    default:
        tx_status_.store(TxnStatus::Finished, std::memory_order_relaxed);
        break;
    }
}

void TransactionExecution::ProcessTxRequest(
    RangeSplitRecoveryTxRequest &recover_req)
{
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
    bool_resp_ = &recover_req.tx_result_;

    const TableName range_table_name =
        TableName{recover_req.ds_split_range_op_msg_.table_name(),
                  TableType::RangePartition,
                  ::txlog::ToLocalType::ConvertTableEngine(
                      recover_req.ds_split_range_op_msg_.table_engine())};
    const TableName table_name =
        TableName{range_table_name.StringView(),
                  TableName::Type(range_table_name.StringView()),
                  range_table_name.Engine()};

    std::vector<std::pair<TxKey, int32_t>> new_range_info;
    for (size_t i = 0; i < recover_req.new_range_keys_.size(); i++)
    {
        new_range_info.emplace_back(std::move(recover_req.new_range_keys_[i]),
                                    recover_req.new_partition_ids_[i]);
    }

    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    std::unique_ptr<SplitFlushRangeOp> split_range_op = nullptr;
    std::unique_lock<std::mutex> lk(
        local_shards->split_flush_range_op_pool_mux_);
    if (local_shards->split_flush_range_op_pool_.empty())
    {
        split_range_op = std::make_unique<SplitFlushRangeOp>(
            table_name,
            std::move(recover_req.table_schema_),
            recover_req.range_entry_,
            std::move(new_range_info),
            this,
            recover_req.is_dirty_);
    }
    else
    {
        split_range_op =
            std::move(local_shards->split_flush_range_op_pool_.back());
        local_shards->split_flush_range_op_pool_.pop_back();
        assert(split_range_op != nullptr);
        split_range_op->Reset(table_name,
                              std::move(recover_req.table_schema_),
                              recover_req.range_entry_,
                              std::move(new_range_info),
                              this,
                              recover_req.is_dirty_);
    }
    lk.unlock();
    assert(split_range_op != nullptr);

    const ::txlog::SplitRangeOpMessage::Stage stage =
        recover_req.ds_split_range_op_msg_.stage();

    if (stage == ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
    {
        split_range_op->prepare_log_op_.hd_result_.SetFinished();
        split_range_op->op_ = &split_range_op->prepare_log_op_;
    }
    else
    {
        split_range_op->commit_log_op_.hd_result_.SetFinished();
        split_range_op->op_ = &split_range_op->commit_log_op_;
    }

    LOG(INFO) << "Recovering split flush tx " << TxNumber() << " on table "
              << table_name.StringView() << ", range id "
              << recover_req.range_entry_->GetRangeInfo()->PartitionId();
    split_flush_op_ = std::move(split_range_op);
    PushOperation(split_flush_op_.get());
}

void TransactionExecution::Process(InitTxnOperation &init_txn)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &init_txn,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    is_collecting_duration_round_ = false;
    if (metrics::enable_tx_metrics)
    {
        is_collecting_duration_round_ =
            tx_processor_->CheckAndUpdateTxCurrentRound();
    }

    if (metrics::enable_tx_metrics && is_collecting_duration_round_)
    {
        tx_duration_start_ = metrics::Clock::now();
    }

    init_txn.is_running_ = true;
    commit_ts_ = 0;
    commit_ts_bound_ = 0;

    cc_handler_->NewTxn(init_txn.hd_result_,
                        iso_level_,
                        init_txn.tx_ng_id_,
                        init_txn.log_group_id_);
}

void TransactionExecution::PostProcess(InitTxnOperation &init_txn)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &init_txn,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    if (init_txn.hd_result_.IsError())
    {
        DLOG(ERROR) << "InitTxnOperation failed for cc error:"
                    << init_txn.hd_result_.ErrorMsg() << ", tx owner "
                    << init_txn.tx_ng_id_;
        state_stack_.clear();

        if (uint64_resp_ != nullptr)
        {
            uint64_resp_->FinishError(TxErrorCode::TX_INIT_FAIL);
            // transaction can be recycled and put into free list.
            tx_status_.store(TxnStatus::Finished, std::memory_order_release);
            Reset();
            uint64_resp_ = nullptr;
        }
        else
        {
            TxRequest *req = nullptr;
            while (TxRequestCount() > 0)
            {
                req = DequeueTxRequest();
                req->SetError(TxErrorCode::TX_INIT_FAIL);
            }
        }

        init_txn.Reset();
        return;
    }

    const InitTxResult &init_result = init_txn.hd_result_.Value();
    txid_ = init_result.txid_;
    uint64_t tx_number = txid_.TxNumber();
    tx_number_.store(tx_number, std::memory_order_release);
    start_ts_ = init_result.start_ts_;
    commit_ts_bound_ = init_result.start_ts_ + 1;
    tx_term_ = init_result.term_;
    state_stack_.pop_back();

    if (uint64_resp_ != nullptr)
    {
        uint64_resp_->Finish(tx_number);
        uint64_resp_ = nullptr;
    }
    init_txn.Reset();
}

/**
 * @brief Process the ReadOperation and generate ReadCcRequest using handler.
 *
 * @param read read is a reference to TransactionExecution::read_
 */
void TransactionExecution::Process(ReadOperation &read)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    read.is_running_ = true;
    if (read.read_type_ == ReadType::Inside)
    {
        const TableName &table_name = *read.read_tx_req_->tab_name_;
        TxRecord &rec = *read.read_tx_req_->rec_;
        const uint64_t ts = read.read_tx_req_->ts_;
        bool is_covering_keys = read.read_tx_req_->is_covering_keys_;

        // Reads the specified key from the local cc map to which this tx is
        // bound. This API is used for reading cc maps replicated in all shards.
        // A typical use case of ReadLocal is to read and start concurrency
        // control of a table catalog.
        if (read.read_tx_req_->read_local_)
        {
            // So far ReadLocal() is use exclusively for reading catalogs.
            // Reading catalogs needs to put read locks, regardless of the tx's
            // concurrency control protocol. So for now, a read's isolation
            // level and cc protocol is fixed.
            if (iso_level_ < IsolationLevel::RepeatableRead)
            {
                read.iso_level_ = IsolationLevel::RepeatableRead;
            }
            else
            {
                read.iso_level_ = iso_level_;
            }
            read.protocol_ = CcProtocol::Locking;

            bool finished = true;
            if (!read.read_tx_req_->is_str_key_)
            {
                finished = cc_handler_->ReadLocal(
                    table_name,
                    *read.read_tx_req_->key_,
                    rec,
                    read.read_type_,
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    command_id_.load(std::memory_order_relaxed),
                    start_ts_,
                    read.hd_result_,
                    read.iso_level_,
                    read.protocol_,
                    read.read_tx_req_->is_for_write_,
                    read.read_tx_req_->is_recovering_);
            }
            else
            {
                finished = cc_handler_->ReadLocal(
                    table_name,
                    *read.read_tx_req_->key_str_,
                    rec,
                    read.read_type_,
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    command_id_.load(std::memory_order_relaxed),
                    start_ts_,
                    read.hd_result_,
                    read.iso_level_,
                    read.protocol_,
                    read.read_tx_req_->is_for_write_,
                    read.read_tx_req_->is_recovering_);
            }

            if (finished)
            {
                command_id_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        else
        {
            assert(!read.read_tx_req_->is_str_key_);
            const TxKey &key = *read.read_tx_req_->key_;
            if (!read.local_cache_miss_)
            {
                // Step 1: fast path if key is update by the same tx.
                const WriteSetEntry *write = rw_set_.FindWrite(table_name, key);
                if (write != nullptr)
                {
                    if (write->op_ == OperationType::Delete)
                    {
                        state_stack_.pop_back();
                        assert(state_stack_.empty());
                        rtp_resp_->Finish(std::pair<RecordStatus, uint64_t>(
                            RecordStatus::Deleted, 0));
                        rtp_resp_ = nullptr;
                    }
                    else
                    {
                        rec.Copy(*write->rec_.get());
                        state_stack_.pop_back();
                        assert(state_stack_.empty());
                        rtp_resp_->Finish(std::pair<RecordStatus, uint64_t>(
                            RecordStatus::Normal, 0));
                        rtp_resp_ = nullptr;
                    }
                    return;
                }

                // Step 2: fast path if key is the same as last read key.
            }

            read.local_cache_miss_ = true;
            read.protocol_ = protocol_;
            read.iso_level_ = iso_level_;

            uint32_t key_shard_code = 0;
            int32_t partition_id = -1;
            // Find the key shard code and lock meta data.
            if (!table_name.IsHashPartitioned())
            {
                if (!lock_range_bucket_result_.IsFinished())
                {
                    read.is_running_ = false;
                    // First read and lock the range the key located in through
                    // lock_range_op_.
                    lock_range_bucket_result_.Value().Reset();
                    lock_range_bucket_result_.Reset();

                    lock_range_op_.Reset(TableName(table_name.StringView(),
                                                   TableType::RangePartition,
                                                   table_name.Engine()),
                                         &key,
                                         &range_rec_,
                                         &lock_range_bucket_result_);

                    // Control flow jumps to lock_range_op_, do not execute
                    // further after `Process(lock_range_op_)` returns.
                    PushOperation(&lock_range_op_);
                    Process(lock_range_op_);
                    return;
                }
                else  // lock range finished and succeeded
                {
                    // If there is an error when getting the key's range ID, the
                    // error would be caught when forwarding the read operation,
                    // which forces the tx state machine moves to
                    // post-processing of the read operation and returns an
                    // error to the tx read request.
                    assert(!lock_range_bucket_result_.IsError());

                    // Uses the lower 10 bits of the key's hash code to shard
                    // the key across CPU cores in a cc node.
                    uint32_t residual = key.Hash() & 0x3FF;
                    NodeGroupId range_ng =
                        range_rec_.GetRangeOwnerNg()->BucketOwner();
                    key_shard_code = range_ng << 10 | residual;
                    partition_id = range_rec_.GetRangeInfo()->PartitionId();
                }
            }
            else
            {
                // Make sure current node is still ng leader since we may visit
                // bucket info meta data here which is only valid when current
                // node is still ng leader.
                if (!CheckLeaderTerm() && !CheckStandbyTerm())
                {
                    read.hd_result_.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
                    PostProcess(read);
                    return;
                }

                // Find bucket info from cache for get key node group.
                uint64_t key_hash = key.Hash();
                uint16_t bucket_id = Sharder::MapKeyHashToBucketId(key_hash);
                const BucketInfo *bucket_info = FastToGetBucket(bucket_id);
                partition_id = Sharder::MapKeyHashToHashPartitionId(key_hash);
                if (bucket_info != nullptr)
                {
                    // Uses the lower 10 bits of the key's hash code to shard
                    // the key across CPU cores in a cc node.
                    uint32_t residual = key_hash & 0x3FF;
                    NodeGroupId bucket_ng = bucket_info->BucketOwner();
                    key_shard_code = bucket_ng << 10 | residual;
                }
                else if (!lock_range_bucket_result_.IsFinished())
                {
                    read.is_running_ = false;
                    // First read and lock the bucket the key located in through
                    // lock_bucket_op_.
                    lock_range_bucket_result_.Value().Reset();
                    lock_range_bucket_result_.Reset();
                    bucket_key_.Reset(bucket_id);
                    // "bucket_tx_key_" has been set point to bucket_key_
                    lock_bucket_op_.Reset(
                        TableName(range_bucket_ccm_name_sv.data(),
                                  range_bucket_ccm_name_sv.size(),
                                  TableType::RangeBucket,
                                  TableEngine::None),
                        &bucket_tx_key_,
                        &bucket_rec_,
                        &lock_range_bucket_result_);

                    // Control flow jumps to lock_bucket_op_, do not execute
                    // further after `Process(lock_bucket_op_)` returns.
                    PushOperation(&lock_bucket_op_);
                    Process(lock_bucket_op_);
                    return;
                }
                else  // lock bucket finished and succeeded
                {
                    // If there is an error when getting the key's bucket info,
                    // the error would be caught when forwarding the read
                    // operation, which forces the tx state machine moves to
                    // post-processing of the read operation and returns an
                    // error to the tx read request.
                    assert(!lock_range_bucket_result_.IsError());

                    const BucketInfo *bucket_info = bucket_rec_.GetBucketInfo();
                    // Cache the locked bucket info for read directly at next.
                    uint16_t bucket_id = bucket_key_.BucketId();
                    locked_buckets_.emplace(bucket_id, bucket_info);

                    // Uses the lower 10 bits of the key's hash code to shard
                    // the key across CPU cores in a cc node.
                    uint32_t residual = key.Hash() & 0x3FF;
                    NodeGroupId bucket_ng = bucket_info->BucketOwner();
                    key_shard_code = bucket_ng << 10 | residual;
                }
            }

            // Step 3: do read.
            read.protocol_ = protocol_;
            read.iso_level_ = iso_level_;
            if (read.read_tx_req_->is_for_share_ &&
                iso_level_ < IsolationLevel::RepeatableRead)
            {
                read.iso_level_ = IsolationLevel::RepeatableRead;
            }
            uint64_t read_ts = 0;
            if (read.iso_level_ == IsolationLevel::Snapshot)
            {
                read_ts = start_ts_;
            }
            else if (ts != 0)
            {
                read_ts = ts;
            }

            cc_handler_->Read(table_name,
                              read.read_tx_req_->schema_version_,
                              key,
                              key_shard_code,
                              rec,
                              read.read_type_,
                              tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              command_id_.load(std::memory_order_relaxed),
                              read_ts,
                              read.hd_result_,
                              read.iso_level_,
                              read.protocol_,
                              read.read_tx_req_->is_for_write_,
                              is_covering_keys,
                              read.read_tx_req_->point_read_on_cache_miss_,
                              partition_id);

            if (!read.hd_result_.Value().is_local_)
            {
                if (metrics::enable_remote_request_metrics)
                {
                    auto meter = tx_processor_->GetMeter();
                    meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                                   metrics::Value::IncDecValue::Increment,
                                   "read");
                    if (is_collecting_duration_round_)
                    {
                        read.op_start_ = metrics::Clock::now();
                    }
                }

                StartTiming();
            }
            else
            {
                // If the read is local and puts no lock, the operation returns
                // instantly and will not timeout.
                LockType lk_type =
                    DeduceReadLockType(table_name.Type(),
                                       read.read_tx_req_->is_for_write_,
                                       read.iso_level_,
                                       is_covering_keys);

                if (lk_type != LockType::NoLock)
                {
                    StartTiming();
                }
            }
        }
    }
    else
    {
        TxRecord &record = read.read_outside_tx_req_->rec_;
        bool is_deleted = read.read_outside_tx_req_->is_deleted_;

        rw_set_.UpdateRead(cache_miss_read_cce_addr_,
                           read.read_outside_tx_req_->commit_ts_);
        cc_handler_->ReadOutside(tx_term_,
                                 command_id_.load(std::memory_order_relaxed),
                                 record,
                                 is_deleted,
                                 read.read_outside_tx_req_->commit_ts_,
                                 cache_miss_read_cce_addr_,
                                 read.hd_result_);

        DLOG_IF(INFO, TRACE_OCC_ERR)
            << "ReadOutside ,txn: " << tx_number_ << " ,cce lock:" << std::hex
            << cache_miss_read_cce_addr_.CceLockPtr() << " ,ts: " << std::dec
            << read.read_outside_tx_req_->commit_ts_
            << " ,is_deleted: " << static_cast<int>(is_deleted);

        return;
    }
}

void TransactionExecution::PostProcess(ReadOperation &read)
{
    // collect metrics: remote read duration
    if (metrics::enable_remote_request_metrics &&
        !read.hd_result_.Value().is_local_)
    {
        metrics::Meter *meter;
        meter = tx_processor_->GetMeter();
        if (is_collecting_duration_round_)
        {
            meter->CollectDuration(
                metrics::NAME_REMOTE_REQUEST_DURATION, read.op_start_, "read");
        }
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Decrement,
                       "read");
    }

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (read_.hd_result_.IsError())
    {
        DLOG(ERROR) << "ReadOperation failed for cc error:"
                    << read_.hd_result_.ErrorMsg() << "; txn: " << TxNumber();
        rtp_resp_->FinishError(ConvertCcError(read_.hd_result_.ErrorCode()));
        rtp_resp_ = nullptr;
    }
    else
    {
        const ReadKeyResult &read_res = read_.hd_result_.Value();
        const ReadTxRequest *read_tx_req = read.read_tx_req_;

        if (read.read_type_ == ReadType::OutsideDeleted ||
            read.read_type_ == ReadType::OutsideNormal)
        {
            rec_resp_->Finish(read_res.rec_status_);
            rec_resp_ = nullptr;
            read.Reset();
            return;
        }

        // optimization for case that we read the same key continuously
        // especially speed up remote read. e.g. Read A, Write B, Read A.
        if (!read.read_tx_req_->read_local_ &&
            read_res.rec_status_ == RecordStatus::Normal)
        {
            // rw_set_.AddCacheRead(
            //     *read_req->tab_name_, *read_req->key_, *read_req->rec_);
        }

        if (read_.read_type_ == ReadType::Inside)
        {
            const TableName *table_name = read_tx_req->tab_name_;
            LockType lock_type = read_res.lock_type_;
            if (lock_type != LockType::NoLock)
            {
                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "Before AddRead, txn: " << tx_number_
                    << " ,cce lock:" << std::hex
                    << read_res.cce_addr_.CceLockPtr() << " ,ts: " << std::dec
                    << read_res.ts_ << " ,rec_status: "
                    << static_cast<int>(read_res.rec_status_)
                    << " ,lock: " << static_cast<int>(read_res.lock_type_)
                    << " ,table: " << table_name->String();
                bool add_res;
                if (read_res.rec_status_ == RecordStatus::Unknown)
                {
                    // Only used to release lock.
                    add_res =
                        rw_set_.AddRead(read_res.cce_addr_, 0, table_name);
                }
                else
                {
                    add_res = rw_set_.AddRead(
                        read_res.cce_addr_, read_res.ts_, table_name);
                }
                if (!add_res)
                {
                    DLOG_IF(INFO, TRACE_OCC_ERR)
                        << "AddRead, occ_err: " << tx_number_
                        << " ,cce lock:" << std::hex
                        << read_res.cce_addr_.CceLockPtr()
                        << " ,ts: " << read_res.ts_ << " ,rec_status: "
                        << static_cast<int>(read_res.rec_status_)
                        << " ,lock: " << static_cast<int>(read_res.lock_type_)
                        << " ,table: " << table_name->String();
                    rtp_resp_->FinishError(
                        TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                    rtp_resp_ = nullptr;
                    read.Reset();
                    return;
                }
            }

            // Read lock early release logic:
            // If it is skread and succeeds and need to trace back pk entry,
            // add into drain_batch_
            if (read_tx_req->tab_name_->Type() == TableType::UniqueSecondary &&
                lock_type == LockType::ReadLock &&
                !read.read_tx_req_->is_covering_keys_)
            {
                assert(TxStatus() != TxnStatus::Recovering);

                uint16_t read_cnt = rw_set_.RemoveDataReadEntry(
                    *read_tx_req->tab_name_, read_res.cce_addr_);
                if (read_cnt == 0)
                {
                    drain_batch_.emplace_back(read_res.cce_addr_, read_res.ts_);
                }
            }
            else if (read_tx_req->tab_name_->Type() == TableType::Primary &&
                     !drain_batch_.empty())
            {
                assert(TxStatus() != TxnStatus::Recovering);
                assert(drain_batch_.size() == 1);

                abundant_lock_op_.Reset();
                PushOperation(&abundant_lock_op_);
                Process(abundant_lock_op_);
            }
        }

        if (read_.read_type_ == ReadType::Inside &&
            (read_res.rec_status_ == RecordStatus::Unknown ||
             read_res.rec_status_ == RecordStatus::VersionUnknown))
        {
            // If the read does not retrieve the value, the tx user is likely to
            // read the data store and brings in the value for caching. Cache
            // the cc entry's address in the tx's local variable.
            cache_miss_read_cce_addr_ = read_res.cce_addr_;
        }
        else
        {
            cache_miss_read_cce_addr_.SetCceLock(0, -1, 0, 0);
        }

        rtp_resp_->Finish(std::make_pair(read_res.rec_status_, read_res.ts_));
        rtp_resp_ = nullptr;
    }
    read.Reset();
}

void TransactionExecution::Process(ReadLocalOperation &lock_local)
{
    lock_local.hd_result_->Reset();
    bool finished =
        cc_handler_->ReadLocal(lock_local.table_name_,
                               *lock_local.key_,
                               *lock_local.rec_,
                               ReadType::Inside,
                               tx_number_.load(std::memory_order_relaxed),
                               tx_term_,
                               CommandId(),
                               start_ts_,
                               *lock_local.hd_result_,
                               IsolationLevel::RepeatableRead,
                               CcProtocol::Locking,
                               false,
                               false,
                               lock_local.execute_immediately_);
    if (finished)
    {
        command_id_.fetch_add(1, std::memory_order_relaxed);
    }
}

void TransactionExecution::PostProcess(ReadLocalOperation &lock_local)
{
    if (lock_local.hd_result_->IsError())
    {
        DLOG(ERROR) << "ReadLocalOperation failed for cc error:"
                    << lock_local.hd_result_->ErrorMsg() << ", txn "
                    << TxNumber();
    }
    else if (lock_local.hd_result_->Value().rec_status_ == RecordStatus::Normal)
    {
        // The read lock on the range is added, put the range cce into read
        // set for later release read lock.
        // The range cannot be changed before this tx finishes
        // post-processing, so the read lock on the range is kept until
        // then.
        const ReadKeyResult &read_res = lock_local.hd_result_->Value();
        rw_set_.AddRead(
            read_res.cce_addr_, read_res.ts_, &lock_local.table_name_);
    }
    state_stack_.pop_back();
}

void TransactionExecution::Process(ScanOpenOperation &scan_open)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_open,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    const TableName &table_name = *scan_open.tx_req_->tab_name_;
    uint64_t schema_version = scan_open.tx_req_->schema_version_;
    ScanIndexType index_type = scan_open.tx_req_->indx_type_;
    const TxKey &start_key = *scan_open.tx_req_->StartKey();
    bool inclusive = scan_open.tx_req_->start_inclusive_;
    ScanDirection direction = scan_open.tx_req_->direct_;
    bool is_ckpt_delta = scan_open.tx_req_->is_ckpt_delta_;
    bool is_for_write = scan_open.tx_req_->is_for_write_;
    bool is_for_share = scan_open.tx_req_->is_for_share_;
    bool is_covering_keys = scan_open.tx_req_->is_covering_keys_;
    bool is_require_keys = scan_open.tx_req_->is_require_keys_;
    bool is_require_recs = scan_open.tx_req_->is_require_recs_;
    bool is_require_sort = scan_open.tx_req_->is_require_sort_;

    // scan_open.Reset();
    /*
    scan_open.Set(&table_name,
                  index_type,
                  &start_key,
                  inclusive,
                  direction,
                  is_ckpt_delta);
    */

    scan_open.hd_result_.Value().scan_alias_ = scan_open.tx_req_->scan_alias_;

    if (!scan_open.lock_cluster_config_result_.IsFinished())
    {
        // LOG(INFO) << "==Process: lock cluste config not finished";
        // Acquire cluster config read lock
        lock_cluster_config_op_.Reset(TableName(cluster_config_ccm_name_sv,
                                                TableType::ClusterConfig,
                                                TableEngine::None),
                                      VoidKey::NegInfTxKey(),
                                      &scan_open.cluster_config_rec_,
                                      &scan_open.lock_cluster_config_result_);
        PushOperation(&lock_cluster_config_op_);
        Process(lock_cluster_config_op_);
        return;
    }

    scan_open.is_running_ = true;

    if (scan_open.tx_req_->read_local_)
    {
        LOG(INFO) << "== Scan Open read lock";
        cc_handler_->ScanOpenLocal(table_name,
                                   index_type,
                                   start_key,
                                   inclusive,
                                   tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   command_id_.load(std::memory_order_relaxed),
                                   commit_ts_bound_,
                                   scan_open.hd_result_,
                                   direction,
                                   IsolationLevel::RepeatableRead,
                                   CcProtocol::Locking,
                                   is_for_write,
                                   is_ckpt_delta);
    }
    else
    {
        IsolationLevel iso_lvl = iso_level_;
        if (is_for_share && iso_level_ < IsolationLevel::RepeatableRead)
        {
            iso_lvl = IsolationLevel::RepeatableRead;
        }

        auto start_time = std::chrono::high_resolution_clock::now();
        cc_handler_->ScanOpen(table_name,
                              schema_version,
                              index_type,
                              start_key,
                              inclusive,
                              tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              command_id_.load(std::memory_order_relaxed),
                              start_ts_,
                              scan_open.hd_result_,
                              direction,
                              iso_lvl,
                              protocol_,
                              is_for_write,
                              is_ckpt_delta,
                              is_covering_keys,
                              is_require_keys,
                              is_require_recs,
                              is_require_sort,
                              scan_open.tx_req_->obj_type_,
                              scan_open.tx_req_->scan_pattern_);

        auto end_time = std::chrono::high_resolution_clock::now();
        LOG(INFO) << "scan open time = "
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         end_time - start_time)
                         .count();
    }

    if (table_name.IsHashPartitioned())
    {
        // immediately forward again.
        command_id_.fetch_add(1, std::memory_order_relaxed);
    }
}

void TransactionExecution::PostProcess(ScanOpenOperation &scan_open)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_open,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());
    const TableName &table_name = *scan_open.table_name_;

    ScanOpenResult &open_result = scan_open.hd_result_.Value();

    if (scan_open.hd_result_.IsError())
    {
        DLOG(ERROR) << "ScanOpenOperation failed for cc error:"
                    << scan_open_.hd_result_.ErrorMsg();

        uint64_resp_->FinishError(
            ConvertCcError(scan_open.hd_result_.ErrorCode()));
        uint64_resp_ = nullptr;

        if (open_result.scanner_ != nullptr)
        {
            DrainScanner(open_result.scanner_.get(), table_name);

            abundant_lock_op_.Reset();
            PushOperation(&abundant_lock_op_);
            Process(abundant_lock_op_);
        }

        scan_open.Reset();
        return;
    }

    auto table_iter = rw_set_.WriteSet().find(table_name);
    if (table_iter != rw_set_.WriteSet().end())
    {
        // TODO(lokax): create write iter for each bucket
        if (scan_open.direction_ == ScanDirection::Forward)
        {
            auto wset_it = rw_set_.InitIter(table_iter->second.second,
                                            *scan_open.start_key_,
                                            scan_open.inclusive_);
            if (wset_it.first != wset_it.second)
            {
                wset_iters_.emplace(open_result.scan_alias_, wset_it);
            }
        }
        else
        {
            auto wset_rit = rw_set_.InitReverseIter(table_iter->second.second,
                                                    *scan_open.start_key_,
                                                    scan_open.inclusive_);
            if (wset_rit.first != wset_rit.second)
            {
                wset_reverse_iters_.emplace(open_result.scan_alias_, wset_rit);
            }
        }

        // When WriteSet is not empty, ScanScanner should be merged with
        // WriteSet. Thus is_require_keys and is_require_sort shoud be enabled.
        open_result.scanner_->SetRequireKeys();
        open_result.scanner_->SetRequireSort();
    }

    assert(scans_.find(open_result.scan_alias_) == scans_.end());

    if (!table_name.IsHashPartitioned())
    {
        // Constructs a pseudo slice prior to the first slice of the scan. And
        // sets the status of the scanner "Blocked".
        open_result.scanner_->SetStatus(ScannerStatus::Blocked);
        scans_.try_emplace(open_result.scan_alias_,
                           std::move(open_result.scanner_),
                           scan_open.tx_req_->schema_version_,
                           scan_open.tx_req_->EndKey(),
                           scan_open.tx_req_->end_inclusive_,
                           UINT32_MAX,
                           UINT32_MAX,
                           scan_open.tx_req_->StartKey()->GetShallowCopy(),
                           !scan_open.tx_req_->start_inclusive_,
                           scan_open.direction_ == ScanDirection::Forward
                               ? SlicePosition::LastSliceInRange
                               : SlicePosition::FirstSliceInRange);
    }
    else
    {
        //  sets the status of the scanner "Blocked".
        open_result.scanner_->SetStatus(ScannerStatus::Blocked);

        auto start_time = std::chrono::high_resolution_clock::now();
        if (scan_open.tx_req_->bucket_scan_save_point_->bucket_groups_.empty())
        {
            // Generate scan plan

            if (!CheckLeaderTerm() && !CheckStandbyTerm())
            {
                if (uint64_resp_ != nullptr)
                {
                    uint64_resp_->FinishError(TxErrorCode::TX_NODE_NOT_LEADER);
                    uint64_resp_ = nullptr;
                }
                scan_open.Reset();
                return;
            }

            LocalCcShards *local_cc_shard =
                Sharder::Instance().GetLocalCcShards();
            // <NodeGroupId, <core_idx, vector<bucket_id>>
            // group by node group and core idx
            absl::flat_hash_map<
                NodeGroupId,
                absl::flat_hash_map<uint16_t, std::vector<uint16_t>>>
                grouped;
            for (size_t bucket_id = 0; bucket_id < total_range_buckets;
                 ++bucket_id)
            {
                NodeGroupId bucket_owner =
                    local_cc_shard->GetBucketOwner(bucket_id, TxCcNodeId());
                uint16_t core_idx =
                    Sharder::Instance().ShardBucketIdToCoreIdx(bucket_id);
                grouped[bucket_owner][core_idx].push_back(bucket_id);
            }

            absl::flat_hash_map<NodeGroupId,
                                std::unordered_map<uint16_t, std::size_t>>
                batch_idx;

            bool finished = false;
            while (!finished)
            {
                finished = true;
                absl::flat_hash_map<NodeGroupId, std::vector<uint16_t>>
                    current_plan_bucket_ids;

                for (auto &[ng, core_map] : grouped)
                {
                    for (auto &[ci, vec] : core_map)
                    {
                        std::size_t &cur = batch_idx[ng][ci];
                        if (cur >= vec.size())
                        {
                            continue;
                        }

                        finished = false;

                        // 10 buckets each core
                        std::size_t end =
                            std::min(cur + ScanState::max_bucket_count_per_core,
                                     vec.size());
                        auto &dst = current_plan_bucket_ids[ng];
                        for (size_t i = cur; i < end; ++i)
                        {
                            dst.push_back(vec[i]);
                        }
                        cur = end;
                    }
                }

                if (!finished)
                {
                    scan_open.tx_req_->bucket_scan_save_point_->bucket_groups_
                        .push_back(std::move(current_plan_bucket_ids));
                }
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        LOG(INFO) << "time = "
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         end_time - start_time)
                         .count();

        scans_.try_emplace(open_result.scan_alias_,
                           std::move(open_result.scanner_));
    }

    if (uint64_resp_ != nullptr)
    {
        uint64_resp_->Finish(open_result.scan_alias_);
        uint64_resp_ = nullptr;
    }
    scan_open.Reset();
}

void TransactionExecution::Process(ScanNextOperation &scan_next)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_next,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    if (scan_next.scan_state_ == nullptr)
    {
        // Called from ScanBatchTxRequest
        uint64_t alias = scan_next.tx_req_->alias_;
        auto scan_it = scans_.find(alias);
        if (scan_it == scans_.end())
        {
            bool_resp_->FinishError(TxErrorCode::NG_TERM_CHANGED);
            bool_resp_ = nullptr;
            state_stack_.pop_back();
            return;
        }

        scan_next.UpdateScanState(&scan_it->second);
    }
    else
    {
        // Called from PostProcess(ScanNextOperation)
    }

    CcScanner &scanner = *scan_next.scan_state_->scanner_;
    if (!scan_next.is_running_)
    {
        // LOG(INFO) << "==ScanNextOperation::Process: reset scan next result";
        ScanNextResult &scan_next_result = scan_next.hd_result_.Value();
        scan_next_result.Clear();
        scan_next_result.current_scan_plan_ =
            scan_next.tx_req_->bucket_scan_plan_;
        scan_next_result.ccm_scanner_ = &scanner;
        scan_next.is_running_ = true;
    }

    // TODO(lokax): to_scan_next
    bool to_scan_next = scanner.Current() == nullptr &&
                        scanner.Status() == ScannerStatus::Blocked;

    if (to_scan_next)
    {
        if (scan_next.scan_state_->current_plan_index_ == SIZE_MAX ||
            scan_next.scan_state_->current_plan_index_ !=
                scan_next.tx_req_->bucket_scan_plan_->PlanIndex())
        {
            // LOG(INFO) << "== Close: ";
            scan_next.scan_state_->current_plan_index_ =
                scan_next.tx_req_->bucket_scan_plan_->PlanIndex();
            scanner.Close();
            scanner.SetStatus(ScannerStatus::Blocked);
        }
    }

    // LOG(INFO) << "==ScanNextOperation::Process: to scan next = "
    //          << to_scan_next;
    bool is_local = true;
    if (to_scan_next && scanner.Type() == CcmScannerType::HashPartition)
    {
        if (scanner.read_local_)
        {
            /*
            cc_handler_->ScanNextBatchLocal(
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                start_ts_,
                scanner,
                scan_next.hd_result_);
            */
        }
        else
        {
            // Update handler result ref count
            auto &ng_scan_buckets =
                scan_next.hd_result_.Value().current_scan_plan_->Buckets();
            scan_next.ResetResultForHashPart(ng_scan_buckets.size());

            // Reset all caches, we need to scan next batch data
            scanner.ResetCaches();

            for (const auto &[node_group_id, bucket_ids] : ng_scan_buckets)
            {
                cc_handler_->ScanNextBatch(
                    scan_next.tx_req_->table_name_,
                    node_group_id,
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    command_id_.load(std::memory_order_relaxed),
                    start_ts_,
                    scanner,
                    scan_next.hd_result_,
                    scan_next.tx_req_->obj_type_,
                    scan_next.tx_req_->scan_pattern_);
            }
        }

        is_local = scan_next.hd_result_.RemoteRefCnt() == 0;
    }
    else if (to_scan_next && scanner.Type() == CcmScannerType::RangePartition)
    {
        ScanState &scan_state = *scan_next.scan_state_;

        if ((scanner.Direction() == ScanDirection::Forward &&
             scan_state.slice_position_ == SlicePosition::LastSlice) ||
            (scanner.Direction() == ScanDirection::Backward &&
             scan_state.slice_position_ == SlicePosition::FirstSlice))
        {
            // The current slice is the last (or first). There is no more slice
            // to scan.
            scanner.SetStatus(ScannerStatus::Closed);
            scan_next.slice_hd_result_.SetFinished();
            scan_next.unlock_range_result_.SetFinished();
            return;
        }
        else if (scan_state.slice_position_ == SlicePosition::Middle)
        {
            // When the current slice is in the middle, there is no need to lock
            // the range, as the range has been locked when scanning the range's
            // first slice. Sets the lock result to be finished, so that in case
            // the to-be-scanned slice is the last (first) of the range, the
            // range lock is released when the scan request returns.
            scan_next.lock_range_result_.SetFinished();

            cc_handler_->ScanNextBatch(
                scan_next.tx_req_->table_name_,
                scan_state.schema_version_,
                scan_state.range_id_,
                scan_state.range_ng_,
                scan_next.RangeNgTerm(),
                scan_state.SliceLastKey(),
                !scan_state.inclusive_,
                scan_state.scan_end_key_,
                scan_state.scan_end_inclusive_,
                scan_next.tx_req_->prefetch_slice_cnt_,
                start_ts_,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                CommandId(),
                scan_next.slice_hd_result_,
                iso_level_,
                protocol_);

            is_local = scan_next.slice_hd_result_.Value().is_local_;
            if (metrics::enable_remote_request_metrics && !is_local)
            {
                auto meter = tx_processor_->GetMeter();
                meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                               metrics::Value::IncDecValue::Increment,
                               "scan_next");
                if (is_collecting_duration_round_)
                {
                    scan_next.op_start_ = metrics::Clock::now();
                }
            }
        }
        else if ((scanner.Direction() == ScanDirection::Forward &&
                  scan_state.slice_position_ ==
                      SlicePosition::LastSliceInRange) ||
                 (scanner.Direction() == ScanDirection::Backward &&
                  scan_state.slice_position_ ==
                      SlicePosition::FirstSliceInRange))
        {
            // The last scan reaches the end of the current range. The next scan
            // moves on to the next range, which starts from the last scan's end
            // key. Before reading the first slice of the next range, the scan
            // first locks the next range.

            if (scan_next.lock_range_result_.IsFinished())
            {
                // If there is an error when getting the key's range ID, the
                // error would be caught when forwarding the scan operation,
                // which forces the tx state machine moves to
                // post-processing of the scan operation and returns an
                // error to the tx scan request.
                assert(!scan_next.lock_range_result_.IsError());

                // The term of the cc node group hosting the to-be-scanned range
                // is unknown. Sets the term to -1, indicating it is not matched
                // against that of the cc node when the first slice of the
                // range. The first scan of the range will return the cc node's
                // term and subsequence scans of the remaining slices in the
                // range will match the term.
                cc_handler_->ScanNextBatch(
                    scan_next.tx_req_->table_name_,
                    0,
                    scan_state.range_id_,
                    scan_state.range_ng_,
                    -1,
                    scan_state.SliceLastKey(),
                    !scan_state.inclusive_,
                    scan_state.scan_end_key_,
                    scan_state.scan_end_inclusive_,
                    scan_next.tx_req_->prefetch_slice_cnt_,
                    start_ts_,
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    CommandId(),
                    scan_next.slice_hd_result_,
                    iso_level_,
                    protocol_);

                is_local = scan_next.slice_hd_result_.Value().is_local_;
                if (metrics::enable_remote_request_metrics && !is_local)
                {
                    auto meter = tx_processor_->GetMeter();
                    meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                                   metrics::Value::IncDecValue::Increment,
                                   "scan_next");
                    if (is_collecting_duration_round_)
                    {
                        scan_next.op_start_ = metrics::Clock::now();
                    }
                }
            }
            else
            {
                scan_next.is_running_ = false;

                ReadType lock_range_read_type =
                    scanner.Direction() == ScanDirection::Forward
                        ? ReadType::RangeLeftInclusive
                        : ReadType::RangeRightExclusive;

                bool finished = cc_handler_->ReadLocal(
                    scan_next.range_table_name_,
                    *scan_state.SliceLastKey(),
                    scan_next.range_rec_,
                    lock_range_read_type,
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    CommandId(),
                    start_ts_,
                    scan_next.lock_range_result_,
                    IsolationLevel::RepeatableRead,
                    CcProtocol::Locking);

                if (finished)
                {
                    command_id_.fetch_add(1, std::memory_order_relaxed);
                }
                return;
            }
        }
    }
    else if (scanner.Type() == CcmScannerType::HashPartition)
    {
        scan_next.hd_result_.SetFinished();
        return;
    }
    else if (scanner.Type() == CcmScannerType::RangePartition)
    {
        scan_next.unlock_range_result_.SetFinished();
        scan_next.slice_hd_result_.SetFinished();
        return;
    }

    if (!is_local ||
        DeduceReadLockType(scanner.IndexType() == ScanIndexType::Primary
                               ? TableType::Primary
                               : TableType::Secondary,
                           scanner.IsReadForWrite(),
                           scanner.Isolation(),
                           scanner.IsCoveringKey()) != LockType::NoLock)
    {
        StartTiming();
    }
}

void TransactionExecution::PostProcess(ScanNextOperation &scan_next)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_next,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    const TableName &table_name = scan_next.tx_req_->table_name_;
    CcScanner &scanner = *scan_next.scan_state_->scanner_;
    if (scanner.Type() == CcmScannerType::RangePartition &&
        metrics::enable_remote_request_metrics &&
        !scan_next.slice_hd_result_.Value().is_local_)
    {
        metrics::Meter *meter;
        meter = tx_processor_->GetMeter();
        if (is_collecting_duration_round_)
        {
            meter->CollectDuration(metrics::NAME_REMOTE_REQUEST_DURATION,
                                   scan_next.op_start_,
                                   "scan_next");
        }
        meter = tx_processor_->GetMeter();
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Decrement,
                       "scan_next");
    }

    if (scanner.Type() == CcmScannerType::HashPartition &&
        scan_next.hd_result_.IsError())
    {
        DrainScanner(&scanner, table_name);

        DLOG(ERROR) << "ScanNextOperation failed for cc error: "
                    << scan_next.hd_result_.ErrorMsg();
        bool_resp_->FinishError(
            ConvertCcError(scan_next.hd_result_.ErrorCode()));
        bool_resp_ = nullptr;
        scan_next.Reset();
        return;
    }
    else if (scanner.Type() == CcmScannerType::RangePartition &&
             scan_next.slice_hd_result_.IsError())
    {
        DrainScanner(&scanner, table_name);

        DLOG(ERROR) << "ScanNextOperation failed for cc error: "
                    << scan_next.slice_hd_result_.ErrorMsg()
                    << ", table: " << scan_next.tx_req_->table_name_.Trace();
        bool_resp_->FinishError(
            ConvertCcError(scan_next.slice_hd_result_.ErrorCode()));
        bool_resp_ = nullptr;
        return;
    }

    enum struct AdvanceType
    {
        Ccm,
        WriteSet,
        Both
    };
    AdvanceType advance_type;

    const ScanTuple *cc_scan_tuple = nullptr;
    std::vector<ScanBatchTuple> &scan_batch = *scan_next.tx_req_->batch_;
    assert(scan_batch.empty());

    if (scanner.Type() == CcmScannerType::HashPartition)
    {
        auto start_time = std::chrono::high_resolution_clock::now();
        scanner.Init();
        while (scanner.Status() == ScannerStatus::Open)
        {
            cc_scan_tuple = scanner.Current();
            if (cc_scan_tuple == nullptr)
            {
                scanner.MoveNext();
                assert(scanner.Status() != ScannerStatus::Open);
                break;
            }

            // Deduces the lock type. If a lock is put on the scanned
            // entry, adds the entry into the read set, so that the tx
            // releases the lock in the commit phase.
            LockType scan_tuple_lock_type =
                scanner.DeduceScanTupleLockType(cc_scan_tuple->rec_status_);
            // "key_ts_ == 0", means the lock is added on gap. Now, gap
            // lock is not used when do scan operation.
            if (scan_tuple_lock_type != LockType::NoLock &&
                !cc_scan_tuple->cce_addr_.Empty() &&
                cc_scan_tuple->key_ts_ != 0 &&
                !cmd_set_.FindObjectCommand(table_name,
                                            cc_scan_tuple->cce_addr_))
            {
                TX_TRACE_ACTION_WITH_CONTEXT(
                    this,
                    "PostProcess.ScanOperation.AddReadSet.cce_ptr",
                    &scan_next,
                    (
                        [this, cc_scan_tuple]() -> std::string
                        {
                            return std::string("\"tx_number\":")
                                .append(std::to_string(this->TxNumber()))
                                .append(",\"tx_term\":")
                                .append(std::to_string(this->tx_term_))
                                .append(",\"cce_lock_ptr\":")
                                .append(std::to_string(
                                    cc_scan_tuple->cce_addr_.CceLockPtr()));
                        }));

                // When the record status is unknown, the read ts is set
                // to 0 to release the lock without validation.
                uint64_t read_ts =
                    cc_scan_tuple->rec_status_ != RecordStatus::Unknown
                        ? cc_scan_tuple->key_ts_
                        : 0;

                bool add_res = rw_set_.AddRead(
                    cc_scan_tuple->cce_addr_, read_ts, &table_name);
                if (!add_res)
                {
                    DrainScanner(&scanner, table_name);
                    bool_resp_->FinishError(
                        TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                    bool_resp_ = nullptr;
                    scan_next.Reset();
                    return;
                }
            }

            if (cc_scan_tuple->key_ts_ > 0)
            {
                assert(cc_scan_tuple->rec_status_ != RecordStatus::Unknown);
                // When the record status is not Normal, the record
                // is set to null in the scan result.
                const TxRecord *rec =
                    cc_scan_tuple->rec_status_ == RecordStatus::Normal
                        ? cc_scan_tuple->Record()
                        : nullptr;

                scan_batch.emplace_back(cc_scan_tuple->Key(),
                                        const_cast<TxRecord *>(rec),
                                        cc_scan_tuple->rec_status_,
                                        cc_scan_tuple->key_ts_,
                                        cc_scan_tuple->cce_addr_);
            }

            scanner.MoveNext();
        }

        // LOG(INFO) << "==== debug scan batch size = " << scan_batch.size();

        bool_resp_->Finish(
            scan_next.tx_req_->bucket_scan_plan_->CurrentPlanIsFinished());

        auto stop_time = std::chrono::high_resolution_clock::now();
        size_t time = std::chrono::duration_cast<std::chrono::microseconds>(
                          stop_time - start_time)
                          .count();
        // LOG(INFO) << "== ScanNext PostPrcess time = " << time;
    }
    else
    {
        if (scanner.Direction() == ScanDirection::Forward)
        {
            auto it = wset_iters_.find(scan_next.alias_);
            while (scanner.Status() == ScannerStatus::Open)
            {
                cc_scan_tuple = scanner.Current();
                if (cc_scan_tuple == nullptr)
                {
                    scanner.MoveNext();
                    assert(scanner.Status() != ScannerStatus::Open);
                    break;
                }

                if (it == wset_iters_.end() ||
                    it->second.first == it->second.second ||
                    cc_scan_tuple->key_ts_ == 0)
                {
                    advance_type = AdvanceType::Ccm;
                }
                else
                {
                    assert(scanner.IsRequireKeys());
                    assert(scanner.IsRequireSort());

                    auto &wset_it = it->second.first;
                    const TxKey &write_key = wset_it->first;
                    TxKey ccm_key = cc_scan_tuple->Key();
                    if (write_key < ccm_key)
                    {
                        advance_type = AdvanceType::WriteSet;
                    }
                    else if (ccm_key < write_key)
                    {
                        advance_type = AdvanceType::Ccm;
                    }
                    else
                    {
                        advance_type = AdvanceType::Both;
                    }
                }

                if (advance_type == AdvanceType::WriteSet)
                {
                    auto &wset_it = it->second.first;
                    const TxKey &write_key = wset_it->first;
                    const WriteSetEntry &local_write = wset_it->second;
                    if (local_write.op_ == OperationType::Delete)
                    {
                        scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                nullptr,
                                                RecordStatus::Deleted,
                                                1);
                    }
                    else
                    {
                        scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                local_write.rec_.get(),
                                                RecordStatus::Normal,
                                                1);
                    }

                    ++wset_it;
                }
                else
                {
                    // Deduces the lock type. If a lock is put on the scanned
                    // entry, adds the entry into the read set, so that the tx
                    // releases the lock in the commit phase.
                    LockType scan_tuple_lock_type =
                        scanner.DeduceScanTupleLockType(
                            cc_scan_tuple->rec_status_);
                    // "key_ts_ == 0", means the lock is added on gap. Now, gap
                    // lock is not used when do scan operation.
                    if (scan_tuple_lock_type != LockType::NoLock &&
                        cc_scan_tuple->key_ts_ != 0 &&
                        !cmd_set_.FindObjectCommand(table_name,
                                                    cc_scan_tuple->cce_addr_))
                    {
                        TX_TRACE_ACTION_WITH_CONTEXT(
                            this,
                            "PostProcess.ScanOperation.AddReadSet.cce_ptr",
                            &scan_next,
                            (
                                [this, cc_scan_tuple]() -> std::string
                                {
                                    return std::string("\"tx_number\":")
                                        .append(
                                            std::to_string(this->TxNumber()))
                                        .append(",\"tx_term\":")
                                        .append(std::to_string(this->tx_term_))
                                        .append(",\"cce_lock_ptr\":")
                                        .append(std::to_string(
                                            cc_scan_tuple->cce_addr_
                                                .CceLockPtr()));
                                }));

                        // When the record status is unknown, the read ts is set
                        // to 0 to release the lock without validation.
                        uint64_t read_ts =
                            cc_scan_tuple->rec_status_ != RecordStatus::Unknown
                                ? cc_scan_tuple->key_ts_
                                : 0;

                        bool add_res = rw_set_.AddRead(
                            cc_scan_tuple->cce_addr_, read_ts, &table_name);
                        if (!add_res)
                        {
                            DrainScanner(&scanner, table_name);
                            bool_resp_->FinishError(
                                TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                            bool_resp_ = nullptr;
                            scan_next.Reset();
                            return;
                        }
                    }

                    if (advance_type == AdvanceType::Ccm)
                    {
                        if (cc_scan_tuple->key_ts_ > 0)
                        {
                            assert(cc_scan_tuple->rec_status_ !=
                                   RecordStatus::Unknown);
                            // When the record status is not Normal, the record
                            // is set to null in the scan result.
                            const TxRecord *rec = cc_scan_tuple->rec_status_ ==
                                                          RecordStatus::Normal
                                                      ? cc_scan_tuple->Record()
                                                      : nullptr;

                            scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                    const_cast<TxRecord *>(rec),
                                                    cc_scan_tuple->rec_status_,
                                                    cc_scan_tuple->key_ts_,
                                                    cc_scan_tuple->cce_addr_);
                        }

                        scanner.MoveNext();
                    }
                    else
                    {
                        auto &wset_it = it->second.first;
                        const TxKey &write_key = wset_it->first;
                        const WriteSetEntry &local_write = wset_it->second;
                        // Returns the key-value pair in the local write set.
                        if (local_write.op_ == OperationType::Delete)
                        {
                            scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                    nullptr,
                                                    RecordStatus::Deleted,
                                                    cc_scan_tuple->key_ts_);
                        }
                        else
                        {
                            scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                    local_write.rec_.get(),
                                                    RecordStatus::Normal,
                                                    cc_scan_tuple->key_ts_);
                        }

                        scanner.MoveNext();
                        ++wset_it;
                    }
                }
            }

            if (it != wset_iters_.end())
            {
                auto &wset_it = it->second.first;
                auto &wset_end = it->second.second;
                const TxKey *batch_end_key = nullptr;
                if (scanner.Status() == ScannerStatus::Blocked)
                {
                    if (scanner.Type() == CcmScannerType::RangePartition)
                    {
                        batch_end_key = scan_next.scan_state_->SliceLastKey();
                    }
                    else
                    {
                        batch_end_key =
                            scan_batch.empty()
                                ? nullptr
                                : &scan_batch[scan_batch.size() - 1].key_;
                    }
                }

                while (wset_it != wset_end &&
                       (batch_end_key == nullptr ||
                        batch_end_key->Type() != KeyType::Normal ||
                        wset_it->first < *batch_end_key))
                {
                    const TxKey &write_key = wset_it->first;
                    const WriteSetEntry &local_write = wset_it->second;
                    // Returns the key-value pair in the local write set.
                    if (local_write.op_ != OperationType::Delete)
                    {
                        scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                local_write.rec_.get(),
                                                RecordStatus::Normal,
                                                1);
                    }
                    else
                    {
                        scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                nullptr,
                                                RecordStatus::Deleted,
                                                1);
                    }

                    ++wset_it;
                }
            }
        }
        // backward scan
        else
        {
            auto rit = wset_reverse_iters_.find(scan_next.alias_);

            while (scanner.Status() == ScannerStatus::Open)
            {
                cc_scan_tuple = scanner.Current();
                if (cc_scan_tuple == nullptr)
                {
                    scanner.MoveNext();
                    assert(scanner.Status() != ScannerStatus::Open);
                    break;
                }

                if (rit == wset_reverse_iters_.end() ||
                    rit->second.first == rit->second.second ||
                    cc_scan_tuple->key_ts_ == 0)
                {
                    advance_type = AdvanceType::Ccm;
                }
                else
                {
                    assert(scanner.IsRequireKeys());
                    assert(scanner.IsRequireSort());

                    auto &wset_it = rit->second.first;
                    const TxKey &write_key = wset_it->first;
                    TxKey ccm_key = cc_scan_tuple->Key();
                    if (ccm_key < write_key)
                    {
                        advance_type = AdvanceType::WriteSet;
                    }
                    else if (write_key < ccm_key)
                    {
                        advance_type = AdvanceType::Ccm;
                    }
                    else
                    {
                        advance_type = AdvanceType::Both;
                    }
                }

                if (advance_type == AdvanceType::WriteSet)
                {
                    auto &wset_it = rit->second.first;
                    const TxKey &write_key = wset_it->first;
                    const WriteSetEntry &local_write = wset_it->second;
                    if (local_write.op_ != OperationType::Delete)
                    {
                        scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                local_write.rec_.get(),
                                                RecordStatus::Normal,
                                                1);
                    }
                    else
                    {
                        scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                nullptr,
                                                RecordStatus::Deleted,
                                                1);
                    }

                    ++wset_it;
                }
                else
                {
                    // Deduces the lock type. If a lock is put on the scanned
                    // entry, adds the entry into the read set, so that the tx
                    // releases the lock in the commit phase.
                    LockType scan_tuple_lock_type =
                        scanner.DeduceScanTupleLockType(
                            cc_scan_tuple->rec_status_);
                    // "key_ts_ == 0", means the lock is added on gap. Now, gap
                    // lock is not used when do scan operation.
                    if (scan_tuple_lock_type != LockType::NoLock &&
                        cc_scan_tuple->key_ts_ != 0 &&
                        !cmd_set_.FindObjectCommand(table_name,
                                                    cc_scan_tuple->cce_addr_))
                    {
                        TX_TRACE_ACTION_WITH_CONTEXT(
                            this,
                            "PostProcess.ScanOperation.AddReadSet.cce_ptr",
                            &scan_next,
                            (
                                [this, cc_scan_tuple]() -> std::string
                                {
                                    return std::string("\"tx_number\":")
                                        .append(
                                            std::to_string(this->TxNumber()))
                                        .append(",\"tx_term\":")
                                        .append(std::to_string(this->tx_term_))
                                        .append(",\"cce_lock_ptr\":")
                                        .append(std::to_string(
                                            cc_scan_tuple->cce_addr_
                                                .CceLockPtr()));
                                }));

                        uint64_t read_ts =
                            cc_scan_tuple->rec_status_ != RecordStatus::Unknown
                                ? cc_scan_tuple->key_ts_
                                : 0;

                        bool add_res = rw_set_.AddRead(
                            cc_scan_tuple->cce_addr_, read_ts, &table_name);
                        if (!add_res)
                        {
                            DrainScanner(&scanner, table_name);
                            bool_resp_->FinishError(
                                TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                            bool_resp_ = nullptr;
                            scan_next.Reset();
                            return;
                        }
                    }

                    if (advance_type == AdvanceType::Ccm)
                    {
                        if (cc_scan_tuple->key_ts_ > 0)
                        {
                            assert(cc_scan_tuple->rec_status_ !=
                                   RecordStatus::Unknown);

                            // When the record status is not Normal, the record
                            // is set to null in the scan result.
                            const TxRecord *rec = cc_scan_tuple->rec_status_ ==
                                                          RecordStatus::Normal
                                                      ? cc_scan_tuple->Record()
                                                      : nullptr;

                            scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                    const_cast<TxRecord *>(rec),
                                                    cc_scan_tuple->rec_status_,
                                                    cc_scan_tuple->key_ts_,
                                                    cc_scan_tuple->cce_addr_);
                        }

                        scanner.MoveNext();
                    }
                    else
                    {
                        auto &wset_it = rit->second.first;
                        const TxKey &write_key = wset_it->first;
                        const WriteSetEntry &local_write = wset_it->second;
                        // Returns the key-value pair in the local write set.
                        if (local_write.op_ == OperationType::Delete)
                        {
                            scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                    nullptr,
                                                    RecordStatus::Deleted,
                                                    cc_scan_tuple->key_ts_);
                        }
                        else
                        {
                            scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                    local_write.rec_.get(),
                                                    RecordStatus::Normal,
                                                    cc_scan_tuple->key_ts_);
                        }

                        scanner.MoveNext();
                        ++wset_it;
                    }
                }
            }

            if (rit != wset_reverse_iters_.end())
            {
                auto &wset_it = rit->second.first;
                auto &wset_end = rit->second.second;
                const TxKey *batch_start_key = nullptr;
                if (scanner.Status() == ScannerStatus::Blocked)
                {
                    if (scanner.Type() == CcmScannerType::RangePartition)
                    {
                        batch_start_key = scan_next.scan_state_->SliceLastKey();
                    }
                    else
                    {
                        batch_start_key =
                            scan_batch.empty() ? nullptr : &scan_batch[0].key_;
                    }
                }

                while (wset_it != wset_end &&
                       (batch_start_key == nullptr ||
                        batch_start_key->Type() != KeyType::Normal ||
                        *batch_start_key < wset_it->first ||
                        *batch_start_key == wset_it->first))
                {
                    const TxKey &write_key = wset_it->first;
                    const WriteSetEntry &local_write = wset_it->second;
                    // Returns the key-value pair in the local write set.
                    if (local_write.op_ != OperationType::Delete)
                    {
                        scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                local_write.rec_.get(),
                                                RecordStatus::Normal,
                                                1);
                    }
                    else
                    {
                        scan_batch.emplace_back(write_key.GetShallowCopy(),
                                                nullptr,
                                                RecordStatus::Deleted,
                                                1);
                    }

                    ++wset_it;
                }
            }
        }

        ScanDirection dir = scan_next.Direction();
        SlicePosition slice_pos = scan_next.scan_state_->slice_position_;
        bool scan_finished = (dir == ScanDirection::Forward &&
                              slice_pos == SlicePosition::LastSlice) ||
                             (dir == ScanDirection::Backward &&
                              slice_pos == SlicePosition::FirstSlice);

        if (scanner.Type() == CcmScannerType::RangePartition &&
            scan_batch.empty())
        {
            // Scan next batch in range partition scans a slice at a time.
            // Keep scanning until we reach the last slice in last range or
            // we get something from the last slice scanned.
            if (!scan_finished)
            {
                scan_next.slice_hd_result_.Value().Reset();
                scan_next.slice_hd_result_.Reset();
                PushOperation(&scan_next);
                Process(scan_next);
                return;
            }
        }

        bool_resp_->Finish(scan_finished);
    }

    bool_resp_ = nullptr;
    scan_next.Reset();
}

void TransactionExecution::ScanClose(
    const std::vector<UnlockTuple> &unlock_batch,
    uint64_t alias,
    const TableName &table_name)
{
    CcScanner *scanner = nullptr;
    auto scan_it = scans_.find(alias);
    if (scan_it == scans_.end())
    {
        return;
    }
    scanner = scan_it->second.scanner_.get();

    if (!unlock_batch.empty())
    {
        drain_batch_.reserve(unlock_batch.size());

        for (const UnlockTuple &tpl : unlock_batch)
        {
            // Newly-inserted records in the write set have empty cc entry
            // address.
            if (tpl.cce_addr_.Empty())
            {
                continue;
            }

            LockType lk_type = scanner->DeduceScanTupleLockType(tpl.status_);
            if (lk_type == LockType::NoLock)
            {
                continue;
            }

            uint16_t read_cnt =
                rw_set_.RemoveDataReadEntry(table_name, tpl.cce_addr_);
            if (read_cnt == 0)
            {
                drain_batch_.emplace_back(tpl.cce_addr_, tpl.version_ts_);
                if (drain_batch_.back().first.CceLockPtr() == 0)
                {
                    LOG(INFO) << "== empty lock ptr";
                }
            }
        }
    }

    if (scanner->Type() == CcmScannerType::RangePartition &&
        scan_it->second.slice_position_ == SlicePosition::Middle)
    {
        // Append last tuple of each ScanCache which has acquired ReadIntent to
        // the drain_batch_.
        //
        // 1) If last_tuple.lk_type is NoLock, then drain_batch_ doesn't include
        // them and should append them into itself. 2) If last_tuple.lk_type is
        // not NoLock, then drain_batch_ has include them, and should skip them.
        // Non-repetition and non-omission.
        std::vector<const ScanTuple *> last_tuples;
        scanner->MemoryShardCacheLastTuples(&last_tuples);
        for (const ScanTuple *last_tuple : last_tuples)
        {
            if (last_tuple)
            {
                LockType lk_type =
                    scanner->DeduceScanTupleLockType(last_tuple->rec_status_);
                if (lk_type == LockType::NoLock)
                {
                    drain_batch_.emplace_back(last_tuple->cce_addr_,
                                              last_tuple->key_ts_);
                    if (drain_batch_.back().first.CceLockPtr() == 0)
                    {
                        LOG(INFO) << "== empty lock ptr";
                    }
                }
            }
        }
    }
    else if (scanner->Type() == CcmScannerType::HashPartition)
    {
        // In hash partition, cross every channel of scanner and get the last
        // tuple, then add it into drain_batch_ to ensure the ReadIntent lock to
        // be released if added.

        /*
        std::vector<const ScanTuple *> last_tuples;
        scanner->MemoryShardCacheLastTuples(&last_tuples);
        for (const ScanTuple *last_tuple : last_tuples)
        {
            if (last_tuple)
            {
                LockType lk_type =
                    scanner->DeduceScanTupleLockType(last_tuple->rec_status_);
                // key ts == 0 means the lock is on the gap. So the read intent
                // is acquired on the last cce during last scan batch.
                if ((lk_type == LockType::NoLock || last_tuple->key_ts_ == 0) &&
                    !cmd_set_.FindObjectCommand(table_name,
                                                last_tuple->cce_addr_))
                {
                    drain_batch_.emplace_back(last_tuple->cce_addr_,
                                              last_tuple->key_ts_);
                    if (drain_batch_.back().first.CceLockPtr() == 0)
                    {
                        LOG(INFO) << "== empty lock ptr";
                    }
                }
            }
        }
        */
    }

    // Release trailing tuple locks acquired during scan. These tuples are
    // tuples scanned beyond scan end key and are not intended to be locked.
    // They were not added into read set. Check if they were put into read set
    // by other operations before, if not, release these locks.
    std::vector<const ScanTuple *> trailing_tuples;
    scanner->MemoryShardCacheTrailingTuples(&trailing_tuples);
    for (auto tuple : trailing_tuples)
    {
        LockType lk_type = scanner->DeduceScanTupleLockType(tuple->rec_status_);
        if (lk_type != LockType::NoLock &&
            rw_set_.GetReadCnt(table_name, tuple->cce_addr_) == 0)
        {
            drain_batch_.emplace_back(tuple->cce_addr_, tuple->key_ts_);
            if (drain_batch_.back().first.CceLockPtr() == 0)
            {
                LOG(INFO) << "== empty lock ptr";
            }
        }
    }

    if (scanner->Type() == CcmScannerType::HashPartition)
    {
        DrainScanner(scanner, table_name);
    }

    cc_handler_->ScanClose(
        table_name, scanner->Direction(), std::move(scan_it->second.scanner_));

    abundant_lock_op_.Reset();
    PushOperation(&abundant_lock_op_);
    Process(abundant_lock_op_);
    scans_.erase(scan_it);
}

TxErrorCode TransactionExecution::Insert(const TableName &table_name,
                                         TxKey tx_key,
                                         TxRecord::Uptr rec)
{
    TxResult<Void> tx_result(nullptr, nullptr);
    void_resp_ = &tx_result;
    Upsert(table_name,
           0,
           std::move(tx_key),
           std::move(rec),
           OperationType::Insert);
    assert(tx_result.Status() != TxResultStatus::Unknown);

    return tx_result.ErrorCode();
}

// Upsert modify tuple without locking in OCC protocol.
void TransactionExecution::Upsert(const TableName &table_name,
                                  uint64_t schema_version,
                                  TxKey key,
                                  TxRecord::Uptr rec,
                                  OperationType op)
{
    TxErrorCode err_code = TxErrorCode::NO_ERROR;
    if ((err_code = rw_set_.AddWrite(
             table_name, schema_version, std::move(key), std::move(rec), op)) !=
        TxErrorCode::NO_ERROR)
    {
        void_resp_->FinishError(err_code);
        void_resp_ = nullptr;
        return;
    }
    void_resp_->Finish(void_);
    void_resp_ = nullptr;
}

void TransactionExecution::Commit()
{
    if (tx_term_ < 0)
    {
        bool_resp_->Finish(false);
        bool_resp_ = nullptr;

        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);
        Reset();
        return;
    }

    bool is_recovering = TxStatus() == TxnStatus::Recovering;
    if (!is_recovering)
    {
        tx_status_.store(TxnStatus::Committing, std::memory_order_relaxed);
    }

    if (cmd_set_.ObjectCountToForwardWrite() > 0)
    {
        assert(txlog_ != nullptr && !txservice_skip_wal);
        PushOperation(&cmd_forward_write_);
        Process(cmd_forward_write_);
        return;
    }

    if (rw_set_.WriteSetSize() > 0)
    {
        assert(!is_recovering);
        lock_write_range_buckets_.Reset();
        PushOperation(&lock_write_range_buckets_);
        Process(lock_write_range_buckets_);
    }
    else if (rw_set_.CatalogWriteSetSize() > 0)
    {
        PushOperation(&catalog_acquire_all_);
        Process(catalog_acquire_all_);
    }
    else
    {
        if (is_recovering)
        {
            // For recover tx commit, commit_ts is already determined and
            // we don't need to update TEntry since it is not assigned for
            // a recovering tx. Just release all the locks in readset and
            // recycle txm.
            PushOperation(&validate_);
            Process(validate_);
        }
        else
        {
            PushOperation(&set_ts_);
        }
    }
}

void TransactionExecution::Abort()
{
    bool is_standby_tx = IsStandbyTx(TxTerm());

    if (tx_term_ < 0 || (!is_standby_tx && !CheckLeaderTerm()))
    {
        if (bool_resp_ != nullptr)
        {
            bool_resp_->Finish(false);
            bool_resp_ = nullptr;
        }

        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);
        Reset();
        return;
    }

    bool need_update_tentry = TxStatus() != TxnStatus::Recovering;
    tx_status_.store(TxnStatus::Aborted, std::memory_order_relaxed);

    {
        // No need to update txn status since we did not assign
        // TEntry for recovering tx.
        uint32_t acquire_write_cnt = rw_set_.WriteSetSize() +
                                     rw_set_.ForwardWriteCnt() +
                                     cmd_set_.ObjectCntWithWriteLock();
        if (acquire_write_cnt > 0 && acquire_write_.hd_result_.IsError())
        {
            std::vector<AcquireKeyResult> &acquire_key_vec =
                acquire_write_.hd_result_.Value();
            size_t error_cnt = 0;
            for (const AcquireKeyResult &acq_key : acquire_key_vec)
            {
                if (acq_key.cce_addr_.Term() < 0)
                {
                    ++error_cnt;
                }
            }
            acquire_write_cnt -= error_cnt;
        }
        else if (lock_write_range_buckets_.lock_result_->IsError())
        {
            acquire_write_cnt = 0;
        }

        post_process_.Reset(acquire_write_cnt,
                            rw_set_.DataReadSetSize(),
                            rw_set_.MetaDataReadSetSize(),
                            rw_set_.CatalogWriteSetSize() *
                                Sharder::Instance().NodeGroupCount(),
                            need_update_tentry);
        PushOperation(&post_process_);
        Process(post_process_);
    }
}

void TransactionExecution::Process(
    LockWriteRangeBucketsOp &lock_write_range_buckets)
{
    if (!lock_write_range_buckets.init_)
    {
        std::unordered_map<TableName, std::pair<uint64_t, TableWriteSet>>
            &wset = rw_set_.WriteSet();
        lock_write_range_buckets.table_it_ = wset.begin();
        lock_write_range_buckets.table_end_ = wset.end();

        lock_write_range_buckets.write_key_it_ =
            lock_write_range_buckets.table_it_->second.second.begin();
        lock_write_range_buckets.write_key_end_ =
            lock_write_range_buckets.table_it_->second.second.end();

        lock_write_range_buckets.init_ = true;
    }

    assert(lock_write_range_buckets.table_it_ !=
           lock_write_range_buckets.table_end_);
    assert(lock_write_range_buckets.write_key_it_ !=
           lock_write_range_buckets.write_key_end_);

    auto table_it = lock_write_range_buckets.table_it_;
    const TxKey &write_key = lock_write_range_buckets.write_key_it_->first;

    lock_write_range_buckets.lock_result_->Value().Reset();
    lock_write_range_buckets.lock_result_->Reset();
    lock_write_range_buckets.is_running_ = true;

    bool finished = false;
    if (table_it->first.IsHashPartitioned())
    {
        // For hash partitioned tables, skip the bucket keys that have been
        // locked.
        do
        {
            table_it = lock_write_range_buckets.table_it_;
            lock_write_range_buckets.Advance(this);
        } while (lock_write_range_buckets.table_it_ !=
                     lock_write_range_buckets.table_end_ &&
                 lock_write_range_buckets.table_it_ != table_it &&
                 lock_write_range_buckets.table_it_->first.IsHashPartitioned());
        if (lock_write_range_buckets.table_it_ ==
            lock_write_range_buckets.table_end_)
        {
            state_stack_.pop_back();
            assert(state_stack_.empty());

            PushOperation(&acquire_write_);
            Process(acquire_write_);
            return;
        }
    }
    if (table_it->first.IsHashPartitioned())
    {
        // Lock bucket key.
        uint16_t bucket_id = Sharder::MapKeyHashToBucketId(write_key.Hash());
        bucket_key_.Reset(bucket_id);

        finished = cc_handler_->ReadLocal(
            range_bucket_ccm_name,
            bucket_tx_key_,
            bucket_rec_,
            ReadType::Inside,
            tx_number_.load(std::memory_order_relaxed),
            tx_term_,
            command_id_.load(std::memory_order_relaxed),
            start_ts_,
            *lock_write_range_buckets.lock_result_,
            IsolationLevel::RepeatableRead,
            CcProtocol::Locking,
            false,
            false,
            lock_write_range_buckets.execute_immediately_);
    }
    else
    {
        // Lock range key, which will also lock bucket key.
        lock_write_range_buckets.range_table_name_ =
            TableName(table_it->first.StringView(),
                      TableType::RangePartition,
                      table_it->first.Engine());

        finished = cc_handler_->ReadLocal(
            lock_write_range_buckets.range_table_name_,
            write_key,
            range_rec_,
            ReadType::Inside,
            tx_number_.load(std::memory_order_relaxed),
            tx_term_,
            command_id_.load(std::memory_order_relaxed),
            start_ts_,
            *lock_write_range_buckets.lock_result_,
            IsolationLevel::RepeatableRead,
            CcProtocol::Locking,
            false,
            false,
            lock_write_range_buckets.execute_immediately_);
    }

    if (finished)
    {
        command_id_.fetch_add(1, std::memory_order_relaxed);
    }
}

void TransactionExecution::PostProcess(
    LockWriteRangeBucketsOp &lock_write_range_buckets)
{
    if (lock_write_range_buckets.lock_result_->IsError())
    {
        DLOG(ERROR) << "LockWriteRangeBucketsOp failed for cc error:"
                    << lock_write_range_buckets.lock_result_->ErrorMsg()
                    << ", tx " << TxNumber();
        state_stack_.pop_back();
        assert(state_stack_.empty());
        lock_write_range_buckets.Reset();
        Abort();
        return;
    }

    const ReadKeyResult &read_res =
        lock_write_range_buckets.lock_result_->Value();
    const TableName &tbl_name = lock_write_range_buckets.table_it_->first;
    if (tbl_name.IsHashPartitioned())
    {
        rw_set_.AddRead(
            read_res.cce_addr_, read_res.ts_, &range_bucket_ccm_name);
    }
    else
    {
        TableName range_tbl_name(tbl_name.StringView(),
                                 TableType::RangePartition,
                                 tbl_name.Engine());
        rw_set_.AddRead(read_res.cce_addr_, read_res.ts_, &range_tbl_name);

        assert(
            [&]()
            {
                TxKey range_start_key = range_rec_.GetRangeInfo()->StartTxKey();
                return !(lock_write_range_buckets.write_key_it_->first <
                         range_start_key);
            }());

        assert(
            [&]()
            {
                TxKey range_end_key = range_rec_.GetRangeInfo()->EndTxKey();
                return lock_write_range_buckets.write_key_it_->first <
                       range_end_key;
            }());
    }

    lock_write_range_buckets.Advance(this);

    if (lock_write_range_buckets.table_it_ ==
        lock_write_range_buckets.table_end_)
    {
        state_stack_.pop_back();
        assert(state_stack_.empty());

        PushOperation(&acquire_write_);
        Process(acquire_write_);
    }
    else
    {
        // There are more ranges to acquire read locks. Returns now and waits
        // for the next round of execution to acquire read locks on the
        // remaining ranges. Note that we do not call Forward() here. This is
        // because doing so leads to recursive calls of Forward(), each
        // acquiring a read lock on one range. This may result in stack overflow
        // when there are many ranges for write-set keys.
        lock_write_range_buckets.is_running_ = false;
        lock_write_range_buckets.execute_immediately_ = true;
        command_id_.fetch_add(1, std::memory_order_relaxed);
    }
}

void TransactionExecution::Process(AcquireWriteOperation &acquire_write)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &acquire_write,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    acquire_write.Reset(rw_set_.WriteSetSize() + rw_set_.ForwardWriteCnt(),
                        rw_set_.WriteSetSize());
    acquire_write.is_running_ = true;

    uint64_t current_ts =
        static_cast<LocalCcHandler *>(cc_handler_)->GetTsBaseValue();

    size_t res_idx = 0, entry_idx = 0;
    auto &wset = rw_set_.WriteSet();
    for (auto &[table_name, pair] : wset)
    {
        uint64_t schema_version = pair.first;
        for (auto &[write_key, write_entry] : pair.second)
        {
            acquire_write.acquire_write_entries_[entry_idx++] = &write_entry;

            // TODO: enable is_insert after Serializable Isolation is
            // supported.
            cc_handler_->AcquireWrite(
                table_name,
                schema_version,
                write_key,
                write_entry.key_shard_code_,
                TxNumber(),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                current_ts,
                false,
                acquire_write.hd_result_,
                res_idx++,
                protocol_,
                iso_level_);
            for (auto &[forward_shard_code, cce_addr] :
                 write_entry.forward_addr_)
            {
                cc_handler_->AcquireWrite(
                    table_name,
                    schema_version,
                    write_key,
                    forward_shard_code,
                    TxNumber(),
                    tx_term_,
                    command_id_.load(std::memory_order_relaxed),
                    current_ts,
                    false,
                    acquire_write.hd_result_,
                    res_idx++,
                    protocol_,
                    iso_level_);
            }
        }
    }

    if (metrics::enable_remote_request_metrics &&
        acquire_write.hd_result_.Value().at(0).remote_ack_cnt_->load(
            std::memory_order_relaxed) > 0)
    {
        auto meter = tx_processor_->GetMeter();
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Increment,
                       "acquire_write");
        if (is_collecting_duration_round_)
        {
            acquire_write.op_start_ = metrics::Clock::now();
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(AcquireWriteOperation &acquire_write)
{
    if (metrics::enable_remote_request_metrics &&
        acquire_write.op_start_ < metrics::TimePoint::max())
    {
        metrics::Meter *meter;
        meter = tx_processor_->GetMeter();
        if (is_collecting_duration_round_)
        {
            meter->CollectDuration(metrics::NAME_REMOTE_REQUEST_DURATION,
                                   acquire_write.op_start_,
                                   "acquire_write");
        }
        meter = tx_processor_->GetMeter();
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Decrement,
                       "acquire_write");
    }

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &acquire_write,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (acquire_write.rset_has_expired_)
    {
        DLOG(ERROR) << "AcquireWriteOperation failed for rset has expired."
                    << "; txn: " << TxNumber();
        bool_resp_->SetErrorCode(TxErrorCode::WRITE_WRITE_CONFLICT);
        Abort();
    }
    else if (acquire_write.hd_result_.IsError())
    {
        bool_resp_->SetErrorCode(
            ConvertCcError(acquire_write.hd_result_.ErrorCode()));
        Abort();
    }
    else
    {
        if (rw_set_.CatalogWriteSetSize() > 0)
        {
            PushOperation(&catalog_acquire_all_);
            Process(catalog_acquire_all_);
        }
        else
        {
            PushOperation(&set_ts_);
            Process(set_ts_);
        }
    }

    acquire_write.Reset(0, 0);
}

void TransactionExecution::Process(CatalogAcquireAllOp &acquire_catalog_write)
{
    catalog_acquire_all_.SetCatalogWriteSet(rw_set_.CatalogWriteSet());
    catalog_acquire_all_.is_running_ = true;
}

void TransactionExecution::PostProcess(
    CatalogAcquireAllOp &acquire_catalog_write)
{
    state_stack_.pop_back();
    if (catalog_acquire_all_.succeed_)
    {
        catalog_acquire_all_.Reset();
        PushOperation(&set_ts_);
        Process(set_ts_);
    }
    else
    {
        catalog_acquire_all_.Reset();
        Abort();
    }
}

void TransactionExecution::Process(SetCommitTsOperation &set_ts)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &set_ts,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    uint64_t candidate = commit_ts_bound_;

    set_ts.is_running_ = true;

    for (const AcquireKeyResult &acquire_key :
         acquire_write_.hd_result_.Value())
    {
        candidate = std::max(candidate, acquire_key.last_vali_ts_ + 1);
        candidate = std::max(candidate, acquire_key.commit_ts_ + 1);
    }

    if (rw_set_.CatalogWriteSetSize() > 0)
    {
        candidate = std::max(candidate, catalog_acquire_all_.MaxTs());
    }

    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, CmdSetEntry>>
        *cmd_set = cmd_set_.ObjectCommandSet();
    for (const auto &[table_name, cce_set] : *cmd_set)
    {
        for (const auto &[cce_addr, cmd_set_entry] : cce_set)
        {
            candidate = std::max(candidate, cmd_set_entry.last_vali_ts_ + 1);
            candidate = std::max(candidate, cmd_set_entry.object_version_ + 1);
            candidate = std::max(candidate, cmd_set_entry.lock_ts_ + 1);
        }
    }

    // Data item read set
    const absl::flat_hash_map<CcEntryAddr,
                              std::pair<ReadSetEntry, const std::string_view>>
        &rset = rw_set_.DataReadSet();
    for (const auto &cce_it : rset)
    {
        const ReadSetEntry &read_entry = cce_it.second.first;
        candidate = std::max(candidate, read_entry.version_ts_ + 1);
    }
    // Catalog, range, cluster config read set
    const absl::flat_hash_map<CcEntryAddr,
                              std::pair<ReadSetEntry, const std::string_view>>
        &catalog_range_rset = rw_set_.MetaDataReadSet();
    for (const auto &cce_it : catalog_range_rset)
    {
        const ReadSetEntry &read_entry = cce_it.second.first;
        candidate = std::max(candidate, read_entry.version_ts_ + 1);
    }

    cc_handler_->SetCommitTimestamp(txid_, candidate, set_ts.hd_result_);
}

void TransactionExecution::PostProcess(SetCommitTsOperation &set_ts)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &set_ts,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (set_ts.hd_result_.IsError())
    {
        DLOG(ERROR) << "SetCommitTsOperation failed for cc error:"
                    << set_ts.hd_result_.ErrorMsg();
        Abort();
    }
    else
    {
        commit_ts_ = set_ts.hd_result_.Value();
        if (rw_set_.DataReadSetSize() > 0)
        {
            PushOperation(&validate_);
            Process(validate_);
        }
        else
        {
            bool needs_write_log =
                !txservice_skip_wal &&
                (cmd_set_.ObjectModified() || rw_set_.WriteSetSize() > 0 ||
                 rw_set_.CatalogWriteSetSize() > 0);
            if (txlog_ != nullptr && needs_write_log)
            {
                bool prepare_log_success = false;
                prepare_log_success = FillDataLogRequest(write_log_);
                PushOperation(&write_log_);
                if (prepare_log_success)
                {
                    Process(write_log_);
                }
                else
                {
                    // node group terms not consistent, hd_res is set to error.
                    // skip writing log
                    assert(write_log_.hd_result_.IsError());
                    PostProcess(write_log_);
                }
            }
            else
            {
                bool need_update_tentry = TxStatus() != TxnStatus::Recovering;
                tx_status_.store(TxnStatus::Committed,
                                 std::memory_order_relaxed);

                {
                    // No need to update txn status since we did not assign
                    // TEntry for recovering tx.
                    uint32_t acquire_write_cnt =
                        rw_set_.WriteSetSize() + rw_set_.ForwardWriteCnt() +
                        cmd_set_.ObjectCntWithWriteLock();
                    post_process_.Reset(
                        acquire_write_cnt,
                        0,
                        rw_set_.MetaDataReadSetSize(),
                        rw_set_.CatalogWriteSetSize() *
                            Sharder::Instance().NodeGroupCount(),
                        need_update_tentry);

                    if (rw_set_.CatalogWriteSetSize() > 0)
                    {
                        PushOperation(&post_process_);
                        flush_update_table_.need_write_log_ = false;
                        PushOperation(&flush_update_table_);
                    }
                    else
                    {
                        PushOperation(&post_process_);
                    }
                }
            }
        }
    }
    set_ts.Reset();
}

void TransactionExecution::Process(ValidateOperation &validate)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &validate,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    // Only release data read lock in this phase.
    const absl::flat_hash_map<CcEntryAddr,
                              std::pair<ReadSetEntry, const std::string_view>>
        &rset = rw_set_.DataReadSet();

    size_t read_data_cnt = rw_set_.DataReadSetSize();
    validate.Reset(read_data_cnt);
    validate.is_running_ = true;

    size_t local_post_cnt = 0, remote_post_cnt = 0;
    for (const auto &[cce_addr, read_entry_pair] : rset)
    {
        const ReadSetEntry &read_entry = read_entry_pair.first;
        CcReqStatus ret =
            cc_handler_->PostRead(tx_number_.load(std::memory_order_relaxed),
                                  tx_term_,
                                  command_id_.load(std::memory_order_relaxed),
                                  read_entry.version_ts_,
                                  0,
                                  commit_ts_,
                                  cce_addr,
                                  validate.hd_result_);

        if (ret == CcReqStatus::SentLocal)
        {
            ++local_post_cnt;
        }
        else if (ret == CcReqStatus::SentRemote)
        {
            ++remote_post_cnt;
        }
    }

    if (metrics::enable_remote_request_metrics &&
        !validate.hd_result_.Value().is_local_)
    {
        auto meter = tx_processor_->GetMeter();
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Increment,
                       "validate");
        if (is_collecting_duration_round_)
        {
            validate.op_start_ = metrics::Clock::now();
        }
    }

    if (local_post_cnt == 0 && remote_post_cnt == 0)
    {
        // All validations have finished. Advance the command Id to forward the
        // tx.
        AdvanceCommand();
    }
    else if (remote_post_cnt > 0)
    {
        StartTiming();
    }
}

void TransactionExecution::PostProcess(ValidateOperation &validate)
{
    // collect metrics: remote validate duration
    if (metrics::enable_remote_request_metrics &&
        !validate.hd_result_.Value().is_local_)
    {
        metrics::Meter *meter;
        meter = tx_processor_->GetMeter();
        if (is_collecting_duration_round_)
        {
            meter->CollectDuration(metrics::NAME_REMOTE_REQUEST_DURATION,
                                   validate.op_start_,
                                   "validate");
        }
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Decrement,
                       "validate");
    }

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &validate,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    // The validation step is optional. Only pops the stack if the last step
    // is the validation step.
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &validate_);
        state_stack_.pop_back();
    }

    if (validate.IsError())
    {
        DLOG_IF(INFO, TRACE_OCC_ERR)
            << "Validate, occ_err, txn: " << tx_number_
            << " ,hd_result_.IsError():"
            << static_cast<int>(validate.hd_result_.ErrorCode())
            << " ,conflict_tx size:" << validate.hd_result_.Value().Size();

        DLOG(ERROR) << "ValidateOperation failed for cc error:"
                    << validate.hd_result_.ErrorMsg() << ", txn " << TxNumber();

        if (bool_resp_ != nullptr)
        {
            bool_resp_->SetErrorCode(
                ConvertCcError(validate.hd_result_.ErrorCode()));
        }
        else if (rec_resp_ != nullptr)
        {
            // auto committed ObjectCommandTxRequest
            rec_resp_->FinishError(
                ConvertCcError(validate.hd_result_.ErrorCode()));
            rec_resp_ = nullptr;
        }
        else if (vct_rec_resp_ != nullptr)
        {
            // auto committed MultiObjectCommandTxRequest
            vct_rec_resp_->FinishError(
                ConvertCcError(validate.hd_result_.ErrorCode()));
            vct_rec_resp_ = nullptr;
        }

        // Clear read set so that Abort won't try to release the read locks
        // again.
        rw_set_.ClearDataReadSet();

        Abort();
    }
    else
    {
        bool needs_write_log =
            !txservice_skip_wal &&
            (cmd_set_.ObjectModified() || rw_set_.WriteSetSize() > 0 ||
             rw_set_.CatalogWriteSetSize() > 0);
        if (txlog_ != nullptr && needs_write_log)
        {
            bool prepare_log_success = false;
            prepare_log_success = FillDataLogRequest(write_log_);
            PushOperation(&write_log_);
            if (prepare_log_success)
            {
                Process(write_log_);
            }
            else
            {
                // node group terms not consistent, hd_res is set to error. skip
                // writing log
                assert(write_log_.hd_result_.IsError());
                PostProcess(write_log_);
            }
        }
        else
        {
            bool need_update_tentry = TxStatus() != TxnStatus::Recovering;
            tx_status_.store(TxnStatus::Committed, std::memory_order_relaxed);

            // This is either a read-only tx or a tx that acquires write lock on
            // an object but does not modify it(i.e. ExecuteOn returns false),
            // there is no need to wait PostWrite in this case because PostWrite
            // is only used for releasing lock. Notifies early before
            // post-processing.
            if (bool_resp_ != nullptr &&
                !(cmd_set_.ObjectModified() || rw_set_.WriteSetSize() > 0))
            {
                bool_resp_->Finish(true);
                bool_resp_ = nullptr;
            }

            {
                // No need to update txn status since we did not assign TEntry
                // for recovering tx.
                post_process_.Reset(rw_set_.WriteSetSize() +
                                        rw_set_.ForwardWriteCnt() +
                                        cmd_set_.ObjectCntWithWriteLock(),
                                    0,
                                    rw_set_.MetaDataReadSetSize(),
                                    rw_set_.CatalogWriteSetSize() *
                                        Sharder::Instance().NodeGroupCount(),
                                    need_update_tentry);
                PushOperation(&post_process_);
                Process(post_process_);
            }
        }
    }
}

bool TransactionExecution::FillDataLogRequest(WriteToLogOp &write_log)
{
    write_log.log_type_ = TxLogType::DATA;
    ACTION_FAULT_INJECTOR("before_write_log");

    write_log.log_closure_.LogRequest().Clear();

    ::txlog::LogRequest &log_req = write_log.log_closure_.LogRequest();
    ::txlog::WriteLogRequest *log_rec = log_req.mutable_write_log_request();

    log_rec->set_tx_term(tx_term_);
    log_rec->set_txn_number(TxNumber());
    log_rec->set_commit_timestamp(commit_ts_);
    log_rec->set_retry(false);

    auto shard_terms = log_rec->mutable_node_terms();
    shard_terms->clear();

    auto data_log_msg = log_rec->mutable_log_content()->mutable_data_log();
    auto shard_logs = data_log_msg->mutable_node_txn_logs();
    shard_logs->clear();

    assert(log_rec->node_terms_size() == 0);

    const auto &wset = rw_set_.WriteSet();

    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, CmdSetEntry>>
        &tx_cmd_set = *cmd_set_.ObjectCommandSet();
    // organize by node group
    std::unordered_map<
        NodeGroupId,
        std::pair<
            std::unordered_map<TableName, std::vector<const CmdSetEntry *>>,
            std::unordered_map<
                TableName,
                std::vector<std::pair<const TxKey *, const WriteSetEntry *>>>>>
        ng_table_set;

    // reorganize all WriteSetEntries from old structure to new structure
    for (const auto &[table_name, pair] : wset)
    {
        for (const auto &[write_key, wset_entry] : pair.second)
        {
            const CcEntryAddr &addr = wset_entry.cce_addr_;
            uint32_t ng_id = addr.NodeGroupId();

            // Only fills WriteLogRequest::node_terms for base table.
            auto [shard_terms_it, inserted] =
                shard_terms->insert({ng_id, addr.Term()});
            if (inserted == false && shard_terms_it->second != addr.Term())
            {
                // Two keys in the tx's write set refer to the same cc node
                // group, but have different terms. It means that the cc node
                // must have failed over at least once and the tx have obtained
                // a write intention before the failure. The tx must abort
                // because the write intention obtained  the failure have been
                // invalidated.
                write_log.hd_result_.SetError(CcErrorCode::NG_TERM_CHANGED);
                return false;
            }

            auto table_rec_it = ng_table_set.try_emplace(ng_id);
            std::unordered_map<
                TableName,
                std::vector<std::pair<const TxKey *, const WriteSetEntry *>>>
                &table_rec_set = table_rec_it.first->second.second;

            auto rec_vec_it = table_rec_set.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_name.StringView(),
                                      table_name.Type(),
                                      table_name.Engine()),
                std::forward_as_tuple());

            rec_vec_it.first->second.emplace_back(&write_key, &wset_entry);

            for (const auto &[forward_shard_code, addr] :
                 wset_entry.forward_addr_)
            {
                // If the wset entry needs to be double written into different
                // ngs, write log for both ngs.
                uint32_t forward_ng_id =
                    Sharder::Instance().ShardToCcNodeGroup(forward_shard_code);
                auto table_rec_it = ng_table_set.try_emplace(forward_ng_id);
                std::unordered_map<
                    TableName,
                    std::vector<
                        std::pair<const TxKey *, const WriteSetEntry *>>>
                    &table_rec_set = table_rec_it.first->second.second;

                auto rec_vec_it = table_rec_set.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(table_name.StringView(),
                                          table_name.Type(),
                                          table_name.Engine()),
                    std::forward_as_tuple());
                rec_vec_it.first->second.emplace_back(&write_key, &wset_entry);
            }
        }
    }

    for (const auto &[table_name, obj_cmd_set] : tx_cmd_set)
    {
        for (const auto &[cce_addr, obj_cmd_entry] : obj_cmd_set)
        {
            // skip those CmdSetEntry that have no successful commands
            if (!obj_cmd_entry.HasSuccessfulCommand())
            {
                continue;
            }
            uint32_t ng_id = cce_addr.NodeGroupId();
            auto shard_term_it = shard_terms->find(ng_id);
            if (shard_term_it == shard_terms->end())
            {
                (*shard_terms)[ng_id] = cce_addr.Term();
            }
            else if (shard_term_it->second != cce_addr.Term())
            {
                // Two keys in the tx's write set refer to the same cc node
                // group, but have different terms. It means that the cc node
                // must have failed over at least once and the tx have obtained
                // a write lock before the failure.
                write_log.hd_result_.SetError(CcErrorCode::NG_TERM_CHANGED);
                return false;
            }

            auto &table_cmds =
                ng_table_set.try_emplace(ng_id).first->second.first;
            auto &obj_cmds_vector =
                table_cmds.try_emplace(table_name).first->second;
            // insert cce into cmd_set
            obj_cmds_vector.emplace_back(&obj_cmd_entry);

            if (obj_cmd_entry.forward_entry_ != nullptr &&
                !obj_cmd_entry.forward_entry_->cce_addr_.Empty())
            {
                const CcEntryAddr &f_cce_addr =
                    obj_cmd_entry.forward_entry_->cce_addr_;

                uint32_t f_ng_id = f_cce_addr.NodeGroupId();
                auto shard_term_it = shard_terms->find(f_ng_id);
                if (shard_term_it == shard_terms->end())
                {
                    (*shard_terms)[f_ng_id] = f_cce_addr.Term();
                }
                else if (shard_term_it->second != f_cce_addr.Term())
                {
                    // Two keys in the tx's write set refer to the same cc node
                    // group, but have different terms. It means that the cc
                    // node must have failed over at least once and the tx have
                    // obtained a write lock before the failure.
                    write_log.hd_result_.SetError(CcErrorCode::NG_TERM_CHANGED);
                    return false;
                }

                auto &f_table_cmds =
                    ng_table_set.try_emplace(f_ng_id).first->second.first;
                auto &f_obj_cmds_vector =
                    f_table_cmds.try_emplace(table_name).first->second;
                // insert cce into cmd_set
                f_obj_cmds_vector.emplace_back(&obj_cmd_entry);
            }
        }
    }

    // construct one log_ng_blob per ng_id
    for (const auto &[ng_id, obj_wset_pair] : ng_table_set)
    {
        auto &[table_cmds, table_rec_set] = obj_wset_pair;
        auto [shard_it, inserted] = shard_logs->insert({ng_id, std::string{}});
        std::string *log_ng_blob = &shard_it->second;

        // The log blob of a table in a node group is in the following
        // format: (1) A 1-byte integer for the length of the table name,
        // followed by (2) The string of the table name. (3) A 1-byte
        // integer for the type of table. (4) A 4-byte integer for the total
        // length of serialized key-record pairs modified by the tx in the
        // node group. (5) A sequence of modified records. Each record is
        // encoded as follows:
        //   (a) The serialized key
        //   (b) A 1-byte flag to indicate if the record is normal, deleted
        //   or void. (c) The serialized record if the record is normal.
        for (const auto &[table_name, wset_entry_vec] : table_rec_set)
        {
            uint8_t tabname_len = table_name.StringView().size();
            const char *ptr = reinterpret_cast<const char *>(&tabname_len);
            log_ng_blob->append(ptr, sizeof(uint8_t));
            log_ng_blob->append(table_name.StringView().data(), tabname_len);
            // 1 byte integer for table engine
            ptr = reinterpret_cast<const char *>(&table_name.Engine());
            log_ng_blob->append(ptr, sizeof(uint8_t));
            // 1 byte integer for table type
            ptr = reinterpret_cast<const char *>(&table_name.Type());
            log_ng_blob->append(ptr, sizeof(uint8_t));

            // The start position of the 4-byte integer for the length of
            // serialized k-v pairs.
            size_t kv_len_start = log_ng_blob->size();
            uint32_t kv_len = 0;
            ptr = reinterpret_cast<const char *>(&kv_len);
            // Reserves 4 bytes in the blob for the k-v length before
            // committed records are serialized and the length of the
            // serialized records are known.
            log_ng_blob->append(ptr, sizeof(uint32_t));

            for (const auto &wset_entry : wset_entry_vec)
            {
                const TxKey *write_key = wset_entry.first;
                const WriteSetEntry *write_entry = wset_entry.second;
                write_key->Serialize(*log_ng_blob);

                uint8_t operation = static_cast<uint8_t>(write_entry->op_);
                log_ng_blob->append(reinterpret_cast<const char *>(&operation),
                                    1);

                if (write_entry->op_ != OperationType::Delete &&
                    write_entry->rec_ != nullptr)
                {
                    write_entry->rec_->Serialize(*log_ng_blob);
                }
            }

            kv_len = log_ng_blob->size() - kv_len_start - sizeof(uint32_t);

            // Refills the reserved 4 bytes after knowing the length of
            // serialized records.
            log_ng_blob->replace(
                kv_len_start, sizeof(uint32_t), ptr, sizeof(uint32_t));
        }

        // Fill command log for this ng.
        for (const auto &[table_name, cmd_entry_vec] : table_cmds)
        {
            uint8_t tabname_len = table_name.StringView().size();
            const char *ptr = reinterpret_cast<const char *>(&tabname_len);
            log_ng_blob->append(ptr, sizeof(uint8_t));
            log_ng_blob->append(table_name.StringView().data(), tabname_len);

            // 1 byte integer for table engine
            ptr = reinterpret_cast<const char *>(&table_name.Engine());
            log_ng_blob->append(ptr, sizeof(uint8_t));
            // 1 byte integer for table type
            ptr = reinterpret_cast<const char *>(&table_name.Type());
            log_ng_blob->append(ptr, sizeof(uint8_t));

            // The start position of the 4-byte integer for the length of
            // serialized key and object commands.
            size_t key_cmd_len_start = log_ng_blob->size();
            uint32_t key_cmd_len = 0;
            ptr = reinterpret_cast<const char *>(&key_cmd_len);
            // Reserve 4 bytes in the blob for the length of serialized key
            // and commands before it is known.
            log_ng_blob->append(ptr, sizeof(uint32_t));
            for (const CmdSetEntry *cmd_entry : cmd_entry_vec)
            {
                const std::string &key_str = cmd_entry->obj_key_str_;
                uint64_t obj_version = cmd_entry->object_version_;
                uint64_t ttl = cmd_entry->ttl_;

                const std::vector<std::string> &cmd_str_list =
                    cmd_entry->cmd_str_list_;

                // write object key, object version, ttl, and commands to
                // log blob
                log_ng_blob->append(key_str);
                log_ng_blob->append(
                    reinterpret_cast<const char *>(&obj_version),
                    sizeof(obj_version));

                log_ng_blob->append(reinterpret_cast<const char *>(&ttl),
                                    sizeof(ttl));

                size_t cmds_len_start = log_ng_blob->size();
                uint32_t cmds_len = 0;
                log_ng_blob->append(reinterpret_cast<const char *>(&cmds_len),
                                    sizeof(cmds_len));

                uint8_t has_overwrite = cmd_entry->ignore_previous_version_;
                log_ng_blob->append(
                    reinterpret_cast<const char *>(&has_overwrite),
                    sizeof(has_overwrite));
                // number of commands
                uint16_t cmd_cnt = cmd_str_list.size();
                log_ng_blob->append(reinterpret_cast<const char *>(&cmd_cnt),
                                    sizeof(cmd_cnt));

                for (const auto &cmd_str : cmd_str_list)
                {
                    uint32_t cmd_len = cmd_str.size();
                    log_ng_blob->append(
                        reinterpret_cast<const char *>(&cmd_len),
                        sizeof(cmd_len));
                    log_ng_blob->append(cmd_str);
                }

                cmds_len =
                    log_ng_blob->size() - cmds_len_start - sizeof(uint32_t);
                log_ng_blob->replace(cmds_len_start,
                                     sizeof(cmds_len),
                                     reinterpret_cast<const char *>(&cmds_len),
                                     sizeof(cmds_len));
            }

            key_cmd_len =
                (log_ng_blob->size() - key_cmd_len_start - sizeof(uint32_t));
            // Refills the reserved 4 bytes after knowing the length of
            // serialized key and commands.
            log_ng_blob->replace(
                key_cmd_len_start, sizeof(uint32_t), ptr, sizeof(uint32_t));
        }
    }

    // fill rw_set_.catalog_wset_ term if any insert request triggers an catalog
    // logical update operation.
    for (size_t idx = 0;
         idx < catalog_acquire_all_.acquire_all_lock_op_.upload_cnt_;
         ++idx)
    {
        const AcquireAllResult &hres_val =
            catalog_acquire_all_.acquire_all_lock_op_.hd_results_[idx].Value();
        uint32_t ng_id = hres_val.local_cce_addr_.NodeGroupId();
        int64_t ng_term = hres_val.local_cce_addr_.Term();
        auto [shard_term_it, inserted] =
            shard_terms->insert({ng_id, hres_val.node_term_});
        if (inserted == false && shard_term_it->second != ng_term)
        {
            write_log.hd_result_.SetError(CcErrorCode::NG_TERM_CHANGED);
            return false;
        }
    }
    for (const auto &[write_key, write_entry] : rw_set_.CatalogWriteSet())
    {
        const CatalogKey *catalog_key = write_key.GetKey<CatalogKey>();
        const CatalogRecord *catalog_rec =
            static_cast<const CatalogRecord *>(write_entry.rec_.get());

        txlog::SchemaOpMessage *schema_msg = data_log_msg->add_schema_logs();
        schema_msg->set_table_name_str(catalog_key->Name().String());
        schema_msg->set_table_type(::txlog::ToRemoteType::ConvertTableType(
            catalog_key->Name().Type()));
        schema_msg->set_table_engine(::txlog::ToRemoteType::ConvertTableEngine(
            catalog_key->Name().Engine()));
        schema_msg->set_old_catalog_blob(catalog_rec->Schema()->SchemaImage());
        schema_msg->set_catalog_ts(catalog_rec->SchemaTs());
        schema_msg->set_new_catalog_blob(catalog_rec->DirtySchemaImage());
        schema_msg->mutable_table_op()->set_op_type(
            static_cast<uint32_t>(OperationType::Update));
        schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_PrepareSchema);
    }

    // fill read set entry term
    for (const auto [ng_id, ng_term] : rw_set_.ReadLockNgTerms())
    {
        // Only fills WriteLogRequest::node_terms for base table.
        auto [shard_term_it, inserted] = shard_terms->insert({ng_id, ng_term});
        if (inserted == false && shard_term_it->second != ng_term)
        {
            // Two keys in the tx's read/write set refer to the same cc node
            // group, but have different terms.
            write_log.hd_result_.SetError(CcErrorCode::NG_TERM_CHANGED);
            return false;
        }
    }

    return true;
}

bool TransactionExecution::FillCommitCatalogsLogRequest(WriteToLogOp &write_log)
{
    write_log.log_type_ = TxLogType::DATA;
    write_log.log_closure_.LogRequest().Clear();

    ::txlog::LogRequest &log_req = write_log.log_closure_.LogRequest();
    ::txlog::WriteLogRequest *log_rec = log_req.mutable_write_log_request();

    log_rec->set_tx_term(tx_term_);
    log_rec->set_txn_number(TxNumber());
    log_rec->set_commit_timestamp(commit_ts_);
    log_rec->clear_node_terms();

    auto data_log_msg = log_rec->mutable_log_content()->mutable_data_log();

    // fill rw_set_.catalog_wset_ if any insert request triggers an catalog
    // logical update operation.
    for (const auto &[write_key, write_entry] : rw_set_.CatalogWriteSet())
    {
        const CatalogKey *catalog_key = write_key.GetKey<CatalogKey>();

        txlog::SchemaOpMessage *schema_msg = data_log_msg->add_schema_logs();
        schema_msg->set_table_name_str(catalog_key->Name().String());
        schema_msg->set_table_type(::txlog::ToRemoteType::ConvertTableType(
            catalog_key->Name().Type()));
        schema_msg->mutable_table_op()->set_op_type(
            static_cast<uint32_t>(OperationType::Update));
        schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_CleanSchema);
    }

    return true;
}

bool TransactionExecution::FillCleanCatalogsLogRequest(WriteToLogOp &write_log)
{
    write_log.log_type_ = TxLogType::DATA;
    write_log.log_closure_.LogRequest().Clear();

    ::txlog::LogRequest &log_req = write_log.log_closure_.LogRequest();
    ::txlog::WriteLogRequest *log_rec = log_req.mutable_write_log_request();

    log_rec->set_tx_term(tx_term_);
    log_rec->set_txn_number(TxNumber());
    log_rec->set_commit_timestamp(commit_ts_);
    log_rec->clear_node_terms();

    auto data_log_msg = log_rec->mutable_log_content()->mutable_data_log();

    // fill rw_set_.catalog_wset_ if any insert request triggers an catalog
    // logical update operation.
    for (const auto &[write_key, write_entry] : rw_set_.CatalogWriteSet())
    {
        const CatalogKey *catalog_key = write_key.GetKey<CatalogKey>();

        txlog::SchemaOpMessage *schema_msg = data_log_msg->add_schema_logs();
        schema_msg->set_table_name_str(catalog_key->Name().String());
        schema_msg->set_table_type(::txlog::ToRemoteType::ConvertTableType(
            catalog_key->Name().Type()));
        schema_msg->mutable_table_op()->set_op_type(
            static_cast<uint32_t>(OperationType::Update));
        schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_CleanSchema);
    }

    return true;
}

void TransactionExecution::Process(WriteToLogOp &write_log)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &write_log,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    write_log.Reset();
    write_log.is_running_ = true;
    if (txservice_skip_wal && write_log.log_type_ == TxLogType::DATA)
    {
        // Only skip data wal log. Other 2pc log still needs to be
        // persisted to make sure that meta data is consistent across
        // cluster.
        write_log.hd_result_.SetFinished();
        PostProcess(write_log);
        return;
    }

    if (metrics::enable_remote_request_metrics)
    {
        auto meter = tx_processor_->GetMeter();
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Increment,
                       "write_log");
        if (is_collecting_duration_round_)
        {
            write_log.op_start_ = metrics::Clock::now();
        }
    }

    assert(txlog_ != nullptr);
    // Note that node_id calculated from global core ID should always be
    // equal to the actual ccshard node id. But from txservice layer's view,
    // only txid is available. Txservice get txid from the bottom layer
    // (ccshard).
    write_log.log_group_id_ = txlog_->GetLogGroupId(tx_number_);
    ::txlog::WriteLogRequest *wlog_req =
        write_log.log_closure_.LogRequest().mutable_write_log_request();
    wlog_req->set_log_group_id(write_log.log_group_id_);
#ifdef EXT_TX_PROC_ENABLED
    write_log.hd_result_.SetToBlock();
    std::atomic_thread_fence(std::memory_order_release);
#endif
    txlog_->WriteLog(write_log.log_group_id_,
                     write_log.log_closure_.Controller(),
                     write_log.log_closure_.LogRequest(),
                     write_log.log_closure_.LogResponse(),
                     write_log.log_closure_);
    ACTION_FAULT_INJECTOR("after_write_log");
}

void TransactionExecution::PostProcess(WriteToLogOp &write_log)
{
    // collect metrics: write log duration
    if (metrics::enable_remote_request_metrics)
    {
        metrics::Meter *meter;
        meter = tx_processor_->GetMeter();
        if (is_collecting_duration_round_)
        {
            meter->CollectDuration(metrics::NAME_REMOTE_REQUEST_DURATION,
                                   write_log.op_start_,
                                   "write_log");
        }
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Decrement,
                       "write_log");
    }
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &write_log,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    WriteToLogOp *log_op = static_cast<WriteToLogOp *>(state_stack_.back());
    state_stack_.pop_back();

    if (state_stack_.empty())
    {
        // Only multi stage op has recovering status
        bool need_update_tentry = TxStatus() != TxnStatus::Recovering;
        assert(need_update_tentry &&
               "Only multi stage op has recovering status");

        if (!log_op->hd_result_.IsError())
        {
            tx_status_.store(TxnStatus::Committed, std::memory_order_relaxed);
        }
        else
        {
            if (log_op->hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                if (bool_resp_ != nullptr)
                {
                    bool_resp_->SetErrorCode(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);
                    bool_resp_->Finish(false);
                    bool_resp_ = nullptr;
                }
                else if (rec_resp_ != nullptr)
                {
                    // auto committed ObjectCommandTxRequest
                    rec_resp_->FinishError(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);
                    rec_resp_ = nullptr;
                }
                else if (vct_rec_resp_ != nullptr)
                {
                    // auto committed MultiObjectCommandTxRequest
                    vct_rec_resp_->FinishError(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);
                    vct_rec_resp_ = nullptr;
                }
                tx_status_.store(TxnStatus::Unknown, std::memory_order_release);
            }
            else
            {
                DLOG(ERROR) << "txn: " << TxNumber()
                            << " WriteToLogOp failed for cc error:"
                            << log_op->hd_result_.ErrorMsg();
                if (bool_resp_ != nullptr)
                {
                    bool_resp_->SetErrorCode(TxErrorCode::WRITE_LOG_FAIL);
                    bool_resp_->Finish(false);
                    bool_resp_ = nullptr;
                }
                else if (rec_resp_ != nullptr)
                {
                    // auto committed ObjectCommandTxRequest
                    rec_resp_->FinishError(TxErrorCode::WRITE_LOG_FAIL);
                    rec_resp_ = nullptr;
                }
                else if (vct_rec_resp_ != nullptr)
                {
                    // auto committed MultiObjectCommandTxRequest
                    vct_rec_resp_->FinishError(TxErrorCode::WRITE_LOG_FAIL);
                    vct_rec_resp_ = nullptr;
                }

                tx_status_.store(TxnStatus::Aborted, std::memory_order_release);
            }
        }

        uint32_t acquire_write_cnt = rw_set_.WriteSetSize() +
                                     rw_set_.ForwardWriteCnt() +
                                     cmd_set_.ObjectCntWithWriteLock();
        TxnStatus status = TxStatus();
        if (status == TxnStatus::Committed)
        {
            // The tx is committed. The tx must have finished validation.
            // Post-processing includes both primary keys that have locks and
            // secondary keys without locks.
            post_process_.Reset(acquire_write_cnt,
                                0,
                                rw_set_.MetaDataReadSetSize(),
                                rw_set_.CatalogWriteSetSize() *
                                    Sharder::Instance().NodeGroupCount(),
                                need_update_tentry);
        }
        else if (status == TxnStatus::Aborted)
        {
            post_process_.Reset(acquire_write_cnt,
                                rw_set_.DataReadSetSize(),
                                rw_set_.MetaDataReadSetSize(),
                                rw_set_.CatalogWriteSetSize() *
                                    Sharder::Instance().NodeGroupCount(),
                                need_update_tentry);
        }
        else if (status == TxnStatus::Unknown)
        {
            post_process_.Reset(0,
                                rw_set_.DataReadSetSize(),
                                rw_set_.MetaDataReadSetSize(),
                                0,
                                need_update_tentry);
        }

        write_log.Reset();

        if (status == TxnStatus::Committed && rw_set_.CatalogWriteSetSize() > 0)
        {
            PushOperation(&post_process_);
            flush_update_table_.need_write_log_ = true;
            PushOperation(&flush_update_table_);
        }
        else
        {
            PushOperation(&post_process_);
            Process(post_process_);
        }
    }
    else
    {
        // The tx is committing a multi-stage operation, e.g., schema
        // changes.
    }
}

void TransactionExecution::Process(UpdateTxnStatus &update_txn)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &update_txn,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    update_txn.Reset();
    update_txn.is_running_ = true;
    cc_handler_->UpdateTxnStatus(txid_,
                                 iso_level_,
                                 tx_status_.load(std::memory_order_relaxed),
                                 update_txn.hd_result_);
}

void TransactionExecution::PostProcess(UpdateTxnStatus &update_txn)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &update_txn,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (bool_resp_ != nullptr && bool_resp_ != &commit_tx_req_->tx_result_)
    {
        if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
        {
            bool_resp_->Finish(true);
        }
        else
        {
            bool_resp_->Finish(false);
        }

        bool_resp_ = nullptr;
    }
    else if (rec_resp_ != nullptr)
    {
        // auto committed ObjectCommandTxRequest
        rec_resp_->Finish(obj_cmd_.hd_result_.Value().rec_status_);
        rec_resp_ = nullptr;
    }
    else if (vct_rec_resp_ != nullptr)
    {
        // auto committed MultiObjectCommandTxRequest
        std::vector<RecordStatus> vct_rec;
        vct_rec.reserve(multi_obj_cmd_.vct_hd_result_.size());
        for (const auto &hresult : multi_obj_cmd_.vct_hd_result_)
        {
            vct_rec.push_back(hresult.Value().rec_status_);
        }

        vct_rec_resp_->Finish(std::move(vct_rec));
        vct_rec_resp_ = nullptr;
    }
    // transaction can be recycled and put into free list.
    tx_status_.store(TxnStatus::Finished, std::memory_order_release);
    update_txn.Reset();

    Reset();
}

void TransactionExecution::Process(PostProcessOp &post_process)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &post_process,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    post_process.is_running_ = true;

    uint64_t tx_number = TxNumber();
    uint16_t command_id = command_id_.load(std::memory_order_relaxed);
    size_t post_local_cnt = 0, post_remote_cnt = 0;
    auto update_post_cnt = [&](CcReqStatus status)
    {
        if (status == CcReqStatus::SentLocal)
        {
            ++post_local_cnt;
        }
        else if (status == CcReqStatus::SentRemote)
        {
            ++post_remote_cnt;
        }
    };

    if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
    {
        // If the tx has finished validation, the read intentions/locks of
        // the read-set keys have been cleared after validation.
        // Post-processing only clears the write locks of the write-set
        // keys.

        const auto &wset = rw_set_.WriteSet();
        for (const auto &[table_name, pair] : wset)
        {
            for (const auto &[key, write_entry] : pair.second)
            {
                CcReqStatus ret =
                    cc_handler_->PostWrite(tx_number,
                                           tx_term_,
                                           command_id,
                                           commit_ts_,
                                           write_entry.cce_addr_,
                                           write_entry.rec_.get(),
                                           write_entry.op_,
                                           write_entry.key_shard_code_,
                                           post_process.hd_result_);
                update_post_cnt(ret);

                for (auto &[forward_shard_code, cce_addr] :
                     write_entry.forward_addr_)
                {
                    CcReqStatus ret =
                        cc_handler_->PostWrite(tx_number,
                                               tx_term_,
                                               command_id,
                                               commit_ts_,
                                               cce_addr,
                                               write_entry.rec_.get(),
                                               write_entry.op_,
                                               forward_shard_code,
                                               post_process.hd_result_);
                    update_post_cnt(ret);
                }
            }
        }

        const std::unordered_map<TableName,
                                 std::unordered_map<CcEntryAddr, CmdSetEntry>>
            *cmd_cce_set = cmd_set_.ObjectCommandSet();
        assert(cmd_cce_set != nullptr);

        for (const auto &[table_name, cce_set] : *cmd_cce_set)
        {
            for (const auto &[cce_addr, cmd_set_entry] : cce_set)
            {
                CcReqStatus ret = cc_handler_->PostWrite(
                    tx_number,
                    tx_term_,
                    command_id,
                    cmd_set_entry.HasSuccessfulCommand() ? commit_ts_ : 0,
                    cce_addr,
                    nullptr,
                    OperationType::CommitCommands,
                    0,
                    post_process.hd_result_);
                update_post_cnt(ret);

                if (cmd_set_entry.forward_entry_ != nullptr &&
                    !cmd_set_entry.forward_entry_->cce_addr_.Empty())
                {
                    assert(cmd_set_entry.HasSuccessfulCommand());
                    // upload commands to the key bucket's new owner.
                    CcReqStatus ret = cc_handler_->UploadTxCommands(
                        tx_number,
                        tx_term_,
                        command_id,
                        cmd_set_entry.forward_entry_->cce_addr_,
                        cmd_set_entry.object_version_,
                        commit_ts_,
                        &cmd_set_entry.cmd_str_list_,
                        cmd_set_entry.ignore_previous_version_,
                        post_process.hd_result_);
                    update_post_cnt(ret);
                }
            }
        }
    }
    else
    {
        // If the tx failed during the acquire phase or was aborted before
        // entering the commit phase, post-processing removes write intents
        // of write-set keys and clears read intents/locks of read-set keys.

        if (TxStatus() != TxnStatus::Unknown)
        {
            const auto &wset = rw_set_.WriteSet();

            for (const auto &[table_name, pair] : wset)
            {
                for (const auto &[key, write_entry] : pair.second)
                {
                    if (write_entry.cce_addr_.Term() >= 0)
                    {
                        assert(!write_entry.cce_addr_.Empty());

                        // Abort doesn't care the OperationType, since PostWrite
                        // is just used to release the lock.
                        CcReqStatus ret = cc_handler_->PostWrite(
                            tx_number_.load(std::memory_order_relaxed),
                            tx_term_,
                            command_id_.load(std::memory_order_relaxed),
                            0,
                            write_entry.cce_addr_,
                            nullptr,
                            write_entry.op_,
                            write_entry.key_shard_code_,
                            post_process.hd_result_);
                        update_post_cnt(ret);
                    }
                    // Keys that were not successfully locked in the cc
                    // map do not need post-processing.

                    for (const auto &[forward_shard_code, cce_addr] :
                         write_entry.forward_addr_)
                    {
                        if (cce_addr.Term() >= 0)
                        {
                            assert(!cce_addr.Empty());
                            CcReqStatus ret =
                                cc_handler_->PostWrite(tx_number,
                                                       tx_term_,
                                                       command_id,
                                                       0,
                                                       cce_addr,
                                                       nullptr,
                                                       write_entry.op_,
                                                       forward_shard_code,
                                                       post_process.hd_result_);
                            update_post_cnt(ret);
                        }
                    }
                }
            }

            const std::unordered_map<
                TableName,
                std::unordered_map<CcEntryAddr, CmdSetEntry>> *cmd_cce_set =
                cmd_set_.ObjectCommandSet();
            assert(cmd_cce_set != nullptr);

            for (const auto &[table_name, cce_set] : *cmd_cce_set)
            {
                for (const auto &[cce_addr, cmd_set_entry] : cce_set)
                {
                    CcReqStatus ret =
                        cc_handler_->PostWrite(tx_number,
                                               tx_term_,
                                               command_id,
                                               0,
                                               cce_addr,
                                               nullptr,
                                               OperationType::CommitCommands,
                                               0,
                                               post_process.hd_result_);
                    update_post_cnt(ret);

                    if (cmd_set_entry.forward_entry_ != nullptr &&
                        !cmd_set_entry.forward_entry_->cce_addr_.Empty())
                    {
                        CcReqStatus ret = cc_handler_->PostWrite(
                            tx_number,
                            tx_term_,
                            command_id,
                            0,
                            cmd_set_entry.forward_entry_->cce_addr_,
                            nullptr,
                            OperationType::CommitCommands,
                            0,
                            post_process.hd_result_);
                        update_post_cnt(ret);
                    }
                }
            }
        }

        // Only release data read lock in this phase.
        const absl::flat_hash_map<
            CcEntryAddr,
            std::pair<ReadSetEntry, const std::string_view>> &rset =
            rw_set_.DataReadSet();

        for (const auto &[cce_addr, read_entry_pair] : rset)
        {
            CcReqStatus ret = cc_handler_->PostRead(
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                0,
                0,
                0,
                cce_addr,
                post_process.hd_result_);
            update_post_cnt(ret);
        }
    }

    if (metrics::enable_remote_request_metrics &&
        !post_process.hd_result_.Value().is_local_)
    {
        auto meter = tx_processor_->GetMeter();
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Increment,
                       "post_process");
        if (is_collecting_duration_round_)
        {
            post_process.op_start_ = metrics::Clock::now();
        }
    }

    if (post_local_cnt == 0 && post_remote_cnt == 0)
    {
        // If there are no pending post read/write ops, advance the command Id
        // to forward the tx.
        AdvanceCommand();
    }
    else if (post_remote_cnt > 0)
    {
        StartTiming();
    }
}

void TransactionExecution::PostProcess(PostProcessOp &post_process)
{
    if (metrics::enable_tx_metrics)
    {
        auto meter = tx_processor_->GetMeter();
        if (metrics::enable_remote_request_metrics &&
            !post_process.hd_result_.Value().is_local_)
        {
            if (is_collecting_duration_round_)
            {
                meter->CollectDuration(metrics::NAME_REMOTE_REQUEST_DURATION,
                                       post_process.op_start_,
                                       "post_process");
            }
            meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                           metrics::Value::IncDecValue::Decrement,
                           "post_process");
        }
        if (is_collecting_duration_round_)
        {
            meter->CollectDuration(metrics::NAME_TX_DURATION,
                                   tx_duration_start_);
        }
        meter->Collect(metrics::NAME_TX_PROCESSED_TOTAL, 1);
    }

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &post_process,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &post_process);
        state_stack_.pop_back();
    }
    assert(state_stack_.empty());

    if (post_process.forward_to_update_txn_op_)
    {
        PushOperation(&update_txn_);
        Process(update_txn_);
    }
    else
    {
        // For recover tx commit, commit_ts is already determined and
        // we don't need to update TEntry since it is not assigned for
        // a recovering tx.

        if (bool_resp_ != nullptr && bool_resp_ != &commit_tx_req_->tx_result_)
        {
            if (tx_status_.load(std::memory_order_relaxed) ==
                TxnStatus::Committed)
            {
                bool_resp_->Finish(true);
            }
            else
            {
                bool_resp_->Finish(false);
            }
            bool_resp_ = nullptr;
        }
        else if (rec_resp_ != nullptr)
        {
            // auto committed ObjectCommandTxRequest
            rec_resp_->Finish(obj_cmd_.hd_result_.Value().rec_status_);
            rec_resp_ = nullptr;
        }
        else if (vct_rec_resp_ != nullptr)
        {
            // auto committed MultiObjectCommandTxRequest
            std::vector<RecordStatus> vct_rec;
            vct_rec.reserve(multi_obj_cmd_.vct_hd_result_.size());
            for (const auto &hresult : multi_obj_cmd_.vct_hd_result_)
            {
                vct_rec.push_back(hresult.Value().rec_status_);
            }

            vct_rec_resp_->Finish(std::move(vct_rec));
            vct_rec_resp_ = nullptr;
        }
        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);

        Reset();
    }
    post_process.Reset(0, 0, 0, 0, true);
}

void TransactionExecution::Process(AcquireAllOp &acq_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &acq_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    auto all_node_groups = Sharder::Instance().AllNodeGroups();
    acq_all_op.Reset(all_node_groups->size());
    acq_all_op.is_running_ = true;
    size_t hd_idx = 0;

    for (uint32_t ng_id : *all_node_groups)
    {
        for (uint32_t key_idx = 0; key_idx < acq_all_op.keys_.size(); key_idx++)
        {
            CcHandlerResult<AcquireAllResult> &hres =
                acq_all_op.hd_results_[hd_idx++];
            hres.Reset();
            hres.Value().remote_ack_cnt_ = &acq_all_op.remote_ack_cnt_;
            cc_handler_->AcquireWriteAll(
                *acq_all_op.table_name_,
                acq_all_op.keys_[key_idx],
                ng_id,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                false,
                hres,
                acq_all_op.protocol_,
                acq_all_op.cc_op_);
        }
    }
    StartTiming();
}

void TransactionExecution::PostProcess(AcquireAllOp &acq_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &acq_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
}

void TransactionExecution::Process(PostWriteAllOp &post_write_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &post_write_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    auto all_node_groups = Sharder::Instance().AllNodeGroups();
    post_write_all_op.Reset(all_node_groups->size());
    post_write_all_op.is_running_ = true;

    for (uint32_t ngid : *all_node_groups)
    {
        if (TxCcNodeId() == ngid)
        {
            // Send out local request at last to prevent it from
            // modifying recs_ while the handler is still using it.
            continue;
        }
        for (uint32_t keyid = 0; keyid < post_write_all_op.keys_.size();
             ++keyid)
        {
            cc_handler_->PostWriteAll(
                *post_write_all_op.table_name_,
                post_write_all_op.keys_[keyid],
                *post_write_all_op.recs_[keyid],
                ngid,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                commit_ts_,
                post_write_all_op.hd_result_,
                post_write_all_op.op_type_,
                post_write_all_op.write_type_);
        }
    }

    for (uint32_t keyid = 0; keyid < post_write_all_op.keys_.size(); ++keyid)
    {
        cc_handler_->PostWriteAll(*post_write_all_op.table_name_,
                                  post_write_all_op.keys_[keyid],
                                  *post_write_all_op.recs_[keyid],
                                  TxCcNodeId(),
                                  tx_number_.load(std::memory_order_relaxed),
                                  tx_term_,
                                  command_id_.load(std::memory_order_relaxed),
                                  commit_ts_,
                                  post_write_all_op.hd_result_,
                                  post_write_all_op.op_type_,
                                  post_write_all_op.write_type_);
    }
    StartTiming();
}

void TransactionExecution::PostProcess(PostWriteAllOp &post_write_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &post_write_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();

    // So far, post-write-all is only used for schema evolution operations.
    assert(!state_stack_.empty());
}

void TransactionExecution::ReleaseMetaDataReadLock(
    CcHandlerResult<PostProcessResult> &meta_data_hd_result)
{
    const absl::flat_hash_map<CcEntryAddr,
                              std::pair<ReadSetEntry, const std::string_view>>
        &rset = rw_set_.MetaDataReadSet();
    size_t ref_cnt = meta_data_hd_result.RefCnt();

    size_t post_local_cnt = 0;
    for (const auto &[cce_addr, read_entry_pair] : rset)
    {
        const ReadSetEntry &read_entry = read_entry_pair.first;
        CcReqStatus ret = cc_handler_->PostRead(TxNumber(),
                                                TxTerm(),
                                                CommandId(),
                                                read_entry.version_ts_,
                                                0,
                                                commit_ts_,
                                                cce_addr,
                                                meta_data_hd_result,
                                                true);
        --ref_cnt;

        if (ret == CcReqStatus::SentLocal)
        {
            ++post_local_cnt;
        }
        else
        {
            // Meta-data requests are always toward to a local shard.
            assert(ret != CcReqStatus::SentRemote);
        }
    }

    assert(ref_cnt == 0);
    if (post_local_cnt == 0)
    {
        AdvanceCommand();
    }
}

void TransactionExecution::ReleaseCatalogWriteAll(
    CcHandlerResult<PostProcessResult> &catalog_post_all_hd_result)
{
    uint64_t commit_ts = TxStatus() == TxnStatus::Committed
                             ? commit_ts_
                             : TransactionOperation::tx_op_failed_ts_;

    std::shared_ptr<std::set<uint32_t>> all_node_groups =
        Sharder::Instance().AllNodeGroups();
    for (uint32_t ngid : *all_node_groups)
    {
        if (TxCcNodeId() == ngid)
        {
            // Send out local request at last to prevent it from
            // modifying recs_ while the handler is still using it.
            continue;
        }
        for (const auto &[write_key, write_entry] : rw_set_.CatalogWriteSet())
        {
            assert(write_entry.op_ == OperationType::Update);
            cc_handler_->PostWriteAll(
                catalog_ccm_name,
                write_key,
                *write_entry.rec_,
                ngid,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                commit_ts,
                catalog_post_all_hd_result,
                OperationType::Update,
                PostWriteType::PostCommit);
        }
    }
    for (const auto &[write_key, write_entry] : rw_set_.CatalogWriteSet())
    {
        assert(write_entry.op_ == OperationType::Update);
        cc_handler_->PostWriteAll(catalog_ccm_name,
                                  write_key,
                                  *write_entry.rec_,
                                  TxCcNodeId(),
                                  tx_number_.load(std::memory_order_relaxed),
                                  tx_term_,
                                  command_id_.load(std::memory_order_relaxed),
                                  commit_ts,
                                  catalog_post_all_hd_result,
                                  OperationType::Update,
                                  PostWriteType::PostCommit);
    }

    StartTiming();
}

void TransactionExecution::DrainScanner(CcScanner *scanner,
                                        const TableName &table_name)
{
    assert(scanner != nullptr);
    // TODO(lokax):
    // drain out the scan tuple in the scan cache
    // scanner->SetDrainCacheMode(true);
    const ScanTuple *cc_scan_tuple = scanner->Current();
    // In case the scan status is blocked before
    if (cc_scan_tuple == nullptr && scanner->Status() == ScannerStatus::Blocked)
    {
        scanner->MoveNext();
        cc_scan_tuple = scanner->Current();
    }

    while (cc_scan_tuple != nullptr)
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "PostProcess.ScanOperation.AddReadSet.cce_ptr",
            &rw_set_,
            (
                [this, cc_scan_tuple]() -> std::string
                {
                    return std::string("\"tx_number\":")
                        .append(std::to_string(this->TxNumber()))
                        .append(",\"tx_term\":")
                        .append(std::to_string(this->tx_term_))
                        .append(",\"cce_lock_ptr\":")
                        .append(std::to_string(
                            cc_scan_tuple->cce_addr_.CceLockPtr()));
                }));

        LockType scan_tuple_lock_type =
            scanner->DeduceScanTupleLockType(cc_scan_tuple->rec_status_);
        // "key_ts_ == 0", means the lock is added on gap. Now, gap lock is
        // not used when do scan operation.
        if (scan_tuple_lock_type != LockType::NoLock &&
            !cc_scan_tuple->cce_addr_.Empty() && cc_scan_tuple->key_ts_ != 0 &&
            rw_set_.RemoveDataReadEntry(table_name, cc_scan_tuple->cce_addr_) ==
                0)
        {
            if (cc_scan_tuple->rec_status_ == RecordStatus::Unknown)
            {
                // Only used to release lock.
                drain_batch_.emplace_back(cc_scan_tuple->cce_addr_, 0);
            }
            else
            {
                drain_batch_.emplace_back(cc_scan_tuple->cce_addr_,
                                          cc_scan_tuple->key_ts_);
            }

            if (drain_batch_.back().first.CceLockPtr() == 0)
            {
                LOG(INFO) << "== empty lock ptr";
            }
        }
        scanner->MoveNext();
        cc_scan_tuple = scanner->Current();
    }
}

void TransactionExecution::Process(DsUpsertTableOp &ds_upsert_table_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ds_upsert_table_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    ds_upsert_table_op.Reset();
    ds_upsert_table_op.is_running_ = true;
    if (txservice_skip_kv)
    {
        ds_upsert_table_op.hd_result_.SetFinished();
        PostProcess(ds_upsert_table_op);
        return;
    }

    cc_handler_->DataStoreUpsertTable(ds_upsert_table_op.table_schema_old_,
                                      ds_upsert_table_op.table_schema_,
                                      ds_upsert_table_op.op_type_,
                                      ds_upsert_table_op.write_time_,
                                      TxCcNodeId(),
                                      TxTerm(),
                                      ds_upsert_table_op.hd_result_,
                                      ds_upsert_table_op.alter_table_info_);
}

void TransactionExecution::PostProcess(DsUpsertTableOp &ds_upsert_table_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ds_upsert_table_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(!state_stack_.empty());
}

void TransactionExecution::Process(ReloadCacheOperation &reload_cache_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &reload_cache_op_,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    reload_cache_op_.is_running_ = true;

    auto all_node_groups = Sharder::Instance().AllNodeGroups();

    for (NodeGroupId ng_id : *all_node_groups)
    {
        cc_handler_->ReloadCache(ng_id,
                                 TxNumber(),
                                 TxTerm(),
                                 CommandId(),
                                 reload_cache_op.hd_result_);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(ReloadCacheOperation &reload_cache_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &reload_cache_op_,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (reload_cache_op.hd_result_.IsError())
    {
        DLOG(INFO) << "ReloadCacheOperation FinishError for cc error: "
                   << reload_cache_op.hd_result_.ErrorMsg();
        void_resp_->FinishError(
            ConvertCcError(reload_cache_op.hd_result_.ErrorCode()));
    }
    else
    {
        void_resp_->Finish(void_);
    }

    void_resp_ = nullptr;
    reload_cache_op.Reset(0);
}

void TransactionExecution::Process(FaultInjectOp &fault_inject_op_)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &fault_inject_op_,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    fault_inject_op_.Reset();
    fault_inject_op_.is_running_ = true;

    cc_handler_->FaultInject(fault_inject_op_.fault_name_,
                             fault_inject_op_.fault_paras_,
                             tx_term_,
                             command_id_.load(std::memory_order_relaxed),
                             txid_,
                             fault_inject_op_.vct_node_id_,
                             fault_inject_op_.hd_result_);
    StartTiming();
}

void TransactionExecution::PostProcess(FaultInjectOp &fault_inject_op_)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &fault_inject_op_,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    bool_resp_->Finish(fault_inject_op_.succeed_);
    bool_resp_ = nullptr;
    fault_inject_op_.Reset();
}

void TransactionExecution::ProcessTxRequest(
    CleanCcEntryForTestTxRequest &clean_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &clean_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    bool_resp_ = &clean_req.tx_result_;

    clean_entry_op_.Set(clean_req.tab_name_,
                        clean_req.key_,
                        clean_req.only_archives_,
                        clean_req.flush_);
    PushOperation(&clean_entry_op_);
    Process(clean_entry_op_);
}

void TransactionExecution::Process(CleanCcEntryForTestOp &clean_entry_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &clean_entry_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    clean_entry_op_.Reset();
    clean_entry_op_.is_running_ = true;

    cc_handler_->CleanCcEntryForTest(
        *clean_entry_op_.tab_name_,
        *clean_entry_op_.key_,
        clean_entry_op_.only_archives_,
        clean_entry_op_.flush_,
        tx_number_.load(std::memory_order_relaxed),
        tx_term_,
        command_id_.load(std::memory_order_relaxed),
        clean_entry_op_.hd_result_);
    return;
}

void TransactionExecution::PostProcess(CleanCcEntryForTestOp &clean_entry_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &clean_entry_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    bool_resp_->Finish(clean_entry_op.succeed_);
    bool_resp_ = nullptr;
    clean_entry_op.Reset();
}

void TransactionExecution::Process(AnalyzeTableAllOp &analyze_table_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &analyze_table_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    analyze_table_all_op.is_running_ = true;

    auto all_node_groups = Sharder::Instance().AllNodeGroups();

    for (NodeGroupId ng_id : *all_node_groups)
    {
        cc_handler_->AnalyzeTableAll(
            *analyze_table_all_op.analyze_tx_req_->table_name_,
            ng_id,
            TxNumber(),
            TxTerm(),
            CommandId(),
            analyze_table_all_op.hd_result_);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(AnalyzeTableAllOp &analyze_table_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &analyze_table_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (analyze_table_all_op.hd_result_.IsError())
    {
        DLOG(INFO) << "AnalyzeTableAllOp FinishError for cc error: "
                   << analyze_table_all_op.hd_result_.ErrorMsg();
        void_resp_->FinishError(
            ConvertCcError(analyze_table_all_op.hd_result_.ErrorCode()));
        void_resp_ = nullptr;
    }
    else
    {
        DLOG(INFO) << "txm notifies analyze tx request ";
        void_resp_->Finish(void_);
        void_resp_ = nullptr;
    }
    analyze_table_all_op.Reset(0);
}

void TransactionExecution::Process(BroadcastStatisticsOp &broadcast_stat_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &broadcast_stat_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    broadcast_stat_op.is_running_ = true;

    auto all_node_groups = Sharder::Instance().AllNodeGroups();

    const BroadcastStatisticsTxRequest *req =
        broadcast_stat_op.broadcast_tx_req_;
    for (NodeGroupId ng_id : *all_node_groups)
    {
        if (ng_id != req->sample_pool_->ng_id())
        {
            cc_handler_->BroadcastStatistics(*req->table_name_,
                                             req->schema_ts_,
                                             *req->sample_pool_,
                                             ng_id,
                                             TxNumber(),
                                             TxTerm(),
                                             CommandId(),
                                             broadcast_stat_op.hd_result_);
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(BroadcastStatisticsOp &broadcast_stat_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &broadcast_stat_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (broadcast_stat_op.hd_result_.IsError())
    {
        DLOG(INFO) << "BroadcastStatisticsOp FinishError for cc error: "
                   << broadcast_stat_op.hd_result_.ErrorMsg();
        void_resp_->FinishError(
            ConvertCcError(broadcast_stat_op.hd_result_.ErrorCode()));
        void_resp_ = nullptr;
    }
    else
    {
        DLOG(INFO) << "txm notifies broadcast tx request ";
        void_resp_->Finish(void_);
        void_resp_ = nullptr;
    }
    broadcast_stat_op.Reset(0);
}

template <typename ResultType>
void TransactionExecution::Process(AsyncOp<ResultType> &ds_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ds_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    ds_op.hd_result_.Reset();
    ds_op.is_running_ = true;

    if (ds_op.op_func_ != nullptr)
    {
        ds_op.op_func_(ds_op);
    }
    StartTiming();
}

template void TransactionExecution::Process(AsyncOp<Void> &ds_op);
template void TransactionExecution::Process(AsyncOp<PostProcessResult> &ds_op);
template void TransactionExecution::Process(
    AsyncOp<GenerateSkParallelResult> &generate_sk_parallel_op);

template <typename ResultType>
void TransactionExecution::PostProcess(AsyncOp<ResultType> &ds_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ds_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    assert(ds_op.hd_result_.IsFinished());
    state_stack_.pop_back();
}

template void TransactionExecution::PostProcess(AsyncOp<Void> &ds_op);
template void TransactionExecution::PostProcess(
    AsyncOp<PostProcessResult> &ds_op);
template void TransactionExecution::PostProcess(
    AsyncOp<GenerateSkParallelResult> &generate_sk_parallel_op);

void TransactionExecution::Process(NoOp &no_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &no_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    no_op.is_running_ = true;
    no_op.hd_result_.SetFinished();
}

void TransactionExecution::PostProcess(NoOp &no_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &no_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
}

void TransactionExecution::Process(PostReadOperation &post_read_operation)
{
    post_read_operation.is_running_ = true;
    CcReqStatus ret =
        cc_handler_->PostRead(tx_number_.load(std::memory_order_relaxed),
                              this->tx_term_,
                              command_id_.load(std::memory_order_relaxed),
                              post_read_operation.read_set_entry_->version_ts_,
                              0,
                              commit_ts_,
                              *post_read_operation.cce_addr_,
                              post_read_operation.hd_result_);
    if (ret == CcReqStatus::Processed)
    {
        AdvanceCommand();
    }
    else if (ret == CcReqStatus::SentRemote)
    {
        StartTiming();
    }
}

void TransactionExecution::PostProcess(PostReadOperation &post_read_operation)
{
    rw_set_.DedupRead(*post_read_operation.cce_addr_);
    state_stack_.pop_back();
}

void TransactionExecution::Process(ReleaseScanExtraLockOp &unlock_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &unlock_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    unlock_op.is_running_ = true;

    if (drain_batch_.size() == 0)
    {
        unlock_op.hd_result_.SetFinished();
    }
    else
    {
        unlock_op.hd_result_.SetRefCnt((uint32_t) drain_batch_.size());
    }

    size_t post_local_cnt = 0, post_remote_cnt = 0;
    for (const auto &addr_pair : drain_batch_)
    {
        CcReqStatus ret = cc_handler_->PostRead(TxNumber(),
                                                TxTerm(),
                                                CommandId(),
                                                addr_pair.second,
                                                0,
                                                commit_ts_,
                                                addr_pair.first,
                                                unlock_op.hd_result_,
                                                false,
                                                false);
        if (ret == CcReqStatus::SentLocal)
        {
            ++post_local_cnt;
        }
        else if (ret == CcReqStatus::SentRemote)
        {
            ++post_remote_cnt;
        }
    }

    if (post_local_cnt == 0 && post_remote_cnt == 0)
    {
        AdvanceCommand();
    }
    else if (post_remote_cnt > 0)
    {
        StartTiming();
    }
}

void TransactionExecution::PostProcess(ReleaseScanExtraLockOp &unlock_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &lock_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    drain_batch_.clear();
    // The lock_op step is optional. Only pops the stack if the last step
    // is the validation step.
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &unlock_op);
        state_stack_.pop_back();
    }
    unlock_op.Reset();
}

void TransactionExecution::Process(KickoutDataOp &kickout_data_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &kickout_data_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    kickout_data_op.is_running_ = true;
    kickout_data_op.hd_result_.Reset();
    kickout_data_op.hd_result_.SetRefCnt(1);
    cc_handler_->KickoutData(*kickout_data_op.table_name_,
                             kickout_data_op.node_group_,
                             tx_number_.load(std::memory_order_relaxed),
                             tx_term_,
                             command_id_.load(std::memory_order_relaxed),
                             kickout_data_op.hd_result_,
                             kickout_data_op.clean_type_,
                             kickout_data_op.bucket_ids_,
                             &kickout_data_op.start_key_,
                             &kickout_data_op.end_key_,
                             kickout_data_op.clean_ts_,
                             kickout_data_op.range_id_,
                             kickout_data_op.range_version_);
}

void TransactionExecution::PostProcess(KickoutDataOp &kickout_data_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &kickout_data_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
        });
    state_stack_.pop_back();
}

ScanCloseTxRequest *TransactionExecution::NextScanCloseTxReq(
    uint64_t alias, const TableName &table_name)
{
    size_t pool_size = scan_close_req_pool_->Size();
    for (size_t idx = 0; idx < pool_size; ++idx)
    {
        std::unique_ptr<ScanCloseTxRequest> scan_close_req =
            std::move(scan_close_req_pool_->Peek());
        scan_close_req_pool_->Dequeue();

        ScanCloseTxRequest *req = scan_close_req.get();
        scan_close_req_pool_->Enqueue(std::move(scan_close_req));

        if (!req->in_use_.load(std::memory_order_relaxed))
        {
            req->Reset(alias, table_name);
            return req;
        }
    }

    std::unique_ptr<ScanCloseTxRequest> scan_close_req =
        std::make_unique<ScanCloseTxRequest>(alias, table_name);
    ScanCloseTxRequest *req = scan_close_req.get();
    scan_close_req_pool_->Enqueue(std::move(scan_close_req));

    return req;
}

void TransactionExecution::Process(ObjectCommandOp &obj_cmd_op)
{
    const TxKey &key = *obj_cmd_op.key_;
    uint32_t key_shard_code = 0;

    int db_idx = GetDbIndex(obj_cmd_op.table_name_);

    if (!obj_cmd_op.is_running_)
    {
        if (FLAGS_cmd_read_catalog && !obj_cmd_op.catalog_read_success_)
        {
            if (locked_db_[db_idx].first == nullptr)
            {
                LocalCcHandler *local_hd =
                    static_cast<LocalCcHandler *>(cc_handler_);

                uint32_t ng_id = TxCcNodeId();
                int64_t ng_term = TxTerm();

                auto [err_code, lock_struct, schema_version] =
                    local_hd->ReadCatalog(
                        *obj_cmd_op.table_name_, ng_id, ng_term, TxNumber());
                if (err_code == CcErrorCode::NO_ERROR)
                {
                    assert(lock_struct != nullptr);
                    assert(schema_version > 0);
                    locked_db_[db_idx].first = lock_struct;
                    locked_db_[db_idx].second = schema_version;
                    obj_cmd_op.catalog_read_success_ = true;
                }
                else if (err_code == CcErrorCode::READ_CATALOG_FAIL)
                {
                    obj_cmd_op.is_running_ = false;
                    // Read and lock the catalog through read_catalog_op_.
                    read_catalog_result_.Reset();
                    read_catalog_result_.Value().Reset();
                    read_catalog_key_ = CatalogKey(*obj_cmd_op.table_name_);
                    read_catalog_record_ = CatalogRecord{};

                    read_catalog_op_.Reset(TableName(catalog_ccm_name_sv.data(),
                                                     catalog_ccm_name_sv.size(),
                                                     TableType::Catalog,
                                                     TableEngine::None),
                                           &catalog_tx_key_,
                                           &read_catalog_record_,
                                           &read_catalog_result_);

                    // Control flow jumps to read_catalog_op_, do not execute
                    // further after `Process(read_catalog_op_)` returns.
                    PushOperation(&read_catalog_op_);
                    Process(read_catalog_op_);
                    return;
                }
                else
                {
                    DLOG(WARNING) << "Command read catalog fails, return error";

                    obj_cmd_op.hd_result_.SetError(
                        CcErrorCode::READ_CATALOG_CONFLICT);
                    PostProcess(obj_cmd_op);
                    return;
                }
            }
            else
            {
                // the schema version of this db will not change during the
                // transaction. So locked_db_[db_idx].second remains valid.
                obj_cmd_op.catalog_read_success_ = true;
            }
            assert(locked_db_[db_idx].second > 0);
        }

        // Make sure current node is still ng leader since we may visit bucket
        // info meta data here which is only valid when current node is still ng
        // leader.
        if (!CheckLeaderTerm() && !CheckStandbyTerm())
        {
            obj_cmd_op.hd_result_.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            PostProcess(obj_cmd_op);
            return;
        }

        uint64_t key_hash = key.Hash();
        uint32_t residual = key_hash & 0x3FF;
        uint16_t bucket_id = Sharder::MapKeyHashToBucketId(key_hash);
        const BucketInfo *bucket_info = FastToGetBucket(bucket_id);
        if (bucket_info != nullptr)
        {
            // Uses the lower 10 bits of the key's hash code to shard the
            // key across CPU cores in a cc node.
            NodeGroupId bucket_ng = bucket_info->BucketOwner();
            key_shard_code = bucket_ng << 10 | residual;

            // If current bucket is migrating, forward to new bucket owner.
            NodeGroupId new_bucket_ng = bucket_info->DirtyBucketOwner();
            if (new_bucket_ng != UINT32_MAX &&
                !obj_cmd_op.command_->IsReadOnly())
            {
                obj_cmd_op.forward_key_shard_ = new_bucket_ng << 10 | residual;
            }
        }
        else if (lock_range_bucket_result_.IsFinished())
        {
            // If there is an error when getting the key's bucket owner, the
            // error would be caught when forwarding the operation, which forces
            // the tx state machine to move to post-processing of the operation
            // and returns an error to the ObjectCommandTxRequest.
            assert(!lock_range_bucket_result_.IsError());

            const BucketInfo *bucket_info = bucket_rec_.GetBucketInfo();
            // Cache the locked bucket info for read directly at next.
            uint16_t bucket_id = bucket_key_.BucketId();
            locked_buckets_.emplace(bucket_id, bucket_info);

            // Uses the lower 10 bits of the key's hash code to shard the
            // key across CPU cores in a cc node.
            NodeGroupId bucket_ng = bucket_info->BucketOwner();
            key_shard_code = bucket_ng << 10 | residual;

            // If current bucket is migrating, forward to new bucket owner.
            NodeGroupId new_bucket_ng = bucket_info->DirtyBucketOwner();
            if (new_bucket_ng != UINT32_MAX &&
                !obj_cmd_op.command_->IsReadOnly())
            {
                obj_cmd_op.forward_key_shard_ = new_bucket_ng << 10 | residual;
            }
        }
        else
        {
            obj_cmd_op.is_running_ = false;
            // First read and lock the bucket the key located in through
            // lock_bucket_op_.
            lock_range_bucket_result_.Reset();
            lock_range_bucket_result_.Value().Reset();
            bucket_key_.Reset(bucket_id);

            lock_bucket_op_.Reset(TableName(range_bucket_ccm_name_sv.data(),
                                            range_bucket_ccm_name_sv.size(),
                                            TableType::RangeBucket,
                                            TableEngine::None),
                                  &bucket_tx_key_,
                                  &bucket_rec_,
                                  &lock_range_bucket_result_);

            // Control flow jumps to lock_bucket_op_, do not execute further
            // after `Process(lock_bucket_op_)` returns.
            PushOperation(&lock_bucket_op_);
            Process(lock_bucket_op_);
            return;
        }
    }

    obj_cmd_op.is_running_ = true;

    uint64_t current_ts =
        static_cast<LocalCcHandler *>(cc_handler_)->GetTsBaseValue();

    CcHandlerResult<ObjectCommandResult> &hd_res = obj_cmd_op.hd_result_;
    hd_res.Reset();
#ifdef EXT_TX_PROC_ENABLED
    // Pass the resume func to cc handler result. So that the tx request sender
    // will be resumed once the cc request finishes, on contrary to after the
    // txm be forwarded in next RunOneRound.
    const std::function<void()> *tx_resume_func =
        rec_resp_->ReleaseResumeFunc();
    if (tx_resume_func != nullptr)
    {
        hd_res.runtime_resume_func_ = tx_resume_func;
    }
#endif

    // Directly commit the new value to the object if autocommit and skip_wal
    // are both set, on contrary to acquiring lock and committing the command in
    // postprocess.
    bool commit = obj_cmd_op.auto_commit_ && txservice_skip_wal;
    cc_handler_->ObjectCommand(*obj_cmd_op.table_name_,
                               locked_db_[db_idx].second,
                               *obj_cmd_op.key_,
                               key_shard_code,
                               *obj_cmd_op.command_,
                               TxNumber(),
                               tx_term_,
                               command_id_.load(std::memory_order_relaxed),
                               current_ts,
                               hd_res,
                               iso_level_,
                               protocol_,
                               commit,
                               obj_cmd_op.always_redirect_);

    StartTiming();
}

void TransactionExecution::PostProcess(ObjectCommandOp &obj_cmd_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &obj_cmd_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    const CcHandlerResult<ObjectCommandResult> &hd_result =
        obj_cmd_op.hd_result_;
    if (hd_result.IsError())
    {
        rec_resp_->FinishError(ConvertCcError(hd_result.ErrorCode()));
        rec_resp_ = nullptr;
        if (obj_cmd_op.auto_commit_)
        {
            Abort();
        }
    }
    else
    {
        const ObjectCommandResult &cmd_result = hd_result.Value();
        RecordStatus obj_status = cmd_result.rec_status_;
        LockType lock_acquired = cmd_result.lock_acquired_;
        bool object_modified = cmd_result.object_modified_;
        const TxCommand *cmd = obj_cmd_op.command_;
        const TableName *table_name = obj_cmd_op.table_name_;
        const CcEntryAddr &cce_addr = cmd_result.cce_addr_;
        uint64_t commit_ts = cmd_result.commit_ts_;
        uint64_t lock_ts = cmd_result.lock_ts_;
        uint64_t last_vali_ts = cmd_result.last_vali_ts_;
        uint64_t ttl = cmd_result.ttl_;
        bool ttl_expired = cmd_result.ttl_expired_;
        bool ttl_reset = cmd_result.ttl_reset_;

        // if (obj_cmd_op.auto_commit_ && txservice_skip_wal)
        //{
        // assert(lock_acquired == LockType::NoLock);
        //}

        assert(obj_status != RecordStatus::Unknown);
        bool version_changed = false;
        if (lock_acquired == LockType::WriteLock)
        {
            // If the cce is expired before the command is applyed
            // Add a retire command before the write command
            if (ttl_expired)
            {
                auto retire_command =
                    obj_cmd_.command_->RetireExpiredTTLObjectCommand();
                cmd_set_.AddObjectCommand(*table_name,
                                          cce_addr,
                                          obj_status,
                                          commit_ts,
                                          lock_ts,
                                          last_vali_ts,
                                          obj_cmd_.key_,
                                          retire_command.get(),
                                          ttl,
                                          obj_cmd_op.forward_key_shard_);
            }
            if (ttl_reset)
            {
                // write a recover obj cmd to log
                // If ttl is reset, we did not record ttl in object result since
                // recover ttl object command is a overwrite command, so
                // commands in this tx can always be successfully replayed. We
                // do not need to write post update ttl in this case.
                assert(ttl == UINT64_MAX);
                auto recover_command =
                    obj_cmd_.command_->RecoverTTLObjectCommand();
                cmd_set_.AddObjectCommand(*table_name,
                                          cce_addr,
                                          obj_status,
                                          commit_ts,
                                          lock_ts,
                                          last_vali_ts,
                                          obj_cmd_.key_,
                                          recover_command,
                                          ttl,
                                          obj_cmd_op.forward_key_shard_);
            }

            // The command modifies the object. Put it into the command set
            // for writing log and post-processing. If the command fails, only
            // to release the write lock.
            cmd_set_.AddObjectCommand(
                *table_name,
                cce_addr,
                obj_status,
                commit_ts,
                lock_ts,
                last_vali_ts,
                obj_cmd_op.key_,
                object_modified ? obj_cmd_op.command_ : nullptr,
                ttl,
                obj_cmd_op.forward_key_shard_);

            uint64_t read_version = rw_set_.DedupRead(cce_addr);
            if (read_version > 0 && read_version != cmd_result.commit_ts_)
            {
                // Each write-set key acquires a write lock and gets the
                // key's last validation ts and commit ts. If the write
                // key has been read before and the key's commit ts
                // mismatches the prior version, this is not a
                // repeatable read.

                LOG(WARNING)
                    << "set rset_has_expired_, txn: " << TxNumber()
                    << "; read_version: " << read_version
                    << "; cmd_result.commit_ts_: " << cmd_result.commit_ts_;
                version_changed = true;
            }
        }
        else if (lock_acquired != LockType::NoLock &&
                 !cmd_set_.FindObjectCommand(*table_name, cce_addr))
        {
            // Read lock is acquired under locking protocol. Add the cce to
            // read set for later PostRead.
            bool add_res = rw_set_.AddRead(cce_addr, commit_ts, table_name);
            if (!add_res)
            {
                // Add read set fail, there is at least two unmatched read. This
                // can't be autocommit request.
                assert(!obj_cmd_op.auto_commit_);
                version_changed = true;
            }
        }

        if (version_changed)
        {
            rec_resp_->FinishError(TxErrorCode::OCC_BREAK_REPEATABLE_READ);
            rec_resp_ = nullptr;
            if (obj_cmd_op.auto_commit_)
            {
                Abort();
            }
            obj_cmd_op.Reset(nullptr, nullptr, nullptr);
            return;
        }

        // The command is directly executed and committed on the object within
        // ApplyCc if autocommit and skip wal are both set. In such case, there
        // is no need to write log and do post write, and no need to add command
        // into write set.
        // For autocommit read-modify-write commands, the ObjectCommandTxRequest
        // sender will be notified after auto commit succeeds, i.e. after
        // PostProcess or WriteLog.
        bool already_committed = obj_cmd_op.auto_commit_ && txservice_skip_wal;

        // Whether we should notify the request sender.
        if (!obj_cmd_op.auto_commit_ || already_committed || cmd->IsReadOnly())
        {
            // Not autocommit, or autocommit and skip wal, or autocommit and
            // this is a read only command. Notify the ObjectCommandTxRequest
            // sender once the command finishes.
            rec_resp_->Finish(obj_status);
            rec_resp_ = nullptr;
        }

        if (already_committed && rw_set_.MetaDataReadSetSize() == 0)
        {
            // The command has already committed, just reset the txm.
            assert(rw_set_.DataReadSetSize() == 0);
            assert(rw_set_.WriteSetSize() == 0);
            assert(cmd_set_.ObjectCntWithWriteLock() == 0);
            tx_status_.store(TxnStatus::Finished, std::memory_order_relaxed);
            cc_handler_->UpdateTxnStatus(
                txid_, iso_level_, TxnStatus::Finished, update_txn_.hd_result_);
            Reset();
            return;
        }

        // Whether we should auto commit the txn. For autocommit commands that
        // need to write log, the request sender will be notified after WriteLog
        // and PostProcess.
        if (obj_cmd_op.auto_commit_)
        {
            Commit();
        }
    }
}

void TransactionExecution::Process(MultiObjectCommandOp &obj_cmd_op)
{
    MultiObjectCommandTxRequest *req = obj_cmd_op.tx_req_;
    const std::vector<TxKey> *vct_key = req->VctKey();
    const std::vector<TxCommand *> *vct_cmd = req->VctCommand();

    int db_idx = GetDbIndex(req->table_name_);

    if (!obj_cmd_op.is_running_)
    {
        if (FLAGS_cmd_read_catalog && !obj_cmd_op.catalog_read_success_)
        {
            assert(db_idx >= 0 && db_idx < 16);
            if (locked_db_[db_idx].first == nullptr)
            {
                LocalCcHandler *local_hd =
                    static_cast<LocalCcHandler *>(cc_handler_);

                uint32_t ng_id = TxCcNodeId();
                int64_t ng_term = TxTerm();

                auto [err_code, lock_struct, schema_version] =
                    local_hd->ReadCatalog(
                        *req->table_name_, ng_id, ng_term, TxNumber());
                if (err_code == CcErrorCode::NO_ERROR)
                {
                    assert(lock_struct != nullptr);
                    locked_db_[db_idx].first = lock_struct;
                    locked_db_[db_idx].second = schema_version;
                    obj_cmd_op.catalog_read_success_ = true;
                }
                else if (err_code == CcErrorCode::READ_CATALOG_FAIL)
                {
                    obj_cmd_op.is_running_ = false;
                    // Read and lock the catalog through read_catalog_op_.
                    read_catalog_result_.Reset();
                    read_catalog_result_.Value().Reset();
                    read_catalog_key_ = CatalogKey(*req->table_name_);
                    read_catalog_record_ = CatalogRecord{};

                    read_catalog_op_.Reset(TableName(catalog_ccm_name_sv.data(),
                                                     catalog_ccm_name_sv.size(),
                                                     TableType::Catalog,
                                                     TableEngine::None),
                                           &catalog_tx_key_,
                                           &read_catalog_record_,
                                           &read_catalog_result_);

                    // Control flow jumps to read_catalog_op_, do not execute
                    // further after `Process(read_catalog_op_)` returns.
                    PushOperation(&read_catalog_op_);
                    Process(read_catalog_op_);
                    return;
                }
                else
                {
                    assert(err_code == CcErrorCode::READ_CATALOG_CONFLICT);
                    DLOG(WARNING) << "MultiObjectCommand read catalog "
                                     "conflicts, return error";

                    obj_cmd_op.atm_err_code_.store(
                        CcErrorCode::READ_CATALOG_CONFLICT,
                        std::memory_order_relaxed);
                    PostProcess(obj_cmd_op);
                    return;
                }
            }
            else
            {
                obj_cmd_op.catalog_read_success_ = true;
            }
        }
    }

    // Make sure current node is still ng leader since we may visit bucket info
    // meta data here which is only valid when current node is still ng leader.
    if (!CheckLeaderTerm() && !CheckStandbyTerm())
    {
        obj_cmd_op.lock_bucket_result_->SetError(
            CcErrorCode::TX_NODE_NOT_LEADER);
        PostProcess(obj_cmd_op);
        return;
    }

    std::vector<std::pair<uint32_t, uint32_t>> &vct_key_shard_code =
        obj_cmd_op.vct_key_shard_code_;
    assert(vct_key_shard_code.size() == vct_key->size());
    if (lock_range_bucket_result_.IsFinished())
    {
        const BucketInfo *bucket_info = bucket_rec_.GetBucketInfo();
        // Cache the locked bucket info for read directly at next.
        uint16_t bucket_id = bucket_key_.BucketId();
        locked_buckets_.emplace(bucket_id, bucket_info);

        const TxKey &key = vct_key->at(obj_cmd_op.bucket_lock_cur_);
        uint32_t residual = key.Hash() & 0x3FF;
        NodeGroupId bucket_ng = bucket_info->BucketOwner();
        vct_key_shard_code[obj_cmd_op.bucket_lock_cur_].first =
            bucket_ng << 10 | residual;

        // If current bucket is migrating, forward to new bucket owner.
        NodeGroupId new_bucket_ng = bucket_info->DirtyBucketOwner();
        if (new_bucket_ng != UINT32_MAX &&
            !vct_cmd->at(obj_cmd_op.bucket_lock_cur_)->IsReadOnly())
        {
            // forward key shard code
            vct_key_shard_code[obj_cmd_op.bucket_lock_cur_].second =
                new_bucket_ng << 10 | residual;
        }
        obj_cmd_op.bucket_lock_cur_++;
    }
    while (obj_cmd_op.bucket_lock_cur_ < vct_key->size())
    {
        const TxKey &key = vct_key->at(obj_cmd_op.bucket_lock_cur_);
        uint64_t key_hash = key.Hash();
        uint16_t bucket_id = Sharder::MapKeyHashToBucketId(key_hash);
        const BucketInfo *bucket_info = FastToGetBucket(bucket_id);
        if (bucket_info != nullptr)
        {
            // Uses the lower 10 bits of the key's hash code to shard the
            // key across CPU cores in a cc node.
            uint32_t residual = key_hash & 0x3FF;
            NodeGroupId bucket_ng = bucket_info->BucketOwner();
            vct_key_shard_code[obj_cmd_op.bucket_lock_cur_].first =
                bucket_ng << 10 | residual;

            // If current bucket is migrating, forward to new bucket owner.
            NodeGroupId new_bucket_ng = bucket_info->DirtyBucketOwner();
            if (new_bucket_ng != UINT32_MAX &&
                !vct_cmd->at(obj_cmd_op.bucket_lock_cur_)->IsReadOnly())
            {
                // forward key shard code
                vct_key_shard_code[obj_cmd_op.bucket_lock_cur_].second =
                    new_bucket_ng << 10 | residual;
            }
            obj_cmd_op.bucket_lock_cur_++;
        }
        else
        {
            // TODO(lzx): acquire mutli key bucket locks concurrently?
            lock_range_bucket_result_.Value().Reset();
            lock_range_bucket_result_.Reset();
            bucket_key_.Reset(bucket_id);

            lock_bucket_op_.Reset(TableName(range_bucket_ccm_name_sv.data(),
                                            range_bucket_ccm_name_sv.size(),
                                            TableType::RangeBucket,
                                            TableEngine::None),
                                  &bucket_tx_key_,
                                  &bucket_rec_,
                                  &lock_range_bucket_result_);

            // Control flow jumps to lock_bucket_op_, do not execute further
            // after `Process(lock_bucket_op_)` returns.
            PushOperation(&lock_bucket_op_);
            Process(lock_bucket_op_);
            return;
        }
    }

    obj_cmd_op.is_running_ = true;
    uint64_t current_ts =
        static_cast<LocalCcHandler *>(cc_handler_)->GetTsBaseValue();
    // bool commit = obj_cmd_op.auto_commit_ && txservice_skip_wal;
    uint32_t local_cnt = 0;  // Count the commands executed on local node
    // For read operation, the objects will be locked only when  iso_level_ >=
    // IsolationLevel::RepeatableRead. So if is is watch keys request, it will
    // change isolevel into RepeatableRead if it less than RepeatableRead.
    IsolationLevel iso =
        (req->is_watch_keys_ && iso_level_ < IsolationLevel::RepeatableRead)
            ? IsolationLevel::RepeatableRead
            : iso_level_;

    for (size_t i = 0; i < vct_key->size(); i++)
    {
        auto &hd_res = obj_cmd_op.vct_hd_result_[i];

        const TxKey &key = vct_key->at(i);
        uint32_t key_shard_code = 0;

        key_shard_code = vct_key_shard_code[i].first;
        // NOTICE: For MultiObjectCommand, must not commit commands in ApplyCc
        hd_res.Reset();
        bool commit = false;
        cc_handler_->ObjectCommand(*req->table_name_,
                                   locked_db_[db_idx].second,
                                   key,
                                   key_shard_code,
                                   *vct_cmd->at(i),
                                   TxNumber(),
                                   tx_term_,
                                   command_id_.load(std::memory_order_relaxed),
                                   current_ts,
                                   hd_res,
                                   iso,
                                   protocol_,
                                   commit,
                                   req->always_redirect_);

        if (hd_res.Value().is_local_)
        {
            local_cnt++;
        }
    }

    obj_cmd_op.atm_local_cnt_.fetch_add(local_cnt, std::memory_order_relaxed);
    StartTiming();
}
void TransactionExecution::PostProcess(MultiObjectCommandOp &obj_cmd_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &obj_cmd_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (lock_range_bucket_result_.IsError())
    {
        DLOG(ERROR) << "MultiObjectCommandOp failed when acquire range locks. "
                       "Error code: "
                    << static_cast<int>(lock_range_bucket_result_.ErrorCode());
        vct_rec_resp_->FinishError(
            ConvertCcError(lock_range_bucket_result_.ErrorCode()));
        vct_rec_resp_ = nullptr;
        return;
    }
    MultiObjectCommandTxRequest *req = obj_cmd_op.tx_req_;
    const std::vector<TxKey> *vct_key = req->VctKey();
    const std::vector<TxCommand *> *vct_cmd = req->VctCommand();

    CcErrorCode err = obj_cmd_op.atm_err_code_.load(std::memory_order_relaxed);
    if (err != CcErrorCode::NO_ERROR)
    {
        for (size_t i = 0; i < obj_cmd_op.vct_hd_result_.size(); i++)
        {
            const auto &cmd_res = obj_cmd_op.vct_hd_result_[i].Value();

            // Add the locked objects into read write set for future unlock.
            if (cmd_res.lock_acquired_ == LockType::WriteLock)
            {
                cmd_set_.AddObjectCommand(*req->table_name_,
                                          cmd_res.cce_addr_,
                                          cmd_res.rec_status_,
                                          cmd_res.commit_ts_,
                                          cmd_res.lock_ts_,
                                          cmd_res.last_vali_ts_,
                                          &vct_key->at(i),
                                          nullptr,
                                          UINT64_MAX);
            }
            else if (cmd_res.lock_acquired_ != LockType::NoLock &&
                     !cmd_set_.FindObjectCommand(*req->table_name_,
                                                 cmd_res.cce_addr_))
            {
                rw_set_.AddRead(
                    cmd_res.cce_addr_, cmd_res.commit_ts_, req->table_name_);
            }
        }

        vct_rec_resp_->FinishError(ConvertCcError(err));
        vct_rec_resp_ = nullptr;
        if (req->auto_commit_)
        {
            Abort();
        }
    }
    else
    {
        std::vector<RecordStatus> vct_rec;

        {
            vct_rec.reserve(obj_cmd_op.vct_hd_result_.size());
            bool version_changed = false;

            for (size_t i = 0; i < obj_cmd_op.vct_hd_result_.size(); i++)
            {
                const auto &cmd_res = obj_cmd_op.vct_hd_result_[i].Value();
                vct_rec.push_back(cmd_res.rec_status_);
                if (obj_cmd_op.vct_hd_result_[i].ErrorCode() ==
                    CcErrorCode::TASK_EXPIRED)
                {
                    continue;
                }

                assert(cmd_res.rec_status_ != RecordStatus::Unknown);

                // For autocommit read-modify-write commands, the
                // ObjectCommandTxRequest sender will be notified after auto
                // commit succeeds, i.e. after PostProcess or WritLog.

                if (cmd_res.lock_acquired_ == LockType::WriteLock)
                {
                    // If the cce is expired before the command is applyed
                    // Add a retire command before the write command
                    if (cmd_res.ttl_expired_)
                    {
                        assert(cmd_res.ttl_ == UINT64_MAX);
                        auto retire_command =
                            vct_cmd->at(i)->RetireExpiredTTLObjectCommand();
                        cmd_set_.AddObjectCommand(
                            *req->table_name_,
                            cmd_res.cce_addr_,
                            cmd_res.rec_status_,
                            cmd_res.commit_ts_,
                            cmd_res.lock_ts_,
                            cmd_res.last_vali_ts_,
                            &vct_key->at(i),
                            retire_command.get(),
                            cmd_res.ttl_,
                            obj_cmd_op.vct_key_shard_code_[i].second);
                    }
                    // The command modifies the object. Put it into the command
                    // set for writing log and post-processing. If the command
                    // fails, only to release the write lock.
                    cmd_set_.AddObjectCommand(
                        *req->table_name_,
                        cmd_res.cce_addr_,
                        cmd_res.rec_status_,
                        cmd_res.commit_ts_,
                        cmd_res.lock_ts_,
                        cmd_res.last_vali_ts_,
                        &vct_key->at(i),
                        cmd_res.object_modified_ ? vct_cmd->at(i) : nullptr,
                        cmd_res.ttl_,
                        obj_cmd_op.vct_key_shard_code_[i].second);

                    uint64_t read_version =
                        rw_set_.DedupRead(cmd_res.cce_addr_);
                    if (read_version > 0 && read_version != cmd_res.commit_ts_)
                    {
                        // Each write-set key acquires a write lock and gets the
                        // key's last validation ts and commit ts. If the write
                        // key has been read before and the key's commit ts
                        // mismatches the prior version, this is not a
                        // repeatable read.

                        LOG(WARNING)
                            << "set rset_has_expired_, txn: " << TxNumber()
                            << "; read_version: " << read_version
                            << "; cmd_result.commit_ts_: "
                            << cmd_res.commit_ts_;

                        version_changed = true;
                    }
                }
                else if (cmd_res.lock_acquired_ != LockType::NoLock &&
                         !cmd_set_.FindObjectCommand(*req->table_name_,
                                                     cmd_res.cce_addr_))
                {
                    // Read lock is acquired under locking protocol. Add the cce
                    // to read set for later PostRead.
                    if (!rw_set_.AddRead(cmd_res.cce_addr_,
                                         cmd_res.commit_ts_,
                                         req->table_name_))
                    {
                        version_changed = true;
                    }
                }
            }

            if (version_changed)
            {
                vct_rec_resp_->FinishError(
                    TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                vct_rec_resp_ = nullptr;
                if (req->auto_commit_)
                {
                    Abort();
                }
                return;
            }
        }

        bool cmd_success = req->Command()->IsPassed();
        // NOTICE: sub commands never be committed in ApplyCc
        if (!req->auto_commit_)
        {
            // Not autocommit, or autocommit and this is a read only command.
            // Notify the ObjectCommandTxRequest sender once the command
            // finishes.
            vct_rec_resp_->Finish(std::move(vct_rec));
            vct_rec_resp_ = nullptr;
        }

        if (req->auto_commit_)
        {
            if (cmd_success)
            {
                Commit();
            }
            else
            {
                Abort();
            }
        }
    }
}

void TransactionExecution::Process(CmdForwardAcquireWriteOp &forward_acquire)
{
    forward_acquire.Reset(cmd_set_.ObjectCountToForwardWrite());
    forward_acquire.is_running_ = true;

    uint64_t current_ts =
        static_cast<LocalCcHandler *>(cc_handler_)->GetTsBaseValue();

    size_t res_idx = 0, entry_idx = 0;
    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, CmdSetEntry>>
        &tx_cmd_set = *cmd_set_.ObjectCommandSet();
    for (const auto &[table_name, obj_cmd_set] : tx_cmd_set)
    {
        int db_idx = GetDbIndex(&table_name);

        for (const auto &[cce_addr, obj_cmd_entry] : obj_cmd_set)
        {
            if (obj_cmd_entry.forward_entry_ == nullptr)
            {
                continue;
            }
            CmdForwardEntry *cmd_forward_entry =
                obj_cmd_entry.forward_entry_.get();
            forward_acquire.acquire_write_entries_[entry_idx++] =
                cmd_forward_entry;

            cc_handler_->AcquireWrite(
                table_name,
                locked_db_[db_idx].second,
                cmd_forward_entry->key_,
                cmd_forward_entry->key_shard_code_,
                TxNumber(),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                current_ts,
                false,
                forward_acquire.hd_result_,
                res_idx++,
                protocol_,
                iso_level_);
        }
    }

    if (metrics::enable_remote_request_metrics &&
        forward_acquire.hd_result_.Value().at(0).remote_ack_cnt_->load(
            std::memory_order_relaxed) > 0)
    {
        auto meter = tx_processor_->GetMeter();
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Increment,
                       "cmd_forward_acquire_write");
        if (is_collecting_duration_round_)
        {
            forward_acquire.op_start_ = metrics::Clock::now();
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(
    CmdForwardAcquireWriteOp &forward_acquire)
{
    if (metrics::enable_remote_request_metrics &&
        forward_acquire.op_start_ < metrics::TimePoint::max())
    {
        metrics::Meter *meter;
        meter = tx_processor_->GetMeter();
        if (is_collecting_duration_round_)
        {
            meter->CollectDuration(metrics::NAME_REMOTE_REQUEST_DURATION,
                                   forward_acquire.op_start_,
                                   "cmd_forward_acquire_write");
        }
        meter = tx_processor_->GetMeter();
        meter->Collect(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                       metrics::Value::IncDecValue::Decrement,
                       "cmd_forward_acquire_write");
    }

    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (forward_acquire.hd_result_.IsError())
    {
        DLOG(ERROR) << "CmdForwardAcquireWriteOp failed for cc error:"
                    << forward_acquire.hd_result_.ErrorMsg() << "  "
                    << static_cast<int>(forward_acquire.hd_result_.ErrorCode())
                    << "; txn: " << TxNumber();
        assert(rec_resp_ != nullptr);
        rec_resp_->SetErrorCode(
            ConvertCcError(forward_acquire.hd_result_.ErrorCode()));
        Abort();
    }
    else
    {
        if (rw_set_.WriteSetSize() > 0)
        {
            assert(false);
            lock_write_range_buckets_.Reset();
            PushOperation(&lock_write_range_buckets_);
            Process(lock_write_range_buckets_);
        }
        else
        {
            PushOperation(&set_ts_);
            Process(set_ts_);
        }
    }
}

void TransactionExecution::Process(KickoutDataAllOp &kickout_data_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &kickout_data_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    auto all_node_groups = Sharder::Instance().AllNodeGroups();
    size_t table_cnt = kickout_data_all_op.table_names_.size();
    kickout_data_all_op.Reset(all_node_groups->size(), table_cnt);
    kickout_data_all_op.is_running_ = true;

    for (uint32_t ng_id : *all_node_groups)
    {
        for (size_t table_idx = 0; table_idx < table_cnt; ++table_idx)
        {
            cc_handler_->KickoutData(
                kickout_data_all_op.table_names_.at(table_idx),
                ng_id,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                kickout_data_all_op.hd_result_,
                kickout_data_all_op.clean_type_,
                nullptr,
                nullptr,
                nullptr,
                kickout_data_all_op.commit_ts_);
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(KickoutDataAllOp &kickout_data_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &kickout_data_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
        });
    state_stack_.pop_back();
}

void TransactionExecution::ProcessTxRequest(BatchReadTxRequest &batch_read_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &batch_read_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    if (tx_term_ < 0)
    {
        batch_read_req.SetError(TxErrorCode::TX_INIT_FAIL);
        return;
    }

    void_resp_ = &batch_read_req.tx_result_;

    batch_read_op_.batch_read_tx_req_ = &batch_read_req;
    batch_read_op_.Reset();
    batch_read_op_.local_cache_checked_ = batch_read_req.local_cache_checked_;

    PushOperation(&batch_read_op_);
    Process(batch_read_op_);
}

void TransactionExecution::Process(BatchReadOperation &batch_read_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &batch_read_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    batch_read_op.is_running_ = true;
    const TableName &table_name = *batch_read_op.batch_read_tx_req_->tab_name_;
    std::vector<txservice::ScanBatchTuple> &read_batch =
        batch_read_op.batch_read_tx_req_->read_batch_;

    if (table_name.IsHashPartitioned() && !CheckLeaderTerm())
    {
        // Check leader term for hash partitioned table since we will
        // access bucket infos that are bind to ng term.
        void_resp_->FinishError(TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
        void_resp_ = nullptr;
        return;
    }
    assert(batch_read_op.hd_result_vec_.size() == read_batch.size());
    if (!batch_read_op.local_cache_checked_)
    {
        for (size_t idx = 0; idx < read_batch.size(); ++idx)
        {
            const TxKey &key = read_batch[idx].key_;
            TxRecord &rec = *read_batch[idx].record_;
            RecordStatus &rec_status = read_batch[idx].status_;

            // Step 1: fast path if key is update by the same tx.
            const WriteSetEntry *write_entry =
                rw_set_.FindWrite(table_name, key);
            if (write_entry != nullptr)
            {
                if (write_entry->op_ == OperationType::Delete)
                {
                    rec_status = RecordStatus::Deleted;
                }
                else
                {
                    rec.Copy(*write_entry->rec_.get());
                    rec_status = RecordStatus::Normal;
                }

                batch_read_op.hd_result_vec_[idx].SetFinished();
            }
            else
            {
                rec_status = RecordStatus::Unknown;
            }
        }

        batch_read_op.local_cache_checked_ = true;
        if (batch_read_op.IsFinished())
        {
            return;
        }
    }

    size_t &lock_key_index = batch_read_op.lock_index_;
    while (lock_key_index < read_batch.size())
    {
        // The key is found in the write set. No need to lock its range.
        if (read_batch[lock_key_index].status_ != RecordStatus::Unknown)
        {
            ++lock_key_index;
            continue;
        }
        if (!table_name.IsHashPartitioned())
        {
            // Lock the range of the to-be-read key.
            batch_read_op.is_running_ = false;
            lock_range_bucket_result_.Value().Reset();
            lock_range_bucket_result_.Reset();

            lock_range_op_.Reset(TableName(table_name.StringView(),
                                           TableType::RangePartition,
                                           table_name.Engine()),
                                 &read_batch[lock_key_index].key_,
                                 &range_rec_,
                                 &lock_range_bucket_result_);
            PushOperation(&lock_range_op_);
            Process(lock_range_op_);
            return;
        }
        else
        {
            const TxKey &key = read_batch[lock_key_index].key_;
            uint64_t key_hash = key.Hash();
            uint16_t bucket_id = Sharder::MapKeyHashToBucketId(key_hash);
            const BucketInfo *bucket_info = FastToGetBucket(bucket_id);
            if (bucket_info != nullptr)
            {
                NodeGroupId bucket_ng = bucket_info->BucketOwner();
                read_batch[lock_key_index].cce_addr_.SetNodeGroupId(bucket_ng);
                ++lock_key_index;
            }
            else
            {
                // TODO(lzx): acquire mutli key bucket locks concurrently?
                lock_range_bucket_result_.Value().Reset();
                lock_range_bucket_result_.Reset();
                bucket_key_.Reset(bucket_id);

                lock_bucket_op_.Reset(TableName(range_bucket_ccm_name_sv.data(),
                                                range_bucket_ccm_name_sv.size(),
                                                TableType::RangeBucket,
                                                TableEngine::None),
                                      &bucket_tx_key_,
                                      &bucket_rec_,
                                      &lock_range_bucket_result_);

                // Control flow jumps to lock_bucket_op_, do not execute further
                // after `Process(lock_bucket_op_)` returns.
                PushOperation(&lock_bucket_op_);
                Process(lock_bucket_op_);
                return;
            }
        }
    }

    const uint64_t corresponding_sk_commit_ts =
        batch_read_op.batch_read_tx_req_->corresponding_sk_commit_ts_;
    IsolationLevel iso_level = iso_level_;
    if (batch_read_op.batch_read_tx_req_->is_for_share_ &&
        iso_level_ < IsolationLevel::RepeatableRead)
    {
        iso_level = IsolationLevel::RepeatableRead;
    }
    uint64_t read_ts = 0;
    if (iso_level == IsolationLevel::Snapshot)
    {
        read_ts = start_ts_;
    }
    else if (corresponding_sk_commit_ts != 0)
    {
        read_ts = corresponding_sk_commit_ts;
    }

    for (size_t idx = 0; idx < read_batch.size(); ++idx)
    {
        if (read_batch[idx].status_ != RecordStatus::Unknown)
        {
            continue;
        }

        const TxKey &key = read_batch[idx].key_;
        TxRecord &rec = *read_batch[idx].record_;

        uint32_t sharding_code = 0;
        size_t key_hash = key.Hash();
        sharding_code =
            read_batch[idx].cce_addr_.NodeGroupId() << 10 | (key_hash & 0x3FF);
        int32_t partition_id = -1;
        if (table_name.IsHashPartitioned())
        {
            partition_id = Sharder::MapKeyHashToHashPartitionId(key_hash);
        }
        else
        {
            partition_id = batch_read_op.range_ids_[idx];
        }
        cc_handler_->Read(table_name,
                          batch_read_op.batch_read_tx_req_->schema_version_,
                          key,
                          sharding_code,
                          rec,
                          ReadType::Inside,
                          TxNumber(),
                          tx_term_,
                          CommandId(),
                          read_batch[idx].version_ts_ > 0
                              ? read_batch[idx].version_ts_
                              : read_ts,
                          batch_read_op.hd_result_vec_[idx],
                          iso_level,
                          protocol_,
                          batch_read_op.batch_read_tx_req_->is_for_write_,
                          false,
                          true,
                          partition_id);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(BatchReadOperation &batch_read_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (lock_range_bucket_result_.IsError())
    {
        DLOG(ERROR)
            << "BatchReadOperation failed when acquire range bucket locks. "
               "Error code: "
            << (int) lock_range_bucket_result_.ErrorCode();
        void_resp_->FinishError(
            ConvertCcError(lock_range_bucket_result_.ErrorCode()));
        void_resp_ = nullptr;
        return;
    }

    const BatchReadTxRequest *read_req = batch_read_op.batch_read_tx_req_;
    const TableName *table_name = read_req->tab_name_;
    std::vector<ScanBatchTuple> &read_batch = read_req->read_batch_;
    CcErrorCode err = CcErrorCode::NO_ERROR;

    for (size_t idx = 0; idx < read_batch.size(); ++idx)
    {
        ScanBatchTuple &tuple = read_batch[idx];
        if (tuple.status_ != RecordStatus::Unknown)
        {
            // The key appears in the write set. The record has been filled.
            tuple.version_ts_ = start_ts_;
            continue;
        }

        CcHandlerResult<ReadKeyResult> &hd_res =
            batch_read_op.hd_result_vec_[idx];
        assert(hd_res.IsFinished());

        if (hd_res.IsError())
        {
            // Only returns the first error code.
            if (err == CcErrorCode::NO_ERROR)
            {
                err = hd_res.ErrorCode();
            }
        }
        else
        {
            const ReadKeyResult &read_res = hd_res.Value();
            // The record has been filled when performing the read. Only sets
            // the record status and timestamp.
            tuple.status_ = read_res.rec_status_;
            tuple.version_ts_ = read_res.ts_;

            LockType lock_type = read_res.lock_type_;
            if (lock_type != LockType::NoLock)
            {
                bool success = false;
                if (read_res.rec_status_ == RecordStatus::Unknown)
                {
                    // Only used to release lock.
                    success =
                        rw_set_.AddRead(read_res.cce_addr_, 0, table_name);
                }
                else
                {
                    success = rw_set_.AddRead(
                        read_res.cce_addr_, read_res.ts_, table_name);
                }

                if (!success && err != CcErrorCode::NO_ERROR)
                {
                    err = CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH;
                }
            }
        }
    }

    if (err == CcErrorCode::NO_ERROR)
    {
        void_resp_->Finish(void_);
        void_resp_ = nullptr;
    }
    else
    {
        void_resp_->FinishError(ConvertCcError(err));
        void_resp_ = nullptr;
    }
}

void TransactionExecution::Process(NotifyStartMigrateOp &notify_migration_op)
{
    uint32_t ng_count = Sharder::Instance().NodeGroupCount();

    notify_migration_op.Reset(ng_count);
    notify_migration_op.is_running_ = true;

    uint64_t cluster_scale_txn = TxNumber();

    assert(notify_migration_op.unfinished_req_cnt_.load(
               std::memory_order_relaxed) == 0);

    for (const auto &migrate_plan : notify_migration_op.migrate_plans_)
    {
        auto nid = migrate_plan.first;
        auto &migrate_info = migrate_plan.second;
        if (!migrate_info.has_migration_tx_)
        {
            notify_migration_op.unfinished_req_cnt_.fetch_add(
                1, std::memory_order_release);
            notify_migration_op.InitDataMigration(cluster_scale_txn, nid);
        }
    }
}

void TransactionExecution::PostProcess(
    NotifyStartMigrateOp &notify_migration_op)
{
    state_stack_.pop_back();
}

void TransactionExecution::Process(
    CheckMigrationIsFinishedOp &check_migration_is_finished_op)
{
    check_migration_is_finished_op.Reset();
    check_migration_is_finished_op.is_running_ = true;

    auto cluster_scale_txn = tx_number_.load(std::memory_order_relaxed);
    uint32_t log_group_id = txlog_->GetLogGroupId(cluster_scale_txn);

    auto &closure = check_migration_is_finished_op.closure_;
    closure.Request().Clear();
    closure.Request().set_log_group_id(log_group_id);
    closure.Request().set_cluster_scale_txn(cluster_scale_txn);
    txlog_->CheckMigrationIsFinished(log_group_id,
                                     closure.Controller(),
                                     closure.Request(),
                                     closure.Response(),
                                     closure);
}

void TransactionExecution::PostProcess(
    CheckMigrationIsFinishedOp &notify_migration_finished_op)
{
    state_stack_.pop_back();
}

void TransactionExecution::Process(
    InvalidateTableCacheOp &invalidate_table_cache_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &invalidate_table_cache_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    auto all_node_groups = Sharder::Instance().AllNodeGroups();
    invalidate_table_cache_op.Reset(all_node_groups->size());
    invalidate_table_cache_op.is_running_ = true;
    for (uint32_t ng_id : *all_node_groups)
    {
        cc_handler_->InvalidateTableCache(
            *invalidate_table_cache_op.table_name_,
            ng_id,
            TxNumber(),
            TxTerm(),
            CommandId(),
            invalidate_table_cache_op.hd_result_);
    }
    StartTiming();
}

void TransactionExecution::PostProcess(
    InvalidateTableCacheOp &invalidate_table_cache_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &invalidate_table_cache_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    state_stack_.pop_back();
}

LockType TransactionExecution::DeduceReadLockType(TableType tbl_type,
                                                  bool read_for_write,
                                                  IsolationLevel iso_level,
                                                  bool is_covering_key,
                                                  RecordStatus rec_status)
{
    if (rec_status == RecordStatus::Deleted && !read_for_write)
    {
        return LockType::NoLock;
    }

    CcOperation cc_op = CcOperation::Read;
    if (read_for_write)
    {
        cc_op = CcOperation::ReadForWrite;
    }
    else if (tbl_type == TableType::Secondary ||
             tbl_type == TableType::UniqueSecondary)
    {
        cc_op = CcOperation::ReadSkIndex;
    }

    return LockTypeUtil::DeduceLockType(
        cc_op, iso_level, protocol_, is_covering_key);
}

void TransactionExecution::RecoverDataMigration(
    const ::txlog::BucketMigrateMessage *migrate_msg,
    size_t cur_idx,
    std::shared_ptr<DataMigrationStatus> status)
{
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();

    std::lock_guard<std::mutex> lk(local_shards->data_migration_op_pool_mux_);

    if (local_shards->migration_op_pool_.empty())
    {
        migration_op_ = std::make_unique<DataMigrationOp>(this, status);
    }
    else
    {
        migration_op_ = std::move(local_shards->migration_op_pool_.back());
        local_shards->migration_op_pool_.pop_back();
        migration_op_->Reset(this, status);
    }
    if (migrate_msg)
    {
        migration_op_->migrate_bucket_idx_ = cur_idx;
        migration_op_->PrepareNextRoundBuckets();

        switch (migrate_msg->stage())
        {
        case ::txlog::BucketMigrateStage::BeforeLocking:
        {
            migration_op_->op_ = &migration_op_->write_before_locking_log_op_;
            migration_op_->write_before_locking_log_op_.hd_result_
                .SetFinished();
            break;
        }
        case ::txlog::BucketMigrateStage::PrepareMigrate:
        {
            migration_op_->op_ = &migration_op_->prepare_log_op_;
            migration_op_->prepare_log_op_.hd_result_.SetFinished();
            break;
        }
        case ::txlog::BucketMigrateStage::CommitMigrate:
        {
            migration_op_->op_ = &migration_op_->commit_log_op_;
            migration_op_->commit_log_op_.hd_result_.SetFinished();
            break;
        }
        default:
        {
            assert(false);
        }
        }
    }

    PushOperation(migration_op_.get());
}

void TransactionExecution::RecoverClusterScale(
    const ::txlog::ClusterScaleOpMessage &scale_op_msg,
    bool dm_started,
    bool dm_finished)
{
    // Read new cluster config from log
    int ng_cnt = scale_op_msg.new_ng_configs_size();
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs;
    std::unordered_map<uint32_t, NodeConfig> node_configs;
    for (int idx = 0; idx < scale_op_msg.node_configs_size(); idx++)
    {
        auto &node_config = scale_op_msg.node_configs(idx);
        node_configs.try_emplace(node_config.node_id(),
                                 node_config.node_id(),
                                 node_config.host_name(),
                                 node_config.port());
    }
    for (int ng_idx = 0; ng_idx < ng_cnt; ng_idx++)
    {
        int node_cnt = scale_op_msg.new_ng_configs(ng_idx).member_nodes_size();
        int ng_id = scale_op_msg.new_ng_configs(ng_idx).ng_id();
        std::vector<NodeConfig> ng_nodes;
        for (int nidx = 0; nidx < node_cnt; nidx++)
        {
            int member_nid =
                scale_op_msg.new_ng_configs(ng_idx).member_nodes(nidx);
            auto &member_node_msg = node_configs[member_nid];
            ng_nodes.emplace_back(
                member_node_msg.node_id_,
                member_node_msg.host_name_,
                member_node_msg.port_,
                scale_op_msg.new_ng_configs(ng_idx).is_candidate(nidx));
        }
        new_ng_configs.try_emplace(ng_id, std::move(ng_nodes));
    }
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();

    std::unique_lock<std::mutex> lk(local_shards->cluster_scale_op_mux_);
    ClusterScaleOpType op_type = ClusterScaleOpType::AddNodeGroup;
    switch (scale_op_msg.event_type())
    {
    case ::txlog::ClusterScaleOpMessage_ScaleOpType_AddNodeGroup:
        op_type = ClusterScaleOpType::AddNodeGroup;
        break;
    case ::txlog::ClusterScaleOpMessage_ScaleOpType_RemoveNode:
        op_type = ClusterScaleOpType::RemoveNode;
        break;
    case ::txlog::ClusterScaleOpMessage_ScaleOpType_AddNodeGroupPeers:
        op_type = ClusterScaleOpType::AddNodeGroupPeers;
        break;
    default:
        assert(false);
    }
    if (local_shards->cluster_scale_op_pool_.empty())
    {
        cluster_scale_op_ = std::make_unique<ClusterScaleOp>(
            scale_op_msg.id(), op_type, std::move(new_ng_configs), this);
    }
    else
    {
        cluster_scale_op_ =
            std::move(local_shards->cluster_scale_op_pool_.back());
        local_shards->cluster_scale_op_pool_.pop_back();
        cluster_scale_op_->Reset(
            scale_op_msg.id(), op_type, std::move(new_ng_configs), this);
    }

    ClusterScaleOp *op = cluster_scale_op_.get();

    if (op_type == ClusterScaleOpType::AddNodeGroup)
    {
        if (scale_op_msg.stage() == ::txlog::ClusterScaleStage::PrepareScale)
        {
            op->op_ = &op->prepare_log_op_;
            op->prepare_log_op_.hd_result_.SetFinished();
        }
        else if (scale_op_msg.stage() ==
                 ::txlog::ClusterScaleStage::ConfigUpdate)
        {
            if (dm_finished)
            {
                op->op_ = &op->check_migration_is_finished_op_;
                op->check_migration_is_finished_op_.migration_is_finished_ =
                    true;
                op->check_migration_is_finished_op_.rpc_is_finished_.store(
                    true);
            }
            else if (dm_started)
            {
                op->op_ = &op->install_cluster_config_op_;
                op->install_cluster_config_op_.keys_.clear();
                op->install_cluster_config_op_.keys_.emplace_back(
                    TxKey(VoidKey::NegativeInfinity()));
                op->install_cluster_config_op_.Reset(1);
                op->install_cluster_config_op_.hd_result_.SetFinished();
            }
            else
            {
                op->op_ = &op->update_cluster_config_log_op_;
                op->update_cluster_config_log_op_.hd_result_.SetFinished();
            }
        }
        else
        {
            assert(false);
        }
    }
    else
    {
        if (scale_op_msg.stage() == ::txlog::ClusterScaleStage::PrepareScale)
        {
            if (dm_finished)
            {
                op->op_ = &op->check_migration_is_finished_op_;
                op->check_migration_is_finished_op_.migration_is_finished_ =
                    true;
                op->check_migration_is_finished_op_.rpc_is_finished_.store(
                    true);
            }
            else
            {
                op->op_ = &op->prepare_log_op_;
                op->prepare_log_op_.hd_result_.SetFinished();
            }
        }
        else if (scale_op_msg.stage() ==
                 ::txlog::ClusterScaleStage::ConfigUpdate)
        {
            op->op_ = &op->update_cluster_config_log_op_;
            op->update_cluster_config_log_op_.hd_result_.SetFinished();
        }
        else
        {
            assert(false);
        }
    }
    // Read migration plan from log. This is only needed if data migrate
    // is not finished.
    if (!dm_finished)
    {
        std::unordered_map<NodeGroupId, BucketMigrateInfo> migrate_plan;
        for (auto &[ng_id, ng_process] :
             scale_op_msg.node_group_bucket_migrate_process())
        {
            BucketMigrateInfo ng_plan;

            ng_plan.has_migration_tx_ =
                ng_process.stage() !=
                ::txlog::NodeGroupMigrateMessage_Stage_NotStarted;
            for (auto &[bucket, bucket_msg] :
                 ng_process.bucket_migrate_process())
            {
                ng_plan.bucket_ids_.push_back(bucket);
                ng_plan.new_owner_ngs_.push_back(bucket_msg.new_owner());
                assert(bucket_msg.bucket_id() == bucket);
                assert(bucket_msg.old_owner() == ng_id);
            }
            migrate_plan.try_emplace(ng_id, std::move(ng_plan));
        }
        op->bucket_migrate_infos_ = std::move(migrate_plan);
    }
    PushOperation(op);
}

}  // namespace txservice
