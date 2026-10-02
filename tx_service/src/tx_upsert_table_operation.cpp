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
/**
 * @file tx_upsert_table_operation.cpp
 * @brief UpsertTableOp, the schema op of CREATE, DROP, TRUNCATE and ALTER
 * TABLE: the steps every op shares, and each op's own path through its data
 * store steps.
 */
#include <algorithm>
#include <boost/context/fixedsize_stack.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>
#include <cassert>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "sequences/sequences.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_operation.h"
#include "tx_service_common.h"
#include "tx_trace.h"

namespace txservice
{
namespace
{
#ifndef NDEBUG
/** @brief Coroutine stacks with a guard page, to catch an overflow. */
using CoroStackAllocator = boost::context::protected_fixedsize_stack;
#else
/** @brief Coroutine stacks. */
using CoroStackAllocator = boost::context::fixedsize_stack;
#endif
}  // namespace

UpsertTableOp::UpsertTableOp(const std::string_view table_name_str,
                             TableEngine table_engine,
                             const std::string &current_image,
                             uint64_t curr_schema_ts,
                             const std::string &dirty_image,
                             OperationType op_type,
                             TransactionExecution *txm)
    : SchemaOp(table_name_str,
               table_engine,
               current_image,
               dirty_image,
               curr_schema_ts,
               op_type,
               txm),
      lock_cluster_config_op_(),
      acquire_all_intent_op_(txm),
      prepare_log_op_(txm),
      post_all_intent_op_(txm),
      unlock_cluster_config_op_(txm),
      create_kv_table_op_(txm),
      drop_kv_table_op_(&table_key_.Name(), OperationType::DropTable, txm),
      truncate_kv_table_op_(txm),
      upsert_kv_table_op_(&table_key_.Name(), op_type, txm),
      update_sequence_table_op_(txm),
      acquire_all_lock_op_(txm),
      downgrade_lock_op_(txm),
      commit_log_op_(txm),
      clean_ccm_op_(txm),
      post_all_lock_op_(txm),
      clean_log_op_(txm),
      read_cluster_result_(txm)
{
    assert(op_type_ == OperationType::CreateTable ||
           op_type_ == OperationType::DropTable ||
           op_type_ == OperationType::TruncateTable ||
           op_type_ == OperationType::Update);

    lock_cluster_config_op_.table_name_ = TableName(cluster_config_ccm_name_sv,
                                                    TableType::ClusterConfig,
                                                    TableEngine::None);
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.keys_.emplace_back(&table_key_);
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    post_all_intent_op_.table_name_ = &catalog_ccm_name;
    post_all_intent_op_.keys_.emplace_back(&table_key_);
    post_all_intent_op_.recs_.push_back(&catalog_rec_);
    post_all_intent_op_.op_type_ = op_type_;
    post_all_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.keys_.emplace_back(&table_key_);
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    downgrade_lock_op_.table_name_ = &catalog_ccm_name;
    downgrade_lock_op_.keys_.emplace_back(&table_key_);
    downgrade_lock_op_.recs_.push_back(&catalog_rec_);
    downgrade_lock_op_.op_type_ = op_type_;
    downgrade_lock_op_.write_type_ = PostWriteType::DowngradeLock;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.keys_.emplace_back(&table_key_);
    post_all_lock_op_.recs_.push_back(&catalog_rec_);
    post_all_lock_op_.op_type_ = op_type_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    clean_ccm_op_.Clear();

    TX_TRACE_ASSOCIATE(this, &acquire_all_intent_op_, "acquire_all_intent_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_intent_op_, "post_all_intent_op_");
    TX_TRACE_ASSOCIATE(this, &create_kv_table_op_, "create_kv_table_op_");
    TX_TRACE_ASSOCIATE(this, &drop_kv_table_op_, "drop_kv_table_op_");
    TX_TRACE_ASSOCIATE(this, &truncate_kv_table_op_, "truncate_kv_table_op_");
    TX_TRACE_ASSOCIATE(this, &upsert_kv_table_op_, "upsert_kv_table_op_");
    TX_TRACE_ASSOCIATE(this, &kv_retry_wait_op_, "kv_retry_wait_op_");
    TX_TRACE_ASSOCIATE(
        this, &update_sequence_table_op_, "update_sequence_table_op_");
    TX_TRACE_ASSOCIATE(this, &acquire_all_lock_op_, "acquire_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &downgrade_lock_op_, "downgrade_lock_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");

    is_force_finished = false;
}

void UpsertTableOp::Forward(TransactionExecution *txm)
{
    // Runs the coroutine, from where it stands on entry, until it suspends
    // again or its body returns.
    switch (coro_status_)
    {
    case CoroStatus::kEmpty:
        coro_status_ = CoroStatus::kRunning;
        coro_ = boost::context::callcc(
            std::allocator_arg,
            CoroStackAllocator(kCoroStackSize),
            [this, txm](boost::context::continuation &&caller)
            {
                caller_ = &caller;
                Run(txm);
                caller_ = nullptr;
                coro_status_ = CoroStatus::kFinished;
                return std::move(caller);
            });
        break;
    case CoroStatus::kRunning:
        // The step the coroutine awaits has finished. A running coroutine's
        // handle has left coro_, so an empty one here would mean a Forward()
        // from inside the coroutine.
        assert(coro_);
        coro_ = coro_.resume();
        break;
    case CoroStatus::kFinished:
        // Never an entry state: Finish() pops the op and pools it as soon as
        // the body returns, and Reset() empties it before its next use.
        assert(false);
        return;
    }

    // Back from the coroutine: it suspended (still kRunning), or its body
    // returned (now kFinished). A returned body ends the op here, on the tx's
    // own stack, since the coroutine and its stack are gone.
    if (coro_status_ == CoroStatus::kFinished)
    {
        assert(!coro_);
        Finish(txm);
    }
}

void UpsertTableOp::SuspendUntilDone(TransactionExecution *txm,
                                     const TransactionOperation &op)
{
    assert(caller_ != nullptr);
    // A step stays on top of the tx's stack until its PostProcess pops it,
    // which can happen inside Process() already, e.g. a DsUpsertTableOp under
    // txservice_skip_kv. Suspending then would leave the op itself on top: the
    // tx's Forward() loop would stop there, and the op would resume only on the
    // processor's next round.
    if (txm->state_stack_.back() == &op)
    {
        *caller_ = caller_->resume();
    }
    // Resumed by Forward(), which the tx calls on the op on top of its stack.
    assert(txm->state_stack_.back() == this);
}

template <typename OpT>
void UpsertTableOp::Await(TransactionExecution *txm, OpT &op)
{
    op_ = &op;
    txm->PushOperation(&op);
    txm->Process(op);
    SuspendUntilDone(txm, op);
}

template <typename OpT>
void UpsertTableOp::Await(TransactionExecution *txm,
                          OpT &op,
                          const char *fault_point,
                          CcErrorCode fault_error)
{
    bool inject = false;
    CODE_FAULT_INJECTOR(fault_point, {
        LOG(INFO) << "FaultInject " << fault_point
                  << ", txn: " << txm->TxNumber();
        inject = true;
    });
    if (!inject)
    {
        Await(txm, op);
        return;
    }
    // The step fails without running. Nothing runs, so nothing is pushed or
    // awaited: the caller reads the error at once. The reset clears what a
    // pooled op's previous use left in the result.
    op_ = &op;
    op.hd_result_.Reset();
    op.hd_result_.SetError(fault_error);
}

void UpsertTableOp::AwaitKvStep(TransactionExecution *txm,
                                DsUpsertTableOp &step)
{
    op_ = &step;
    StartKvStep(txm, step);
    SuspendUntilDone(txm, step);
}

void UpsertTableOp::Run(TransactionExecution *txm)
{
    switch (entry_)
    {
    case Entry::kStart:
        if (!RunUntilPrepareLog(txm))
        {
            return;
        }
        [[fallthrough]];
    case Entry::kAfterPrepareLog:
        if (!RunUntilCommitLog(txm))
        {
            return;
        }
        [[fallthrough]];
    case Entry::kAfterCommitLog:
        RunAfterCommitLog(txm);
        break;
    }
}

bool UpsertTableOp::RunUntilPrepareLog(TransactionExecution *txm)
{
    // The cluster config read lock, held until the op's last post write all,
    // so that every step sent to all node groups reaches the same ones.
    Await(txm, lock_cluster_config_op_);
    if (lock_cluster_config_op_.hd_result_->IsError())
    {
        LOG(ERROR) << "Upsert table read cluster config failed, tx_number:"
                   << txm->TxNumber();
        MarkFailed(txm);
        ForceFinish(txm);
        return false;
    }

    // The cluster gate admits new ops only here, where an op takes the cluster
    // config read lock (EloqDoc docs/cdc-change-stream.md §6.2). An op past
    // this step, or a recovered one, which enters after it, is never gated:
    // those are what a pause drains. Nothing but the lock is held yet.
    if (cluster_conf_rec_.GatesSchemaOps())
    {
        LOG(WARNING) << "Upsert table on " << table_key_.Name().StringView()
                     << " refused: the cluster gate is paused, tx_number:"
                     << txm->TxNumber();
        txm->upsert_resp_->SetErrorCode(TxErrorCode::CLUSTER_GATED);
        MarkFailed(txm);
        ReleaseClusterConfig(txm);
        result_ = UpsertResult::Failed;
        return false;
    }

    // Write intents on the table's catalog entry, on every node group.
    DLOG(INFO) << "txn: " << txm->TxNumber()
               << " process acquire_all_intent_op_";
    Await(txm, acquire_all_intent_op_);
    if (acquire_all_intent_op_.fail_cnt_.load(std::memory_order_relaxed) > 0)
    {
        LOG(ERROR) << "Upsert table acquire write intent failed, tx_number:"
                   << txm->TxNumber();
        txm->upsert_resp_->SetErrorCode(
            TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL);
        // Fails to acquire the write intent on the schema. Since write
        // intents only conflict with other writes, there must be another tx
        // trying to modify the same table's schema. Stops the schema
        // operation, releasing the write intents acquired, once: remaining
        // ones, if there are any, will be recovered by individual cc nodes
        // separately. No log was written, so none is cleaned.
        MarkFailed(txm);
        if (RunPostWriteAll(txm, false))
        {
            ReleaseClusterConfig(txm);
        }
        // The op fails whether or not the node kept its leadership: a
        // ForceFinish above reports it Failed as well (commit timestamp 0).
        result_ = UpsertResult::Failed;
        return false;
    }

    // Assigns a commit timestamp to the tx state machine as the version of
    // the new schema. Unlike the conventional commit ts, the schema's commit
    // ts is not chosen via the SetTs stage. But it follows a similar set of
    // rules: the new schema's version should be greater than 1) the current
    // version, 2) the maximal commit ts of all tx's that have read the schema,
    // and 3) the local time when the tx starts.
    txm->commit_ts_ = txm->commit_ts_bound_ + 1;
    for (size_t idx = 0; idx < acquire_all_intent_op_.upload_cnt_; ++idx)
    {
        const AcquireAllResult &acq_all_res =
            acquire_all_intent_op_.hd_results_[idx].Value();
        uint64_t ts =
            std::max(acq_all_res.commit_ts_ + 1, acq_all_res.last_vali_ts_ + 1);
        txm->commit_ts_ = std::max(txm->commit_ts_, ts);
    }
    ResolveSchemaImages(txm->commit_ts_);

    // The prepare log. The op is guaranteed to proceed once it is flushed.
    FillPrepareLogRequest(txm);
    DLOG(INFO) << "txn: " << txm->TxNumber() << " process prepare_log_op_";
    Await(txm,
          prepare_log_op_,
          "upsert_table_prepare_log_fail",
          CcErrorCode::WRITE_LOG_FAILED);
    while (prepare_log_op_.hd_result_.IsError())
    {
        if (prepare_log_op_.hd_result_.ErrorCode() !=
            CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
        {
            LOG(ERROR) << "Upsert table write prepare log failed, tx_number:"
                       << txm->TxNumber();
            // Fails to flush the prepare log. The schema operation is
            // considered failed if the prepare log is not flushed. The commit
            // ts is set to 0 to signal that the ending releases all write
            // intents.
            MarkFailed(txm);
            txm->upsert_resp_->SetErrorCode(
                TxErrorCode::UPSERT_TABLE_PREPARE_FAIL);
            RunEnding(txm);
            return false;
        }
        if (!txm->CheckLeaderTerm())
        {
            LOG(ERROR) << "Upsert table write prepare log result unknown, "
                          "tx_number:"
                       << txm->TxNumber()
                       << ", not leader any more, stop retrying";
            // Not leader anymore, just quit. New leader will know whether
            // prepare log succeeds and continue the rest if it does. Should
            // not release the write intents. If prepare log is not written,
            // the write intents will be released individually via orphan lock
            // recovery mechanism.
            txm->upsert_resp_->SetErrorCode(
                TxErrorCode::LOG_SERVICE_UNREACHABLE);
            result_ = UpsertResult::Failed;
            return false;
        }
        // prepare log result unknown, keep retrying until getting a clear
        // response, either success or failure, or the coordinator itself is no
        // longer leader
        LOG(WARNING) << "Upsert table write prepare log result unknown, "
                        "tx_number:"
                     << txm->TxNumber() << ", keep retrying";
        prepare_log_op_.log_closure_.LogRequest()
            .mutable_write_log_request()
            ->set_retry(true);
        Await(txm, prepare_log_op_);
    }
    return true;
}

bool UpsertTableOp::RunUntilCommitLog(TransactionExecution *txm)
{
    ACTION_FAULT_INJECTOR("upsert_table_crash_after_prepare_log");

    // Installs the dirty schema on every node group, and gets back a local
    // view (pointer) of the committed and dirty schema.
    DLOG(INFO) << "txn: " << txm->TxNumber() << " process post_all_intent_op_";
    Await(txm, post_all_intent_op_);
    while (post_all_intent_op_.IsFailed())
    {
        // After the prepare log is flushed, the schema op is guaranteed to
        // proceed. Retry this step to install the dirty schema in the tx
        // service, if the tx node is still the leader. The tx is also allowed
        // to proceed if the tx is in the recovery mode and the tx node is a
        // leader candidate.
        if (!txm->CheckLeaderTerm())
        {
            ForceFinish(txm);
            return false;
        }
        // set catalog_rec_'s binary_value_ to image_str since it could be set
        // to TableSchemaView pointer in localshard.
        catalog_rec_.SetSchemaImage(image_str_);
        Await(txm, post_all_intent_op_);
    }

    CODE_FAULT_INJECTOR("upsert_table_hold_before_commit_lock",
                        { HoldBeforeCommitLock(txm); });

    // The op's own steps, up to and including its commit log.
    switch (op_type_)
    {
    case OperationType::CreateTable:
        return CreateUntilCommitLog(txm);
    case OperationType::Update:
        return AlterUntilCommitLog(txm);
    case OperationType::DropTable:
        return DropUntilCommitLog(txm);
    case OperationType::TruncateTable:
        return TruncateUntilCommitLog(txm);
    default:
        // An UpsertTableOp runs only the four table ops.
        assert(false);
        return false;
    }
}

bool UpsertTableOp::CreateUntilCommitLog(TransactionExecution *txm)
{
    // The kv tables at the prepare-stage timestamp; the catalog row waits for
    // the commit log, which fixes the timestamp it must carry. No commit log
    // has decided the create yet, so a data store failure gives it up rather
    // than retry.
    PrepareCreateKvTables(txm);
    Await(txm, create_kv_table_op_);
    bool failed = create_kv_table_op_.hd_result_.IsError();

    // The sequence table, also before the commit log.
    if (!failed && PrepareSequenceStep())
    {
        DLOG(INFO) << "txn: " << txm->TxNumber()
                   << " process update_sequence_table_op_";
        Await(txm, update_sequence_table_op_);
        failed = update_sequence_table_op_.hd_result_.IsError();
    }

    if (failed)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceFinish(txm);
            return false;
        }
        if (!AbandonCreate(txm))
        {
            return false;
        }
        // The commit log with commit timestamp 0 fails the op. The write
        // intents are still held and rejecting the dirty schema needs no write
        // lock, so the op skips the write locks, as ALTER TABLE's failure does.
        return WriteCommitLog(txm);
    }

    // The write locks, at the commit-stage timestamp, then the commit log.
    if (!AcquireWriteLocks(txm))
    {
        return false;
    }
    AssignCommitStageTs(txm);
    return WriteCommitLog(txm);
}

bool UpsertTableOp::DropUntilCommitLog(TransactionExecution *txm)
{
    // DROP TABLE: its data store steps come after the commit log.
    if (!AcquireWriteLocks(txm))
    {
        return false;
    }
    AssignCommitStageTs(txm);
    return WriteCommitLog(txm);
}

bool UpsertTableOp::TruncateUntilCommitLog(TransactionExecution *txm)
{
    // The table's data goes once no DML can reach it: after the write locks.
    if (!AcquireWriteLocks(txm))
    {
        return false;
    }
    AssignCommitStageTs(txm);

    if (!txservice_skip_kv &&
        !Sharder::Instance().GetDataStoreHandler()->IsSharedStorage())
    {
        // Without shared storage each node group truncates its own store,
        // after the commit log (TruncateAfterCommitLog). A node group that
        // recovers from the commit log truncates its store in the replay,
        // before it takes writes, and the later request finds it at the
        // commit ts and skips it (KickoutCcEntryCc): no write it takes is
        // wiped.
        return WriteCommitLog(txm);
    }

    // With shared storage the coordinator empties the table, before the commit
    // log: a participant that recovers from the commit log, and takes writes
    // into the emptied table, then finds no later step that wipes them. A
    // truncate never rolls back after its prepare log, so each step is retried
    // until it succeeds.

    // The cc maps first, so that no checkpoint writes a dirty entry of the old
    // table into the kv table once it is dropped.
    if (!CleanCcm(txm, false))
    {
        return false;
    }

    // Drops the kv table, which keeps its name. The catalog row waits for the
    // commit log, which fixes the timestamp it carries.
    PrepareTruncateKvTable(txm);
    Await(txm, truncate_kv_table_op_);
    while (truncate_kv_table_op_.hd_result_.IsError())
    {
        if (!AwaitKvRetry(txm,
                          &truncate_kv_table_op_,
                          "kv table truncate",
                          truncate_kv_table_op_.hd_result_.ErrorMsg()))
        {
            return false;
        }
        Await(txm, truncate_kv_table_op_);
    }

    // Seeds the table's sequences again.
    if (PrepareSequenceStep())
    {
        Await(txm, update_sequence_table_op_);
        while (update_sequence_table_op_.hd_result_.IsError())
        {
            if (!AwaitKvRetry(txm,
                              &update_sequence_table_op_,
                              "sequence table update",
                              update_sequence_table_op_.hd_result_.ErrorMsg()))
            {
                return false;
            }
            Await(txm, update_sequence_table_op_);
        }
    }
    return WriteCommitLog(txm);
}

bool UpsertTableOp::AlterUntilCommitLog(TransactionExecution *txm)
{
    // The new catalog row, before the commit log. The post write right after
    // the prepare log installed the dirty schema in the tx service and
    // returned a local view (pointer) of the committed and dirty schema.
    AwaitUpsertKvTable(txm, OperationType::Update);
    if (KvWriteFailed(txm))
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceFinish(txm);
            return false;
        }
        // ALTER TABLE writes its catalog row before the commit log, so a
        // failure rolls the op back: commit timestamp 0 tells the commit log
        // and the post write to reject the dirty schema.
        //
        // The commit log is still needed. Without it, a participant that fails
        // between releasing its write intent and the coordinator's clean log
        // would recover from the prepare log (whose commit timestamp is not 0)
        // a write lock and a dirty schema that no coordinator releases or
        // rejects. The write intent is still held here, and rejecting the
        // dirty schema needs no write lock, so the op skips the write locks.
        MarkFailed(txm);
        return WriteCommitLog(txm);
    }

    if (!AcquireWriteLocks(txm))
    {
        return false;
    }
    return WriteCommitLog(txm);
}

void UpsertTableOp::RunAfterCommitLog(TransactionExecution *txm)
{
    // Crashes the node with the commit log written and the op's data store
    // step not yet done, which recovery must roll forward.
    ACTION_FAULT_INJECTOR("upsert_table_crash_after_commit_log");
    bool go_on = true;
    switch (op_type_)
    {
    case OperationType::CreateTable:
        go_on = CreateAfterCommitLog(txm);
        break;
    case OperationType::DropTable:
        go_on = DropAfterCommitLog(txm);
        break;
    case OperationType::TruncateTable:
        go_on = TruncateAfterCommitLog(txm);
        break;
    case OperationType::Update:
        // ALTER TABLE wrote its catalog row before the commit log: nothing
        // else to do before the ending (PostCommit, or Rollback for a failed
        // catalog row write).
        ACTION_FAULT_INJECTOR("upsert_table_post_all_lock");
        break;
    default:
        // An UpsertTableOp runs only the four table ops.
        assert(false);
        break;
    }
    if (go_on)
    {
        RunEnding(txm);
    }
}

bool UpsertTableOp::CreateAfterCommitLog(TransactionExecution *txm)
{
    if (txm->commit_ts_ != tx_op_failed_ts_)
    {
        // The catalog row, now that the commit log fixed its timestamp; the
        // kv tables exist since before the lock. Only the row: op type Update
        // is the store's catalog-only write. Retried until the store answers.
        AwaitUpsertKvTable(txm, OperationType::Update);
        while (KvWriteFailed(txm))
        {
            if (!AwaitKvRetry(txm,
                              &upsert_kv_table_op_,
                              "catalog row write",
                              upsert_kv_table_op_.hd_result_.ErrorMsg()))
            {
                return false;
            }
            AwaitUpsertKvTable(txm, OperationType::Update);
        }
    }
    // The catalog row carries the commit-stage timestamp: the table is
    // complete in the store. A create that gave up writes nothing and goes on
    // to reject its dirty schema.
    ACTION_FAULT_INJECTOR("upsert_table_post_all_lock");
    return true;
}

bool UpsertTableOp::DropAfterCommitLog(TransactionExecution *txm)
{
    // Deletes the kv table, retried until the store answers.
    AwaitUpsertKvTable(txm, OperationType::DropTable);
    while (KvWriteFailed(txm))
    {
        if (!AwaitKvRetry(txm,
                          &upsert_kv_table_op_,
                          "kv table drop",
                          upsert_kv_table_op_.hd_result_.ErrorMsg()))
        {
            return false;
        }
        AwaitUpsertKvTable(txm, OperationType::DropTable);
    }

    // Deletes the table's sequences, retried until the store answers.
    if (PrepareSequenceStep())
    {
        Await(txm, update_sequence_table_op_);
        while (update_sequence_table_op_.hd_result_.IsError())
        {
            if (!AwaitKvRetry(txm,
                              &update_sequence_table_op_,
                              "sequence table update",
                              update_sequence_table_op_.hd_result_.ErrorMsg()))
            {
                return false;
            }
            Await(txm, update_sequence_table_op_);
        }
    }

    // Empties the table's cc maps on every node group; the ending's
    // PostCommit then drops the empty maps.
    return CleanCcm(txm, false);
}

bool UpsertTableOp::TruncateAfterCommitLog(TransactionExecution *txm)
{
    if (!txservice_skip_kv &&
        !Sharder::Instance().GetDataStoreHandler()->IsSharedStorage())
    {
        // Each node group empties its cc maps, then truncates its own store,
        // catalog row included (clean_ccm_op_).
        return CleanCcm(txm, true);
    }

    // The catalog row, now that the commit log fixed its timestamp: only the
    // row, op type Update, retried until the store answers. The data went
    // before the commit log.
    AwaitUpsertKvTable(txm, OperationType::Update);
    while (KvWriteFailed(txm))
    {
        if (!AwaitKvRetry(txm,
                          &upsert_kv_table_op_,
                          "catalog row write",
                          upsert_kv_table_op_.hd_result_.ErrorMsg()))
        {
            return false;
        }
        AwaitUpsertKvTable(txm, OperationType::Update);
    }
    return true;
}

void UpsertTableOp::RunEnding(TransactionExecution *txm)
{
    // PostCommit, or Rollback for an op that failed, retried until it
    // succeeds.
    if (!RunPostWriteAll(txm, true))
    {
        return;
    }

    // The op's last post write all: the cluster config lock can go.
    if (!ReleaseClusterConfig(txm))
    {
        return;
    }

    // The clean log, retried while the node is the leader.
    FillCleanLogRequestCommon(txm, clean_log_op_);
    DLOG(INFO) << "txn: " << txm->TxNumber() << " process clean_log_op_";
    Await(txm, clean_log_op_);
    while (clean_log_op_.hd_result_.IsError() && txm->CheckLeaderTerm())
    {
        clean_log_op_.log_closure_.LogRequest()
            .mutable_write_log_request()
            ->set_retry(true);
        Await(txm, clean_log_op_);
    }
    result_ = CleanLogResult(txm);
}

bool UpsertTableOp::AcquireWriteLocks(TransactionExecution *txm)
{
    DLOG(INFO) << "txn: " << txm->TxNumber() << " process acquire_all_lock_op_";
    Await(txm, acquire_all_lock_op_);
    while (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_relaxed) > 0)
    {
        // Fails to acquire the write lock. The schema operation can only roll
        // forward after flushing the prepare log. Retries the request if the
        // tx node is still the leader or the tx is in the recovery mode and
        // the cc node is a leader candidate.
        if (!txm->CheckLeaderTerm())
        {
            ForceFinish(txm);
            return false;
        }
        LOG(ERROR) << "Upsert table schema transaction failed to acquire write "
                      "lock, tx_number:"
                   << txm->TxNumber();
        if (acquire_all_lock_op_.IsDeadlock())
        {
            // We downgrade catalog write lock to avoid blocking other
            // transaction which acquiring catalog read lock. And then retry
            // to acquire catalog write lock.
            LOG(ERROR) << "Upsert table schema transaction deadlocks with "
                          "other transaction, downgrade write lock, "
                          "tx_number: "
                       << txm->TxNumber();
            Await(txm, downgrade_lock_op_);
            while (true)
            {
                if (!txm->CheckLeaderTerm())
                {
                    ForceFinish(txm);
                    return false;
                }
                if (!downgrade_lock_op_.hd_result_.IsError())
                {
                    break;
                }
                Await(txm, downgrade_lock_op_);
            }
        }
        Await(txm, acquire_all_lock_op_);
    }
    return true;
}

bool UpsertTableOp::WriteCommitLog(TransactionExecution *txm)
{
    FillCommitLogRequest(txm);
    DLOG(INFO) << "txn: " << txm->TxNumber() << " process commit_log_op_";
    Await(txm, commit_log_op_);
    while (commit_log_op_.hd_result_.IsError())
    {
        // Fails to flush the commit log. Retries the operation if the tx node
        // is still the leader or the tx is in the  recovery mode and the cc
        // node is a leader candidate.
        if (!txm->CheckLeaderTerm())
        {
            ForceFinish(txm);
            return false;
        }
        commit_log_op_.log_closure_.LogRequest()
            .mutable_write_log_request()
            ->set_retry(true);
        Await(txm, commit_log_op_);
    }
    return true;
}

bool UpsertTableOp::RunPostWriteAll(TransactionExecution *txm, bool retry)
{
    assert(post_all_lock_op_.write_type_ == (txm->commit_ts_ == tx_op_failed_ts_
                                                 ? PostWriteType::Rollback
                                                 : PostWriteType::PostCommit));
    while (true)
    {
        DLOG(INFO) << "txn: " << txm->TxNumber()
                   << " process post_all_lock_op_";
        Await(txm, post_all_lock_op_);
        if (!txm->CheckLeaderTerm())
        {
            // The tx node is no longer the leader or leader candidate(during
            // recovery), ForceFinish.
            ForceFinish(txm);
            return false;
        }

        // The tx's modification of the schema has finished. If the tx has
        // previously read the same schema and keeps a pointer in the read set
        // to the cc entry of the schema, removes it from the read set. As a
        // result, the tx will not try to release the read lock of the schema
        // when committing.
        for (size_t idx = 0; idx < acquire_all_intent_op_.upload_cnt_; ++idx)
        {
            const CcEntryAddr &schema_entry_addr =
                acquire_all_intent_op_.hd_results_[idx].Value().local_cce_addr_;
            if (schema_entry_addr.NodeGroupId() == txm->TxCcNodeId())
            {
                txm->rw_set_.DedupRead(schema_entry_addr);
            }
        }

        if (!retry || !post_all_lock_op_.hd_result_.IsError())
        {
            return true;
        }
        // post_all_lock_op_ returns an error:
        // 1. if flush kv succeeds, the schema op is guaranteed to succeed and
        // can only roll forward. Retry this step to install the committed
        // schema and remove write locks;
        // 2. if flush kx fails, the schema op has to roll backward. Retry this
        // step to reject dirty schema and remove write locks.
        DLOG(INFO) << "txn: " << txm->TxNumber() << " retry post_all_lock_op_, "
                   << post_all_lock_op_.hd_result_.ErrorMsg();
    }
}

bool UpsertTableOp::ReleaseClusterConfig(TransactionExecution *txm)
{
    op_ = &unlock_cluster_config_op_;
    StartUnlockClusterConfig(txm, unlock_cluster_config_op_);
    SuspendUntilDone(txm, unlock_cluster_config_op_);
    if (unlock_cluster_config_op_.hd_result_.IsError())
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceFinish(txm);
            return false;
        }
        // Releasing a local read lock should never fail
        assert(false);
    }
    return true;
}

void UpsertTableOp::HoldBeforeCommitLock(TransactionExecution *txm)
{
    static constexpr char kHold[] = "upsert_table_hold_before_commit_lock";
    static constexpr char kHeld[] = "upsert_table_held_before_commit_lock";
    LOG(INFO) << "FaultInject " << kHold << ": holding the schema op on table "
              << table_key_.Name().StringView()
              << " before its commit-stage write locks, txn: "
              << txm->TxNumber();
    FaultInject::Instance().InjectFault(kHeld, "");
    kv_retry_wait_op_.SetBlockingWork(
        []()
        {
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(60);
            while (FaultInject::Entry(kHold) != nullptr &&
                   std::chrono::steady_clock::now() < deadline)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            return CcErrorCode::NO_ERROR;
        });
    Await(txm, kv_retry_wait_op_);
    FaultInject::Instance().InjectFault(kHeld, "remove");
    LOG(INFO) << "FaultInject " << kHold << ": released the schema op on table "
              << table_key_.Name().StringView() << ", txn: " << txm->TxNumber();
}

bool UpsertTableOp::AwaitKvRetry(TransactionExecution *txm,
                                 TransactionOperation *step,
                                 std::string_view step_name,
                                 const std::string &error)
{
    if (!txm->CheckLeaderTerm())
    {
        ForceFinish(txm);
        return false;
    }
    ArmKvRetryWait(txm, step, step_name, error);
    Await(txm, kv_retry_wait_op_);
    // The pause is over: the step runs again, if the node is still the leader.
    if (!txm->CheckLeaderTerm())
    {
        ForceFinish(txm);
        return false;
    }
    return true;
}

void UpsertTableOp::ForceFinish(TransactionExecution *txm)
{
    is_force_finished = true;
    result_ = CleanLogResult(txm);
}

UpsertResult UpsertTableOp::CleanLogResult(TransactionExecution *txm)
{
    CODE_FAULT_INJECTOR("alter_schema_term_changed", {
        LOG(INFO) << "FaultInject  alter_schema_term_changed";
        is_force_finished = true;
    });
    if (txm->commit_ts_ == tx_op_failed_ts_)
    {
        // Flush kv error or fail to flush prepare_log.
        return UpsertResult::Failed;
    }
    return is_force_finished ? UpsertResult::Unverified
                             : UpsertResult::Succeeded;
}

void UpsertTableOp::Finish(TransactionExecution *txm)
{
    txm->upsert_resp_->Finish(result_);
    txm->state_stack_.pop_back();
    assert(txm->state_stack_.empty());

    catalog_rec_.Reset();
    LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
    std::unique_lock<std::mutex> lk(shards->table_schema_op_pool_mux_);
    // Moves this op into the pool: nothing may touch it afterwards.
    shards->table_schema_op_pool_.emplace_back(std::move(txm->schema_op_));
}

void UpsertTableOp::Reset(const std::string_view table_name_str,
                          TableEngine table_engine,
                          const std::string &current_image,
                          uint64_t curr_schema_ts,
                          const std::string &dirty_image,
                          OperationType op_type,
                          TransactionExecution *txm)
{
    assert(op_type_ == OperationType::CreateTable ||
           op_type_ == OperationType::Update ||
           op_type_ == OperationType::DropTable ||
           op_type_ == OperationType::TruncateTable);

    // reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;

    // reset SchemaOp
    table_key_.Name() = TableName(table_name_str.data(),
                                  table_name_str.size(),
                                  TableType::Primary,
                                  table_engine);
    catalog_rec_.SetSchemaImage(current_image);
    catalog_rec_.SetDirtySchemaImage(dirty_image);
    image_str_ = current_image;
    dirty_image_str_ = dirty_image;
    curr_schema_ts_ = curr_schema_ts;

    // reset UpsertTableOp
    op_type_ = op_type;
    op_ = nullptr;

    // reset op
    read_cluster_result_.Reset();
    cluster_conf_rec_.Reset();
    lock_cluster_config_op_.Reset();
    lock_cluster_config_op_.key_ = VoidKey::NegInfTxKey();
    lock_cluster_config_op_.table_name_ = TableName(cluster_config_ccm_name_sv,
                                                    TableType::ClusterConfig,
                                                    TableEngine::None);
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;
    prepare_log_op_.Reset();
    unlock_cluster_config_op_.Reset();
    create_kv_table_op_.Reset();
    drop_kv_table_op_.Reset();
    truncate_kv_table_op_.Reset();
    upsert_kv_table_op_.Reset();
    ResetKvRetry(txm);
    update_sequence_table_op_.Reset();
    commit_log_op_.Reset();
    clean_log_op_.Reset();

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.keys_.clear();
    acquire_all_intent_op_.keys_.emplace_back(&table_key_);
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    post_all_intent_op_.table_name_ = &catalog_ccm_name;
    post_all_intent_op_.keys_.clear();
    post_all_intent_op_.keys_.emplace_back(&table_key_);
    post_all_intent_op_.recs_.clear();
    post_all_intent_op_.recs_.push_back(&catalog_rec_);
    post_all_intent_op_.op_type_ = op_type_;
    post_all_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    upsert_kv_table_op_.alter_table_info_ = nullptr;
    upsert_kv_table_op_.write_time_ = 0;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.keys_.clear();
    acquire_all_lock_op_.keys_.emplace_back(&table_key_);
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    downgrade_lock_op_.table_name_ = &catalog_ccm_name;
    downgrade_lock_op_.keys_.clear();
    downgrade_lock_op_.keys_.emplace_back(&table_key_);
    downgrade_lock_op_.recs_.clear();
    downgrade_lock_op_.recs_.push_back(&catalog_rec_);
    downgrade_lock_op_.op_type_ = op_type_;
    downgrade_lock_op_.write_type_ = PostWriteType::DowngradeLock;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.keys_.clear();
    post_all_lock_op_.keys_.emplace_back(&table_key_);
    post_all_lock_op_.recs_.clear();
    post_all_lock_op_.recs_.push_back(&catalog_rec_);
    post_all_lock_op_.op_type_ = op_type_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    clean_ccm_op_.Clear();
    clean_ccm_op_.hd_result_.Reset();

    // reset cc_handler_res txm
    read_cluster_result_.ResetTxm(txm);
    acquire_all_intent_op_.ResetHandlerTxm(txm);
    prepare_log_op_.ResetHandlerTxm(txm);
    post_all_intent_op_.ResetHandlerTxm(txm);
    unlock_cluster_config_op_.ResetHandlerTxm(txm);
    create_kv_table_op_.ResetHandlerTxm(txm);
    drop_kv_table_op_.ResetHandlerTxm(txm);
    truncate_kv_table_op_.ResetHandlerTxm(txm);
    upsert_kv_table_op_.ResetHandlerTxm(txm);
    update_sequence_table_op_.ResetHandlerTxm(txm);
    acquire_all_lock_op_.ResetHandlerTxm(txm);
    downgrade_lock_op_.ResetHandlerTxm(txm);
    commit_log_op_.ResetHandlerTxm(txm);
    clean_ccm_op_.ResetHandlerTxm(txm);
    post_all_lock_op_.ResetHandlerTxm(txm);
    clean_log_op_.ResetHandlerTxm(txm);

    is_force_finished = false;
    assert(!coro_ && caller_ == nullptr);
    coro_status_ = CoroStatus::kEmpty;
    entry_ = Entry::kStart;
    result_ = UpsertResult::Failed;
}

void UpsertTableOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    FillPrepareLogRequestCommon(txm, prepare_log_op_);
    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    auto &node_terms = *prepare_log_rec->mutable_node_terms();
    node_terms.clear();
    for (size_t idx = 0; idx < acquire_all_intent_op_.upload_cnt_; ++idx)
    {
        const AcquireAllResult &hres_val =
            acquire_all_intent_op_.hd_results_[idx].Value();
        uint32_t ng_id = hres_val.local_cce_addr_.NodeGroupId();
        node_terms[ng_id] = hres_val.node_term_;
    }
}

void UpsertTableOp::FillCommitLogRequest(TransactionExecution *txm)
{
    FillCommitLogRequestCommon(txm, commit_log_op_);

    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    // Commit timestamp 0 only for ALTER TABLE's failed catalog write or a
    // create that gave up on its kv tables or sequence table update. Such an
    // op is rolled back at timestamp 0: its DDL record is dropped, with no
    // change event.
    assert(txm->commit_ts_ != tx_op_failed_ts_ ||
           upsert_kv_table_op_.hd_result_.IsError() ||
           create_kv_table_op_.hd_result_.IsError() ||
           update_sequence_table_op_.hd_result_.IsError());
    const bool rolled_back =
        post_all_lock_op_.write_type_ == PostWriteType::Rollback;
    assert(rolled_back == (txm->commit_ts_ == tx_op_failed_ts_));
    commit_log_rec->set_commit_timestamp(txm->commit_ts_);
    commit_log_rec->mutable_log_content()
        ->mutable_schema_log()
        ->set_rolled_back(rolled_back);
}

void UpsertTableOp::AwaitUpsertKvTable(TransactionExecution *txm,
                                       OperationType op_type)
{
    upsert_kv_table_op_.op_type_ = op_type;
    upsert_kv_table_op_.table_schema_old_ = catalog_rec_.Schema();
    upsert_kv_table_op_.table_schema_ = catalog_rec_.DirtySchema();
    upsert_kv_table_op_.alter_table_info_ = nullptr;
    upsert_kv_table_op_.write_time_ = txm->commit_ts_;
    DLOG(INFO) << "txn: " << txm->TxNumber() << " process upsert_kv_table_op_";
    AwaitKvStep(txm, upsert_kv_table_op_);
}

bool UpsertTableOp::KvWriteFailed(TransactionExecution *txm)
{
    bool write_failed = upsert_kv_table_op_.hd_result_.IsError();
    // Fails one write, then disarms itself, so that a retry must write.
    CODE_FAULT_INJECTOR("upsert_table_kv_write_fail", {
        LOG(INFO) << "FaultInject upsert_table_kv_write_fail, txn: "
                  << txm->TxNumber();
        FaultInject::Instance().InjectFault("upsert_table_kv_write_fail",
                                            "remove");
        write_failed = true;
    });
    return write_failed;
}

bool UpsertTableOp::PrepareSequenceStep()
{
    const TableSchema *schema = op_type_ == OperationType::CreateTable
                                    ? catalog_rec_.DirtySchema()
                                    : catalog_rec_.Schema();
    // The sequence table holds two kinds of sequences. Every range-partitioned
    // table, a base table or an index (indexes are always range-partitioned),
    // has a range-id sequence: a create seeds it with the id of the table's
    // initial range (Sequences::InitialRangePartitionIdOf), a range split
    // takes new ids from it (LocalCcShards::GetNextRangePartitionId), a drop
    // deletes it and a truncate seeds it again. A table with an auto-increment
    // column also has that column's sequence. A hash-partitioned table without
    // indexes or an auto-increment column has neither.
    const bool need_update_sequence_table =
        !table_key_.Name().IsHashPartitioned() ||
        !schema->IndexNames().empty() || schema->HasAutoIncrement();
    if (!need_update_sequence_table)
    {
        return false;
    }

    update_sequence_table_op_.SetBlockingWork(
        [this, schema]()
        {
            assert(txservice::Sequences::Initialized());
            bool succ = true;
            const TableName &table_name = table_key_.Name();
            const std::vector<TableName> index_names = schema->IndexNames();
            if (op_type_ == OperationType::CreateTable ||
                op_type_ == OperationType::TruncateTable)
            {
                if (!table_name.IsHashPartitioned())
                {
                    succ = Sequences::InitIdOfTableRangePartition(
                        table_name,
                        Sequences::InitialRangePartitionIdOf(table_name));
                }
                succ = succ &&
                       std::all_of(
                           index_names.begin(),
                           index_names.end(),
                           [](const TableName &index_name)
                           {
                               return Sequences::InitIdOfTableRangePartition(
                                   index_name,
                                   Sequences::InitialRangePartitionIdOf(
                                       index_name));
                           });
            }
            else
            {
                assert(op_type_ == OperationType::DropTable);
                if (!table_name.IsHashPartitioned())
                {
                    succ = Sequences::DeleteSequence(
                        table_name, SequenceType::RangePartitionId);
                }
                succ =
                    succ && std::all_of(index_names.begin(),
                                        index_names.end(),
                                        [](const TableName &index_name)
                                        {
                                            return Sequences::DeleteSequence(
                                                index_name,
                                                SequenceType::RangePartitionId);
                                        });
            }

            if (succ && schema->HasAutoIncrement())
            {
                succ = op_type_ == OperationType::DropTable
                           ? Sequences::DeleteSequence(
                                 table_name, SequenceType::AutoIncrementColumn)
                           : Sequences::InitIdOfAutoIncrementColumn(table_name);
            }

            // Fails the update once, then disarms itself: a create gives up, a
            // drop retries.
            CODE_FAULT_INJECTOR("upsert_table_update_sequence_fail", {
                LOG(INFO) << "FaultInject upsert_table_update_sequence_fail";
                FaultInject::Instance().InjectFault(
                    "upsert_table_update_sequence_fail", "remove");
                succ = false;
            });

            if (!succ)
            {
                LOG(ERROR) << "UpsertTableOp: failed to update the sequence "
                              "table for table: "
                           << table_name.StringView();
                return CcErrorCode::UPDATE_SEQUENCE_TABLE_FAIL;
            }
            return CcErrorCode::NO_ERROR;
        });
    return true;
}

void UpsertTableOp::PrepareCreateKvTables(TransactionExecution *txm)
{
    assert(op_type_ == OperationType::CreateTable);
    LOG(INFO) << "UpsertTableOp: create the kv tables of table "
              << table_key_.Name().StringView() << ", txn: " << txm->TxNumber();

    // The prepare-stage timestamp, which the in-memory ranges also carry
    // (PostCommit); recovery from the prepare log creates them again with the
    // same one.
    const uint64_t version = txm->commit_ts_;
    create_kv_table_op_.SetBlockingWork(
        [this, version, txn = txm->TxNumber()]()
        {
            store::DataStoreHandler *const store_hd =
                Sharder::Instance().GetLocalCcShards()->store_hd_;
            if (store_hd == nullptr || store_hd->ByPassDataStore())
            {
                return CcErrorCode::NO_ERROR;
            }
            const TableSchema *schema = catalog_rec_.DirtySchema();
            assert(schema != nullptr);
            bool succ = store_hd->CreateKvTable(table_key_.Name(), version);
            for (const TableName &index_name : schema->IndexNames())
            {
                succ = succ && store_hd->CreateKvTable(index_name, version);
            }
            // Fails the creation once, after the tables are created, so that
            // the create gives up with real tables to drop; then disarms
            // itself.
            CODE_FAULT_INJECTOR("upsert_table_create_kv_table_fail", {
                LOG(INFO) << "FaultInject upsert_table_create_kv_table_fail, "
                             "txn: "
                          << txn;
                FaultInject::Instance().InjectFault(
                    "upsert_table_create_kv_table_fail", "remove");
                succ = false;
            });
            return succ ? CcErrorCode::NO_ERROR : CcErrorCode::DATA_STORE_ERR;
        });
}

void UpsertTableOp::PrepareTruncateKvTable(TransactionExecution *txm)
{
    assert(op_type_ == OperationType::TruncateTable);
    const TableSchema *schema = catalog_rec_.Schema();
    assert(schema != nullptr);
    LOG(INFO) << "UpsertTableOp: truncate the kv table of table "
              << table_key_.Name().StringView() << ", txn: " << txm->TxNumber();
    truncate_kv_table_op_.SetBlockingWork(
        [kv_table_name = schema->GetKVCatalogInfo()->kv_table_name_]()
        {
            store::DataStoreHandler *const store_hd =
                Sharder::Instance().GetLocalCcShards()->store_hd_;
            if (txservice_skip_kv || store_hd == nullptr ||
                store_hd->ByPassDataStore())
            {
                return CcErrorCode::NO_ERROR;
            }
            return store_hd->DropKvTable(kv_table_name)
                       ? CcErrorCode::NO_ERROR
                       : CcErrorCode::DATA_STORE_ERR;
        });
}

bool UpsertTableOp::CleanCcm(TransactionExecution *txm, bool base_table_only)
{
    assert(clean_ccm_op_.table_names_.empty());
    if (base_table_only)
    {
        clean_ccm_op_.table_names_.emplace_back(
            table_key_.Name().StringView().data(),
            table_key_.Name().StringView().size(),
            table_key_.Name().Type(),
            table_key_.Name().Engine());
    }
    else
    {
        const TableSchema *table_old_schema = catalog_rec_.Schema();
        assert(table_old_schema->GetBaseTableName() == table_key_.Name());
        auto clean_ccm_names = table_old_schema->IndexNames();
        clean_ccm_names.emplace_back(
            table_old_schema->GetBaseTableName().StringView().data(),
            table_old_schema->GetBaseTableName().StringView().size(),
            table_old_schema->GetBaseTableName().Type(),
            table_old_schema->GetBaseTableName().Engine());
        clean_ccm_op_.table_names_ = std::move(clean_ccm_names);
    }
    clean_ccm_op_.clean_type_ = CleanType::CleanCcm;
    clean_ccm_op_.commit_ts_ = txm->CommitTs();

    LOG(INFO) << "UpsertTableOp: Clean all ccmap on all node groups, txn: "
              << txm->TxNumber();

    Await(txm, clean_ccm_op_);
    while (clean_ccm_op_.hd_result_.IsError())
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceFinish(txm);
            return false;
        }
        LOG(ERROR) << "UpsertTableOp: failed to truncate table, err code: "
                   << (int) clean_ccm_op_.hd_result_.ErrorCode()
                   << ", err msg: " << clean_ccm_op_.hd_result_.ErrorMsg()
                   << ", txn: " << txm->TxNumber() << ", keep retrying";
        Await(txm, clean_ccm_op_);
    }

    clean_ccm_op_.Clear();

    // Clear write set before commit dirty schema.
    txm->rw_set_.ClearWriteSet(table_key_.Name());
    txm->rw_set_.ClearReadSet(table_key_.Name());
    return true;
}

void UpsertTableOp::AssignCommitStageTs(TransactionExecution *txm)
{
    assert(op_type_ == OperationType::CreateTable ||
           op_type_ == OperationType::DropTable ||
           op_type_ == OperationType::TruncateTable);
    const uint64_t prepare_ts = txm->commit_ts_;
    for (size_t idx = 0; idx < acquire_all_lock_op_.upload_cnt_; ++idx)
    {
        const AcquireAllResult &acq_all_res =
            acquire_all_lock_op_.hd_results_[idx].Value();
        txm->commit_ts_ = std::max(txm->commit_ts_,
                                   std::max(acq_all_res.last_vali_ts_ + 1,
                                            acq_all_res.commit_ts_ + 1));
    }
    DLOG(INFO) << "UpsertTableOp: table " << table_key_.Name().StringView()
               << " commits at " << txm->commit_ts_ << ", prepared at "
               << prepare_ts << ", txn: " << txm->TxNumber();
}

bool UpsertTableOp::AbandonCreate(TransactionExecution *txm)
{
    assert(op_type_ == OperationType::CreateTable);
    const uint64_t prepare_ts = txm->commit_ts_;
    LOG(ERROR) << "UpsertTableOp: the data store failed the "
               << (create_kv_table_op_.hd_result_.IsError()
                       ? "kv table creation"
                       : "sequence table update")
               << " of table " << table_key_.Name().StringView()
               << " before the commit log; the create fails, txn: "
               << txm->TxNumber();
    MarkFailed(txm);
    txm->upsert_resp_->SetErrorCode(TxErrorCode::DATA_STORE_ERROR);

    // The store's drop-table path over the schema the kv tables were created
    // from, at the version they carry. One attempt.
    drop_kv_table_op_.table_schema_old_ = catalog_rec_.DirtySchema();
    drop_kv_table_op_.table_schema_ = nullptr;
    drop_kv_table_op_.alter_table_info_ = nullptr;
    drop_kv_table_op_.write_time_ = prepare_ts;
    AwaitKvStep(txm, drop_kv_table_op_);
    if (drop_kv_table_op_.hd_result_.IsError())
    {
        LOG(WARNING) << "UpsertTableOp: failed to drop the kv tables of table "
                     << table_key_.Name().StringView()
                     << " after its create gave up; they stay orphaned, txn: "
                     << txm->TxNumber();
    }
    if (!txm->CheckLeaderTerm())
    {
        // No commit log yet: whoever recovers the op from its prepare log
        // tries the kv tables again.
        ForceFinish(txm);
        return false;
    }
    return true;
}

void UpsertTableOp::MarkFailed(TransactionExecution *txm)
{
    txm->commit_ts_ = tx_op_failed_ts_;
    post_all_lock_op_.write_type_ = PostWriteType::Rollback;
}

}  // namespace txservice
