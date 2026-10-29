/**
 *    Copyright (C) 2026 EloqData Inc.
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
 * @file
 * @brief ClusterGateOp, the transaction that pauses or resumes the cluster
 * gate (EloqDoc docs/cdc-change-stream.md §6.2), and its entry points on
 * TransactionExecution.
 */

#include <butil/fast_rand.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>

#include "error_messages.h"
#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "log_type.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_operation.h"
#include "tx_request.h"
#include "tx_trace.h"

namespace txservice
{

template <typename Op>
void ClusterGateOp::RunStage(TransactionExecution *txm, Op *stage)
{
    op_ = stage;
    txm->PushOperation(stage);
    txm->Process(*stage);
}

ClusterGateOp::ClusterGateOp(TransactionExecution *txm)
    : CompositeTransactionOperation(),
      read_gate_op_(),
      acquire_intent_op_(txm),
      acquire_write_op_(txm),
      gate_log_op_(txm),
      install_op_(txm),
      read_result_(txm)
{
    acquire_intent_op_.table_name_ = &cluster_config_ccm_name;
    acquire_intent_op_.keys_.emplace_back(TxKey(VoidKey::NegativeInfinity()));
    acquire_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_intent_op_.protocol_ = CcProtocol::OCC;

    acquire_write_op_.table_name_ = &cluster_config_ccm_name;
    acquire_write_op_.keys_.emplace_back(TxKey(VoidKey::NegativeInfinity()));
    acquire_write_op_.cc_op_ = CcOperation::Write;
    acquire_write_op_.protocol_ = CcProtocol::Locking;

    read_gate_op_.table_name_ = TableName(cluster_config_ccm_name_sv,
                                          TableType::ClusterConfig,
                                          TableEngine::None);
    read_gate_op_.key_ = VoidKey::NegInfTxKey();
    read_gate_op_.hd_result_ = &read_result_;
    read_gate_op_.rec_ = &read_rec_;
    // A write intent: schema ops' read locks pass it, another gate op on this
    // node group waits for it, and stage 2b upgrades it to the write lock.
    read_gate_op_.is_for_write_ = true;

    install_op_.table_name_ = &cluster_config_ccm_name;
    install_op_.keys_.emplace_back(TxKey(VoidKey::NegativeInfinity()));
    install_op_.recs_.push_back(&install_rec_);
    install_op_.op_type_ = OperationType::Update;
    install_op_.write_type_ = PostWriteType::Commit;
}

void ClusterGateOp::Reset(ClusterGateOpType op_type,
                          ClusterGate::Mode mode,
                          TransactionExecution *txm)
{
    op_ = nullptr;
    op_type_ = op_type;
    mode_ = mode;
    error_ = TxErrorCode::NO_ERROR;
    restart_after_release_ = false;
    attempt_ = 0;

    acquire_intent_op_.ResetHandlerTxm(txm);
    acquire_write_op_.ResetHandlerTxm(txm);
    read_result_.ResetTxm(txm);
    read_result_.Reset();
    read_rec_.Reset();
    gate_log_op_.Reset();
    gate_log_op_.ResetHandlerTxm(txm);
    install_op_.ResetHandlerTxm(txm);
    install_rec_.Reset();
}

void ClusterGateOp::ResumeAtInstall(const ClusterGate &gate, uint64_t commit_ts)
{
    op_type_ =
        gate.paused ? ClusterGateOpType::kPause : ClusterGateOpType::kResume;
    mode_ = gate.mode;
    // What FillGateLogRequest() set before the crash. A record without a gate
    // would take the map's topology path.
    install_rec_.SetClusterGate(gate);
    install_rec_.SetVersion(commit_ts);
    op_ = &gate_log_op_;
    gate_log_op_.hd_result_.SetFinished();
}

void ClusterGateOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        ++attempt_;
        LOG(INFO) << "Cluster gate "
                  << (op_type_ == ClusterGateOpType::kPause ? "pause"
                                                            : "resume")
                  << ", attempt " << attempt_
                  << ": reading the gate for write, txn: " << txm->TxNumber();
        txm->commit_ts_ = txm->commit_ts_bound_ + 1;
        RunStage(txm, &read_gate_op_);
    }
    else if (op_ == &read_gate_op_)
    {
        if (read_result_.IsError())
        {
            LOG(ERROR) << "Cluster gate failed to read the cluster config, "
                          "txn: "
                       << txm->TxNumber();
            ForceToFinish(
                txm,
                TransactionExecution::ConvertCcError(read_result_.ErrorCode()));
            return;
        }
        if (op_type_ == ClusterGateOpType::kPause && read_rec_.GatesSchemaOps())
        {
            // Only the write intent here is held; the abort releases it.
            LOG(WARNING) << "Cluster gate pause refused: the gate is already "
                            "paused, txn: "
                         << txm->TxNumber();
            ForceToFinish(txm, TxErrorCode::CLUSTER_GATE_HELD);
            return;
        }
        RunStage(txm, &acquire_intent_op_);
    }
    else if (op_ == &acquire_intent_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm, TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
            return;
        }
        bool conflict =
            acquire_intent_op_.fail_cnt_.load(std::memory_order_relaxed) > 0;
        // One conflict on demand, for tests that run a single node group,
        // where no other gate op or cluster scale can hold the entry.
        CODE_FAULT_INJECTOR("cluster_gate_intent_conflict_once", {
            FaultInject::Instance().InjectFault(
                "cluster_gate_intent_conflict_once", "remove");
            conflict = true;
        });
        if (conflict)
        {
            // Another gate op or a cluster scale holds the entry somewhere:
            // OCC failed rather than waited, so nothing waits on this tx.
            RestartLater(txm, "write intents");
            return;
        }
        RunStage(txm, &acquire_write_op_);
    }
    else if (op_ == &acquire_write_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm, TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
            return;
        }
        if (acquire_write_op_.fail_cnt_.load(std::memory_order_relaxed) > 0)
        {
            // Not a lock conflict, which this stage waits out, but a node
            // group without a leader or failing over.
            RestartLater(txm, "write locks");
            return;
        }
        txm->commit_ts_ = std::max(txm->commit_ts_, acquire_write_op_.MaxTs());
        FillGateLogRequest(txm);
        RunStage(txm, &gate_log_op_);
    }
    else if (op_ == &gate_log_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            // Whether the entry was written is for the log to say: if it was,
            // this node group's replay resumes the op at its install.
            ForceToFinish(txm, TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
            return;
        }
        if (gate_log_op_.hd_result_.IsError())
        {
            if (gate_log_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // Retried with the retry flag, so that the log service looks
                // the entry up in its slot before rejecting it on a term.
                gate_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request()
                    ->set_retry(true);
                RunStage(txm, &gate_log_op_);
                return;
            }
            // Refused, for instance because a node group failed over after
            // granting its lock: the entry is not in the log, so the op can
            // start over like after a conflict.
            RestartLater(txm, "log entry");
            return;
        }
        ACTION_FAULT_INJECTOR("cluster_gate_after_log");
        RunStage(txm, &install_op_);
    }
    else if (op_ == &install_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm, TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
            return;
        }
        if (install_op_.hd_result_.IsError())
        {
            // Both the install and the release are idempotent: a node group
            // that already ran it no longer holds the lock for this tx and
            // ignores the retry.
            RetryInstallLater(txm);
            return;
        }
        if (restart_after_release_)
        {
            BackOffAndRestart(txm);
            return;
        }
        Finish(txm);
    }
}

void ClusterGateOp::RestartLater(TransactionExecution *txm, const char *stage)
{
    LOG(WARNING) << "Cluster gate attempt " << attempt_ << " failed at its "
                 << stage
                 << "; releasing and retrying, txn: " << txm->TxNumber();
    restart_after_release_ = true;
    // The cluster config map releases the tx's locks and commits nothing on
    // tx_op_failed_ts_, on node groups where nothing was granted too.
    txm->commit_ts_ = tx_op_failed_ts_;
    RunStage(txm, &install_op_);
}

void ClusterGateOp::BackOffAndRestart(TransactionExecution *txm)
{
    restart_after_release_ = false;
    // The release dropped the read's lock with every other; if the read is
    // still in the read set, its address may name a recycled lock object.
    txm->rw_set_.DedupRead(read_result_.Value().cce_addr_);
    read_result_.Reset();
    read_rec_.Reset();
    op_ = nullptr;
    // Jittered, so that two gate ops that keep meeting do not retry in step.
    // The sleep op pops itself when the time is up, and Forward() of this op,
    // now on top again, starts over at stage 1.
    txm->sleep_op_.sleep_secs_ = 1 + butil::fast_rand_less_than(3);
    txm->PushOperation(&txm->sleep_op_);
    txm->StartTiming();
}

void ClusterGateOp::RetryInstallLater(TransactionExecution *txm)
{
    op_ = &install_op_;
    txm->PushOperation(&install_op_);
    install_op_.is_running_ = false;
    // The sleep op pops itself when the second is up, and the install, now
    // on top, runs again.
    txm->sleep_op_.sleep_secs_ = 1;
    txm->PushOperation(&txm->sleep_op_);
    txm->StartTiming();
}

void ClusterGateOp::FillGateLogRequest(TransactionExecution *txm)
{
    ClusterGate gate;
    gate.mode = mode_;
    gate.paused = op_type_ == ClusterGateOpType::kPause;
    install_rec_.SetClusterGate(gate);
    install_rec_.SetVersion(txm->commit_ts_);

    gate_log_op_.log_type_ = TxLogType::COMMIT;
    gate_log_op_.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *log_rec =
        gate_log_op_.log_closure_.LogRequest().mutable_write_log_request();
    log_rec->set_tx_term(txm->tx_term_);
    log_rec->set_txn_number(txm->TxNumber());
    log_rec->set_commit_timestamp(txm->commit_ts_);
    ::txlog::ClusterGateLogMessage *gate_msg =
        log_rec->mutable_log_content()->mutable_cluster_gate_log();
    gate_msg->set_mode(gate.mode == ClusterGate::Mode::kDdlDml
                           ? ::txlog::ClusterGateLogMessage_GateMode_GateDdlDml
                           : ::txlog::ClusterGateLogMessage_GateMode_GateDdl);
    gate_msg->set_paused(gate.paused);

    // Unlike cluster scale, the gate writes no log before it locks, so a node
    // group's replay could not restore a lock it lost by failing over. The
    // terms make the log service refuse the entry instead.
    auto &node_terms = *log_rec->mutable_node_terms();
    node_terms.clear();
    for (size_t idx = 0; idx < acquire_write_op_.upload_cnt_; ++idx)
    {
        const AcquireAllResult &res =
            acquire_write_op_.hd_results_[idx].Value();
        node_terms[res.local_cce_addr_.NodeGroupId()] = res.node_term_;
    }
}

void ClusterGateOp::ForceToFinish(TransactionExecution *txm, TxErrorCode err)
{
    error_ = err;
    txm->commit_ts_ = tx_op_failed_ts_;
    Finish(txm);
}

void ClusterGateOp::Finish(TransactionExecution *txm)
{
    // Everything read from the op is copied first: once it is back in the
    // pool, another request may take it and reset it.
    const bool succeeded = txm->commit_ts_ != tx_op_failed_ts_;
    const uint64_t commit_ts = txm->commit_ts_;
    const TxErrorCode error = error_;
    TxResult<size_t> *resp = txm->uint64_resp_;
    txm->uint64_resp_ = nullptr;
    txm->state_stack_.pop_back();

    if (succeeded)
    {
        LOG(INFO) << "Cluster gate "
                  << (op_type_ == ClusterGateOpType::kPause ? "paused"
                                                            : "resumed")
                  << " at " << commit_ts << ", txn: " << txm->TxNumber();
    }

    {
        // Back to the pool, not freed: a remote response that comes in after
        // the op finished still reads one of its stages' cc handler results.
        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        std::lock_guard<std::mutex> lk(shards->op_pool_mux_);
        shards->cluster_gate_op_pool_.push_back(
            std::move(txm->CompositeOpSlot<ClusterGateOp>()));
    }

    if (resp != nullptr)
    {
        if (succeeded)
        {
            resp->Finish(commit_ts);
        }
        else
        {
            resp->FinishError(error);
        }
    }

    if (succeeded)
    {
        txm->Commit();
    }
    else
    {
        txm->Abort();
    }
}

namespace
{
/**
 * @brief Takes a cluster gate op from LocalCcShards' pool, or builds one when
 * the pool is empty.
 *
 * @param txm The transaction that will run it.
 * @return The op, still to be reset for its request.
 */
std::unique_ptr<ClusterGateOp> TakeClusterGateOp(TransactionExecution *txm)
{
    LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
    std::lock_guard<std::mutex> lk(shards->op_pool_mux_);
    if (shards->cluster_gate_op_pool_.empty())
    {
        return std::make_unique<ClusterGateOp>(txm);
    }
    std::unique_ptr<ClusterGateOp> op =
        std::move(shards->cluster_gate_op_pool_.back());
    shards->cluster_gate_op_pool_.pop_back();
    return op;
}
}  // namespace

void TransactionExecution::ProcessTxRequest(ClusterGateTxRequest &req)
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

    // The request's result shares the scan open response, of the same type.
    static_assert(std::is_same_v<size_t, uint64_t>);
    uint64_resp_ = &req.tx_result_;
    std::unique_ptr<ClusterGateOp> &op = CompositeOpSlot<ClusterGateOp>();
    op = TakeClusterGateOp(this);
    op->Reset(req.op_type_, req.mode_, this);
    PushOperation(op.get());
}

void TransactionExecution::RecoverClusterGate(
    const ::txlog::ClusterGateLogMessage &gate_msg)
{
    ClusterGate gate;
    gate.mode =
        gate_msg.mode() == ::txlog::ClusterGateLogMessage_GateMode_GateDdlDml
            ? ClusterGate::Mode::kDdlDml
            : ClusterGate::Mode::kDdl;
    gate.paused = gate_msg.paused();
    std::unique_ptr<ClusterGateOp> &op = CompositeOpSlot<ClusterGateOp>();
    op = TakeClusterGateOp(this);
    op->Reset(
        gate.paused ? ClusterGateOpType::kPause : ClusterGateOpType::kResume,
        gate.mode,
        this);
    op->ResumeAtInstall(gate, commit_ts_);
    PushOperation(op.get());
}

}  // namespace txservice
