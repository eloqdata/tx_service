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
#include "remote/cc_node_service.h"

#include <brpc/controller.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <mutex>
#include <utility>

#include "cc/local_cc_shards.h"
#include "cc_handler_result.h"
#include "cc_protocol.h"
#include "cc_req_misc.h"
#include "cc_request.h"
#include "cc_request.pb.h"
#include "error_messages.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "sk_generator.h"
#include "tx_operation_result.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_util.h"
#include "type.h"
#include "util.h"

namespace txservice
{
thread_local CcRequestPool<ReadCc> read_pool;

namespace remote
{
CcNodeService::CcNodeService(LocalCcShards &local_shards)
    : local_shards_(local_shards)
{
}

void CcNodeService::OnLeaderStart(::google::protobuf::RpcController *controller,
                                  const OnLeaderStartRequest *request,
                                  OnLeaderStartResponse *response,
                                  ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    NodeGroupId ng_id = request->node_group_id();
    int64_t term = request->node_group_term();

    // Parse latest cluster config
    std::shared_ptr<std::unordered_map<uint32_t, NodeConfig>> node_configs =
        std::make_shared<std::unordered_map<uint32_t, NodeConfig>>();
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> ng_configs;
    for (const auto &node_config : request->node_configs())
    {
        node_configs->try_emplace(node_config.node_id());
        node_configs->at(node_config.node_id()).node_id_ =
            node_config.node_id();
        node_configs->at(node_config.node_id()).host_name_ =
            node_config.host_name();
        node_configs->at(node_config.node_id()).port_ = node_config.port() - 1;
    }
    for (const auto &ng_config : request->cluster_config())
    {
        ng_configs.try_emplace(ng_config.ng_id());
        for (const auto &node : ng_config.member_nodes())
        {
            ng_configs.at(ng_config.ng_id()).emplace_back();
            ng_configs.at(ng_config.ng_id()).back().node_id_ = node.node_id();
            ng_configs.at(ng_config.ng_id()).back().host_name_ =
                node_configs->at(node.node_id()).host_name_;
            ng_configs.at(ng_config.ng_id()).back().port_ =
                node_configs->at(node.node_id()).port_;
            ng_configs.at(ng_config.ng_id()).back().is_candidate_ =
                node.is_candidate();
        }
    }

    // Update cluster config
    Sharder::Instance().UpdateInMemoryClusterConfig(
        ng_configs, std::move(node_configs), request->config_version());

    uint64_t replay_start_ts = 0;
    uint32_t next_leader_node = UINT32_MAX;
    bool retry = false;
    bool success = Sharder::Instance().OnLeaderStart(
        ng_id, term, replay_start_ts, retry, &next_leader_node);
    response->set_error(!success);
    response->set_log_replay_start_ts(replay_start_ts);
    response->set_retry(retry);
    response->set_next_leader_node(next_leader_node);
}

void CcNodeService::OnLeaderStop(::google::protobuf::RpcController *controller,
                                 const OnLeaderStopRequest *request,
                                 OnLeaderChangeResponse *response,
                                 ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    NodeGroupId ng_id = request->node_group_id();
    int64_t term = request->node_group_term();
    bool success = Sharder::Instance().OnLeaderStop(ng_id, term);
    response->set_error(!success);
}

void CcNodeService::OnStartFollowing(
    ::google::protobuf::RpcController *controller,
    const OnStartFollowingRequest *request,
    OnLeaderChangeResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    if (request->node_group_id() != Sharder::Instance().NativeNodeGroup())
    {
        DLOG(ERROR) << "start-following, ng mismatch:"
                    << request->node_group_id()
                    << "!=" << Sharder::Instance().NativeNodeGroup();
        // only call on start following to subscribe on standby nodes
        response->set_error(true);
        return;
    }
    Sharder::Instance().OnStartFollowing(request->node_group_id(),
                                         request->node_group_term(),
                                         request->leader_node_id());
    response->set_error(false);
}

void CcNodeService::CheckTxStatus(::google::protobuf::RpcController *controller,
                                  const CheckTxStatusRequest *request,
                                  CheckTxStatusResponse *response,
                                  ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    TxNumber tx_number = request->tx_number();
    // The higher 4 bytes represent the global core ID, which is a
    // combination of node ID and local core ID.
    uint32_t global_core_id = tx_number >> 32L;
    uint32_t tx_ng = global_core_id >> 10;

    if (!Sharder::Instance().CheckLeaderTerm(tx_ng, request->tx_term()))
    {
        // The target tx's term does not match that of the leader. It means that
        // the leader of the cc node group must have failed over and not contain
        // the tx.
        response->set_tx_status(CheckTxStatusResponse_TxStatus::
                                    CheckTxStatusResponse_TxStatus_NOT_FOUND);
        return;
    }

    CheckTxStatusCc check_tx_cc(tx_number);
    local_shards_.EnqueueCcRequest(global_core_id, &check_tx_cc);
    check_tx_cc.Wait();

    if (check_tx_cc.Exists())
    {
        switch (check_tx_cc.TxStatus())
        {
        case TxnStatus::Committed:
            response->set_tx_status(
                CheckTxStatusResponse_TxStatus::
                    CheckTxStatusResponse_TxStatus_COMMITTED);
            break;
        case TxnStatus::Aborted:
            response->set_tx_status(CheckTxStatusResponse_TxStatus::
                                        CheckTxStatusResponse_TxStatus_ABORTED);
            break;
        case TxnStatus::Unknown:
            response->set_tx_status(
                CheckTxStatusResponse_TxStatus::
                    CheckTxStatusResponse_TxStatus_RESULT_UNKNOWN);
            break;
        default:
            response->set_tx_status(CheckTxStatusResponse_TxStatus::
                                        CheckTxStatusResponse_TxStatus_ONGOING);
            break;
        }
    }
    else
    {
        response->set_tx_status(CheckTxStatusResponse_TxStatus::
                                    CheckTxStatusResponse_TxStatus_NOT_FOUND);
    }
}

/**
 * @brief RPC NotifyNewLeaderStart update the leader cache on the Sharder
 * without referring to the braft service.
 */
void CcNodeService::NotifyNewLeaderStart(
    ::google::protobuf::RpcController *controller,
    const NotifyNewLeaderStartRequest *request,
    NotifyNewLeaderStartResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps you to call done->Run() in RAII style. If you need
    // to process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->ng_id();
    uint32_t node_id = request->node_id();

    // update the leader cache directly.
    Sharder::Instance().UpdateLeader(ng_id, node_id);

    response->set_error(false);
}

/**
 * @brief RPC service: get min start_ts of all active transactions on the node.
 */
void CcNodeService::GetMinTxStartTs(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::GetMinTxStartTsRequest *request,
    ::txservice::remote::GetMinTxStartTsResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->ng_id();
    uint64_t min_ts = UINT64_MAX;
    if (Sharder::Instance().LeaderTerm(local_shards_.NodeId()) > 0)
    {
        auto term = Sharder::Instance().LeaderTerm(ng_id);
        min_ts = local_shards_.StatsLocalActiveSiTxs();

        response->set_term(term);
        response->set_ts(min_ts);
        response->set_error(false);
    }
    else
    {
        response->set_error(true);
    }
}

void CcNodeService::NodeGroupAddPeers(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::NodeGroupAddPeersRequest *request,
    ::txservice::remote::ClusterScaleResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    using namespace txservice;

    if (Sharder::Instance().LeaderTerm(Sharder::Instance().NativeNodeGroup()) <
        0)
    {
        // Node is not leader of requested node group.
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    // Start cluster scale tx and wait for the log is written before
    // returning.
    TxService *tx_service =
        Sharder::Instance().GetLocalCcShards()->GetTxservice();
    TransactionExecution *txm = tx_service->NewTx();
    InitTxRequest init_req;
    // Set isolation level to RepeatableRead to ensure the readlock
    // will be set during the execution of the following
    // ReadTxRequest.
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    // Write all cluster scale log to log group 0. Log group will check
    // if there's another cluster scale event in progress and reject
    // the prepare log request if so.
    init_req.log_group_id_ = 0;
    init_req.Reset();
    txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "Failed to init tx for cluster scale event.";
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    std::vector<std::pair<std::string, uint16_t>> delta_nodes;
    for (int i = 0; i < request->host_list_size(); i++)
    {
        delta_nodes.emplace_back(request->host_list(i), request->port_list(i));
    }
    std::vector<bool> is_candidate;
    for (int i = 0; i < request->is_candidate_size(); i++)
    {
        is_candidate.push_back(request->is_candidate(i));
    }

    ClusterScaleTxRequest scale_req(request->id(),
                                    ClusterScaleOpType::AddNodeGroupPeers,
                                    &delta_nodes,
                                    request->ng_id(),
                                    &is_candidate);
    txm->Execute(&scale_req);
    scale_req.Wait();

    if (scale_req.IsError())
    {
        if (scale_req.ErrorCode() == TxErrorCode::LOG_SERVICE_UNREACHABLE)
        {
            // write log result unkown, need to query new leader later.
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::UNKOWN);
        }
        else if (scale_req.ErrorCode() ==
                 TxErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERROR)
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
            response->set_cluster_config(scale_req.Result());
        }
        else if (scale_req.ErrorCode() ==
                 TxErrorCode::INVALID_CLUSTER_SCALE_REQUEST)
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::
                    INVALID_REQUEST);
            txservice::AbortTx(txm);
        }
        else
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        }

        return;
    }

    response->set_result(
        ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
    response->set_cluster_config(scale_req.Result());
}

void CcNodeService::ClusterAddNodeGroup(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::ClusterAddNodeGroupRequest *request,
    ::txservice::remote::ClusterScaleResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    using namespace txservice;

    if (Sharder::Instance().LeaderTerm(Sharder::Instance().NativeNodeGroup()) <=
        0)
    {
        // Node is not preferred leader of node group.
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    std::vector<std::pair<std::string, uint16_t>> delta_nodes;
    for (int i = 0; i < request->host_list_size(); i++)
    {
        delta_nodes.emplace_back(request->host_list(i), request->port_list(i));
    }
    // Start cluster scale tx and wait for the log is written before
    // returning.
    TxService *tx_service =
        Sharder::Instance().GetLocalCcShards()->GetTxservice();
    TransactionExecution *txm = tx_service->NewTx();
    InitTxRequest init_req;
    // Set isolation level to RepeatableRead to ensure the readlock
    // will be set during the execution of the following
    // ReadTxRequest.
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    // Write all cluster scale log to log group 0. Log group will check
    // if there's another cluster scale event in progress and reject
    // the prepare log request if so.
    init_req.log_group_id_ = 0;
    init_req.Reset();
    txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "Failed to init tx for cluster scale event.";
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    ClusterScaleTxRequest scale_req(
        request->id(), ClusterScaleOpType::AddNodeGroup, &delta_nodes);
    txm->Execute(&scale_req);
    scale_req.Wait();

    if (scale_req.IsError())
    {
        if (scale_req.ErrorCode() == TxErrorCode::LOG_SERVICE_UNREACHABLE)
        {
            // write log result unkown, need to query new leader later.
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::UNKOWN);
        }
        else if (scale_req.ErrorCode() ==
                 TxErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERROR)
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
            response->set_cluster_config(scale_req.Result());
        }
        else if (scale_req.ErrorCode() ==
                 TxErrorCode::INVALID_CLUSTER_SCALE_REQUEST)
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::
                    INVALID_REQUEST);
            txservice::AbortTx(txm);
        }
        else
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        }

        return;
    }

    response->set_result(
        ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
    response->set_cluster_config(scale_req.Result());
}

void CcNodeService::ClusterRemoveNode(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::ClusterRemoveNodeRequest *request,
    ::txservice::remote::ClusterScaleResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    using namespace txservice;

    if (Sharder::Instance().LeaderTerm(Sharder::Instance().NativeNodeGroup()) <=
        0)
    {
        // Node is not preferred leader of node group.
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    // Start cluster scale tx and wait for the log is written before
    // returning.
    TxService *tx_service =
        Sharder::Instance().GetLocalCcShards()->GetTxservice();
    TransactionExecution *txm = tx_service->NewTx();
    InitTxRequest init_req;
    // Set isolation level to RepeatableRead to ensure the readlock
    // will be set during the execution of the following
    // ReadTxRequest.
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    // Write all cluster scale log to log group 0. Log group will check
    // if there's another cluster scale event in progress and reject
    // the prepare log request if so.
    init_req.log_group_id_ = 0;
    init_req.Reset();
    txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "Failed to init tx for cluster scale event.";
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    uint16_t remove_node_count = request->remove_node_count();
    std::vector<std::pair<std::string, uint16_t>> delta_nodes;
    for (int i = 0; i < request->host_list_size(); i++)
    {
        delta_nodes.emplace_back(request->host_list(i), request->port_list(i));
    }

    if (remove_node_count > 0 && delta_nodes.empty())
    {
        // remove specified count of node groups from tail
        std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs =
            Sharder::Instance().GetNodeGroupConfigs();

        uint32_t largest_ng_id = 0;
        for (auto &[ng_id, _] : ng_configs)
        {
            largest_ng_id = std::max(ng_id, largest_ng_id);
        }

        while (remove_node_count > 0 && delta_nodes.size() < ng_configs.size())
        {
            auto it = ng_configs.find(largest_ng_id);
            if (it != ng_configs.end())
            {
                for (auto &member : it->second)
                {
                    if (member.is_candidate_)
                    {
                        delta_nodes.emplace_back(member.host_name_,
                                                 member.port_);
                    }
                }
                remove_node_count--;
            }
            largest_ng_id--;
        }
    }

    if (delta_nodes.empty())
    {
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        txservice::AbortTx(txm);
        return;
    }

    ClusterScaleTxRequest scale_req(
        request->id(), ClusterScaleOpType::RemoveNode, &delta_nodes);
    txm->Execute(&scale_req);
    scale_req.Wait();

    if (scale_req.IsError())
    {
        if (scale_req.ErrorCode() == TxErrorCode::LOG_SERVICE_UNREACHABLE)
        {
            // write log result unkown, need to query new leader later.
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::UNKOWN);
        }
        else if (scale_req.ErrorCode() ==
                 TxErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERROR)
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
            response->set_cluster_config(scale_req.Result());
        }
        else if (scale_req.ErrorCode() ==
                 TxErrorCode::INVALID_CLUSTER_SCALE_REQUEST)
        {
            // The cluster scale request is invalid, the removed node is not
            // in the cluster.
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::
                    INVALID_REQUEST);
            txservice::AbortTx(txm);
        }
        else
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        }
        return;
    }

    response->set_result(
        ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
    response->set_cluster_config(scale_req.Result());
}

/**
 * @brief RPC service: flush all tuples whose commit timestamp less than the
 *  @@request.ckpt_ts into data store.
 */
void CcNodeService::FlushDataAll(::google::protobuf::RpcController *controller,
                                 const FlushDataAllRequest *request,
                                 FlushDataAllResponse *response,
                                 ::google::protobuf::Closure *done)
{
    // This object helps to call done->Run() in RAII style. If you need to
    // process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->node_group_id();
    int64_t ng_term = request->node_group_term();

    std::string_view table_name_sv{request->table_name_str()};
    TableType table_type =
        ToLocalType::ConvertCcTableType(request->table_type());
    txservice::TableEngine table_engine =
        ToLocalType::ConvertTableEngine(request->table_engine());
    TableName table_name = TableName(table_name_sv, table_type, table_engine);

    uint64_t data_sync_ts = request->data_sync_ts();
    bool is_dirty = request->is_dirty();
    if (table_type == TableType::Secondary)
    {
        ACTION_FAULT_INJECTOR("term_FlushDataAllRPC_SK_crashed");
    }

    bthread::Mutex b_thd_mu;
    bthread::ConditionVariable b_thd_cv;
    bool finished = false;
    CcErrorCode error_code = CcErrorCode::NO_ERROR;
    std::thread worker_thd = std::thread(
        [&table_name,
         ng_id,
         &ng_term,
         data_sync_ts,
         is_dirty,
         &b_thd_mu,
         &b_thd_cv,
         &error_code,
         &finished,
         &local_shards = this->local_shards_]()
        {
            int64_t leader_term = INIT_TERM;
            while (Sharder::Instance().LeaderTerm(ng_id) < 0 &&
                   Sharder::Instance().CandidateLeaderTerm(ng_id) > 0)
            {
                // The RPC server can receive the remote request, but this
                // node has not finish log replay, including data(.pk) log and
                // catalog(.table range info) log, so should wait until log
                // replay finished.
                LOG(INFO) << "CcNodeService FlushDataAll on ng#" << ng_id
                          << " waiting log replay finished.";
                std::this_thread::sleep_for(3s);
            }

            if ((leader_term = Sharder::Instance().LeaderTerm(ng_id)) < 0)
            {
                error_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                std::unique_lock b_thd_lk(b_thd_mu);
                finished = true;
                b_thd_cv.notify_one();
                return;
            }
            ng_term = ng_term < 0 ? leader_term : ng_term;
            DLOG(INFO) << "CcNodeService FlushDataAll RPC on #ng" << ng_id
                       << ", with node group term: " << ng_term
                       << ". And flush table:" << table_name.Trace();

            uint64_t table_last_synced_ts = 0;
            std::shared_ptr<DataSyncStatus> status =
                std::make_shared<DataSyncStatus>(ng_id, ng_term, false);

            local_shards.EnqueueDataSyncTaskForTable(table_name,
                                                     ng_id,
                                                     ng_term,
                                                     data_sync_ts,
                                                     table_last_synced_ts,
                                                     false,
                                                     is_dirty,
                                                     false,
                                                     status);

            std::unique_lock<std::mutex> lk(status->mux_);
            status->all_task_started_ = true;
            status->cv_.wait(
                lk, [&status] { return status->unfinished_tasks_ == 0; });

            error_code = status->err_code_;

            std::unique_lock b_thd_lk(b_thd_mu);
            finished = true;
            b_thd_cv.notify_one();
        });

    std::unique_lock lk(b_thd_mu);
    while (!finished)
    {
        b_thd_cv.wait(lk);
    }

    response->set_error_code(static_cast<google::protobuf::int32>(error_code));
    worker_thd.join();
    DLOG(INFO) << "CcNodeService FlushDataAll RPC on #ng" << ng_id
               << ", with node group term: " << ng_term
               << " finished with error: " << CcErrorMessage(error_code);
}

void CcNodeService::InitDataMigration(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::InitMigrationRequest *request,
    ::txservice::remote::InitMigrationResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    // We don't know if this RPC request is stale or new.
    // We just create a new transaction to do data migration.
    // the write_first_prepare_log request of migration transaction will
    // be rejected if this RPC request is stale or if the
    // ClusterScaleTx is finished transaction.

    TxLog *tx_log = Sharder::Instance().GetLogAgent();
    auto cluster_scale_tx_log_ng_id =
        tx_log->GetLogGroupId(request->tx_number());

    TxService *tx_service =
        Sharder::Instance().GetLocalCcShards()->GetTxservice();
    std::vector<TransactionExecution *> txms;
    std::vector<TxNumber> txns;

    // Fill in the migration plan that will be passed to workers
    std::vector<uint16_t> bucket_ids;
    std::vector<NodeGroupId> new_owner_ngs;
    size_t migrate_infos_size =
        static_cast<size_t>(request->migrate_infos_size());
    bucket_ids.reserve(migrate_infos_size);
    new_owner_ngs.reserve(migrate_infos_size);

    for (size_t idx = 0; idx < migrate_infos_size; ++idx)
    {
        const auto &migrate_info = request->migrate_infos(idx);
        bucket_ids.push_back(migrate_info.bucket_id());
        new_owner_ngs.push_back(migrate_info.new_owner());
    }
    // Each worker processes all buckets that is on a specific core.
    int worker_tx_cnt = Sharder::Instance().GetLocalCcShards()->Count();
    // Put all buckets on the same core to the same task.
    std::vector<std::vector<uint16_t>> bucket_ids_per_task(worker_tx_cnt);
    std::vector<std::vector<NodeGroupId>> new_owner_ngs_per_task(worker_tx_cnt);
    for (size_t i = 0; i < bucket_ids.size(); i++)
    {
        uint16_t core_id =
            Sharder::Instance().ShardBucketIdToCoreIdx(bucket_ids[i]);
        bucket_ids_per_task[core_id].push_back(bucket_ids[i]);
        new_owner_ngs_per_task[core_id].push_back(new_owner_ngs[i]);
    }

    // Remove empty tasks.
    for (auto task_it = bucket_ids_per_task.begin();
         task_it != bucket_ids_per_task.end();)
    {
        if (task_it->empty())
        {
            worker_tx_cnt--;
            task_it = bucket_ids_per_task.erase(task_it);
        }
        else
        {
            task_it++;
        }
    }
    for (int i = 0; i < worker_tx_cnt; i++)
    {
        TransactionExecution *txm = tx_service->NewTx();

        InitTxRequest init_req;
        // Set isolation level to RepeatableRead to ensure the readlock
        // will be set during the execution of the following
        // ReadTxRequest.
        init_req.iso_level_ = IsolationLevel::RepeatableRead;
        init_req.protocol_ = CcProtocol::Locking;
        // Set tx node group id
        init_req.tx_ng_id_ = request->orig_owner();
        // Set log node group id to ensure the log will be write to
        // special location.
        init_req.log_group_id_ = cluster_scale_tx_log_ng_id;

        init_req.Reset();
        txm->Execute(&init_req);
        init_req.Wait();

        if (init_req.IsError())
        {
            response->set_success(false);
            for (auto cur_txm : txms)
            {
                AbortTxRequest abort_req;
                cur_txm->Execute(&abort_req);
                abort_req.Wait();
            }
            return;
        }

        txms.push_back(txm);
        txns.push_back(txm->TxNumber());
    }

    std::shared_ptr<DataMigrationStatus> status =
        std::make_shared<DataMigrationStatus>(request->tx_number(),
                                              std::move(bucket_ids_per_task),
                                              std::move(new_owner_ngs_per_task),
                                              std::move(txns));
    // All worker txs has been started, now write the first prepare log,
    // the first tx will write a prepare log that marks the node group
    // migration process has been started. The log contains all of the
    // worker txns, so once this log is written, the migration of this
    // node gorup is always going to succeed.
    DataMigrationTxRequest migrate_req(status);
    txms[0]->Execute(&migrate_req);
    migrate_req.Wait();

    if (migrate_req.IsError())
    {
        if (migrate_req.ErrorCode() ==
            TxErrorCode::DUPLICATE_MIGRATION_TX_ERROR)
        {
            // Migration on this node group is already in progress.
            // We can mark the migration init as success.
            response->set_success(true);
        }
        else
        {
            response->set_success(false);
        }
        // Abort rest of the workers.
        for (size_t i = 1; i < txms.size(); i++)
        {
            AbortTxRequest abort_req;
            txms[i]->Execute(&abort_req);
            abort_req.Wait();
        }
    }
    else
    {
        assert(!migrate_req.IsError());

        response->set_success(true);
        for (size_t i = 1; i < txms.size(); i++)
        {
            // If the log is successfully written, start the rest of the
            // workers.
            DataMigrationTxRequest migrate_req(status);
            txms[i]->Execute(&migrate_req);
            // Wait for shared ptr is passed into txm before destructing
            // tx req.
            migrate_req.Wait();
        }
    }
}

void CcNodeService::CheckClusterScaleStatus(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::ClusterScaleStatusRequest *request,
    ::txservice::remote::ClusterScaleStatusResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    auto *txlog = Sharder::Instance().GetLogAgent();
    auto status = txlog->CheckClusterScaleStatus(0, request->id());

    switch (status)
    {
    case ::txlog::CheckClusterScaleStatusResponse::Status::
        CheckClusterScaleStatusResponse_Status_UNKNOWN:
    {
        response->set_status(ClusterScaleStatus::UNKNOWN);
        break;
    }
    case ::txlog::CheckClusterScaleStatusResponse::Status::
        CheckClusterScaleStatusResponse_Status_NO_STARTED:
    {
        response->set_status(ClusterScaleStatus::NOT_STARTED);
        break;
    }
    case ::txlog::CheckClusterScaleStatusResponse::Status::
        CheckClusterScaleStatusResponse_Status_STARTED:
    {
        response->set_status(ClusterScaleStatus::IN_PROGRESS);
        break;
    }
    case ::txlog::CheckClusterScaleStatusResponse::Status::
        CheckClusterScaleStatusResponse_Status_FINISHED:
    {
        response->set_status(ClusterScaleStatus::FINISHED);
        break;
    }
    default:
        assert(false && "Dead branch");
    }
}

void CcNodeService::GetClusterNodes(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::GetClusterNodesRequest *request,
    ::txservice::remote::GetClusterNodesResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    // First make sure we're the preferred leader of ng since we'll need to
    // put read lock on cluster config cc map when reading cluster config.
    if (Sharder::Instance().LeaderTerm(Sharder::Instance().NativeNodeGroup()) <
        0)
    {
        response->set_error(true);
        return;
    }

    // Put a read lock on cluster config first before reading the node list.
    TxService *tx_service =
        Sharder::Instance().GetLocalCcShards()->GetTxservice();
    auto txm = tx_service->NewTx();
    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.Reset();
    txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        response->set_error(true);
        return;
    }
    ReadTxRequest read_req;
    ClusterConfigRecord rec;
    read_req.Set(&cluster_config_ccm_name,
                 0,
                 VoidKey::NegInfTxKey(),
                 &rec,
                 false,
                 false,
                 true);
    txm->Execute(&read_req);
    read_req.Wait();
    RecordStatus rec_status = read_req.Result().first;

    if (read_req.IsError() || rec_status != RecordStatus::Normal)
    {
        CommitTxRequest commit_req;
        txm->CommitTx(commit_req);
        response->set_error(true);
        return;
    }

    // Now read node list from sharder
    auto ng_configs = Sharder::Instance().GetNodeGroupConfigs();
    std::vector<NodeConfig> all_nodes;
    ExtractNodesConfigs(ng_configs, all_nodes);
    for (const auto &node : all_nodes)
    {
        assert(node.node_id_ != UINT32_MAX && !node.host_name_.empty());
        response->add_host_list(node.host_name_);
        response->add_port_list(node.port_);
    }
    CommitTxRequest commit_req;
    txm->Execute(&commit_req);
    commit_req.Wait();
    response->set_error(false);
}

void CcNodeService::GenerateSkFromPk(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::GenerateSkFromPkRequest *request,
    ::txservice::remote::GenerateSkFromPkResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps to call done->Run() in RAII style. If you need to
    // process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    NodeGroupId ng_id = request->node_group_id();
    uint64_t scan_ts = request->scan_ts();

    std::string_view table_name_sv{request->table_name_str()};
    txservice::TableEngine table_engine =
        ToLocalType::ConvertTableEngine(request->table_engine());
    TableName base_table_name =
        TableName(table_name_sv, TableType::Primary, table_engine);

    int32_t partition_id = request->partition_id();
    const std::string &end_key_str = request->end_key();
    const std::string &start_key_str = request->start_key();

    std::vector<TableName> new_indexes_name;
    new_indexes_name.reserve(request->new_sk_name_str_size());
    for (int idx = 0; idx < request->new_sk_name_str_size(); ++idx)
    {
        std::string_view table_name_sv{request->new_sk_name_str(idx)};
        new_indexes_name.emplace_back(TableName(
            table_name_sv,
            ToLocalType::ConvertCcTableType(request->new_sk_type(idx)),
            table_engine));
    }

    uint64_t tx_number = request->tx_number();
    int64_t tx_term = request->tx_term();

    bthread::Mutex bthd_mux;
    bthread::ConditionVariable bthd_cv;
    bool is_finished = false;
    size_t scanned_pk_items_count = 0;
    CcErrorCode task_res = CcErrorCode::NO_ERROR;
    PackSkError pack_sk_err;
    std::vector<bool> new_indexes_multikey;
    new_indexes_multikey.reserve(new_indexes_name.size());
    std::vector<std::string> new_indexes_multikey_paths;
    new_indexes_multikey_paths.reserve(new_indexes_name.size());
    std::vector<int64_t> ng_terms_vec;
    std::thread worker_thd = std::thread(
        [&base_table_name,
         &ng_id,
         &scan_ts,
         &partition_id,
         &start_key_str,
         &end_key_str,
         &new_indexes_name,
         &bthd_mux,
         &bthd_cv,
         &is_finished,
         &scanned_pk_items_count,
         &task_res,
         &pack_sk_err,
         &new_indexes_multikey,
         &new_indexes_multikey_paths,
         &ng_terms_vec,
         tx_number,
         tx_term]()
        {
            while (Sharder::Instance().LeaderTerm(ng_id) < 0 &&
                   Sharder::Instance().CandidateLeaderTerm(ng_id) > 0)
            {
                // The RPC server can receive the remote request, but this
                // node has not finish log replay, including data(.pk) log and
                // catalog(.table range info) log, so should wait until log
                // replay finished.
                LOG(WARNING) << "CcNodeService GenerateSkFromPk of ng#" << ng_id
                             << " for partition id: " << partition_id
                             << " waiting log replay finished.";
                std::this_thread::sleep_for(3s);
            }

            if (Sharder::Instance().LeaderTerm(ng_id) < 0)
            {
                LOG(WARNING) << "CcNodeService GenerateSkFromPk non-leader "
                             << "node receive this task of ng#" << ng_id
                             << " for partition id: " << partition_id;
                std::unique_lock<bthread::Mutex> lk(bthd_mux);
                task_res = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                is_finished = true;
                bthd_cv.notify_all();
                return;
            }
            DLOG(INFO) << "CcNodeService GenerateSkFromPk RPC of ng#" << ng_id
                       << " for partition id: " << partition_id
                       << ". Base table:" << base_table_name.Trace();

            LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
            std::unique_ptr<SkGenerator> sk_generator = nullptr;
            {
                std::lock_guard<std::mutex> lk(
                    cc_shards->table_index_op_pool_mux_);
                if (cc_shards->sk_generator_pool_.empty())
                {
                    sk_generator = std::make_unique<SkGenerator>();
                }
                else
                {
                    assert(cc_shards->sk_generator_pool_.back() != nullptr);
                    sk_generator =
                        std::move(cc_shards->sk_generator_pool_.back());
                    cc_shards->sk_generator_pool_.pop_back();
                }
            }

            sk_generator->Reset(start_key_str,
                                end_key_str,
                                scan_ts,
                                base_table_name,
                                ng_id,
                                partition_id,
                                tx_number,
                                tx_term,
                                new_indexes_name);
            sk_generator->ProcessTask();

            CcErrorCode cc_err = sk_generator->TaskResult();
            if (cc_err != CcErrorCode::NO_ERROR)
            {
                LOG(ERROR) << "Finish this generate index task of ng#" << ng_id
                           << " for partition id: " << partition_id
                           << " caused by error: " << CcErrorMessage(cc_err);
            }
            else
            {
                scanned_pk_items_count = sk_generator->ScannedItemsCount();
                auto &terms = sk_generator->NodeGroupTerms();
                size_t ng_cnt = terms.size();
                ng_terms_vec.resize(ng_cnt, INIT_TERM);
                for (size_t idx = 0; idx < ng_cnt; ++idx)
                {
                    ng_terms_vec.at(idx) = terms.at(idx);
                }
            }

            std::unique_lock<bthread::Mutex> lk(bthd_mux);
            task_res = cc_err;
            if (task_res == CcErrorCode::NO_ERROR)
            {
                for (uint16_t idx = 0; idx < new_indexes_name.size(); ++idx)
                {
                    // The multikey attribute in SkGenerator is accumulated.
                    new_indexes_multikey.push_back(
                        sk_generator->IsMultiKey(idx));
                    new_indexes_multikey_paths.push_back(
                        sk_generator->SerializeMultiKeyPaths(idx));
                }
            }
            else if (task_res == CcErrorCode::PACK_SK_ERR)
            {
                pack_sk_err = std::move(sk_generator->GetPackSkError());
            }
            is_finished = true;
            bthd_cv.notify_all();

            // recycle skgenerator
            {
                std::lock_guard<std::mutex> lk(
                    cc_shards->table_index_op_pool_mux_);
                cc_shards->sk_generator_pool_.emplace_back(
                    std::move(sk_generator));
            }
        });

    std::unique_lock<bthread::Mutex> lk(bthd_mux);
    while (!is_finished)
    {
        bthd_cv.wait(lk);
    }

    response->set_error_code(static_cast<int>(task_res));
    response->set_pk_items_count(scanned_pk_items_count);
    if (task_res == CcErrorCode::NO_ERROR)
    {
        for (uint16_t idx = 0; idx < new_indexes_name.size(); ++idx)
        {
            remote::MultiKeyAttr *e = response->add_indexes_multikey_attr();
            e->set_multikey(new_indexes_multikey[idx]);
            e->set_multikey_paths(std::move(new_indexes_multikey_paths[idx]));
        }
    }
    else if (task_res == CcErrorCode::PACK_SK_ERR)
    {
        response->set_pack_err_code(pack_sk_err.code_);
        response->set_pack_err_msg(pack_sk_err.message_);
    }
    response->mutable_ng_terms()->Add(ng_terms_vec.cbegin(),
                                      ng_terms_vec.cend());

    worker_thd.join();
    DLOG(INFO) << "CcNodeService GenerateSkFromPk RPC of ng#" << ng_id
               << " for partition id: " << partition_id
               << " finished with error: " << CcErrorMessage(task_res)
               << ". Scanned items count: " << scanned_pk_items_count;
}

void CcNodeService::UploadBatch(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UploadBatchRequest *request,
    ::txservice::remote::UploadBatchResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps to call done->Run() in RAII style. If you need to
    // process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    NodeGroupId ng_id = request->node_group_id();
    int64_t ng_term = request->node_group_term();

    std::string_view table_name_sv{request->table_name_str()};
    TableType table_type =
        ToLocalType::ConvertCcTableType(request->table_type());
    txservice::TableEngine table_engine =
        ToLocalType::ConvertTableEngine(request->table_engine());
    TableName table_name = TableName(table_name_sv, table_type, table_engine);
    UploadBatchType data_type =
        ToLocalType::ConvertUploadBatchType(request->kind());

    CODE_FAULT_INJECTOR("term_UploadBatch_Timeout", {
        // The rpc timeout.
        FaultInject::Instance().InjectFault("term_UploadBatch_Timeout",
                                            "remove");
        bthread::Mutex b_mux_;
        bthread::ConditionVariable b_cv_;
        std::unique_lock<bthread::Mutex> lk(b_mux_);
        b_cv_.wait_for(lk, 2000000);
        DLOG(ERROR) << "UploadBatch service timeout for table: "
                    << table_name.Trace() << " of ng#" << ng_id;
        return;
    });

    DLOG(INFO) << "CcNodeService Received UploadBatch RPC of #ng" << ng_id
               << " for table:" << table_name.Trace();

    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();
    uint32_t batch_size = request->batch_size();

    auto write_entry_tuple =
        UploadBatchCc::WriteEntryTuple(request->keys(),
                                       request->records(),
                                       request->commit_ts(),
                                       request->rec_status());

    size_t finished_req = 0;
    bthread::Mutex req_mux;
    bthread::ConditionVariable req_cv;

    UploadBatchCc req;
    req.Use();
    req.Reset(table_name,
              ng_id,
              ng_term,
              core_cnt,
              batch_size,
              write_entry_tuple,
              req_mux,
              req_cv,
              finished_req,
              data_type);
    for (size_t core = 0; core < core_cnt; ++core)
    {
        cc_shards->EnqueueToCcShard(core, &req);
    }

    {
        std::unique_lock<bthread::Mutex> req_lk(req_mux);
        while (finished_req != 1 || req.InUse())
        {
            req_cv.wait_for(req_lk, 1000000);
        }
    }

    response->set_error_code(ToRemoteType::ConvertCcErrorCode(req.ErrorCode()));
    response->set_ng_term(req.CcNgTerm());
    DLOG(INFO) << "CcNodeService UploadBatch RPC of #ng" << ng_id
               << " finished with error: "
               << static_cast<uint32_t>(req.ErrorCode());
}

void CcNodeService::PublishBucketsMigrating(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::PubBucketsMigratingRequest *request,
    ::txservice::remote::PubBucketsMigratingResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    const NodeGroupId ng_id = request->node_group_id();
    DLOG(INFO) << "CcNodeService received PublishBucketsMigrating RPC of #ng"
               << ng_id << ", is_migrating:" << (int) request->is_migrating();

    if (Sharder::Instance().LeaderTerm(ng_id) < 0)
    {
        response->set_node_group_id(ng_id);
        response->set_success(false);
        LOG(INFO) << "CcNodeService finish PublishBucketsMigrating RPC of #ng"
                  << ng_id << ", current node is not leader !!!";
        return;
    }

    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    bool is_migrating = request->is_migrating();
    cc_shards->SetBucketMigrating(is_migrating);

    if (is_migrating)
    {
        WaitNoNakedBucketRefCc req;
        cc_shards->EnqueueToCcShard(0, &req);
        req.Wait();
    }

    response->set_node_group_id(ng_id);
    response->set_success(true);

    LOG(INFO) << "CcNodeService finish PublishBucketsMigrating RPC of #ng"
              << ng_id;
}

void CcNodeService::UploadRangeSlices(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UploadRangeSlicesRequest *request,
    ::txservice::remote::UploadRangeSlicesResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    std::string_view table_name_sv{request->table_name_str()};
    txservice::TableEngine table_engine =
        ToLocalType::ConvertTableEngine(request->table_engine());
    TableName table_name =
        TableName(table_name_sv, TableType::RangePartition, table_engine);
    NodeGroupId ng_id = request->node_group_id();
    int32_t old_partition_id = request->old_partition_id();
    uint64_t version_ts = request->version_ts();
    int32_t new_partition_id = request->new_partition_id();
    const std::string &keys = request->new_slices_keys();
    const std::string &sizes = request->new_slices_sizes();
    const std::string &status = request->new_slices_status();
    uint32_t keys_num = request->new_slices_num();
    bool has_dml_since_ddl = request->has_dml_since_ddl();

    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();
    uint16_t rand_core = std::rand() % core_cnt;
    // upload range slices info
    UploadRangeSlicesCc req;
    req.Reset(table_name,
              ng_id,
              old_partition_id,
              version_ts,
              new_partition_id,
              has_dml_since_ddl,
              &keys,
              &sizes,
              &status,
              keys_num);
    cc_shards->EnqueueToCcShard(rand_core, &req);
    req.Wait();

    response->set_ng_term(req.CcNgTerm());
    response->set_error_code(ToRemoteType::ConvertCcErrorCode(req.ErrorCode()));

    if (req.ErrorCode() != CcErrorCode::NO_ERROR)
    {
        LOG(INFO) << "CcNodeService finish UploadRangeSlices RPC of #ng"
                  << ng_id << ", ng_term:" << req.CcNgTerm() << ", Range#"
                  << old_partition_id << ", DirtyRange#" << new_partition_id
                  << ", error_code:" << (int) req.ErrorCode();
    }
    else
    {
        DLOG(INFO) << "CcNodeService finish UploadRangeSlices RPC of #ng"
                   << ng_id << ", ng_term:" << req.CcNgTerm() << ", Range#"
                   << old_partition_id << ", DirtyRange#" << new_partition_id
                   << ", error_code:" << (int) req.ErrorCode();
    }
}

void CcNodeService::UploadBatchSlices(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UploadBatchSlicesRequest *request,
    ::txservice::remote::UploadBatchResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps to call done->Run() in RAII style. If you need to
    // process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    NodeGroupId ng_id = request->node_group_id();
    int64_t ng_term = request->node_group_term();

    std::string_view table_name_sv{request->table_name_str()};
    TableType table_type =
        ToLocalType::ConvertCcTableType(request->table_type());
    txservice::TableEngine table_engine =
        ToLocalType::ConvertTableEngine(request->table_engine());
    TableName table_name = TableName(table_name_sv, table_type, table_engine);

    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();

    auto write_entry_tuple =
        UploadBatchSlicesCc::WriteEntryTuple(request->keys(),
                                             request->records(),
                                             request->commit_ts(),
                                             request->rec_status());

    auto slices_info = std::make_shared<UploadBatchSlicesCc::SliceUpdation>();
    slices_info->range_ = request->partition_id();
    slices_info->new_range_ = request->new_partition_id();
    slices_info->version_ts_ = request->version_ts();
    slices_info->slice_idxs_.resize(request->slices_idxs_size());
    for (uint32_t i = 0; i < slices_info->slice_idxs_.size(); i++)
    {
        slices_info->slice_idxs_[i] = request->slices_idxs(i);
    }

    UploadBatchSlicesCc req;
    req.Reset(
        table_name, ng_id, ng_term, core_cnt, write_entry_tuple, slices_info);

    // Select a core randomly to parse items. After parsed, this core will push
    // the request to other cores to emplace keys.
    uint16_t rand_core = std::rand() % core_cnt;
    cc_shards->EnqueueToCcShard(rand_core, &req);
    req.Wait();

    CcErrorCode err = CcErrorCode::NO_ERROR;
    if (req.ErrorCode() != CcErrorCode::NO_ERROR)
    {
        LOG(INFO) << "CcNodeService UploadBatch RPC of #ng" << ng_id
                  << " finished with error: " << static_cast<uint32_t>(err);
        err = req.ErrorCode();
    }
    else
    {
        DLOG(INFO) << "CcNodeService UploadBatch RPC of #ng" << ng_id
                   << " finished with error: " << static_cast<uint32_t>(err);
    }

    response->set_error_code(ToRemoteType::ConvertCcErrorCode(err));
    response->set_ng_term(ng_term);
}

void CcNodeService::FetchPayload(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::FetchPayloadRequest *request,
    ::txservice::remote::FetchPayloadResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    int64_t primary_term = request->primary_leader_term();
    // Verify leader term
    if (!Sharder::Instance().CheckLeaderTerm(request->node_group_id(),
                                             primary_term))
    {
        response->set_error_code((int) CcErrorCode::NG_TERM_CHANGED);
        return;
    }

    ReadCc *read_cc = read_pool.NextRequest();
    bthread::Mutex mux;
    bthread::ConditionVariable cv;
    bool finished = false;
    CcHandlerResult<ReadKeyResult> res(nullptr);

    res.post_lambda_ = [&mux, &cv, &finished](CcHandlerResult<ReadKeyResult> *)
    {
        std::unique_lock<bthread::Mutex> lk(mux);
        finished = true;
        cv.notify_one();
    };

    TableName table_name(
        request->table_name_str(),
        ToLocalType::ConvertCcTableType(request->table_type()),
        ToLocalType::ConvertTableEngine(request->table_engine()));
    // The first 32bits of standby term is the primary node ng term.
    read_cc->Reset(
        &table_name,
        0,
        &request->key_str(),
        request->key_shard_code(),
        response->mutable_payload(),
        ReadType::Inside,
        0,  // this request won't acquire any lock. Just a place holder.
        primary_term,
        0,
        &res,
        IsolationLevel::ReadCommitted,
        CcProtocol::OCC,
        false,
        false,
        nullptr,
        true);

    // Send read cc to get the payload
    local_shards_.EnqueueCcRequest(request->key_shard_code(), read_cc);
    {
        std::unique_lock<bthread::Mutex> lk(mux);
        while (!finished)
        {
            cv.wait(lk);
        }
    }

    if (res.IsError())
    {
        response->set_error_code(
            remote::ToRemoteType::ConvertCcErrorCode(res.ErrorCode()));
        DLOG(INFO) << "Fetch payload failed with " << res.ErrorMsg();
    }
    else
    {
        assert(res.Value().lock_type_ == LockType::NoLock);
        if (res.Value().rec_status_ == RecordStatus::Normal)
        {
            response->set_is_deleted(false);
        }
        else
        {
            assert(res.Value().rec_status_ == RecordStatus::Deleted);
            response->set_is_deleted(true);
        }
        response->set_version(res.Value().ts_);
        response->set_error_code(0);
    }
}

void CcNodeService::FetchCatalog(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::FetchPayloadRequest *request,
    ::txservice::remote::FetchPayloadResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    int64_t primary_term = request->primary_leader_term();
    // Verify leader term
    if (!Sharder::Instance().CheckLeaderTerm(request->node_group_id(),
                                             primary_term))
    {
        response->set_error_code((int) CcErrorCode::NG_TERM_CHANGED);
        return;
    }

    ReadCc *read_cc = read_pool.NextRequest();
    bthread::Mutex mux;
    bthread::ConditionVariable cv;
    bool finished = false;
    CcHandlerResult<ReadKeyResult> res(nullptr);
    res.post_lambda_ = [&mux, &cv, &finished](CcHandlerResult<ReadKeyResult> *)
    {
        std::unique_lock<bthread::Mutex> lk(mux);
        finished = true;
        cv.notify_one();
    };
    TableName table_name(
        request->table_name_str(),
        ToLocalType::ConvertCcTableType(request->table_type()),
        ToLocalType::ConvertTableEngine(request->table_engine()));
    CatalogKey catalog_key;
    const std::string key_str = request->key_str();
    size_t offset = 0;
    catalog_key.Deserialize(key_str.c_str(), offset, nullptr);
    TxKey tx_key(&catalog_key);
    CatalogRecord catalog_rec;
    // The first 32bits of standby term is the primary node ng term.
    read_cc->Reset(
        &table_name,
        0,
        &tx_key,
        request->key_shard_code(),
        &catalog_rec,
        ReadType::Inside,
        0,  // this request won't acquire any lock. Just a place holder.
        primary_term,
        0,
        &res,
        IsolationLevel::ReadCommitted,
        CcProtocol::OCC,
        false,
        false,
        nullptr,
        false,
        false);

    // Send read cc to get the payload
    local_shards_.EnqueueCcRequest(request->key_shard_code(), read_cc);
    {
        std::unique_lock<bthread::Mutex> lk(mux);
        while (!finished)
        {
            cv.wait(lk);
        }
    }

    if (res.IsError())
    {
        response->set_error_code(
            remote::ToRemoteType::ConvertCcErrorCode(res.ErrorCode()));
        DLOG(INFO) << "Fetch catalog failed with " << res.ErrorMsg();
    }
    else
    {
        assert(res.Value().lock_type_ == LockType::NoLock);
        if (res.Value().rec_status_ == RecordStatus::Normal)
        {
            response->set_payload(catalog_rec.Schema()->SchemaImage());
            response->set_is_deleted(false);
        }
        else
        {
            assert(res.Value().rec_status_ == RecordStatus::Deleted);
            response->set_is_deleted(true);
        }
        response->set_version(res.Value().ts_);
        response->set_error_code(0);
    }
}

void CcNodeService::StandbyStartFollowing(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::StandbyStartFollowingRequest *request,
    ::txservice::remote::StandbyStartFollowingResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    bthread::Mutex mux;
    bool err = false;
    uint64_t start_seq;
    WaitableCc add_sub_cc;

    for (uint16_t core_id = 0; core_id < local_shards_.Count(); core_id++)
    {
        add_sub_cc.Reset(

            [ng_id = request->node_group_id(),
             ng_term = request->ng_term(),
             node_id = request->node_id(),
             &err,
             &mux,
             &start_seq](CcShard &ccs)
            {
                if (!Sharder::Instance().CheckLeaderTerm(ng_id, ng_term))
                {
                    std::unique_lock<bthread::Mutex> lk(mux);
                    err = true;
                }
                else
                {
                    std::unique_lock<bthread::Mutex> lk(mux);
                    start_seq = ccs.GetNextForwardSequnceId();
                }

                return true;
            });
        local_shards_.EnqueueCcRequest(core_id, &add_sub_cc);
        add_sub_cc.Wait();
        std::unique_lock<bthread::Mutex> lk(mux);
        if (err)
        {
            response->set_error(true);
            return;
        }

        response->add_start_sequence_id(start_seq);
    }

    auto subscribe_id = Sharder::Instance().GetNextSubscribeId();

    response->set_subscribe_id(subscribe_id);
    response->set_error(false);
}

void CcNodeService::UpdateStandbyCkptTs(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UpdateStandbyCkptTsRequest *request,
    ::txservice::remote::UpdateStandbyCkptTsResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    if (Sharder::Instance().GetDataStoreHandler()->IsSharedStorage())
    {
        Sharder::Instance().UpdateNodeGroupCkptTs(
            request->node_group_id(), request->primary_succ_ckpt_ts());
    }

    // response does not matter
    response->set_error(false);
}

void CcNodeService::UpdateStandbyConsistentTs(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UpdateStandbyConsistentTsRequest *request,
    ::txservice::remote::UpdateStandbyConsistentTsResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    WaitableCc update_consistent_ts_cc;
    for (int32_t seq_grp = 0; seq_grp < request->seq_ids_size(); seq_grp++)
    {
        update_consistent_ts_cc.Reset(
            [ng_id = request->node_group_id(),
             primary_term = request->ng_term(),
             seq_id = request->seq_ids(seq_grp),
             seq_grp,
             consistent_ts = request->consistent_ts()](CcShard &ccs)
            {
                int64_t standby_node_term =
                    Sharder::Instance().StandbyNodeTerm();
                if (standby_node_term < 0)
                {
                    return true;
                }
                int64_t current_primary_term =
                    PrimaryTermFromStandbyTerm(standby_node_term);
                if (current_primary_term == primary_term)
                {
                    ccs.UpdateStandbyConsistentTs(
                        seq_grp, seq_id, consistent_ts, standby_node_term);
                }
                else
                {
                    LOG(INFO) << "primary node term mismatch " << primary_term
                              << " , " << current_primary_term;
                }

                return true;
            }

        );
        local_shards_.EnqueueCcRequest(seq_grp, &update_consistent_ts_cc);
        update_consistent_ts_cc.Wait();
    }

    // response does not matter
    response->set_error(false);
}

void CcNodeService::RequestStorageSnapshotSync(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::StorageSnapshotSyncRequest *request,
    ::txservice::remote::StorageSnapshotSyncResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    auto store_hd = Sharder::Instance().GetLocalCcShards()->store_hd_;
    if (!store_hd)
    {
        // kv store not enabled
        response->set_error(true);
        return;
    }

    int64_t standby_node_term = request->standby_node_term();
    int64_t primary_leader_term = PrimaryTermFromStandbyTerm(standby_node_term);

    if (!Sharder::Instance().CheckLeaderTerm(request->ng_id(),
                                             primary_leader_term))
    {
        response->set_error(true);
        return;
    }

    CODE_FAULT_INJECTOR("disable_sync_snapshot_to_standby", {
        LOG(INFO) << "FaultInject  disable_sync_snapshot_to_standby";
        response->set_error(true);
        return;
    });

    // If kvstore is enabled, we must flush data in-memory to kvstore firstly.
    // For non-shared kvstore, also we create and send the snapshot to standby
    // nodes.
    // Then, notify standby nodes that data committed before subscribe timepoint
    // has been flushed to kvstore. (standby nodes begin fetch record from
    // kvstore on cache miss).
    store::SnapshotManager::Instance().OnSnapshotSyncRequested(request);
    response->set_error(false);
}

void CcNodeService::OnSnapshotSynced(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::OnSnapshotSyncedRequest *request,
    ::txservice::remote::OnSnapshotSyncedResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    auto store_hd = Sharder::Instance().GetLocalCcShards()->store_hd_;
    if (!store_hd)
    {
        // kv store not enabled or does not need to sync
        response->set_error(true);
        return;
    }

    bool succ = Sharder::Instance().OnSnapshotReceived(request);
    response->set_error(!succ);
}

void CcNodeService::FetchNodeInfo(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::FetchNodeInfoRequest *request,
    ::txservice::remote::FetchNodeInfoResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t ng_id = request->ng_id();
    uint32_t node_id = request->node_id();

    response->set_ng_id(ng_id);
    response->set_node_id(node_id);

    if (Sharder::Instance().LeaderTerm(ng_id) > 0)
    {
        response->set_role(NodeRole::LeaderNode);
        response->set_status(NodeStatus::Online);
        return;
    }
    else if (Sharder::Instance().CandidateLeaderTerm(ng_id) > 0)
    {
        response->set_role(NodeRole::LeaderNode);
        response->set_status(NodeStatus::Loading);
        return;
    }
    else if (Sharder::Instance().NativeNodeGroup() == ng_id)
    {
        if (Sharder::Instance().StandbyNodeTerm() > 0)
        {
            response->set_role(NodeRole::StandbyNode);
            response->set_status(NodeStatus::Online);
            return;
        }
        else if (Sharder::Instance().CandidateStandbyNodeTerm() > 0)
        {
            response->set_role(NodeRole::StandbyNode);
            response->set_status(NodeStatus::Loading);
            return;
        }
    }

    response->set_role(NodeRole::VoterNode);
    response->set_status(NodeStatus::Loading);
}

void CcNodeService::ResetStandbySequenceId(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::ResetStandbySequenceIdRequest *request,
    ::txservice::remote::ResetStandbySequenceIdResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t ng_id = request->ng_id();
    uint32_t node_id = request->node_id();

    int64_t standby_node_term = request->standby_node_term();
    Sharder::Instance().GetLocalCcShards()->AddHeartbeatTargetNode(
        node_id, standby_node_term);
    std::unordered_map<uint32_t, uint64_t> seq_ids;
    for (auto i = 0; i < request->seq_grp_size(); i++)
    {
        seq_ids.emplace(request->seq_grp(i), request->seq_id(i));
    }

    WaitableCc reset_seq_cc(
        [ng_id, standby_node_term, node_id, &seq_ids](CcShard &ccs)
        {
            if (Sharder::Instance().CheckLeaderTerm(
                    ng_id, PrimaryTermFromStandbyTerm(standby_node_term)))
            {
                ccs.AddSubscribedStandby(
                    node_id, seq_ids.at(ccs.core_id_), standby_node_term);
            }
            return true;
        },
        seq_ids.size());

    for (auto [seq_grp, _] : seq_ids)
    {
        local_shards_.EnqueueCcRequest(seq_grp, &reset_seq_cc);
    }
    reset_seq_cc.Wait();

    response->set_error(false);
}

void CcNodeService::CreateBackup(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::CreateBackupRequest *request,
    ::txservice::remote::CreateBackupResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->ng_id();
    response->set_ng_id(ng_id);
    int64_t leader_term = Sharder::Instance().LeaderTerm(ng_id);
    if (leader_term < 0)
    {
        DLOG(INFO) << "Reject backup task for this node is not leader";
        response->set_status(BackupTaskStatus::Failed);
        return;
    }

    auto *store_hd = Sharder::Instance().GetDataStoreHandler();
    assert(!request->backup_name().empty());
    assert(store_hd != nullptr);
    if (store_hd && !request->backup_name().empty())
    {
        assert(!request->dest_path().empty());
        auto st = store::SnapshotManager::Instance().CreateBackup(request);
        response->set_status(st);
    }
    else
    {
        LOG(ERROR) << "Failed to create backup for kvstore is disabled.";
        response->set_status(BackupTaskStatus::Failed);
    }
}

void CcNodeService::FetchBackup(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::FetchBackupRequest *request,
    ::txservice::remote::FetchBackupResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->ng_id();
    int64_t leader_term = Sharder::Instance().LeaderTerm(ng_id);
    if (leader_term < 0)
    {
        DLOG(INFO) << "Reject backup task for this node is not leader";
        response->set_status(BackupTaskStatus::Unknown);
        return;
    }

    const std::string &backup_name = request->backup_name();
    auto *store_hd = Sharder::Instance().GetDataStoreHandler();
    assert(store_hd != nullptr);
    if (store_hd && !backup_name.empty())
    {
        auto st = store::SnapshotManager::Instance().GetBackupStatus(
            ng_id, backup_name);
        response->set_status(st);
    }
    else
    {
        LOG(ERROR) << "Failed to create backup for kvstore is disabled.";
        response->set_status(BackupTaskStatus::Unknown);
    }
}

void CcNodeService::TerminateBackup(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::TerminateBackupRequest *request,
    ::txservice::remote::TerminateBackupResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->ng_id();
    const std::string &backup_name = request->backup_name();
    response->set_ng_id(ng_id);

    auto *store_hd = Sharder::Instance().GetDataStoreHandler();
    assert(!backup_name.empty());
    assert(store_hd != nullptr);
    if (store_hd && !backup_name.empty())
    {
        store::SnapshotManager::Instance().TerminateBackup(ng_id, backup_name);
    }
    else
    {
        LOG(ERROR) << "Failed to Terminate backup for kvstore is disabled.";
    }
}

void CcNodeService::CreateClusterBackup(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::CreateClusterBackupRequest *request,
    ::txservice::remote::ClusterBackupResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    // Also print default value in json response.
    auto *brpc_cntl = static_cast<brpc::Controller *>(controller);
    brpc_cntl->set_always_print_primitive_fields(true);


    const std::string &backup_name = request->backup_name();
    const std::string &dest_path = request->dest_path();
    const std::string &dest_user = request->dest_user();
    const std::string &dest_host = request->dest_host();
    response->set_backup_name(backup_name);

    if (backup_name.empty())
    {
        response->set_result("failed");
        return;
    }

    std::unordered_map<txservice::NodeGroupId,
                       txservice::remote::BackupTaskStatus>
        backup_status;
    auto result = BackupUtil::CreateBackup(
        backup_name, dest_path, dest_host, dest_user, backup_status);

    for (auto &[ng_id, st] : backup_status)
    {
        auto *ref = response->add_backup_infos();
        ref->set_ng_id(ng_id);
        ref->set_status(st);
    }

    switch (result)
    {
    case BackupUtil::BackupResult::Failed:
        response->set_result("failed");
        break;
    case BackupUtil::BackupResult::Finished:
        response->set_result("finished");
        break;
    case BackupUtil::BackupResult::Running:
        response->set_result("running");
        break;
    default:
        assert(false);
        break;
    }
}

void CcNodeService::FetchClusterBackup(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::FetchClusterBackupRequest *request,
    ::txservice::remote::ClusterBackupResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    // Also print default value in json response.
    auto *brpc_cntl = static_cast<brpc::Controller *>(controller);
    brpc_cntl->set_always_print_primitive_fields(true);

    const std::string &backup_name = request->backup_name();
    response->set_backup_name(backup_name);

    if (backup_name.empty())
    {
        response->set_result("failed");
        return;
    }

    std::unordered_map<txservice::NodeGroupId,
                       txservice::remote::BackupTaskStatus>
        backup_status;
    auto result = BackupUtil::GetBackupStatus(backup_name, backup_status);

    for (auto &[ng_id, st] : backup_status)
    {
        auto *ref = response->add_backup_infos();
        ref->set_ng_id(ng_id);
        ref->set_status(st);
    }

    switch (result)
    {
    case BackupUtil::BackupResult::Failed:
        response->set_result("failed");
        break;
    case BackupUtil::BackupResult::Finished:
        response->set_result("finished");
        break;
    case BackupUtil::BackupResult::Running:
        response->set_result("running");
        break;
    default:
        assert(false);
        break;
    }
}

void CcNodeService::NotifyShutdownCkpt(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::NotifyShutdownCkptRequest *request,
    ::txservice::remote::NotifyShutdownCkptResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    LOG(INFO) << "Cluster shutting down...";

    // 1. Block external requests
    bool succeed = Sharder::Instance().NotifyShutdown();
    if (!succeed)
    {
        LOG(WARNING) << "Shutdown has been triggered already.";
        response->set_trigger_ckpt_ts(0);
        response->set_status(ShutdownStatus::ShutdownOngoing);
        return;
    }

    // 2. Wait for ongoing tx to finish
    while (!local_shards_.GetTxService()->AllTxFinished())
    {
        bthread_usleep(1000000);
    }

    // 3. Identify leader node groups and trigger final round of checkpoint
    uint64_t trigger_ckpt_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    uint32_t native_ng_id = Sharder::Instance().NativeNodeGroup();
    if (Sharder::Instance().LeaderTerm(native_ng_id) < 0)
    {
        LOG(WARNING) << "This node is no longer leader. ng_id: "
                     << native_ng_id;
        response->set_trigger_ckpt_ts(0);
        response->set_status(ShutdownStatus::ShutdownFailed);
        return;
    }

    local_shards_.GetTxService()->ckpt_.Terminate();

    response->set_trigger_ckpt_ts(trigger_ckpt_ts);
    response->set_status(ShutdownStatus::ShutdownTriggered);
}

void CcNodeService::CheckCkptStatus(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::CheckCkptStatusRequest *request,
    ::txservice::remote::CheckCkptStatusResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t native_ng_id = Sharder::Instance().NativeNodeGroup();
    if (Sharder::Instance().LeaderTerm(native_ng_id) < 0)
    {
        LOG(WARNING) << "This node is no longer leader. ng_id: "
                     << native_ng_id;
        response->set_status(CkptStatus::CkptFailed);
        return;
    }

    if (Sharder::Instance().GetNodeGroupCkptTs(native_ng_id) <=
        request->trigger_ckpt_ts())
    {
        response->set_status(CkptStatus::CkptRunning);
        return;
    }

    response->set_status(CkptStatus::CkptFinished);
    return;
}

void CcNodeService::UpdateClusterConfig(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UpdateClusterConfigRequest *request,
    ::google::protobuf::Empty *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    auto node_configs =
        std::make_shared<std::unordered_map<uint32_t, NodeConfig>>();
    for (int i = 0; i < request->new_node_configs_size(); i++)
    {
        auto node_config_buf = request->new_node_configs(i);
        node_configs->try_emplace(node_config_buf.node_id(),
                                  node_config_buf.node_id(),
                                  node_config_buf.host_name(),
                                  node_config_buf.port() - 1);
    }

    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs;
    for (int idx = 0; idx < request->new_cluster_config_size(); idx++)
    {
        auto ng_config_buf = request->new_cluster_config(idx);

        std::vector<NodeConfig> members;
        for (int nid = 0; nid < ng_config_buf.member_nodes_size(); nid++)
        {
            NodeConfig tmp_node_conf =
                node_configs->at(ng_config_buf.member_nodes(nid).node_id());
            tmp_node_conf.is_candidate_ =
                ng_config_buf.member_nodes(nid).is_candidate();
            members.push_back(std::move(tmp_node_conf));
        }
        new_ng_configs.try_emplace(ng_config_buf.ng_id(), std::move(members));
    }
    Sharder::Instance().UpdateInMemoryClusterConfig(
        new_ng_configs, std::move(node_configs), request->config_version());
}
}  // namespace remote
}  // namespace txservice
