#include "raft_host_manager_service.h"

#include <brpc/closure_guard.h>
#include <brpc/http_status_code.h>

#include <vector>

#include "cc_request.pb.h"
#include "raft_host_manager.h"

namespace txservice::remote
{
using namespace host_manager;

// Called by Sharder::INIT(...)
void RaftHostManagerService::StartNode(
    ::google::protobuf::RpcController *controller,
    const StartNodeRequest *request,
    StartNodeResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t node_id = request->node_id();
    LOG(INFO) << "Received start node req from node" << node_id
              << ", tx service pid: " << request->txservice_pid()
              << ", host manager pid: " << getpid();
    std::unordered_map<uint32_t, std::vector<NodeConfig>> cluster_config;
    std::unordered_map<uint32_t, NodeConfig> nodes_config;
    for (int idx = 0; idx < request->node_configs_size(); idx++)
    {
        auto &node = request->node_configs(idx);
        nodes_config.try_emplace(
            node.node_id(), node.node_id(), node.host_name(), node.port());
    }

    for (int idx = 0; idx < request->cluster_config_size(); idx++)
    {
        auto &ng = request->cluster_config(idx);
        std::vector<NodeConfig> ng_nodes;
        for (int member_id = 0; member_id < ng.member_nodes_size(); member_id++)
        {
            NodeConfig tmp_node_conf =
                nodes_config[ng.member_nodes(member_id).node_id()];
            tmp_node_conf.is_candidate_ =
                ng.member_nodes(member_id).is_candidate();
            ng_nodes.push_back(std::move(tmp_node_conf));
        }
        cluster_config.try_emplace(ng.ng_id(), std::move(ng_nodes));
    }

    if (nodes_config.find(node_id) == nodes_config.end())
    {
        // Cannot find node in cluster
        LOG(ERROR) << "Cannot find node in cluster."
                   << "node_id: " << node_id;
        response->set_error(true);
        return;
    }
    if (request->log_ips_size())
    {
        // log service ip list
        std::vector<std::string> log_ips;
        std::vector<uint16_t> log_ports;
        for (int i = 0; i < request->log_ips_size(); i++)
        {
            log_ips.push_back(request->log_ips(i));
            log_ports.push_back(request->log_ports(i));
        }
        hm_.InitLogAgent(log_ips, log_ports, request->log_replica_num());
    }

    // start braft node
    if (!hm_.ConnectToTxNode(node_id,
                             nodes_config.at(node_id),
                             std::move(cluster_config),
                             request->config_version(),
                             request->txservice_pid()))
    {
        // Add node failed
        DLOG(INFO) << "Failed to connect to TxNode " << node_id;
        response->set_error(true);
    }
    else
    {
        DLOG(INFO) << "Succeed to connect to TxNode " << node_id;
        response->set_error(false);
    }
}

void RaftHostManagerService::CheckHealth(
    ::google::protobuf::RpcController *controller,
    const ::google::protobuf::Empty *request,
    ::txservice::remote::HmHealthzHttpResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);
    cntl->http_response().set_content_type("application/json");

    TxNodeStatus status = hm_.Status();
    switch (status)
    {
    /*
    case TxNodeStatus::Uninitialized:
        response->set_hm_status(
            ::txservice::remote::HmHealthzHttpResponse_HmStatus::
                HmHealthzHttpResponse_HmStatus_Uninitialized);
        break;
    */
    case TxNodeStatus::Initializing:
        response->set_hm_status(
            ::txservice::remote::HmHealthzHttpResponse_HmStatus::
                HmHealthzHttpResponse_HmStatus_Initializing);
        break;
    case TxNodeStatus::Started:
        response->set_hm_status(
            ::txservice::remote::HmHealthzHttpResponse_HmStatus::
                HmHealthzHttpResponse_HmStatus_Started);
        break;
    case TxNodeStatus::Terminated:
        response->set_hm_status(
            ::txservice::remote::HmHealthzHttpResponse_HmStatus::
                HmHealthzHttpResponse_HmStatus_Terminated);
        break;
    case TxNodeStatus::Attaching:
        response->set_hm_status(
            ::txservice::remote::HmHealthzHttpResponse_HmStatus::
                HmHealthzHttpResponse_HmStatus_Attaching);
        break;
    case TxNodeStatus::Detaching:
        response->set_hm_status(
            ::txservice::remote::HmHealthzHttpResponse_HmStatus::
                HmHealthzHttpResponse_HmStatus_Detaching);
        break;
    default:
        assert(false);
        break;
    }

    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
}

void RaftHostManagerService::DetachTxService(
    ::google::protobuf::RpcController *controller,
    const ::google::protobuf::Empty *request,
    ::txservice::remote::DetachTxServiceResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    hm_.Detach(response);
}

void RaftHostManagerService::AttachTxService(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::AttachTxServiceRequest *request,
    ::txservice::remote::AttachTxServiceResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    hm_.Attach(request, response);
}

void RaftHostManagerService::Transfer(
    ::google::protobuf::RpcController *controller,
    const TransferRequest *request,
    TransferResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t ng_id = request->ng_id();
    uint32_t node_id = request->node_id();
    int64_t term = request->term();
    LOG(INFO) << "Received leader transfer request from ng " << ng_id
              << ", node " << node_id;
    TransferError error = hm_.TransferLeader(ng_id, node_id, term);
    response->set_error_code(error);
}

void RaftHostManagerService::GetLeader(
    ::google::protobuf::RpcController *controller,
    const GetLeaderRequest *request,
    GetLeaderResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t ng_id = request->ng_id();
    int leader = hm_.GetLeader(ng_id);
    if (leader < 0)
    {
        response->set_error(true);
    }
    else
    {
        response->set_error(false);
        response->set_node_id(leader);
    }
}

void RaftHostManagerService::UpdateClusterConfig(
    ::google::protobuf::RpcController *controller,
    const UpdateClusterConfigRequest *request,
    UpdateClusterConfigResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    response->set_error(false);
    hm_.UpdateClusterConfig(request, response);
}
}  // namespace txservice::remote
