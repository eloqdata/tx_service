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
#include "log_agent.h"

#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>

#include <thread>

#include "log.pb.h"
#include "log_util.h"

namespace txlog
{
static const uint32_t RefreshThreshold = 5;

/**
 * @brief Initialize all the log service stub and store them in log_stub_map_.
 * For each log request, we use the current cached log_group_leader to search
 * the stub directly.
 *
 * Note that stub CallMethods are thread safe, which can be shared by
 * Txprocessors and replay/recover threads.
 *
 */
void LogAgent::Init(std::vector<std::string> &ip_list,
                    std::vector<uint16_t> &port_list,
                    const uint32_t start_log_group_id,
                    const uint32_t log_group_replica_num)
{
    for (uint32_t i = 0; i < ip_list.size(); ++i)
    {
        log_nodes_.try_emplace(i, ip_list[i], port_list[i]);
    }
    std::pair<std::unordered_map<uint32_t, LogUtil::RaftGroupConfig>, uint32_t>
        log_raft_group_config = LogUtil::GenerateLogRaftGroupConfig(
            ip_list, port_list, start_log_group_id, log_group_replica_num);

    LogUtil::DumpLogRaftGroupConfig(log_raft_group_config);

    log_group_cnt_ = log_raft_group_config.first.size();
    log_group_replica_num_ = log_group_replica_num;

    for (auto &[log_group_id, raft_group_config] : log_raft_group_config.first)
    {
        std::string log_group_name = raft_group_config.group_name_;
        std::string log_group_conf = raft_group_config.group_conf_;
        log_group_config_map_.try_emplace(log_group_id, raft_group_config);
        if (braft::rtb::update_configuration(log_group_name, log_group_conf) !=
            0)
        {
            LOG(ERROR) << "Fail to register in the routing table the log group "
                       << log_group_conf;
        }

        log_group_ids_.push_back(log_group_id);
        lg_leader_cache_.try_emplace(log_group_id, 0);
        std::vector<uint32_t> group_nodes = raft_group_config.group_nodes_;
        for (uint32_t node_id : group_nodes)
        {
            auto it = log_channel_map_.find(node_id);
            if (it != log_channel_map_.end())
            {
                continue;
            }

            // initialize the channel and stub map.
            auto channel_it = log_channel_map_.try_emplace(
                node_id, std::make_shared<brpc::Channel>());
            brpc::Channel &channel = *channel_it.first->second;

            brpc::ChannelOptions options;
            // The original timeout is 500ms, which is too short to finish the
            // raft log request. Increase it to 10 seconds.
            options.timeout_ms = 10000;
            options.max_retry = 3;
            butil::ip_t ip_t;
            if (0 != butil::str2ip(log_nodes_[node_id].first.c_str(), &ip_t))
            {
                // for case `ips_[node_id]` is hostname format.
                std::string naming_service_url;
                braft::HostNameAddr hostname_addr(log_nodes_[node_id].first,
                                                  log_nodes_[node_id].second);
                braft::HostNameAddr2NSUrl(hostname_addr, naming_service_url);
                if (channel.Init(naming_service_url.c_str(),
                                 braft::LOAD_BALANCER_NAME,
                                 &options) != 0)
                {
                    LOG(ERROR) << "Fail to init channel to "
                               << log_nodes_[node_id].first.c_str() << ":"
                               << log_nodes_[node_id]
                                      .second;  // convert to log service port
                    return;
                }
            }
            else
            {
                if (channel.Init(
                        log_nodes_[node_id].first.c_str(),
                        static_cast<int>(
                            log_nodes_[node_id]
                                .second),  // convert to log service port
                        &options) != 0)
                {
                    LOG(ERROR) << "Fail to init channel to "
                               << log_nodes_[node_id].first.c_str() << ":"
                               << log_nodes_[node_id]
                                      .second;  // convert to log service port
                    return;
                }
            }
        }
    }
}

std::shared_ptr<brpc::Channel> LogAgent::GetChannel(uint32_t log_group_id)
{
    std::shared_lock<std::shared_mutex> lock(config_map_mutex_);
    uint32_t node_id =
        lg_leader_cache_.at(log_group_id).load(std::memory_order_acquire);

    auto log_channel_it = log_channel_map_.find(node_id);
    if (log_channel_it == log_channel_map_.end())
    {
        return nullptr;
    }
    return log_channel_it->second;
}
/**
 * @brief Refresh log leader by asking braft service. Note that
 * braft::rtb::refresh_leader is a blocking call, don't use it in TxProcessor
 * thread.
 */
int LogAgent::RefreshLeader(uint32_t log_group_id, int timeout_ms)
{
    // Leader is unknown or outdated in the route table. Asks the route table to
    // refresh the leader by sending RPCs.
    std::string log_group_name("lg");
    log_group_name.append(std::to_string(log_group_id));
    butil::Status st = braft::rtb::refresh_leader(log_group_name, timeout_ms);

    if (!st.ok())
    {
        // Not sure about the leader, sleep for a while and the ask
        // again.
        LOG(WARNING) << "Fail to refresh_leader : " << st;
        return -1;
    }

    braft::PeerId leader;
    // Selects the leader of the target group from the route table.
    if (braft::rtb::select_leader(log_group_name, &leader) != 0)
    {
        LOG(WARNING) << "Fail to select leader : " << st;
        return -1;
    }
    std::string leader_ip_port;
    if (leader.type_ == braft::PeerId::Type::EndPoint)
    {
        leader_ip_port.append(butil::endpoint2str(leader.addr).c_str());
    }
    else
    {
        leader_ip_port.append(leader.hostname_addr.to_string());
    }

    size_t comma_pos = leader_ip_port.find(':');
    assert(comma_pos != std::string::npos);
    std::string leader_ip_str = leader_ip_port.substr(0, comma_pos);
    uint16_t leader_port = std::stoi(leader_ip_port.substr(comma_pos + 1));
    std::shared_lock<std::shared_mutex> lock(config_map_mutex_);
    bool found = false;
    brpc::Channel *channel = nullptr;
    std::unique_ptr<brpc::Channel> channel_uptr;

    for (size_t idx = 0;
         idx < log_group_config_map_.at(log_group_id).group_nodes_.size();
         ++idx)
    {
        uint32_t node_id =
            log_group_config_map_.at(log_group_id).group_nodes_[idx];
        if (log_nodes_.at(node_id).first == leader_ip_str &&
            log_nodes_.at(node_id).second == leader_port)
        {
            uint32_t old_node_id = lg_leader_cache_.at(log_group_id)
                                       .load(std::memory_order_acquire);

            if (old_node_id != node_id)
            {
                LOG(INFO) << "Refresh log group:" << log_group_id
                          << " leader from node_id: " << old_node_id
                          << " to node_id: " << node_id;
            }
            lg_leader_cache_.at(log_group_id)
                .store(node_id, std::memory_order_release);
            found = true;
            channel = log_channel_map_.at(node_id).get();
            break;
        }
    }

    lock.unlock();

    if (!found)
    {
        channel_uptr = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions options;
        // The original timeout is 500ms, which is too short to finish the
        // raft log request. Increase it to 10 seconds.
        options.timeout_ms = 10000;
        options.max_retry = 3;
        if (leader.type_ != braft::PeerId::Type::EndPoint)
        {
            // for case `ips_[node_id]` is hostname format.
            std::string naming_service_url;
            braft::HostNameAddr hostname_addr(leader_ip_str, leader_port);
            braft::HostNameAddr2NSUrl(hostname_addr, naming_service_url);
            if (channel_uptr->Init(naming_service_url.c_str(),
                                   braft::LOAD_BALANCER_NAME,
                                   &options) != 0)
            {
                LOG(ERROR) << "Fail to init channel to " << leader_ip_str << ":"
                           << leader_port;
                return -1;
            }
        }
        else
        {
            if (channel_uptr->Init(leader_ip_str.c_str(),
                                   static_cast<int>(leader_port),
                                   &options) != 0)
            {
                LOG(ERROR) << "Fail to init channel to " << leader_ip_str << ":"
                           << leader_port;
                return -1;
            }
        }

        channel = channel_uptr.get();
    }

    LogService_Stub stub(channel);
    GetLogGroupConfigRequest request;
    request.set_log_group_id(log_group_id);
    GetLogGroupConfigResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    stub.GetLogGroupConfig(&cntl, &request, &resp, nullptr);
    if (cntl.Failed())
    {
        LOG(ERROR) << "Failed to GetLogGroupConfig, error text:"
                   << cntl.ErrorText();
        return -1;
    }

    if (resp.error() == true)
    {
        LOG(ERROR) << "Failed to GetLogGroupConfig";
        return -1;
    }

    std::vector<std::string> ips;
    std::vector<uint16_t> ports;
    for (int i = 0; i < resp.ip_size(); ++i)
    {
        ips.push_back(resp.ip(i));
        ports.push_back(resp.port(i));
    }

    if (!UpdateLogGroupConfig(ips, ports, log_group_id))
    {
        // Failed to get latest log group config. It is not safe to visit
        // the new leader now since it might not been added to log group config
        // map yet.
        return -1;
    }

    if (!found)
    {
        lock.lock();
        // If we did not find the leader node before updating group nodes,
        // try to find the leader node again after updating group nodes.
        for (size_t idx = 0;
             idx < log_group_config_map_.at(log_group_id).group_nodes_.size();
             ++idx)
        {
            uint32_t node_id =
                log_group_config_map_.at(log_group_id).group_nodes_[idx];
            if (log_nodes_.at(node_id).first == leader_ip_str &&
                log_nodes_.at(node_id).second == leader_port)
            {
                uint32_t old_node_id = lg_leader_cache_.at(log_group_id)
                                           .load(std::memory_order_acquire);

                if (old_node_id != node_id)
                {
                    LOG(INFO) << "Refresh log group:" << log_group_id
                              << " leader from node_id: " << old_node_id
                              << " to node_id: " << node_id;
                }
                lg_leader_cache_.at(log_group_id)
                    .store(node_id, std::memory_order_release);
                break;
            }
        }
    }

    return 0;
}

void LogAgent::RequestRefreshLeader(uint32_t log_group_id)
{
    std::unique_lock lk(check_leader_mutex_);
    request_check_group_leader_ = log_group_id;
    check_leader_cv_.notify_one();
}

void LogAgent::WriteLog(uint32_t log_group_id,
                        brpc::Controller *controller,
                        const LogRequest *request,
                        LogResponse *response,
                        ::google::protobuf::Closure *done)
{
    auto channel = GetChannel(log_group_id);
    if (channel == nullptr)
    {
        controller->SetFailed("Log channel not found for group " +
                              std::to_string(log_group_id));
        return;
    }
    LogService_Stub stub(channel.get());
    stub.WriteLog(controller, request, response, done);
}

void LogAgent::CheckMigrationIsFinished(
    uint32_t log_group_id,
    brpc::Controller *controller,
    const CheckMigrationIsFinishedRequest *request,
    CheckMigrationIsFinishedResponse *response,
    ::google::protobuf::Closure *done)
{
    auto channel = GetChannel(log_group_id);
    if (channel == nullptr)
    {
        controller->SetFailed("Log channel not found for group " +
                              std::to_string(log_group_id));
        return;
    }
    LogService_Stub stub(channel.get());
    stub.CheckMigrationIsFinished(controller, request, response, done);
}

CheckClusterScaleStatusResponse::Status LogAgent::CheckClusterScaleStatus(
    uint32_t log_group_id, const std::string &id)
{
    assert(log_group_id == 0);

    brpc::Controller cntl;
    LogRequest request;
    CheckClusterScaleStatusRequest *check_cluster_scale_status_req =
        request.mutable_check_scale_status_request();
    check_cluster_scale_status_req->set_id(id);
    check_cluster_scale_status_req->set_log_group_id(log_group_id);
    LogResponse response;

    auto channel = GetChannel(log_group_id);
    if (channel == nullptr)
    {
        LOG(ERROR)
            << "CheckClusterScaleStatus: Log channel not found for group "
            << log_group_id;
        return CheckClusterScaleStatusResponse::Status::
            CheckClusterScaleStatusResponse_Status_UNKNOWN;
    }
    LogService_Stub stub(channel.get());

    cntl.Reset();
    cntl.set_timeout_ms(300);

    stub.CheckClusterScaleStatus(&cntl, &request, &response, nullptr);
    if (cntl.Failed())
    {
        LOG(INFO) << "Failed to send RPC#CheckClusterScaleStatus, error text:"
                  << cntl.ErrorText();
        return CheckClusterScaleStatusResponse::Status::
            CheckClusterScaleStatusResponse_Status_UNKNOWN;
    }

    if (response.response_status() !=
        LogResponse::ResponseStatus::LogResponse_ResponseStatus_Success)
    {
        return CheckClusterScaleStatusResponse::Status::
            CheckClusterScaleStatusResponse_Status_UNKNOWN;
    }

    return response.check_scale_status_response().status();
}

void LogAgent::UpdateCheckpointTs(uint32_t cc_node_group_id,
                                  int64_t term,
                                  uint64_t checkpoint_timestamp)
{
    brpc::Controller cntl;
    // prepare request
    LogRequest req;
    UpdateCheckpointTsRequest *update_ckpt_ts_req =
        req.mutable_update_ckpt_ts_request();
    update_ckpt_ts_req->set_cc_node_group_id(cc_node_group_id);
    update_ckpt_ts_req->set_cc_ng_term(term);
    update_ckpt_ts_req->set_ckpt_timestamp(checkpoint_timestamp);
    LogResponse resp;
    std::shared_lock<std::shared_mutex> lock(config_map_mutex_);
    for (const auto &[lg_id, node_id] : lg_leader_cache_)
    {
        DLOG(INFO) << "UpdateCheckpointTs lg_id:" << lg_id
                   << " node_id:" << node_id.load(std::memory_order_acquire)
                   << " ckpt_ts:" << checkpoint_timestamp;
        cntl.Reset();
        cntl.set_timeout_ms(100);
        update_ckpt_ts_req->set_log_group_id(lg_id);
        auto channel = GetChannel(lg_id);
        if (channel == nullptr)
        {
            LOG(ERROR) << "UpdateCheckpointTs: Log channel not found for group "
                       << lg_id;
            continue;
        }
        LogService_Stub stub(channel.get());
        stub.UpdateCheckpointTs(&cntl, &req, &resp, nullptr);
    }
}

void LogAgent::ReplayLog(uint32_t cc_node_group_id,
                         int64_t term,
                         const std::string &source_ip,
                         uint16_t source_port,
                         int log_group,
                         uint64_t start_ts,
                         std::atomic<bool> &interrupt,
                         bool no_replay)
{
    LogRequest req;
    ReplayLogRequest *replay_log_req = req.mutable_replay_log_request();
    replay_log_req->set_cc_node_group_id(cc_node_group_id);
    replay_log_req->set_term(term);
    replay_log_req->set_source_ip(source_ip);
    replay_log_req->set_source_port(source_port);
    replay_log_req->set_no_replay(no_replay);
    replay_log_req->set_start_ts(start_ts);
    LogResponse resp;
    resp.set_response_status(
        LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);

    std::vector<uint32_t> dest_log_groups;
    if (log_group < 0)  // send ReplayLogRequest to all log groups
    {
        dest_log_groups = log_group_ids_;
    }
    else
    {
        dest_log_groups.push_back(log_group);
    }

    for (auto log_group_id : dest_log_groups)
    {
        // While loop to make sure recover is finished. If not re-sending the
        // message until it succeeds. (Or, the cc node is considered not
        // finishing failover and not being able to serve anyway.)
        while (true)
        {
            // get the leader of log_group using blocking RefreshLeader call.
            int err = RefreshLeader(log_group_id);
            while (err != 0)
            {
                DLOG(INFO) << "Refresh log group " << log_group_id
                           << " leader failed";
                if (interrupt.load(std::memory_order_acquire))
                {
                    return;
                }

                using namespace std::chrono_literals;
                std::this_thread::sleep_for(2s);
                err = RefreshLeader(log_group_id);
            }
            auto channel = GetChannel(log_group_id);
            if (channel == nullptr)
            {
                LOG(ERROR)
                    << "Failed to replay log, log channel not found for group "
                    << log_group_id;
                continue;
            }
            LogService_Stub stub(channel.get());

            replay_log_req->set_log_group_id(log_group_id);
            resp.clear_replay_log_response();
            brpc::Controller cntl;
            cntl.Reset();
            stub.ReplayLog(&cntl, &req, &resp, nullptr);

            auto res_status = resp.response_status();
            if (!cntl.Failed() && res_status ==
                                      LogResponse::ResponseStatus::
                                          LogResponse_ResponseStatus_Success)
            {
                break;
            }

            if (cntl.Failed())
            {
                LOG(ERROR) << "Failed to ReplayLog, cntl failed";
            }

            if (res_status !=
                LogResponse::ResponseStatus::LogResponse_ResponseStatus_Success)
            {
                LOG(ERROR) << "Failed to ReplayLog, status code: "
                           << res_status;
            }

            using namespace std::chrono_literals;
            std::this_thread::sleep_for(2s);

            if (interrupt.load(std::memory_order_acquire))
            {
                return;
            }
        }
    }
}

void LogAgent::RemoveCcNodeGroup(uint32_t cc_node_group_id, int64_t term)
{
    brpc::Controller cntl;
    // prepare request
    LogRequest req;
    RemoveCcNodeGroupRequest *remove_cc_node_group_req =
        req.mutable_remove_cc_node_group_request();
    remove_cc_node_group_req->set_cc_node_group_id(cc_node_group_id);
    remove_cc_node_group_req->set_cc_ng_term(term);
    LogResponse resp;
    std::shared_lock<std::shared_mutex> lock(config_map_mutex_);
    for (const auto &[lg_id, node_id] : lg_leader_cache_)
    {
        DLOG(INFO) << "RemoveCcNodeGroup lg_id:" << lg_id
                   << " node_id:" << node_id.load(std::memory_order_acquire)
                   << ", cc_node_group_id:" << cc_node_group_id
                   << ", term:" << term;
        cntl.Reset();
        cntl.set_timeout_ms(100);
        remove_cc_node_group_req->set_log_group_id(lg_id);
        auto channel = GetChannel(lg_id);
        if (channel == nullptr)
        {
            LOG(ERROR) << "RemoveCcNodeGroup: Log channel not found for group "
                       << lg_id;
            continue;
        }
        LogService_Stub stub(channel.get());
        stub.RemoveCcNodeGroup(&cntl, &req, &resp, nullptr);
    }
}

RecoverTxResponse_TxStatus LogAgent::RecoverTx(uint64_t lock_tx_number,
                                               int64_t lock_tx_coord_term,
                                               uint64_t write_lock_ts,
                                               uint32_t cc_ng_id,
                                               int64_t cc_ng_term,
                                               const std::string &source_ip,
                                               uint16_t source_port,
                                               uint32_t log_group_id)
{
    RecoverTxRequest request;
    RecoverTxResponse response;

    // Create stub on the fly using channel
    auto channel = GetChannel(log_group_id);
    if (channel == nullptr)
    {
        LOG(ERROR) << "RecoverTx: Log channel not found for group "
                   << log_group_id;
        return RecoverTxResponse_TxStatus_RecoverError;
    }
    LogService_Stub stub(channel.get());

    request.set_lock_tx_number(lock_tx_number);
    request.set_lock_tx_coord_term(lock_tx_coord_term);
    request.set_cc_node_group_id(cc_ng_id);
    request.set_cc_ng_term(cc_ng_term);
    request.set_source_ip(source_ip);
    request.set_source_port(source_port);
    request.set_log_group_id(log_group_id);
    request.set_write_lock_ts(write_lock_ts);

    brpc::Controller cntl;
    stub.RecoverTx(&cntl, &request, &response, nullptr);

    // If there is an error, does nothing. The next conflicting tx will try a
    // new recovery.
    return cntl.Failed() ? RecoverTxResponse_TxStatus_RecoverError
                         : response.tx_status();
}

/**
 * @brief TransferLeader is used by test case only.
 */
void LogAgent::TransferLeader(uint32_t log_group_id, uint32_t leader_idx)
{
    TransferRequest req;
    TransferResponse res;
    req.set_lg_id(log_group_id);
    req.set_leader_idx(leader_idx);

    auto channel = GetChannel(log_group_id);
    if (channel == nullptr)
    {
        LOG(ERROR) << "TransferLeader: Log channel not found for group "
                   << log_group_id;
        return;
    }
    LogService_Stub stub(channel.get());
    brpc::Controller cntl;
    cntl.set_timeout_ms(-1);
    stub.TransferLeader(&cntl, &req, &res, nullptr);
    if (cntl.Failed())
    {
        LOG(ERROR) << "Fail the TransferLeader RPC of log group "
                   << log_group_id << ". Error code: " << cntl.ErrorCode()
                   << ". Msg: " << cntl.ErrorText();
    }
    else if (res.error())
    {
        // TODO: consider retry logic.
        LOG(ERROR) << "Fail to transfer the leader of log group "
                   << log_group_id;
    }
}
void LogAgent::CheckLeaderRun()
{
    int request_check_group_leader = -1;
    while (true)
    {
        if (request_check_group_leader != -1)
        {
            RefreshLeader(request_check_group_leader);
            request_check_group_leader = -1;
        }
        else
        {
            for (auto lg_id : log_group_ids_)
            {
                RefreshLeader(lg_id, 500);
            }
        }

        std::unique_lock lk(check_leader_mutex_);
        check_leader_cv_.wait_for(lk,
                                  std::chrono::seconds(RefreshThreshold),
                                  [this] {
                                      return request_check_group_leader_ !=
                                                 -1 ||
                                             check_leader_terminate_;
                                  });
        if (request_check_group_leader_ != -1)
        {
            request_check_group_leader = request_check_group_leader_;
            request_check_group_leader_ = -1;
        }
        if (check_leader_terminate_)
        {
            break;
        }
    }
}

void LogAgent::UpdateLeaderCache(uint32_t lg_id, uint32_t node_id)
{
    std::shared_lock<std::shared_mutex> lock(config_map_mutex_);
    lg_leader_cache_.at(lg_id).store(node_id, std::memory_order_release);
}

bool LogAgent::UpdateLogGroupConfig(std::vector<std::string> &ips,
                                    std::vector<uint16_t> &ports,
                                    uint32_t log_group_id)
{
    std::shared_lock<std::shared_mutex> share_lock(config_map_mutex_);

    // Check if the log group id is valid
    bool found = false;
    for (uint32_t i = 0; i < log_group_ids_.size(); ++i)
    {
        if (log_group_ids_[i] == log_group_id)
        {
            found = true;
            break;
        }
    }
    if (!found)
    {
        LOG(ERROR) << "Log group id " << log_group_id << " not found";
        return false;
    }

    // Create channels to new nodes
    std::unordered_map<uint32_t, std::shared_ptr<brpc::Channel>> new_channels;
    std::unordered_map<uint32_t, std::pair<std::string, uint16_t>>
        new_log_nodes;
    std::vector<uint32_t> new_log_node_ids;
    uint32_t next_node_id = 0;
    for (uint32_t i = 0; i < ips.size(); ++i)
    {
        bool found = false;
        for (const auto &[node_id, log_node] : log_nodes_)
        {
            if (log_node.first == ips[i] && log_node.second == ports[i])
            {
                new_log_node_ids.push_back(node_id);
                found = true;
                break;
            }
        }
        if (!found)
        {
            while (log_nodes_.find(next_node_id) != log_nodes_.end())
            {
                ++next_node_id;
            }
            new_log_nodes.try_emplace(next_node_id, ips[i], ports[i]);
            new_log_node_ids.push_back(next_node_id);
            // Create a new channel
            std::shared_ptr<brpc::Channel> channel =
                std::make_shared<brpc::Channel>();
            brpc::ChannelOptions options;
            // The original timeout is 500ms, which is too short to finish the
            // raft log request. Increase it to 10 seconds.
            options.timeout_ms = 10000;
            options.max_retry = 3;
            butil::ip_t ip_t;
            if (0 != butil::str2ip(ips[i].c_str(), &ip_t))
            {
                // for case `ips_[node_id]` is hostname format.
                std::string naming_service_url;
                braft::HostNameAddr hostname_addr(ips[i], ports[i]);
                braft::HostNameAddr2NSUrl(hostname_addr, naming_service_url);
                if (channel->Init(naming_service_url.c_str(),
                                  braft::LOAD_BALANCER_NAME,
                                  &options) != 0)
                {
                    LOG(ERROR) << "Fail to init channel to " << ips[i].c_str()
                               << ":" << ports[i];
                    return false;
                }
            }
            else
            {
                if (channel->Init(ips[i].c_str(),
                                  static_cast<int>(ports[i]),
                                  &options) != 0)
                {
                    LOG(ERROR) << "Fail to init channel to " << ips[i].c_str()
                               << ":" << ports[i];
                    return false;
                }
            }
            new_channels[next_node_id++] = std::move(channel);
        }
    }
    // sort the log node ids in group in order
    std::sort(new_log_node_ids.begin(), new_log_node_ids.end());
    // Compare with old log node ids in group. Check if any node is removed.

    std::vector<uint32_t> removed_log_node_ids;
    const auto &old_log_node_ids =
        log_group_config_map_.at(log_group_id).group_nodes_;

    // Use a set for efficient lookup of new node IDs
    std::unordered_set<uint32_t> new_node_id_set(new_log_node_ids.begin(),
                                                 new_log_node_ids.end());

    // Find nodes that exist in old group but not in new group
    for (const auto &old_node_id : old_log_node_ids)
    {
        if (new_node_id_set.find(old_node_id) == new_node_id_set.end())
        {
            // This old node is not in the new configuration
            removed_log_node_ids.push_back(old_node_id);
        }
    }

    // Check if we have removed any nodes from cluster
    share_lock.unlock();

    if (removed_log_node_ids.empty() && new_log_nodes.empty())
    {
        // No change in log group config
        return true;
    }

    {
        std::lock_guard<std::shared_mutex> lock(config_map_mutex_);
        // Remove removed nodes from log_nodes_
        for (const auto &node_id : removed_log_node_ids)
        {
            log_nodes_.erase(node_id);
            log_channel_map_.erase(node_id);
        }
        // Add new nodes to log_nodes_
        for (const auto &[node_id, log_node] : new_log_nodes)
        {
            log_nodes_[node_id] = std::move(log_node);
        }

        for (auto &channel_pair : new_channels)
        {
            log_channel_map_[channel_pair.first] =
                std::move(channel_pair.second);
        }
        log_group_config_map_.at(log_group_id).group_nodes_ =
            std::move(new_log_node_ids);
        lg_leader_cache_.try_emplace(log_group_id, log_group_id);

        std::string log_group_name = "lg" + std::to_string(log_group_id);
        std::string log_group_conf = "";
        for (uint32_t i = 0; i < ips.size(); ++i)
        {
            log_group_conf += ips[i] + ":" + std::to_string(ports[i]) + ":0,";
        }
        log_group_conf.pop_back();
        if (braft::rtb::update_configuration(log_group_name, log_group_conf) !=
            0)
        {
            // update conf should never fail unless log group is empty.
            assert(false);
            LOG(ERROR) << "Fail to register in the routing table the log group "
                       << log_group_conf;
        }
    }

    return true;
}
}  // namespace txlog
