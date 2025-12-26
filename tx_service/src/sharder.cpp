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
#include "sharder.h"

#include <brpc/channel.h>
#include <spawn.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "cc_req_base.h"
#include "cc_req_misc.h"
#include "cc_request.pb.h"
#include "cc_shard.h"
#include "fault/cc_node.h"
#include "fault/log_replay_service.h"
#include "remote/cc_node_service.h"
#include "remote/cc_stream_receiver.h"
#include "remote/cc_stream_sender.h"
#include "tx_command.h"
#include "tx_service.h"
#include "tx_worker_pool.h"

// gflags 2.1.1 missing GFLAGS_NAMESPACE. This is a workaround to handle gflags
// ABI issue.
#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#endif

namespace txservice
{
Sharder::Sharder() = default;
Sharder::~Sharder()
{
    // Shutdown() must be called during stop.
    assert(cc_stream_receiver_ == nullptr);
    assert(cc_node_service_ == nullptr);
    assert(recovery_service_ == nullptr);
    assert(tx_worker_pool_ == nullptr);
    assert(sharder_worker_ == nullptr);
}

void Sharder::Shutdown()
{
    LOG(INFO) << "Shutting down the sharder at node #" << node_id_;

    if (cc_stream_receiver_)
    {
        cc_stream_receiver_->Shutdown();
    }
    cc_stream_server_.Stop(0);
    cc_stream_server_.Join();
    cc_stream_receiver_ = nullptr;

    log_agent_interrupt_.store(true, std::memory_order_release);
    if (recovery_service_)
    {
        recovery_service_->Shutdown();
    }
    log_replay_server_.Stop(0);
    log_replay_server_.Join();

    cc_node_server_.Stop(0);
    cc_node_server_.Join();
    cc_node_service_ = nullptr;

    // CcNode will access recovery_service_ to replay log when becoming node
    // group leader, so recovery_service_ should be destructed after all
    // CcNodes are stopped.
    recovery_service_ = nullptr;

    if (local_shards_->store_hd_)
    {
        local_shards_->store_hd_->OnShutdown();
    }

    if (tx_worker_pool_)
    {
        tx_worker_pool_->Shutdown();
        tx_worker_pool_ = nullptr;
    }
    if (sharder_worker_)
    {
        sharder_worker_->Shutdown();
        sharder_worker_ = nullptr;
    }

    LOG(INFO) << "The sharder at node #" << node_id_ << " shut down.";
}

void Sharder::CloseStreamSender()
{
    LOG(INFO) << "Close Stream sender at node #" << node_id_;
    cc_stream_sender_ = nullptr;
}

void Sharder::GetNodeAddress(uint32_t node_id, std::string &ip, uint16_t &port)
{
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);

    auto it = cluster_config_.nodes_configs_->find(node_id);
    if (it != cluster_config_.nodes_configs_->end())
    {
        ip = it->second.host_name_;
        port = it->second.port_;
    }
    else
    {
        ip = "";
        port = 0;
    }
}

int Sharder::Init(
    uint32_t node_id,
    uint32_t ng_id,
    const std::unordered_map<uint32_t, std::vector<NodeConfig>> *ng_configs,
    uint64_t config_version,
    const std::vector<std::string> *txlog_ips,
    const std::vector<uint16_t> *txlog_ports,
    const std::string *hm_ip,
    const uint16_t *hm_port,
    const std::string *hm_bin_path,
    LocalCcShards *local_shards,
    std::unique_ptr<TxLog> log_agent,
    const std::string &local_path,
    const std::string &cluster_config_path,
    const uint16_t rep_group_cnt,
    bool fork_host_manager)
{
    node_id_ = node_id;
    native_ng_ = ng_id;
    local_shards_ = local_shards;
    rep_group_cnt_ = rep_group_cnt;
    log_agent_ = std::move(log_agent);

    cluster_config_.nodes_configs_ =
        std::make_shared<std::unordered_map<uint32_t, NodeConfig>>();
    cluster_config_.ng_ids_ = std::make_shared<std::set<uint32_t>>();
    std::unordered_map<uint32_t, NodeConfig> &nodes_configs =
        *cluster_config_.nodes_configs_;

    {
        std::lock_guard<std::shared_mutex> lk(cluster_cnf_mux_);
        for (uint32_t nid = 0; nid < 1000; nid++)
        {
            ng_leader_cache_[nid].store(nid);
            ng_leader_term_cache_[nid].store(-1);
            leader_term_cache_[nid].store(-1);
            candidate_leader_term_cache_[nid].store(-1);
        }

        standby_node_term_cache_.store(-1, std::memory_order_relaxed);
        candidate_standby_node_term_cache_.store(-1, std::memory_order_relaxed);
        standby_becoming_leader_term_cache_.store(-1, std::memory_order_relaxed);

        if (ng_configs != nullptr)
        {
            for (const auto &pair : *ng_configs)
            {
                std::vector<NodeConfig> group_config;
                for (const auto &config : pair.second)
                {
                    group_config.emplace_back(config);
                }
                cluster_config_.ng_configs_.try_emplace(
                    pair.first, std::move(group_config));
                cluster_config_.ng_ids_->emplace(pair.first);
            }
            cluster_config_.version_ = config_version;
        }
        else
        {
            cluster_config_.ng_configs_.try_emplace(0);
            cluster_config_.ng_ids_->emplace(0);
            cluster_config_.version_ = config_version;
        }

        ExtractNodesConfigs(cluster_config_.ng_configs_, nodes_configs);

        if (txlog_ips != nullptr)
        {
            txlog_ips_ = *txlog_ips;
            txlog_ports_ = *txlog_ports;

            for (uint16_t port : txlog_ports_)
            {
                DLOG(INFO) << "txlog_port = " << port;
            }
        }

        if (log_agent_ != nullptr)
        {
            log_agent_->Init(txlog_ips_, txlog_ports_, 0);
        }

#ifdef EXT_TX_PROC_ENABLED
        tx_worker_pool_ = std::make_unique<TxWorkerPool>(
            "tx", local_shards_->Count() >= 2 ? local_shards_->Count() / 2 : 1);
#else
        tx_worker_pool_ =
            std::make_unique<TxWorkerPool>("tx", local_shards_->Count());
#endif
        sharder_worker_ = std::make_unique<TxWorkerPool>("sharder", 1);

        assert(nodes_configs.find(node_id_) != nodes_configs.end());
        const NodeConfig &node_conf = nodes_configs.at(node_id_);
        host_name_ = node_conf.host_name_;
        port_ = node_conf.port_;
        recovery_service_ = std::make_unique<fault::RecoveryService>(
            *local_shards_,
            GetLogAgent(),
            node_conf.host_name_,
            GET_LOG_REPLAY_RPC_PORT(node_conf.port_));
        if (log_replay_server_.AddService(recovery_service_.get(),
                                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(ERROR)
                << "Failed to start add the log replay service to the log "
                   "replay server.";
            return -1;
        }

        for (uint32_t ng_id = 0; ng_id < cluster_config_.ng_configs_.size();
             ++ng_id)
        {
            for (size_t idx = 0;
                 idx < cluster_config_.ng_configs_.at(ng_id).size();
                 ++idx)
            {
                if (cluster_config_.ng_configs_.at(ng_id).at(idx).node_id_ ==
                    node_id_)
                {
                    cluster_config_.cc_nodes_.try_emplace(
                        ng_id,
                        std::make_shared<fault::CcNode>(
                            ng_id,
                            node_id_,
                            *local_shards_,
                            log_agent_ != nullptr ? log_agent_->LogGroupCount()
                                                  : 0));
                }
            }
        }
    }

    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
    cc_nodes_init_.store(true, std::memory_order_release);

    // Initialize cc stream related structs. Even we do not need it if cluster
    // is running in single node mode, the cluster might scale into multi-node
    // state later.
    cc_stream_receiver_ =
        std::make_unique<remote::CcStreamReceiver>(*local_shards_, msg_pool_);
    if (cc_stream_server_.AddService(cc_stream_receiver_.get(),
                                     brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR)
            << "Failed to add the cc stream service to the cc stream server.";
        return -1;
    }

    cc_stream_sender_ = std::make_unique<remote::CcStreamSender>(msg_pool_);

    cc_node_service_ = std::make_unique<remote::CcNodeService>(*local_shards_);
    if (cc_node_server_.AddService(cc_node_service_.get(),
                                   brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR) << "Failed to add the cc node service to the server.";
        return -1;
    }

    // Start the cc_stream_server_ after TxProcessor start using interface
    // StartCcStreamReceiver().

    brpc::ServerOptions server_options;
    // server_options.num_threads 0 means use default bthread worker count.
    server_options.num_threads = 0;
    if (cc_node_server_.Start(GET_CCNODE_RPC_PORT(port_), &server_options) != 0)
    {
        LOG(ERROR) << "Failed to start the cc node server.";
        return -1;
    }

    // The log replay server uses local_port+3 for receiving streams from log
    // groups.

    if (log_replay_server_.Start(GET_LOG_REPLAY_RPC_PORT(port_),
                                 &server_options) != 0)
    {
        LOG(ERROR) << "Failed to start the log replay server.";
        return -1;
    }

    // Notify host manager that this node has been started
    if (hm_ip && !hm_ip->empty())
    {
        hm_ip_ = *hm_ip;
        hm_port_ = *hm_port;
#ifdef FORK_HM_PROCESS
        // Fork host manager process.
        if (fork_host_manager)
        {
            hm_ip_ = "0.0.0.0";
            assert(hm_bin_path != nullptr);
            DLOG(INFO) << "Forking host manager process with " << *hm_bin_path
                       << ", the hm will be listening on " << hm_ip_ << ":"
                       << hm_port_;

            std::string log_path = local_path + "/tx_service";
            std::string arg_ip = "--hm_ip=" + hm_ip_;
            std::string arg_port = "--hm_port=" + std::to_string(hm_port_);
            std::string arg_raft_log = "--hm_raft_log_path=" + log_path;
            std::string arg_cluster_config_path =
                "--cluster_config_path=" + cluster_config_path;
#if BRPC_WITH_GLOG
            std::string arg_log_dir = "--log_dir=" + FLAGS_log_dir;
#endif

            char *argv[] = {const_cast<char *>("host_manager"),
                            const_cast<char *>(arg_ip.c_str()),
                            const_cast<char *>(arg_port.c_str()),
                            const_cast<char *>(arg_raft_log.c_str()),
                            const_cast<char *>(arg_cluster_config_path.c_str()),
#if BRPC_WITH_GLOG
                            const_cast<char *>(arg_log_dir.c_str()),
#endif
                            nullptr};

            pid_t pid;
            int ret = posix_spawn(
                &pid, hm_bin_path->c_str(), nullptr, nullptr, argv, environ);
            if (ret != 0)
            {
                LOG(FATAL) << "Failed to spawn host manager process, errno: "
                           << ret << " (" << strerror(ret) << ")";
                return -1;
            }
            else
            {
                DLOG(INFO) << "Spawned host manager process with PID: " << pid;
            }
        }
#endif
        int max_retries = 300;
        int retries = 0;
        int delay_ms = 200;
        bool connected = false;
        while (retries < max_retries && !connected)
        {
            if (0 == hm_channel_.Init(hm_ip_.c_str(), hm_port_, nullptr))
            {
                connected = true;
            }
            else
            {
                LOG(WARNING)
                    << "Failed to init channel to host manager. Retrying in "
                    << delay_ms << " ms... (Attempt " << (retries + 1) << " of "
                    << max_retries << ")";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(delay_ms));
                ++retries;
            }
        }
        if (!connected)
        {
            LOG(ERROR) << "Failed to init channel to host manager after "
                       << max_retries << " attempts.";
            return -1;
        }

        remote::HostMangerService_Stub stub(&hm_channel_);
        brpc::Controller cntl;
        remote::StartNodeRequest req;
        remote::StartNodeResponse response;
        req.set_node_id(node_id_);
        req.set_ng_id(native_ng_);
        req.set_config_version(config_version);
        req.set_txservice_pid(getpid());
        if (log_agent_)
        {
            req.set_log_replica_num(log_agent_->LogGroupReplicaNum());
            assert(txlog_ips && txlog_ports);
            for (auto &ip : *txlog_ips)
            {
                req.add_log_ips(ip);
            }
            for (auto port : *txlog_ports)
            {
                req.add_log_ports(port);
            }
        }
        else
        {
            req.clear_log_replica_num();
            req.clear_log_ips();
            req.clear_log_ports();
        }

        // ng members
        for (const auto &ng_config : cluster_config_.ng_configs_)
        {
            remote::NodegroupConfigBuf *ng_buf = req.add_cluster_config();
            ng_buf->set_ng_id(ng_config.first);
            for (auto &member : ng_config.second)
            {
                auto *member_node = ng_buf->add_member_nodes();
                member_node->set_node_id(member.node_id_);
                member_node->set_is_candidate(member.is_candidate_);
            }
        }

        // nodes
        for (const auto &[nid, node_config] : nodes_configs)
        {
            auto node_buf = req.add_node_configs();
            node_buf->set_node_id(node_config.node_id_);
            node_buf->set_host_name(node_config.host_name_);
            node_buf->set_port(GET_CCNODE_RPC_PORT(node_config.port_));
        }

        // Give host manager some time to start, otherwise the RPC connect might
        // be refused and won't be able to reconnect until after 3 seconds
        // (controlled by flag health_check_interval).
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        cntl.set_timeout_ms(500);
        stub.StartNode(&cntl, &req, &response, nullptr);
        for (int retry = 1000; retry > 0 && cntl.Failed(); --retry)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            cntl.Reset();
            cntl.set_timeout_ms(500);
            stub.StartNode(&cntl, &req, &response, nullptr);
        }
        if (cntl.Failed())
        {
            LOG(ERROR) << "Failed to notify host manager on node start"
                       << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
            return -1;
        }
        if (response.error())
        {
            LOG(ERROR) << "Failed to notify host manager on node start.";
            return -1;
        }

        hm_channel_init_.store(true, std::memory_order_release);
    }
    else
    {
        bool retry = false;
        uint64_t start_ts = 0;
        cluster_config_.cc_nodes_.at(native_ng_)
            ->OnLeaderStart(1, start_ts, retry);
        assert(!retry);
        // Notify log service to replay log
        if (log_agent_)
        {
            log_agent_->ReplayLog(native_ng_,
                                  1,
                                  host_name_,
                                  GET_LOG_REPLAY_RPC_PORT(port_),
                                  -1,
                                  start_ts,
                                  log_agent_interrupt_);
        }
        else
        {
            // Test env. Directly finish log replay.
            Sharder::Instance().SetLeaderTerm(native_ng_, 1);
            Sharder::Instance().SetCandidateTerm(native_ng_, -1);
            Sharder::Instance().NodeGroupFinishRecovery(native_ng_);
        }
    }

    return 0;
}

// Used on HashPartition
uint16_t Sharder::ShardBucketIdToCoreIdx(uint16_t bucket_id)
{
    return (bucket_id & 0x3FF) % local_shards_->Count();
}

std::shared_ptr<brpc::Channel> Sharder::GetCcNodeServiceChannel(
    uint32_t node_id)
{
    if (!cc_nodes_init_.load(std::memory_order_acquire))
    {
        return nullptr;
    }
    std::shared_lock<std::shared_mutex> lk(node_channel_mux_);
    auto channel_it = cc_node_service_channels_.find(node_id);
    if (channel_it == cc_node_service_channels_.end())
    {
        // If channel to this node is not initialized yet, try to construct the
        // channel to this node.
        lk.unlock();
        std::unique_lock<std::shared_mutex> unique_lk(node_channel_mux_);
        std::string ip;
        uint16_t port;
        GetNodeAddress(node_id, ip, port);
        if (ip.empty())
        {
            // Invalid node id
            return nullptr;
        }
        channel_it = cc_node_service_channels_.find(node_id);
        if (channel_it == cc_node_service_channels_.end() ||
            channel_it->second == nullptr)
        {
            auto channel = std::make_shared<brpc::Channel>();
            if (channel->Init(ip.c_str(), GET_CCNODE_RPC_PORT(port), NULL) != 0)
            {
                LOG(ERROR) << "Failed to init the cc node service channel.";
                return nullptr;
            }
            if (channel_it == cc_node_service_channels_.end())
            {
                cc_node_service_channels_.try_emplace(node_id, channel);
            }
            else
            {
                channel_it->second = channel;
            }
            return channel;
        }

        return channel_it->second;
    }

    return channel_it->second;
}

std::shared_ptr<brpc::Channel> Sharder::UpdateCcNodeServiceChannel(
    uint32_t node_id, std::shared_ptr<brpc::Channel> old_channel)
{
    if (!cc_nodes_init_.load(std::memory_order_acquire))
    {
        return nullptr;
    }
    std::string ip;
    uint16_t port;
    GetNodeAddress(node_id, ip, port);
    assert(!ip.empty());
    std::unique_lock<std::shared_mutex> lk(node_channel_mux_);
    auto channel_it = cc_node_service_channels_.find(node_id);
    if (channel_it == cc_node_service_channels_.end() ||
        channel_it->second == nullptr)
    {
        auto channel = std::make_shared<brpc::Channel>();
        if (channel->Init(ip.c_str(), GET_CCNODE_RPC_PORT(port), NULL) != 0)
        {
            LOG(ERROR) << "Failed to init the cc node service channel.";
            return nullptr;
        }
        if (channel_it == cc_node_service_channels_.end())
        {
            cc_node_service_channels_.try_emplace(node_id, channel);
        }
        else
        {
            channel_it->second = channel;
        }
        return channel;
    }

    if (channel_it->second == old_channel)
    {
        // No one has updated this channel since we read it, update it by
        // ourselves.
        auto channel = std::make_shared<brpc::Channel>();
        if (channel->Init(ip.c_str(), GET_CCNODE_RPC_PORT(port), NULL) != 0)
        {
            LOG(ERROR) << "Failed to update the cc node service channel.";
            return nullptr;
        }
        channel_it->second = channel;
        return channel_it->second;
    }
    else
    {
        // Someone has already updated this channel, return the updated channel
        // directly.
        return channel_it->second;
    }
}

bool Sharder::CheckLeaderTerm(uint32_t ng_id, int64_t term) const
{
    int64_t node_term = LeaderTerm(ng_id);

    if (node_term < 0)
    {
        // The cc node is not the leader. The cc maps in this node does not
        // own cc entries belonging to this shard/node group.
        return false;
    }

    return term == node_term;
}

int64_t Sharder::LeaderTerm(uint32_t ng_id) const
{
    if (!cc_nodes_init_.load(std::memory_order_acquire))
    {
        return -1;
    }
    return leader_term_cache_[ng_id].load(std::memory_order_acquire);
}

int64_t Sharder::CandidateLeaderTerm(uint32_t ng_id) const
{
    if (!cc_nodes_init_.load(std::memory_order_acquire))
    {
        return -1;
    }

    return candidate_leader_term_cache_[ng_id].load(std::memory_order_acquire);
}

void Sharder::UpdateLeaders()
{
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);
    for (const auto &ng_pair : cluster_config_.ng_configs_)
    {
        UpdateLeader(ng_pair.first);
    }
}

void Sharder::UpdateLeader(uint32_t ng_id)
{
    if (!hm_ip_.empty())
    {
        Sharder::Instance().sharder_worker_->SubmitWork(
            [ng_id, this](size_t)
            {
                brpc::Controller cntl;
                remote::GetLeaderRequest req;
                req.set_ng_id(ng_id);
                remote::GetLeaderResponse resp;
                remote::HostMangerService_Stub stub(&hm_channel_);
                stub.GetLeader(&cntl, &req, &resp, nullptr);
                if (!cntl.Failed() && !resp.error())
                {
                    UpdateLeader(ng_id, resp.node_id());
                }
            });
    }
}

void Sharder::UpdateLeader(uint32_t ng_id, uint32_t node_id, int64_t term)
{
    auto cached_leader_term =
        ng_leader_term_cache_[ng_id].load(std::memory_order_relaxed);
    // If the term is passed and out-dated, skip it.
    if (term != -1 && cached_leader_term != -1 && term <= cached_leader_term)
    {
        DLOG(INFO) << "skip out-dated node group leader update, ng: " << ng_id
                   << ", term: " << term
                   << ", cached term: " << cached_leader_term;
        return;
    }

    while (!ng_leader_term_cache_[ng_id].compare_exchange_weak(
        cached_leader_term, term))
    {
        if (term != -1 && cached_leader_term != -1 &&
            term <= cached_leader_term)
        {
            DLOG(INFO) << "skip out-dated node group leader update, ng: "
                       << ng_id << ", term: " << term
                       << ", cached term: " << cached_leader_term;
            return;
        }
    }
    LOG(INFO) << "update node group ng" << ng_id
              << " leader to node_id: " << node_id;
    ng_leader_cache_[ng_id].store(node_id, std::memory_order_release);
}

void Sharder::FinishLogReplay(uint32_t cc_ng_id,
                              int64_t cc_ng_term,
                              uint32_t log_group_id,
                              uint32_t latest_txn_no,
                              uint64_t latest_commit_ts,
                              uint64_t last_ckpt_ts)
{
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it == cluster_config_.cc_nodes_.end())
    {
        return;
    }

    find_it->second->FinishLogGroupReplay(
        log_group_id, cc_ng_term, latest_txn_no, last_ckpt_ts);

    // The ts_base must be bigger than the node group's last_ckpt_ts and
    // latest_commit_ts of all node groups' committed transactions, so that the
    // commit ts of new txns can be set correctly.
    uint64_t ts_base = std::max(latest_commit_ts, last_ckpt_ts);
    local_shards_->UpdateTsBase(ts_base);
}

bool Sharder::CheckLogGroupReplayFinished(uint32_t cc_ng_id,
                                          uint32_t log_group_id,
                                          int64_t cc_ng_term)
{
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it == cluster_config_.cc_nodes_.end())
    {
        return false;
    }

    return find_it->second->CheckLogGroupReplayFinished(log_group_id,
                                                        cc_ng_term);
}

void Sharder::WaitClusterReady()
{
    bool recovery_all_finished = false;
    do
    {
        std::unique_lock<std::mutex> lk(recovery_state_mux_);
        // cluster_config_ might be updated during replay. We need
        // to obtain the latest cluster_config_ before checking in every loop.
        std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);
        for (auto &pair : cluster_config_.ng_configs_)
        {
            uint32_t ng_id = pair.first;
            if (recovered_leader_set_.find(ng_id) ==
                recovered_leader_set_.end())
            {
                uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
                if (dest_node_id == node_id_)
                {
                    // Wait FinishLogReplay emplace recovered_leader_set_.
                }
                else
                {
                    // Send message to remote node to check whether it finish
                    // the log recovery. we use the cc_stream_sender and
                    // cc_stream_receiver to test whether the stream is
                    // established or not.
                    remote::CcMessage send_msg;

                    send_msg.set_type(
                        remote::CcMessage::MessageType::
                            CcMessage_MessageType_RecoverStateCheckRequest);

                    remote::RecoverStateCheckRequest *recover_req =
                        send_msg.mutable_recover_state_check_req();
                    recover_req->set_src_node_id(node_id_);
                    recover_req->set_node_group_id(ng_id);

                    cc_stream_sender_->SendMessageToNg(ng_id, send_msg);
                    // Keep updating leaders to make sure that the request is
                    // sent to the leader of each node group.
                    UpdateLeaders();
                }
            }
        }
        size_t ng_cnt = cluster_config_.ng_configs_.size();
        cnf_lk.unlock();

        recovery_all_finished = recovery_state_cv_.wait_for(
            lk,
            1s,
            [this, ng_cnt]()
            { return recovered_leader_set_.size() == ng_cnt; });
    } while (!recovery_all_finished);
}

void Sharder::WaitNodeBecomeNativeGroupLeader()
{
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(native_ng_);
    while (dest_node_id != node_id_)
    {
        DLOG(INFO) << "Waiting for node #" << node_id_
                   << " to become the leader of native group " << native_ng_
                   << " Current leader is node #" << dest_node_id << ".";
        cnf_lk.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        cnf_lk.lock();
        dest_node_id = Sharder::Instance().LeaderNodeId(native_ng_);
    }
    DLOG(INFO) << "Node #" << node_id_ << " is now the leader of native group "
               << native_ng_ << ".";
}

void Sharder::RecoverTx(uint64_t lock_tx_number,
                        int64_t lock_tx_coord_term,
                        uint64_t write_lock_ts,
                        uint32_t lock_cc_ng_id,
                        int64_t lock_cc_ng_term)
{
    if (LeaderTerm(lock_cc_ng_id) > 0)
    {
        recovery_service_->RecoverTx(lock_tx_number,
                                     lock_tx_coord_term,
                                     write_lock_ts,
                                     lock_cc_ng_id,
                                     lock_cc_ng_term);
    }
}

bool Sharder::OnLeaderStart(uint32_t ng_id,
                            int64_t term,
                            uint64_t &replay_start_ts,
                            bool &retry,
                            uint32_t *next_leader_node)
{
    std::shared_ptr<fault::CcNode> node;
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        auto find_it = cluster_config_.cc_nodes_.find(ng_id);
        // TODO: is this always true when cluster config is changed?
        assert(find_it != cluster_config_.cc_nodes_.end());
        node = find_it->second;
    }

    return node->OnLeaderStart(term, replay_start_ts, retry, next_leader_node);
}

bool Sharder::OnLeaderStop(uint32_t ng_id, int64_t term)
{
    std::shared_ptr<fault::CcNode> node;
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        auto find_it = cluster_config_.cc_nodes_.find(ng_id);
        // TODO: is this always true when cluster config is changed?
        assert(find_it != cluster_config_.cc_nodes_.end());
        node = find_it->second;
    }

    return node->OnLeaderStop(term);
}

bool Sharder::Failover(const std::string &target_host,
                       const uint16_t target_port,
                       std::string &error_message)
{
    uint32_t node_group = txservice::Sharder::Instance().NativeNodeGroup();
    std::shared_ptr<fault::CcNode> node;
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        auto find_it = cluster_config_.cc_nodes_.find(node_group);
        // TODO: is this always true when cluster config is changed?
        assert(find_it != cluster_config_.cc_nodes_.end());
        node = find_it->second;
    }

    return node->Failover(target_host, target_port, error_message);
}

bool Sharder::OnSnapshotReceived(const remote::OnSnapshotSyncedRequest *req)
{
    std::shared_ptr<fault::CcNode> node;
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        auto find_it = cluster_config_.cc_nodes_.find(native_ng_);
        // TODO: is this always true when cluster config is changed?
        assert(find_it != cluster_config_.cc_nodes_.end());
        node = find_it->second;
    }

    return node->OnSnapshotReceived(req);
}

void Sharder::OnStartFollowing(uint32_t ng_id,
                               int64_t term,
                               uint32_t leader_node,
                               bool resubscribe)
{
    std::shared_ptr<fault::CcNode> node;
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        auto find_it = cluster_config_.cc_nodes_.find(ng_id);
        // TODO: is this always true when cluster config is changed?
        assert(find_it != cluster_config_.cc_nodes_.end());
        node = find_it->second;
    }

    node->OnStartFollowing(leader_node, term, resubscribe);
}

void Sharder::LogTransferLeader(uint32_t log_group_id, uint32_t leader_idx)
{
    log_agent_->TransferLeader(log_group_id, leader_idx);
}

void Sharder::CleanCcTable(const TableName &tabname)
{
    return local_shards_->CleanCcTable(tabname);
}

void Sharder::NotifyCheckPointer()
{
    return local_shards_->NotifyCheckPointer();
}

Checkpointer *Sharder::GetCheckpointer()
{
    return local_shards_->GetTxService()->GetCheckpointer();
}

store::DataStoreHandler *Sharder::GetDataStoreHandler()
{
    return local_shards_->store_hd_;
}

CcShard *Sharder::GetCcShard(size_t core_idx)
{
    return local_shards_->GetCcShard(core_idx);
}

std::vector<uint32_t> Sharder::LocalNodeGroups()
{
    std::vector<uint32_t> ngs;
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
    for (auto &pair : cluster_config_.cc_nodes_)
    {
        ngs.push_back(pair.first);
    }
    return ngs;
}

int64_t Sharder::TryPinNodeGroupData(uint32_t cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config_.cc_nodes_.end())
    {
        return find_it->second->PinData();
    }
    return -1;
}

int64_t Sharder::TryPinStandbyNodeGroupData()
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it =
        cluster_config_.cc_nodes_.find(Sharder::Instance().NativeNodeGroup());
    if (find_it != cluster_config_.cc_nodes_.end())
    {
        return find_it->second->StandbyPinData();
    }
    return -1;
}

void Sharder::UnpinNodeGroupData(uint32_t cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config_.cc_nodes_.end())
    {
        find_it->second->UnpinData();
    }
}

uint64_t Sharder::GetNodeGroupCkptTs(uint32_t cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config_.cc_nodes_.end())
    {
        return find_it->second->GetCkptTs();
    }
    return 0;
}

bool Sharder::UpdateNodeGroupCkptTs(uint32_t cc_ng_id, uint64_t ckpt_ts)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config_.cc_nodes_.end())
    {
        return find_it->second->UpdateCkptTs(ckpt_ts);
    }
    return false;
}

void Sharder::SetCommandLineOptions()
{
    // set brpc circuit_breaker max isolation duration smaller than election
    // timeout so that restarted node will join raft group before trying to
    // start a new vote
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "circuit_breaker_max_isolation_duration_ms", "4500");
}

size_t Sharder::GetLocalCcShardsCount()
{
    return local_shards_->Count();
}

std::unordered_map<uint32_t, std::vector<NodeConfig>>
Sharder::AddNodeGroupToCluster(
    const std::vector<std::pair<std::string, uint16_t>> &new_nodes)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
    // Make a copy of the current ng configs.
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs(
        cluster_config_.ng_configs_);

    uint32_t new_node_id = 0;
    if (cluster_config_.nodes_configs_ != nullptr)
    {
        for (auto &[nid, node] : *cluster_config_.nodes_configs_)
        {
            for (auto &new_node : new_nodes)
            {
                if (new_node.first == node.host_name_ &&
                    new_node.second == node.port_)
                {
                    // The new added node is already in the cluster. Return
                    // empty map to indicate the add node group to cluster
                    // fails.
                    LOG(ERROR) << "Add node group to cluster fails since the "
                               << "new added node is already in the cluster";
                    return std::unordered_map<uint32_t,
                                              std::vector<NodeConfig>>();
                }
            }
            new_node_id = std::max(new_node_id, nid);
        }
        new_node_id++;
    }

    // Add a new node group for each new added node, and assign the nodes
    // that are in least number of node groups as the member of new node
    // groups.
    NodeGroupId new_ng_id = 0;
    for (auto &node : new_nodes)
    {
        // For ng_id in ng_configs may be not continuous, try to fill the gaps.
        while (new_ng_configs.find(new_ng_id) != new_ng_configs.end())
        {
            new_ng_id++;
        }
        while (cluster_config_.nodes_configs_->find(new_node_id) !=
               cluster_config_.nodes_configs_->end())
        {
            new_node_id += 1;
        }

        // Add this node to the new node group as the preferred leader.
        std::vector<NodeConfig> members{
            NodeConfig(new_node_id, node.first, node.second, true)};
        new_node_id += 1;
        new_ng_configs.try_emplace(new_ng_id, std::move(members));
        new_ng_id += 1;
    }

    RebalanceNgMembers(new_ng_configs);

    return new_ng_configs;
}

std::unordered_map<uint32_t, std::vector<NodeConfig>>
Sharder::RemoveNodeFromCluster(
    const std::vector<std::pair<std::string, uint16_t>> &removed_nodes)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
    // Make a copy of the current ng configs.
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs(
        cluster_config_.ng_configs_);

    std::unordered_set<uint32_t> removed_ng_ids;
    std::unordered_set<uint32_t> removed_node_ids;

    for (auto &it : *cluster_config_.nodes_configs_)
    {
        for (auto &rm_node : removed_nodes)
        {
            if (rm_node.first == it.second.host_name_ &&
                rm_node.second == it.second.port_)
            {
                removed_node_ids.emplace(it.first);
                break;
            }
        }
    }
    if (removed_node_ids.size() != removed_nodes.size())
    {
        LOG(ERROR) << "Remove node from cluster fails since some nodes are not "
                   << "found in the cluster";
        return std::unordered_map<uint32_t, std::vector<NodeConfig>>();
    }
    for (auto &it : new_ng_configs)
    {
        size_t remaining_candidate_node = 0;
        for (const NodeConfig &member : it.second)
        {
            if (member.is_candidate_)
            {
                remaining_candidate_node++;
                for (const auto &rm_node : removed_nodes)
                {
                    if (rm_node.first == member.host_name_ &&
                        rm_node.second == member.port_)
                    {
                        remaining_candidate_node--;
                        break;
                    }
                }
            }
        }

        // If there's no remaining candidate node, remove the node group.
        if (remaining_candidate_node == 0)
        {
            removed_ng_ids.emplace(it.first);
        }
    }

    // Remove the nodes groups from ng_configs
    for (uint32_t ng_id : removed_ng_ids)
    {
        // Remove the node groups where these nodes are preferred leader.
        new_ng_configs.erase(ng_id);
    }

    for (auto &ng_config : new_ng_configs)
    {
        // Remove these nodes from node groups where they are members.
        for (auto member_it = ng_config.second.begin();
             member_it != ng_config.second.end();)
        {
            if (removed_node_ids.find(member_it->node_id_) !=
                removed_node_ids.end())
            {
                member_it = ng_config.second.erase(member_it);
            }
            else
            {
                member_it++;
            }
        }
    }

    RebalanceNgMembers(new_ng_configs);

    return new_ng_configs;
}

void Sharder::RebalanceNgMembers(
    std::unordered_map<uint32_t, std::vector<NodeConfig>> &new_ng_configs)
{
    uint32_t rep_group_cnt = rep_group_cnt_ < new_ng_configs.size()
                                 ? rep_group_cnt_
                                 : new_ng_configs.size();
    assert(rep_group_cnt > 0);

    // Loop over current ng configs, and build a map from node id
    // to the number of node groups this node is in.
    std::unordered_map<uint32_t, int> node_ng_count;
    std::unordered_map<uint32_t, NodeConfig> nodes_configs;
    for (auto &[ng, members] : new_ng_configs)
    {
        for (auto &node : members)
        {
            auto res_pair = node_ng_count.try_emplace(node.node_id_, 0);
            res_pair.first->second++;
            if (res_pair.second)
            {
                nodes_configs.try_emplace(node.node_id_, node);
            }
        }
    }

    // Rebalance the members in each node groups.
    // Make sure each ng has at least rep_group_cnt members.
    for (auto &config_pair : new_ng_configs)
    {
        // Find members for this new node group.
        std::vector<NodeConfig> &members = config_pair.second;
        while (members.size() < rep_group_cnt)
        {
            int least_node_id = -1;
            int least_node_ng_count = INT32_MAX;
            // Find the node with the least number of node groups.
            for (auto &[node_id, ng_count] : node_ng_count)
            {
                if (ng_count < least_node_ng_count)
                {
                    // check if this node is already in this node group.
                    bool skip = false;
                    for (auto &node : members)
                    {
                        if (node.node_id_ == node_id)
                        {
                            skip = true;
                            break;
                        }
                    }
                    if (!skip)
                    {
                        least_node_id = node_id;
                        least_node_ng_count = ng_count;
                    }
                }
            }
            assert(least_node_id != -1);
            members.emplace_back(nodes_configs[least_node_id]);
            // the filled member should not be preferred leader.
            members.back().is_candidate_ = false;
            node_ng_count[least_node_id]++;
        }
    }
}

std::unordered_map<uint32_t, std::vector<NodeConfig>>
Sharder::AddNodeGroupPeersToCluster(
    uint32_t ng_id,
    const std::vector<std::pair<std::string, uint16_t>> &new_nodes,
    const std::vector<bool> &is_candidate)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
    // Make a copy of the current ng configs.
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs(
        cluster_config_.ng_configs_);
    uint32_t new_node_id = 0;
    auto ng_it = new_ng_configs.find(ng_id);
    assert(ng_it != new_ng_configs.end());
    auto &members = ng_it->second;
    for (size_t i = 0; i < new_nodes.size(); i++)
    {
        for (auto &node : members)
        {
            if (node.host_name_ == new_nodes[i].first &&
                node.port_ == new_nodes[i].second)
            {
                LOG(ERROR) << "Add peers to node group fails since the "
                           << "new added node is already in the node group";
                return std::unordered_map<uint32_t, std::vector<NodeConfig>>();
            }
        }
        while (cluster_config_.nodes_configs_->find(new_node_id) !=
               cluster_config_.nodes_configs_->end())
        {
            new_node_id += 1;
        }
        members.emplace_back(new_node_id,
                             new_nodes[i].first,
                             new_nodes[i].second,
                             is_candidate[i]);
        new_node_id += 1;
    }
    return new_ng_configs;
}

void Sharder::UpdateInMemoryClusterConfig(
    const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
        &new_ng_configs,
    std::shared_ptr<std::unordered_map<uint32_t, NodeConfig>> new_nodes_sptr,
    uint64_t version)
{
    {
        std::unique_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        if (cluster_config_.version_ >= version)
        {
            return;
        }

        LOG(INFO) << "Updating in memory cluster config to version " << version;
        // print out new_ng_configs
        LOG(INFO) << "new_ng_configs:";
        for (const auto &[ng_id, nodes] : new_ng_configs)
        {
            LOG(INFO) << "ng_id: " << ng_id;
            for (const auto &node : nodes)
            {
                LOG(INFO) << "node_id: " << node.node_id_
                          << ", host_name: " << node.host_name_
                          << ", port: " << node.port_;
            }
        }

        // First remove node groups that are removed from the cluster.
        std::unordered_set<uint32_t> removed_ngs;
        for (const auto &[ng_id, _] : cluster_config_.ng_configs_)
        {
            if (new_ng_configs.find(ng_id) == new_ng_configs.end())
            {
                // This ng is removed from new cluster configs
                removed_ngs.emplace(ng_id);
            }
        }
        for (auto ng_id : removed_ngs)
        {
            cluster_config_.ng_configs_.erase(ng_id);
        }

        for (auto &ng_pair : new_ng_configs)
        {
            bool is_member = false;
            for (auto &node : ng_pair.second)
            {
                if (node.node_id_ == node_id_)
                {
                    is_member = true;
                    break;
                }
            }
            if (is_member)
            {
                // If this node is a member of a new node group, create
                // cc node for it.
                auto cc_node_it = cluster_config_.cc_nodes_.find(ng_pair.first);
                if (cc_node_it == cluster_config_.cc_nodes_.end())
                {
                    cluster_config_.cc_nodes_.try_emplace(
                        ng_pair.first,
                        std::make_shared<fault::CcNode>(
                            ng_pair.first,
                            node_id_,
                            *local_shards_,
                            log_agent_->LogGroupCount()));
                }
            }

            // Update ng config in cluster config
            auto ng_cnf_it =
                cluster_config_.ng_configs_.try_emplace(ng_pair.first);
            ng_cnf_it.first->second = ng_pair.second;
        }
        cluster_config_.version_ = version;
        cluster_config_.nodes_configs_ = std::move(new_nodes_sptr);
        if (cluster_config_.ng_ids_.use_count() == 1)
        {
            cluster_config_.ng_ids_->clear();
        }
        else
        {
            cluster_config_.ng_ids_ = std::make_shared<std::set<NodeGroupId>>();
        }
        for (auto &[ng_id, _] : cluster_config_.ng_configs_)
        {
            cluster_config_.ng_ids_->emplace(ng_id);
        }
    }

    assert(cluster_config_.nodes_configs_->size() > 0);
    cc_stream_sender_->UpdateRemoteNodes(*cluster_config_.nodes_configs_);
    cc_stream_sender_->NotifyConnectStream();

    // remove node if it is in subscribed standby node list.
    WaitableCc remove_subscribed_standby_cc(
        [](CcShard &ccs)
        {
            auto nodes_configs = Sharder::Instance().GetAllNodesConfigs();
            std::vector<uint32_t> subscribed_standby_nodes =
                ccs.GetSubscribedStandbys();
            for (auto node_id : subscribed_standby_nodes)
            {
                if (nodes_configs->find(node_id) == nodes_configs->end())
                {
                    ccs.RemoveSubscribedStandby(node_id);
                }
            }

            return true;
        },
        local_shards_->Count());

    for (size_t i = 0; i < local_shards_->Count(); i++)
    {
        local_shards_->EnqueueToCcShard(i, &remove_subscribed_standby_cc);
    }

    remove_subscribed_standby_cc.Wait();
}

void Sharder::UpdateClusterConfig(
    const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
        &new_ng_configs,
    uint64_t version,
    CcRequestBase *cc_req,
    CcShard *cc_shard)
{
    if (ClusterConfigVersion() >= version)
    {
        // If the given version is older than current version, do
        // nothing.
        cc_shard->Enqueue(cc_req);
        return;
    }

    // Since sharder worker is a single thread worker, we don't need to worry
    // about cluster config being updated by multiple threads at the same time.
    sharder_worker_->SubmitWork(
        [this, new_ng_configs, cc_req, cc_shard, version](size_t)
        {
            if (ClusterConfigVersion() >= version)
            {
                // If the given version is older than current version, do
                // nothing.
                cc_shard->Enqueue(cc_req);
                return;
            }
            remote::HostMangerService_Stub stub(&hm_channel_);
            brpc::Controller cntl;
            remote::UpdateClusterConfigRequest req;
            remote::UpdateClusterConfigResponse resp;

            for (const auto &ng_config : new_ng_configs)
            {
                remote::NodegroupConfigBuf *ng_buf =
                    req.add_new_cluster_config();
                ng_buf->set_ng_id(ng_config.first);
                for (auto &member : ng_config.second)
                {
                    auto *member_node = ng_buf->add_member_nodes();
                    member_node->set_node_id(member.node_id_);
                    member_node->set_is_candidate(member.is_candidate_);
                }
            }

            // nodes
            auto new_nodes_sptr =
                std::make_shared<std::unordered_map<uint32_t, NodeConfig>>();
            auto &new_nodes_configs = *new_nodes_sptr;
            ExtractNodesConfigs(new_ng_configs, new_nodes_configs);
            for (const auto &[nid, node_config] : new_nodes_configs)
            {
                auto node_buf = req.add_new_node_configs();
                node_buf->set_node_id(node_config.node_id_);
                node_buf->set_host_name(node_config.host_name_);
                node_buf->set_port(GET_CCNODE_RPC_PORT(node_config.port_));
            }

            // auto last_term = LeaderTerm(node_id_);
            // assert(node_id_ == native_ng_);
            // req.set_ng_id(node_id_);
            req.set_ng_id(native_ng_);
            req.set_config_version(version);
            cntl.set_timeout_ms(10000);
            stub.UpdateClusterConfig(&cntl, &req, &resp, nullptr);
            if (cntl.Failed())
            {
                LOG(ERROR) << "RPC to host manager to update node group "
                              "configs failed "
                           << cntl.ErrorText();
                cc_shard->Enqueue(cc_req);
                return;
            }
            if (resp.error())
            {
                LOG(ERROR)
                    << "Failed to update node group configs on host manager";
                cc_shard->Enqueue(cc_req);
                return;
            }

            UpdateInMemoryClusterConfig(
                new_ng_configs, std::move(new_nodes_sptr), version);

            cc_shard->Enqueue(cc_req);
        });
}

void Sharder::StartCcStreamReceiver()
{
    // The cc_stream_receiver_ object has been add to this server during
    // Sharder::Init().
    brpc::ServerOptions server_options;
    // server_options.num_threads 0 means use default bthread worker count.
    server_options.num_threads = 0;
    if (cc_stream_server_.Start(port_, &server_options) != 0)
    {
        LOG(ERROR) << "Failed to start the cc stream server.";
    }
}

void Sharder::ConnectCcStreamSender()
{
    std::unordered_map<uint32_t, NodeConfig> nodes_configs;
    ExtractNodesConfigs(cluster_config_.ng_configs_, nodes_configs);
    cc_stream_sender_->UpdateRemoteNodes(nodes_configs);
    cc_stream_sender_->NotifyConnectStream();
}

}  // namespace txservice
