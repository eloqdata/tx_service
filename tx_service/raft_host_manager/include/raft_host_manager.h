#pragma once

#include <braft/node.h>
#include <braft/raft.h>
#include <braft/route_table.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/strings/string_piece.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "cc_request.pb.h"
#include "log_agent.h"
#include "raft_host_manager_service.h"

#define GET_RAFT_NODE_PORT(port) port + 4
#define GET_LOCAL_TX_PORT(port) port - 1

namespace host_manager
{
class RaftHostManager;
class TxNode;
class UpdateClusterConfigFileClosure;
struct NodeConfig
{
public:
    NodeConfig() = default;
    NodeConfig(uint32_t node_id,
               const std::string &host_name,
               uint16_t port,
               bool is_candidate = false)
        : node_id_(node_id),
          host_name_(host_name),
          port_(port),
          is_candidate_(is_candidate)
    {
    }

    NodeConfig(const NodeConfig &rhs)
        : node_id_(rhs.node_id_),
          host_name_(rhs.host_name_),
          port_(rhs.port_),
          is_candidate_(rhs.is_candidate_)
    {
    }

    NodeConfig &operator=(const NodeConfig &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }
        node_id_ = rhs.node_id_;
        host_name_ = rhs.host_name_;
        port_ = rhs.port_;
        is_candidate_ = rhs.is_candidate_;
        return *this;
    }

    bool operator==(const NodeConfig &rhs) const
    {
        return node_id_ == rhs.node_id_ && host_name_ == rhs.host_name_ &&
               port_ == rhs.port_ && is_candidate_ == rhs.is_candidate_;
    }

    uint32_t node_id_{UINT32_MAX};
    std::string host_name_{""};
    uint16_t port_{0};
    bool is_candidate_{false};
};

enum TransferError : int32_t
{
    Ok = 0,
    NotLeader = 1,
    TermOutdated = 2,
    Error = 3
};

class UpdateClusterConfigClosure : public braft::Closure
{
public:
    explicit UpdateClusterConfigClosure(
        const ::txservice::remote::UpdateClusterConfigRequest *request,
        ::txservice::remote::UpdateClusterConfigResponse *response)
        : request_(request), response_(response)
    {
    }
    ~UpdateClusterConfigClosure()
    {
    }

    void Run() override
    {
        // Free closure on exit if change_peers succeed.
        // Each node is only responsible for updating the ng of which it
        // is the preferred leader. So we must be the only one braft ng
        // change.
        std::unique_lock<bthread::Mutex> lk(mux_);
        finished_ = true;
        cv_.notify_one();
    }

    void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        while (!finished_)
        {
            cv_.wait(lk);
        }
    }

    void SetError(int error_code)
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        response_->set_error(error_code);
    }

    const ::txservice::remote::UpdateClusterConfigRequest *request()
    {
        return request_;
    }

    ::txservice::remote::UpdateClusterConfigResponse *response()
    {
        return response_;
    }

private:
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;
    const ::txservice::remote::UpdateClusterConfigRequest *request_;
    ::txservice::remote::UpdateClusterConfigResponse *response_;
    bool finished_{false};
};

class RaftNode : public braft::StateMachine
{
public:
    RaftNode() = delete;
    RaftNode(uint32_t ng_id,
             const std::vector<NodeConfig> &ng_config,
             RaftHostManager &host_manager);
    ~RaftNode()
    {
        if (node_)
        {
            delete node_;
        }
    }
    int Start();
    void Join()
    {
        if (node_)
        {
            node_->join();
        }
        if (request_leader_transfer_thd_.joinable())
        {
            request_leader_transfer_thd_.join();
        }
        if (leader_transfer_thd_.joinable())
        {
            leader_transfer_thd_.join();
        }
    }
    void Shutdown()
    {
        if (node_)
        {
            node_->shutdown(nullptr);
        }
    }
    void UpdateNodeGroupConfig(const std::vector<NodeConfig> &new_config,
                               UpdateClusterConfigFileClosure *done);

    bool UpdateClusterConfig(
        const ::txservice::remote::UpdateClusterConfigRequest *request,
        ::txservice::remote::UpdateClusterConfigResponse *response);
    TransferError TransferLeader(uint32_t node_id);
    TransferError CheckTermAndTransferLeader(uint32_t node_id, int64_t term);
    // This should only be called when ng is deleted from cluster. This will
    // shutdown cc node in this ng and delete cc ng log of this ng.
    void Remove(bool truncate_log);

    const std::vector<NodeConfig> &MemberNodes()
    {
        return ng_config_;
    }

private:
    void on_apply(::braft::Iterator &iter) override;
    void on_leader_start(int64_t term) override;
    void on_leader_stop(const butil::Status &status) override;
    void on_start_following(const ::braft::LeaderChangeContext &ctx) override;
    void on_stop_following(const ::braft::LeaderChangeContext &ctx) override
    {
    }
    void on_error(const ::braft::Error &e) override
    {
    }
    void on_shutdown() override
    {
    }
    void on_configuration_committed(const braft::Configuration &conf) override
    {
    }

    void on_snapshot_save(braft::SnapshotWriter *writer,
                          braft::Closure *done) override;

    int on_snapshot_load(braft::SnapshotReader *reader) override
    {
        // We do not need to load snapshot.
        return 0;
    }

    static braft::NodeOptions BaseNodeOptions()
    {
        braft::NodeOptions node_options;
        node_options.election_timeout_ms = 5000;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = 0;
        node_options.disable_cli = false;
        return node_options;
    }

    bool HasStandByNodes()
    {
        return has_standby_node_;
    }

    bool IsCandidate(uint32_t node_id)
    {
        for (const auto &node : ng_config_)
        {
            if (node.node_id_ == node_id)
            {
                return node.is_candidate_;
            }
        }
        LOG(WARNING) << "Node " << node_id << "not in NodeGroup " << ng_id_;
        assert(false);
        return false;
    }

    int32_t NodeIdx(uint32_t node_id)
    {
        for (size_t idx = 0; idx < ng_config_.size(); idx++)
        {
            if (ng_config_[idx].node_id_ == node_id)
            {
                return idx;
            }
        }
        LOG(WARNING) << "Node " << node_id << "not in NodeGroup " << ng_id_;
        assert(false);
        return -1;
    }

    int32_t NodeIdx(const braft::PeerId &peer_id)
    {
        std::string peer_host;
        uint16_t peer_port;
        if (peer_id.type_ == braft::PeerId::Type::HostName)
        {
            peer_host = std::string(peer_id.hostname_addr.hostname.c_str());
            peer_port = peer_id.hostname_addr.port;
        }
        else
        {
            assert(peer_id.type_ == braft::PeerId::Type::EndPoint);
            peer_host = std::string(butil::ip2str(peer_id.addr.ip).c_str());
            peer_port = peer_id.addr.port;
        }

        for (size_t idx = 0; idx < ng_config_.size(); idx++)
        {
            if (ng_config_[idx].host_name_ == peer_host &&
                GET_RAFT_NODE_PORT(ng_config_[idx].port_) == peer_port)
            {
                return idx;
            }
        }
        assert(false);
        LOG(WARNING) << "Peer " << peer_id.to_string() << "not in NodeGroup "
                     << ng_id_;
        return -1;
    }

    bthread::Mutex mux_;
    uint32_t ng_id_;
    size_t node_idx_;
    bool has_standby_node_;
    std::vector<NodeConfig> ng_config_;
    RaftHostManager &host_manager_;
    braft::Node *volatile node_;
    // background thread to request leader transfer if not node group leader of
    // preferred node group.
    std::thread request_leader_transfer_thd_;
    // background thread to transfer leader to next candidate.
    std::thread leader_transfer_thd_;
    std::atomic<int64_t> leader_term_{-1};

    std::atomic<int64_t> current_term_{0};
};

enum TxNodeStatus
{
    Initializing = 0,
    Started,
    Terminated,
    Attaching,
    Detaching,
};
class RaftHostManager
{
public:
    static RaftHostManager &Instance()
    {
        static RaftHostManager hm_;
        return hm_;
    }
    bool Start(const std::string &ip,
               uint16_t port,
               const std::string &raft_path,
               bool enable_brpc_builtin_services,
               const std::string &service_bin_path,
               const std::string &data_substrate_config_path,
               const std::string &cluster_config_path,
               const std::string &engine_specific_config_path,
               bool fork_from_txservice);
    void Shutdown();
    void Detach(::txservice::remote::DetachTxServiceResponse *response);
    void Attach(const ::txservice::remote::AttachTxServiceRequest *request,
                ::txservice::remote::AttachTxServiceResponse *response);

    void ClearResources();

    bool ConnectToTxNode(
        uint32_t node_id,
        const NodeConfig &node_conf,
        std::unordered_map<uint32_t, std::vector<NodeConfig>> &&cluster_config,
        uint64_t config_version,
        int32_t txservice_pid);
    void InitLogAgent(std::vector<std::string> &ip_list,
                      std::vector<uint16_t> &port_list,
                      uint32_t log_group_replica_num);
    void NotifyLogReplay(uint32_t ng_id, int64_t term, uint64_t start_ts);
    void NotifyNewLeaderStart(uint32_t ng_id, int64_t term);
    int GetLeader(uint32_t ng_id);
    void UpdateClusterConfig(
        const ::txservice::remote::UpdateClusterConfigRequest *request,
        ::txservice::remote::UpdateClusterConfigResponse *response);
    void ApplyClusterConfigChange(
        const ::txservice::remote::UpdateClusterConfigRequest *request,
        ::txservice::remote::UpdateClusterConfigResponse *response,
        braft::Closure *done);
    void CommitConfigChange();
    void RevertConfigChange();
    TransferError TransferLeader(uint32_t ng_id, uint32_t node_id, int64_t term)
    {
        if (tx_node_status_.load(std::memory_order_acquire) ==
            TxNodeStatus::Started)
        {
            std::unique_lock<bthread::Mutex> config_lk(mux_);
            auto node_it = nodes_.find(ng_id);
            if (node_it != nodes_.end())
            {
                RaftNode &node = node_it->second;
                config_lk.unlock();
                return node.CheckTermAndTransferLeader(node_id, term);
            }
        }

        return TransferError::Error;
    }
    bool TxProcessDead()
    {
        return tx_node_status_.load(std::memory_order_acquire) ==
               TxNodeStatus::Terminated;
    }

    TxNodeStatus Status() const
    {
        return tx_node_status_.load(std::memory_order_relaxed);
    }

    uint64_t ConfigVersion()
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        return config_version_;
    }

    void GetNodeAddress(uint32_t ng_id, std::string &ip, uint16_t &port)
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        auto node_it = cluster_config_.find(ng_id);
        if (node_it != cluster_config_.end())
        {
            ip = node_it->second.front().host_name_;
            port = node_it->second.front().port_;
        }
        else
        {
            ip.clear();
            port = 0;
        }
    }

    // CAREFUL: Remove raft node permanently removes history of this node group.
    // This should only be called when node group is deleted from cluster.
    void RemoveRaftNode(uint32_t ng_id, bool truncate_log)
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        auto node_it = nodes_.find(ng_id);
        if (node_it != nodes_.end())
        {
            RaftNode &node = node_it->second;
            lk.unlock();
            node.Remove(truncate_log);
            lk.lock();
            nodes_.erase(ng_id);
        }
    }

    void ConfigRouteTable(
        const std::unordered_map<uint32_t, std::vector<NodeConfig>> &ng_configs)
    {
        for (auto &pair : ng_configs)
        {
            uint32_t ng_id = pair.first;
            std::string group_id("ng");
            group_id.append(std::to_string(ng_id));

            std::string group_conf;
            for (uint32_t idx = 0; idx < pair.second.size(); ++idx)
            {
                if (idx > 0)
                {
                    group_conf.append(",");
                }

                group_conf.append(pair.second.at(idx).host_name_);
                group_conf.append(":");
                group_conf.append(std::to_string(
                    GET_RAFT_NODE_PORT(pair.second.at(idx).port_)));
                group_conf.append(":");
                group_conf.append(std::to_string(0));
            }
            braft::rtb::update_configuration(group_id, group_conf);
        }
    }

    void NotifyTxServiceClusterConfigChange(
        const ::txservice::remote::UpdateClusterConfigRequest &request)
    {
        txservice::remote::CcRpcService_Stub txservice_channel_stub(
            txservice_channel_.get());
        brpc::Controller cntl;
        txservice_channel_stub.UpdateClusterConfig(
            &cntl, &request, nullptr, nullptr);

        while (cntl.Failed() && !TxProcessDead())
        {
            bthread_usleep(1000);
            cntl.Reset();
            txservice_channel_stub.UpdateClusterConfig(
                &cntl, &request, nullptr, nullptr);
        }

        assert(request.new_cluster_config_size() > 0);
    }

    std::string hm_ip_;
    uint16_t hm_port_;
    std::atomic<TxNodeStatus> tx_node_status_{TxNodeStatus::Attaching};

private:
    RaftHostManager() : hm_service_(*this)
    {
    }

    int32_t ForkTxService(const std::string &engine_bin_path,
                          const std::string &data_substrate_config_path,
                          const std::string &engine_specific_config_path);

    brpc::Server hm_server_;
    txservice::remote::RaftHostManagerService hm_service_;
    bool enable_brpc_builtin_services_{true};

    // protects cluster_config_, config_version_ and nodes_ since it might be
    // updated by multiple rpc threads
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;
    std::unordered_map<uint32_t, std::vector<NodeConfig>> cluster_config_;
    uint64_t config_version_{0};
    // Dirty version is only set during cluster config change. A cluster config
    // update request that sees dirty_version_ == new version should wait for
    // the current config update to finish.
    uint64_t dirty_version_{0};
    NodeConfig node_config_;
    brpc::Server raft_server_;
    std::unordered_map<uint32_t, RaftNode> nodes_;
    std::unique_ptr<brpc::Channel> txservice_channel_;
    std::string raft_path_{""};
    std::string cluster_config_file_path_{""};
    std::unique_ptr<::txlog::LogAgent> log_agent_{nullptr};
    // Interrupt log agent workload to avoid it blocking shutdown.
    std::atomic_bool log_agent_interrupt_{false};
    int32_t txservice_pid_{-1};
    std::string eloq_data_path_{""};
    std::string local_data_path_for_rocksdb_cloud_fs_{""};
    std::string local_data_path_for_txlog_rocksdb_cloud_fs_{""};
    std::vector<std::string> eloq_store_local_data_path_list_;
    friend class RaftNode;
};

class UpdateClusterConfigFileClosure
{
public:
    explicit UpdateClusterConfigFileClosure(
        braft::Closure *rpc_closure,
        RaftHostManager *host_manager,
        const ::txservice::remote::UpdateClusterConfigRequest *request =
            nullptr)
        : rpc_closure_(rpc_closure), host_manager_(host_manager)
    {
        if (request)
        {
            request_ = *request;
        }
    }

    void SetAllChangePeersStarted()
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        all_change_peers_started_ = true;
        if (unfinished_change_peers_ == 0)
        {
            // Update cluster config file and commit config change after all
            // change peers are applied.
            std::unique_ptr<UpdateClusterConfigFileClosure> self_guard(this);
            if (error_code_ == 0)
            {
                for (auto ng_id : deleted_node_groups_)
                {
                    host_manager_->RemoveRaftNode(ng_id.first, ng_id.second);
                }
                host_manager_->CommitConfigChange();
            }
            else
            {
                host_manager_->RevertConfigChange();
            }
            if (rpc_closure_)
            {
                // Return rpc request.
                rpc_closure_->Run();
            }
            else if (error_code_ == 0)
            {
                // This update is replicated to current node by raft. We
                // need to notify tx service to update cluster config.
                host_manager_->NotifyTxServiceClusterConfigChange(request_);
            }
        }
    }

    void IncrUnfinishedChangePeers()
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        unfinished_change_peers_++;
    }

    void DecrUnfinishedChangePeers(int error_code)
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        unfinished_change_peers_--;
        if (error_code != 0)
        {
            error_code_ = error_code;
            if (rpc_closure_)
            {
                static_cast<UpdateClusterConfigClosure *>(rpc_closure_)
                    ->SetError(error_code);
            }
        }

        if (all_change_peers_started_ && unfinished_change_peers_ == 0)
        {
            std::unique_ptr<UpdateClusterConfigFileClosure> self_guard(this);
            // Update cluster config file and commit config change after all
            // change peers are applied.
            if (error_code_ == 0)
            {
                for (auto ng_id : deleted_node_groups_)
                {
                    host_manager_->RemoveRaftNode(ng_id.first, ng_id.second);
                }
                host_manager_->CommitConfigChange();
            }
            else
            {
                host_manager_->RevertConfigChange();
            }
            if (rpc_closure_)
            {
                // Return rpc request.
                rpc_closure_->Run();
            }
            else if (error_code_ == 0)
            {
                // This update is replicated to current node by raft. We
                // need to notify tx service to update cluster config.
                host_manager_->NotifyTxServiceClusterConfigChange(request_);
            }
        }
    }

    void AddDeletedNodeGroup(uint32_t ng_id, bool truncate_log)
    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        deleted_node_groups_.push_back(std::make_pair(ng_id, truncate_log));
    }

private:
    braft::Closure *rpc_closure_;
    RaftHostManager *host_manager_;
    // This is only initialized on non leader node and is used to notify tx
    // service to update cluster config.
    ::txservice::remote::UpdateClusterConfigRequest request_;
    bthread::Mutex mux_;
    bool all_change_peers_started_{false};
    uint32_t unfinished_change_peers_{0};
    // Node groups that we are no longer a member of.
    std::vector<std::pair<uint32_t, bool>> deleted_node_groups_;
    int error_code_{0};
};

class ChangePeerClosure : public braft::Closure
{
public:
    explicit ChangePeerClosure(UpdateClusterConfigFileClosure *done,
                               braft::Configuration &config,
                               braft::Node *node)
        : done_(done), new_config_(config), node_(node)
    {
    }
    ~ChangePeerClosure()
    {
    }

    void Run() override
    {
        std::unique_ptr<ChangePeerClosure> self_guard(this);
        if (!status().ok())
        {
            if (node_->is_leader() &&
                !RaftHostManager::Instance().TxProcessDead())
            {
                self_guard.release();
                // Retry
                LOG(ERROR)
                    << "Failed to update braft config during cluster config "
                       "update. Error message: "
                    << status().error_str() << ". Retrying...";
                status().reset();
                node_->change_peers(new_config_, this);
            }
            else
            {
                done_->DecrUnfinishedChangePeers(1);
            }
            return;
        }

        // Free closure on exit if change_peers succeed.
        done_->DecrUnfinishedChangePeers(0);
    }

private:
    UpdateClusterConfigFileClosure *done_;
    braft::Configuration new_config_;
    braft::Node *node_;
};
}  // namespace host_manager
