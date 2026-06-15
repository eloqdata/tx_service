#include "raft_host_manager.h"

#include <arpa/inet.h>
#include <braft/configuration.h>
#include <braft/raft.h>
#include <braft/route_table.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/errno.h>
#include <bthread/mutex.h>
#include <butil/status.h>
#include <linux/limits.h>
#include <spawn.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>
#include <yaml-cpp/yaml.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "INIReader.h"
#include "cc_request.pb.h"
#include "log_agent.h"
#include "raft_host_manager_service.h"
// gflags 2.1.1 missing GFLAGS_NAMESPACE. This is a workaround to handle gflags
// ABI issue.
#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#endif

void hm_exit_handler(int sig_num);
namespace host_manager
{
using namespace txservice::remote;

// eloq_store_data_path_list={your_eloqstore_data_path1,
// your_eloqstore_data_path2,...}
static void ParseEloqStorePathList(
    const std::string_view storage_path_list,
    std::vector<std::string> &storage_path_vector)
{
    storage_path_vector.clear();
    const char path_delimiter = ',';
    std::string token;
    std::istringstream tokenStream(storage_path_list.data());
    while (std::getline(tokenStream, token, path_delimiter))
    {
        storage_path_vector.emplace_back(token);
    }
}

RaftNode::RaftNode(uint32_t ng_id,
                   const std::vector<NodeConfig> &ng_config,
                   RaftHostManager &hm)
    : ng_id_(ng_id), ng_config_(ng_config), host_manager_(hm), node_(nullptr)
{
    size_t nid = 0;
    for (; nid < ng_config_.size(); ++nid)
    {
        if (ng_config_.at(nid).node_id_ == host_manager_.node_config_.node_id_)
        {
            node_idx_ = nid;
            break;
        }
    }

    assert((int32_t) node_idx_ == NodeIdx(host_manager_.node_config_.node_id_));
    int32_t candidate_cnt = 0;
    for (const NodeConfig &node : ng_config_)
    {
        if (node.is_candidate_)
        {
            candidate_cnt++;
        }
    }
    has_standby_node_ = candidate_cnt > 1;

    assert(nid < ng_config_.size());
}

int RaftNode::Start()
{
    braft::NodeOptions node_options = BaseNodeOptions();
    braft::PeerId local_node;

    std::string raft_conf;
    for (size_t idx = 0; idx < ng_config_.size(); ++idx)
    {
        if (idx > 0)
        {
            raft_conf.append(",");
        }

        raft_conf.append(ng_config_.at(idx).host_name_);
        raft_conf.append(":");
        raft_conf.append(
            std::to_string(GET_RAFT_NODE_PORT(ng_config_.at(idx).port_)));
        raft_conf.append(":");
        raft_conf.append(std::to_string(0));
    }
    if (node_options.initial_conf.parse_from(raft_conf) != 0)
    {
        LOG(ERROR) << "Failed to parse configuration " << raft_conf;
        return -1;
    }
    node_options.fsm = this;
    node_options.snapshot_interval_s = 600;
    node_options.log_uri =
        host_manager_.raft_path_ + "/ng" + std::to_string(ng_id_) + "/log";
    node_options.raft_meta_uri =
        host_manager_.raft_path_ + "/ng" + std::to_string(ng_id_) + "/meta";
    node_options.snapshot_uri =
        host_manager_.raft_path_ + "/ng" + std::to_string(ng_id_) + "/snapshot";

    // preferred leader has lower election timeout.
    if (ng_config_.at(node_idx_).is_candidate_)
    {
        node_options.election_timeout_ms = 2000;
    }
    else
    {
        node_options.election_timeout_ms = 5000;
    }

    std::string group_id("ng");
    group_id.append(std::to_string(ng_id_));

    if (0 == butil::str2ip(host_manager_.node_config_.host_name_.c_str(),
                           &local_node.addr.ip))
    {
        butil::str2endpoint(
            host_manager_.node_config_.host_name_.c_str(),
            GET_RAFT_NODE_PORT(host_manager_.node_config_.port_),
            &local_node.addr);
    }
    else
    {
        // for case `ip_` is hostname format
        local_node.type_ = braft::PeerId::Type::HostName;
        local_node.hostname_addr.hostname =
            host_manager_.node_config_.host_name_;
        local_node.hostname_addr.port =
            GET_RAFT_NODE_PORT(host_manager_.node_config_.port_);
        // node init process would check ip addr is not butil::IP_ANY
        local_node.addr.ip = butil::my_ip();
        local_node.addr.port =
            GET_RAFT_NODE_PORT(host_manager_.node_config_.port_);
    }

    local_node.idx = 0;
    braft::Node *node = new braft::Node(group_id, local_node);

    if (node->init(node_options) != 0)
    {
        LOG(ERROR) << "Failed to init raft node";
        delete node;
        return -1;
    }
    node_ = node;

    return 0;
}

bool RaftHostManager::Start(const std::string &ip,
                            uint16_t port,
                            const std::string &raft_path,
                            bool enable_brpc_builtin_services,
                            const std::string &service_bin_path,
                            const std::string &data_substrate_config_path,
                            const std::string &cluster_config_path,
                            const std::string &engine_specific_config_path,
                            bool fork_from_txservice)
{
    hm_ip_ = ip;
    hm_port_ = port;
    raft_path_ = raft_path;
    cluster_config_file_path_ = cluster_config_path;
    // Add host manager service, enable health check
    if (hm_server_.AddService(&hm_service_,
                              brpc::SERVER_DOESNT_OWN_SERVICE,
                              "/healthz => CheckHealth") != 0)
    {
        LOG(FATAL)
            << "Failed to add host manager service to host manager server";
        return false;
    }

    enable_brpc_builtin_services_ = enable_brpc_builtin_services;
    brpc::ServerOptions server_options;
    server_options.has_builtin_services = enable_brpc_builtin_services;
    int res = hm_server_.Start(hm_port_, &server_options);
    for (int retry = 5; res != 0 && retry > 0; retry--)
    {
        bthread_usleep(500000);
        res = hm_server_.Start(hm_port_, &server_options);
    }
    if (res != 0)
    {
        LOG(FATAL) << "Failed to start host manager service";
        return false;
    }

    if (!fork_from_txservice)
    {
        // we need to fork txservice.
        if (!service_bin_path.empty() && !data_substrate_config_path.empty())
        {
            assert(!fork_from_txservice);
            int32_t txservice_pid = ForkTxService(service_bin_path,
                                                  data_substrate_config_path,
                                                  engine_specific_config_path);
            if (txservice_pid == -1)
            {
                LOG(FATAL) << "Failed to fork tx service";
                return false;
            }
        }
        else
        {
            LOG(INFO)
                << "Start host manager alone without fork txservice, empty bin "
                   "path and config path";
            tx_node_status_.store(TxNodeStatus::Terminated,
                                  std::memory_order_release);
        }
    }
    // else:  Do nothing, unsafe to set tx node status

    LOG(INFO) << "Raft Host Manager has been started on " << ip << ":" << port;
    return true;
}

void RaftHostManager::Shutdown()
{
    log_agent_interrupt_.store(true, std::memory_order_relaxed);
    // Shutdown host manager rpc server to stop receiving new
    // requests
    hm_server_.Stop(0);
    hm_server_.Join();

    // Shutdown all raft nodes in this tx node.
    // std::unique_lock<bthread::Mutex> lk(mux_);
    for (auto &node : nodes_)
    {
        node.second.Shutdown();
    }

    raft_server_.Stop(0);
    for (auto &node : nodes_)
    {
        node.second.Join();
    }
    raft_server_.Join();
    LOG(INFO) << "Stopped raft service for node " << node_config_.node_id_;
}

int RaftHostManager::GetLeader(uint32_t ng_id)
{
    if (tx_node_status_.load(std::memory_order_acquire) !=
        TxNodeStatus::Started)
    {
        return -1;
    }
    std::string node_group_id("ng");
    node_group_id.append(std::to_string(ng_id));

    // Blocking the thread until query_leader finishes
    butil::Status st = braft::rtb::refresh_leader(node_group_id, 1000);
    if (!st.ok())
    {
        LOG(WARNING) << "Failed to refresh leader of group " << node_group_id
                     << ", error: " << st.error_str();
        return -1;
    }

    braft::PeerId leader;
    // Get the cached leader of the target group from RouteTable
    if (braft::rtb::select_leader(node_group_id, &leader) != 0)
    {
        LOG(WARNING) << "Failed to select the leader.";
        return -1;
    }

    std::string leader_ip_port(butil::endpoint2str(leader.addr).c_str());
    size_t comma_pos = leader_ip_port.find(':');
    assert(comma_pos != std::string::npos);
    std::string leader_ip_str = leader_ip_port.substr(0, comma_pos);
    uint16_t leader_port = leader.addr.port;

    std::unique_lock<bthread::Mutex> lk(mux_);
    for (auto &node : cluster_config_.at(ng_id))
    {
        if (node.host_name_ == leader_ip_str &&
            GET_RAFT_NODE_PORT(node.port_) == leader_port)
        {
            return node.node_id_;
        }
    }

    return -1;
}

static std::string TxNodeStatusToString(TxNodeStatus status)
{
    switch (status)
    {
    case TxNodeStatus::Started:
        return "started";
    case TxNodeStatus::Initializing:
        return "initializing";
    case TxNodeStatus::Terminated:
        return "terminated";
    case TxNodeStatus::Attaching:
        return "Attaching";
    case TxNodeStatus::Detaching:
        return "Detaching";
    default:
        assert(false);
        return "";
    }
}

void RaftHostManager::ClearResources()
{
    // notify
    log_agent_interrupt_.store(true, std::memory_order_relaxed);

    // Shutdown all raft nodes in this tx node.
    for (auto &node : nodes_)
    {
        node.second.Shutdown();
    }
    raft_server_.Stop(0);
    for (auto &node : nodes_)
    {
        node.second.Join();
    }
    raft_server_.Join();
    // Clear all services
    raft_server_.ClearServices();

    LOG(INFO) << "Stopped raft service for node " << node_config_.node_id_;

    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        // reset to default value
        nodes_.clear();
        cluster_config_.clear();
        log_agent_ = nullptr;
        // Release all underling cached channels in rtb to prevent RPC fail when
        // AttachTxService
        braft::rtb::clear_internal_channels();
        txservice_channel_ = nullptr;
        node_config_ = NodeConfig();
        config_version_ = 0;
        dirty_version_ = 0;
        cluster_config_file_path_ = "";
        raft_path_ = "";
        log_agent_interrupt_.store(false, std::memory_order_relaxed);
    }
}

void RaftHostManager::Detach(
    ::txservice::remote::DetachTxServiceResponse *response)
{
    TxNodeStatus status = TxNodeStatus::Started;
    if (!tx_node_status_.compare_exchange_strong(
            status, TxNodeStatus::Detaching, std::memory_order_acq_rel))
    {
        if (status == TxNodeStatus::Terminated)
        {
            response->set_err_code(::txservice::remote::ErrorCode::Success);
            // host manager is terminated
        }
        else if (status == TxNodeStatus::Detaching)
        {
            response->set_err_code(::txservice::remote::ErrorCode::InProgress);
        }
        else
        {
            LOG(ERROR) << "DetachTxService: hm status is "
                       << TxNodeStatusToString(status);
            response->set_err_code(
                ::txservice::remote::ErrorCode::StatusUpdateError);
        }

        return;
    }

    ClearResources();

    // unregister signal handler
    struct sigaction sa;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sa.sa_handler = SIG_IGN;
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGHUP, &sa, NULL);

    if (txservice_pid_ != -1)
    {
        LOG(INFO) << "DetachTxService: graceful shutdown txservice";
        // kill -15 pid
        if (kill(txservice_pid_, SIGTERM) == -1)
        {
            // EPERM  The calling process does not have permission to
            // send the signal to any of the target processes.
            if (errno == EPERM)
            {
                LOG(ERROR) << "Fail to shutdown txservice, txservice pid: "
                           << txservice_pid_ << ", errno: " << errno
                           << ", err msg: " << strerror(errno);
                assert(false);
            }
        }

        while (true)
        {
            // If sig is 0, then no signal is sent, but error checking is still
            // performed; this can be used to check for the existence of a
            // process ID or process group ID.
            if (kill(txservice_pid_, 0) == -1)
            {
                if (errno == ESRCH)
                {
                    LOG(INFO) << "DetachTxService: txservice already exit";
                    txservice_pid_ = -1;
                    // No such process
                    break;
                }
                else
                {
                    // EINVAL An invalid signal was specified.
                    // EPERM  The calling process does not have permission to
                    // send the signal to any of the target processes.

                    LOG(ERROR)
                        << "Fail to check process, pid: " << txservice_pid_
                        << ", errno: " << errno
                        << ", err msg: " << strerror(errno);
                    assert(false);
                }
            }

            // sleep 1s
            bthread_usleep(1000000);
        }
    }

    if (!eloq_data_path_.empty())
    {
        // remove eloq data path
        std::string cmd = "rm -rf " + eloq_data_path_ + "/*";
        [[maybe_unused]] int ignore = system(cmd.c_str());
        eloq_data_path_ = "";
    }

    if (!local_data_path_for_rocksdb_cloud_fs_.empty())
    {
        // remove local data path for cloud fs
        std::string cmd =
            "rm -rf " + local_data_path_for_rocksdb_cloud_fs_ + "/*";
        [[maybe_unused]] int ignore = system(cmd.c_str());
        local_data_path_for_rocksdb_cloud_fs_ = "";
    }

    if (!eloq_store_local_data_path_list_.empty())
    {
        for (const auto &eloq_store_path : eloq_store_local_data_path_list_)
        {
            // remove local data path for cloud fs
            std::string cmd = "rm -rf " + eloq_store_path + "/*";
            [[maybe_unused]] int ignore = system(cmd.c_str());
        }
        eloq_store_local_data_path_list_.clear();
    }

    if (!local_data_path_for_txlog_rocksdb_cloud_fs_.empty())
    {
        // remove local data path for txlog cloud fs
        std::string cmd =
            "rm -rf " + local_data_path_for_txlog_rocksdb_cloud_fs_ + "/*";
        [[maybe_unused]] int ignore = system(cmd.c_str());
        local_data_path_for_txlog_rocksdb_cloud_fs_ = "";
    }

    // recover signal
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sa.sa_handler = hm_exit_handler;
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGHUP, &sa, NULL);

    // update status to `Terminated`
    tx_node_status_.store(TxNodeStatus::Terminated, std::memory_order_release);
    response->set_err_code(::txservice::remote::ErrorCode::Success);
    LOG(INFO) << "DetachTxService: success";
    return;
}

int32_t RaftHostManager::ForkTxService(
    const std::string &service_bin_path,
    const std::string &data_substrate_config_path,
    const std::string &engine_specific_config_path)
{
    assert(!service_bin_path.empty());
    assert(!data_substrate_config_path.empty());
    assert(!hm_ip_.empty());

    LOG(INFO) << "ForkTxService: " << service_bin_path << " "
              << data_substrate_config_path << " "
              << engine_specific_config_path << " " << hm_ip_ << " "
              << hm_port_;

    {
        std::ifstream config_file(data_substrate_config_path);
        if (!config_file)
        {
            LOG(ERROR) << "Failed to open config file: "
                       << data_substrate_config_path;
            return -1;
        }

        LOG(INFO) << "The config file content is:";
        std::string line;
        while (std::getline(config_file, line))
        {
            LOG(INFO) << line;
        }
        config_file.close();
    }

    {
        std::filesystem::path bin_path(service_bin_path);
        const std::string binary_name = bin_path.filename().string();
        if (binary_name.find("eloqdoc") != std::string::npos)
        {
            if (engine_specific_config_path.empty())
            {
                LOG(ERROR) << "Eloqdoc yaml config path is empty";
                return -1;
            }

            std::ifstream config_file(engine_specific_config_path);
            if (!config_file)
            {
                LOG(ERROR) << "Failed to open yaml config file: "
                           << engine_specific_config_path;
                return -1;
            }

            LOG(INFO) << "The yaml config file content is:";
            std::string line;
            while (std::getline(config_file, line))
            {
                LOG(INFO) << line;
            }
            config_file.close();
        }
    }

    std::vector<std::string> flag_strings;
    // Build command line flags
    flag_strings.emplace_back(service_bin_path);

    // Check if the service is EloqDoc or EloqKV/EloqSQL.
    // We can determine this by checking the service binary name.
    if (service_bin_path.find("eloqdoc") != std::string::npos)
    {
        // EloqDoc config file is in YAML format
        LOG(INFO) << "Try to launch EloqDoc with YAML main config and INI data "
                     "substrate config";

        flag_strings.emplace_back("--config=" + engine_specific_config_path);
        flag_strings.emplace_back("--data_substrate_config=" +
                                  data_substrate_config_path);
    }
    else
    {
        LOG(INFO) << "Try to Launch EloqKV/EloqSQL. The config file is in "
                     "INI format";
        flag_strings.emplace_back("--config=" + data_substrate_config_path);
    }

    INIReader config_reader(data_substrate_config_path);

    eloq_data_path_ = config_reader.Get("local", "eloq_data_path", "");
    if (eloq_data_path_.empty())
    {
        LOG(ERROR)
            << "Failed to parse eloq_data_path from service config file: "
            << data_substrate_config_path;
        return -1;
    }

    local_data_path_for_rocksdb_cloud_fs_ =
        config_reader.Get("store", "rocksdb_storage_path", "");

    std::string eloq_store_data_path_list =
        config_reader.Get("store", "eloq_store_data_path_list", "");

    if (!eloq_store_data_path_list.empty())
    {
        // parse eloq stora data path
        ParseEloqStorePathList(eloq_store_data_path_list,
                               eloq_store_local_data_path_list_);
        if (eloq_store_local_data_path_list_.empty())
        {
            LOG(ERROR) << "Failed to parse eloq store data path list";
            return -1;
        }

        assert(!eloq_store_local_data_path_list_.empty());
    }

    if (local_data_path_for_rocksdb_cloud_fs_.empty() &&
        eloq_store_data_path_list.empty())
    {
        LOG(ERROR) << "Failed to parse local storage path from "
                      "service config file: "
                   << data_substrate_config_path;
        return -1;
    }
    else if (!local_data_path_for_rocksdb_cloud_fs_.empty() &&
             !eloq_store_data_path_list.empty())
    {
        LOG(ERROR) << "Ambiguous storage configuration in "
                   << data_substrate_config_path
                   << ": both 'rocksdb_storage_path' ("
                   << local_data_path_for_rocksdb_cloud_fs_
                   << ") and 'eloq_store_data_path_list' ("
                   << eloq_store_data_path_list
                   << ") are defined. Please provide only one.";
        return -1;
    }

    local_data_path_for_txlog_rocksdb_cloud_fs_ =
        config_reader.Get("local", "txlog_rocksdb_storage_path", "");
    if (local_data_path_for_txlog_rocksdb_cloud_fs_.empty())
    {
        LOG(ERROR) << "Failed to parse local_data_path_for_txlog_cloud_fs "
                      "from service config file: "
                   << data_substrate_config_path;
        return -1;
    }

    flag_strings.emplace_back("--fork_host_manager=false");
    flag_strings.emplace_back("--hm_ip=" + hm_ip_);
    flag_strings.emplace_back("--hm_port=" + std::to_string(hm_port_));

    LOG(INFO) << "Service config file parsed successfully. "
              << "Eloq data path: " << eloq_data_path_
              << ", Local data path for cloud fs: "
              << (!local_data_path_for_rocksdb_cloud_fs_.empty()
                      ? local_data_path_for_rocksdb_cloud_fs_
                      : eloq_store_data_path_list)
              << ", Local data path for txlog cloud fs: "
              << local_data_path_for_txlog_rocksdb_cloud_fs_;

    std::vector<char *> command_flags;
    // Convert string flags to char* array for execv
    command_flags.reserve(flag_strings.size() + 1);
    for (const auto &flag : flag_strings)
    {
        command_flags.push_back(const_cast<char *>(flag.c_str()));
    }
    command_flags.push_back(nullptr);

    pid_t pid = 0;
    int ret = posix_spawn(&pid,
                          service_bin_path.c_str(),
                          nullptr,
                          nullptr,
                          command_flags.data(),
                          environ);

    if (ret != 0)
    {
        LOG(ERROR) << "Failed to posix_spawn txservice, errno: " << ret
                   << ", err msg: " << strerror(ret);
        return -1;
    }

    return static_cast<int32_t>(pid);
}

void RaftHostManager::Attach(
    const ::txservice::remote::AttachTxServiceRequest *request,
    ::txservice::remote::AttachTxServiceResponse *response)
{
    TxNodeStatus status = TxNodeStatus::Terminated;
    if (!tx_node_status_.compare_exchange_strong(
            status, TxNodeStatus::Attaching, std::memory_order_acq_rel))
    {
        if (status == TxNodeStatus::Started)
        {
            response->set_err_code(::txservice::remote::ErrorCode::Success);
            response->set_err_msg("");
        }
        else if (status == TxNodeStatus::Attaching ||
                 status == TxNodeStatus::Initializing)
        {
            response->set_err_code(::txservice::remote::ErrorCode::InProgress);
            response->set_err_msg("Attch in progress");
        }
        else
        {
            response->set_err_code(
                ::txservice::remote::ErrorCode::StatusUpdateError);
            std::string err_msg =
                "Fail to update hm status to `Attaching`, current hm status: " +
                TxNodeStatusToString(status);
            response->set_err_msg(err_msg);
        }

        return;
    }

    if (request->service_bin_path().empty())
    {
        response->set_err_code(
            ::txservice::remote::ErrorCode::InvalidRequestParamsError);
        response->set_err_msg("service bin path is empty");
        tx_node_status_.store(TxNodeStatus::Terminated,
                              std::memory_order_release);
        return;
    }

    if (request->data_substrate_config_path().empty())
    {
        response->set_err_code(
            ::txservice::remote::ErrorCode::InvalidRequestParamsError);
        response->set_err_msg("data substrate config path is empty");
        tx_node_status_.store(TxNodeStatus::Terminated,
                              std::memory_order_release);
        return;
    }

    std::filesystem::path bin_path(request->service_bin_path());
    const std::string binary_name = bin_path.filename().string();
    if (binary_name.find("eloqdoc") != std::string::npos)
    {
        if (request->engine_specific_config_path().empty())
        {
            response->set_err_code(
                ::txservice::remote::ErrorCode::InvalidRequestParamsError);
            response->set_err_msg("engine specific config path is empty");
            tx_node_status_.store(TxNodeStatus::Terminated,
                                  std::memory_order_release);
            return;
        }
    }

    // Check if binary exists
    struct stat buffer;
    if (stat(request->service_bin_path().c_str(), &buffer) != 0)
    {
        std::string err_msg =
            "Executable file not found at: " + request->service_bin_path();
        LOG(ERROR) << err_msg;
        response->set_err_code(
            ::txservice::remote::ErrorCode::InvalidRequestParamsError);
        response->set_err_msg(err_msg);
        tx_node_status_.store(TxNodeStatus::Terminated,
                              std::memory_order_release);
        return;
    }

    assert(!request->hm_raft_log_path().empty());
    // update raft path
    raft_path_ = request->hm_raft_log_path();
    // Fork txservice
    int32_t pid = ForkTxService(request->service_bin_path(),
                                request->data_substrate_config_path(),
                                request->engine_specific_config_path());

    if (pid == -1)
    {
        std::string err_msg =
            "Fail to fork process, errno: " + std::to_string(errno) +
            ", err msg: " + strerror(errno);
        response->set_err_code(::txservice::remote::ErrorCode::SystemError);
        response->set_err_msg(err_msg);

        tx_node_status_.store(TxNodeStatus::Terminated,
                              std::memory_order_release);
        return;
    }

    status = tx_node_status_.load(std::memory_order_acquire);
    while (status != TxNodeStatus::Started)
    {
        if (status == TxNodeStatus::Terminated)
        {
            response->set_err_code(::txservice::remote::ErrorCode::SystemError);
            response->set_err_msg("Fail to init txservice");
            LOG(ERROR) << "Failed to init txservice";
            // host manager will be killed
            return;
        }
        // 200ms
        bthread_usleep(200000);
        status = tx_node_status_.load(std::memory_order_acquire);
    }

    response->set_err_code(::txservice::remote::ErrorCode::Success);
    response->set_err_msg("");
    LOG(INFO) << "AttachTxService: success";
    return;
}

bool RaftHostManager::ConnectToTxNode(
    uint32_t node_id,
    const NodeConfig &node_conf,
    std::unordered_map<uint32_t, std::vector<NodeConfig>> &&cluster_config,
    uint64_t version,
    int32_t txservice_pid)
{
    TxNodeStatus status = TxNodeStatus::Attaching;
    if (!tx_node_status_.compare_exchange_strong(
            status, TxNodeStatus::Initializing, std::memory_order_acquire))
    {
        // Reject connect req if tx node is already initialized.
        LOG(WARNING) << "Tx node is already connected, current status: "
                     << static_cast<int>(status);
        if (config_version_ != version)
        {
            LOG(ERROR) << "Current config_version_: " << config_version_
                       << " != " << " desired version: " << version;
            return false;
        }
        else
        {
            return true;
        }
    }

    {
        std::unique_lock<bthread::Mutex> lk(mux_);
        txservice_pid_ = txservice_pid;
        raft_path_.append("/");
        raft_path_.append(std::to_string(node_id));
        cluster_config_ = std::move(cluster_config);
        config_version_ = version;
        // node_config_ = cluster_config_.at(node_id).front();
        node_config_ = node_conf;
        // Create raft node for each node group that this tx node belongs to.
        for (auto &ng : cluster_config_)
        {
            for (auto &node : ng.second)
            {
                if (node.node_id_ == node_id)
                {
                    nodes_.try_emplace(ng.first,
                                       ng.first,
                                       cluster_config_.at(ng.first),
                                       *this);
                    break;
                }
            }
        }

        if (txservice_channel_.get() == nullptr)
        {
            txservice_channel_ = std::make_unique<brpc::Channel>();
        }
    }

    int max_retries = 300;
    int retries = 0;
    int delay_ms = 200;
    bool connected = false;
    while (retries < max_retries && !connected)
    {
        if (0 ==
            txservice_channel_->Init("0.0.0.0", node_config_.port_, nullptr))
        {
            connected = true;
        }
        else
        {
            LOG(WARNING) << "Failed to connect tx service. Retrying in "
                         << delay_ms << " ms... (Attempt " << (retries + 1)
                         << " of " << max_retries << ")";
            bthread_usleep(delay_ms * 1000);
            ++retries;
        }
    }
    if (!connected)
    {
        LOG(ERROR) << "Failed to init tx service channel for node #"
                   << node_config_.node_id_;
        return false;
    }

    LOG(INFO) << "braft::add_service, port = "
              << GET_RAFT_NODE_PORT(node_config_.port_);
    // Start raft server
    if (braft::add_service(&raft_server_,
                           GET_RAFT_NODE_PORT(node_config_.port_)) != 0)
    {
        LOG(ERROR) << "Failed to init braft service for node #"
                   << node_config_.node_id_;
        return false;
    }

    // set brpc circuit_breaker max isolation duration smaller than election
    // timeout so that restarted node will join raft group before trying to
    // start a new vote
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "circuit_breaker_max_isolation_duration_ms", "4500");
    brpc::ServerOptions server_options;
    server_options.has_builtin_services = enable_brpc_builtin_services_;
    if (raft_server_.Start(GET_RAFT_NODE_PORT(node_config_.port_),
                           &server_options) != 0)
    {
        LOG(ERROR) << "Failed to start braft server for node #"
                   << node_config_.node_id_;
        return false;
    }

    std::unique_lock<bthread::Mutex> lk(mux_);
    // Start raft nodes in each node group
    for (auto &node : nodes_)
    {
        if (node.second.Start() != 0)
        {
            return false;
        }
    }

    ConfigRouteTable(cluster_config_);

    status = TxNodeStatus::Initializing;
    [[maybe_unused]] bool res = tx_node_status_.compare_exchange_strong(
        status, TxNodeStatus::Started, std::memory_order_release);
    assert(res);
    return true;
}

void RaftHostManager::InitLogAgent(std::vector<std::string> &ip_list,
                                   std::vector<uint16_t> &port_list,
                                   uint32_t log_group_replica_num)
{
    if (!log_agent_)
    {
        log_agent_ = std::make_unique<::txlog::LogAgent>();
        log_agent_->Init(ip_list, port_list, 0, log_group_replica_num);
        LOG(INFO) << "Host manager init log agent success";
    }
}

void RaftHostManager::NotifyLogReplay(uint32_t ng_id,
                                      int64_t term,
                                      uint64_t start_ts)
{
    if (log_agent_)
    {
        LOG(INFO) << "Notified log groups to start log replay for ng#" << ng_id
                  << "with term " << term << ", start ts " << start_ts << "...";
        log_agent_->ReplayLog(ng_id,
                              term,
                              node_config_.host_name_,
                              node_config_.port_ + 2,
                              -1,
                              start_ts,
                              log_agent_interrupt_);
        LOG(INFO) << "Notified log groups to start log replay for ng#" << ng_id
                  << " with term " << term;
    }
}

void RaftHostManager::NotifyNewLeaderStart(uint32_t leader_ng_id, int64_t term)
{
    uint32_t node_id;
    std::string node_ip;
    uint16_t node_port;

    std::unique_lock<bthread::Mutex> lk(mux_);
    LOG(INFO) << "Notify new leader start of ng#" << leader_ng_id << "...";
    std::unordered_set<uint32_t> notified_node_ids;
    for (auto ng_it = cluster_config_.begin(); ng_it != cluster_config_.end();
         ng_it++)
    {
        for (const NodeConfig &node : ng_it->second)
        {
            node_id = node.node_id_;
            node_ip = node.host_name_;
            node_port = node.port_;
            if (notified_node_ids.find(node_id) != notified_node_ids.end())
            {
                continue;
            }
            notified_node_ids.insert(node_id);

            brpc::Channel channel;

            int max_retries = 50;
            int retries = 0;
            int delay_ms = 200;
            bool connected = false;
            while (retries < max_retries && !connected)
            {
                if (0 == channel.Init(node_ip.c_str(), node_port, nullptr))
                {
                    connected = true;
                }
                else
                {
                    LOG(WARNING)
                        << "Failed to init the channel to the leader of ng#"
                        << leader_ng_id
                        << " to notify leader start. Retrying in " << delay_ms
                        << " ms... (Attempt " << (retries + 1) << " of "
                        << max_retries << ")";
                    ++retries;
                }
            }

            if (!connected)
            {
                LOG(ERROR) << "Failed to init the channel to the leader of ng#"
                           << leader_ng_id << " to notify leader start.";
                continue;
            }

            txservice::remote::CcRpcService_Stub stub(&channel);
            txservice::remote::NotifyNewLeaderStartRequest req;
            req.set_ng_id(leader_ng_id);
            req.set_node_id(node_config_.node_id_);
            req.set_term(term);
            txservice::remote::NotifyNewLeaderStartResponse res;
            res.set_error(false);

            brpc::Controller cntl;
            cntl.set_timeout_ms(100);
            stub.NotifyNewLeaderStart(&cntl, &req, &res, nullptr);

            // Retry is not needed at here, the remote nodes will also
            // refresh their leader caches passively.
            if (cntl.Failed())
            {
                LOG(ERROR) << "Failed to send NotifyNewLeaderStart RPC of ng#"
                           << leader_ng_id
                           << ". Error code: " << cntl.ErrorCode()
                           << ". Msg: " << cntl.ErrorText();
            }
            else if (res.error())
            {
                LOG(ERROR) << "Failed to notify the new leader of ng#"
                           << leader_ng_id << " to remote node id:" << node_id;
            }
        }
    }
}

void RaftHostManager::UpdateClusterConfig(
    const ::txservice::remote::UpdateClusterConfigRequest *request,
    ::txservice::remote::UpdateClusterConfigResponse *response)
{
    if (tx_node_status_.load(std::memory_order_acquire) !=
        TxNodeStatus::Started)
    {
        response->set_error(1);
        LOG(INFO)
            << "Tx node is not started yet, ignore update cluster config.";
    }

    std::unique_lock<bthread::Mutex> cnf_lk(mux_);
    auto node_it = nodes_.find(request->ng_id());
    if (node_it == nodes_.end())
    {
        LOG(ERROR) << "Current node is not a member of node group #"
                   << request->ng_id();
        response->set_error(1);
    }
    cnf_lk.unlock();

    // Apply the update cluster config request to the raft node.
    node_it->second.UpdateClusterConfig(request, response);
}

bool RaftNode::UpdateClusterConfig(
    const ::txservice::remote::UpdateClusterConfigRequest *request,
    ::txservice::remote::UpdateClusterConfigResponse *response)
{
    int64_t term = leader_term_.load(std::memory_order_acquire);
    if (term < 0)
    {
        // Only leader can update cluster config.
        response->set_error(1);
        return false;
    }

    braft::Task task;
    task.expected_term = term;
    // serialize cluster config request into raft entry.
    butil::IOBuf buf;
    butil::IOBufAsZeroCopyOutputStream buf_wrapper(&buf);
    if (!request->SerializeToZeroCopyStream(&buf_wrapper))
    {
        LOG(ERROR) << "Failed to serialize the update cluster config request";
        response->set_error(1);
        return false;
    }
    task.data = &buf;
    bthread::Mutex mux;
    bthread::ConditionVariable cv;
    UpdateClusterConfigClosure closure(request, response);
    task.done = &closure;
    node_->apply(task);
    closure.Wait();
    return response->error();
}

void RaftHostManager::ApplyClusterConfigChange(
    const ::txservice::remote::UpdateClusterConfigRequest *request,
    ::txservice::remote::UpdateClusterConfigResponse *response,
    braft::Closure *done)
{
    // The closure here is the one to return rpc request. It should not be
    // used until raft config change is applied and cluster config file is
    // updated.
    UpdateClusterConfigClosure *closure =
        static_cast<UpdateClusterConfigClosure *>(done);
    std::unordered_map<uint32_t, NodeConfig> node_configs;
    for (int i = 0; i < request->new_node_configs_size(); i++)
    {
        auto node_config_buf = request->new_node_configs(i);
        node_configs.try_emplace(node_config_buf.node_id(),
                                 node_config_buf.node_id(),
                                 node_config_buf.host_name(),
                                 node_config_buf.port());
    }

    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs;
    for (int idx = 0; idx < request->new_cluster_config_size(); idx++)
    {
        auto ng_config_buf = request->new_cluster_config(idx);

        std::vector<NodeConfig> members;
        for (int nid = 0; nid < ng_config_buf.member_nodes_size(); nid++)
        {
            NodeConfig tmp_node_conf =
                node_configs[ng_config_buf.member_nodes(nid).node_id()];
            tmp_node_conf.is_candidate_ =
                ng_config_buf.member_nodes(nid).is_candidate();
            members.push_back(std::move(tmp_node_conf));
        }
        new_ng_configs.try_emplace(ng_config_buf.ng_id(), std::move(members));
    }
    std::unique_lock<bthread::Mutex> cnf_lk(mux_);
    if (request->config_version() < config_version_)
    {
        brpc::ClosureGuard done_guard(done);
        LOG(INFO) << "Current config version is newer than the new one, "
                     "current version: "
                  << config_version_
                  << ", new version: " << request->config_version();
        if (closure)
        {
            closure->SetError(1);
        }
        return;
    }

    // if the version is the same, just ignore request and return success.
    if (request->config_version() == config_version_)
    {
        brpc::ClosureGuard done_guard(done);
        LOG(INFO) << "Current config version is the same as the new one, "
                     "current version: "
                  << config_version_
                  << ", new version: " << request->config_version();
        return;
    }

    if (request->config_version() == dirty_version_)
    {
        brpc::ClosureGuard done_guard(done);
        LOG(INFO) << "Current dirty config version is the same as the new one, "
                     "new version: "
                  << request->config_version()
                  << ", waiting for the update to finish.";
        // We cannot wait here in on_apply since it will block change peers from
        // finishing. Return immediately and make the requester retry.
        if (closure)
        {
            closure->SetError(1);
        }
        return;
    }

    // Now we're sure that we need to update cluster config. We need to first
    // update in memory cluster config, then change raft group configuration if
    // current node is the leader of the node group. After all node group config
    // changes are applied, we can update the cluster config file. Rpc request
    // will be returned after the cluster config file is updated.

    // Passing the rpc request closure to UpdateClusterConfigFileClosure so that
    // it can be triggered to return rpc request after the cluster config file
    // is updated.
    UpdateClusterConfigFileClosure *update_cluster_config_file_closure =
        new UpdateClusterConfigFileClosure(
            done, this, done ? nullptr : request);

    // Update in memory state.
    // Remove the cc nodes for the deleted nodes.
    for (auto node_it = nodes_.begin(); node_it != nodes_.end(); node_it++)
    {
        auto ng_iter = new_ng_configs.find(node_it->first);
        if (ng_iter == new_ng_configs.end())
        {
            LOG(INFO) << "remove cc node for deleted node group "
                      << node_it->first;
            // Log also needs to be truncated since the whole node group is
            // gone.
            update_cluster_config_file_closure->AddDeletedNodeGroup(
                node_it->first, true);
        }
        else
        {
            bool is_member = false;
            for (auto &node : ng_iter->second)
            {
                if (node.node_id_ == node_config_.node_id_)
                {
                    is_member = true;
                    break;
                }
            }

            if (!is_member)
            {
                // Do not truncate log since we're not removing the node group.
                // It's just we're no longer a member of the node group.
                update_cluster_config_file_closure->AddDeletedNodeGroup(
                    node_it->first, false);
            }
        }
    }

    // remove deleted node groups from cluster_configs_
    for (auto ng_it = cluster_config_.begin(); ng_it != cluster_config_.end();)
    {
        if (new_ng_configs.find(ng_it->first) == new_ng_configs.end())
        {
            std::string group_id("ng");
            group_id.append(std::to_string(ng_it->first));
            braft::rtb::remove_group(group_id);
            ng_it = cluster_config_.erase(ng_it);
        }
        else
        {
            ng_it++;
        }
    }

    for (auto &ng_pair : new_ng_configs)
    {
        bool is_member = false;
        for (auto &node : ng_pair.second)
        {
            if (node.node_id_ == node_config_.node_id_)
            {
                is_member = true;
                break;
            }
        }
        if (is_member)
        {
            auto cc_node_it = nodes_.find(ng_pair.first);
            if (cc_node_it == nodes_.end())
            {
                // If this node is a member of a new node group, create
                // cc node for it.
                auto node_ins_it = nodes_.try_emplace(
                    ng_pair.first, ng_pair.first, ng_pair.second, *this);
                assert(node_ins_it.second);
                int res = node_ins_it.first->second.Start();
                // At this point the cluster config update log is already
                // written and the scale must succeed. Keep retrying until
                // server shutdown.
                while (res != 0 && !TxProcessDead())
                {
                    bthread_usleep(10000);
                    res = node_ins_it.first->second.Start();
                }
                if (res != 0)
                {
                    return;
                }
            }
            else
            {
                // Update existing raft node if the config has been changed.
                cc_node_it->second.UpdateNodeGroupConfig(
                    ng_pair.second, update_cluster_config_file_closure);
            }
        }

        // Update ng config in cluster config
        auto ng_cnf_it = cluster_config_.try_emplace(ng_pair.first);
        ng_cnf_it.first->second = ng_pair.second;
    }
    dirty_version_ = request->config_version();

    cnf_lk.unlock();
    // start a new bthread to set all change peers started since unfinished
    // on_apply might block cluster config change from committing.
    bthread_t tid = 0;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_start_background(
        &tid,
        &attr,
        [](void *args) -> void *
        {
            static_cast<UpdateClusterConfigFileClosure *>(args)
                ->SetAllChangePeersStarted();
            return nullptr;
        },
        update_cluster_config_file_closure);
}

void RaftHostManager::CommitConfigChange()
{
    std::unique_lock<bthread::Mutex> lk(mux_);
    // Persist cluster config change to config file.
    std::string config_str;
    // total ngs
    config_str.append(std::to_string(cluster_config_.size()));
    config_str.append("\n");

    for (const auto &pair : cluster_config_)
    {
        // ng id
        config_str.append(std::to_string(pair.first));
        config_str.append(" ");

        // member nodes
        for (const auto &node : pair.second)
        {
            // node id
            config_str.append(std::to_string(node.node_id_));
            config_str.append(" ");

            // host name
            config_str.append(node.host_name_);
            config_str.append(" ");

            // port
            config_str.append(std::to_string(GET_LOCAL_TX_PORT(node.port_)));
            config_str.append(" ");

            // is candidate
            config_str.append(std::to_string(node.is_candidate_));
            config_str.append(" ");
        }
        config_str.append("\n");
    }

    // version
    config_str.append(std::to_string(dirty_version_));
    config_str.append("\n");

    lk.unlock();
    std::ofstream ofs(cluster_config_file_path_ + ".tmp");
    ofs << config_str;
    ofs.close();
    // rename the tmp file to the final file so that the file update is atomic
    std::rename((cluster_config_file_path_ + ".tmp").c_str(),
                cluster_config_file_path_.c_str());

    // Remove the tmp file
    std::filesystem::remove(cluster_config_file_path_ + ".tmp");

    lk.lock();
    assert(dirty_version_ > config_version_);
    config_version_ = dirty_version_;
    dirty_version_ = 0;
    LOG(INFO) << "update cluster config file, config version: "
              << config_version_;
    cv_.notify_all();
    lk.unlock();
    ConfigRouteTable(cluster_config_);
}

void RaftHostManager::RevertConfigChange()
{
    std::unique_lock<bthread::Mutex> lk(mux_);
    dirty_version_ = 0;
    LOG(INFO) << "revert cluster config change, dirty version: "
              << dirty_version_;
    cv_.notify_all();
}

void RaftNode::UpdateNodeGroupConfig(
    const std::vector<NodeConfig> &new_ng_config,
    UpdateClusterConfigFileClosure *update_cluster_config_file_closure)
{
    std::unique_lock<bthread::Mutex> lk(mux_);
    uint32_t node_id = ng_config_.at(node_idx_).node_id_;
    ng_config_ = new_ng_config;
    node_idx_ = NodeIdx(node_id);
    if (node_->is_leader())
    {
        // If this node is leader, update configuration in raft node.
        std::string raft_conf;
        for (size_t idx = 0; idx < ng_config_.size(); ++idx)
        {
            if (idx > 0)
            {
                raft_conf.append(",");
            }

            raft_conf.append(ng_config_.at(idx).host_name_);
            raft_conf.append(":");
            raft_conf.append(
                std::to_string(GET_RAFT_NODE_PORT(ng_config_.at(idx).port_)));
            raft_conf.append(":");
            raft_conf.append(std::to_string(0));
        }
        braft::Configuration braft_config;
        braft_config.parse_from(raft_conf);

        update_cluster_config_file_closure->IncrUnfinishedChangePeers();
        ChangePeerClosure *closure = new ChangePeerClosure(
            update_cluster_config_file_closure, braft_config, node_);
        node_->change_peers(braft_config, closure);
    }
}

void RaftNode::Remove(bool truncate_log)
{
    if (node_->is_leader() && truncate_log)
    {
        // Remove the node group from log service.
        host_manager_.log_agent_->RemoveCcNodeGroup(
            ng_id_, leader_term_.load(std::memory_order_acquire));
    }
    Shutdown();
    Join();
    // Remove local raft data path.
    std::string raft_data_path =
        host_manager_.raft_path_ + "/ng" + std::to_string(ng_id_);
    assert(raft_data_path.find("local://") == 0);
    if (raft_data_path.find("local://") == 0)
    {
        raft_data_path.erase(0, 8);
    }
    std::error_code error_code;
    bool raft_data_exist = std::filesystem::exists(raft_data_path, error_code);
    assert(raft_data_exist && error_code.value() == 0);
    if (raft_data_exist)
    {
        std::filesystem::remove_all(raft_data_path, error_code);
        if (error_code.value() != 0)
        {
            while (error_code == std::errc::directory_not_empty)
            {
                // dir not empty error on remove_all call usually means the file
                // is being used by other process. Retry removing the dir.
                bthread_usleep(1000);
                std::filesystem::remove_all(raft_data_path, error_code);
            }
            LOG(ERROR) << "Failed to remove raft data path " << raft_data_path
                       << ", error code: " << error_code.message();
            std::abort();
        }
    }
}

void RaftNode::on_apply(braft::Iterator &iter)
{
    for (; iter.valid(); iter.next())
    {
        // If iter_log.done() is EITHER null OR equal to task.done
        // - If iter_log.done() is not null, this task is applied by this
        //   node(this node is leaders). We can skip the deserialization and
        //   get the closure from iter_log.done();
        // - If iter_log.done() is null, this task is synced from the leader.
        //   We have to parse the log.
        if (iter.done())
        {
            UpdateClusterConfigClosure *closure =
                dynamic_cast<UpdateClusterConfigClosure *>(iter.done());
            auto request = closure->request();
            auto response = closure->response();
            host_manager_.ApplyClusterConfigChange(
                request, response, iter.done());
        }
        else
        {
            butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
            ::txservice::remote::UpdateClusterConfigRequest request;
            CHECK(request.ParseFromZeroCopyStream(&wrapper));

            host_manager_.ApplyClusterConfigChange(&request, nullptr, nullptr);
        }
    }
}

void RaftNode::on_leader_start(int64_t term)
{
    if (RaftHostManager::Instance().TxProcessDead())
    {
        // Skip notifying tx process if it's already dead.
        LOG(WARNING) << "Skip notifying tx process if it's already dead";
        return;
    }

    DLOG(INFO) << "RaftNode::on_leader_start, node group id = " << ng_id_
               << ", term = " << term;

    leader_term_.store(term, std::memory_order_release);

    if (HasStandByNodes() && !ng_config_.at(node_idx_).is_candidate_)
    {
        // This node is not candidate, wait for candidate nodes asking for
        // transfer leader.
        DLOG(INFO) << "Non-candidate ignore on_leader_start, waitting for "
                      "candidates transfer leadership, ng_id:"
                   << ng_id_ << ", node_idx:" << node_idx_;
        return;
    }

    // Notify tx node to escalate to leader of node group
    auto *channel = host_manager_.txservice_channel_.get();
    CcRpcService_Stub stub(channel);
    OnLeaderStartRequest req;
    req.set_node_group_id(ng_id_);
    req.set_node_group_term(term);
    // Set latest cluster config
    std::unordered_map<uint32_t, NodeConfig> node_configs;
    for (const auto &[ng_id, ng_config] : host_manager_.cluster_config_)
    {
        auto ng_config_buf = req.add_cluster_config();
        ng_config_buf->set_ng_id(ng_id);
        for (const auto &node : ng_config)
        {
            if (node_configs.find(node.node_id_) == node_configs.end())
            {
                node_configs[node.node_id_] = node;
            }
            auto node_config_buf = ng_config_buf->add_member_nodes();
            node_config_buf->set_node_id(node.node_id_);
            node_config_buf->set_is_candidate(node.is_candidate_);
        }
    }
    for (const auto &[node_id, node_config] : node_configs)
    {
        auto node_config_buf = req.add_node_configs();
        node_config_buf->set_node_id(node_id);
        node_config_buf->set_host_name(node_config.host_name_);
        node_config_buf->set_port(node_config.port_);
    }
    req.set_config_version(host_manager_.config_version_);
    OnLeaderStartResponse resp;
    brpc::Controller cntl;
    stub.OnLeaderStart(&cntl, &req, &resp, nullptr);

    // Send `OnLeaderStartRequest` to local tx node. We need to keep retrying
    // until notify successfully. Otherwise, Braft has a leader, but tx node has
    // not leader forever.
    while ((cntl.Failed() || resp.error()) &&
           !RaftHostManager::Instance().TxProcessDead())
    {
        if (cntl.Failed())
        {
            LOG(ERROR) << "Node " << host_manager_.node_config_.node_id_
                       << " failed to escalate to leader of node group "
                       << ng_id_ << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
        }
        else if (resp.error())
        {
            if (resp.retry() || !HasStandByNodes())
            {
                // This is because another request is being processed.
                LOG(INFO)
                    << "Node " << host_manager_.node_config_.node_id_
                    << " failed to process `OnLeaderStartRequest`, node group "
                    << ng_id_ << "Keep retrying";
            }
            else
            {
                if (leader_transfer_thd_.joinable())
                {
                    leader_transfer_thd_.join();
                }
                leader_transfer_thd_ = std::thread(
                    [this, target_node = resp.next_leader_node(), term = term]
                    {
                        // Txservice cannot be escalated to leader.
                        // Try to transfer leader to next candidate.
                        if (target_node != UINT32_MAX)
                        {
                            LOG(INFO) << "Transfering leader to previous "
                                         "leader "
                                      << target_node;
                            if (TransferLeader(target_node) ==
                                TransferError::Ok)
                            {
                                return;
                            }
                        }

                        int err = TransferError::Error;
                        size_t next_candidate_idx = node_idx_ + 1;
                        while (err != TransferError::Ok && node_->is_leader())
                        {
                            if (next_candidate_idx >= ng_config_.size())
                            {
                                next_candidate_idx = 0;
                            }
                            while (!ng_config_.at(next_candidate_idx)
                                        .is_candidate_ ||
                                   next_candidate_idx == node_idx_)
                            {
                                next_candidate_idx++;
                                if (next_candidate_idx >= ng_config_.size())
                                {
                                    next_candidate_idx = 0;
                                }
                            }

                            LOG(INFO)
                                << "Node "
                                << host_manager_.node_config_.node_id_
                                << " is not qualified to become the leader of #"
                                << ng_id_
                                << ". Trying to transfer leader to next "
                                   "candidate node "
                                << ng_config_.at(next_candidate_idx).node_id_;
                            err = TransferLeader(
                                ng_config_.at(next_candidate_idx).node_id_);
                            next_candidate_idx++;
                        }
                    });

                return;
            }
        }

        bthread_usleep(1000000);
        resp.Clear();
        cntl.Reset();
        stub.OnLeaderStart(&cntl, &req, &resp, nullptr);
    }

    if (RaftHostManager::Instance().TxProcessDead())
    {
        LOG(ERROR) << "Node " << host_manager_.node_config_.node_id_
                   << " failed to escalate to leader of node group " << ng_id_
                   << ". TxProcess is dead.";
    }
    else
    {
        LOG(INFO) << "Node " << host_manager_.node_config_.node_id_
                  << " escalated to leader of node group " << ng_id_;

        host_manager_.NotifyNewLeaderStart(ng_id_, term);
        host_manager_.NotifyLogReplay(ng_id_, term, resp.log_replay_start_ts());
    }
}

void RaftNode::on_leader_stop(const butil::Status &status)
{
    int64_t term = leader_term_.load(std::memory_order_acquire);

    if (term == -1)
    {
        // TODO: `on_leader_start` is not called before
        // `on_leader_stop` in some case(braft receives ReqestVote with higer
        // term before `on_leader_start` is called). We need to verify this
        // case.
        LOG(INFO) << "RaftNode::on_leader_stop, leader_term is -1, "
                     "on_leader_start has not been called";
        return;
    }

    leader_term_.store(-1, std::memory_order_release);

    if (RaftHostManager::Instance().TxProcessDead())
    {
        // Skip notifying tx process if it's already dead.
        DLOG(ERROR) << "Skip notifying tx process if it's already dead";
        return;
    }

    if (HasStandByNodes() && !ng_config_.at(node_idx_).is_candidate_)
    {
        // This node is not candidate, nothing was done on_leader_start.
        DLOG(INFO) << "Non-candidate ignore on_leader_stop, ng_id:" << ng_id_
                   << ", node_idx:" << node_idx_;
        return;
    }

    LOG(INFO) << "RaftNode::on_leader_stop, node group id = " << ng_id_
              << ", term: " << term << ", err msg: " << status.error_cstr();

    auto *channel = host_manager_.txservice_channel_.get();
    CcRpcService_Stub stub(channel);
    OnLeaderStopRequest req;
    req.set_node_group_id(ng_id_);
    req.set_node_group_term(term);
    OnLeaderChangeResponse resp;
    brpc::Controller cntl;
    int current_timeout_ms = 500;
    cntl.set_timeout_ms(current_timeout_ms);
    stub.OnLeaderStop(&cntl, &req, &resp, nullptr);

    // Send `OnLedaerStopRequest` to local tx node. We need to keep retrying
    // until notify successfully.
    while ((cntl.Failed() || resp.error()) &&
           !RaftHostManager::Instance().TxProcessDead())
    {
        if (cntl.Failed())
        {
            // tx node failed to step down as leader.
            LOG(ERROR) << "Node " << host_manager_.node_config_.node_id_
                       << " failed to step down as leader of node group "
                       << ng_id_ << ", err code: " << cntl.ErrorCode()
                       << ", err msg: " << cntl.ErrorText();

            // Check if the failure is due to timeout and increase the timeout.
            // Otherwise, on_leader_stop will never finish if the network is
            // indeed very slow.
            if (cntl.ErrorCode() == brpc::ERPCTIMEDOUT)
            {
                current_timeout_ms += 1000;
                LOG(INFO) << "Timeout occurred, raising timeout limit to "
                          << current_timeout_ms << "ms for subsequent retries";
            }
        }
        else if (resp.error())
        {
            // This is because another request is being processed.
            LOG(INFO) << "Node " << host_manager_.node_config_.node_id_
                      << " failed to process `OnLeaderStopRequest`, node group "
                      << ng_id_ << ", Keep retrying";
        }

        bthread_usleep(1000000);

        // TODO: Signal hm thread to kill this tx node
        resp.Clear();
        cntl.Reset();
        cntl.set_timeout_ms(current_timeout_ms);
        stub.OnLeaderStop(&cntl, &req, &resp, nullptr);
    }

    LOG(INFO) << "Node " << host_manager_.node_config_.node_id_
              << " stepped down as leader of node group " << ng_id_;
}

void RaftNode::on_start_following(const ::braft::LeaderChangeContext &ctx)
{
    std::unique_lock<bthread::Mutex> lk(mux_);

    LOG(INFO) << "CC node " << ng_config_.at(node_idx_).host_name_ << ":"
              << ng_config_.at(node_idx_).port_ << " starts following in ng#"
              << ng_id_ << ", term: " << ctx.term()
              << ", leader node: " << ctx.leader_id().to_string();

    int32_t leader_node_idx = NodeIdx(ctx.leader_id());
    if (leader_node_idx < 0)
    {
        LOG(WARNING) << "Skip on_start_following for new leader is not the "
                        "member node of ng# "
                     << ng_id_;
        return;
    }
    bool node_is_candidate = ng_config_.at(node_idx_).is_candidate_;
    bool leader_is_candidate = ng_config_.at(leader_node_idx).is_candidate_;
    uint32_t leader_node_id = ng_config_.at(leader_node_idx).node_id_;
    lk.unlock();

    current_term_.store(ctx.term(), std::memory_order_relaxed);
    if (node_is_candidate)
    {
        // If the leader is not candidate nodes, ask for leader transfer.
        if (!leader_is_candidate)
        {
            // If the preferred leader of node group followed a voter node,
            // request for leader transfer
            if (request_leader_transfer_thd_.joinable())
            {
                request_leader_transfer_thd_.join();
            }
            // Start a separate thread to request leader transfer so that it
            // won't block braft state machine forward.
            request_leader_transfer_thd_ = std::thread(
                [this, leader_id = ctx.leader_id(), term = ctx.term()]
                {
                    braft::NodeStatus status;
                    brpc::Channel channel;
                    do
                    {
                        bool init_channel_success = true;
                        do
                        {
                            init_channel_success = true;
                            if (leader_id.type_ ==
                                braft::PeerId::Type::HostName)
                            {
                                if (channel.Init(
                                        leader_id.hostname_addr.hostname
                                            .c_str(),
                                        leader_id.hostname_addr.port - 1,
                                        nullptr) != 0)
                                {
                                    init_channel_success = false;
                                }
                            }
                            else
                            {
                                assert(leader_id.type_ ==
                                       braft::PeerId::Type::EndPoint);
                                if (channel.Init(
                                        butil::ip2str(leader_id.addr.ip)
                                            .c_str(),
                                        leader_id.addr.port - 1,
                                        nullptr) != 0)
                                {
                                    init_channel_success = false;
                                }
                            }

                            if (!init_channel_success)
                            {
                                // Fails to establish the channel to the leader.
                                // Silently returns. LeaderTransfer will be
                                // retried if this node is still not preferred
                                // group leader.
                                LOG(ERROR)
                                    << "Failed to init the channel to the "
                                       "leader of ng#"
                                    << ng_id_ << " for leader transfer.";
                                bthread_usleep(100 * 1000L);
                            }

                            LOG(INFO) << "Node "
                                      << host_manager_.node_config_.node_id_
                                      << " is not leader of ng#" << ng_id_
                                      << ", requesting leader transfer from "
                                      << leader_id.to_string();
                            node_->get_status(&status);
                        } while (!host_manager_.TxProcessDead() &&
                                 status.state < braft::STATE_SHUTTING &&
                                 status.state != braft::STATE_LEADER &&
                                 term == current_term_.load(
                                             std::memory_order_relaxed) &&
                                 !init_channel_success);

                        if (term !=
                            current_term_.load(std::memory_order_relaxed))
                        {
                            break;
                        }

                        HostMangerService_Stub stub(&channel);
                        TransferRequest req;
                        req.set_ng_id(ng_id_);
                        req.set_node_id(host_manager_.node_config_.node_id_);
                        req.set_term(term);
                        TransferResponse res;
                        res.set_error_code(TransferError::Ok);
                        brpc::Controller cntl;
                        cntl.set_timeout_ms(3000);
                        stub.Transfer(&cntl, &req, &res, nullptr);
                        if (cntl.Failed())
                        {
                            LOG(ERROR)
                                << "Failed the Transfer RPC of ng#" << ng_id_
                                << ". Error code: " << cntl.ErrorCode()
                                << ". Msg: " << cntl.ErrorText();
                            bthread_usleep(100 * 1000L);
                        }
                        else if (res.error_code() != TransferError::Ok)
                        {
                            LOG(ERROR) << "Failed to transfer the leader of ng#"
                                       << ng_id_ << " to this node, error code:"
                                       << res.error_code();
                            if (res.error_code() != TransferError::Error)
                            {
                                break;
                            }
                            bthread_usleep(100 * 1000L);
                        }
                        else
                        {
                            LOG(INFO) << "Transfer rpc succeeds, this node "
                                         "should "
                                         "reclaim ng#"
                                      << ng_id_ << " leadership later";
                            break;
                        }
                        cntl.Reset();
                        node_->get_status(&status);
                    } while (!host_manager_.TxProcessDead() &&
                             status.state < braft::STATE_SHUTTING &&
                             status.state != braft::STATE_LEADER &&
                             term ==
                                 current_term_.load(std::memory_order_relaxed));
                });
        }
        else
        {
            auto *channel = host_manager_.txservice_channel_.get();
            CcRpcService_Stub stub(channel);
            OnStartFollowingRequest req;
            req.set_node_group_id(ng_id_);
            req.set_node_group_term(ctx.term());
            req.set_leader_node_id(leader_node_id);
            OnLeaderChangeResponse resp;
            brpc::Controller cntl;
            cntl.set_timeout_ms(5000);
            stub.OnStartFollowing(&cntl, &req, &resp, nullptr);

            while (cntl.Failed() &&
                   !RaftHostManager::Instance().TxProcessDead())
            {
                resp.Clear();
                cntl.Reset();
                cntl.set_timeout_ms(5000);
                bthread_usleep(1000000);
                stub.OnStartFollowing(&cntl, &req, &resp, nullptr);
            }
        }
    }
    else
    {
        /*
         *Adjusting the election_timeout_ms on the non-preferred leader node
         *to match the preferred leader node ensures that the non-preferred
         *leader can quickly catch up with the leader election process. This
         *prevents prolonged absence of the new leader from the node
         *group, improving system reliability.
         */
        node_->reset_election_timeout_ms(2000, 2000);
    }
}

void RaftNode::on_snapshot_save(braft::SnapshotWriter *writer,
                                braft::Closure *done)
{
    // The tx service log entry only contains cluster topology change. And it
    // is immediately persisted in cluster config file in on_apply. So we
    // don't need to save snapshot.
    brpc::ClosureGuard done_guard(done);
    // If there's is concurrent cluster config change, we should wait for it
    // to finish.
    {
        std::unique_lock<bthread::Mutex> lk(host_manager_.mux_);
        if (host_manager_.dirty_version_ != 0)
        {
            done->status().set_error(EINVAL,
                                     "Concurrent cluster config change");
        }
    }
}

TransferError RaftNode::TransferLeader(uint32_t node_id)
{
    std::unique_lock<bthread::Mutex> lk(mux_);
    LOG(INFO) << "Node group " << ng_id_ << " transfer leader from "
              << ng_config_.at(node_idx_).node_id_ << " to " << node_id
              << "...";
    if (node_->is_leader())
    {
        if (!IsCandidate(node_id))
        {
            LOG(ERROR) << "Node group " << ng_id_ << " transfer leader from "
                       << ng_config_.at(node_idx_).node_id_ << " to " << node_id
                       << " failed."
                       << "Invalid transfer leader request. "
                          "Transfer leader request should only be sent by the "
                          "preferred leader of node group.";
            return TransferError::Error;
        }
        butil::EndPoint addr;
        auto node_idx = NodeIdx(node_id);
        braft::PeerId node_peer;
        if (0 == butil::str2ip(ng_config_.at(node_idx).host_name_.c_str(),
                               &node_peer.addr.ip))
        {
            butil::str2endpoint(
                ng_config_.at(node_idx).host_name_.c_str(),
                GET_RAFT_NODE_PORT(ng_config_.at(node_idx).port_),
                &node_peer.addr);
        }
        else
        {
            node_peer.type_ = braft::PeerId::Type::HostName;
            node_peer.hostname_addr.hostname =
                ng_config_.at(node_idx).host_name_.c_str();
            node_peer.hostname_addr.port =
                GET_RAFT_NODE_PORT(ng_config_.at(node_idx).port_);
        }
        node_peer.idx = 0;

        int err = node_->transfer_leadership_to(node_peer);

        size_t retry = 3;
        while (!host_manager_.TxProcessDead() && retry > 0 && err != 0 &&
               node_->is_leader())
        {
            bthread_usleep(1000000);
            err = node_->transfer_leadership_to(node_peer);
            --retry;
        }

        if (retry == 0 && err != 0)
        {
            LOG(ERROR) << "Node group " << ng_id_ << " transfer leader from "
                       << node_idx_ << " to " << node_id << " failed."
                       << " Error code: " << err;
        }
        return err == 0 ? TransferError::Ok : TransferError::Error;
    }
    else
    {
        LOG(ERROR) << "Node group " << ng_id_
                   << " transfer leader failed. Only leader can perform "
                      "transfer leader.";
        return TransferError::NotLeader;
    }
}

TransferError RaftNode::CheckTermAndTransferLeader(uint32_t node_id,
                                                   int64_t term)
{
    braft::NodeStatus status;
    node_->get_status(&status);
    if (status.term > term)
    {
        return TransferError::TermOutdated;
    }
    return TransferLeader(node_id);
}
}  // namespace host_manager
