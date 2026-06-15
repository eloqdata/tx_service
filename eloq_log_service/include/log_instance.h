#pragma once
#include <braft/protobuf_file.h>  // braft::ProtoBufFile
#include <braft/raft.h>           // braft::Node braft::StateMachine
#include <brpc/channel.h>

#include <atomic>

#include "log_utils.h"
#ifdef WITH_CLOUD_AZ_INFO
#include <braft/route_table.h>  // braft::rtb::refresh_leader
#endif
#include <braft/storage.h>    // braft::SnapshotWriter
#include <braft/util.h>       // braft::AsyncClosureGuard
#include <brpc/controller.h>  // brpc::Controller
#include <brpc/server.h>      // brpc::Server
#include <bthread/bthread.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "log.pb.h"
#include "log_shipping_agent.h"
#include "log_state.h"

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include "rocksdb_cloud_config.h"
#endif

#if defined(LOG_STATE_TYPE_RKDB_ALL)
#include "log_state_rocksdb_impl.h"
#endif

#if defined(LOG_STATE_TYPE_MEM)
#include "log_state_memory_impl.h"
#endif

namespace txlog
{
struct Peer
{
    std::string ip;
    uint32_t port;
};

struct TaskInfo
{
    TaskInfo() = default;
    TaskInfo(const LogRequest *request,
             LogResponse *response,
             google::protobuf::Closure *done,
             uint32_t log_group_id,
             int64_t term)
        : request_(request),
          response_(response),
          done_(done),
          log_group_id_(log_group_id),
          term_(term)
    {
    }

    // TaskInfo& operator=(const TaskInfo& other)
    //{
    // request_= other.request_;
    // response_=other.response_;
    // log_group_id_=other.log_group_id_;
    // term_=other.term_;

    // return *this;
    //}

    const LogRequest *request_;
    LogResponse *response_;
    google::protobuf::Closure *done_;
    uint32_t log_group_id_;
    int64_t term_;
};

class LogInstance : public braft::StateMachine
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    ,
                    public LogStateRocksDBCloudImplObserver
#endif
{
public:
    explicit LogInstance(uint32_t log_group_id,
                         uint32_t node_id,
                         const std::string &ip,
                         uint32_t port,
                         const std::string &raft_conf,
                         const std::string &group,
                         const std::string &storage_path,
#ifdef WITH_CLOUD_AZ_INFO
                         const std::string &prefer_zone,
                         const std::string &current_zone,
#endif
#if defined(LOG_STATE_TYPE_RKDB_ALL)
                         const std::string &rocksdb_storage_path,
                         const size_t rocksdb_scan_threads,
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
                         const RocksDBCloudConfig &rocksdb_cloud_config,
                         const size_t in_mem_data_log_queue_size_high_watermark,
#else
                         const size_t sst_files_size_limit,
#endif
                         const size_t rocksdb_max_write_buffer_number,
                         const size_t rocksdb_max_background_jobs,
                         const size_t rocksdb_target_file_size_base,
#endif
                         uint32_t snapshot_interval,
                         bool enable_request_checkpoint,
                         uint32_t check_replay_log_size_interval_sec,
                         uint64_t notify_checkpointer_threshold_size)
        : log_group_id_(log_group_id),
          node_id_(node_id),
          node_(NULL),
          term_if_is_lg_leader_(-1),
          log_state_(),
#ifdef WITH_CLOUD_AZ_INFO
          prefer_zone_(prefer_zone),
          current_zone_(current_zone),
#endif
          ip_(ip),
          port_(port),
          raft_conf_(raft_conf),
          group_(group),
          storage_path_(storage_path),
          log_replay_workers_mutex_(),
          snapshot_interval_(snapshot_interval),
          enable_request_checkpoint_(enable_request_checkpoint),
          check_replay_log_size_interval_sec_(
              check_replay_log_size_interval_sec),
          notify_checkpointer_threshold_size_(
              notify_checkpointer_threshold_size),
          config_mutex_()
#if defined(LOG_STATE_TYPE_RKDB_ALL)
          ,
          rocksdb_storage_path_(rocksdb_storage_path)
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
          ,
          dbc_clone_after_leader_step_down_(nullptr)
#endif
#endif
    {
        // Parse raft_conf_ to get peers' information
        // raft_conf_ format: ip:port:0,ip:port:1,ip:port:2
        DLOG(INFO) << "This raft node conf_ is " << raft_conf_;

        for (size_t i = 0; i < raft_conf_.size();)
        {
            size_t pos = raft_conf_.find(',', i);
            if (pos == std::string::npos)
                pos = raft_conf_.size();
            std::string str = raft_conf_.substr(i, pos - i);
            i = pos + 1;

            pos = str.find(':');
            size_t pos2 = str.find(':', pos + 1);
            Peer peer;
            peer.ip = str.substr(0, pos);
            peer.port = std::stoi(str.substr(pos + 1, pos2 - pos - 1));
            peer_vct_.push_back(peer);
        }

#if defined(LOG_STATE_TYPE_RKDB_ALL)
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
        log_state_ = std::make_unique<LogStateRocksDBCloudImpl>(
            rocksdb_storage_path_,
            rocksdb_cloud_config,
            term_if_is_lg_leader_,
            this,
            in_mem_data_log_queue_size_high_watermark,
            rocksdb_max_write_buffer_number,
            rocksdb_max_background_jobs,
            rocksdb_target_file_size_base,
            rocksdb_scan_threads);
#else
        // specify log_state rocksdb path from braft storage path, trimming
        // "local://" prefix.
        log_state_ = std::make_unique<LogStateRocksDBImpl>(
            rocksdb_storage_path_, sst_files_size_limit, rocksdb_scan_threads);
#endif

#else
        log_state_ = std::make_unique<LogStateMemoryImpl>();
#endif
    }

    ~LogInstance() override
    {
        shutdown();
        join();
        delete node_;
    }

    static void SetWriteLogErrorResponse(
        const LogRequest *request,
        LogResponse *response,
        LogResponse::ResponseStatus status = LogResponse_ResponseStatus_Fail);

    void GetLogGroupConfig(const GetLogGroupConfigRequest *request,
                           GetLogGroupConfigResponse *response,
                           google::protobuf::Closure *done)
    {
        brpc::ClosureGuard done_guard(done);

        // Get the current configuration
        std::unique_lock<bthread::Mutex> lock(config_mutex_);
        for (const auto &peer : peer_vct_)
        {
            response->add_ip(peer.ip);
            response->add_port(peer.port);
        }
        response->set_error(false);
    }

    static void SetWriteLogErrorResponse(const LogRequest *request,
                                         LogResponse *response);

    int Start()
    {
        LOG(INFO) << "starting log instance at " << ip_ << ":" << port_;

        // start rocksdb log_state if it is not rocksdb cloud.
        DLOG(INFO) << "starting log_state";
        int log_state_start_status = log_state_->Start();
        if (log_state_start_status != 0)
        {
            LOG(ERROR) << "Fail to start log_state";
            return log_state_start_status;
        }

        braft::NodeOptions node_options = BaseNodeOptions();
        /*
         * Setting the log node election_timeout_ms to 5 seconds on the
         * non-preferred leader node is to ensure that the preferred leader is
         * elected promptly during the boot-up phase of the log node cluster.
         */
#ifndef NDEBUG
        node_options.election_timeout_ms = IsPreferLeader() ? 1000 : 5000;
#endif
        braft::PeerId log_node;
        int err = SetDefaultOptions(&node_options);
        if (err != 0)
        {
            LOG(ERROR) << "Fail to SetDefaultOption";
            return err;
        }
        // assign different peerId if there are more than one raft instance in
        // one server.
        DLOG(INFO) << "initializing raft node";
        if (0 != butil::str2ip(ip_.c_str(), &log_node.addr.ip))
        {
            // for case `ip_` is hostname format
            log_node.type_ = braft::PeerId::Type::HostName;
            log_node.hostname_addr.hostname = ip_;
            log_node.hostname_addr.port = port_;

#ifdef WITH_CLOUD_AZ_INFO
            log_node.prefer_zone.assign(prefer_zone_);
            log_node.current_zone.assign(current_zone_);
#endif
            // node init process would check ip addr is not butil::IP_ANY
            log_node.addr.ip = butil::my_ip();
            log_node.addr.port = port_;
        }
        else
        {
            butil::str2endpoint(ip_.c_str(), port_, &log_node.addr);
        }
        node_ = new braft::Node(group_, log_node);
        DLOG(INFO) << "This raft node initial conf is "
                   << node_options.initial_conf;
        if (node_->init(node_options) != 0)
        {
            LOG(ERROR) << "Fail to init raft node";
            delete node_;
            node_ = nullptr;
            return -1;
        }
        return 0;
    }

    bool WaitForLeaderReady()
    {
        if (term_if_is_lg_leader_.load(butil::memory_order_acquire) == -1)
        {
            LOG(INFO) << "This node is lg" << log_group_id_ << "not leader";
            return false;
        }

        return true;
    }

    // Implements Service methods
    void WriteLog(const LogRequest *request,
                  LogResponse *response,
                  google::protobuf::Closure *done);

    void ReplayLog(const LogRequest *request,
                   LogResponse *response,
                   google::protobuf::Closure *done);

    void NotifyCheckpointTs(const LogRequest *request,
                            LogResponse *response,
                            google::protobuf::Closure *done);

    void RemoveCcNodeGroup(const LogRequest *request,
                           LogResponse *response,
                           google::protobuf::Closure *done);

    void RecoverTx(const RecoverTxRequest *request,
                   RecoverTxResponse *response,
                   google::protobuf::Closure *done);

    void CheckMigrationIsFinished(
        const CheckMigrationIsFinishedRequest *request,
        CheckMigrationIsFinishedResponse *response,
        google::protobuf::Closure *done);

    void CheckClusterScaleStatus(const LogRequest *request,
                                 LogResponse *response,
                                 google::protobuf::Closure *done);

    void TransferLeader(const TransferRequest *request,
                        TransferResponse *response,
                        google::protobuf::Closure *done);
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    void OnInMemStateFull(
        size_t log_count,
        size_t log_size,
        std::function<void(bool, uint64_t)> done) const override;
#endif

    static void *ManualSnapshot(void *arg);

    void ApplyTask(const braft::Task &task)
    {
        node_->apply(task);
    }

    int64_t GetTermIfIsLgLeader() const
    {
        return term_if_is_lg_leader_.load(butil::memory_order_acquire);
    }

    bool is_leader() const
    {
        return term_if_is_lg_leader_.load(butil::memory_order_acquire) > 0;
    }

    // Shut this node down.
    void shutdown()
    {
        if (node_)
        {
            node_->shutdown(nullptr);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join()
    {
        if (node_)
        {
            node_->join();
        }
        if (leader_transfer_thd_.joinable())
        {
            leader_transfer_thd_.join();
        }
    }

    void CheckReplayLogSizeAndNotifyCkptIfNeeded();

    void ChangePeersToAdd(const std::vector<Peer> &new_peers,
                          uint32_t log_group_id,
                          ChangePeersResponse *response,
                          google::protobuf::Closure *done);
    void ChangePeersToRemove(const std::vector<Peer> &remove_peers,
                             uint32_t log_group_id,
                             ChangePeersResponse *response,
                             google::protobuf::Closure *done);

private:
    void UpdateInMemoryConfig(const braft::Configuration &new_peers);
    uint32_t log_group_id_;
    uint32_t node_id_;
    braft::Node *volatile node_;
    /**
     * log raft group term if I am the leader, otherwise -1.
     * might be false positive, i.e. this node might not know it's not leader
     * anymore and still thinks it's leader.
     * This can be used to determine this node is not leader but can not be
     * used to determine this node is the latest leader. Use
     * leader_lease_valid() instead.
     */
    butil::atomic<int64_t> term_if_is_lg_leader_;
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    std::unique_ptr<LogStateRocksDBCloudImpl> log_state_;
#ifdef WITH_CLOUD_AZ_INFO
    std::string prefer_zone_;
    std::string current_zone_;
#endif
#elif defined(LOG_STATE_TYPE_RKDB)
    std::unique_ptr<LogStateRocksDBImpl> log_state_;
#else
    std::unique_ptr<LogState> log_state_;
#endif
    std::string ip_;
    uint32_t port_;
    std::string raft_conf_;
    std::vector<Peer> peer_vct_;  // Parsed from raft_conf_
    std::string group_;
    std::string storage_path_;
    std::unordered_map<uint32_t, std::unique_ptr<LogShippingAgent>>
        log_replay_workers_;
    std::mutex log_replay_workers_mutex_;
    uint32_t snapshot_interval_{600};
    bool enable_request_checkpoint_;
    uint32_t check_replay_log_size_interval_sec_;
    uint64_t notify_checkpointer_threshold_size_;
    std::atomic<bool> stop_replay_log_size_checker_thd_{false};
    std::mutex stop_replay_log_size_checker_mutex_;
    std::condition_variable stop_replay_log_size_checker_cond_;
    std::thread replay_log_size_checker_thd_;
    std::thread leader_transfer_thd_;

    brpc::Channel channel_;
    bthread::Mutex config_mutex_;  // Mutex to protect raft_conf_ and peer_vct_
                                   //
#if defined(LOG_STATE_TYPE_RKDB_ALL)
    std::string rocksdb_storage_path_;
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    /**
     * The last sst file number of the db cloud before the latest snapshot flush
     */
    std::shared_ptr<DBCloudContainer> dbc_clone_after_leader_step_down_{
        nullptr};
#endif

    friend class WriteLogClosure;

    struct LogStateAddLogItemTask
    {
        const WriteLogRequest *write_log_request{};
        braft::Closure *done{};
    };

    static constexpr size_t kLogStateAddLogItemTaskBatchSize = 256;

    bool BatchAddLogItems(
        std::array<LogStateAddLogItemTask, kLogStateAddLogItemTaskBatchSize>
            &write_log_tasks,
        size_t num) const;

    void on_apply(braft::Iterator &iter) override;

    enum struct ExecuteResult
    {
        Finished = 0,
        NeedBatch = 1
    };

    ExecuteResult execute(const LogRequest *request, LogResponse *response);

    void on_leader_start(int64_t term) override
    {
        LOG(INFO) << "Log node " << ip_ << ":" << port_
                  << " becomes the leader of the log group #" << log_group_id_
                  << ", term: " << term << ", leader lease valid? "
                  << (IsLatestLeader() ? "yes" : "no");

        term_if_is_lg_leader_.store(term, butil::memory_order_release);
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
        // Open RocksDB Cloud in async manner, and log item will be cached in
        // in-memory state
        log_state_->AsyncStartCloudDB(term - 1, term);
#endif
        LogGroupLeaderUpdate();
        if (enable_request_checkpoint_ &&
            !replay_log_size_checker_thd_.joinable())
        {
            stop_replay_log_size_checker_thd_.store(false,
                                                    std::memory_order_release);
            replay_log_size_checker_thd_ = std::thread(
                &LogInstance::CheckReplayLogSizeAndNotifyCkptIfNeeded, this);
            LOG(INFO) << "Start checking replay log size every "
                      << check_replay_log_size_interval_sec_ << " seconds.";
            LOG(INFO) << "notify_checkpointer_threshold_size: "
                      << FormatSize(notify_checkpointer_threshold_size_);
        }
    }

    void on_leader_stop(const butil::Status &status) override;

    void on_shutdown() override
    {
    }

    void on_error(const ::braft::Error &e) override
    {
        LOG(ERROR) << "Met raft error " << e;
    }

    using braft::StateMachine::on_configuration_committed;
    void on_configuration_committed(const ::braft::Configuration &conf) override
    {
        UpdateInMemoryConfig(conf);
    }

    void on_stop_following(const ::braft::LeaderChangeContext &ctx) override
    {
    }

    bool IsPreferLeader()
    {
        std::string my_peer = ip_ + ":" + std::to_string(port_) + ":0";
        if (raft_conf_.find(my_peer) == 0)
        {
            return true;
        }
        return false;
    }

#ifdef WITH_CLOUD_AZ_INFO
    bool IsPreferLeaderWithZoneInfo()
    {
        if (prefer_zone_.length() == 0 || current_zone_.length() == 0)
        {
            return IsPreferLeader();
        }

        if (prefer_zone_ == current_zone_)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
#endif

    void on_start_following(const ::braft::LeaderChangeContext &ctx) override;

    void on_snapshot_save(braft::SnapshotWriter *writer,
                          braft::Closure *done) override;

    int on_snapshot_load(braft::SnapshotReader *reader) override
    {
        LOG(INFO) << "start to load snapshot ";
        CHECK_EQ(-1, term_if_is_lg_leader_)
            << "Leader is not supposed to load snapshot";
        log_state_->Clear();
        const std::string &snapshot_path = reader->get_path();
        std::vector<std::string> files;
        reader->list_files(&files);
        int read_snapshot_status =
            log_state_->ReadSnapshot(snapshot_path, files);
        LOG(INFO) << "finish loading snapshot";
        return read_snapshot_status;
    }

    static void *save_snapshot(void *arg);

    braft::NodeOptions BaseNodeOptions()
    {
        braft::NodeOptions node_options;
        node_options.election_timeout_ms = 1000;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = snapshot_interval_;
        node_options.disable_cli = false;
        return node_options;
    }

    /*
     * Set braft node configuration.
     */
    int SetDefaultOptions(braft::NodeOptions *node_options);

    void LogGroupLeaderUpdate();

    /**
     * If this node is the group's latest leader.
     * Only available when all nodes in the raft group set
     * |raft_enable_leader_lease| flag to true.
     * If returns true, this node is guaranteed to be the latest leader for now.
     * @return
     */
    bool IsLatestLeader() const
    {
        return node_->is_leader_lease_valid();
    }

    void ProposeTaskAndWait(braft::Task &task,
                            const LogRequest *request,
                            LogResponse *response);
};

struct SnapshotClosure
{
    LogState *log_state_ref;
    LogInstance *log_instance_ref;
    braft::SnapshotWriter *writer;
    braft::Closure *done;
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    // copy the dbc_ from log_state at the time of on_snapshot_save to prevent
    // leader step down during the save_snapshot
    std::shared_ptr<DBCloudContainer> dbc_flush;
    // leader term when on_snapshot_save
    int64_t leader_term;
    uint64_t last_tx_num;
#endif
};

class SaveSnapshotDone : public braft::Closure
{
public:
    explicit SaveSnapshotDone(uint64_t log_count,
                              std::function<void(bool, uint64_t)> done)
        : log_count_(log_count), done_(std::move(done))
    {
    }

    void Run() override
    {
        LOG(INFO) << "TriggerSnapshotClosure::Run";
        std::unique_ptr<SaveSnapshotDone> self_guard(this);
        if (status().ok())
        {
            LOG(INFO) << "TriggerSnapshotClosure::Run status ok";
            done_(true, log_count_);
        }
        else
        {
            LOG(ERROR) << "Fail to trigger snapshot, status: "
                       << status().error_str();
            done_(false, 0);
        }
    }

private:
    uint64_t log_count_;
    std::function<void(bool, uint64_t)> done_;
};

}  // namespace txlog
