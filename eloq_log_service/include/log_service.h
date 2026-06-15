#pragma once

#include <braft/node.h>
#include <braft/node_manager.h>
#include <braft/route_table.h>
#include <brpc/closure_guard.h>
#include <bthread/mutex.h>
#include <google/protobuf/stubs/callback.h>

#include <atomic>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "INIReader.h"
#include "log.pb.h"
#include "log_instance.h"
#include "log_util.h"
#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include "rocksdb_cloud_config.h"
#endif

namespace txlog
{
/*
 * LogServiceImpl is defined in log.proto, it implements the log
 * request interface.
 */
class LogServiceImpl : public LogService
{
public:
    enum Status
    {
        DETACHED = 0,
        DETACHING = 1,
        ATTACHED = 2,
        ATTACHING = 3
    };

    LogServiceImpl()
    {
    }

    /*
     * Add log group into local log replica map
     *
     * Each raft log service node could belong to multiple log groups. In
     * current design, one log service node could serves at most two log groups.
     * For example, log service nodes includes n1,n2,n3,n4 and replica num = 3.
     * Then there are two log groups (n1,n2,n3), (n4,n1,n2). We have n1 and n2
     * belong to two log groups.
     */
    void AddLogReplicas(uint32_t node_id,
                        const std::vector<std::string> &ip_list,
                        const std::vector<uint16_t> &port_list,
                        const std::string &storage_path,
                        const uint32_t start_log_group_id,
                        uint32_t log_group_replica_num,
#ifdef WITH_CLOUD_AZ_INFO
                        const std::string &prefer_zone,
                        const std::string &current_zone,
#endif
#if defined(LOG_STATE_TYPE_RKDB_ALL)
                        const std::string &rocksdb_storage_path,
                        const size_t rocksdb_scan_threads,
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
                        txlog::RocksDBCloudConfig rocksdb_cloud_config,
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
    {
        std::pair<std::unordered_map<uint32_t, LogUtil::RaftGroupConfig>,
                  uint32_t>
            log_raft_group_config = LogUtil::GenerateLogRaftGroupConfig(
                ip_list, port_list, start_log_group_id, log_group_replica_num);
        assert(log_replicas_.empty());

        for (auto &[log_group_id, raft_group_config] :
             log_raft_group_config.first)
        {
            std::string log_group_name = raft_group_config.group_name_;
            std::string log_group_conf = raft_group_config.group_conf_;
            if (braft::rtb::update_configuration(log_group_name,
                                                 log_group_conf) != 0)
            {
                LOG(ERROR)
                    << "Fail to register in the routing table the log group "
                    << log_group_conf;
            }

            std::vector<uint32_t> group_nodes = raft_group_config.group_nodes_;
            for (uint32_t nid : group_nodes)
            {
                if (nid == node_id)
                {
                    // The log group has a replica in this node. Puts the log
                    // replica under the log rpc service.
                    std::string log_path(storage_path);
                    log_path.append("/");
                    log_path.append(std::to_string(log_group_id));

#if defined(LOG_STATE_TYPE_RKDB_ALL)
                    // Same as log_path, but for rocksdb
                    std::string rocksdb_path(rocksdb_storage_path);
                    rocksdb_path.append("/");
                    rocksdb_path.append(std::to_string(log_group_id));
#endif

                    std::string log_group("lg");
                    log_group.append(std::to_string(log_group_id));
                    log_replicas_.try_emplace(
                        log_group_id,
                        log_group_id,
                        node_id,
                        ip_list.at(node_id),
                        port_list.at(node_id),
                        log_group_conf,
                        log_group,
                        log_path,
#ifdef WITH_CLOUD_AZ_INFO
                        prefer_zone,
                        current_zone,
#endif
#if defined(LOG_STATE_TYPE_RKDB_ALL)
                        rocksdb_path,
                        rocksdb_scan_threads,
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
                        rocksdb_cloud_config,
                        in_mem_data_log_queue_size_high_watermark,
#else
                        sst_files_size_limit,
#endif
                        rocksdb_max_write_buffer_number,
                        rocksdb_max_background_jobs,
                        rocksdb_target_file_size_base,
#endif
                        snapshot_interval,
                        enable_request_checkpoint,
                        check_replay_log_size_interval_sec,
                        notify_checkpointer_threshold_size);
                }
            }
        }
    }

    /*
     * Find the right raft log instance based on log group id and append
     * new log records.
     */
    void WriteLog(::google::protobuf::RpcController *controller,
                  const LogRequest *request,
                  LogResponse *response,
                  ::google::protobuf::Closure *done) override
    {
        // Assume no write log request should come in if detach is called.
        // Computes the log group ID from the tx ID.
        const WriteLogRequest &log_req = request->write_log_request();

        // The Write log caller determine the log group
        uint32_t log_group_id = log_req.log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            brpc::ClosureGuard done_guard(done);
            LogInstance::SetWriteLogErrorResponse(
                request, response, LogResponse_ResponseStatus_NotLeader);
            return;
        }

        logger_it->second.WriteLog(request, response, done);
    }

    void AttachLog(::google::protobuf::RpcController *controller,
                   const ::txlog::AttachLogRequest *request,
                   ::txlog::AttachLogResponse *response,
                   ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);

        // Only one thread can attachlog/detachlog
        Status expected = Status::DETACHED;
        if (!service_status_.compare_exchange_strong(
                expected, Status::ATTACHING, std::memory_order_acq_rel))
        {
            if (expected == Status::ATTACHED)
            {
                // Do nothing
                response->set_err_code(::txlog::AttachLogErrorCode::NO_ERROR);
                response->set_err_msg("ok");
                return;
            }
            else if (expected == Status::ATTACHING)
            {
                // wait for attaching to finish
                while (service_status_.load(std::memory_order_relaxed) !=
                       Status::ATTACHED)
                {
                    bthread_usleep(100 * 1000);
                }
                return;
            }
            else
            {
                // CP should make sure that status is not in detaching before
                // calling attach log.
                LOG(ERROR)
                    << "AttachLog called when log service in detaching status.";
                std::abort();
            }
        }

        INIReader config_reader(request->config_path());

        if (request->config_path().empty())
        {
            LOG(ERROR) << "Error: Can't load empty config file";
            service_status_.store(Status::DETACHED, std::memory_order_release);
            response->set_err_code(
                ::txlog::AttachLogErrorCode::CONFIG_PARSE_ERROR);
            response->set_err_msg("The config_path argument is empty");
            return;
        }

        if (config_reader.ParseError() != 0)
        {
            std::string err_msg =
                "Failed to parse config file, The first error line is ";
            err_msg += std::to_string(config_reader.ParseError());
            LOG(ERROR) << err_msg;

            service_status_.store(Status::DETACHED, std::memory_order_release);
            response->set_err_code(
                ::txlog::AttachLogErrorCode::CONFIG_PARSE_ERROR);
            response->set_err_msg(err_msg);
            return;
        }

        uint32_t start_log_group_id =
            config_reader.GetInteger("local", "start_log_group_id", 0);
        std::string conf = config_reader.GetString("local", "conf", "");
        int32_t node_id = config_reader.GetInteger("local", "node_id", 0);
        uint32_t snapshot_interval =
            config_reader.GetInteger("local", "snapshot_interval", 600);
        std::string storage_path = config_reader.GetString(
            "local", "storage_path", "/tmp/log_service/raft_data");
        uint32_t log_group_replica_num =
            config_reader.GetInteger("local", "log_group_replica_num", 3);
        bool enable_request_checkpoint = config_reader.GetBoolean(
            "local", "enable_request_checkpoint", false);
        uint32_t check_replay_log_size_interval_sec = config_reader.GetInteger(
            "local", "check_replay_log_size_interval_sec", 10);
        std::string notify_checkpointer_threshold_size_str =
            config_reader.GetString(
                "local", "notify_checkpointer_threshold_size", "1GB");

#if defined(LOG_STATE_TYPE_RKDB_ALL)
        std::string rocksdb_storage_path =
            config_reader.GetString("local", "rocksdb_storage_path", "");
        if (rocksdb_storage_path.empty())
        {
            rocksdb_storage_path = storage_path + "/rocksdb";
        }
        uint32_t rocksdb_max_write_buffer_number = config_reader.GetInteger(
            "rocksdb", "rocksdb_max_write_buffer_number", 8);
        uint32_t rocksdb_max_background_jobs = config_reader.GetInteger(
            "rocksdb", "rocksdb_max_background_jobs", 12);
        std::string rocksdb_target_file_size_base_str = config_reader.GetString(
            "rocksdb", "rocksdb_target_file_size_base", "64MB");
        std::string rocksdb_sst_files_size_limit_str = config_reader.GetString(
            "rocksdb", "rocksdb_sst_files_size_limit", "500MB");
        uint32_t rocksdb_scan_threads =
            config_reader.GetInteger("rocksdb", "rocksdb_scan_threads", 1);
#endif

#if defined(LOG_STATE_TYPE_RKDB_S3)
        std::string aws_access_key_id =
            config_reader.GetString("rocksdb_cloud", "aws_access_key_id", "");
        std::string aws_secret_key =
            config_reader.GetString("rocksdb_cloud", "aws_secret_key", "");
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
        std::string region =
            config_reader.GetString("rocksdb_cloud", "region", "");
        std::string bucket_name =
            config_reader.GetString("rocksdb_cloud", "bucket_name", "");

        std::string bucket_prefix =
            config_reader.GetString("rocksdb_cloud", "bucket_prefix", "");
        std::string sst_file_cache_size = config_reader.GetString(
            "rocksdb_cloud", "sst_file_cache_size", "10GB");
        uint32_t rocksdb_cloud_ready_timeout = config_reader.GetInteger(
            "rocksdb_cloud", "rocksdb_cloud_ready_timeout", 10);
        uint32_t rocksdb_cloud_file_deletion_delay = config_reader.GetInteger(
            "rocksdb_cloud", "rocksdb_cloud_file_deletion_delay", 3600);
        uint32_t log_retention_days =
            config_reader.GetInteger("rocksdb_cloud", "log_retention_days", 90);
        std::string log_purger_schedule = config_reader.GetString(
            "rocksdb_cloud", "log_purger_schedule", "00:00:01");
        std::string archive_object_path =
            config_reader.GetString("rocksdb_cloud", "archive_object_path", "");
        uint32_t archive_move_interval_seconds = config_reader.GetInteger(
            "rocksdb_cloud", "archive_move_interval_seconds", 600);
        uint32_t in_mem_data_log_queue_size_high_watermark =
            config_reader.GetInteger(
                "rocksdb",
                "in_mem_data_log_queue_size_high_watermark",
                50 * 10000);
#endif
#ifdef WITH_CLOUD_AZ_INFO
        std::string prefer_zone =
            config_reader.GetString("rocksdb_cloud", "prefer_zone", "");
        std::string current_zone =
            config_reader.GetString("rocksdb_cloud", "current_zone", "");
#endif

        LOG(INFO)
            << "Attach log service, new configuration: "
            << "start_log_group_id = " << start_log_group_id
            << ", conf = " << conf << ", node id = " << node_id
            << ", snpashot interval = " << snapshot_interval
            << ", storage path = " << storage_path
            << ", log_group_replica_num = " << log_group_replica_num
            << ", enable_request_checkpoint = " << enable_request_checkpoint
            << ", check_replay_log_size_interval_sec = "
            << check_replay_log_size_interval_sec
            << ", notify_checkpointer_threashold_size = "
            << notify_checkpointer_threshold_size_str
#if defined(LOG_STATE_TYPE_RKDB_S3)
            << ", aws_access_key_id = " << aws_access_key_id
            << ", aws_secret_key = " << aws_secret_key
#endif
#if defined(LOG_STATE_TYPE_RKDB_ALL)
            << ", rocksdb_storage_path = " << rocksdb_storage_path
            << ", rocksdb_max_write_buffer_number = "
            << rocksdb_max_write_buffer_number
            << ", rocksdb_max_backgroud_jobs = " << rocksdb_max_background_jobs
            << ", rocksdb_target_file_size_base = "
            << rocksdb_target_file_size_base_str
            << ", rocksdb_scan_threads = " << rocksdb_scan_threads
#if defined(LOG_STATE_TYPE_RKDB)
            << ", rocksdb_sst_files_size_limit = "
            << rocksdb_sst_files_size_limit_str
#endif
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
            << ", region = " << region << ", bucket_name = " << bucket_name
            << ", bucket_prefix = " << bucket_prefix
            << ", sst_file_cache_size = " << sst_file_cache_size
            << ", rocksdb_cloud_ready_timeout = " << rocksdb_cloud_ready_timeout
            << ", rocksdb_cloud_file_deletion_delay = "
            << rocksdb_cloud_file_deletion_delay
            << ", log_retention_days = " << log_retention_days
            << ", log_purger_schedule = " << log_purger_schedule
            << ", archive_object_path = " << archive_object_path
            << ", archive_move_interval_seconds = "
            << archive_move_interval_seconds
            << ", in_mem_data_log_queue_size_high_watermark = "
            << in_mem_data_log_queue_size_high_watermark
#endif
#endif
#ifdef WITH_CLOUD_AZ_INFO
            << ", prefer_zone = " << prefer_zone
            << ", current_zone = " << current_zone
#endif
            ;

        std::vector<std::string> ip_list;
        std::vector<uint16_t> port_list;
        std::vector<std::string> ip_port_list = txlog::split(conf, ",");
        if ((uint16_t) node_id >= ip_port_list.size())
        {
            std::string err_msg =
                "Invalid configuration: `node_id` must be less than node size, "
                "node id = " +
                std::to_string(node_id) +
                ", node size = " + std::to_string(ip_port_list.size());

            LOG(ERROR) << err_msg;

            service_status_.store(Status::DETACHED, std::memory_order_release);
            response->set_err_code(
                ::txlog::AttachLogErrorCode::CONFIG_PARSE_ERROR);
            response->set_err_msg(err_msg);
            return;
        }
        for (const auto &ip_port : ip_port_list)
        {
            auto p = ip_port.find(':');
            if (p == std::string::npos)
            {
                std::string err_msg =
                    "Invalid configuration: expecting "
                    "ip:port,ip:port,ip:port... in conf";

                LOG(ERROR) << err_msg;

                service_status_.store(Status::DETACHED,
                                      std::memory_order_release);
                response->set_err_code(
                    ::txlog::AttachLogErrorCode::CONFIG_PARSE_ERROR);
                response->set_err_msg(err_msg);
                return;
            }

            std::string ip = ip_port.substr(0, p), port = ip_port.substr(p + 1);
            ip_list.push_back(ip);
            port_list.emplace_back(std::stoi(port));
        }

        uint64_t notify_checkpointer_threshold_size =
            txlog::parse_size(notify_checkpointer_threshold_size_str);
#if defined(LOG_STATE_TYPE_RKDB_ALL)
        uint64_t rocksdb_target_file_size_base =
            txlog::parse_size(rocksdb_target_file_size_base_str);
#if !defined(LOG_STATE_TYPE_RKDB_CLOUD)
        size_t rocksdb_sst_files_size_limit =
            txlog::parse_size(rocksdb_sst_files_size_limit_str);
#endif
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
        txlog::RocksDBCloudConfig rocksdb_cloud_config;

#if (LOG_STATE_TYPE_RKDB_S3)
        rocksdb_cloud_config.aws_access_key_id_ = aws_access_key_id;
        rocksdb_cloud_config.aws_secret_key_ = aws_secret_key;
#endif

        rocksdb_cloud_config.region_ = region;
        rocksdb_cloud_config.bucket_name_ = bucket_name;
        rocksdb_cloud_config.bucket_prefix_ =
            bucket_prefix + std::to_string(start_log_group_id) + '-';
        rocksdb_cloud_config.sst_file_cache_size_ =
            txlog::parse_size(sst_file_cache_size);
        rocksdb_cloud_config.db_ready_timeout_us_ =
            rocksdb_cloud_ready_timeout * 1000 * 1000;
        rocksdb_cloud_config.db_file_deletion_delay_ =
            rocksdb_cloud_file_deletion_delay;
        rocksdb_cloud_config.log_retention_days_ = log_retention_days;
        rocksdb_cloud_config.archive_object_path_ =
            archive_object_path.empty()
                ? bucket_prefix + std::to_string(start_log_group_id) + '-' +
                      bucket_name + "_archives"
                : archive_object_path;
        rocksdb_cloud_config.archive_move_interval_seconds_ =
            archive_move_interval_seconds;

        std::tm log_purger_tm{};
        std::istringstream iss(log_purger_schedule);
        iss >> std::get_time(&log_purger_tm, "%H:%M:%S");

        if (iss.fail())
        {
            std::string err_msg =
                "The argument `log_purger_schedule` has invalid time format. "
                "expected format: HH:MM:SS";
            LOG(ERROR) << err_msg;
            service_status_.store(Status::DETACHED, std::memory_order_release);

            response->set_err_code(
                ::txlog::AttachLogErrorCode::CONFIG_PARSE_ERROR);
            response->set_err_msg(err_msg);

            return;
        }
        else
        {
            rocksdb_cloud_config.log_purger_starting_hour_ =
                log_purger_tm.tm_hour;
            rocksdb_cloud_config.log_purger_starting_minute_ =
                log_purger_tm.tm_min;
            rocksdb_cloud_config.log_purger_starting_second_ =
                log_purger_tm.tm_sec;
        }

#endif

        AddLogReplicas(node_id,
                       ip_list,
                       port_list,
                       std::string("local://") + storage_path,
                       start_log_group_id,
                       log_group_replica_num,
#ifdef WITH_CLOUD_AZ_INFO
                       prefer_zone,
                       current_zone,
#endif

#if defined(LOG_STATE_TYPE_RKDB_ALL)
                       rocksdb_storage_path,
                       rocksdb_scan_threads,
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
                       rocksdb_cloud_config,
                       in_mem_data_log_queue_size_high_watermark,
#else

                       rocksdb_sst_files_size_limit,
#endif
                       rocksdb_max_write_buffer_number,
                       rocksdb_max_background_jobs,
                       rocksdb_target_file_size_base,
#endif
                       snapshot_interval,
                       enable_request_checkpoint,
                       check_replay_log_size_interval_sec,
                       notify_checkpointer_threshold_size);

        assert(service_status_ == Status::DETACHED);

        // Start braft node
        int err = Start();
        if (err != 0)
        {
            LOG(ERROR) << "Attach log service: fail to start raft node";
            log_replicas_.clear();
            service_status_.store(Status::DETACHED, std::memory_order_release);
            response->set_err_code(
                ::txlog::AttachLogErrorCode::INSTANCE_INIT_ERROR);
            response->set_err_msg("Failed to start raft log instance");
        }
        else
        {
            LOG(INFO) << "Attach log service: service is attached";
            service_status_.store(Status::ATTACHED, std::memory_order_release);
            response->set_err_code(::txlog::AttachLogErrorCode::NO_ERROR);
            response->set_err_msg("ok");
        }
    }

    void DetachLog(::google::protobuf::RpcController *controller,
                   const ::google::protobuf::Empty *request,
                   ::google::protobuf::Empty *response,
                   ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);

        // Only one thread can attachlog/detachlog
        Status expected = Status::ATTACHED;
        if (!service_status_.compare_exchange_strong(
                expected, Status::DETACHING, std::memory_order_acq_rel))
        {
            if (expected == Status::DETACHED)
            {
                LOG(INFO) << "Detach log service: service is detached";
                // Do nothing
                return;
            }
            else if (expected == Status::DETACHING)
            {
                // wait for detaching to finish
                while (service_status_.load(std::memory_order_relaxed) !=
                       Status::DETACHED)
                {
                    bthread_usleep(100 * 1000);
                }
                return;
            }
            else
            {
                // Invalid status. CP should check log service status before
                // calling this method.
                LOG(ERROR)
                    << "DetachLog called when log service in attaching status.";
                std::abort();
            }
        }

        for (auto &[node_id, instance] : log_replicas_)
        {
            // shutdown raft node.
            // tx service node is shutdown first before log service node so
            // there should not be any other concurrent rpcs.
            instance.shutdown();
            instance.join();
        }
        log_replicas_.clear();

        service_status_.store(Status::DETACHED, std::memory_order_release);
        LOG(INFO) << "Detach log service: service is detached";
    }

    void CheckClusterScaleStatus(::google::protobuf::RpcController *controller,
                                 const LogRequest *request,
                                 LogResponse *response,
                                 ::google::protobuf::Closure *done) override
    {
        const CheckClusterScaleStatusRequest &check_status_req =
            request->check_scale_status_request();

        uint32_t log_group_id = check_status_req.log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            brpc::ClosureGuard done_guard(done);
            response->set_response_status(
                LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);
            return;
        }

        logger_it->second.CheckClusterScaleStatus(request, response, done);
    }

    void CheckMigrationIsFinished(
        ::google::protobuf::RpcController *controller,
        const ::txlog::CheckMigrationIsFinishedRequest *request,
        ::txlog::CheckMigrationIsFinishedResponse *response,
        ::google::protobuf::Closure *done) override
    {
        uint32_t log_group_id = request->log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            LOG(ERROR)
                << "CheckMigrationIsFinished request toward the log group #"
                << log_group_id
                << " is directed to a node that does not contain the "
                   "corresponding log state machine.";
            brpc::ClosureGuard done_guard(done);
            // Set finished to false. The RPC sender keep retrying.
            response->set_finished(false);
            return;
        }

        return logger_it->second.CheckMigrationIsFinished(
            request, response, done);
    }

    /*
     * Find the right raft log instance based on log group id and update
     * node term.
     */
    void ReplayLog(::google::protobuf::RpcController *controller,
                   const LogRequest *request,
                   LogResponse *response,
                   ::google::protobuf::Closure *done) override
    {
        uint32_t log_group_id = request->replay_log_request().log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            LOG(ERROR) << "A replay log request toward the log group #"
                       << log_group_id
                       << " is directed to a node that does not contain the "
                          "corresponding log state machine.";
            brpc::ClosureGuard done_guard(done);
            response->set_response_status(
                LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);

            return;
        }

        return logger_it->second.ReplayLog(request, response, done);
    }

    /*
     * Find the right raft log instance based on log group id and update node
     * group's checkpoint timestamp.
     */
    void UpdateCheckpointTs(::google::protobuf::RpcController *controller,
                            const LogRequest *request,
                            LogResponse *response,
                            ::google::protobuf::Closure *done) override
    {
        uint32_t log_group_id =
            request->update_ckpt_ts_request().log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            brpc::ClosureGuard done_guard(done);
            response->set_response_status(
                LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);
            return;
        }

        return logger_it->second.NotifyCheckpointTs(request, response, done);
    }

    void RecoverTx(::google::protobuf::RpcController *controller,
                   const ::txlog::RecoverTxRequest *request,
                   ::txlog::RecoverTxResponse *response,
                   ::google::protobuf::Closure *done) override
    {
        uint32_t log_group_id = request->log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            LOG(ERROR) << "Recover tx request is directed to the wrong node. "
                          "Target log group ID: "
                       << log_group_id;

            brpc::ClosureGuard done_guard(done);
            response->set_tx_status(
                ::txlog::RecoverTxResponse_TxStatus::
                    RecoverTxResponse_TxStatus_RecoverError);
            return;
        }

        logger_it->second.RecoverTx(request, response, done);
    }

    void TransferLeader(::google::protobuf::RpcController *controller,
                        const ::txlog::TransferRequest *request,
                        ::txlog::TransferResponse *response,
                        ::google::protobuf::Closure *done) override
    {
        uint32_t gid = request->lg_id();

        auto logger_it = log_replicas_.find(gid);
        if (logger_it == log_replicas_.end())
        {
            brpc::ClosureGuard done_guard(done);
            response->set_error(true);
            return;
        }

        logger_it->second.TransferLeader(request, response, done);
    }

    void CheckHealth(::google::protobuf::RpcController *controller,
                     const ::txlog::HealthzHttpRequest *request,
                     ::txlog::HealthzHttpResponse *response,
                     ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        std::vector<scoped_refptr<braft::NodeImpl>> nodes;
        braft::NodeManager::GetInstance()->get_all_nodes(&nodes);
        cntl->http_response().set_content_type("application/json");

        butil::IOBufBuilder os;
        Status status = service_status_.load(std::memory_order_acquire);

        switch (status)
        {
        case Status::DETACHED:
            response->set_service_status(
                ::txlog::HealthzHttpResponse_Status::
                    HealthzHttpResponse_Status_DETACHED);
            break;
        case Status::ATTACHED:
            response->set_service_status(
                ::txlog::HealthzHttpResponse_Status::
                    HealthzHttpResponse_Status_ATTACHED);
            break;
        case Status::ATTACHING:
            response->set_service_status(
                ::txlog::HealthzHttpResponse_Status::
                    HealthzHttpResponse_Status_ATTACHING);
            break;
        case Status::DETACHING:
            response->set_service_status(
                ::txlog::HealthzHttpResponse_Status::
                    HealthzHttpResponse_Status_DETACHING);
            break;
        default:
            assert(false);
        };

        if (nodes.empty() && status == Status::ATTACHED)
        {
            cntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            os.move_to(cntl->response_attachment());
            return;
        }

        assert(!nodes.empty() || status != Status::ATTACHED);

        braft::NodeStatus raft_node_status;
        txlog::RaftStat *raft_stat = nullptr;

        // currently, just get the `state` info(which including
        // LEADER/FOLLOWER/CANDIDATE etc.)
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            const braft::NodeId node_id = nodes[i]->node_id();
            raft_stat = response->add_raft_stat();
            raft_stat->set_log_group(node_id.group_id);
            nodes[i]->get_status(&raft_node_status);
            raft_stat->set_state(braft::state2str(raft_node_status.state));
        }

        cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    }

    /*
     * Find the right raft log instance based on log group id and remove node
     * group.
     */
    void RemoveCcNodeGroup(::google::protobuf::RpcController *controller,
                           const LogRequest *request,
                           LogResponse *response,
                           ::google::protobuf::Closure *done) override
    {
        uint32_t log_group_id =
            request->remove_cc_node_group_request().log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            brpc::ClosureGuard done_guard(done);
            response->set_response_status(
                LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);
            return;
        }

        return logger_it->second.RemoveCcNodeGroup(request, response, done);
    }

    void AddPeer(::google::protobuf::RpcController *controller,
                 const AddPeerRequest *request,
                 ChangePeersResponse *response,
                 google::protobuf::Closure *done) override
    {
        uint32_t log_group_id = request->log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            brpc::ClosureGuard done_guard(done);
            response->set_success(false);
            return;
        }

        std::vector<Peer> peers;
        for (int i = 0; i < request->ip_size(); i++)
        {
            Peer peer;
            peer.ip = request->ip(i);
            peer.port = request->port(i);
            peers.push_back(peer);
        }

        return logger_it->second.ChangePeersToAdd(
            peers, log_group_id, response, done);
    }

    void RemovePeer(::google::protobuf::RpcController *controller,
                    const RemovePeerRequest *request,
                    ChangePeersResponse *response,
                    google::protobuf::Closure *done) override
    {
        uint32_t log_group_id = request->log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        if (logger_it == log_replicas_.end())
        {
            brpc::ClosureGuard done_guard(done);
            response->set_success(false);
            return;
        }

        std::vector<Peer> peers;
        for (int i = 0; i < request->ip_size(); i++)
        {
            Peer peer;
            peer.ip = request->ip(i);
            peer.port = request->port(i);
            peers.push_back(peer);
        }
        return logger_it->second.ChangePeersToRemove(
            peers, log_group_id, response, done);
    }

    void GetLogGroupConfig(::google::protobuf::RpcController *controller,
                           const GetLogGroupConfigRequest *request,
                           GetLogGroupConfigResponse *response,
                           ::google::protobuf::Closure *done) override
    {
        uint32_t log_group_id = request->log_group_id();

        auto logger_it = log_replicas_.find(log_group_id);
        // Node is not part of the queried log group
        if (logger_it == log_replicas_.end())
        {
            brpc::ClosureGuard done_guard(done);
            LOG(INFO) << "GetLogGroupConfig request toward the log group #"
                      << log_group_id
                      << " is directed to a node that does not contain the "
                         "corresponding log state machine.";
            response->set_error(true);
            return;
        }

        logger_it->second.GetLogGroupConfig(request, response, done);
    }

    int Start()
    {
        int err_code = 0;
        for (auto &logger_pair : log_replicas_)
        {
            err_code = logger_pair.second.Start();
        }

        return err_code;
    }

    void Join()
    {
        for (auto &logger_pair : log_replicas_)
        {
            logger_pair.second.join();
        }
    }

    void Shutdown()
    {
        for (auto &logger_pair : log_replicas_)
        {
            logger_pair.second.shutdown();
        }
    }

private:
    std::atomic<Status> service_status_{Status::ATTACHED};

    // raft log nodes (no matter leader or followers) which belong to this
    // raft service.
    std::unordered_map<uint32_t, LogInstance> log_replicas_;
};
}  // namespace txlog
