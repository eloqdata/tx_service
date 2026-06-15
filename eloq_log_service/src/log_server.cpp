#include "log_server.h"

#include <braft/route_table.h>
#include <gflags/gflags.h>
#include <unistd.h>

#include <cstdint>
#include <string>
#include <vector>

#include "log_service.h"

// gflags 2.1.1 missing GFLAGS_NAMESPACE. This is a workaround to handle gflags
// ABI issue.
#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#else
#ifndef GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = google;
#endif
#endif

namespace bthread
{
DECLARE_int32(bthread_concurrency);
}

namespace txlog
{
/*
 * LogServer is the driver of log service.
 *
 * Parameter:
 * node_id: the current log node id.
 * port: port of current log node, which is used to start log group rpc service.
 * ip_list: ip list of all the log node in the cluster.
 * port_list: port list of all the log node in the cluster.
 * storage_path: the location to store the raft file.
 *
 * TODO: The architecture of log service is decoupled with other components
 * including runtime, txservice (cc node) and storage engine. Hence log service
 * could have its own configuration in future.
 */
LogServer::LogServer(uint32_t node_id,
                     uint16_t port,
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
                     RocksDBCloudConfig rocksdb_cloud_config,
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
    : port_(port),
      storage_path_(storage_path)
#if defined(LOG_STATE_TYPE_RKDB_ALL)
      ,
      rocksdb_storage_path_(rocksdb_storage_path)
#endif
{
    if (log_group_replica_num < 1)
    {
        LOG(ERROR) << "Invalid log_group_replica_num: " << log_group_replica_num
                   << ", should be at least 1";
        std::abort();
    }

    raft_log_service_.AddLogReplicas(node_id,
                                     ip_list,
                                     port_list,
                                     storage_path,
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

int LogServer::Start(bool enable_brpc_builtin_services)
{
    SetCommandLineOptions();

    // Add log service into RPC server. SERVER_DOESNT_OWN_SERVICE means
    // brpc_server_ doesn't manage the lifecycle of log_service_. They are both
    // handled by LogServer.
    // Add your service into RPC server
    // and specify a URL for `CheckHealth` method in the service
    if (brpc_server_.AddService(&raft_log_service_,
                                brpc::SERVER_DOESNT_OWN_SERVICE,
                                "/healthz => CheckHealth") != 0)
    {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&brpc_server_, port_) != 0)
    {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // put raft log service start() before the brpc server start() to prevent
    // request incoming before raft log service is ready
    if (raft_log_service_.Start() != 0)
    {
        LOG(ERROR) << "Fail to start raft service";
        return -1;
    }

    // It's recommended to start the server before Counter is started to
    // avoid the case that it becomes the leader while the service is
    // unreachable by clients. Notice the default options of server is used
    // here. Check out details from the doc of brpc if you would like change
    // some options;

    brpc::ServerOptions options;
    // use all cpu cores if flag bthread_concurrency is not set
    options.num_threads = bthread::FLAGS_bthread_concurrency;
    options.has_builtin_services = enable_brpc_builtin_services;
    if (IsDefaultGFlag("bthread_concurrency") && NUM_VCPU)
        options.num_threads = NUM_VCPU + 1;

    if (brpc_server_.Start(port_, &options) != 0)
    {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    LOG(INFO) << "Raft Service is running on " << brpc_server_.listen_address();

    return 0;
}

void LogServer::SetCommandLineOptions()
{
    // enable raft leader lease
    GFLAGS_NAMESPACE::SetCommandLineOption("raft_enable_leader_lease", "true");

    // set brpc circuit_breaker max isolation duration smaller than election
    // timeout so that restarted node will join raft group before trying to
    // start a new vote
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "circuit_breaker_max_isolation_duration_ms", "4500");
}
}  // namespace txlog
