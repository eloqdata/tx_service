#pragma once

#include <brpc/server.h>

#include <cstdint>
#include <string>
#include <vector>

#include "log_service.h"
#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include "rocksdb_cloud_config.h"
#endif

namespace txlog
{
const auto NUM_VCPU = std::thread::hardware_concurrency();

/*
 * LogServer is the driver of log service. It used to
 * 1. setup route table for braft service.
 * 2. initialize raft log service.
 * 3. register brpc server with raft log service and braft service.
 * 4. start brpc server and braft state machine.
 */
class LogServer
{
public:
    LogServer(uint32_t node_id,
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
              // The high water mark size of the in-memory data log queue
              // determines the threshold at which a snapshot is triggered to
              // purge the log queue. When the size of the log queue reaches
              // this threshold, the system initiates a snapshot operation to
              // remove older log entries and free up memory resources
              const size_t in_mem_data_log_queue_size_high_watermark = 50 *
                                                                       10000,
#else
              const size_t sst_files_size_limit = 500 * 1024 * 1024,
#endif
              const size_t rocksdb_max_write_buffer_number = 8,
              const size_t rocksdb_max_background_jobs = 12,
              const size_t rocksdb_target_file_size_base = 64 * 1024 * 1024,
#endif
              uint32_t snapshot_interval = 600,
              bool enable_request_checkpoint = false,
              uint32_t check_replay_log_size_interval_sec = 10,
              uint64_t notify_checkpointer_threshold_size = 128 * 1024 * 1024);

    ~LogServer()
    {
        brpc_server_.Stop(0);  // legacy parameter. Just pass in 0.
        brpc_server_.Join();

        raft_log_service_.Shutdown();
        raft_log_service_.Join();
    }

    void CloseBraft()
    {
        raft_log_service_.Shutdown();
        brpc_server_.Stop(0);
    }

    int Start(bool enable_brpc_builtin_services = true);

    const std::string &GetStoragePath() const
    {
        return storage_path_;
    }

#if defined(LOG_STATE_TYPE_RKDB_ALL)
    const std::string &GetRocksDBStoragePath() const
    {
        return rocksdb_storage_path_;
    }
#endif

private:
    brpc::Server brpc_server_;
    LogServiceImpl raft_log_service_;

    uint32_t port_;

    std::string storage_path_{""};
#if defined(LOG_STATE_TYPE_RKDB_ALL)
    std::string rocksdb_storage_path_{""};
#endif

    void SetCommandLineOptions();
};
}  // namespace txlog
