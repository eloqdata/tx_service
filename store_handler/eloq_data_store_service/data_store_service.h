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
#pragma once

#include <brpc/channel.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store.h"
#include "data_store_factory.h"
#include "data_store_service_config.h"
#include "data_store_service_util.h"
#include "ds_request.pb.h"
#include "thread_worker_pool.h"

namespace EloqDS
{

class SyncFileCacheLocalRequest;

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
// Must not include "s3_file_downloader.h" in this file, for there is a enum
// type definition in "S3Client.h" conflicted with eloqsql.
class S3FileDownloader;
#endif

enum class WriteOpType
{
    DELETE = 0,
    PUT = 1,
};

/**
 * @brief Wrapper class for any object that needs to be cached with TTL.
 */
class TTLWrapper
{
public:
    TTLWrapper()
    {
        UpdateLastAccessTime();
    }

    virtual ~TTLWrapper() = default;

    // Delete copy operations to avoid accidental copies.
    TTLWrapper(const TTLWrapper &) = delete;
    TTLWrapper &operator=(const TTLWrapper &) = delete;

    // Default move operations (if needed).
    TTLWrapper(TTLWrapper &&) noexcept = default;
    TTLWrapper &operator=(TTLWrapper &&) noexcept = default;

    // Accessors
    uint64_t GetLastAccessTime() const
    {
        return last_access_time_;
    }

    void UpdateLastAccessTime()
    {
        last_access_time_ =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
    }

protected:
    bool InUse() const
    {
        return in_use_;
    }

    void SetInUse(bool in_use)
    {
        in_use_ = in_use;
    }

private:
    bool in_use_{false};
    uint64_t last_access_time_{0};

    friend class TTLWrapperCache;
};

/**
 * @brief Cache for TTLWrapper objects with cache ID as key.
 *        Cached objects are checked for TTL expiration periodically,
 *        and to be removed if expired and not in use.
 */
class TTLWrapperCache
{
public:
    TTLWrapperCache();

    ~TTLWrapperCache();

    /**
     * @brief Start the TTL check worker.
     */
    void TTLCheckWorker();

    /**
     * @brief Emplace a TTLWrapper object with the specified session ID.
     * @param session_id The cache ID.
     * @param iter The TTLWrapper object to be cached.
     */
    void Emplace(std::string &cache_id, std::unique_ptr<TTLWrapper> iter);

    /**
     * @brief Borrow a TTLWrapper object with the specified cache ID.
     *        The borrowed object is marked as in use.
     * @param cache_id The cache ID.
     * @return The borrowed TTLWrapper object.
     */
    TTLWrapper *Borrow(const std::string &cache_id);

    /**
     * @brief Return a TTLWrapper object with the specified cache ID.
     *        The returned object is marked as not in use, and the last access
     * time is updated.
     * @param cache_id The cache ID.
     * @param iter The TTLWrapper object to be returned.
     */
    void Return(TTLWrapper *iter);

    /**
     * @brief Erase a TTLWrapper object with the specified cache ID.
     * @param cache_id The cache ID.
     */
    void Erase(const std::string &cache_id);

    /**
     * @brief Clear the cache
     *       All cached objects are removed.
     *       This function should be called after all cached objects are not in
     * use.
     */
    void Clear();

    /**
     * @brief Force to erase all in use iters
     */
    void ForceEraseIters();

private:
    // Scan iterator TTL check interval in milliseconds
    static const uint64_t TTL_CHECK_INTERVAL_MS_{3000};

    // scan iterator cache
    bthread::Mutex mutex_;
    bthread::ConditionVariable ttl_wrapper_cache_cv_;
    std::unordered_map<std::string, std::unique_ptr<TTLWrapper>>
        ttl_wrapper_cache_;
    // scan iterator TTL check
    bool ttl_check_running_{false};
    std::unique_ptr<ThreadWorkerPool> ttl_check_worker_;
};

class DataStoreService : EloqDS::remote::DataStoreRpcService
{
public:
    DataStoreService(const DataStoreServiceClusterManager &config,
                     const std::string &config_file_path,
                     const std::string &migration_log_path,
                     std::unique_ptr<DataStoreFactory> &&data_store_factory);

    ~DataStoreService();

    bool StartService(bool create_db_if_missing);

    brpc::Server *GetBrpcServer()
    {
        return server_.get();
    }

    /**
     * @brief RPC handler for point read operation
     * @param controller RPC controller
     * @param request Write request
     * @param response Write response
     * @param done Callback function
     */
    void Read(::google::protobuf::RpcController *controller,
              const ::EloqDS::remote::ReadRequest *request,
              ::EloqDS::remote::ReadResponse *response,
              ::google::protobuf::Closure *done) override;

    /**
     * @brief Point read operation
     * @param table_name Table name
     * @param partition_id Partition id
     * @param shard_id Shard id
     * @param key Key
     * @param record Record (output)
     * @param ts Timestamp (output)
     * @param result Result (output)
     * @param done Callback function
     */
    void Read(const std::string_view table_name,
              int32_t partition_id,
              uint32_t shard_id,
              const std::string_view key,
              std::string *record,
              uint64_t *ts,
              uint64_t *ttl,
              ::EloqDS::remote::CommonResult *result,
              ::google::protobuf::Closure *done);

    /**
     * @brief RPC handler for batch write operation
     * @param controller RPC controller
     * @param request Write request
     * @param response Write response
     * @param done Callback function
     */
    void BatchWriteRecords(
        ::google::protobuf::RpcController *controller,
        const ::EloqDS::remote::BatchWriteRecordsRequest *request,
        ::EloqDS::remote::BatchWriteRecordsResponse *response,
        ::google::protobuf::Closure *done) override;

    /**
     * @brief Batch write operation
     * @param table_name Table name
     * @param partition_id Partition id
     * @param shard_id Shard id
     * @param keys Keys
     * @param records Records
     * @param ts Timestamps
     * @param op_types Operation types
     * @param need_flush Need flush
     * @param result Result (output)
     * @param done Callback function
     */
    void BatchWriteRecords(std::string_view table_name,
                           int32_t partition_id,
                           uint32_t shard_id,
                           const std::vector<std::string_view> &key_parts,
                           const std::vector<std::string_view> &record_parts,
                           const std::vector<uint64_t> &ts,
                           const std::vector<uint64_t> &ttl,
                           const std::vector<WriteOpType> &op_types,
                           bool skip_wal,
                           remote::CommonResult &result,
                           ::google::protobuf::Closure *done,
                           const uint16_t key_parts_count,
                           const uint16_t record_parts_count);

    /**
     * @brief RPC handler for flush data operation
     *        Flush data operation guarantees all data in memory is persisted to
     * disk.
     * @param controller RPC controller
     * @param request Checkpoint end request
     * @param response Checkpoint end response
     * @param done Callback function
     */
    void FlushData(::google::protobuf::RpcController *controller,
                   const ::EloqDS::remote::FlushDataRequest *request,
                   ::EloqDS::remote::FlushDataResponse *response,
                   ::google::protobuf::Closure *done) override;

    /**
     * @brief Flush data operation
     * @param table_name Table name
     * @param shard_id Shard id
     * @param result Result (output)
     * @param done Callback function
     */
    void FlushData(const std::vector<std::string> &kv_table_names,
                   const uint32_t shard_id,
                   remote::CommonResult &result,
                   ::google::protobuf::Closure *done);

    /**
     * @brief Delete range of data operation
     * @param shard_id Shard id
     * @param table_name Table name
     * @param result Result (output)
     * @param done Callback function
     */
    void DeleteRange(::google::protobuf::RpcController *controller,
                     const ::EloqDS::remote::DeleteRangeRequest *request,
                     ::EloqDS::remote::DeleteRangeResponse *response,
                     ::google::protobuf::Closure *done) override;

    /**
     * @brief Delete range of data operation
     * @param table_name Table name
     * @param partition_id Partition id
     * @param shard_id Shard id
     * @param start_key Start key,
     *        if empty, delete from the beginning of the table
     * @param end_key End key
     *        if empty, delete to the end of the table
     * @param result Result (output)
     * @param done Callback function
     */
    void DeleteRange(const std::string_view table_name,
                     const int32_t partition_id,
                     uint32_t shard_id,
                     const std::string_view start_key,
                     const std::string_view end_key,
                     const bool skip_wal,
                     remote::CommonResult &result,
                     ::google::protobuf::Closure *done);

    /**
     * @brief RPC handler for create table operation
     * @param controller RPC controller
     * @param request Create table request
     * @param response Create table response
     * @param done Callback function
     */
    void CreateTable(::google::protobuf::RpcController *controller,
                     const ::EloqDS::remote::CreateTableRequest *request,
                     ::EloqDS::remote::CreateTableResponse *response,
                     ::google::protobuf::Closure *done) override;

    /**
     * @brief Create table operation
     * @param table_name Table name
     * @param result Result (output)
     * @param done Callback function
     */
    void CreateTable(const std::string_view table_name,
                     uint32_t shard_id,
                     remote::CommonResult &result,
                     ::google::protobuf::Closure *done);

    /**
     * @brief RPC handler for drop table operation
     * @param controller RPC controller
     * @param request Drop table request
     * @param response Drop table response
     * @param done Callback function
     */
    void DropTable(::google::protobuf::RpcController *controller,
                   const ::EloqDS::remote::DropTableRequest *request,
                   ::EloqDS::remote::DropTableResponse *response,
                   ::google::protobuf::Closure *done) override;

    /**
     * @brief Drop table operation
     * @param table_name Table name
     * @param result Result (output)
     * @param done Callback function
     */
    void DropTable(const std::string_view table_name,
                   uint32_t shard_id,
                   remote::CommonResult &result,
                   ::google::protobuf::Closure *done);

    /**
     * @brief RPC handler for scan next operation
     * @param controller RPC controller
     * @param request Scan request
     * @param response Scan response
     * @param done Callback function
     */
    void ScanNext(::google::protobuf::RpcController *controller,
                  const ::EloqDS::remote::ScanRequest *request,
                  ::EloqDS::remote::ScanResponse *response,
                  ::google::protobuf::Closure *done) override;

    /**
     * @brief Scan next operation
     * @param table_name Table name
     * @param partition_id Partition id
     * @param shard_id Shard id
     * @param start_key Start key
     * @param end_key End key
     * @param inclusive_start Inclusive start
     * @param scan_forward Scan forward
     * @param batch_size Batch size
     * @param search_conditions Search conditions
     * @param items Items (output)
     * @param session_id Session ID (output)
     * @param result Result (output)
     * @param done Callback function
     */
    void ScanNext(const std::string_view table_name,
                  int32_t partition_id,
                  uint32_t shard_id,
                  const std::string_view start_key,
                  const std::string_view end_key,
                  bool inclusive_start,
                  bool inclusive_end,
                  bool scan_forward,
                  uint32_t batch_size,
                  const std::vector<remote::SearchCondition> *search_conditions,
                  std::vector<ScanTuple> *items,
                  std::string *session_id,
                  bool generate_session_id,
                  ::EloqDS::remote::CommonResult *result,
                  ::google::protobuf::Closure *done);

    /**
     * @brief RPC handler for scan close operation
     * @param controller RPC controller
     * @param request Scan request
     * @param response Scan response
     * @param done Callback function
     */
    void ScanClose(::google::protobuf::RpcController *controller,
                   const ::EloqDS::remote::ScanRequest *request,
                   ::EloqDS::remote::ScanResponse *response,
                   ::google::protobuf::Closure *done) override;

    /**
     * @brief Scan close operation
     * @param table_name Table name
     * @param partition_id Partition id
     * @param shard_id Shard id
     * @param session_id Session ID
     * @param result Result (output)
     * @param done Callback function
     */
    void ScanClose(const std::string_view table_name,
                   int32_t partition_id,
                   uint32_t shard_id,
                   std::string *session_id,
                   ::EloqDS::remote::CommonResult *result,
                   ::google::protobuf::Closure *done);

    /**
     * @brief RPC handler for create snapshot for backup operation
     * @param controller RPC controller
     * @param request Create snapshot for backup request
     * @param response Create snapshot for backup response
     * @param done Callback function
     */
    void CreateSnapshotForBackup(
        ::google::protobuf::RpcController *controller,
        const ::EloqDS::remote::CreateSnapshotForBackupRequest *request,
        ::EloqDS::remote::CreateSnapshotForBackupResponse *response,
        ::google::protobuf::Closure *done) override;

    /**
     * @brief RPC handler for file cache synchronization (generic for any
     * storage backend)
     * @param controller RPC controller
     * @param request File cache sync request
     * @param response Empty response (google.protobuf.Empty)
     * @param done Callback function
     */
    void SyncFileCache(::google::protobuf::RpcController *controller,
                       const ::EloqDS::remote::SyncFileCacheRequest *request,
                       ::google::protobuf::Empty *response,
                       ::google::protobuf::Closure *done) override;

    /**
     * @brief Create snapshot for backup operation
     * @param result Result (output)
     * @param backup_files Backup files (output)
     * @param backup_ts Backup timestamp
     * @param done Callback function
     */
    void CreateSnapshotForBackup(uint32_t shard_id,
                                 std::string_view backup_name,
                                 uint64_t backup_ts,
                                 std::vector<std::string> *backup_files,
                                 remote::CommonResult *result,
                                 ::google::protobuf::Closure *done);

    /**
     * @brief Append the key string of this node to the specified string stream.
     */
    void AppendThisNodeKey(std::stringstream &ss);

    /**
     * @brief Generate a session id for scan operation
     * @return Session id
     */
    std::string GenerateSessionId();

    /**
     * @brief Emplace scan iterator into scan iter cache
     * @param iter Scan iterator
     * @return Session id
     */
    void EmplaceScanIter(uint32_t shard_id,
                         std::string &session_id,
                         std::unique_ptr<TTLWrapper> iter);

    /**
     * @brief Find and mark scan iterator in use
     * @param session_id Session id
     * @return Scan iterator wrapper
     */
    TTLWrapper *BorrowScanIter(uint32_t shard_id,
                               const std::string &session_id);

    /**
     * @brief Return scan iterator to scan iter cache
     * @param iter Scan iterator wrapper
     */
    void ReturnScanIter(uint32_t shard_id, TTLWrapper *iter);

    /**
     * @brief Erase scan iterator from scan iter cache
     * @param session_id Session id
     */
    void EraseScanIter(uint32_t shard_id, const std::string &session_id);

    /**
     * @brief Force to erase all remained scan iterator from scan iter cache by
     * shard id
     * @param shard_id Shard id
     */
    void ForceEraseScanIters(uint32_t shard_id);

    /**
     * @brief Preapre sharding error
     *        Fill the error code and the topology change in the result
     */
    void PrepareShardingError(uint32_t shard_id,
                              ::EloqDS::remote::CommonResult *result)
    {
        cluster_manager_.PrepareShardingError(shard_id, result);
    }

    void FetchDSSClusterConfig(
        ::google::protobuf::RpcController *controller,
        const ::google::protobuf::Empty *request,
        ::EloqDS::remote::FetchDSSClusterConfigResponse *response,
        ::google::protobuf::Closure *done) override;

    void UpdateDSSClusterConfig(
        ::google::protobuf::RpcController *controller,
        const ::EloqDS::remote::UpdateDSSClusterConfigRequest *request,
        ::EloqDS::remote::UpdateDSSClusterConfigResponse *response,
        ::google::protobuf::Closure *done) override;

    void ShardMigrate(::google::protobuf::RpcController *controller,
                      const ::EloqDS::remote::ShardMigrateRequest *request,
                      ::EloqDS::remote::ShardMigrateResponse *response,
                      ::google::protobuf::Closure *done) override;

    void ShardMigrateStatus(
        ::google::protobuf::RpcController *controller,
        const ::EloqDS::remote::ShardMigrateStatusRequest *request,
        ::EloqDS::remote::ShardMigrateStatusResponse *response,
        ::google::protobuf::Closure *done) override;

    void OpenDSShard(::google::protobuf::RpcController *controller,
                     const ::EloqDS::remote::OpenDSShardRequest *request,
                     ::EloqDS::remote::OpenDSShardResponse *response,
                     ::google::protobuf::Closure *done) override;

    void SwitchDSShardMode(
        ::google::protobuf::RpcController *controller,
        const ::EloqDS::remote::SwitchDSShardModeRequest *request,
        ::EloqDS::remote::SwitchDSShardModeResponse *response,
        ::google::protobuf::Closure *done) override;

    void UpdateDSShardConfig(
        ::google::protobuf::RpcController *controller,
        const ::EloqDS::remote::UpdateDSShardConfigRequest *request,
        ::EloqDS::remote::UpdateDSShardConfigResponse *response,
        ::google::protobuf::Closure *done) override;

    void FaultInjectForTest(::google::protobuf::RpcController *controller,
                            const ::EloqDS::remote::FaultInjectRequest *request,
                            ::EloqDS::remote::FaultInjectResponse *response,
                            ::google::protobuf::Closure *done) override;

    static bool FetchConfigFromPeer(const std::string &peer_addr,
                                    DataStoreServiceClusterManager &config);

    // =======================================================================
    // Group: Internal function for shard related operations
    // =======================================================================
    DSShardStatus FetchDSShardStatus(uint32_t shard_id)
    {
        if (data_shards_.at(shard_id).shard_id_ == shard_id)
        {
            return data_shards_.at(shard_id).shard_status_.load(
                std::memory_order_acquire);
        }
        return DSShardStatus::Closed;
    }

    void AddListenerForUpdateConfig(
        std::function<void(const DataStoreServiceClusterManager &)> listener)
    {
        update_config_listener_ = listener;
    }

    const DataStoreFactory *GetDataStoreFactory() const
    {
        return data_store_factory_.get();
    }

    void IncreaseWriteReqCount(uint32_t shard_id)
    {
        data_shards_.at(shard_id).ongoing_write_requests_.fetch_add(
            1, std::memory_order_release);
    }

    void DecreaseWriteReqCount(uint32_t shard_id)
    {
        data_shards_.at(shard_id).ongoing_write_requests_.fetch_sub(
            1, std::memory_order_release);
    }

    bool IsOwnerOfShard(uint32_t shard_id) const
    {
        const auto &ds_ref = data_shards_.at(shard_id);
        return ds_ref.shard_status_.load(std::memory_order_acquire) !=
               DSShardStatus::Closed;
    }

    void CloseDataStore(uint32_t shard_id);
    void OpenDataStore(uint32_t shard_id);

    DataStoreServiceClusterManager &GetClusterManager()
    {
        return cluster_manager_;
    }

private:
    DataStore *GetDataStore(uint32_t shard_id)
    {
        if (data_shards_.at(shard_id).shard_id_ == shard_id)
        {
            return data_shards_.at(shard_id).data_store_.get();
        }
        else
        {
            return nullptr;
        }
    }

    bool ConnectAndStartDataStore(uint32_t data_shard_id,
                                  DSShardStatus open_mode,
                                  bool create_db_if_missing = false);

    bool SwitchReadWriteToReadOnly(uint32_t shard_id);
    bool SwitchReadOnlyToClosed(uint32_t shard_id);
    bool SwitchReadOnlyToReadWrite(uint32_t shard_id);

    bool WriteMigrationLog(uint32_t shard_id,
                           const std::string &event_id,
                           const std::string &target_node_ip,
                           uint16_t target_node_port,
                           uint32_t migration_status,
                           uint64_t shard_version);

    bool ReadMigrationLog(uint32_t &shard_id,
                          std::string &event_id,
                          std::string &target_node_ip,
                          uint16_t &target_node_port,
                          uint32_t &migration_status,
                          uint64_t &shard_next_version);

    std::unique_ptr<brpc::Server> server_;

    DataStoreServiceClusterManager cluster_manager_;
    std::string config_file_path_;
    std::string migration_log_path_;

    /**
     * @brief Per-shard data structure encapsulating all shard-specific state.
     * Each DataShard manages its own data store, status, and scan cache.
     * Thread-safety: shard_status_ and ongoing_write_requests_ are atomic.
     * data_store_ and scan_iter_cache_ access is protected by shard_status_
     * state machine.
     */
    struct DataShard
    {
        void ShutDown()
        {
            if (data_store_ != nullptr)
            {
                data_store_->Shutdown();
                // Don't set data_store_ to nullptr here, as it may be used
                // in read operations because we don't use read counter.
            }

            if (scan_iter_cache_ != nullptr)
            {
                scan_iter_cache_->Clear();
                scan_iter_cache_ = nullptr;
            }
        }

        uint32_t shard_id_{UINT32_MAX};
        std::unique_ptr<DataStore> data_store_{nullptr};
        std::atomic<DSShardStatus> shard_status_{DSShardStatus::Closed};
        std::atomic<uint64_t> ongoing_write_requests_{0};
        std::unique_ptr<TTLWrapperCache> scan_iter_cache_{nullptr};

        // Whether the file cache sync is running. Used to avoid concurrent
        // local ssd file operations between db and file sync worker.
        std::atomic<bool> is_file_sync_running_{false};
    };

    std::array<DataShard, 1000> data_shards_;

    std::unique_ptr<DataStoreFactory> data_store_factory_;

    // Now, only for update client's config
    std::function<void(DataStoreServiceClusterManager &)>
        update_config_listener_;

private:
    struct MigrateLog
    {
        std::string event_id;
        uint32_t shard_id;
        std::string target_node_host;
        uint16_t target_node_port;
        uint32_t status{0};
        uint64_t shard_next_version{0};
        std::chrono::system_clock::time_point creation_time;

        MigrateLog() : creation_time(std::chrono::system_clock::now())
        {
        }

        MigrateLog(std::string event_id,
                   uint32_t shard_id,
                   std::string target_node_host,
                   uint16_t target_node_port,
                   uint32_t status,
                   uint64_t shard_next_version)
            : event_id(event_id),
              shard_id(shard_id),
              target_node_host(target_node_host),
              target_node_port(target_node_port),
              status(status),
              shard_next_version(shard_next_version),
              creation_time(std::chrono::system_clock::now())
        {
        }
    };

    void CleanupOldMigrateLogs();

    std::pair<remote::ShardMigrateError, std::string> NewMigrateTask(
        const std::string &event_id,
        int data_shard_id,
        std::string target_node_host,
        uint16_t target_node_port,
        uint64_t shard_next_version);
    void CheckAndRecoverMigrateTask();
    std::string GetMigrateLogPath(const std::string &event_id);
    bool MigrationLogExists();
    bool RemoveMigrationLog(const std::string &event_id);

    bool DoMigrate(const std::string &event_id, MigrateLog *log);
    bool NotifyTargetNodeOpenDSShard(const DSSNode &target_node,
                                     uint32_t data_shard_id,
                                     remote::DSShardStatus mode,
                                     const EloqDS::DSShard &shard_config);
    bool NotifyTargetNodeSwitchDSShardMode(const DSSNode &target_node,
                                           uint32_t data_shard_id,
                                           uint64_t data_shard_version,
                                           remote::DSShardStatus mode);
    bool NotifyNodesUpdateDSShardConfig(const std::set<DSSNode> &nodes,
                                        const DSShard &shard_config);

    ThreadWorkerPool migrate_worker_{1};

    // map{event_id->migrate_log}
    std::shared_mutex migrate_task_mux_;
    std::unordered_map<std::string, MigrateLog> migrate_task_map_;

    /**
     * @brief Worker thread for periodic file cache sync to standby nodes
     * @param interval_sec Sync interval in seconds
     */
    void FileCacheSyncWorker(uint32_t interval_sec);

    /**
     * @brief Process file cache sync request (called by file_sync_worker_)
     * @param req Local request containing the sync request
     */
    void ProcessSyncFileCache(SyncFileCacheLocalRequest *req);

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    /**
     * @brief Create S3 downloader instance
     * @return Unique pointer to S3FileDownloader, or nullptr on failure
     */
    std::unique_ptr<S3FileDownloader> CreateS3Downloader(uint32_t shard_id) const;
#endif

    /**
     * @brief Get SST file cache size limit from config
     * @return Cache size limit in bytes
     */
    uint64_t GetSstFileCacheSizeLimit() const;

    /**
     * @brief Determine which files to keep based on cache size limit and file
     * number Files with lower file numbers are prioritized (older files are
     * kept first) Files with higher file numbers are excluded if cache size
     * limit is exceeded
     * @param file_info_map Map of all available files from primary node
     * @param cache_size_limit Maximum cache size in bytes
     * @return Set of file names that should be kept on local disk
     */
    std::set<std::string> DetermineFilesToKeep(
        const std::map<std::string, ::EloqDS::remote::FileInfo> &file_info_map,
        uint64_t cache_size_limit) const;

    std::unique_ptr<ThreadWorkerPool> file_cache_sync_worker_;
    std::unique_ptr<ThreadWorkerPool> file_sync_worker_;

    // File cache sync worker synchronization
    std::mutex file_cache_sync_mutex_;
    std::condition_variable file_cache_sync_cv_;
    bool file_cache_sync_running_{false};
};

}  // namespace EloqDS
