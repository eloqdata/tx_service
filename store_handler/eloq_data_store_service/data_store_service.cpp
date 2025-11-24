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
#include "data_store_service.h"

#include <brpc/closure_guard.h>
#include <brpc/server.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store_fault_inject.h"  // ACTION_FAULT_INJECTOR
#include "internal_request.h"
#include "object_pool.h"
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
#include "rocksdb_cloud_data_store.h"
#include "s3_file_downloader.h"
#endif

namespace EloqDS
{

thread_local ObjectPool<FlushDataRpcRequest> rpc_flush_data_req_pool_;
thread_local ObjectPool<FlushDataLocalRequest> local_flush_data_req_pool_;

thread_local ObjectPool<DeleteRangeRpcRequest> rpc_delete_range_req_pool_;
thread_local ObjectPool<DeleteRangeLocalRequest> local_delete_range_req_pool_;

thread_local ObjectPool<WriteRecordsLocalRequest>
    local_write_records_request_pool_;
thread_local ObjectPool<WriteRecordsRpcRequest> rpc_write_records_request_pool_;

thread_local ObjectPool<ReadLocalRequest> local_read_request_pool_;
thread_local ObjectPool<ReadRpcRequest> rpc_read_request_pool_;

thread_local ObjectPool<CreateTableRpcRequest> rpc_create_table_req_pool_;
thread_local ObjectPool<CreateTableLocalRequest> local_create_table_req_pool_;

thread_local ObjectPool<DropTableRpcRequest> rpc_drop_table_req_pool_;
thread_local ObjectPool<DropTableLocalRequest> local_drop_table_req_pool_;

thread_local ObjectPool<ScanLocalRequest> local_scan_request_pool_;
thread_local ObjectPool<ScanRpcRequest> rpc_scan_request_pool_;

thread_local ObjectPool<CreateSnapshotForBackupRpcRequest>
    rpc_create_snapshot_req_pool_;
thread_local ObjectPool<CreateSnapshotForBackupLocalRequest>
    local_create_snapshot_req_pool_;

thread_local ObjectPool<SyncFileCacheLocalRequest> local_sync_file_cache_req_pool_;

TTLWrapperCache::TTLWrapperCache()
{
    ttl_check_running_ = true;
    ttl_check_worker_ = std::make_unique<ThreadWorkerPool>(1);
    ttl_check_worker_->SubmitWork([this]() { TTLCheckWorker(); });
}

TTLWrapperCache::~TTLWrapperCache()
{
    {
        std::unique_lock<bthread::Mutex> lk(mutex_);
        ttl_check_running_ = false;
        ttl_wrapper_cache_cv_.notify_one();
    }
    ttl_check_worker_->Shutdown();
    Clear();
}

void TTLWrapperCache::TTLCheckWorker()
{
    while (true)
    {
        {
            // Wait with timeout while holding the lock.
            std::unique_lock<bthread::Mutex> lk(mutex_);
            ttl_wrapper_cache_cv_.wait_for(lk, TTL_CHECK_INTERVAL_MS_ * 1000);
            if (!ttl_check_running_)
            {
                break;
            }
        }

        // Grab a snapshot of the keys currently in the scan iterator cache.
        std::vector<std::string> keys;
        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            for (const auto &entry : ttl_wrapper_cache_)
            {
                keys.push_back(entry.first);
            }
        }

        // Get the current time.
        uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

        // Process keys in chunks to avoid holding the lock for too long.
        constexpr size_t chunk_size = 100;
        for (size_t i = 0; i < keys.size(); i += chunk_size)
        {
            size_t end = std::min(i + chunk_size, keys.size());
            {
                std::unique_lock<bthread::Mutex> lk(mutex_);
                for (size_t j = i; j < end; ++j)
                {
                    auto it = ttl_wrapper_cache_.find(keys[j]);
                    if (it != ttl_wrapper_cache_.end())
                    {
                        // Only check iterators that are not in use.
                        if (!it->second->InUse())
                        {
                            auto last_access_time =
                                it->second->GetLastAccessTime();
                            if (now - last_access_time > TTL_CHECK_INTERVAL_MS_)
                            {
                                ttl_wrapper_cache_.erase(it);
                            }
                        }
                    }
                }
            }
            // Yield briefly between chunks.
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

void TTLWrapperCache::Emplace(std::string &session_id,
                              std::unique_ptr<TTLWrapper> iter)
{
    std::unique_lock<bthread::Mutex> lk(mutex_);
    ttl_wrapper_cache_.emplace(session_id, std::move(iter));
}

void TTLWrapperCache::Erase(const std::string &session_id)
{
    std::unique_lock<bthread::Mutex> lk(mutex_);
    ttl_wrapper_cache_.erase(session_id);
}

TTLWrapper *TTLWrapperCache::Borrow(const std::string &session_id)
{
    std::unique_lock<bthread::Mutex> lk(mutex_);
    auto it = ttl_wrapper_cache_.find(session_id);
    if (it != ttl_wrapper_cache_.end())
    {
        it->second->SetInUse(true);
        return it->second.get();
    }
    return nullptr;
}

void TTLWrapperCache::Return(TTLWrapper *iter)
{
    std::unique_lock<bthread::Mutex> lk(mutex_);
    iter->SetInUse(false);
    iter->UpdateLastAccessTime();
}

void TTLWrapperCache::Clear()
{
    std::unique_lock<bthread::Mutex> lk(mutex_);
    auto it = ttl_wrapper_cache_.begin();
    while (it != ttl_wrapper_cache_.end())
    {
        assert(it->second->InUse() == false);
        it = ttl_wrapper_cache_.erase(it);
    }
}

void TTLWrapperCache::ForceEraseIters()
{
    std::unique_lock<bthread::Mutex> lk(mutex_);
    auto it = ttl_wrapper_cache_.begin();
    while (it != ttl_wrapper_cache_.end())
    {
        it = ttl_wrapper_cache_.erase(it);
    }
}

DataStoreService::DataStoreService(
    const DataStoreServiceClusterManager &config,
    const std::string &config_file_path,
    const std::string &migration_log_path,
    std::unique_ptr<DataStoreFactory> &&data_store_factory)
    : cluster_manager_(config),
      config_file_path_(config_file_path),
      migration_log_path_(migration_log_path),
      data_store_factory_(std::move(data_store_factory))
{
    assert(data_store_factory_ != nullptr);
    // Create the directory if it doesn't exist
    std::filesystem::create_directories(migration_log_path_);

    for (size_t i = 0; i < data_shards_.size(); i++)
    {
        data_shards_[i].shard_id_ = i;
        data_shards_[i].shard_status_.store(DSShardStatus::Closed);
    }
}

DataStoreService::~DataStoreService()
{
    if (server_ != nullptr)
    {
        server_->Stop(0);
        server_->Join();
        server_.reset(nullptr);
    }

    migrate_worker_.Shutdown();

    // Stop file cache sync worker
    if (file_cache_sync_worker_ != nullptr)
    {
        {
            std::unique_lock<std::mutex> lk(file_cache_sync_mutex_);
            file_cache_sync_running_ = false;
            file_cache_sync_cv_.notify_one();
        }
        file_cache_sync_worker_->Shutdown();
    }

    // Stop file sync worker
    if (file_sync_worker_ != nullptr)
    {
        file_sync_worker_->Shutdown();
    }

    // shutdown all data_store
    for (auto &it : data_shards_)
    {
        it.ShutDown();
    }
}

bool DataStoreService::StartService(bool create_db_if_missing)
{
    if (server_ != nullptr)
    {
        return true;
    }

    auto dss_shards = cluster_manager_.GetShardsForThisNode();
    LOG(INFO) << "DataStoreService start with shards: " << dss_shards.size();

    if (!dss_shards.empty())
    {
        for (uint32_t shard_id : dss_shards)
        {
            auto &ds_ref = data_shards_[shard_id];
            auto open_mode = cluster_manager_.FetchDSShardStatus(shard_id);
            if (open_mode == DSShardStatus::ReadOnly ||
                open_mode == DSShardStatus::ReadWrite)
            {
                auto expect_status = DSShardStatus::Closed;
                if (ds_ref.shard_status_.compare_exchange_strong(
                        expect_status, DSShardStatus::Starting))
                {
                    ds_ref.data_store_ = data_store_factory_->CreateDataStore(
                        create_db_if_missing, shard_id, this, true);
                    if (ds_ref.data_store_ == nullptr)
                    {
                        LOG(ERROR) << "Failed to create data store on starting "
                                      "DataStoreService, shard id: "
                                   << shard_id;
                        return false;
                    }
                    ds_ref.scan_iter_cache_ =
                        std::make_unique<TTLWrapperCache>();

                    if (open_mode == DSShardStatus::ReadOnly)
                    {
                        ds_ref.data_store_->SwitchToReadOnly();
                    }
                    ds_ref.shard_status_.store(open_mode,
                                               std::memory_order_release);
                    LOG(INFO) << "Created data store on starting "
                                 "DataStoreService, shard id: "
                              << shard_id;
                }
            }
        }
    }

    server_ = std::make_unique<brpc::Server>();
    if (server_->AddService(this, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR) << "Failed to add DataStoreService to server";
        return false;
    }

    brpc::ServerOptions options;
    options.num_threads = 0;
    options.has_builtin_services = true;
    auto this_node = cluster_manager_.GetThisNode();
    if (server_->Start(this_node.port_, &options) != 0)
    {
        LOG(ERROR) << "Failed to start DataStoreService";
        return false;
    }
    LOG(INFO) << "DataStoreService started on port " << this_node.port_;

#ifdef DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3
    // Start file cache sync worker (for primary node to send file cache to standby)
    uint32_t sync_interval_sec =
        cluster_manager_.GetFileCacheSyncIntervalSec();
    file_cache_sync_running_ = true;
    file_cache_sync_worker_ = std::make_unique<ThreadWorkerPool>(1);
    file_cache_sync_worker_->SubmitWork(
        [this, sync_interval_sec]() { FileCacheSyncWorker(sync_interval_sec); });

    // Start file sync worker (for standby node to process incoming sync requests)
    file_sync_worker_ = std::make_unique<ThreadWorkerPool>(1);
    // ThreadWorkerPool manages its own worker threads internally
    // We just need to create it and submit work items to it
#endif

    CheckAndRecoverMigrateTask();

    return true;
}

bool DataStoreService::ConnectAndStartDataStore(uint32_t data_shard_id,
                                                DSShardStatus open_mode,
                                                bool create_db_if_missing)
{
    if (open_mode == DSShardStatus::Closed)
    {
        return true;
    }
    // assert(open_mode == DSShardStatus::ReadOnly);
    auto &shard_ref = data_shards_.at(data_shard_id);

    DSShardStatus expect_status = DSShardStatus::Closed;
    if (!shard_ref.shard_status_.compare_exchange_strong(
            expect_status, DSShardStatus::Starting))
    {
        if (expect_status == open_mode)
        {
            return true;
        }
        while (expect_status == DSShardStatus::Starting)
        {
            bthread_usleep(10000);
            expect_status =
                shard_ref.shard_status_.load(std::memory_order_acquire);
        }
        return expect_status == open_mode;
    }

    // Make sure file sync is not running
    while (shard_ref.is_file_sync_running_.load(std::memory_order_relaxed))
    {
        bthread_usleep(10000);
    }

    DLOG(INFO) << "Connecting and starting data store for shard id:"
               << data_shard_id << ", open_mode:" << static_cast<int>(open_mode)
               << ", create_db_if_missing:" << create_db_if_missing
               << ", data_store_ is null:"
               << (shard_ref.data_store_ == nullptr);
    assert(data_store_factory_ != nullptr);
    if (shard_ref.data_store_ == nullptr)
    {
        shard_ref.data_store_ = data_store_factory_->CreateDataStore(
            create_db_if_missing, data_shard_id, this, true);
        if (shard_ref.data_store_ == nullptr)
        {
            LOG(ERROR) << "Failed to create data store";
            return false;
        }
    }
    else
    {
        bool res = shard_ref.data_store_->Initialize();
        if (!res)
        {
            LOG(ERROR) << "Failed to initialize data store";
            return false;
        }

        res = shard_ref.data_store_->StartDB();
        if (!res)
        {
            LOG(ERROR) << "Failed to start db instance in data store service";
            return false;
        }
    }

    if (shard_ref.scan_iter_cache_ == nullptr)
    {
        shard_ref.scan_iter_cache_ = std::make_unique<TTLWrapperCache>();
    }

    if (open_mode == DSShardStatus::ReadOnly)
    {
        shard_ref.data_store_->SwitchToReadOnly();
        cluster_manager_.SwitchShardToReadOnly(data_shard_id,
                                               DSShardStatus::Closed);
    }
    else
    {
        assert(open_mode == DSShardStatus::ReadWrite);
        cluster_manager_.SwitchShardToReadWrite(data_shard_id,
                                                DSShardStatus::Closed);
    }

    expect_status = DSShardStatus::Starting;
    shard_ref.shard_status_.compare_exchange_strong(
        expect_status, open_mode, std::memory_order_release);
    return true;
}

void DataStoreService::Read(::google::protobuf::RpcController *controller,
                            const ::EloqDS::remote::ReadRequest *request,
                            ::EloqDS::remote::ReadResponse *response,
                            ::google::protobuf::Closure *done)
{
    uint32_t shard_id = request->shard_id();

    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        auto *result = response->mutable_result();
        PrepareShardingError(shard_id, result);
        return;
    }

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadOnly &&
        shard_status != DSShardStatus::ReadWrite)
    {
        brpc::ClosureGuard done_guard(done);
        auto *result = response->mutable_result();
        result->set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result->set_error_msg("KV store not opened yet.");
        return;
    }

    assert(ds_ref.data_store_ != nullptr);
    // decrease read req count when read done
    ReadRpcRequest *req = rpc_read_request_pool_.NextObject();
    req->Reset(this, request, response, done);

    ds_ref.data_store_->Read(req);
}

void DataStoreService::Read(const std::string_view table_name,
                            const int32_t partition_id,
                            const uint32_t shard_id,
                            const std::string_view key,
                            std::string *record,
                            uint64_t *ts,
                            uint64_t *ttl,
                            ::EloqDS::remote::CommonResult *result,
                            ::google::protobuf::Closure *done)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, result);
        return;
    }

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadOnly &&
        shard_status != DSShardStatus::ReadWrite)
    {
        brpc::ClosureGuard done_guard(done);
        record->clear();
        *ts = 0;
        result->set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result->set_error_msg("KV store not opened yet.");
        return;
    }

    assert(ds_ref.data_store_ != nullptr);
    ReadLocalRequest *req = local_read_request_pool_.NextObject();
    req->Reset(this,
               table_name,
               partition_id,
               shard_id,
               key,
               record,
               ts,
               ttl,
               result,
               done);
    ds_ref.data_store_->Read(req);
}

void DataStoreService::FlushData(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::FlushDataRequest *request,
    ::EloqDS::remote::FlushDataResponse *response,
    ::google::protobuf::Closure *done)
{
    uint32_t shard_id = request->shard_id();
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        ::EloqDS::remote::CommonResult *result = response->mutable_result();
        PrepareShardingError(shard_id, result);
        return;
    }

    IncreaseWriteReqCount(shard_id);
    DataShard &ds_ref = data_shards_.at(shard_id);

    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        ::EloqDS::remote::CommonResult *result = response->mutable_result();
        if (shard_status == DSShardStatus::Closed)
        {
            PrepareShardingError(shard_id, result);
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result->set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    FlushDataRpcRequest *req = rpc_flush_data_req_pool_.NextObject();
    req->Reset(this, request, response, done);

    // Process request async.
    ds_ref.data_store_->FlushData(req);
}

void DataStoreService::FlushData(const std::vector<std::string> &kv_table_names,
                                 const uint32_t shard_id,
                                 remote::CommonResult &result,
                                 ::google::protobuf::Closure *done)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, &result);
        return;
    }

    IncreaseWriteReqCount(shard_id);
    DataShard &ds_ref = data_shards_.at(shard_id);

    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        if (shard_status == DSShardStatus::Closed)
        {
            PrepareShardingError(shard_id, &result);
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result.set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    FlushDataLocalRequest *req = local_flush_data_req_pool_.NextObject();
    req->Reset(this, &kv_table_names, shard_id, result, done);

    // Process request async.
    ds_ref.data_store_->FlushData(req);
}

void DataStoreService::DeleteRange(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::DeleteRangeRequest *request,
    ::EloqDS::remote::DeleteRangeResponse *response,
    ::google::protobuf::Closure *done)
{
    uint32_t shard_id = request->shard_id();
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        ::EloqDS::remote::CommonResult *result = response->mutable_result();
        PrepareShardingError(shard_id, result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        // This object helps to call done->Run() in RAII style. If you need to
        // process the request asynchronously, pass done_guard.release().
        brpc::ClosureGuard done_guard(done);
        ::EloqDS::remote::CommonResult *result = response->mutable_result();
        if (shard_status == DSShardStatus::Closed)
        {
            PrepareShardingError(shard_id, result);
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result->set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    DeleteRangeRpcRequest *req = rpc_delete_range_req_pool_.NextObject();
    req->Reset(this, request, response, done);

    // Process request async.
    ds_ref.data_store_->DeleteRange(req);
}

void DataStoreService::DeleteRange(const std::string_view table_name,
                                   const int32_t partition_id,
                                   const uint32_t shard_id,
                                   const std::string_view start_key,
                                   const std::string_view end_key,
                                   const bool skip_wal,
                                   remote::CommonResult &result,
                                   ::google::protobuf::Closure *done)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, &result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        if (shard_status == DSShardStatus::Closed)
        {
            PrepareShardingError(shard_id, &result);
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result.set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    DeleteRangeLocalRequest *req = local_delete_range_req_pool_.NextObject();
    req->Reset(this,
               table_name,
               partition_id,
               shard_id,
               start_key,
               end_key,
               skip_wal,
               result,
               done);

    // Process request async.
    ds_ref.data_store_->DeleteRange(req);
}

void DataStoreService::CreateTable(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::CreateTableRequest *request,
    ::EloqDS::remote::CreateTableResponse *response,
    ::google::protobuf::Closure *done)
{
    uint32_t shard_id = request->shard_id();
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        ::EloqDS::remote::CommonResult *result = response->mutable_result();
        PrepareShardingError(shard_id, result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        ::EloqDS::remote::CommonResult *result = response->mutable_result();
        if (shard_status == DSShardStatus::Closed)
        {
            PrepareShardingError(shard_id, result);
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result->set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    CreateTableRpcRequest *req = rpc_create_table_req_pool_.NextObject();
    req->Reset(this, request, response, done);

    // Process request async.
    ds_ref.data_store_->CreateTable(req);
}

void DataStoreService::CreateTable(const std::string_view table_name,
                                   uint32_t shard_id,
                                   remote::CommonResult &result,
                                   ::google::protobuf::Closure *done)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, &result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        if (shard_status == DSShardStatus::Closed)
        {
            PrepareShardingError(shard_id, &result);
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result.set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    CreateTableLocalRequest *req = local_create_table_req_pool_.NextObject();
    req->Reset(this, table_name, shard_id, result, done);

    // Process request async.
    ds_ref.data_store_->CreateTable(req);
}

void DataStoreService::DropTable(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::DropTableRequest *request,
    ::EloqDS::remote::DropTableResponse *response,
    ::google::protobuf::Closure *done)
{
    uint32_t shard_id = request->shard_id();
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        ::EloqDS::remote::CommonResult *result = response->mutable_result();
        PrepareShardingError(shard_id, result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        ::EloqDS::remote::CommonResult *result = response->mutable_result();
        if (shard_status == DSShardStatus::Closed)
        {
            PrepareShardingError(shard_id, result);
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result->set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    DropTableRpcRequest *req = rpc_drop_table_req_pool_.NextObject();
    req->Reset(this, request, response, done);

    // Process request async.
    ds_ref.data_store_->DropTable(req);
}

void DataStoreService::DropTable(const std::string_view table_name,
                                 uint32_t shard_id,
                                 remote::CommonResult &result,
                                 ::google::protobuf::Closure *done)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, &result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        if (shard_status == DSShardStatus::Closed)
        {
            PrepareShardingError(shard_id, &result);
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result.set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    DropTableLocalRequest *req = local_drop_table_req_pool_.NextObject();
    req->Reset(this, table_name, shard_id, result, done);

    // Process request async.
    ds_ref.data_store_->DropTable(req);
}

void DataStoreService::BatchWriteRecords(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::BatchWriteRecordsRequest *request,
    ::EloqDS::remote::BatchWriteRecordsResponse *response,
    ::google::protobuf::Closure *done)
{
    uint32_t shard_id = request->shard_id();

    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        auto *result = response->mutable_result();
        PrepareShardingError(shard_id, result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        auto *result = response->mutable_result();
        if (shard_status == DSShardStatus::Closed)
        {
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER);
            result->set_error_msg("Requested data not on local node.");
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result->set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    WriteRecordsRpcRequest *batch_write_req =
        rpc_write_records_request_pool_.NextObject();
    batch_write_req->Reset(this, request, response, done);

    ds_ref.data_store_->BatchWriteRecords(batch_write_req);
}

void DataStoreService::ScanNext(
    const std::string_view table_name,
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
    ::google::protobuf::Closure *done)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, result);
        return;
    }

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite &&
        shard_status != DSShardStatus::ReadOnly)
    {
        brpc::ClosureGuard done_guard(done);
        result->set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result->set_error_msg("KV store not opened yet.");
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    ScanLocalRequest *req = local_scan_request_pool_.NextObject();
    req->Reset(this,
               table_name,
               partition_id,
               shard_id,
               start_key,
               end_key,
               inclusive_start,
               inclusive_end,
               scan_forward,
               batch_size,
               search_conditions,
               items,
               session_id,
               generate_session_id,
               result,
               done);

    ds_ref.data_store_->ScanNext(req);
}

void DataStoreService::ScanNext(::google::protobuf::RpcController *controller,
                                const ::EloqDS::remote::ScanRequest *request,
                                ::EloqDS::remote::ScanResponse *response,
                                ::google::protobuf::Closure *done)
{
    uint32_t shard_id = request->shard_id();

    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, response->mutable_result());
        return;
    }

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite &&
        shard_status != DSShardStatus::ReadOnly)
    {
        brpc::ClosureGuard done_guard(done);
        auto *result = response->mutable_result();
        result->set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result->set_error_msg("KV store not opened yet.");
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    ScanRpcRequest *req = rpc_scan_request_pool_.NextObject();
    req->Reset(this, request, response, done);

    ds_ref.data_store_->ScanNext(req);
}

void DataStoreService::ScanClose(::google::protobuf::RpcController *controller,
                                 const ::EloqDS::remote::ScanRequest *request,
                                 ::EloqDS::remote::ScanResponse *response,
                                 ::google::protobuf::Closure *done)
{
    uint32_t shard_id = request->shard_id();

    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, response->mutable_result());
        return;
    }

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite &&
        shard_status != DSShardStatus::ReadOnly)
    {
        assert(false);
        brpc::ClosureGuard done_guard(done);
        auto *result = response->mutable_result();
        result->set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result->set_error_msg("KV store not opened yet.");
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    ScanRpcRequest *req = rpc_scan_request_pool_.NextObject();
    req->Reset(this, request, response, done);

    ds_ref.data_store_->ScanClose(req);
}

void DataStoreService::ScanClose(const std::string_view table_name,
                                 int32_t partition_id,
                                 uint32_t shard_id,
                                 std::string *session_id,
                                 remote::CommonResult *result,
                                 ::google::protobuf::Closure *done)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, result);
        return;
    }

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite &&
        shard_status != DSShardStatus::ReadOnly)
    {
        assert(false);
        brpc::ClosureGuard done_guard(done);
        result->set_error_code(::EloqDS::remote::DataStoreError::DB_NOT_OPEN);
        result->set_error_msg("KV store not opened yet.");
        return;
    }

    assert(ds_ref.data_store_ != nullptr);

    ScanLocalRequest *req = local_scan_request_pool_.NextObject();
    req->Reset(this, table_name, partition_id, shard_id, session_id, false, result, done);

    ds_ref.data_store_->ScanClose(req);
}

void DataStoreService::AppendThisNodeKey(std::stringstream &ss)
{
    cluster_manager_.AppendThisNodeKey(ss);
}

std::string DataStoreService::GenerateSessionId()
{
    std::stringstream ss;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    // make sure the session id is unique across the cluster nodes
    AppendThisNodeKey(ss);
    // make sure the session id is unique in this node
    ss << std::chrono::system_clock::now().time_since_epoch().count() << "-"
       << dis(gen);
    return ss.str();
}

void DataStoreService::EmplaceScanIter(uint32_t shard_id,
                                       std::string &session_id,
                                       std::unique_ptr<TTLWrapper> iter)
{
    DataShard &ds_ref = data_shards_.at(shard_id);
    if (ds_ref.scan_iter_cache_ != nullptr)
    {
        ds_ref.scan_iter_cache_->Emplace(session_id, std::move(iter));
    }
}

TTLWrapper *DataStoreService::BorrowScanIter(uint32_t shard_id,
                                             const std::string &session_id)
{
    DataShard &ds_ref = data_shards_.at(shard_id);
    if (ds_ref.scan_iter_cache_ != nullptr)
    {
        auto *scan_iter_wrapper = ds_ref.scan_iter_cache_->Borrow(session_id);
        return scan_iter_wrapper;
    }
    return nullptr;
}

void DataStoreService::ReturnScanIter(uint32_t shard_id, TTLWrapper *iter)
{
    DataShard &ds_ref = data_shards_.at(shard_id);
    if (ds_ref.scan_iter_cache_ != nullptr)
    {
        ds_ref.scan_iter_cache_->Return(iter);
    }
}

void DataStoreService::EraseScanIter(uint32_t shard_id,
                                     const std::string &session_id)
{
    DataShard &ds_ref = data_shards_.at(shard_id);
    if (ds_ref.scan_iter_cache_ != nullptr)
    {
        ds_ref.scan_iter_cache_->Erase(session_id);
    }
}

void DataStoreService::ForceEraseScanIters(uint32_t shard_id)
{
    DataShard &ds_ref = data_shards_.at(shard_id);
    if (ds_ref.scan_iter_cache_ != nullptr)
    {
        ds_ref.scan_iter_cache_->ForceEraseIters();
    }
}

void DataStoreService::BatchWriteRecords(
    std::string_view table_name,
    int32_t partition_id,
    uint32_t shard_id,
    const std::vector<std::string_view> &key_parts,
    const std::vector<std::string_view> &record_parts,
    const std::vector<uint64_t> &ts,
    const std::vector<uint64_t> &ttl,
    const std::vector<WriteOpType> &op_types,
    bool skip_wal,
    remote::CommonResult &result,
    google::protobuf::Closure *done,
    const uint16_t parts_cnt_per_key,
    const uint16_t parts_cnt_per_record)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, &result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        if (shard_status == DSShardStatus::Closed)
        {
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER);
            result.set_error_msg("Requested data not on local node.");
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result.set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result.set_error_msg("Write to read-only DB.");
        }
        return;
    }

    assert(ds_ref.data_store_ != nullptr);
    WriteRecordsLocalRequest *batch_write_req =
        local_write_records_request_pool_.NextObject();
    batch_write_req->Reset(this,
                           table_name,
                           partition_id,
                           shard_id,
                           key_parts,
                           record_parts,
                           ts,
                           ttl,
                           op_types,
                           skip_wal,
                           result,
                           done,
                           parts_cnt_per_key,
                           parts_cnt_per_record);

    ds_ref.data_store_->BatchWriteRecords(batch_write_req);
}

void DataStoreService::CreateSnapshotForBackup(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::CreateSnapshotForBackupRequest *request,
    ::EloqDS::remote::CreateSnapshotForBackupResponse *response,
    ::google::protobuf::Closure *done)
{
    auto *result = response->mutable_result();
    uint32_t shard_id = request->shard_id();

    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        if (shard_status == DSShardStatus::Closed)
        {
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER);
            result->set_error_msg("Requested data not on local node.");
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result->set_error_msg("Write to read-only DB.");
        }
        return;
    }

    CreateSnapshotForBackupRpcRequest *req =
        rpc_create_snapshot_req_pool_.NextObject();

    req->Reset(this, request, response, done);
    ds_ref.data_store_->CreateSnapshotForBackup(req);
}

void DataStoreService::CreateSnapshotForBackup(
    uint32_t shard_id,
    std::string_view backup_name,
    uint64_t backup_ts,
    std::vector<std::string> *backup_files,
    remote::CommonResult *result,
    ::google::protobuf::Closure *done)
{
    if (!IsOwnerOfShard(shard_id))
    {
        brpc::ClosureGuard done_guard(done);
        PrepareShardingError(shard_id, result);
        return;
    }

    IncreaseWriteReqCount(shard_id);

    DataShard &ds_ref = data_shards_.at(shard_id);
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);
    if (shard_status != DSShardStatus::ReadWrite)
    {
        DecreaseWriteReqCount(shard_id);
        brpc::ClosureGuard done_guard(done);
        if (shard_status == DSShardStatus::Closed)
        {
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER);
            result->set_error_msg("Requested data not on local node.");
        }
        else
        {
            assert(shard_status == DSShardStatus::ReadOnly);
            result->set_error_code(
                ::EloqDS::remote::DataStoreError::WRITE_TO_READ_ONLY_DB);
            result->set_error_msg("Write to read-only DB.");
        }
        return;
    }

    CreateSnapshotForBackupLocalRequest *req =
        local_create_snapshot_req_pool_.NextObject();

    req->Reset(
        this, shard_id, backup_name, backup_ts, backup_files, result, done);

    // Process request async
    ds_ref.data_store_->CreateSnapshotForBackup(req);
}

void DataStoreService::SyncFileCache(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::SyncFileCacheRequest *request,
    ::google::protobuf::Empty *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    // Validate shard ID
    // if (request->shard_id() != shard_id_)
    // {
    //     LOG(WARNING) << "Invalid shard ID in SyncFileCache request: "
    //                 << request->shard_id() << " (expected " << shard_id_ <<
    //                 ")";
    //     // Note: Since response is Empty, we can't return error code.
    //     // Errors are logged and RPC completes successfully.
    //     // The primary node can check logs if needed.
    //     return;
    // }

    // TODO(lzx): validate this node is the follower of the shard.
    auto &ds_ref = data_shards_.at(request->shard_id());
    auto shard_status = ds_ref.shard_status_.load(std::memory_order_acquire);

    // Only process if we're a standby node (closed status)
    if (shard_status != DSShardStatus::Closed)
    {
        LOG(WARNING) << "SyncFileCache called on non-standby node (status: "
                     << static_cast<int>(shard_status) << ")";
        return;
    }
    
    // Submit to file sync worker for async processing (file I/O should not block bthread)
    SyncFileCacheLocalRequest *req = local_sync_file_cache_req_pool_.NextObject();
    req->SetRequest(request, done_guard.release());
    
    if (file_sync_worker_ == nullptr)
    {
        LOG(ERROR) << "FileSyncWorker not initialized, cannot process SyncFileCache";
        req->Free();
        // Note: done_guard was released, so we need to manually call done
        brpc::ClosureGuard done_guard(done);
        return;
    }
    
    bool res = file_sync_worker_->SubmitWork([this, req]() {
        ProcessSyncFileCache(req);
    });
    
    if (!res)
    {
        req->Free();
        LOG(ERROR) << "Failed to submit SyncFileCache work to file sync worker";
        // Note: done_guard was released, so we need to manually call done
        brpc::ClosureGuard done_guard(done);
    }
}

void DataStoreService::ProcessSyncFileCache(SyncFileCacheLocalRequest *req)
{
    std::unique_ptr<PoolableGuard> poolable_guard = std::make_unique<PoolableGuard>(req);
    
    const auto *request = req->GetRequest();

    uint32_t shard_id = request->shard_id();
    auto &ds_ref = data_shards_.at(shard_id);

    if (ds_ref.shard_status_.load(std::memory_order_acquire) !=
        DSShardStatus::Closed)
    {
        LOG(WARNING) << "Shard status is not closed, skipping file sync";
        req->Finish();
        return;
    }
    
    // Get storage path from factory (even though DB is closed, path still exists)
    if (data_store_factory_ == nullptr)
    {
        LOG(ERROR) << "DataStoreFactory is null, cannot process file cache sync";
        req->Finish();
        return;
    }
    
    std::string storage_path = data_store_factory_->GetStoragePath();
    if (storage_path.empty())
    {
        LOG(ERROR) << "Storage path is empty, cannot process file cache sync";
        req->Finish();
        return;
    }
    
    // Construct full storage path with shard ID: {storage_path}/ds_{shard_id}/db/
    std::string db_path = storage_path + "/ds_" + std::to_string(shard_id) + "/db/";

    // Create local db_path if not exists
    if (!std::filesystem::exists(db_path))
    {
        std::filesystem::create_directories(db_path);
    }
    
    // Build file info map from request
    std::map<std::string, ::EloqDS::remote::FileInfo> file_info_map;
    for (const auto &file_info : request->files())
    {
        file_info_map[file_info.file_name()] = file_info;
    }
    
    // Step 1: Decide the list of files to keep based on cache size and file number
    // Prioritize files with lower file numbers (older files are more likely to be needed)
    uint64_t cache_size_limit = GetSstFileCacheSizeLimit();
    std::set<std::string> files_to_keep = DetermineFilesToKeep(
        file_info_map, cache_size_limit);
    
    // Step 2: List local directory and remove SST files that don't belong to keep list
    uint32_t deleted_count = 0;
    std::error_code ec;
    std::filesystem::directory_iterator dir_ite(db_path, ec);
    
    if (ec.value() != 0)
    {
        LOG(ERROR) << "Failed to list local directory: " << ec.message();
        req->Finish();
        return;
    }

    ds_ref.is_file_sync_running_.store(true, std::memory_order_release);
    for (const auto &entry : dir_ite)
    {
        if (ds_ref.shard_status_.load(std::memory_order_acquire) !=
            DSShardStatus::Closed)
        {
            LOG(WARNING) << "Shard status is not closed, skipping file sync";
            ds_ref.is_file_sync_running_.store(false,
                                               std::memory_order_release);
            req->Finish();
            return;
        }
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            // Only process .sst- files (format: {file_number}.sst-{epoch})
            if (filename.find(".sst-") != std::string::npos)
            {
                if (files_to_keep.find(filename) == files_to_keep.end())
                {
                    std::string file_path = db_path + filename;
                    std::error_code del_ec;
                    if (std::filesystem::remove(file_path, del_ec))
                    {
                        deleted_count++;
                        DLOG(INFO) << "Deleted file not in keep list: " << filename;
                    }
                    else
                    {
                        LOG(WARNING) << "Failed to delete file " << filename 
                                    << ": " << del_ec.message();
                    }
                }
            }
        }
    }
    
    // Step 3: Download missing files from S3 that are in the keep list
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    std::unique_ptr<S3FileDownloader> downloader = CreateS3Downloader();
    if (downloader == nullptr)
    {
        LOG(ERROR) << "Failed to create S3 downloader, skipping downloads";
        ds_ref.is_file_sync_running_.store(false, std::memory_order_release);
        req->Finish();
        return;
    }
    
    uint32_t downloaded_count = 0;
    if (!std::filesystem::exists(db_path + "IDENTITY"))
    {
        downloader->DownloadFile("IDENTITY", db_path + "IDENTITY");
    }
    if (!std::filesystem::exists(db_path + "CURRENT"))
    {
        // Create dummy local CURRENT file. This file needs to be in local db dir so that
        // rocksdb cloud will not clean up the db dir when opening db. Rocksdbcloud
        // ignores content of CURRENT file so we can set any content we want.
        std::ofstream current_file(db_path + "CURRENT");
        current_file << "MANIFEST-000001\n";
        current_file.close();
    }


    for (const auto &filename : files_to_keep)
    {
        if (ds_ref.shard_status_.load(std::memory_order_acquire) !=
            DSShardStatus::Closed)
        {
            LOG(WARNING) << "Shard status is not closed, skipping file sync";
            break;
        }
        std::string file_path = db_path + filename;
        
        // Check if file already exists locally
        if (std::filesystem::exists(file_path))
        {
            continue;  // Already have this file
        }
        
        // Download from S3
        auto it = file_info_map.find(filename);
        if (it == file_info_map.end())
        {
            LOG(WARNING) << "File " << filename 
                        << " in keep list but not in file info map";
            continue;
        }
        
        if (downloader->DownloadFile(filename, file_path))
        {
            downloaded_count++;
            DLOG(INFO) << "Downloaded " << filename;
        }
        else
        {
            LOG(ERROR) << "Failed to download " << filename;
        }
    }
#else
    // S3 download not available for non-RocksDB Cloud S3 backends
    uint32_t downloaded_count = 0;
    DLOG(INFO) << "S3 download not available for this data store type";
#endif
    
    DLOG(INFO) << "File cache sync complete: received=" << request->files_size()
              << ", keep_list_size=" << files_to_keep.size()
              << ", deleted=" << deleted_count 
              << ", downloaded=" << downloaded_count;
    
    // Finish the RPC (response is Empty, so just call done)
    req->Finish();
    ds_ref.is_file_sync_running_.store(false, std::memory_order_release);
}

void DataStoreService::FetchDSSClusterConfig(
    ::google::protobuf::RpcController *controller,
    const ::google::protobuf::Empty *request,
    ::EloqDS::remote::FetchDSSClusterConfigResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    response->clear_cluster_config();
    auto *cluster_config = response->mutable_cluster_config();
    auto cluster_manager = cluster_manager_;
    cluster_config->set_sharding_algorithm(
        cluster_manager.GetShardingAlgorithm()->GetName());
    cluster_config->set_topology_version(cluster_manager_.GetTopologyVersion());
    auto tmp_shards = cluster_manager.GetAllShards();
    for (const auto &it : tmp_shards)
    {
        auto *new_shard = cluster_config->add_shards();
        new_shard->set_shard_id(it.second.shard_id_);
        new_shard->set_shard_version(it.second.version_);
        for (const auto &node : it.second.nodes_)
        {
            auto *member_node = new_shard->add_member_nodes();
            member_node->set_host_name(node.host_name_);
            member_node->set_port(node.port_);
            DLOG(INFO) << "FetchDSSClusterConfig, DSSNode: " << node.host_name_
                       << ":" << node.port_;
        }
    }
}

void DataStoreService::UpdateDSSClusterConfig(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::UpdateDSSClusterConfigRequest *request,
    ::EloqDS::remote::UpdateDSSClusterConfigResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    auto &new_config = request->cluster_config();

    const std::string &sharding_algorithm = new_config.sharding_algorithm();
    uint64_t topology_version = new_config.topology_version();
    std::map<uint32_t, DSShard> shards;
    const auto &shard_bufs = new_config.shards();
    for (const auto &shard_buf : shard_bufs)
    {
        int shard_id = shard_buf.shard_id();
        uint64_t version = shard_buf.shard_version();
        auto ins_pair2 = shards.try_emplace(shard_id);
        DSShard &shard = ins_pair2.first->second;
        shard.shard_id_ = shard_id;
        shard.version_ = version;
        shard.nodes_.reserve(shard_buf.member_nodes_size());
        for (const auto &node_buf : shard_buf.member_nodes())
        {
            DSSNode node;
            node.host_name_ = node_buf.host_name();
            node.port_ = node_buf.port();
            shard.nodes_.emplace_back(std::move(node));
        }
    }

    // write to file at 1st
    assert(topology_version == cluster_manager_.GetTopologyVersion() + 1);
    DataStoreServiceClusterManager new_config1 = cluster_manager_;
    new_config1.Update(sharding_algorithm, shards, topology_version);
    new_config1.Save(config_file_path_);

    // update in memory at 2nd
    cluster_manager_.Update(sharding_algorithm, shards, topology_version);

    response->set_error_code(0);
}

void DataStoreService::ShardMigrate(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::ShardMigrateRequest *request,
    ::EloqDS::remote::ShardMigrateResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    std::string event_id = request->event_id();

    {
        std::shared_lock<std::shared_mutex> lk(migrate_task_mux_);
        if (migrate_task_map_.find(event_id) != migrate_task_map_.end())
        {
            response->set_error_code(
                remote::ShardMigrateError::DUPLICATE_REQUEST);
            return;
        }
    }

    int shard_id = request->shard_id();
    const std::string &to_node_host = request->to_node_host();
    uint16_t to_node_port = request->to_node_port();
    uint64_t shard_version;
    if (!cluster_manager_.IsOwnerOfShard(shard_id, &shard_version))
    {
        response->set_error_code(remote::ShardMigrateError::REQUEST_NOT_OWNER);
        return;
    }

    auto res = NewMigrateTask(
        event_id, shard_id, to_node_host, to_node_port, shard_version + 1);

    response->set_error_code(res.first);
    response->set_event_id(res.second);
}

void DataStoreService::ShardMigrateStatus(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::ShardMigrateStatusRequest *request,
    ::EloqDS::remote::ShardMigrateStatusResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    std::string event_id = request->event_id();
    std::shared_lock<std::shared_mutex> lk(migrate_task_mux_);
    auto it = migrate_task_map_.find(event_id);
    if (it != migrate_task_map_.end())
    {
        response->set_finished(it->second.status > 4);
        response->set_status(it->second.status);
        return;
    }
    lk.unlock();

    response->set_finished(true);
    response->set_status(0);
}

void DataStoreService::OpenDSShard(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::OpenDSShardRequest *request,
    ::EloqDS::remote::OpenDSShardResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t shard_id = request->shard_id();
    uint64_t shard_version = request->version();

    assert(request->mode() != remote::DSShardStatus::CLOSED);

    DSShardStatus mode = DSShardStatus::ReadOnly;
    switch (request->mode())
    {
    case ::EloqDS::remote::DSShardStatus::READ_ONLY:
        mode = DSShardStatus::ReadOnly;
        DLOG(INFO) << "OpenDSShard with READ_ONLY mode for shard " << shard_id;
        break;
    case ::EloqDS::remote::DSShardStatus::READ_WRITE:
        mode = DSShardStatus::ReadWrite;
        DLOG(INFO) << "OpenDSShard with READ_WRITE mode for shard " << shard_id;
        break;
    case ::EloqDS::remote::DSShardStatus::CLOSED:
        mode = DSShardStatus::Closed;
        DLOG(INFO) << "OpenDSShard with CLOSED mode for shard " << shard_id;
        break;
    default:
        assert(false);
    }

    // Connect before setting cluster config to avoid visiting data store when
    // it is not ready yet.
    bool res = ConnectAndStartDataStore(shard_id, mode, false);
    if (!res)
    {
        LOG(ERROR) << "Failed to connect and start data store";
        response->set_error_code(
            ::EloqDS::remote::DataStoreError::STATUS_SWITCH_FAILED);
        response->set_error_msg("Failed to connect and start data store");
        return;
    }

    {
        std::vector<DSSNode> members;
        for (const auto &member : request->members())
        {
            members.push_back(DSSNode(member.host_name(), member.port()));
        }
        assert(members.size() > 0);

        auto res = cluster_manager_.UpdateShardMembers(
            shard_id, shard_version, members, &mode, config_file_path_);
        if (!res)
        {
            LOG(ERROR)
                << "UpdateShardMembers failed for version mismatch, shard "
                << shard_id;
            response->set_error_code(1);
            return;
        }
    }

    ACTION_FAULT_INJECTOR("panic_after_target_open_dsshard");

    response->set_error_code(0);
}

void DataStoreService::SwitchDSShardMode(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::SwitchDSShardModeRequest *request,
    ::EloqDS::remote::SwitchDSShardModeResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t shard_id = request->shard_id();
    uint64_t shard_version = request->version();

    assert(request->mode() != remote::DSShardStatus::CLOSED);

    DSShardStatus mode = DSShardStatus::ReadOnly;
    switch (request->mode())
    {
    case ::EloqDS::remote::DSShardStatus::READ_ONLY:
        mode = DSShardStatus::ReadOnly;
        break;
    case ::EloqDS::remote::DSShardStatus::READ_WRITE:
        mode = DSShardStatus::ReadWrite;
        break;
    case ::EloqDS::remote::DSShardStatus::CLOSED:
        mode = DSShardStatus::Closed;
        assert(false);
        LOG(ERROR) << "Should not switch to closed mode, shard " << shard_id;
        response->set_error_code(2);
        return;
    default:
        assert(false);
    }

    bool res = false;

    if (cluster_manager_.FetchDSShardVersion(shard_id) > shard_version)
    {
        LOG(ERROR) << "SwitchDSShardMode failed for version mismatch, shard "
                   << shard_id;
        response->set_error_code(3);
        return;
    }

    if (cluster_manager_.FetchDSShardStatus(shard_id) == mode)
    {
        response->set_error_code(0);
        return;
    }

    auto new_config = cluster_manager_;
    new_config.UpdateDSShardStatus(shard_id, mode);
    new_config.Save(config_file_path_);

    if (mode == DSShardStatus::ReadOnly)
    {
        DLOG(INFO) << "SwitchDSShardMode to read only for shard " << shard_id;
        res = SwitchReadWriteToReadOnly(shard_id);
    }
    else if (mode == DSShardStatus::ReadWrite)
    {
        DLOG(INFO) << "SwitchDSShardMode to read write for shard " << shard_id;
        res = SwitchReadOnlyToReadWrite(shard_id);
    }

    ACTION_FAULT_INJECTOR("panic_after_target_switch_rw");

    // notify client update config
    if (update_config_listener_)
    {
        update_config_listener_(new_config);
    }
    response->set_error_code(res ? 0 : 1);
}

void DataStoreService::UpdateDSShardConfig(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::UpdateDSShardConfigRequest *request,
    ::EloqDS::remote::UpdateDSShardConfigResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t shard_id = request->shard_id();
    uint64_t shard_version = request->version();
    std::vector<DSSNode> members;
    for (const auto &member : request->members())
    {
        LOG(INFO) << "Member Hostname: " << member.host_name()
                  << ", Port: " << member.port();
        members.push_back(DSSNode(member.host_name(), member.port()));
    }

    ACTION_FAULT_INJECTOR("panic_before_update_ds_config");

    bool res = cluster_manager_.UpdateShardMembers(
        shard_id, shard_version, members, nullptr, config_file_path_);
    if (!res)
    {
        LOG(ERROR) << "UpdateShardMembers failed for version mismatch, shard "
                   << shard_id;
        response->set_error_code(1);
        return;
    }
    LOG(INFO) << "UpdateDSShardConfig for shard " << shard_id
              << ", listener: " << (update_config_listener_ != nullptr);
    // notify client update config
    if (update_config_listener_)
    {
        update_config_listener_(cluster_manager_);
    }

    ACTION_FAULT_INJECTOR("panic_after_update_ds_config");

    response->set_error_code(0);
}

void DataStoreService::FaultInjectForTest(
    ::google::protobuf::RpcController *controller,
    const ::EloqDS::remote::FaultInjectRequest *request,
    ::EloqDS::remote::FaultInjectResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    std::string fault_name = request->fault_name();
    std::string fault_paras = request->fault_paras();
    FaultInject::Instance().InjectFault(fault_name, fault_paras);

    response->set_finished(true);
}

//-------DataShard Migrate-------

bool DataStoreService::WriteMigrationLog(uint32_t shard_id,
                                         const std::string &event_id,
                                         const std::string &target_node_ip,
                                         uint16_t target_node_port,
                                         uint32_t migration_status,
                                         uint64_t shard_next_version)
{
    std::string log_file_path = migration_log_path_ + "/DSMigrateStatus";
    std::string temp_file_path = log_file_path + ".tmp";

    {
        std::ofstream temp_file(temp_file_path.c_str(),
                                std::ios::out | std::ios::trunc);
        if (!temp_file.is_open())
        {
            LOG(ERROR) << "Failed to open temporary migration log file: "
                       << temp_file_path;
            return false;
        }

        temp_file << "{\n";
        temp_file << "    shard_id: " << shard_id << "\n";
        temp_file << "    target_node_ip: " << target_node_ip << "\n";
        temp_file << "    target_node_port: " << target_node_port << "\n";
        temp_file << "    event_id: " << event_id << "\n";
        temp_file << "    status: " << migration_status << "\n";
        temp_file << "    shard_next_version: " << shard_next_version << "\n";
        temp_file << "}";

        temp_file.close();

        if (temp_file.fail())
        {
            LOG(ERROR) << "Failed to write temporary migration log file: "
                       << temp_file_path;
            return false;
        }
    }

    if (std::rename(temp_file_path.c_str(), log_file_path.c_str()) != 0)
    {
        LOG(ERROR) << "Failed to rename temporary migration log file to final "
                      "log file: "
                   << log_file_path;
        return false;
    }

    return true;
}

bool DataStoreService::ReadMigrationLog(uint32_t &shard_id,
                                        std::string &event_id,
                                        std::string &target_node_ip,
                                        uint16_t &target_node_port,
                                        uint32_t &migration_status,
                                        uint64_t &shard_next_version)
{
    std::string log_file_path = migration_log_path_ + "/DSMigrateStatus";
    std::ifstream log_file(log_file_path, std::ios::in);
    if (!log_file.is_open())
    {
        LOG(ERROR) << "Failed to open migration log file: " << log_file_path;
        return false;
    }

    std::string line;
    while (std::getline(log_file, line))
    {
        if (line.find("shard_id") != std::string::npos)
        {
            shard_id = std::stoi(line.substr(
                line.find(":") + 1, line.find("}") - line.find(":") - 1));
        }
        else if (line.find("target_node_ip") != std::string::npos)
        {
            target_node_ip = line.substr(line.find(":") + 1,
                                         line.find("}") - line.find(":") - 1);
            target_node_ip.erase(
                remove(target_node_ip.begin(), target_node_ip.end(), ' '),
                target_node_ip.end());
        }
        else if (line.find("target_node_port") != std::string::npos)
        {
            target_node_port = std::stoi(line.substr(
                line.find(":") + 1, line.find("}") - line.find(":") - 1));
        }
        else if (line.find("event_id") != std::string::npos)
        {
            event_id = line.substr(line.find(":") + 1,
                                   line.find("}") - line.find(":") - 1);
            event_id.erase(remove(event_id.begin(), event_id.end(), ' '),
                           event_id.end());
        }
        else if (line.find("status") != std::string::npos)
        {
            migration_status = std::stoi(line.substr(
                line.find(":") + 1, line.find("}") - line.find(":") - 1));
        }
        else if (line.find("shard_next_version") != std::string::npos)
        {
            shard_next_version = std::stoi(line.substr(
                line.find(":") + 1, line.find("}") - line.find(":") - 1));
        }
    }

    log_file.close();

    return true;
}

bool DataStoreService::MigrationLogExists()
{
    std::string log_file_path = migration_log_path_ + "/DSMigrateStatus";
    return std::filesystem::exists(log_file_path);
}

bool DataStoreService::RemoveMigrationLog(const std::string &event_id)
{
    std::string log_file_path = migration_log_path_ + "/DSMigrateStatus";
    if (std::filesystem::exists(log_file_path))
    {
        return std::filesystem::remove(log_file_path);
    }
    return true;
}

bool DataStoreService::FetchConfigFromPeer(
    const std::string &peer_addr, DataStoreServiceClusterManager &config)
{
    // Parse peer address
    size_t colon_pos = peer_addr.find(':');
    if (colon_pos == std::string::npos)
    {
        LOG(ERROR) << "Invalid peer address format: " << peer_addr;
        return false;
    }

    std::string host = peer_addr.substr(0, colon_pos);
    int port = std::stoi(peer_addr.substr(colon_pos + 1));

    // Create channel to peer
    brpc::Channel channel;

    // Retry channel init up to 100 times
    int max_retries = 100;
    const int64_t interval_us = 100 * 1000;  // 100ms

    for (int retry = 0; retry < max_retries; retry++)
    {
        if (channel.Init(host.c_str(), port, nullptr) == 0)
        {
            break;
        }

        if (retry == max_retries - 1)
        {
            LOG(ERROR) << "Failed to init channel to peer " << peer_addr
                       << " after " << max_retries << " retries";
            return false;
        }

        LOG(INFO) << "Failed to init channel to peer " << peer_addr
                  << ", retry " << (retry + 1) << "/" << max_retries;
        bthread_usleep(interval_us);
    }

    // Create stub
    EloqDS::remote::DataStoreRpcService_Stub stub(&channel);

    // Prepare request
    brpc::Controller cntl;
    google::protobuf::Empty request;
    EloqDS::remote::FetchDSSClusterConfigResponse response;

    // Retry RPC call up to 10s
    max_retries = 10;
    for (int retry = 0; retry < max_retries; retry++)
    {
        cntl.Reset();
        cntl.set_timeout_ms(1000);
        stub.FetchDSSClusterConfig(&cntl, &request, &response, nullptr);

        if (!cntl.Failed())
        {
            break;
        }

        if (retry == max_retries - 1)
        {
            LOG(ERROR) << "Failed to fetch config from peer after "
                       << max_retries << " retries: " << cntl.ErrorText();
            return false;
        }

        LOG(INFO) << "Failed to fetch config from peer: " << cntl.ErrorText()
                  << ", retry " << (retry + 1) << "/" << max_retries;
    }

    // Parse response into config
    std::map<uint32_t, DSShard> shards_map;
    for (const auto &shard_buf : response.cluster_config().shards())
    {
        DSShard shard;
        shard.shard_id_ = shard_buf.shard_id();
        shard.version_ = shard_buf.shard_version();
        for (const auto &node : shard_buf.member_nodes())
        {
            shard.nodes_.emplace_back(node.host_name(),
                                      static_cast<uint16_t>(node.port()));
            DLOG(INFO) << "FetchDSSClusterConfig, DSSNode: " << node.host_name()
                       << ":" << node.port();
        }
        shards_map[shard_buf.shard_id()] = shard;
    }
    config.Update(response.cluster_config().sharding_algorithm(),
                  shards_map,
                  response.cluster_config().topology_version());

    return true;
}

void DataStoreService::CloseDataStore(uint32_t shard_id)
{
    auto &ds_ref = data_shards_.at(shard_id);
    if (!IsOwnerOfShard(shard_id))
    {
        LOG(INFO)
            << "CloseDataStore no-op for DSS shard is not owned by this node, "
            << shard_id << ", shard_id_: " << ds_ref.shard_id_
            << ", shard_status_: " << ds_ref.shard_status_.load();
        return;
    }
    if (ds_ref.shard_status_.load() == DSShardStatus::ReadWrite)
    {
        bool res = SwitchReadWriteToReadOnly(shard_id);
        if (!res)
        {
            LOG(ERROR) << "SwitchReadWriteToReadOnly failed for DSS shard "
                       << shard_id << ", shard_id_: " << ds_ref.shard_id_
                       << ", shard_status_: " << ds_ref.shard_status_.load();
        }
    }
    if (ds_ref.shard_status_.load() == DSShardStatus::ReadOnly)
    {
        bool res = SwitchReadOnlyToClosed(shard_id);
        if (!res)
        {
            LOG(ERROR) << "SwitchReadOnlyToClosed failed for DSS shard "
                       << shard_id << ", shard_id_: " << ds_ref.shard_id_
                       << ", shard_status_: " << ds_ref.shard_status_.load();
        }
        else
        {
            LOG(INFO) << "SwitchReadOnlyToClosed success for DSS shard "
                      << shard_id << ", shard_id_: " << ds_ref.shard_id_
                      << ", shard_status_: " << ds_ref.shard_status_.load();
        }
    }
}

void DataStoreService::OpenDataStore(uint32_t shard_id)
{
    auto start_time = std::chrono::steady_clock::now();
    auto &ds_ref = data_shards_.at(shard_id);
    if (ds_ref.shard_status_.load() != DSShardStatus::Closed)
    {
        LOG(INFO) << "OpenDataStore no-op for DSS shard status is not closed, "
                  << shard_id << ", shard_id_: " << ds_ref.shard_id_
                  << ", shard_status_: "
                  << static_cast<int>(ds_ref.shard_status_.load());
        return;
    }
    DSShardStatus open_mode = DSShardStatus::ReadWrite;
    bool create_db_if_missing = false;
    auto res =
        ConnectAndStartDataStore(shard_id, open_mode, create_db_if_missing);
    auto end_time = std::chrono::steady_clock::now();
    auto use_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time - start_time)
                        .count();
    if (!res)
    {
        LOG(ERROR) << "OpenDataStore failed for DSS shard " << shard_id
                   << ", shard_id_: " << ds_ref.shard_id_ << ", shard_status_: "
                   << static_cast<int>(ds_ref.shard_status_.load())
                   << ", use time: " << use_time << " ms";
    }
    else
    {
        LOG(INFO) << "OpenDataStore success for DSS shard " << shard_id
                  << ", shard_id_: " << ds_ref.shard_id_ << ", shard_status_: "
                  << static_cast<int>(ds_ref.shard_status_.load())
                  << ", use time: " << use_time << " ms";
    }
}

std::pair<remote::ShardMigrateError, std::string>
DataStoreService::NewMigrateTask(const std::string &event_id,
                                 int data_shard_id,
                                 std::string target_node_host,
                                 uint16_t target_node_port,
                                 uint64_t shard_next_version)
{
    // write log file and update status to 1
    std::lock_guard<std::shared_mutex> lk(migrate_task_mux_);
    auto ins_pair = migrate_task_map_.try_emplace(event_id);
    if (!ins_pair.second)
    {
        return std::make_pair(remote::ShardMigrateError::DUPLICATE_REQUEST,
                              event_id);
    }
    MigrateLog &log_obj = ins_pair.first->second;
    log_obj.event_id = event_id;
    log_obj.shard_id = data_shard_id;
    log_obj.target_node_host = target_node_host;
    log_obj.target_node_port = target_node_port;
    log_obj.status = 1;
    log_obj.shard_next_version = shard_next_version;
    bool res = MigrationLogExists();
    if (res)
    {
        return std::make_pair(remote::ShardMigrateError::DUPLICATE_REQUEST,
                              event_id);
    }
    else
    {
        res = WriteMigrationLog(log_obj.shard_id,
                                event_id,
                                log_obj.target_node_host,
                                log_obj.target_node_port,
                                log_obj.status,
                                log_obj.shard_next_version);
        assert(res);
        migrate_worker_.SubmitWork(
            [this, event_id, &log_obj]()
            {
                DoMigrate(event_id, &log_obj);
                CleanupOldMigrateLogs();
            });

        return std::make_pair(remote::ShardMigrateError::IN_PROGRESS, event_id);
    }
}

void DataStoreService::CheckAndRecoverMigrateTask()
{
    // check if the log file exists
    if (!MigrationLogExists())
    {
        return;
    }

    // check if the log file is valid
    MigrateLog log_obj;
    if (!ReadMigrationLog(log_obj.shard_id,
                          log_obj.event_id,
                          log_obj.target_node_host,
                          log_obj.target_node_port,
                          log_obj.status,
                          log_obj.shard_next_version))
    {
        return;
    }

    std::lock_guard<std::shared_mutex> lk(migrate_task_mux_);
    auto ins_pair =
        migrate_task_map_.try_emplace(log_obj.event_id, std::move(log_obj));
    if (!ins_pair.second)
    {
        return;
    }

    auto *log_ptr = &(ins_pair.first->second);

    migrate_worker_.SubmitWork([this, log_ptr]()
                               { DoMigrate(log_ptr->event_id, log_ptr); });
}

bool DataStoreService::DoMigrate(const std::string &event_id,
                                 MigrateLog *log_ptr)
{
    LOG(INFO) << "Begin to handle migration event " << event_id << ", shard "
              << log_ptr->shard_id;
    // Ensure data store is connected
    LOG(INFO) << "DoMigrate, checking data store connection for shard "
              << log_ptr->shard_id;
    while (true)
    {
        if (log_ptr->status > 3)
        {
            // At step-3, this node has been removed from ds shard members
            // and the change is also saved to ds config file.
            LOG(INFO) << "Migrate step#" << log_ptr->status
                      << ", skip checking data store connection for shard "
                      << log_ptr->shard_id;
            break;
        }
        auto data_store = GetDataStore(log_ptr->shard_id);
        if (data_store != nullptr)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        LOG(INFO) << "Waiting for data store connection for shard "
                  << log_ptr->shard_id;
    }

    LOG(INFO) << "Connected to data store for shard " << log_ptr->shard_id;

    MigrateLog &log = *log_ptr;

    ACTION_FAULT_INJECTOR("panic_before_ds_migration_1");

    // Switch shard to read-only
    if (log.status == 1)
    {
        LOG(INFO) << "Switching shard " << log.shard_id << " to read-only";
        bool res = SwitchReadWriteToReadOnly(log.shard_id);
        // Only when shard status is not rw, SwitchToReadOnly return false.
        if (!res)
        {
            // abort migration if failed to switch to read-only
            // it could be a backup task is running on the shard
            DSShardStatus shard_status =
                cluster_manager_.FetchDSShardStatus(log.shard_id);
            LOG(ERROR) << "Failed to switch shard " << log.shard_id
                       << " to read-only, status: " << shard_status;
            return false;
        }
        cluster_manager_.Save(config_file_path_);

        log.status = 2;
        WriteMigrationLog(log.shard_id,
                          event_id,
                          log.target_node_host,
                          log.target_node_port,
                          log.status,
                          log.shard_next_version);
    }

    ACTION_FAULT_INJECTOR("panic_before_ds_migration_2");

    DSSNode target_node(log.target_node_host, log.target_node_port);

    // update cluster config file on member nodes
    DSSNode current_node = cluster_manager_.GetThisNode();

    // Notify target node to open DB in read-only mode
    if (log.status == 2)
    {
        LOG(INFO) << "Notifying target node to open DB in read-only mode";
        EloqDS::DSShard new_ds_shard = cluster_manager_.GetShard(log.shard_id);
        new_ds_shard.version_ = log.shard_next_version;
        new_ds_shard.nodes_.clear();
        new_ds_shard.nodes_.push_back(target_node);
        bool res = NotifyTargetNodeOpenDSShard(target_node,
                                               log.shard_id,
                                               remote::DSShardStatus::READ_ONLY,
                                               new_ds_shard);
        // Only when there are more than one leader in different network
        // partitions and this migration task sent to the older leader
        // (version is less than target node), "NotifyTargetNodeOpenDSShard"
        // will be failed for version mismatch. Now, we only peform
        // migration between two nodes, that will not ocurr.
        assert(res);
        if (!res)
        {
            LOG(ERROR) << "Failed to notify target node to open DB in "
                          "read-only mode";
            return false;
        }
        log.status = 3;
        WriteMigrationLog(log.shard_id,
                          event_id,
                          log.target_node_host,
                          log.target_node_port,
                          log.status,
                          log.shard_next_version);
    }

    ACTION_FAULT_INJECTOR("panic_before_ds_migration_3");

    // Notify other nodes to update shard config
    if (log.status == 3)
    {
        LOG(INFO) << "Notifying other nodes to update shard config";
        std::set<DSSNode> nodes;
        nodes.insert(target_node);
        auto all_shards = cluster_manager_.GetAllShards();
        for (const auto &[_, group] : all_shards)
        {
            for (const auto &node : group.nodes_)
            {
                LOG(INFO) << "Node Hostname: " << node.host_name_
                          << ", Port: " << node.port_;
                if (node != current_node)
                {
                    nodes.insert(node);
                }
            }
        }

        // local config update
        cluster_manager_.ReplaceShardMembers(log.shard_id,
                                             {&current_node},
                                             {&target_node},
                                             log.shard_next_version);
        cluster_manager_.Save(config_file_path_);
        if (update_config_listener_)
        {
            update_config_listener_(cluster_manager_);
        }

        bool res = NotifyNodesUpdateDSShardConfig(
            nodes, cluster_manager_.GetShard(log.shard_id));
        // Only when there are more than one leader in different network
        // partitions and this migration task sent to the older leader
        // (version is less than target node), "NotifyTargetNodeOpenDSShard"
        // will be failed for version mismatch. Now, we only peform
        // migration between two nodes, that will not ocurr.
        assert(res);
        if (!res)
        {
            LOG(ERROR) << "Failed to update shard config on all nodes";
            return false;
        }
        log.status = 4;
        WriteMigrationLog(log.shard_id,
                          event_id,
                          log.target_node_host,
                          log.target_node_port,
                          log.status,
                          log.shard_next_version);
    }

    ACTION_FAULT_INJECTOR("panic_before_ds_migration_4");

    // Notify target node to switch to read-write mode
    if (log.status == 4)
    {
        // Notify target node open DB in ReadWrite mode
        LOG(INFO) << "Notifying target node to switch to read-write mode";

        // Must switch to closed before notify target node switch mode
        SwitchReadOnlyToClosed(log.shard_id);

        bool res = NotifyTargetNodeSwitchDSShardMode(
            target_node,
            log.shard_id,
            log.shard_next_version,
            remote::DSShardStatus::READ_WRITE);
        // Only when the mode of target node is not ReadOnly,
        // SwitchDsShardMode will be failed. At step-2, the target node was
        // opened with ReadOnly mode, this step should not fail.
        assert(res);
        if (!res)
        {
            LOG(ERROR) << "Failed to notify target node to switch to "
                          "read-write mode";
            return false;
        }
        LOG(INFO) << "Target node switched to read-write mode successfully";
        log.status = 5;

        // Finalize migration
        LOG(INFO) << "Finalizing migration for shard " << log.shard_id;
        // write config file before remove migration log
        cluster_manager_.UpdateDSShardStatus(log.shard_id,
                                             DSShardStatus::Closed);
        cluster_manager_.Save(config_file_path_);
        // remove migration log
        RemoveMigrationLog(event_id);

        // notify client update config
        if (update_config_listener_)
        {
            update_config_listener_(cluster_manager_);
        }

        LOG(INFO) << "Migration completed for event " << log.event_id
                  << "(migrate shard " << log.shard_id << " to target node "
                  << log.target_node_host << ":" << log.target_node_port
                  << " success).";

        return true;
    }

    LOG(ERROR) << "Unexpected status in migration process";
    return false;
}

bool DataStoreService::NotifyTargetNodeOpenDSShard(
    const DSSNode &target_node,
    uint32_t data_shard_id,
    remote::DSShardStatus mode,
    const DSShard &shard_node_group)
{
    auto channel = cluster_manager_.GetDataStoreServiceChannel(target_node);
    assert(channel != nullptr);

    remote::OpenDSShardRequest req;
    req.set_shard_id(data_shard_id);
    req.set_mode(mode);
    req.set_version(shard_node_group.version_);
    for (const auto &node : shard_node_group.nodes_)
    {
        auto *node_buf = req.add_members();
        node_buf->set_host_name(node.host_name_);
        node_buf->set_port(node.port_);
    }

    remote::OpenDSShardResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    ::EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
    stub.OpenDSShard(&cntl, &req, &resp, nullptr);
    uint32_t retry_cnt = 0;
    while (cntl.Failed())
    {
        retry_cnt++;
        LOG(ERROR) << "Error " << cntl.ErrorCode() << ", " << cntl.ErrorText();
        LOG(INFO) << "Failed to notify target node to open DB , retry...";

        if (cntl.ErrorCode() == brpc::EOVERCROWDED ||
            cntl.ErrorCode() == EAGAIN ||
            cntl.ErrorCode() == brpc::ERPCTIMEDOUT)
        {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::min(2000U, 200U * retry_cnt)));
        }
        else if (cntl.ErrorCode() == EHOSTDOWN)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500U));
        }
        else
        {
            channel =
                cluster_manager_.UpdateDataStoreServiceChannel(target_node);
            if (channel == nullptr)
            {
                // retry to UpdateDataStoreServiceChannel()
                std::this_thread::sleep_for(std::chrono::milliseconds(
                    std::min(2000U, 200U * retry_cnt)));
                LOG(INFO) << "UpdateDataStoreServiceChannel failed, retry.";
                continue;
            }
        }

        resp.Clear();
        cntl.Reset();
        cntl.set_timeout_ms(5000);

        ::EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
        stub.OpenDSShard(&cntl, &req, &resp, nullptr);
    }

    if (resp.error_code() != 0)
    {
        LOG(ERROR) << "Target node failed to open DB in " << mode
                   << " mode for shard " << data_shard_id << ", response error "
                   << resp.error_code();
        return false;
    }
    else
    {
        DLOG(INFO) << "OpenDSShard with " << mode << " mode for shard "
                   << data_shard_id << " success";
        return true;
    }
}

bool DataStoreService::SwitchReadWriteToReadOnly(uint32_t shard_id)
{
    if (!IsOwnerOfShard(shard_id))
    {
        return false;
    }

    auto &ds_ref = data_shards_.at(shard_id);
    DSShardStatus expected = DSShardStatus::ReadWrite;
    if (!ds_ref.shard_status_.compare_exchange_strong(
            expected, DSShardStatus::ReadOnly) &&
        expected != DSShardStatus::ReadOnly)
    {
        DLOG(ERROR) << "SwitchReadWriteToReadOnly failed, shard status is not "
                       "ReadWrite or ReadOnly";
        return false;
    }

    // wait for all write requests to finish
    while (ds_ref.ongoing_write_requests_.load(std::memory_order_acquire) > 0)
    {
        bthread_usleep(1000);
    }
    if (ds_ref.shard_status_.load(std::memory_order_acquire) ==
        DSShardStatus::ReadOnly)
    {
        cluster_manager_.SwitchShardToReadOnly(shard_id, expected);
        ds_ref.data_store_->SwitchToReadOnly();
        return true;
    }
    else
    {
        DLOG(ERROR) << "Switch data store to ReadOnly failed for shard "
                       "status never be ReadOnly";
        return false;
    }
}

bool DataStoreService::SwitchReadOnlyToClosed(uint32_t shard_id)
{
    if (!IsOwnerOfShard(shard_id))
    {
        return false;
    }
    auto &ds_ref = data_shards_.at(shard_id);
    DSShardStatus expected = DSShardStatus::ReadOnly;
    if (!ds_ref.shard_status_.compare_exchange_strong(expected,
                                                      DSShardStatus::Closed) &&
        expected != DSShardStatus::Closed)
    {
        DLOG(ERROR) << "SwitchReadOnlyToClosed failed, shard status is not "
                       "ReadOnly or Closed";
        return false;
    }

    if (expected == DSShardStatus::ReadOnly)
    {
        cluster_manager_.SwitchShardToClosed(shard_id, expected);
        ds_ref.data_store_->Shutdown();
    }
    return true;
}

bool DataStoreService::SwitchReadOnlyToReadWrite(uint32_t shard_id)
{
    if (!IsOwnerOfShard(shard_id))
    {
        DLOG(INFO) << "SwitchReadOnlyToReadWrite failed, shard " << shard_id
                   << " is not owner";
        return false;
    }
    auto &ds_ref = data_shards_.at(shard_id);
    DSShardStatus expected = DSShardStatus::ReadOnly;
    if (!ds_ref.shard_status_.compare_exchange_strong(
            expected, DSShardStatus::ReadWrite) &&
        expected != DSShardStatus::ReadWrite)
    {
        DLOG(ERROR) << "SwitchReadOnlyToReadWrite failed, shard status is not "
                       "ReadOnly or ReadWrite";
        return false;
    }

    ds_ref.data_store_->SwitchToReadWrite();
    cluster_manager_.SwitchShardToReadWrite(shard_id, expected);
    return true;
}

bool DataStoreService::NotifyTargetNodeSwitchDSShardMode(
    const DSSNode &target_node,
    uint32_t data_shard_id,
    uint64_t data_shard_version,
    remote::DSShardStatus mode)
{
    auto channel = cluster_manager_.GetDataStoreServiceChannel(target_node);
    assert(channel != nullptr);

    remote::SwitchDSShardModeRequest req;
    req.set_shard_id(data_shard_id);
    req.set_version(data_shard_version);
    req.set_mode(mode);
    remote::SwitchDSShardModeResponse resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    ::EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
    stub.SwitchDSShardMode(&cntl, &req, &resp, nullptr);

    while (cntl.Failed())
    {
        LOG(ERROR) << "Error " << cntl.ErrorCode() << ", " << cntl.ErrorText();
        LOG(INFO) << "Failed to notify target node to switch mode to RW , "
                     "retry...";

        if (cntl.ErrorCode() == brpc::EOVERCROWDED ||
            cntl.ErrorCode() == EAGAIN ||
            cntl.ErrorCode() == brpc::ERPCTIMEDOUT)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        else if (cntl.ErrorCode() == EHOSTDOWN)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500U));
        }
        else
        {
            channel =
                cluster_manager_.UpdateDataStoreServiceChannel(target_node);
            if (channel == nullptr)
            {
                // retry to UpdateDataStoreServiceChannel()
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                LOG(INFO) << "UpdateDataStoreServiceChannel failed, retry.";
                continue;
            }
        }

        resp.Clear();
        cntl.Reset();
        cntl.set_timeout_ms(5000);
        ::EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
        stub.SwitchDSShardMode(&cntl, &req, &resp, nullptr);
    }

    if (resp.error_code() != 0)
    {
        LOG(ERROR) << "Failed to notify target node switch DB mode to " << mode
                   << " for shard " << data_shard_id << ", response error "
                   << resp.error_code();
        return false;
    }
    else
    {
        return true;
    }
}

bool DataStoreService::NotifyNodesUpdateDSShardConfig(
    const std::set<DSSNode> &nodes, const DSShard &shard_config)
{
    // all rpc share the same request
    ::EloqDS::remote::UpdateDSShardConfigRequest req;
    req.set_shard_id(shard_config.shard_id_);
    req.set_version(shard_config.version_);
    for (const auto &node : shard_config.nodes_)
    {
        auto *member = req.add_members();
        member->set_host_name(node.host_name_);
        member->set_port(node.port_);
    }

    std::list<::EloqDS::remote::UpdateDSShardConfigResponse> resp_list;
    std::list<brpc::Controller> cntl_list;
    std::list<std::shared_ptr<brpc::Channel>> channel_list;
    std::list<const DSSNode *> nodes_list;

    for (const auto &node : nodes)
    {
        auto channel = cluster_manager_.GetDataStoreServiceChannel(node);
        assert(channel != nullptr);
        channel_list.emplace_back(std::move(channel));

        resp_list.emplace_back();
        cntl_list.emplace_back();
        nodes_list.emplace_back(&node);
    }

    while (resp_list.size() > 0)
    {
        auto resp_it = resp_list.begin();
        auto cntl_it = cntl_list.begin();
        auto channel_it = channel_list.begin();
        for (; resp_it != resp_list.end(); resp_it++, cntl_it++, channel_it++)
        {
            auto *resp = &(*resp_it);
            auto *cntl = &(*cntl_it);
            auto *channel = channel_it->get();

            resp->Clear();
            cntl->Reset();
            cntl->set_timeout_ms(5000);
            cntl->set_max_retry(2);
            ::EloqDS::remote::DataStoreRpcService_Stub stub(channel);
            stub.UpdateDSShardConfig(cntl, &req, resp, brpc::DoNothing());
        }

        for (auto &ref : cntl_list)
        {
            // wait all rpc call
            brpc::Join(ref.call_id());
        }

        resp_it = resp_list.begin();
        cntl_it = cntl_list.begin();
        channel_it = channel_list.begin();
        auto node_it = nodes_list.begin();
        for (; resp_it != resp_list.end();
             resp_it++, cntl_it++, channel_it++, node_it++)
        {
            if (cntl_it->Failed())
            {
                LOG(INFO) << "Failed to notify node update config for shard , "
                             "error "
                          << cntl_it->ErrorCode() << ", "
                          << cntl_it->ErrorText();
                // retry
                if (cntl_it->ErrorCode() == brpc::EOVERCROWDED ||
                    cntl_it->ErrorCode() == EAGAIN ||
                    cntl_it->ErrorCode() == brpc::ERPCTIMEDOUT)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                else if (cntl_it->ErrorCode() == EHOSTDOWN)
                {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(500U));
                }
                else
                {
                    *channel_it =
                        cluster_manager_.UpdateDataStoreServiceChannel(
                            *(*node_it));
                    while (*channel_it == nullptr)
                    {
                        // retry to UpdateDataStoreServiceChannel()
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(100));
                        LOG(INFO) << "UpdateDataStoreServiceChannel "
                                     "failed, retry.";
                        *channel_it =
                            cluster_manager_.UpdateDataStoreServiceChannel(
                                *(*node_it));
                    }
                }

                continue;
            }
            else if (resp_it->error_code() != 0)
            {
                // The only reason for the error is version mismatch, which
                // should  not happen if only one migration worker.
                LOG(ERROR) << "Failed to notify node update config for shard "
                           << shard_config.shard_id_
                           << ", response error: " << resp_it->error_code();
                assert(false);
                // retry
                continue;
            }
            else
            {
                resp_it = resp_list.erase(resp_it);
                cntl_it = cntl_list.erase(cntl_it);
                channel_it = channel_list.erase(channel_it);
                node_it = nodes_list.erase(node_it);
            }
        }
    }

    return true;
}

void DataStoreService::CleanupOldMigrateLogs()
{
    auto now = std::chrono::system_clock::now();
    auto one_day_ago = now - std::chrono::hours(24);

    std::lock_guard<std::shared_mutex> lock(migrate_task_mux_);
    for (std::unordered_map<std::string, MigrateLog>::iterator it =
             migrate_task_map_.begin();
         it != migrate_task_map_.end();)
    {
        if (it->second.creation_time < one_day_ago && it->second.status > 4)
        {
            it = migrate_task_map_.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void DataStoreService::FileCacheSyncWorker(uint32_t interval_sec)
{
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    while (true)
    {
        {
            // Wait for interval or stop signal using condition variable
            std::unique_lock<std::mutex> lk(file_cache_sync_mutex_);
            file_cache_sync_cv_.wait_for(
                lk,
                std::chrono::seconds(interval_sec),
                [this] { return !file_cache_sync_running_; });
            
            if (!file_cache_sync_running_)
            {
                break;
            }
        }

        for (uint32_t shard_id = 0; shard_id < data_shards_.size(); ++shard_id)
        {
            auto &ds_ref = data_shards_.at(shard_id);

            // Only sync if we're the primary node
            if (ds_ref.shard_status_.load(std::memory_order_acquire) !=
                DSShardStatus::ReadWrite)
            {
                continue;
            }

            if (ds_ref.data_store_ == nullptr)
            {
                continue;
            }

            // Collect file cache
            std::vector<::EloqDS::remote::FileInfo> file_infos;
            auto *cloud_store =
                dynamic_cast<RocksDBCloudDataStore *>(ds_ref.data_store_.get());
            if (cloud_store == nullptr)
            {
                continue;  // Not a RocksDB Cloud store
            }

            if (!cloud_store->CollectCachedSstFiles(file_infos))
            {
                LOG(WARNING) << "Failed to collect file cache for sync";
                continue;
            }

            // Get standby nodes from cluster manager
            const auto shard = cluster_manager_.GetShard(shard_id);
            const auto &members =
                shard.nodes_;  // Access nodes_ vector directly

            // Send to each standby node
            for (const auto &member : members)
            {
                if (member == cluster_manager_.GetThisNode())
                {
                    continue;  // Skip self
                }

                // Get channel to standby node by node (not by shard id)
                DSSNode standby_node(member.host_name_, member.port_);
                auto channel =
                    cluster_manager_.GetDataStoreServiceChannel(standby_node);
                if (channel == nullptr)
                {
                    LOG(WARNING) << "Failed to get channel to standby node "
                                 << member.host_name_ << ":" << member.port_;
                    continue;
                }

                // Create RPC stub and send
                ::EloqDS::remote::DataStoreRpcService_Stub stub(channel.get());
                ::EloqDS::remote::SyncFileCacheRequest request;
                google::protobuf::Empty response;
                brpc::Controller cntl;

                request.set_shard_id(shard_id);
                for (const auto &file_info : file_infos)
                {
                    *request.add_files() = file_info;
                }

                stub.SyncFileCache(&cntl, &request, &response, nullptr);

                if (cntl.Failed())
                {
                    LOG(WARNING) << "Failed to sync file cache to standby: "
                                 << cntl.ErrorText();
                }
                else
                {
                    DLOG(INFO)
                        << "Synced " << file_infos.size()
                        << " files to standby node " << member.host_name_;
                }
            }
        }
    }
#endif
}

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
std::unique_ptr<S3FileDownloader> DataStoreService::CreateS3Downloader() const
{
    if (data_store_factory_ == nullptr)
    {
        return nullptr;
    }
    
    // Get S3 configuration from factory
    std::string bucket_name = data_store_factory_->GetS3BucketName();
    std::string object_path = data_store_factory_->GetS3ObjectPath();
    std::string region = data_store_factory_->GetS3Region();
    std::string endpoint_url = data_store_factory_->GetS3EndpointUrl();
    
    if (bucket_name.empty())
    {
        LOG(ERROR) << "S3 configuration incomplete, cannot create downloader";
        return nullptr;
    }
    
    // Construct S3 URL: s3://bucket-name/object-path/
    std::string s3_url = "s3://" + bucket_name;
    if (!object_path.empty())
    {
        s3_url += "/" + object_path;
        // Ensure URL ends with '/' if object_path doesn't end with it
        if (object_path.back() != '/')
        {
            s3_url += "/";
        }
    }
    
    // Get AWS credentials from factory
    std::string aws_access_key_id = data_store_factory_->GetAwsAccessKeyId();
    std::string aws_secret_key = data_store_factory_->GetAwsSecretKey();
    
    // Note: If credentials are empty, S3FileDownloader will use default credential provider
    
    return std::make_unique<S3FileDownloader>(
        s3_url, region, aws_access_key_id, aws_secret_key, endpoint_url);
}
#endif

uint64_t DataStoreService::GetSstFileCacheSizeLimit() const
{
    if (data_store_factory_ == nullptr)
    {
        // Return default if factory is not available
        return 20ULL * 1024 * 1024 * 1024;  // Default 20GB
    }
    
    uint64_t cache_size = data_store_factory_->GetSstFileCacheSize();
    if (cache_size == 0)
    {
        // Return default if factory returns 0 (not applicable or not set)
        return 20ULL * 1024 * 1024 * 1024;  // Default 20GB
    }
    
    return cache_size;
}

std::set<std::string> DataStoreService::DetermineFilesToKeep(
    const std::map<std::string, ::EloqDS::remote::FileInfo> &file_info_map,
    uint64_t cache_size_limit) const
{
    std::set<std::string> files_to_keep;
    
    // Sort files by file number (ascending) - lower numbers are older, prioritize keeping these
    std::vector<std::pair<uint64_t, std::string>> files_sorted;
    for (const auto &[filename, file_info] : file_info_map)
    {
        files_sorted.push_back({file_info.file_number(), filename});
    }
    
    std::sort(files_sorted.begin(), files_sorted.end(),
              [](const auto &a, const auto &b) {
                  return a.first < b.first;  // Ascending: lower file numbers first
              });
    
    // Add files to keep list until we reach cache size limit
    uint64_t current_size = 0;
    for (const auto &[file_number, filename] : files_sorted)
    {
        auto it = file_info_map.find(filename);
        if (it == file_info_map.end())
        {
            continue;
        }
        
        uint64_t file_size = it->second.file_size();
        
        // If adding this file would exceed limit, stop (files with higher numbers won't be kept)
        if (current_size + file_size > cache_size_limit)
        {
            break;
        }
        
        files_to_keep.insert(filename);
        current_size += file_size;
    }
    
    DLOG(INFO) << "Determined " << files_to_keep.size() 
              << " files to keep (total size: " << current_size 
              << " bytes, limit: " << cache_size_limit << " bytes)";
    
    return files_to_keep;
}

}  // namespace EloqDS