/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify it
 *    under either GNU Affero General Public License v3 or GNU General Public
 *    License v2.
 */

#include <gflags/gflags.h>
#include <unistd.h>

#include <catch2/catch_all.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
#include <aws/core/Aws.h>
#endif

#include "INIReader.h"
#include "data_store_service_client.h"
#include "data_sync_task.h"
#include "eloq_basic_catalog_factory.h"
#include "eloq_data_store_service/data_store_service.h"
#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
#include "eloq_data_store_service/eloq_store_config.h"
#include "eloq_data_store_service/eloq_store_data_store_factory.h"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
#include "eloq_data_store_service/rocksdb_cloud_data_store_factory.h"
#include "eloq_data_store_service/rocksdb_config.h"
#endif
#include "eloq_string_key_record.h"
#include "harness/port_util.h"

using namespace txservice;

namespace
{
constexpr std::string_view kRunRealStoreEnv = "ELOQ_RUN_REAL_STORE_TEST";
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
constexpr std::string_view kObjectStoreUrlEnv = "ELOQ_TEST_OBJECT_STORE_URL";

std::string RequireEnvironment(std::string_view name)
{
    const char *value = std::getenv(std::string(name).c_str());
    if (value == nullptr || *value == '\0')
    {
        throw std::runtime_error("missing required environment variable " +
                                 std::string(name));
    }
    return value;
}
#endif

std::shared_ptr<DataSyncTask> MakeTask(const TableName &table_name,
                                       int64_t term)
{
    return std::make_shared<DataSyncTask>(table_name,
                                          /*id=*/0,
                                          /*range_version=*/0,
                                          /*ng_id=*/1,
                                          term,
                                          /*data_sync_ts=*/100,
                                          /*status=*/nullptr,
                                          /*is_dirty=*/false,
                                          /*need_adjust_ts=*/false,
                                          /*hres=*/nullptr);
}

std::unique_ptr<FlushTaskEntry> MakeFlushEntry(
    std::shared_ptr<DataSyncTask> task,
    std::unique_ptr<std::vector<FlushRecord>> records)
{
    return std::make_unique<FlushTaskEntry>(
        std::move(records),
        std::make_unique<std::vector<FlushRecord>>(),
        std::make_unique<std::vector<std::pair<TxKey, int32_t>>>(),
        /*data_sync_txm=*/nullptr,
        std::move(task),
        /*table_schema=*/nullptr,
        /*size=*/0);
}

FlushRecord MakeObjectRecord(int key,
                             RecordStatus status,
                             uint64_t commit_ts,
                             int32_t partition_id)
{
    std::shared_ptr<TxRecord> payload;
    if (status == RecordStatus::Normal)
    {
        auto record = std::make_shared<BlobTxRecord>();
        record->value_ = "value-" + std::to_string(key);
        record->ttl_ = UINT64_MAX;
        payload = std::move(record);
    }
    return FlushRecord(
        TxKey(std::make_unique<EloqStringKey>(std::to_string(key))),
        std::move(payload),
        status,
        commit_ts,
        /*cce=*/nullptr,
        /*post_flush_size=*/0,
        partition_id);
}

FlushRecord MakeSerializedRecord(int key,
                                 uint64_t commit_ts,
                                 int32_t partition_id)
{
    auto record = std::make_shared<EloqStringRecord>();
    const std::string value = "value-" + std::to_string(key);
    record->SetEncodedBlob(
        reinterpret_cast<const unsigned char *>(value.data()), value.size());
    return FlushRecord(
        TxKey(std::make_unique<EloqStringKey>(std::to_string(key))),
        std::move(record),
        RecordStatus::Normal,
        commit_ts,
        /*cce=*/nullptr,
        /*post_flush_size=*/0,
        partition_id);
}

struct ReadCompletionState
{
    std::mutex mutex_;
    std::condition_variable cv_;
    bool done_{false};
};

class ReadCompletionClosure final : public google::protobuf::Closure
{
public:
    explicit ReadCompletionClosure(
        std::shared_ptr<ReadCompletionState> completion)
        : completion_(std::move(completion))
    {
    }

    void Run() override
    {
        // Protobuf callbacks may be self-deleting. Destroy the callback before
        // waking the test thread so no callback member outlives the wait.
        auto completion = std::move(completion_);
        delete this;
        {
            std::lock_guard lock(completion->mutex_);
            completion->done_ = true;
        }
        completion->cv_.notify_one();
    }

private:
    std::shared_ptr<ReadCompletionState> completion_;
};

EloqDS::remote::DataStoreError ReadKey(EloqDS::DataStoreService &service,
                                       std::string_view table_name,
                                       int32_t partition_id,
                                       int key,
                                       uint64_t &record_ts)
{
    std::string record;
    uint64_t ttl = 0;
    EloqDS::remote::CommonResult result;
    const std::string key_string = std::to_string(key);
    auto completion = std::make_shared<ReadCompletionState>();
    service.Read(table_name,
                 partition_id,
                 /*shard_id=*/0,
                 key_string,
                 /*reopen=*/false,
                 &record,
                 &record_ts,
                 &ttl,
                 &result,
                 new ReadCompletionClosure(completion));
    std::unique_lock lock(completion->mutex_);
    completion->cv_.wait(lock, [&] { return completion->done_; });
    return static_cast<EloqDS::remote::DataStoreError>(result.error_code());
}

class RealStoreFixture
{
public:
    RealStoreFixture()
    {
        GFLAGS_NAMESPACE::SetCommandLineOption("bthread_concurrency", "4");
        const auto unique =
            std::to_string(::getpid()) + "-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
        dir_ = std::filesystem::temp_directory_path() /
               ("real-datastore-test-" + unique);
        std::filesystem::create_directories(dir_);
        config_path_ = dir_ / "store.ini";

        std::ofstream config(config_path_);
        if (!config)
        {
            throw std::runtime_error("failed to create real-store test config");
        }
        config << "[store]\n";
#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
        config << "eloq_store_worker_num=4\n"
               << "eloq_store_init_page_count=1024\n"
               << "eloq_store_root_meta_cache_size=16MB\n"
               << "eloq_store_buffer_pool_size=64MB\n"
               << "eloq_store_local_space_limit=1GB\n";
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
        config << "rocksdb_write_buffer_size=4MB\n"
               << "rocksdb_target_file_size_base=4MB\n"
               << "rocksdb_cloud_sst_file_cache_size=64MB\n"
               << "rocksdb_cloud_sst_file_cache_num_shard_bits=0\n"
               << "rocksdb_cloud_db_ready_timeout_sec=30\n"
               << "rocksdb_cloud_db_file_deletion_delay_sec=0\n"
               << "rocksdb_cloud_run_purger=false\n"
               << "rocksdb_cloud_warm_up_thread_num=1\n";
#endif
        config.close();

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
        std::string base_url = RequireEnvironment(kObjectStoreUrlEnv);
        while (!base_url.empty() && base_url.back() == '/')
        {
            base_url.pop_back();
        }
        object_store_url_ = base_url + "/run-" + unique;
        Aws::InitAPI(aws_options_);
        aws_initialized_ = true;
#endif
        Start(/*create_if_missing=*/true);
    }

    ~RealStoreFixture()
    {
        Stop();
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
        if (aws_initialized_)
        {
            Aws::ShutdownAPI(aws_options_);
        }
#endif
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    EloqDS::DataStoreServiceClient &Client()
    {
        return *client_;
    }

    EloqDS::DataStoreService &Service()
    {
        return *service_;
    }

    void RestartWithoutVolatileCloudState()
    {
        Stop();
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
        // Force the reopened DB to recover its durable state through S3Proxy
        // instead of reusing the local RocksDB directory.
        std::filesystem::remove_all(dir_ / "rocksdb_data");
#endif
        Start(/*create_if_missing=*/false);
    }

private:
    std::unique_ptr<EloqDS::DataStoreFactory> MakeFactory()
    {
        INIReader config(config_path_.string());
        if (config.ParseError() < 0)
        {
            throw std::runtime_error("failed to parse real-store test config");
        }

#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
        uint32_t node_memory_mb = 256;
        EloqDS::EloqStoreConfig store_config(config,
                                             dir_.string(),
                                             node_memory_mb,
                                             /*core_number=*/4,
                                             /*standalone=*/true);
        return std::make_unique<EloqDS::EloqStoreDataStoreFactory>(
            std::move(store_config));
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
        EloqDS::RocksDBConfig rocksdb_config(config, dir_.string());
        EloqDS::RocksDBCloudConfig cloud_config(config);
        cloud_config.aws_access_key_id_ =
            RequireEnvironment("AWS_ACCESS_KEY_ID");
        cloud_config.aws_secret_key_ =
            RequireEnvironment("AWS_SECRET_ACCESS_KEY");
        cloud_config.oss_url_ = object_store_url_;
        cloud_config.region_ = "us-east-1";
        cloud_config.branch_name_ = "main";
        return std::make_unique<EloqDS::RocksDBCloudDataStoreFactory>(
            rocksdb_config,
            cloud_config,
            /*tx_enable_cache_replacement=*/false);
#else
        throw std::runtime_error("unsupported real-store test backend");
#endif
    }

    void Start(bool create_if_missing)
    {
        constexpr int kMaxBindRetries = 16;
        for (int attempt = 0; attempt < kMaxBindRetries && !service_; ++attempt)
        {
            auto [fd, port] = txservice::test::BindEphemeralPort();
            ::close(fd);
            cluster_manager_ =
                std::make_unique<EloqDS::DataStoreServiceClusterManager>();
            cluster_manager_->Initialize("127.0.0.1", port);
            auto candidate = std::make_unique<EloqDS::DataStoreService>(
                *cluster_manager_,
                config_path_.string(),
                (dir_ / "DSMigrateLog").string(),
                MakeFactory());
            if (candidate->StartService(create_if_missing))
            {
                service_ = std::move(candidate);
            }
            else
            {
                cluster_manager_.reset();
            }
        }
        if (!service_)
        {
            throw std::runtime_error("failed to start production datastore");
        }

        txservice::CatalogFactory *catalog_factories[3]{
            &range_catalog_factory_, &hash_catalog_factory_, nullptr};
        client_ = std::make_unique<EloqDS::DataStoreServiceClient>(
            /*is_bootstrap=*/false,
            catalog_factories,
            *cluster_manager_,
            /*bind_data_shard_with_ng=*/false,
            service_.get());
    }

    void Stop()
    {
        client_.reset();
        service_.reset();
        cluster_manager_.reset();
    }

    std::filesystem::path dir_;
    std::filesystem::path config_path_;
    EloqRangeCatalogFactory range_catalog_factory_;
    EloqHashCatalogFactory hash_catalog_factory_;
    std::unique_ptr<EloqDS::DataStoreServiceClusterManager> cluster_manager_;
    std::unique_ptr<EloqDS::DataStoreService> service_;
    std::unique_ptr<EloqDS::DataStoreServiceClient> client_;
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    std::string object_store_url_;
    Aws::SDKOptions aws_options_;
    bool aws_initialized_{false};
#endif
};
}  // namespace

TEST_CASE("production datastore persists only newest-term checkpoint data",
          "[real-datastore][checkpoint-flush]")
{
    const char *run_real_store =
        std::getenv(std::string(kRunRealStoreEnv).c_str());
    if (run_real_store == nullptr || std::string_view(run_real_store) != "1")
    {
        SKIP("set ELOQ_RUN_REAL_STORE_TEST=1 to run production datastore IO");
    }

#if !defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE) && \
    !defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    SKIP("the configured datastore is not covered by this integration test");
#else
    RealStoreFixture fixture;

    const TableName hash_table{std::string_view("real_hash_terms"),
                               TableType::Primary,
                               TableEngine::EloqKv};
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        hash_flush;
    auto &hash_entries = hash_flush["eloqkv_real_hash_terms"];
    auto add_hash = [&](int64_t term, int key, int32_t partition_id)
    {
        auto records = std::make_unique<std::vector<FlushRecord>>();
        records->push_back(MakeObjectRecord(key,
                                            RecordStatus::Normal,
                                            /*commit_ts=*/1000 + key,
                                            partition_id));
        hash_entries.push_back(
            MakeFlushEntry(MakeTask(hash_table, term), std::move(records)));
    };
    add_hash(/*term=*/10, /*key=*/1, /*partition_id=*/0);
    add_hash(/*term=*/11, /*key=*/2, /*partition_id=*/0);
    add_hash(/*term=*/10, /*key=*/3, /*partition_id=*/1);
    add_hash(/*term=*/11, /*key=*/4, /*partition_id=*/1);
    REQUIRE(fixture.Client().PutAll(hash_flush));

    const TableName range_table{std::string_view("real_range_terms"),
                                TableType::Primary,
                                TableEngine::InternalRange};
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        range_flush;
    auto &range_entries = range_flush["irange_real_range_terms"];
    auto add_range = [&](int64_t term, int key)
    {
        auto records = std::make_unique<std::vector<FlushRecord>>();
        records->push_back(MakeSerializedRecord(
            key, /*commit_ts=*/2000 + key, /*partition_id=*/5));
        range_entries.push_back(
            MakeFlushEntry(MakeTask(range_table, term), std::move(records)));
    };
    add_range(/*term=*/20, /*key=*/10);
    add_range(/*term=*/21, /*key=*/11);
    add_range(/*term=*/20, /*key=*/12);
    add_range(/*term=*/21, /*key=*/13);
    REQUIRE(fixture.Client().PutAll(range_flush));

    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        delete_flush;
    auto delete_records = std::make_unique<std::vector<FlushRecord>>();
    delete_records->push_back(MakeObjectRecord(/*key=*/4,
                                               RecordStatus::Deleted,
                                               /*commit_ts=*/3004,
                                               /*partition_id=*/1));
    delete_flush["eloqkv_real_hash_terms"].push_back(MakeFlushEntry(
        MakeTask(hash_table, /*term=*/12), std::move(delete_records)));
    REQUIRE(fixture.Client().PutAll(delete_flush));

    if (fixture.Client().NeedPersistKV())
    {
        // Checkpoint writes bypass RocksDB's WAL. Exercise the same explicit
        // durability boundary used by FlushDataImpl before deleting all local
        // RocksDB state and reopening solely from object storage.
        REQUIRE(fixture.Client().PersistKV(
            {"eloqkv_real_hash_terms", "irange_real_range_terms"}));
    }

    auto verify = [&]
    {
        uint64_t record_ts = 0;
        REQUIRE(ReadKey(fixture.Service(),
                        "eloqkv_real_hash_terms",
                        /*partition_id=*/0,
                        /*key=*/1,
                        record_ts) ==
                EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(ReadKey(fixture.Service(),
                        "eloqkv_real_hash_terms",
                        /*partition_id=*/0,
                        /*key=*/2,
                        record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 1002);
        REQUIRE(ReadKey(fixture.Service(),
                        "eloqkv_real_hash_terms",
                        /*partition_id=*/1,
                        /*key=*/3,
                        record_ts) ==
                EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(ReadKey(fixture.Service(),
                        "eloqkv_real_hash_terms",
                        /*partition_id=*/1,
                        /*key=*/4,
                        record_ts) ==
                EloqDS::remote::DataStoreError::KEY_NOT_FOUND);

        REQUIRE(ReadKey(fixture.Service(),
                        "irange_real_range_terms",
                        /*partition_id=*/5,
                        /*key=*/10,
                        record_ts) ==
                EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(ReadKey(fixture.Service(),
                        "irange_real_range_terms",
                        /*partition_id=*/5,
                        /*key=*/11,
                        record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 2011);
        REQUIRE(ReadKey(fixture.Service(),
                        "irange_real_range_terms",
                        /*partition_id=*/5,
                        /*key=*/12,
                        record_ts) ==
                EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(ReadKey(fixture.Service(),
                        "irange_real_range_terms",
                        /*partition_id=*/5,
                        /*key=*/13,
                        record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 2013);
    };

    verify();
    fixture.RestartWithoutVolatileCloudState();
    verify();
#endif
}
