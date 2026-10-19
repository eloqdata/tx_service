/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify it
 *    under either GNU Affero General Public License v3 or GNU General Public
 *    License v2.
 */

#include <gflags/gflags.h>
#include <unistd.h>

#include <atomic>
#include <catch2/catch_all.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc/cc_req_base.h"
#include "cc/cc_req_misc.h"
#include "cc/cc_request.h"
#define private public
#include "cc/local_cc_shards.h"
#undef private
#include "data_store_service_client_closure.h"
#include "data_sync_task.h"
#include "eloq_basic_catalog_factory.h"
#include "eloq_data_store_service/data_store_service.h"
#include "eloq_string_key_record.h"
#include "harness/mem_data_store_factory.h"
#include "harness/port_util.h"
#include "harness/test_node.h"
#include "include/mock/mock_catalog_factory.h"

using namespace std::chrono_literals;
using namespace txservice;

namespace
{
class ListRequest : public CcRequestBase
{
public:
    bool Execute(CcShard &) override
    {
        return false;
    }
};

class ShardRequest : public CcRequestBase
{
public:
    explicit ShardRequest(bool abort_if_oom = false)
        : abort_if_oom_(abort_if_oom)
    {
    }

    bool Execute(CcShard &) override
    {
        execute_count_.fetch_add(1, std::memory_order_release);
        return true;
    }

    bool AbortIfOom() const override
    {
        return abort_if_oom_;
    }

    void AbortCcRequest(CcErrorCode error) override
    {
        abort_error_.store(error, std::memory_order_release);
        Free();
    }

    int ExecuteCount() const
    {
        return execute_count_.load(std::memory_order_acquire);
    }

    CcErrorCode AbortError() const
    {
        return abort_error_.load(std::memory_order_acquire);
    }

private:
    const bool abort_if_oom_;
    std::atomic<int> execute_count_{0};
    std::atomic<CcErrorCode> abort_error_{CcErrorCode::NO_ERROR};
};

class ShardCleanerFixture
{
public:
    explicit ShardCleanerFixture(uint32_t node_memory_limit_mb)
        : tx_cnf_{{"node_memory_limit_mb", node_memory_limit_mb},
                  {"enable_key_cache", 0},
                  {"reltime_sampling", 0},
                  {"range_split_worker_num", 1},
                  {"range_slice_memory_limit_percent", 20},
                  {"core_num", 1},
                  {"realtime_sampling", 0},
                  {"checkpointer_interval", 10},
                  {"checkpointer_delay_seconds", 0},
                  {"checkpointer_min_ckpt_request_interval", 5},
                  {"enable_shard_heap_defragment", 0},
                  {"node_log_limit_mb", 1000},
                  {"collect_active_tx_ts_interval_seconds", 2},
                  {"rep_group_cnt", 1}},
          catalog_factories_{
              &catalog_factory_, &catalog_factory_, &catalog_factory_},
          local_shards_(/*node_id=*/0,
                        /*ng_id=*/0,
                        tx_cnf_,
                        catalog_factories_,
                        /*system_handler=*/nullptr,
                        &ng_configs_,
                        /*cluster_config_version=*/2,
                        /*store_hd=*/nullptr,
                        /*tx_service=*/nullptr,
                        /*enable_mvcc=*/false)
    {
        local_shards_.BindThreadToFastMetaDataShard(0);
        shard_ = local_shards_.GetCcShard(0);
        assert(shard_ != nullptr);
        shard_->Init();
    }

    CcShard &Shard()
    {
        return *shard_;
    }

private:
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs_{
        {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> tx_cnf_;
    MockCatalogFactory catalog_factory_;
    CatalogFactory *catalog_factories_[NUM_EXTERNAL_ENGINES];
    LocalCcShards local_shards_;
    CcShard *shard_{nullptr};
};

class Watchdog
{
public:
    explicit Watchdog(std::chrono::milliseconds budget)
    {
        thread_ = std::thread(
            [this, budget]
            {
                const auto deadline = std::chrono::steady_clock::now() + budget;
                while (!done_.load(std::memory_order_acquire))
                {
                    if (std::chrono::steady_clock::now() >= deadline)
                    {
                        std::abort();
                    }
                    std::this_thread::sleep_for(1ms);
                }
            });
    }

    ~Watchdog()
    {
        done_.store(true, std::memory_order_release);
        thread_.join();
    }

private:
    std::atomic<bool> done_{false};
    std::thread thread_;
};

DataSyncTask MakeTask(const TableName &table_name, int64_t term)
{
    return DataSyncTask(table_name,
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

std::shared_ptr<DataSyncTask> MakeTaskPtr(const TableName &table_name,
                                          int64_t term,
                                          NodeGroupId node_group_id = 1,
                                          int32_t id = 0)
{
    return std::make_shared<DataSyncTask>(table_name,
                                          id,
                                          /*range_version=*/0,
                                          node_group_id,
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

std::unique_ptr<FlushTaskEntry> MakeArchiveEntry(
    std::shared_ptr<DataSyncTask> task,
    std::unique_ptr<std::vector<FlushRecord>> records)
{
    return std::make_unique<FlushTaskEntry>(
        std::make_unique<std::vector<FlushRecord>>(),
        std::move(records),
        std::make_unique<std::vector<std::pair<TxKey, int32_t>>>(),
        /*data_sync_txm=*/nullptr,
        std::move(task),
        /*table_schema=*/nullptr,
        /*size=*/0);
}

std::unique_ptr<FlushTaskEntry> MakeMvBaseEntry(
    std::shared_ptr<DataSyncTask> task,
    std::unique_ptr<std::vector<std::pair<TxKey, int32_t>>> base_records)
{
    return std::make_unique<FlushTaskEntry>(
        std::make_unique<std::vector<FlushRecord>>(),
        std::make_unique<std::vector<FlushRecord>>(),
        std::move(base_records),
        /*data_sync_txm=*/nullptr,
        std::move(task),
        /*table_schema=*/nullptr,
        /*size=*/0);
}

EloqDS::remote::DataStoreError ReadKey(EloqDS::DataStoreService &service,
                                       std::string_view table_name,
                                       int32_t partition_id,
                                       std::string_view key,
                                       uint64_t &record_ts)
{
    std::string record;
    uint64_t ttl = 0;
    EloqDS::remote::CommonResult result;
    service.Read(table_name,
                 partition_id,
                 /*shard_id=*/0,
                 key,
                 /*reopen=*/false,
                 &record,
                 &record_ts,
                 &ttl,
                 &result,
                 /*done=*/nullptr);
    return static_cast<EloqDS::remote::DataStoreError>(result.error_code());
}

FlushRecord MakeObjectRecord(int key,
                             std::string value,
                             RecordStatus status,
                             uint64_t commit_ts,
                             uint64_t ttl,
                             int32_t partition_id)
{
    std::shared_ptr<TxRecord> payload;
    if (status == RecordStatus::Normal)
    {
        auto blob = std::make_shared<BlobTxRecord>();
        blob->value_ = std::move(value);
        blob->ttl_ = ttl;
        payload = std::move(blob);
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
                                 std::string value,
                                 RecordStatus status,
                                 uint64_t commit_ts,
                                 int32_t partition_id)
{
    std::shared_ptr<TxRecord> payload;
    if (status == RecordStatus::Normal)
    {
        auto record = std::make_shared<EloqStringRecord>();
        record->SetEncodedBlob(
            reinterpret_cast<const unsigned char *>(value.data()),
            value.size());
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

class PutAllFixture
{
public:
    explicit PutAllFixture(bool fail_flush_data = false)
    {
        GFLAGS_NAMESPACE::SetCommandLineOption("bthread_concurrency", "4");
#ifdef ELOQ_MODULE_ENABLED
        // A later dispatch test starts TxService on the same process-global
        // brpc worker pool. Configure those workers before this fixture starts
        // the first brpc server, matching TestNode's bring-up requirement.
        GFLAGS_NAMESPACE::SetCommandLineOption("brpc_worker_as_ext_processor",
                                               "true");
        GFLAGS_NAMESPACE::SetCommandLineOption("worker_polling_time_us",
                                               "100000");
#endif
        dir_ = std::filesystem::temp_directory_path() /
               ("checkpoint_putall_" + std::to_string(::getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(dir_);

        constexpr int kMaxBindRetries = 16;
        for (int attempt = 0; attempt < kMaxBindRetries && !service_; ++attempt)
        {
            auto [fd, port] = txservice::test::BindEphemeralPort();
            ::close(fd);
            cluster_manager_.Initialize("127.0.0.1", port);
            auto candidate = std::make_unique<EloqDS::DataStoreService>(
                cluster_manager_,
                (dir_ / "dss_config.ini").string(),
                (dir_ / "DSMigrateLog").string(),
                std::make_unique<EloqDS::MemDataStoreFactory>(fail_flush_data));
            if (candidate->StartService(/*create_db_if_missing=*/true))
            {
                service_ = std::move(candidate);
            }
        }
        if (!service_)
        {
            throw std::runtime_error("failed to start PutAll test service");
        }

        txservice::CatalogFactory *catalog_factories[3]{
            &range_catalog_factory_, &hash_catalog_factory_, nullptr};
        client_ = std::make_unique<EloqDS::DataStoreServiceClient>(
            /*is_bootstrap=*/false,
            catalog_factories,
            cluster_manager_,
            /*bind_data_shard_with_ng=*/false,
            service_.get());
    }

    ~PutAllFixture()
    {
        client_.reset();
        service_.reset();
        std::filesystem::remove_all(dir_);
    }

    EloqDS::DataStoreServiceClient &Client()
    {
        return *client_;
    }

    EloqDS::DataStoreService &Service()
    {
        return *service_;
    }

private:
    std::filesystem::path dir_;
    EloqRangeCatalogFactory range_catalog_factory_;
    EloqHashCatalogFactory hash_catalog_factory_;
    EloqDS::DataStoreServiceClusterManager cluster_manager_;
    std::unique_ptr<EloqDS::DataStoreService> service_;
    std::unique_ptr<EloqDS::DataStoreServiceClient> client_;
};
}  // namespace

TEST_CASE("CcRequestList preserves intrusive links across every removal shape",
          "[checkpoint-flush][cc-request-list]")
{
    CcRequestList list;
    ListRequest first;
    ListRequest second;
    ListRequest third;

    REQUIRE(list.Empty());
    REQUIRE(list.Size() == 0);
    REQUIRE(list.Front() == nullptr);
    REQUIRE_FALSE(list.Contains(&first));

    list.PushBack(&first);
    REQUIRE(list.Contains(&first));
    REQUIRE_FALSE(list.Contains(&second));
    list.PushBack(&second);
    list.PushBack(&third);
    REQUIRE(list.Size() == 3);
    REQUIRE(list.Front() == &first);
    REQUIRE(list.Contains(&first));
    REQUIRE(list.Contains(&second));
    REQUIRE(list.Contains(&third));
    REQUIRE(CcRequestList::NextOf(&first) == &second);
    REQUIRE(CcRequestList::NextOf(&second) == &third);

    list.Remove(&second);
    REQUIRE(list.Size() == 2);
    REQUIRE_FALSE(list.Contains(&second));
    REQUIRE(CcRequestList::NextOf(&first) == &third);

    REQUIRE(list.PopFront() == &first);
    REQUIRE(list.Front() == &third);
    REQUIRE(list.PopFront() == &third);
    REQUIRE(list.PopFront() == nullptr);
    REQUIRE(list.Empty());

    // Removal clears both intrusive links, so the same request can safely park
    // again on this or a different wait collection.
    list.PushBack(&second);
    REQUIRE(list.Contains(&second));
    REQUIRE(list.PopFront() == &second);
    REQUIRE_FALSE(list.Contains(&second));
    REQUIRE(list.Empty());

    second.Use();
    REQUIRE(second.InUse());
    second.Free();
    REQUIRE_FALSE(second.InUse());
}

TEST_CASE("shard cleaner preserves a wake received during an active pass",
          "[checkpoint-flush][shard-clean][sticky-wake]")
{
    // These requests outlive local_shards, whose unprocessed queue retains
    // their addresses until fixture teardown.
    ShardRequest parked_request;
    ShardCleanCc clean_pass;
    ShardCleanerFixture fixture(/*node_memory_limit_mb=*/0);
    CcShard &shard = fixture.Shard();

    // Initialization may already have queued the cleaner when the zero-byte
    // heap became full. Either way, this call leaves the owned cleaner in-use
    // and queued without a processor consuming it.
    shard.WakeUpShardCleanCc();
    const size_t active_cleaner_queue_size = shard.QueueSize();
    REQUIRE(active_cleaner_queue_size > 0);

    // This second call must record a sticky wake instead of enqueueing another
    // copy of the cleaner.
    shard.WakeUpShardCleanCc();
    REQUIRE(shard.QueueSize() == active_cleaner_queue_size);

    parked_request.Use();
    shard.EnqueueWaitListIfMemoryFull(&parked_request);
    REQUIRE(shard.WaitListSizeForMemory() == 1);

    // A zero-byte heap is deterministically full. Marking defrag in-flight
    // keeps the unproductive-pass branch from notifying a checkpointer, which
    // is intentionally absent from this standalone fixture.
    shard.GetShardHeap()->SetDefragHeapCcOnFly(true);
    clean_pass.Use();
    REQUIRE_FALSE(clean_pass.Execute(shard));

    // Execute consumed the sticky wake and queued another pass rather than
    // returning true while the ordinary request remained parked.
    REQUIRE(shard.WaitListSizeForMemory() == 1);
    REQUIRE(shard.QueueSize() == active_cleaner_queue_size + 1);
    REQUIRE_FALSE(shard.TakeShardCleanCcWakeUp());
}

TEST_CASE("shard cleaner clears a sticky wake after draining parked requests",
          "[checkpoint-flush][shard-clean][sticky-wake]")
{
    ShardRequest parked_request;
    ShardCleanCc clean_pass;
    ShardCleanerFixture fixture(/*node_memory_limit_mb=*/1000);
    CcShard &shard = fixture.Shard();

    shard.WakeUpShardCleanCc();
    const size_t active_cleaner_queue_size = shard.QueueSize();
    REQUIRE(active_cleaner_queue_size > 0);
    shard.WakeUpShardCleanCc();
    REQUIRE(shard.QueueSize() == active_cleaner_queue_size);

    parked_request.Use();
    shard.EnqueueWaitListIfMemoryFull(&parked_request);
    clean_pass.Use();
    REQUIRE(clean_pass.Execute(shard));

    // Available memory released the parked request. The concurrent wake has
    // therefore already been served and must not survive into a later pass.
    REQUIRE(shard.WaitListSizeForMemory() == 0);
    REQUIRE(shard.QueueSize() == active_cleaner_queue_size + 1);
    REQUIRE_FALSE(shard.TakeShardCleanCcWakeUp());
}

TEST_CASE("checkpoint publication honors backend and MVCC durability bounds",
          "[checkpoint-flush][durability]")
{
    REQUIRE_FALSE(DeferCkptTsUpdate(/*need_persist_kv=*/false,
                                    /*enable_mvcc=*/false));
    REQUIRE(DeferCkptTsUpdate(/*need_persist_kv=*/true,
                              /*enable_mvcc=*/false));
    REQUIRE(DeferCkptTsUpdate(/*need_persist_kv=*/false,
                              /*enable_mvcc=*/true));
    REQUIRE(DeferCkptTsUpdate(/*need_persist_kv=*/true,
                              /*enable_mvcc=*/true));
}

TEST_CASE("UpdateCceCkptTsCc fan-in publishes one completion",
          "[checkpoint-flush][ckpt-fan-in]")
{
    Watchdog watchdog(20s);
    constexpr size_t kCoreCount = 4;
    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        entries;
    for (size_t core = 0; core < kCoreCount; ++core)
    {
        entries[core].emplace_back(nullptr, 10 + core, 0);
    }

    const TableName table{
        std::string_view("fan_in"), TableType::Primary, TableEngine::EloqKv};
    UpdateCceCkptTsCc request(/*node_group_id=*/1,
                              /*term=*/7,
                              table,
                              entries);
    std::atomic<int> callback_count{0};
    request.SetOnFinished(
        [&callback_count]
        { callback_count.fetch_add(1, std::memory_order_relaxed); });

    std::vector<std::thread> finishers;
    for (size_t core = 0; core < kCoreCount; ++core)
    {
        finishers.emplace_back([&request] { request.SetFinished(); });
    }
    for (auto &finisher : finishers)
    {
        finisher.join();
    }

    REQUIRE(request.IsFinished());
    REQUIRE(callback_count.load(std::memory_order_relaxed) == 1);
}

TEST_CASE("UpdateCceCkptTsCc blocking waiter waits for every core",
          "[checkpoint-flush][ckpt-fan-in]")
{
    Watchdog watchdog(20s);
    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        entries;
    entries[0].emplace_back(nullptr, 10, 0);
    entries[1].emplace_back(nullptr, 11, 0);

    const TableName table{
        std::string_view("wait"), TableType::Primary, TableEngine::EloqKv};

    SECTION("waiter starts before completion")
    {
        std::thread first;
        std::thread second;
        bool finished = false;
        {
            UpdateCceCkptTsCc request(1, 7, table, entries);
            first = std::thread([&request] { request.SetFinished(); });
            second = std::thread(
                [&request]
                {
                    std::this_thread::sleep_for(2ms);
                    request.SetFinished();
                });

            request.Wait();
            finished = request.IsFinished();
        }
        // Wait returning must be a sufficient lifetime barrier; callers do not
        // join cc-shard threads before destroying a stack-owned request.
        first.join();
        second.join();
        REQUIRE(finished);
    }

    SECTION("completion arrives before Wait")
    {
        UpdateCceCkptTsCc request(1, 7, table, entries);
        request.SetFinished();
        request.SetFinished();
        request.Wait();
        REQUIRE(request.IsFinished());
    }
}

TEST_CASE("UpdateCceCkptTsCc coroutine wakeup covers both completion races",
          "[checkpoint-flush][ckpt-fan-in]")
{
    Watchdog watchdog(20s);
    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        entries;
    entries[0].emplace_back(nullptr, 10, 0);
    const TableName table{
        std::string_view("coro_wait"), TableType::Primary, TableEngine::EloqKv};

    SECTION("waiter arms before completion")
    {
        UpdateCceCkptTsCc request(1, 7, table, entries);
        std::mutex scheduler_mutex;
        std::condition_variable scheduler_cv;
        bool yielded = false;
        bool resume_permit = false;
        const std::function<void()> yield = [&]
        {
            std::unique_lock lk(scheduler_mutex);
            yielded = true;
            scheduler_cv.notify_all();
            scheduler_cv.wait(lk, [&] { return resume_permit; });
        };
        const std::function<void()> resume = [&]
        {
            std::lock_guard lk(scheduler_mutex);
            resume_permit = true;
            scheduler_cv.notify_all();
        };
        request.SetCoroCallbacks(&yield, &resume);
        std::thread finisher(
            [&]
            {
                std::unique_lock lk(scheduler_mutex);
                scheduler_cv.wait(lk, [&] { return yielded; });
                lk.unlock();
                request.SetFinished();
            });

        request.Wait();
        finisher.join();
        REQUIRE(request.IsFinished());
    }

    SECTION("completion arrives before waiter arms")
    {
        int yield_count = 0;
        const std::function<void()> yield = [&] { ++yield_count; };
        const std::function<void()> resume = [] {};
        std::thread finisher;
        bool finished = false;
        {
            UpdateCceCkptTsCc request(1, 7, table, entries);
            request.SetCoroCallbacks(&yield, &resume);
            finisher = std::thread([&request] { request.SetFinished(); });
            while (!request.IsFinished())
            {
                std::this_thread::yield();
            }
            request.Wait();
            finished = request.IsFinished();
        }
        // The terminal shard may still be returning from SetFinished(), but it
        // must not access the request after publishing zero for an unarmed
        // coroutine waiter.
        finisher.join();
        REQUIRE(finished);
        REQUIRE(yield_count == 0);
    }
}

TEST_CASE("UpdateCceCkptTsCc coroutine fan-in resumes exactly once",
          "[checkpoint-flush][ckpt-fan-in]")
{
    Watchdog watchdog(20s);
    constexpr size_t kCoreCount = 4;
    absl::flat_hash_map<size_t, std::vector<UpdateCceCkptTsCc::CkptTsEntry>>
        entries;
    for (size_t core = 0; core < kCoreCount; ++core)
    {
        entries[core].emplace_back(nullptr, 10 + core, 0);
    }
    const TableName table{std::string_view("coro_fan_in"),
                          TableType::Primary,
                          TableEngine::EloqKv};

    std::mutex scheduler_mutex;
    std::condition_variable scheduler_cv;
    bool yielded = false;
    bool resume_permit = false;
    int resume_count = 0;
    const std::function<void()> yield = [&]
    {
        std::unique_lock lk(scheduler_mutex);
        yielded = true;
        scheduler_cv.notify_all();
        scheduler_cv.wait(lk, [&] { return resume_permit; });
    };
    const std::function<void()> resume = [&]
    {
        std::lock_guard lk(scheduler_mutex);
        ++resume_count;
        resume_permit = true;
        scheduler_cv.notify_all();
    };

    std::vector<std::thread> finishers;
    bool finished = false;
    {
        UpdateCceCkptTsCc request(1, 7, table, entries);
        request.SetCoroCallbacks(&yield, &resume);
        for (size_t core = 0; core < kCoreCount; ++core)
        {
            finishers.emplace_back(
                [&]
                {
                    {
                        std::unique_lock lk(scheduler_mutex);
                        scheduler_cv.wait(lk, [&] { return yielded; });
                    }
                    request.SetFinished();
                });
        }

        request.Wait();
        finished = request.IsFinished();
    }
    for (auto &finisher : finishers)
    {
        finisher.join();
    }

    REQUIRE(finished);
    REQUIRE(resume_count == 1);
}

TEST_CASE("SyncPutAllData releases progress per completion and wakes once",
          "[checkpoint-flush][partition-progress]")
{
    Watchdog watchdog(20s);
    EloqDS::SyncPutAllData sync;
    sync.Reset();
    sync.total_partitions_ = 3;
    sync.total_bytes_ = 60;

    std::mutex scheduler_mutex;
    std::condition_variable scheduler_cv;
    int yield_count = 0;
    int resume_permits = 0;
    std::vector<std::pair<uint64_t, uint64_t>> progress;

    const std::function<void()> yield = [&]
    {
        std::unique_lock lk(scheduler_mutex);
        ++yield_count;
        scheduler_cv.notify_all();
        scheduler_cv.wait(lk, [&] { return resume_permits > 0; });
        --resume_permits;
    };
    const std::function<void()> resume = [&]
    {
        std::lock_guard lk(scheduler_mutex);
        ++resume_permits;
        scheduler_cv.notify_all();
    };
    const std::function<void(uint64_t, uint64_t)> report =
        [&](uint64_t done, uint64_t total)
    { progress.emplace_back(done, total); };
    sync.SetCoroCallbacks(&yield, &resume);
    sync.SetProgressCallback(&report);

    // The quota release runs inside OnPartitionCompleted on the completing
    // thread; the waiter is only woken by the final completion. Progress must
    // therefore accumulate without any intermediate waiter wake-ups.
    std::thread completions(
        [&]
        {
            {
                std::unique_lock lk(scheduler_mutex);
                scheduler_cv.wait(lk, [&] { return yield_count >= 1; });
            }
            sync.OnPartitionCompleted(10);
            sync.OnPartitionCompleted(20);
            sync.OnPartitionCompleted(30);
        });

    sync.Wait(&yield, &resume);
    completions.join();

    // One suspension, one wake: intermediate completions released quota
    // directly instead of resuming the waiter.
    REQUIRE(yield_count == 1);
    REQUIRE(progress.size() == 3);
    REQUIRE((progress[0] == std::pair<uint64_t, uint64_t>{10, 60}));
    REQUIRE((progress[1] == std::pair<uint64_t, uint64_t>{30, 60}));
    REQUIRE((progress[2] == std::pair<uint64_t, uint64_t>{60, 60}));
}

TEST_CASE("SyncPutAllData clears progress callbacks before pool reuse",
          "[checkpoint-flush][partition-progress][pool-lifecycle]")
{
    EloqDS::SyncPutAllData sync;
    sync.Reset();
    const std::function<void(uint64_t, uint64_t)> report = [](uint64_t,
                                                              uint64_t) {};

    sync.SetProgressCallback(&report);
    REQUIRE(sync.progress_fn_ == &report);
    sync.Reset();
    REQUIRE(sync.progress_fn_ == nullptr);

    sync.SetProgressCallback(&report);
    sync.Clear();
    REQUIRE(sync.progress_fn_ == nullptr);
}

TEST_CASE("one partition combines checkpoint entries from one term",
          "[checkpoint-flush][partition-term]")
{
    const TableName table{
        std::string_view("one_term"), TableType::Primary, TableEngine::EloqKv};
    DataSyncTask first_task = MakeTask(table, 10);
    DataSyncTask second_task = MakeTask(table, 10);

    EloqDS::PartitionFlushState partition;
    partition.Reset(/*pid=*/7, /*is_range_partitioned=*/false);
    partition.charged_mem_bytes_ = 99;
    partition.AddCkptTsEntry(&first_task, 0, nullptr, 100, 1);
    partition.AddCkptTsEntry(&first_task, 0, nullptr, 101, 2);
    partition.AddCkptTsEntry(&second_task, 0, nullptr, 102, 3);

    EloqDS::SyncPutAllData sync;
    sync.Reset();
    sync.total_partitions_ = 1;
    sync.total_bytes_ = 99;
    partition.ArmCkptTsUpdate(&sync);

    REQUIRE(partition.ckpt_ts_task_ == &first_task);
    REQUIRE(partition.ckpt_ts_entries_.at(0).size() == 3);
    REQUIRE(partition.ckpt_ts_update_.has_value());
    partition.ckpt_ts_update_->SetFinished();
    REQUIRE(sync.completed_partitions_ == 1);
    REQUIRE(sync.completed_bytes_ == 99);
}

TEST_CASE("deferred checkpoint publication aggregates only newest-term entries",
          "[checkpoint-flush][partition-term][deferred-publication]")
{
    const TableName hash_table{std::string_view("deferred_hash"),
                               TableType::Primary,
                               TableEngine::EloqKv};
    const TableName stale_table{std::string_view("deferred_stale"),
                                TableType::Primary,
                                TableEngine::EloqKv};
    FlushTaskEntryMap flush_task;

    auto add_hash_record = [&](std::string_view kv_table_name,
                               const TableName &table_name,
                               int64_t term,
                               NodeGroupId node_group_id,
                               int32_t core_id,
                               uintptr_t cce_address,
                               uint64_t commit_ts)
    {
        auto task = MakeTaskPtr(table_name, term, node_group_id, core_id);
        auto records = std::make_unique<std::vector<FlushRecord>>();
        records->push_back(MakeObjectRecord(static_cast<int>(commit_ts),
                                            "value",
                                            RecordStatus::Normal,
                                            commit_ts,
                                            /*ttl=*/UINT64_MAX,
                                            /*partition_id=*/core_id));
        records->back().cce_ = reinterpret_cast<LruEntry *>(cce_address);
        records->back().post_flush_size_ = commit_ts;
        flush_task[kv_table_name].push_back(
            MakeFlushEntry(std::move(task), std::move(records)));
    };

    // Node group 1 retains both term-31 entries and combines their shard maps.
    // The older entries surround them to ensure selection is independent of
    // input order. Node group 2 has an unrelated, lower numeric term and must
    // remain eligible.
    add_hash_record("eloqkv_deferred_hash", hash_table, 30, 1, 0, 0x10, 100);
    add_hash_record("eloqkv_deferred_hash", hash_table, 31, 1, 0, 0x20, 101);
    add_hash_record("eloqkv_deferred_hash", hash_table, 30, 1, 1, 0x30, 102);
    add_hash_record("eloqkv_deferred_hash", hash_table, 31, 1, 1, 0x40, 103);
    add_hash_record("eloqkv_deferred_hash", hash_table, 8, 2, 2, 0x50, 104);
    add_hash_record("eloqkv_deferred_hash", hash_table, 31, 1, 3, 0, 105);
    // Tasks flagged as no-ckpt-ts-report used to be excluded here; the flag is
    // gone (every flushed record publishes so its cce leaves the in-flight
    // checkpoint state), so this record must appear under core 3.
    add_hash_record("eloqkv_deferred_hash", hash_table, 31, 1, 3, 0x60, 106);
    auto null_vector_entry = MakeFlushEntry(
        MakeTaskPtr(hash_table, /*term=*/31, /*node_group_id=*/1, /*id=*/3),
        std::make_unique<std::vector<FlushRecord>>());
    null_vector_entry->data_sync_vec_.reset();
    flush_task["eloqkv_deferred_hash"].push_back(std::move(null_vector_entry));
    // Term selection is batch-wide. This other table has no retained entry
    // because node group 1's term 31 appears above.
    add_hash_record("eloqkv_deferred_stale", stale_table, 30, 1, 3, 0x70, 107);

    const NewestTermByNodeGroup newest_terms = FindNewestTerms(flush_task);
    REQUIRE(newest_terms.at(1) == 31);
    REQUIRE(newest_terms.at(2) == 8);

    std::vector<CkptTsUpdateGroup> groups = CollectCkptTsUpdateGroups(
        flush_task.at("eloqkv_deferred_hash"), newest_terms, 4);
    REQUIRE(groups.size() == 2);

    const CkptTsUpdateGroup *node_group_1 = nullptr;
    const CkptTsUpdateGroup *node_group_2 = nullptr;
    for (const CkptTsUpdateGroup &group : groups)
    {
        if (group.node_group_id_ == 1)
        {
            node_group_1 = &group;
        }
        else if (group.node_group_id_ == 2)
        {
            node_group_2 = &group;
        }
    }

    REQUIRE(node_group_1 != nullptr);
    REQUIRE(node_group_1->node_group_term_ == 31);
    REQUIRE(node_group_1->cce_entries_.size() == 3);
    REQUIRE(node_group_1->cce_entries_.at(0).size() == 1);
    REQUIRE(node_group_1->cce_entries_.at(0).front().cce_ ==
            reinterpret_cast<LruEntry *>(uintptr_t{0x20}));
    REQUIRE(node_group_1->cce_entries_.at(1).size() == 1);
    REQUIRE(node_group_1->cce_entries_.at(1).front().cce_ ==
            reinterpret_cast<LruEntry *>(uintptr_t{0x40}));
    REQUIRE(node_group_1->cce_entries_.at(3).size() == 1);
    REQUIRE(node_group_1->cce_entries_.at(3).front().cce_ ==
            reinterpret_cast<LruEntry *>(uintptr_t{0x60}));

    REQUIRE(node_group_2 != nullptr);
    REQUIRE(node_group_2->node_group_term_ == 8);
    REQUIRE(node_group_2->cce_entries_.size() == 1);
    REQUIRE(node_group_2->cce_entries_.at(2).size() == 1);
    REQUIRE(CollectCkptTsUpdateGroups(
                flush_task.at("eloqkv_deferred_stale"), newest_terms, 4)
                .empty());

    std::vector<std::unique_ptr<FlushTaskEntry>> empty_entries;
    REQUIRE(CollectCkptTsUpdateGroups(empty_entries, newest_terms, 4).empty());
    REQUIRE(FindNewestTerms(FlushTaskEntryMap{}).empty());
}

TEST_CASE("deferred range checkpoint publication maps every retained range",
          "[checkpoint-flush][partition-term][deferred-publication]")
{
    const TableName table{std::string_view("deferred_range"),
                          TableType::Primary,
                          TableEngine::InternalRange};
    FlushTaskEntryMap flush_task;
    auto add_range_record = [&](int64_t term,
                                int32_t range_id,
                                uintptr_t cce_address,
                                uint64_t commit_ts)
    {
        auto records = std::make_unique<std::vector<FlushRecord>>();
        records->push_back(MakeSerializedRecord(static_cast<int>(commit_ts),
                                                "value",
                                                RecordStatus::Normal,
                                                commit_ts,
                                                range_id));
        records->back().cce_ = reinterpret_cast<LruEntry *>(cce_address);
        flush_task["irange_deferred_range"].push_back(MakeFlushEntry(
            MakeTaskPtr(table, term, /*node_group_id=*/3, range_id),
            std::move(records)));
    };

    add_range_record(/*term=*/11, /*range_id=*/1026, 0x10, 200);
    add_range_record(/*term=*/12, /*range_id=*/1027, 0x20, 201);
    add_range_record(/*term=*/12, /*range_id=*/1028, 0x30, 202);

    const NewestTermByNodeGroup newest_terms = FindNewestTerms(flush_task);
    std::vector<CkptTsUpdateGroup> groups = CollectCkptTsUpdateGroups(
        flush_task.at("irange_deferred_range"), newest_terms, 4);

    REQUIRE(groups.size() == 1);
    REQUIRE(groups.front().node_group_term_ == 12);
    REQUIRE(groups.front().cce_entries_.size() == 2);
    REQUIRE(groups.front().cce_entries_.at(3).front().commit_ts_ == 201);
    REQUIRE(groups.front().cce_entries_.at(0).front().commit_ts_ == 202);
}

TEST_CASE("deferred flush dispatches one newest-term update per table shard",
          "[checkpoint-flush][partition-term][deferred-publication][dispatch]")
{
    Watchdog watchdog(20s);
    PutAllFixture fixture;
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
        {1, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> tx_cnf{
        {"node_memory_limit_mb", 1000},
        {"enable_key_cache", 0},
        {"reltime_sampling", 0},
        {"range_split_worker_num", 1},
        {"range_slice_memory_limit_percent", 20},
        {"core_num", 1},
        {"realtime_sampling", 0},
        {"checkpointer_interval", 10},
        {"checkpointer_delay_seconds", 0},
        {"checkpointer_min_ckpt_request_interval", 5},
        {"enable_shard_heap_defragment", 0},
        {"node_log_limit_mb", 1000},
        {"collect_active_tx_ts_interval_seconds", 2},
        {"rep_group_cnt", 1}};
    MockCatalogFactory catalog_factory;
    CatalogFactory *catalog_factories[NUM_EXTERNAL_ENGINES]{
        &catalog_factory, &catalog_factory, &catalog_factory};

    // RocksDB-backed builds defer because PersistKV is required. EloqStore
    // normally publishes base records per partition, so enable MVCC there to
    // exercise its deferred-after-archives boundary with the same test.
    const bool enable_mvcc = !fixture.Client().NeedPersistKV();
    LocalCcShards local_shards(/*node_id=*/0,
                               /*ng_id=*/1,
                               tx_cnf,
                               catalog_factories,
                               /*system_handler=*/nullptr,
                               &ng_configs,
                               /*cluster_config_version=*/2,
                               &fixture.Client(),
                               /*tx_service=*/nullptr,
                               enable_mvcc);
    local_shards.BindThreadToFastMetaDataShard(0);
    local_shards.GetCcShard(0)->Init();

    const TableName table{std::string_view("deferred_dispatch"),
                          TableType::Primary,
                          TableEngine::EloqKv};
    FlushDataTask flush_task;
    auto add_record =
        [&](int64_t term, int key, uintptr_t cce_address, uint64_t commit_ts)
    {
        auto task = MakeTaskPtr(table, term, /*node_group_id=*/1, /*id=*/0);
        // Post-processing is outside this test's concern. Keep one synthetic
        // flight outstanding so it does not finish a task with no test status.
        task->flight_task_cnt_ = 2;
        auto records = std::make_unique<std::vector<FlushRecord>>();
        records->push_back(MakeObjectRecord(key,
                                            "value",
                                            RecordStatus::Normal,
                                            commit_ts,
                                            /*ttl=*/UINT64_MAX,
                                            /*partition_id=*/0));
        records->back().cce_ = reinterpret_cast<LruEntry *>(cce_address);
        flush_task.flush_task_entries_["eloqkv_deferred_dispatch"].push_back(
            MakeFlushEntry(std::move(task), std::move(records)));
    };

    add_record(/*term=*/998, /*key=*/301, /*cce=*/0x10, /*commit_ts=*/601);
    add_record(/*term=*/999, /*key=*/302, /*cce=*/0x20, /*commit_ts=*/602);
    add_record(/*term=*/999, /*key=*/303, /*cce=*/0x30, /*commit_ts=*/603);

    size_t processed_cc_requests = 0;
    const std::function<void()> yield = [&]
    {
        size_t processed = 0;
        do
        {
            processed = local_shards.ProcessRequests(/*thd_id=*/0);
            processed_cc_requests += processed;
        } while (processed > 0);
        std::this_thread::yield();
    };
    const std::function<void()> resume = [] {};
    const std::function<void()> sync_yield = [] {};

    Sharder &sharder = Sharder::Instance();
    const int64_t saved_leader_term = sharder.LeaderTerm(/*ng_id=*/1);
    const int64_t saved_standby_term = sharder.StandbyNodeTerm();
    // Keep the sentinel CCE pointers opaque: the stale request must finish at
    // its term fence before UpdateCceCkptTsCc dereferences any of them.
    sharder.SetLeaderTerm(/*ng_id=*/1, /*term=*/-1);
    sharder.SetStandbyNodeTerm(/*standby_term=*/-1);
    local_shards.FlushDataImpl(
        &flush_task, /*worker_idx=*/0, sync_yield, yield, resume);
    sharder.SetLeaderTerm(/*ng_id=*/1, saved_leader_term);
    sharder.SetStandbyNodeTerm(saved_standby_term);

    // Both retained entries target shard 0 and are published by one aggregated
    // request. The obsolete term-998 entry was neither stored nor published.
    REQUIRE(processed_cc_requests == 1);
    uint64_t record_ts = 0;
    REQUIRE(ReadKey(fixture.Service(),
                    "eloqkv_deferred_dispatch",
                    /*partition_id=*/0,
                    "301",
                    record_ts) ==
            EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
    REQUIRE(ReadKey(fixture.Service(),
                    "eloqkv_deferred_dispatch",
                    /*partition_id=*/0,
                    "302",
                    record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
    REQUIRE(record_ts == 602);
    REQUIRE(ReadKey(fixture.Service(),
                    "eloqkv_deferred_dispatch",
                    /*partition_id=*/0,
                    "303",
                    record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
    REQUIRE(record_ts == 603);
}

TEST_CASE("PersistKV failure does not publish deferred checkpoint timestamps",
          "[checkpoint-flush][deferred-publication][persist-failure]")
{
    Watchdog watchdog(20s);
    PutAllFixture fixture(/*fail_flush_data=*/true);
    if (!fixture.Client().NeedPersistKV())
    {
        SKIP("EloqStore has no post-PutAll persistence boundary");
    }

    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
        {1, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> tx_cnf{
        {"node_memory_limit_mb", 1000},
        {"enable_key_cache", 0},
        {"reltime_sampling", 0},
        {"range_split_worker_num", 1},
        {"range_slice_memory_limit_percent", 20},
        {"core_num", 1},
        {"realtime_sampling", 0},
        {"checkpointer_interval", 10},
        {"checkpointer_delay_seconds", 0},
        {"checkpointer_min_ckpt_request_interval", 5},
        {"enable_shard_heap_defragment", 0},
        {"node_log_limit_mb", 1000},
        {"collect_active_tx_ts_interval_seconds", 2},
        {"rep_group_cnt", 1}};
    MockCatalogFactory catalog_factory;
    CatalogFactory *catalog_factories[NUM_EXTERNAL_ENGINES]{
        &catalog_factory, &catalog_factory, &catalog_factory};
    LocalCcShards local_shards(/*node_id=*/0,
                               /*ng_id=*/1,
                               tx_cnf,
                               catalog_factories,
                               /*system_handler=*/nullptr,
                               &ng_configs,
                               /*cluster_config_version=*/2,
                               &fixture.Client(),
                               /*tx_service=*/nullptr,
                               /*enable_mvcc=*/false);
    local_shards.BindThreadToFastMetaDataShard(0);
    local_shards.GetCcShard(0)->Init();

    const TableName table{std::string_view("persist_failure"),
                          TableType::Primary,
                          TableEngine::EloqKv};
    auto task = MakeTaskPtr(table, /*term=*/999, /*node_group_id=*/1, /*id=*/0);
    // Keep one synthetic flight outstanding so post-processing records the
    // flush error without trying to finish a task whose status is omitted by
    // this focused fixture.
    task->flight_task_cnt_ = 2;
    auto records = std::make_unique<std::vector<FlushRecord>>();
    records->push_back(MakeObjectRecord(/*key=*/401,
                                        "value",
                                        RecordStatus::Normal,
                                        /*commit_ts=*/701,
                                        /*ttl=*/UINT64_MAX,
                                        /*partition_id=*/0));
    records->back().cce_ = reinterpret_cast<LruEntry *>(uintptr_t{0x10});

    FlushDataTask flush_task;
    flush_task.flush_task_entries_["eloqkv_persist_failure"].push_back(
        MakeFlushEntry(task, std::move(records)));

    size_t processed_cc_requests = 0;
    const std::function<void()> yield = [&]
    {
        size_t processed = 0;
        do
        {
            processed = local_shards.ProcessRequests(/*thd_id=*/0);
            processed_cc_requests += processed;
        } while (processed > 0);
        std::this_thread::yield();
    };
    const std::function<void()> resume = [] {};
    const std::function<void()> sync_yield = [] {};

    Sharder &sharder = Sharder::Instance();
    const int64_t saved_leader_term = sharder.LeaderTerm(/*ng_id=*/1);
    const int64_t saved_standby_term = sharder.StandbyNodeTerm();
    // If a regression dispatches the update despite failed persistence, the
    // stale term stops it before dereferencing the sentinel CCE and the
    // request count below still exposes the incorrect publication attempt.
    sharder.SetLeaderTerm(/*ng_id=*/1, /*term=*/-1);
    sharder.SetStandbyNodeTerm(/*standby_term=*/-1);
    local_shards.FlushDataImpl(
        &flush_task, /*worker_idx=*/0, sync_yield, yield, resume);
    sharder.SetLeaderTerm(/*ng_id=*/1, saved_leader_term);
    sharder.SetStandbyNodeTerm(saved_standby_term);

    REQUIRE(processed_cc_requests == 0);
    REQUIRE(task->ckpt_err_ == DataSyncTask::CkptErrorCode::FLUSH_ERROR);
}

TEST_CASE("partition callback reports success and failure exactly once",
          "[checkpoint-flush][partition-callback]")
{
    txservice::CatalogFactory *catalog_factories[3]{nullptr, nullptr, nullptr};
    EloqDS::DataStoreServiceClusterManager cluster_manager;
    EloqDS::DataStoreServiceClient client(/*is_bootstrap=*/false,
                                          catalog_factories,
                                          cluster_manager,
                                          /*bind_data_shard_with_ng=*/false);

    EloqDS::PartitionFlushState partition;
    EloqDS::SyncPutAllData sync;
    EloqDS::PartitionCallbackData callback;
    EloqDS::remote::CommonResult result;

    SECTION("successful partition with no checkpoint entries")
    {
        partition.Reset(/*pid=*/3, /*is_range_partitioned=*/false);
        partition.charged_mem_bytes_ = 17;
        sync.Reset();
        sync.total_partitions_ = 1;
        sync.total_bytes_ = 17;
        callback.Reset(&partition, &sync, "table");
        result.set_error_code(EloqDS::remote::DataStoreError::NO_ERROR);

        EloqDS::PartitionBatchCallback(&callback, nullptr, client, result);

        REQUIRE_FALSE(partition.IsFailed());
        REQUIRE(sync.completed_partitions_ == 1);
        REQUIRE(sync.completed_bytes_ == 17);
    }

    SECTION("failed partition")
    {
        partition.Reset(/*pid=*/4, /*is_range_partitioned=*/false);
        FlushRecord failed_record = MakeObjectRecord(
            9, "failed-payload", RecordStatus::Normal, 109, UINT64_MAX, 0);
        const uint64_t failed_charge = failed_record.FlushSize();
        partition.AddFlushRecord(&failed_record);
        sync.Reset();
        sync.total_partitions_ = 1;
        sync.total_bytes_ = failed_charge;
        callback.Reset(&partition, &sync, "table");
        result.set_error_code(EloqDS::remote::DataStoreError::WRITE_FAILED);
        result.set_error_msg("injected write failure");

        EloqDS::PartitionBatchCallback(&callback, nullptr, client, result);

        REQUIRE(partition.IsFailed());
        REQUIRE(partition.result.error_code() ==
                EloqDS::remote::DataStoreError::WRITE_FAILED);
        REQUIRE(sync.completed_partitions_ == 1);
        REQUIRE(sync.completed_bytes_ == failed_charge);
        REQUIRE(failed_record.Payload() == nullptr);
        REQUIRE(failed_record.Key().KeyPtr() == nullptr);
    }
}

TEST_CASE("partition completion frees record buffers and returns their charge",
          "[checkpoint-flush][partition-memory]")
{
    FlushRecord rec_a = MakeObjectRecord(
        1, "payload-aaaaaaaa", RecordStatus::Normal, 100, UINT64_MAX, 0);
    FlushRecord rec_b = MakeObjectRecord(2,
                                         "payload-bbbbbbbbbbbbbbbb",
                                         RecordStatus::Normal,
                                         101,
                                         UINT64_MAX,
                                         0);
    rec_a.cce_ = reinterpret_cast<LruEntry *>(uintptr_t{0x10});
    rec_a.post_flush_size_ = 77;

    BlobTxRecord blob_payload;
    blob_payload.value_ = "payload-owned-by-flush-record";
    FlushRecord rec_c(TxKey(std::make_unique<EloqStringKey>("3")),
                      blob_payload,
                      RecordStatus::Normal,
                      /*commit_ts=*/102,
                      /*cce=*/nullptr,
                      /*post_flush_size=*/0,
                      /*partition_id=*/0);
    REQUIRE_FALSE(rec_c.HoldsVersionedPayload());

    EloqDS::PartitionFlushState partition;
    partition.Reset(/*pid=*/5, /*is_range_partitioned=*/false);
    const uint64_t expected_charge =
        rec_a.FlushSize() + rec_b.FlushSize() + rec_c.FlushSize();
    REQUIRE(expected_charge > 0);
    partition.AddFlushRecord(&rec_a);
    partition.AddFlushRecord(&rec_b);
    partition.AddFlushRecord(&rec_c);
    REQUIRE(partition.charged_mem_bytes_ == expected_charge);

    // Completion frees the key/payload buffers and returns the exact charge.
    REQUIRE(partition.ReleaseFlushRecordsMemory() == expected_charge);
    REQUIRE(rec_a.Payload() == nullptr);
    REQUIRE(rec_b.Payload() == nullptr);
    REQUIRE(rec_c.Payload() == nullptr);
    REQUIRE(rec_a.Key().KeyPtr() == nullptr);
    REQUIRE(rec_c.Key().KeyPtr() == nullptr);
    // Metadata needed by later stages survives the release.
    REQUIRE(rec_a.cce_ == reinterpret_cast<LruEntry *>(uintptr_t{0x10}));
    REQUIRE(rec_a.commit_ts_ == 100);
    REQUIRE(rec_a.post_flush_size_ == 77);

    // Idempotent: a failure report after a success path frees nothing more
    // and releases no additional quota.
    REQUIRE(partition.ReleaseFlushRecordsMemory() == 0);
    REQUIRE(partition.charged_mem_bytes_ == 0);
}

TEST_CASE(
    "PutAll releases record memory only at a per-partition durability "
    "boundary",
    "[checkpoint-flush][partition-memory][put-all]")
{
    PutAllFixture fixture;
    const TableName table{std::string_view("partition_memory"),
                          TableType::Primary,
                          TableEngine::EloqKv};
    auto records = std::make_unique<std::vector<FlushRecord>>();
    BlobTxRecord blob_payload;
    blob_payload.value_ = "checkpoint-owned-payload";
    records->emplace_back(TxKey(std::make_unique<EloqStringKey>("memory-key")),
                          blob_payload,
                          RecordStatus::Normal,
                          /*commit_ts=*/200,
                          /*cce=*/nullptr,
                          /*post_flush_size=*/0,
                          /*partition_id=*/0);
    FlushRecord *record = &records->front();
    const uint64_t charge = record->FlushSize();

    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        flush_task;
    flush_task["eloqkv_partition_memory"].push_back(
        MakeFlushEntry(MakeTaskPtr(table, /*term=*/1), std::move(records)));

    std::vector<std::pair<uint64_t, uint64_t>> progress;
    const std::function<void(uint64_t, uint64_t)> report_progress =
        [&](uint64_t done, uint64_t total)
    { progress.emplace_back(done, total); };

    // Match FlushDataImpl: RocksDB-backed stores do not install partition
    // progress because PutAll is not their durability boundary.
    const auto *progress_fptr =
        fixture.Client().NeedPersistKV() ? nullptr : &report_progress;
    REQUIRE(fixture.Client().PutAll(flush_task,
                                    /*yield_fptr=*/nullptr,
                                    /*resume_fptr=*/nullptr,
                                    /*sync_yield_fptr=*/nullptr,
                                    progress_fptr));

    if (fixture.Client().NeedPersistKV())
    {
        // RocksDB-backed stores cannot report partition durability before
        // PersistKV, so FlushDataImpl keeps the callback disabled and the
        // record buffers alive.
        REQUIRE(progress.empty());
        REQUIRE(record->Payload() != nullptr);
        REQUIRE(record->Key().KeyPtr() != nullptr);
    }
    else
    {
        // EloqStore makes the partition durable in BatchWriteRecords, so the
        // callback frees the owned record buffers and reports their exact
        // charge before PutAll returns.
        REQUIRE(progress ==
                std::vector<std::pair<uint64_t, uint64_t>>{{charge, charge}});
        REQUIRE(record->Payload() == nullptr);
        REQUIRE(record->Key().KeyPtr() == nullptr);
    }
}

TEST_CASE("PutAll discards lower-term records from a merged table batch",
          "[checkpoint-flush][partition-term][put-all]")
{
    PutAllFixture fixture;

    SECTION("hash partition")
    {
        const TableName table{std::string_view("hash_terms"),
                              TableType::Primary,
                              TableEngine::EloqKv};
        auto old_before = MakeTaskPtr(table, 30);
        auto newest = MakeTaskPtr(table, 31);
        auto old_after = MakeTaskPtr(table, 30);
        auto same_term = MakeTaskPtr(table, 31);
        auto old_only_partition = MakeTaskPtr(table, 30);
        auto other_node_group = MakeTaskPtr(table, 10, /*node_group_id=*/2);

        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            flush_task;
        auto &entries = flush_task["eloqkv_hash_terms"];
        auto add_record = [&](std::shared_ptr<DataSyncTask> task,
                              int key,
                              int32_t partition_id = 0)
        {
            auto records = std::make_unique<std::vector<FlushRecord>>();
            records->push_back(MakeObjectRecord(key,
                                                "value",
                                                RecordStatus::Normal,
                                                /*commit_ts=*/100 + key,
                                                /*ttl=*/UINT64_MAX,
                                                partition_id));
            entries.push_back(
                MakeFlushEntry(std::move(task), std::move(records)));
        };
        add_record(old_before, 1);
        add_record(newest, 2);
        add_record(old_after, 3);
        add_record(same_term, 4);
        add_record(old_only_partition, 5, /*partition_id=*/1);
        add_record(other_node_group, 6, /*partition_id=*/2);

        const TableName stale_other_table{
            std::string_view("hash_terms_stale_table"),
            TableType::Primary,
            TableEngine::EloqKv};
        auto stale_other_records = std::make_unique<std::vector<FlushRecord>>();
        stale_other_records->push_back(MakeObjectRecord(7,
                                                        "stale-table-value",
                                                        RecordStatus::Normal,
                                                        /*commit_ts=*/107,
                                                        /*ttl=*/UINT64_MAX,
                                                        /*partition_id=*/3));
        flush_task["eloqkv_hash_terms_stale_table"].push_back(
            MakeFlushEntry(MakeTaskPtr(stale_other_table, /*term=*/30),
                           std::move(stale_other_records)));

        REQUIRE(fixture.Client().PutAll(flush_task));
        REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 3);
        uint64_t record_ts = 0;
        REQUIRE(
            ReadKey(
                fixture.Service(), "eloqkv_hash_terms", 0, "1", record_ts) ==
            EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(
            ReadKey(
                fixture.Service(), "eloqkv_hash_terms", 0, "2", record_ts) ==
            EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 102);
        REQUIRE(
            ReadKey(
                fixture.Service(), "eloqkv_hash_terms", 0, "3", record_ts) ==
            EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(
            ReadKey(
                fixture.Service(), "eloqkv_hash_terms", 0, "4", record_ts) ==
            EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 104);
        REQUIRE(
            ReadKey(
                fixture.Service(), "eloqkv_hash_terms", 1, "5", record_ts) ==
            EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(
            ReadKey(
                fixture.Service(), "eloqkv_hash_terms", 2, "6", record_ts) ==
            EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 106);
        REQUIRE(ReadKey(fixture.Service(),
                        "eloqkv_hash_terms_stale_table",
                        3,
                        "7",
                        record_ts) ==
                EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
    }

    SECTION("range partition")
    {
        const TableName table{std::string_view("range_terms"),
                              TableType::Primary,
                              TableEngine::InternalRange};
        auto old_before = MakeTaskPtr(table, 40);
        auto newest = MakeTaskPtr(table, 41);
        auto old_after = MakeTaskPtr(table, 40);
        auto same_term = MakeTaskPtr(table, 41);
        auto old_only_partition = MakeTaskPtr(table, 40);
        auto other_node_group = MakeTaskPtr(table, 7, /*node_group_id=*/2);

        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            flush_task;
        auto &entries = flush_task["irange_range_terms"];
        auto add_record = [&](std::shared_ptr<DataSyncTask> task,
                              int key,
                              int32_t partition_id = 5)
        {
            auto records = std::make_unique<std::vector<FlushRecord>>();
            records->push_back(MakeSerializedRecord(key,
                                                    "value",
                                                    RecordStatus::Normal,
                                                    /*commit_ts=*/200 + key,
                                                    partition_id));
            entries.push_back(
                MakeFlushEntry(std::move(task), std::move(records)));
        };
        add_record(old_before, 10);
        add_record(newest, 11);
        add_record(old_after, 12);
        add_record(same_term, 13);
        add_record(old_only_partition, 14, /*partition_id=*/6);
        add_record(other_node_group, 15, /*partition_id=*/7);

        REQUIRE(fixture.Client().PutAll(flush_task));
        REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 3);
        uint64_t record_ts = 0;
        REQUIRE(
            ReadKey(
                fixture.Service(), "irange_range_terms", 5, "10", record_ts) ==
            EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(
            ReadKey(
                fixture.Service(), "irange_range_terms", 5, "11", record_ts) ==
            EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 211);
        REQUIRE(
            ReadKey(
                fixture.Service(), "irange_range_terms", 5, "12", record_ts) ==
            EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(
            ReadKey(
                fixture.Service(), "irange_range_terms", 5, "13", record_ts) ==
            EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 213);
        REQUIRE(
            ReadKey(
                fixture.Service(), "irange_range_terms", 6, "14", record_ts) ==
            EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
        REQUIRE(
            ReadKey(
                fixture.Service(), "irange_range_terms", 7, "15", record_ts) ==
            EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 215);
    }

    SECTION("newest task may have no base records")
    {
        const TableName old_table{std::string_view("old_nonempty"),
                                  TableType::Primary,
                                  TableEngine::EloqKv};
        auto old_records = std::make_unique<std::vector<FlushRecord>>();
        old_records->push_back(MakeObjectRecord(/*key=*/80,
                                                "obsolete",
                                                RecordStatus::Normal,
                                                /*commit_ts=*/480,
                                                /*ttl=*/UINT64_MAX,
                                                /*partition_id=*/0));

        const TableName newest_table{std::string_view("newest_empty"),
                                     TableType::Primary,
                                     TableEngine::EloqKv};
        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            flush_task;
        flush_task["eloqkv_old_nonempty"].push_back(MakeFlushEntry(
            MakeTaskPtr(old_table, /*term=*/80), std::move(old_records)));
        flush_task["eloqkv_newest_empty"].push_back(
            MakeFlushEntry(MakeTaskPtr(newest_table, /*term=*/81),
                           std::make_unique<std::vector<FlushRecord>>()));

        REQUIRE(fixture.Client().PutAll(flush_task));
        REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 0);
        uint64_t record_ts = 0;
        REQUIRE(ReadKey(fixture.Service(),
                        "eloqkv_old_nonempty",
                        /*partition_id=*/0,
                        "80",
                        record_ts) ==
                EloqDS::remote::DataStoreError::KEY_NOT_FOUND);
    }
}

TEST_CASE("PutAll handles empty entry slots and object deletes",
          "[checkpoint-flush][put-all]")
{
    PutAllFixture fixture;
    const TableName table{
        std::string_view("objects"), TableType::Primary, TableEngine::EloqKv};
    const std::string kv_table_name = "eloqkv_objects";
    auto empty_task = MakeTaskPtr(table, 21);
    auto data_task = MakeTaskPtr(table, 21);

    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        flush_task;
    flush_task.try_emplace("empty_table");
    auto &entries = flush_task[kv_table_name];
    entries.push_back(MakeFlushEntry(
        empty_task, std::make_unique<std::vector<FlushRecord>>()));

    auto records = std::make_unique<std::vector<FlushRecord>>();
    records->push_back(MakeObjectRecord(1,
                                        "live",
                                        RecordStatus::Normal,
                                        /*commit_ts=*/101,
                                        /*ttl=*/UINT64_MAX,
                                        /*partition_id=*/0));
    records->push_back(MakeObjectRecord(2,
                                        "expired",
                                        RecordStatus::Normal,
                                        /*commit_ts=*/102,
                                        /*ttl=*/0,
                                        /*partition_id=*/0));
    records->push_back(MakeObjectRecord(3,
                                        "",
                                        RecordStatus::Deleted,
                                        /*commit_ts=*/103,
                                        /*ttl=*/0,
                                        /*partition_id=*/0));
    entries.push_back(MakeFlushEntry(data_task, std::move(records)));

    REQUIRE(fixture.Client().PutAll(flush_task));
    REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 1);

    const TableName internal_table{std::string_view("internal"),
                                   TableType::Primary,
                                   TableEngine::InternalHash};
    auto internal_task = MakeTaskPtr(internal_table, 22);
    auto internal_records = std::make_unique<std::vector<FlushRecord>>();
    internal_records->push_back(MakeSerializedRecord(10,
                                                     "serialized",
                                                     RecordStatus::Normal,
                                                     /*commit_ts=*/104,
                                                     /*partition_id=*/1));
    // Non-object hash tables retain tombstones as encoded PUTs with a retired
    // TTL, rather than issuing a physical DELETE.
    internal_records->push_back(MakeSerializedRecord(11,
                                                     "",
                                                     RecordStatus::Deleted,
                                                     /*commit_ts=*/105,
                                                     /*partition_id=*/1));
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        internal_flush;
    internal_flush["internal"].push_back(
        MakeFlushEntry(internal_task, std::move(internal_records)));

    REQUIRE(fixture.Client().PutAll(internal_flush));
    REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 3);
}

TEST_CASE("PutAll chains partition batches past the 64 MiB boundary",
          "[checkpoint-flush][put-all][batch-rollover]")
{
    PutAllFixture fixture;
    constexpr size_t kBatchBoundary = 64ULL * 1024 * 1024;

    SECTION("hash partition")
    {
        const TableName table{std::string_view("hash_batch_rollover"),
                              TableType::Primary,
                              TableEngine::EloqKv};
        auto records = std::make_unique<std::vector<FlushRecord>>();
        records->push_back(MakeObjectRecord(/*key=*/100,
                                            std::string(kBatchBoundary, 'h'),
                                            RecordStatus::Normal,
                                            /*commit_ts=*/600,
                                            /*ttl=*/UINT64_MAX,
                                            /*partition_id=*/0));
        // Batch selection happens before serializing the current record. This
        // second record therefore closes the oversized first batch and must be
        // sent by the callback chain as a separate request.
        records->push_back(MakeObjectRecord(/*key=*/101,
                                            "tail",
                                            RecordStatus::Normal,
                                            /*commit_ts=*/601,
                                            /*ttl=*/UINT64_MAX,
                                            /*partition_id=*/0));

        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            flush_task;
        flush_task["eloqkv_hash_batch_rollover"].push_back(MakeFlushEntry(
            MakeTaskPtr(table, /*term=*/90), std::move(records)));

        REQUIRE(fixture.Client().PutAll(flush_task));
        uint64_t record_ts = 0;
        REQUIRE(ReadKey(fixture.Service(),
                        "eloqkv_hash_batch_rollover",
                        /*partition_id=*/0,
                        "100",
                        record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 600);
        REQUIRE(ReadKey(fixture.Service(),
                        "eloqkv_hash_batch_rollover",
                        /*partition_id=*/0,
                        "101",
                        record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 601);
    }

    SECTION("range partition")
    {
        const TableName table{std::string_view("range_batch_rollover"),
                              TableType::Primary,
                              TableEngine::InternalRange};
        auto records = std::make_unique<std::vector<FlushRecord>>();
        records->push_back(
            MakeSerializedRecord(/*key=*/110,
                                 std::string(kBatchBoundary, 'r'),
                                 RecordStatus::Normal,
                                 /*commit_ts=*/610,
                                 /*partition_id=*/9));
        records->push_back(MakeSerializedRecord(/*key=*/111,
                                                "tail",
                                                RecordStatus::Normal,
                                                /*commit_ts=*/611,
                                                /*partition_id=*/9));

        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            flush_task;
        flush_task["irange_range_batch_rollover"].push_back(MakeFlushEntry(
            MakeTaskPtr(table, /*term=*/91), std::move(records)));

        REQUIRE(fixture.Client().PutAll(flush_task));
        uint64_t record_ts = 0;
        REQUIRE(ReadKey(fixture.Service(),
                        "irange_range_batch_rollover",
                        /*partition_id=*/9,
                        "110",
                        record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 610);
        REQUIRE(ReadKey(fixture.Service(),
                        "irange_range_batch_rollover",
                        /*partition_id=*/9,
                        "111",
                        record_ts) == EloqDS::remote::DataStoreError::NO_ERROR);
        REQUIRE(record_ts == 611);
    }
}

TEST_CASE("archive flush discards every lower-term task before datastore IO",
          "[checkpoint-flush][partition-term][archive]")
{
    PutAllFixture fixture;
    const TableName table{std::string_view("archive_terms"),
                          TableType::Primary,
                          TableEngine::InternalHash};

    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        flush_task;
    flush_task.try_emplace("empty_archive_bucket");
    auto &entries = flush_task["ihash_archive_terms"];
    auto add_record = [&](int64_t term,
                          NodeGroupId node_group_id,
                          int key,
                          int32_t partition_id)
    {
        auto records = std::make_unique<std::vector<FlushRecord>>();
        records->push_back(MakeSerializedRecord(key,
                                                "archive-value",
                                                RecordStatus::Normal,
                                                /*commit_ts=*/300 + key,
                                                partition_id));
        entries.push_back(MakeArchiveEntry(
            MakeTaskPtr(table, term, node_group_id), std::move(records)));
    };

    add_record(/*term=*/50, /*node_group_id=*/1, /*key=*/20, /*pid=*/0);
    add_record(/*term=*/51, /*node_group_id=*/1, /*key=*/21, /*pid=*/0);
    add_record(/*term=*/50, /*node_group_id=*/1, /*key=*/22, /*pid=*/0);
    add_record(/*term=*/51, /*node_group_id=*/1, /*key=*/23, /*pid=*/0);
    // An older-only source partition is still obsolete because selection is
    // across the whole node-group batch, not per source or archive partition.
    add_record(/*term=*/50, /*node_group_id=*/1, /*key=*/24, /*pid=*/1);
    // Terms are scoped to a node group, so this lower numeric term is valid.
    add_record(/*term=*/8, /*node_group_id=*/2, /*key=*/25, /*pid=*/2);

    const TableName stale_other_table{
        std::string_view("archive_terms_stale_table"),
        TableType::Primary,
        TableEngine::InternalHash};
    auto stale_other_records = std::make_unique<std::vector<FlushRecord>>();
    stale_other_records->push_back(MakeSerializedRecord(26,
                                                        "stale-archive-value",
                                                        RecordStatus::Normal,
                                                        /*commit_ts=*/326,
                                                        /*partition_id=*/3));
    flush_task["ihash_archive_terms_stale_table"].push_back(
        MakeArchiveEntry(MakeTaskPtr(stale_other_table, /*term=*/50),
                         std::move(stale_other_records)));

    REQUIRE(fixture.Client().PutArchivesAll(flush_task));
    REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 3);
}

TEST_CASE("base-to-archive copy does not read lower-term task keys",
          "[checkpoint-flush][partition-term][archive]")
{
    PutAllFixture fixture;
    const TableName table{std::string_view("copy_terms"),
                          TableType::Primary,
                          TableEngine::InternalHash};

    auto old_bases = std::make_unique<std::vector<std::pair<TxKey, int32_t>>>();
    old_bases->emplace_back(TxKey(std::make_unique<EloqStringKey>("obsolete")),
                            0);

    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        flush_task;
    flush_task.try_emplace("empty_copy_bucket");
    flush_task["ihash_copy_terms"].push_back(
        MakeMvBaseEntry(MakeTaskPtr(table, /*term=*/60), std::move(old_bases)));
    const TableName newer_table{std::string_view("copy_terms_newer_table"),
                                TableType::Primary,
                                TableEngine::InternalHash};
    flush_task["ihash_copy_terms_newer_table"].push_back(MakeMvBaseEntry(
        MakeTaskPtr(newer_table, /*term=*/61),
        std::make_unique<std::vector<std::pair<TxKey, int32_t>>>()));

    REQUIRE(fixture.Client().CopyBaseToArchive(flush_task));
    REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 0);
}

TEST_CASE("base-to-archive copy retains only the newest task metadata",
          "[checkpoint-flush][partition-term][archive]")
{
    PutAllFixture fixture;
    const TableName table{std::string_view("copy_retained"),
                          TableType::Primary,
                          TableEngine::EloqSql};
    const std::string_view kv_table_name = "eloqsql_copy_retained";

    auto seed_records = std::make_unique<std::vector<FlushRecord>>();
    seed_records->push_back(MakeSerializedRecord(/*key=*/70,
                                                 "base-version",
                                                 RecordStatus::Normal,
                                                 /*commit_ts=*/370,
                                                 /*partition_id=*/4));
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        seed;
    seed[kv_table_name].push_back(MakeFlushEntry(
        MakeTaskPtr(table, /*term=*/70), std::move(seed_records)));
    REQUIRE(fixture.Client().PutAll(seed));
    REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 1);

    auto obsolete_bases =
        std::make_unique<std::vector<std::pair<TxKey, int32_t>>>();
    obsolete_bases->emplace_back(
        TxKey(std::make_unique<EloqStringKey>("must-not-be-read")), 4);
    auto newest_bases =
        std::make_unique<std::vector<std::pair<TxKey, int32_t>>>();
    newest_bases->emplace_back(TxKey(std::make_unique<EloqStringKey>("70")), 4);

    auto old_task = MakeTaskPtr(table, /*term=*/69);
    auto newest_task = MakeTaskPtr(table, /*term=*/70);
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        flush_task;
    auto &entries = flush_task[kv_table_name];
    entries.push_back(MakeMvBaseEntry(old_task, std::move(obsolete_bases)));
    entries.push_back(MakeMvBaseEntry(newest_task, std::move(newest_bases)));

    REQUIRE(fixture.Client().CopyBaseToArchive(flush_task));
    // The base record remains and exactly one newest-term archive version is
    // added. Reading the obsolete key would synthesize a second tombstone.
    REQUIRE(fixture.Service().GetApproxStoreKeyCount(/*shard_id=*/0) == 2);
}

TEST_CASE("live shard dispatch covers checkpoint and intrusive wait lists",
          "[checkpoint-flush][partition-dispatch][cc-wait-list]")
{
    Watchdog watchdog(20s);
    txservice::test::TestNode node(
        txservice::test::TestNodeOptions{}.CoreNum(4).EnableMvcc(false));

    // On EloqStore builds this exercises PutAll's live hash/range collection
    // path. The deliberately stale node group makes UpdateCceCkptTsCc reject
    // the request before dereferencing the sentinel CCE addresses. On
    // RocksDB-backed builds publication is deferred and the same input checks
    // that PutAll does not retain or touch those addresses.
    {
        PutAllFixture fixture;
        auto sentinel_cce = reinterpret_cast<LruEntry *>(uintptr_t{1});
        const TableName hash_table{std::string_view("collect_hash"),
                                   TableType::Primary,
                                   TableEngine::EloqKv};
        auto hash_records = std::make_unique<std::vector<FlushRecord>>();
        hash_records->push_back(MakeObjectRecord(/*key=*/90,
                                                 "hash",
                                                 RecordStatus::Normal,
                                                 /*commit_ts=*/590,
                                                 /*ttl=*/UINT64_MAX,
                                                 /*partition_id=*/0));
        hash_records->back().cce_ = sentinel_cce;
        hash_records->back().post_flush_size_ = 11;

        const TableName range_table{std::string_view("collect_range"),
                                    TableType::Primary,
                                    TableEngine::InternalRange};
        auto range_records = std::make_unique<std::vector<FlushRecord>>();
        range_records->push_back(MakeSerializedRecord(/*key=*/91,
                                                      "range",
                                                      RecordStatus::Normal,
                                                      /*commit_ts=*/591,
                                                      /*partition_id=*/1));
        range_records->back().cce_ = sentinel_cce;
        range_records->back().post_flush_size_ = 12;

        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            flush_task;
        flush_task["eloqkv_collect_hash"].push_back(MakeFlushEntry(
            MakeTaskPtr(hash_table, /*term=*/999), std::move(hash_records)));
        flush_task["irange_collect_range"].push_back(MakeFlushEntry(
            MakeTaskPtr(range_table, /*term=*/999), std::move(range_records)));
        const std::function<void(uint64_t, uint64_t)> report_progress =
            [](uint64_t, uint64_t) {};

        REQUIRE(fixture.Client().PutAll(flush_task,
                                        /*yield_fptr=*/nullptr,
                                        /*resume_fptr=*/nullptr,
                                        /*sync_yield_fptr=*/nullptr,
                                        &report_progress));
    }

    const TableName table{
        std::string_view("dispatch"), TableType::Primary, TableEngine::EloqKv};
    DataSyncTask stale_task = MakeTask(table, /*term=*/1);

    EloqDS::PartitionFlushState partition;
    partition.Reset(/*pid=*/0, /*is_range_partitioned=*/false);
    partition.charged_mem_bytes_ = 31;
    partition.AddCkptTsEntry(
        &stale_task, /*core_idx=*/0, nullptr, /*commit_ts=*/10, 0);
    partition.AddCkptTsEntry(
        &stale_task, /*core_idx=*/1, nullptr, /*commit_ts=*/11, 0);

    EloqDS::SyncPutAllData sync;
    sync.Reset();
    sync.total_partitions_ = 1;
    sync.total_bytes_ = 31;
    partition.ArmCkptTsUpdate(&sync);

    EloqDS::PartitionCallbackData callback;
    callback.Reset(&partition, &sync, "eloqkv_dispatch");
    EloqDS::remote::CommonResult result;
    result.set_error_code(EloqDS::remote::DataStoreError::NO_ERROR);

    txservice::CatalogFactory *catalog_factories[3]{nullptr, nullptr, nullptr};
    EloqDS::DataStoreServiceClusterManager cluster_manager;
    EloqDS::DataStoreServiceClient client(/*is_bootstrap=*/false,
                                          catalog_factories,
                                          cluster_manager,
                                          /*bind_data_shard_with_ng=*/false);
    EloqDS::PartitionBatchCallback(&callback, nullptr, client, result);

    sync.Wait();
    REQUIRE(partition.ckpt_ts_update_->IsFinished());
    REQUIRE(sync.completed_partitions_ == 1);
    REQUIRE(sync.completed_bytes_ == 31);

    auto *shards = Sharder::Instance().GetLocalCcShards();
    REQUIRE(shards != nullptr);
    CcShard *shard = shards->GetCcShard(0);
    REQUIRE(shard != nullptr);

    ShardRequest abort_first(/*abort_if_oom=*/true);
    ShardRequest retained;
    ShardRequest abort_last(/*abort_if_oom=*/true);
    abort_first.Use();
    retained.Use();
    abort_last.Use();
    shard->EnqueueWaitListIfMemoryFull(&abort_first);
    shard->EnqueueWaitListIfMemoryFull(&retained);
    shard->EnqueueWaitListIfMemoryFull(&abort_last);
    REQUIRE(shard->WaitListSizeForMemory() == 3);

    shard->AbortRequestsAfterMemoryFree();
    REQUIRE(abort_first.AbortError() == CcErrorCode::OUT_OF_MEMORY);
    REQUIRE(abort_last.AbortError() == CcErrorCode::OUT_OF_MEMORY);
    REQUIRE(shard->WaitListSizeForMemory() == 1);
    REQUIRE(shard->DequeueWaitListAfterMemoryFree(/*deque_all=*/true));

    ShardRequest schema_waiter;
    schema_waiter.Use();
    shard->EnqueueWaitListIfSchemaMismatch(&schema_waiter);
    shard->DequeueWaitListAfterSchemaUpdated();

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (
        (retained.ExecuteCount() != 1 || schema_waiter.ExecuteCount() != 1) &&
        std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(1ms);
    }
    REQUIRE(retained.ExecuteCount() == 1);
    REQUIRE(schema_waiter.ExecuteCount() == 1);
    REQUIRE_FALSE(retained.InUse());
    REQUIRE_FALSE(schema_waiter.InUse());

    // ApplyCc used to bypass the base Free() state transition for remote
    // requests, leaving a recycled request permanently marked in use.
    ApplyCc remote_apply(/*is_local=*/false);
    remote_apply.Use();
    remote_apply.Free();
    REQUIRE_FALSE(remote_apply.InUse());
}
