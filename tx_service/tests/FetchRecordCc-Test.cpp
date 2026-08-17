/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify it
 *    under either GNU Affero General Public License v3 or GNU General Public
 *    License v2.
 */

#include <algorithm>
#include <catch2/catch_all.hpp>
#include <map>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

// This test verifies the ownership boundary between CcShard's active-fetch
// index and its FetchRecordCc pool.
#define protected public
#define private public
#include "cc/cc_entry.h"
#include "cc/cc_req_misc.h"
#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "cc/template_cc_map.h"
#include "data_store_service_client.h"
#include "include/mock/mock_catalog_factory.h"
#include "sharder.h"
#undef private
#undef protected

namespace txservice
{
namespace
{
using TestKey = CompositeKey<std::string, int>;
using TestRecord = CompositeRecord<int>;
using TestCcMap = TemplateCcMap<TestKey,
                                TestRecord,
                                /*Versioned=*/true,
                                /*RangePartitioned=*/true>;
using TestCcEntry = CcEntry<TestKey, TestRecord, true, true>;

MockCatalogFactory mock_catalog_factory;

struct FetchRecordFixture
{
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
        {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
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
        {"rep_group_cnt", 1},
    };
    CatalogFactory *catalog_factory[5] = {
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
    };
    LocalCcShards local_cc_shards;
    CcShard shard;
    std::string raft_path;

    explicit FetchRecordFixture(store::DataStoreHandler *store_hd = nullptr)
        : local_cc_shards(0,
                          0,
                          tx_cnf,
                          catalog_factory,
                          nullptr,
                          &ng_configs,
                          2,
                          store_hd,
                          nullptr,
                          true),
          shard(0,
                1,
                10000,
                false,
                0,
                local_cc_shards,
                catalog_factory,
                nullptr,
                &ng_configs,
                2)
    {
        local_cc_shards.BindThreadToFastMetaDataShard(0);
        shard.Init();
        Sharder::Instance(0,
                          &ng_configs,
                          0,
                          nullptr,
                          nullptr,
                          &local_cc_shards,
                          nullptr,
                          &raft_path);
        // Sharder::Instance's legacy arguments are ignored; lightweight shard
        // tests wire the state they exercise instead of running
        // Sharder::Init().
        Sharder &sharder = Sharder::Instance();
        sharder.node_id_ = 0;
        sharder.native_ng_ = 0;
        sharder.local_shards_ = &local_cc_shards;
        sharder.ng_leader_cache_[0].store(0, std::memory_order_relaxed);
        sharder.ng_leader_term_cache_[0].store(-1, std::memory_order_relaxed);
        sharder.leader_term_cache_[0].store(-1, std::memory_order_relaxed);
        sharder.candidate_leader_term_cache_[0].store(
            -1, std::memory_order_relaxed);
        sharder.standby_node_term_cache_.store(-1, std::memory_order_relaxed);
        sharder.candidate_standby_node_term_cache_.store(
            -1, std::memory_order_relaxed);
    }

    ~FetchRecordFixture()
    {
        local_cc_shards.Terminate();
        Sharder::Instance().local_shards_ = nullptr;
    }
};

class ReentrantRequester : public CcRequestBase
{
public:
    ReentrantRequester(LruEntry *cce, FetchRecordCc *finishing_fetch)
        : cce_(cce), finishing_fetch_(finishing_fetch)
    {
    }

    bool Execute(CcShard &ccs) override
    {
        ++execute_count_;
        active_fetch_removed_ = !ccs.fetch_record_reqs_.contains(cce_);

        // Force the pool scan to start at the finishing request. It must skip
        // that object because Execute() has not returned to the dispatcher yet.
        ccs.fetch_record_cc_pool_.head_ = 0;
        FetchRecordCc *nested_fetch = ccs.fetch_record_cc_pool_.NextRequest();
        finishing_fetch_not_reused_ = nested_fetch != finishing_fetch_;
        nested_fetch->Free();

        // Mirror a requester resumed after FetchRecord: CcShard::FetchRecord
        // added this pin on its behalf.
        cce_->GetKeyGapLockAndExtraData()->ReleasePin();
        cce_->RecycleKeyLock(ccs);
        return true;
    }

    int execute_count_{0};
    bool active_fetch_removed_{false};
    bool finishing_fetch_not_reused_{false};

private:
    LruEntry *cce_;
    FetchRecordCc *finishing_fetch_;
};

class AbortTrackingRequester : public CcRequestBase
{
public:
    bool Execute(CcShard &) override
    {
        ++execute_count_;
        return true;
    }

    void AbortCcRequest(CcErrorCode error) override
    {
        abort_error_ = error;
        Free();
    }

    int execute_count_{0};
    CcErrorCode abort_error_{CcErrorCode::NO_ERROR};
};

class StubStoreHandler : public EloqDS::DataStoreServiceClient
{
public:
    StubStoreHandler(
        CatalogFactory *catalog_factories[3],
        const EloqDS::DataStoreServiceClusterManager &cluster_manager,
        store::DataStoreHandler::DataStoreOpStatus result)
        : DataStoreServiceClient(
              false, catalog_factories, cluster_manager, false),
          result_(result)
    {
    }

    store::DataStoreHandler::DataStoreOpStatus FetchRecord(
        FetchRecordCc *fetch_record_cc, FetchSnapshotCc *) override
    {
        ++fetch_count_;
        last_fetch_ = fetch_record_cc;
        return result_;
    }

    store::DataStoreHandler::DataStoreOpStatus result_;
    size_t fetch_count_{0};
    FetchRecordCc *last_fetch_{nullptr};
};

class QueuedRequester : public CcRequestBase
{
public:
    explicit QueuedRequester(LruEntry *cce) : cce_(cce)
    {
    }

    bool Execute(CcShard &ccs) override
    {
        ++execute_count_;
        active_fetch_present_ = ccs.fetch_record_reqs_.contains(cce_);
        cce_->GetKeyGapLockAndExtraData()->ReleasePin();
        cce_->RecycleKeyLock(ccs);
        return true;
    }

    size_t execute_count_{0};
    bool active_fetch_present_{false};

private:
    LruEntry *cce_;
};

int64_t CurrentTerm()
{
    return std::max({Sharder::Instance().CandidateLeaderTerm(0),
                     Sharder::Instance().LeaderTerm(0),
                     Sharder::Instance().StandbyNodeTerm()});
}
}  // namespace

TEST_CASE("FetchRecordCc completion follows pooled request ownership",
          "[fetch-record][pool]")
{
    FetchRecordFixture fixture;
    const TableName table{std::string_view("fetch_record_pool"),
                          TableType::Primary,
                          TableEngine::EloqSql};
    TestCcMap cc_map(&fixture.shard, 0, table, 1, nullptr, true);

    TestKey key = std::make_tuple(std::string("key"), 1);
    bool emplaced = false;
    auto it = cc_map.FindEmplace(key, &emplaced, false, false);
    REQUIRE(emplaced);
    auto *cce = static_cast<TestCcEntry *>(it->second);
    REQUIRE(cce != nullptr);

    cce->GetOrCreateKeyLock(&fixture.shard, &cc_map, it.GetPage());
    KeyGapLockAndExtraData *lock = cce->GetKeyGapLockAndExtraData();
    REQUIRE(lock != nullptr);
    // BackFill releases the pin held by the active datastore operation.
    lock->AddPin();
    // The resumed requester owns a second pin until its Execute call.
    lock->AddPin();

    FetchRecordCc *fetch = fixture.shard.fetch_record_cc_pool_.NextRequest();
    REQUIRE(fetch->InUse());
    fetch->ccs_ = &fixture.shard;
    fetch->cc_ng_id_ = 0;
    fetch->cc_ng_term_ = CurrentTerm();
    fetch->cce_ = cce;
    fetch->lock_ = lock;
    fetch->rec_ts_ = 2;
    fetch->rec_status_ = RecordStatus::Deleted;
    fetch->error_code_ = 0;
    fetch->only_fetch_archives_ = false;

    ReentrantRequester requester(cce, fetch);
    requester.Use();
    fetch->AddRequester(&requester);
    fixture.shard.fetch_record_reqs_.try_emplace(cce, fetch);

    fixture.shard.Enqueue(fetch);
    const size_t processed = fixture.shard.ProcessRequests();

    REQUIRE(processed == 1);
    REQUIRE(requester.execute_count_ == 1);
    REQUIRE(requester.active_fetch_removed_);
    REQUIRE(requester.finishing_fetch_not_reused_);
    REQUIRE_FALSE(requester.InUse());
    REQUIRE_FALSE(fixture.shard.fetch_record_reqs_.contains(cce));
    // ProcessRequests owns the final Free() after Execute() returns true.
    REQUIRE_FALSE(fetch->InUse());
}

TEST_CASE("FetchRecordCc term-change completion is recycled by the dispatcher",
          "[fetch-record][pool]")
{
    FetchRecordFixture fixture;
    const TableName table{std::string_view("fetch_record_term_change"),
                          TableType::Primary,
                          TableEngine::EloqSql};
    TestCcMap cc_map(&fixture.shard, 0, table, 1, nullptr, true);

    TestKey key = std::make_tuple(std::string("key"), 1);
    bool emplaced = false;
    auto it = cc_map.FindEmplace(key, &emplaced, false, false);
    REQUIRE(emplaced);
    auto *cce = static_cast<TestCcEntry *>(it->second);
    REQUIRE(cce != nullptr);

    FetchRecordCc *fetch = fixture.shard.fetch_record_cc_pool_.NextRequest();
    fetch->ccs_ = &fixture.shard;
    fetch->cc_ng_id_ = 0;
    fetch->cc_ng_term_ = CurrentTerm() + 1;
    fetch->cce_ = cce;

    AbortTrackingRequester requester;
    requester.Use();
    fetch->AddRequester(&requester);
    fixture.shard.fetch_record_reqs_.try_emplace(cce, fetch);

    fixture.shard.Enqueue(fetch);
    REQUIRE(fixture.shard.ProcessRequests() == 1);

    REQUIRE(requester.abort_error_ == CcErrorCode::NG_TERM_CHANGED);
    REQUIRE_FALSE(requester.InUse());
    REQUIRE_FALSE(fixture.shard.fetch_record_reqs_.contains(cce));
    REQUIRE_FALSE(fetch->InUse());
}

TEST_CASE("FetchRecordCc abandoned before enqueue is freed explicitly",
          "[fetch-record][pool]")
{
    FetchRecordFixture fixture;
    const TableName table{std::string_view("fetch_record_start_failure"),
                          TableType::Primary,
                          TableEngine::EloqSql};
    MockTableSchema schema(table, "", 1);
    TestCcMap cc_map(&fixture.shard, 0, table, 1, &schema, true);

    TestKey key = std::make_tuple(std::string("key"), 1);
    bool emplaced = false;
    auto it = cc_map.FindEmplace(key, &emplaced, false, false);
    REQUIRE(emplaced);
    auto *cce = static_cast<TestCcEntry *>(it->second);
    REQUIRE(cce != nullptr);
    cce->GetOrCreateKeyLock(&fixture.shard, &cc_map, it.GetPage());

    fixture.shard.fetch_record_cc_pool_.head_ = 0;
    FetchRecordCc *first_pool_entry =
        fixture.shard.fetch_record_cc_pool_.pool_.front().get();
    AbortTrackingRequester requester;
    requester.Use();

    const auto result =
        fixture.shard.FetchRecord(table,
                                  &schema,
                                  TxKey(std::make_unique<TestKey>(key)),
                                  cce,
                                  0,
                                  CurrentTerm(),
                                  &requester,
                                  0,
                                  true);

    REQUIRE(result == store::DataStoreHandler::DataStoreOpStatus::Retry);
    REQUIRE_FALSE(fixture.shard.fetch_record_reqs_.contains(cce));
    REQUIRE_FALSE(first_pool_entry->InUse());
    // The failed start happened before FetchRecord took a requester pin or
    // transferred requester ownership to the shard queue.
    REQUIRE(requester.InUse());
    requester.Free();
}

TEST_CASE("FetchRecordCc datastore start retry frees the pooled request",
          "[fetch-record][pool]")
{
    CatalogFactory *catalog_factories[3] = {
        &mock_catalog_factory, &mock_catalog_factory, &mock_catalog_factory};
    EloqDS::DataStoreServiceClusterManager cluster_manager;
    StubStoreHandler store_handler(
        catalog_factories,
        cluster_manager,
        store::DataStoreHandler::DataStoreOpStatus::Retry);
    FetchRecordFixture fixture(&store_handler);
    const TableName table{std::string_view("fetch_record_store_retry"),
                          TableType::Primary,
                          TableEngine::EloqSql};
    MockTableSchema schema(table, "", 1);
    TestCcMap cc_map(&fixture.shard, 0, table, 1, &schema, true);

    TestKey key = std::make_tuple(std::string("key"), 1);
    bool emplaced = false;
    auto it = cc_map.FindEmplace(key, &emplaced, false, false);
    REQUIRE(emplaced);
    auto *cce = static_cast<TestCcEntry *>(it->second);
    REQUIRE(cce != nullptr);
    cce->GetOrCreateKeyLock(&fixture.shard, &cc_map, it.GetPage());

    fixture.shard.fetch_record_cc_pool_.head_ = 0;
    FetchRecordCc *first_pool_entry =
        fixture.shard.fetch_record_cc_pool_.pool_.front().get();
    AbortTrackingRequester requester;
    requester.Use();

    const auto result =
        fixture.shard.FetchRecord(table,
                                  &schema,
                                  TxKey(std::make_unique<TestKey>(key)),
                                  cce,
                                  0,
                                  CurrentTerm(),
                                  &requester,
                                  0);

    REQUIRE(result == store::DataStoreHandler::DataStoreOpStatus::Retry);
    REQUIRE_FALSE(fixture.shard.fetch_record_reqs_.contains(cce));
    REQUIRE_FALSE(first_pool_entry->InUse());
    REQUIRE(requester.InUse());
    requester.Free();
}

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
TEST_CASE("FetchRecordCc keeps ownership while EloqStore reopen is in flight",
          "[fetch-record][pool][eloqstore]")
{
    CatalogFactory *catalog_factories[3] = {
        &mock_catalog_factory, &mock_catalog_factory, &mock_catalog_factory};
    EloqDS::DataStoreServiceClusterManager cluster_manager;
    StubStoreHandler store_handler(
        catalog_factories,
        cluster_manager,
        store::DataStoreHandler::DataStoreOpStatus::Success);
    FetchRecordFixture fixture(&store_handler);
    const TableName table{std::string_view("fetch_record_reopen_in_flight"),
                          TableType::Primary,
                          TableEngine::EloqSql};
    TestCcMap cc_map(&fixture.shard, 0, table, 1, nullptr, true);

    TestKey key = std::make_tuple(std::string("key"), 1);
    bool emplaced = false;
    auto it = cc_map.FindEmplace(key, &emplaced, false, false);
    REQUIRE(emplaced);
    auto *cce = static_cast<TestCcEntry *>(it->second);
    REQUIRE(cce != nullptr);

    cce->GetOrCreateKeyLock(&fixture.shard, &cc_map, it.GetPage());
    KeyGapLockAndExtraData *lock = cce->GetKeyGapLockAndExtraData();
    REQUIRE(lock != nullptr);
    lock->BufferedCommandList().txn_cmd_list_.emplace_back(
        1, 2, false, 0, std::vector<std::unique_ptr<TxCommand>>{});
    lock->AddPin();
    lock->AddPin();

    FetchRecordCc *fetch = fixture.shard.fetch_record_cc_pool_.NextRequest();
    fetch->ccs_ = &fixture.shard;
    fetch->cc_ng_id_ = 0;
    fetch->cc_ng_term_ = CurrentTerm();
    fetch->cce_ = cce;
    fetch->lock_ = lock;
    fetch->rec_ts_ = 2;
    fetch->rec_status_ = RecordStatus::Deleted;
    fetch->error_code_ = 0;
    fetch->only_fetch_archives_ = false;

    QueuedRequester requester(cce);
    requester.Use();
    fetch->AddRequester(&requester);
    fixture.shard.fetch_record_reqs_.try_emplace(cce, fetch);
    fixture.shard.Enqueue(fetch);

    REQUIRE(fixture.shard.ProcessRequests() == 1);
    REQUIRE(fixture.shard.ProcessRequests() == 1);
    REQUIRE(store_handler.fetch_count_ == 1);
    REQUIRE(store_handler.last_fetch_ == fetch);
    REQUIRE(requester.execute_count_ == 1);
    REQUIRE(requester.active_fetch_present_);
    REQUIRE_FALSE(requester.InUse());
    REQUIRE(fixture.shard.fetch_record_reqs_.at(cce) == fetch);
    REQUIRE(fetch->InUse());

    // Model EloqStore completing the reopened operation after it consumes the
    // buffered command. This second completion has no reason to reopen again.
    lock->BufferedCommandList().Clear();
    fetch->rec_ts_ = 3;
    fetch->rec_status_ = RecordStatus::Deleted;
    fetch->SetFinish(0);
    REQUIRE(fixture.shard.ProcessRequests() == 1);
    REQUIRE_FALSE(fixture.shard.fetch_record_reqs_.contains(cce));
    REQUIRE_FALSE(fetch->InUse());
}

TEST_CASE("FetchRecordCc reopen retry is recycled by the dispatcher",
          "[fetch-record][pool][eloqstore]")
{
    CatalogFactory *catalog_factories[3] = {
        &mock_catalog_factory, &mock_catalog_factory, &mock_catalog_factory};
    EloqDS::DataStoreServiceClusterManager cluster_manager;
    StubStoreHandler store_handler(
        catalog_factories,
        cluster_manager,
        store::DataStoreHandler::DataStoreOpStatus::Retry);
    FetchRecordFixture fixture(&store_handler);
    const TableName table{std::string_view("fetch_record_reopen_retry"),
                          TableType::Primary,
                          TableEngine::EloqSql};
    TestCcMap cc_map(&fixture.shard, 0, table, 1, nullptr, true);

    TestKey key = std::make_tuple(std::string("key"), 1);
    bool emplaced = false;
    auto it = cc_map.FindEmplace(key, &emplaced, false, false);
    REQUIRE(emplaced);
    auto *cce = static_cast<TestCcEntry *>(it->second);
    REQUIRE(cce != nullptr);

    cce->GetOrCreateKeyLock(&fixture.shard, &cc_map, it.GetPage());
    KeyGapLockAndExtraData *lock = cce->GetKeyGapLockAndExtraData();
    REQUIRE(lock != nullptr);
    lock->BufferedCommandList().txn_cmd_list_.emplace_back(
        1, 2, false, 0, std::vector<std::unique_ptr<TxCommand>>{});
    lock->AddPin();
    lock->AddPin();

    FetchRecordCc *fetch = fixture.shard.fetch_record_cc_pool_.NextRequest();
    fetch->ccs_ = &fixture.shard;
    fetch->cc_ng_id_ = 0;
    fetch->cc_ng_term_ = CurrentTerm();
    fetch->cce_ = cce;
    fetch->lock_ = lock;
    fetch->rec_ts_ = 2;
    fetch->rec_status_ = RecordStatus::Deleted;
    fetch->error_code_ = 0;
    fetch->only_fetch_archives_ = false;

    QueuedRequester requester(cce);
    requester.Use();
    fetch->AddRequester(&requester);
    fixture.shard.fetch_record_reqs_.try_emplace(cce, fetch);
    fixture.shard.Enqueue(fetch);

    REQUIRE(fixture.shard.ProcessRequests() == 1);
    REQUIRE(fixture.shard.ProcessRequests() == 1);
    REQUIRE(store_handler.fetch_count_ == 1);
    REQUIRE(requester.execute_count_ == 1);
    REQUIRE_FALSE(requester.active_fetch_present_);
    REQUIRE_FALSE(requester.InUse());
    REQUIRE_FALSE(fixture.shard.fetch_record_reqs_.contains(cce));
    REQUIRE_FALSE(fetch->InUse());
}
#endif

}  // namespace txservice
