/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License, version 2, as published by the Free
 *    Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program. If not, see
 *    <http://www.gnu.org/licenses/>.
 */
#include <algorithm>
#include <array>
#include <catch2/catch_all.hpp>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

// This test verifies the ownership boundary between CcShard's active-fetch
// index and its FetchRecordCc pool.
#define protected public
#define private public
#include "cc/cc_entry.h"
#include "cc/cc_req_misc.h"
#include "cc/cc_req_pool.h"
#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "cc/template_cc_map.h"
#include "data_store_service_client.h"
#include "include/mock/mock_catalog_factory.h"
#include "sharder.h"
#include "tx_key.h"
#undef private
#undef protected

namespace txservice
{
namespace
{
constexpr size_t kPayloadSize = 64 * 1024;

class CountedKey : public CompositeKey<std::string>
{
public:
    CountedKey() = default;

    explicit CountedKey(size_t *destroyed)
        : CompositeKey<std::string>(std::string(kPayloadSize, 'k')),
          destroyed_(destroyed)
    {
    }

    ~CountedKey()
    {
        if (destroyed_ != nullptr)
        {
            ++*destroyed_;
        }
    }

    static const TxKeyInterface *TxKeyImpl()
    {
        static const TxKeyInterface key_interface{CountedKey{}};
        return &key_interface;
    }

private:
    size_t *destroyed_{nullptr};
};

class ObservedFetchRecordCc : public FetchRecordCc
{
public:
    explicit ObservedFetchRecordCc(size_t *destroyed = nullptr)
        : destroyed_(destroyed)
    {
    }

    ~ObservedFetchRecordCc() override
    {
        if (destroyed_ != nullptr)
        {
            ++*destroyed_;
        }
    }

    void Populate(size_t *destroyed_keys)
    {
        tx_key_ = TxKey(std::make_unique<CountedKey>(destroyed_keys));
        rec_str_.assign(kPayloadSize, 'v');
        kv_session_id_.assign(kPayloadSize, 's');
        kv_start_key_.assign(kPayloadSize, 'a');
        kv_end_key_.assign(kPayloadSize, 'z');
        archive_records_ = std::make_unique<
            std::vector<std::tuple<uint64_t, RecordStatus, std::string>>>();
        archive_records_->emplace_back(
            1, RecordStatus::Normal, std::string(kPayloadSize, 'h'));
        requesters_.assign(256, nullptr);
    }

    size_t RequesterCapacity() const
    {
        return requesters_.capacity();
    }

private:
    size_t *destroyed_{nullptr};
};

void CheckReleased(const ObservedFetchRecordCc &request)
{
    const size_t empty_capacity = std::string{}.capacity();
    REQUIRE_FALSE(request.InUse());
    REQUIRE_FALSE(request.tx_key_.IsOwner());
    REQUIRE(request.archive_records_ == nullptr);
    REQUIRE(request.RequesterCount() == 0);
    REQUIRE(request.RequesterCapacity() == 0);
    for (const std::string *buffer : {&request.rec_str_,
                                      &request.kv_session_id_,
                                      &request.kv_start_key_,
                                      &request.kv_end_key_})
    {
        REQUIRE(buffer->empty());
        REQUIRE(buffer->capacity() <= empty_capacity);
    }
}

class DeferredFetchStore : public EloqDS::DataStoreServiceClient
{
public:
    using DataStoreServiceClient::DataStoreServiceClient;

    DataStoreOpStatus FetchRecord(FetchRecordCc *request,
                                  FetchSnapshotCc *snapshot = nullptr) override
    {
        REQUIRE(snapshot == nullptr);
        requests_.push_back(request);
        request->rec_str_.assign(kPayloadSize, 'v');
        request->rec_status_ = RecordStatus::Normal;
        request->rec_ts_ = 1;
        return DataStoreOpStatus::Success;
    }

    std::vector<FetchRecordCc *> requests_;
};

/** A real shard queue with manually completed storage; no RPC service starts.
 */
class FetchShardFixture
{
public:
    FetchShardFixture()
    {
        cluster_.Initialize("127.0.0.1", 8600);
        store_ = std::make_unique<DeferredFetchStore>(
            false, factories_.data(), cluster_, true);
        shards_ = std::make_unique<LocalCcShards>(0,
                                                  0,
                                                  config_,
                                                  factories_.data(),
                                                  nullptr,
                                                  &nodes_,
                                                  1,
                                                  store_.get(),
                                                  nullptr,
                                                  true);
        shards_->BindThreadToFastMetaDataShard(0);
        Shard().Init();
    }

    CcShard &Shard()
    {
        return *shards_->GetCcShard(0);
    }

    DeferredFetchStore &Store()
    {
        return *store_;
    }

private:
    MockCatalogFactory factory_;
    std::array<CatalogFactory *, 5> factories_{
        &factory_, &factory_, &factory_, &factory_, &factory_};
    std::unordered_map<uint32_t, std::vector<NodeConfig>> nodes_{
        {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> config_{
        {"node_memory_limit_mb", 128},
        {"range_slice_memory_limit_percent", 20},
        {"realtime_sampling", 0},
        {"range_split_worker_num", 1},
        {"core_num", 1},
        {"enable_key_cache", 0},
        {"enable_shard_heap_defragment", 0}};
    EloqDS::DataStoreServiceClusterManager cluster_;
    std::unique_ptr<DeferredFetchStore> store_;
    std::unique_ptr<LocalCcShards> shards_;
};

class RetryingBackFillMap
    : public TemplateCcMap<CompositeKey<int>, CompositeRecord<int>, true, true>
{
public:
    explicit RetryingBackFillMap(CcShard *shard, const TableName &table)
        : TemplateCcMap(shard, 0, table, 1)
    {
    }

    bool BackFill(LruEntry *entry,
                  uint64_t commit_ts,
                  RecordStatus status,
                  const std::string &record) override
    {
        REQUIRE(record == std::string(kPayloadSize, 'v'));
        ++calls_;
        if (entry == retry_entry_ && !retried_)
        {
            retried_ = true;
            return false;
        }
        entry->GetKeyGapLockAndExtraData()->ReleasePin();
        return true;
    }

    LruEntry *retry_entry_{nullptr};
    size_t calls_{0};
    bool retried_{false};
};
}  // namespace

TEST_CASE("FetchRecordCc releases owned results before pool reuse",
          "[fetch-record-cc]")
{
    CcRequestPool<ObservedFetchRecordCc> pool(1);
    size_t destroyed_keys = 0;
    ObservedFetchRecordCc *request = pool.NextRequest();
    REQUIRE(request != nullptr);
    request->Populate(&destroyed_keys);
    REQUIRE(request->rec_str_.capacity() >= kPayloadSize);
    REQUIRE(request->tx_key_.IsOwner());

    CcRequestBase *base = request;
    base->Free();

    CheckReleased(*request);
    REQUIRE(destroyed_keys == 1);
    ObservedFetchRecordCc *reused = pool.NextRequest();
    REQUIRE(reused == request);
    REQUIRE(reused->InUse());
    reused->rec_str_ = "small";
    reused->Free();
    CheckReleased(*reused);
    REQUIRE(destroyed_keys == 1);
}

TEST_CASE("FetchRecordCc recycling preserves borrowed keys",
          "[fetch-record-cc]")
{
    size_t destroyed_keys = 0;
    {
        CountedKey key(&destroyed_keys);
        ObservedFetchRecordCc request;
        request.Use();
        request.tx_key_ = TxKey(&key);
        request.Free();

        REQUIRE_FALSE(request.InUse());
        REQUIRE_FALSE(request.tx_key_.IsOwner());
        REQUIRE(destroyed_keys == 0);
        REQUIRE(key.ToString().size() > kPayloadSize);
    }
    REQUIRE(destroyed_keys == 1);
}

TEST_CASE("FetchRecordCc pooled owner recycles without destroying the request",
          "[fetch-record-cc]")
{
    size_t destroyed_requests = 0;
    size_t destroyed_keys = 0;
    {
        ObservedFetchRecordCc request(&destroyed_requests);
        request.Use();
        request.Populate(&destroyed_keys);
        {
            FetchRecordCc::uptr owner(&request, FetchRecordCc::Deleter{true});
        }

        REQUIRE(destroyed_requests == 0);
        REQUIRE(destroyed_keys == 1);
        CheckReleased(request);
    }
    REQUIRE(destroyed_requests == 1);
    REQUIRE(destroyed_keys == 1);
}

TEST_CASE("FetchRecordCc overflow owner destroys once after ownership transfer",
          "[fetch-record-cc]")
{
    size_t destroyed_requests = 0;
    size_t destroyed_keys = 0;
    {
        auto request =
            std::make_unique<ObservedFetchRecordCc>(&destroyed_requests);
        request->Use();
        request->Populate(&destroyed_keys);
        FetchRecordCc::uptr owner(request.release(),
                                  FetchRecordCc::Deleter{false});

        FetchRecordCc::uptr transferred = std::move(owner);
        REQUIRE(owner == nullptr);
        REQUIRE(transferred->InUse());
        REQUIRE(transferred->rec_str_.size() == kPayloadSize);
        REQUIRE(destroyed_requests == 0);
        REQUIRE(destroyed_keys == 0);

        transferred.reset();
        REQUIRE(destroyed_requests == 1);
        REQUIRE(destroyed_keys == 1);
    }
    REQUIRE(destroyed_requests == 1);
    REQUIRE(destroyed_keys == 1);
}

TEST_CASE(
    "FetchRecordCc keeps overflow and deduplicated fetches alive on retry",
    "[fetch-record-cc]")
{
    constexpr size_t request_count = 129;
    std::array<size_t, request_count> destroyed_keys{};
    FetchShardFixture fixture;
    CcShard &shard = fixture.Shard();
    const TableName table(
        std::string("fetch-retry"), TableType::Primary, TableEngine::EloqSql);
    MockTableSchema schema(table, "", 1);
    RetryingBackFillMap map(&shard, table);
    LruPage page(&map);
    std::array<LruEntry, request_count> entries;
    std::array<KeyGapLockAndExtraData, request_count> locks;
    // This fixture has no cluster services: all three term sources report
    // -1. Matching that term exercises lifecycle, not leader election.
    const int64_t term = Sharder::Instance().LeaderTerm(0);

    for (size_t i = 0; i < request_count; ++i)
    {
        locks[i].Reset(&map, &page, &entries[i]);
        entries[i].cc_lock_and_extra_ = &locks[i];
        REQUIRE(shard.FetchRecord(
                    table,
                    &schema,
                    TxKey(std::make_unique<CountedKey>(&destroyed_keys[i])),
                    &entries[i],
                    0,
                    term,
                    nullptr,
                    0) == store::DataStoreHandler::DataStoreOpStatus::Success);
    }
    REQUIRE(fixture.Store().requests_.size() == request_count);

    // The 129th active fetch exceeds the shard's reusable pool. A second
    // waiter on that same entry must share its in-flight storage request.
    const size_t overflow = request_count - 1;
    FetchRecordCc *request = fixture.Store().requests_[overflow];
    CountedKey borrowed_key;
    REQUIRE(shard.FetchRecord(table,
                              &schema,
                              TxKey(&borrowed_key),
                              &entries[overflow],
                              0,
                              term,
                              nullptr,
                              0) ==
            store::DataStoreHandler::DataStoreOpStatus::Success);
    REQUIRE(fixture.Store().requests_.size() == request_count);
    REQUIRE(request->RequesterCount() == 2);

    map.retry_entry_ = &entries[overflow];
    request->SetFinish(0);
    REQUIRE(shard.ProcessRequests() == 1);
    REQUIRE(map.calls_ == 1);
    REQUIRE(request->InUse());
    REQUIRE(request->rec_str_.size() == kPayloadSize);
    REQUIRE(request->RequesterCount() == 2);
    REQUIRE(destroyed_keys[overflow] == 0);

    REQUIRE(shard.ProcessRequests() == 1);
    REQUIRE(map.calls_ == 2);
    REQUIRE(destroyed_keys[overflow] == 1);

    for (size_t i = 0; i < overflow; ++i)
    {
        fixture.Store().requests_[i]->SetFinish(0);
    }
    size_t processed = 0;
    while (processed < overflow)
    {
        const size_t batch = shard.ProcessRequests();
        REQUIRE(batch > 0);
        processed += batch;
    }
    REQUIRE(processed == overflow);
    for (size_t i = 0; i < request_count; ++i)
    {
        REQUIRE(destroyed_keys[i] == 1);
    }
    for (size_t i = 0; i < overflow; ++i)
    {
        REQUIRE_FALSE(fixture.Store().requests_[i]->InUse());
        REQUIRE(fixture.Store().requests_[i]->rec_str_.capacity() <=
                std::string{}.capacity());
    }
}
}  // namespace txservice

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
        sharder.ng_leader_term_cache_[0] = -1;
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
    ReentrantRequester(LruEntry *cce,
                       FetchRecordCc *finishing_fetch,
                       const size_t *destroyed_fetches)
        : cce_(cce),
          finishing_fetch_(finishing_fetch),
          destroyed_fetches_(destroyed_fetches)
    {
    }

    bool Execute(CcShard &ccs) override
    {
        ++execute_count_;
        active_fetch_removed_ = !ccs.fetch_record_reqs_.contains(cce_);
        finishing_fetch_alive_ = *destroyed_fetches_ == 0;

        // Force the pool scan to start at the finishing request. It must skip
        // that object while the completion still owns it.
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
    bool finishing_fetch_alive_{false};

private:
    LruEntry *cce_;
    FetchRecordCc *finishing_fetch_;
    const size_t *destroyed_fetches_;
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

TEST_CASE("FetchRecordCc inline completion retains pooled and temporary owners",
          "[fetch-record][pool]")
{
    const bool temporary = GENERATE(false, true);
    size_t destroyed_fetches = 0;
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

    FetchRecordCc *fetch =
        temporary ? new ObservedFetchRecordCc(&destroyed_fetches)
                  : fixture.shard.fetch_record_cc_pool_.NextRequest();
    if (temporary)
    {
        fetch->Use();
    }
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

    ReentrantRequester requester(cce, fetch, &destroyed_fetches);
    requester.Use();
    fetch->AddRequester(&requester);
    fixture.shard.fetch_record_reqs_.try_emplace(
        cce, FetchRecordCc::uptr(fetch, FetchRecordCc::Deleter{!temporary}));

    fixture.shard.Enqueue(fetch);
    const size_t processed = fixture.shard.ProcessRequests();

    REQUIRE(processed == 1);
    REQUIRE(requester.execute_count_ == 1);
    REQUIRE(requester.active_fetch_removed_);
    REQUIRE(requester.finishing_fetch_not_reused_);
    REQUIRE(requester.finishing_fetch_alive_);
    REQUIRE_FALSE(requester.InUse());
    REQUIRE_FALSE(fixture.shard.fetch_record_reqs_.contains(cce));
    // The completion owner releases the request after inline requesters finish.
    if (temporary)
    {
        REQUIRE(destroyed_fetches == 1);
    }
    else
    {
        REQUIRE_FALSE(fetch->InUse());
    }
}

TEST_CASE("FetchRecordCc term-change completion releases its active owner",
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
    fixture.shard.fetch_record_reqs_.try_emplace(
        cce, FetchRecordCc::uptr(fetch, FetchRecordCc::Deleter{true}));

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
    fixture.shard.fetch_record_reqs_.try_emplace(
        cce, FetchRecordCc::uptr(fetch, FetchRecordCc::Deleter{true}));
    fixture.shard.Enqueue(fetch);

    REQUIRE(fixture.shard.ProcessRequests() == 1);
    REQUIRE(fixture.shard.ProcessRequests() == 1);
    REQUIRE(store_handler.fetch_count_ == 1);
    REQUIRE(store_handler.last_fetch_ == fetch);
    REQUIRE(requester.execute_count_ == 1);
    REQUIRE(requester.active_fetch_present_);
    REQUIRE_FALSE(requester.InUse());
    REQUIRE(fixture.shard.fetch_record_reqs_.at(cce).get() == fetch);
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

TEST_CASE("FetchRecordCc reopen retry releases its active owner",
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
    fixture.shard.fetch_record_reqs_.try_emplace(
        cce, FetchRecordCc::uptr(fetch, FetchRecordCc::Deleter{true}));
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

namespace
{
/** The map a point read on a hash-partitioned table runs against. */
using PointReadCcMap =
    TemplateCcMap<CompositeKey<int>, CompositeRecord<int>, true, false>;
/** An entry of PointReadCcMap. */
using PointReadCcEntry =
    CcEntry<CompositeKey<int>, CompositeRecord<int>, true, false>;

/** The value the store holds for every key in the point-read tests. */
constexpr int kStoredValue = 4242;
/** The commit timestamp the store reports for that value. */
constexpr uint64_t kStoredCommitTs = 9;

/**
 * @brief A store that answers every record fetch with one stored value and
 * leaves completing the fetch to the test.
 *
 * FetchRecord() fills the answer in and returns; the test then calls the
 * fetch's SetFinish(), as the store's callback thread would.
 */
class AnsweringStore : public EloqDS::DataStoreServiceClient
{
public:
    /**
     * @param catalog_factories The catalog factories of the fixture.
     * @param cluster_manager The data store cluster the client is given.
     */
    AnsweringStore(
        CatalogFactory *catalog_factories[3],
        const EloqDS::DataStoreServiceClusterManager &cluster_manager)
        : DataStoreServiceClient(
              false, catalog_factories, cluster_manager, false)
    {
    }

    store::DataStoreHandler::DataStoreOpStatus FetchRecord(
        FetchRecordCc *fetch, FetchSnapshotCc *) override
    {
        fetch->rec_status_ = RecordStatus::Normal;
        fetch->rec_ts_ = kStoredCommitTs;
        fetch->rec_str_.clear();
        CompositeRecord<int>(kStoredValue).Serialize(fetch->rec_str_);
        fetches_.push_back(fetch);
        return store::DataStoreHandler::DataStoreOpStatus::Success;
    }

    std::vector<FetchRecordCc *> fetches_;  ///< Fetches issued so far.
};

/**
 * @brief Gives node group 0 a valid leader term for one test and puts the
 * fixture's "no leader" state back afterwards.
 *
 * FetchRecordFixture leaves every term at -1, which is enough for the fetch
 * lifecycle tests above but makes a real read fail as "not leader".
 */
struct ScopedLeaderTerm
{
    ScopedLeaderTerm()
    {
        Sharder &sharder = Sharder::Instance();
        sharder.cc_nodes_init_.store(true, std::memory_order_release);
        sharder.leader_term_cache_[0].store(1, std::memory_order_release);
    }

    ~ScopedLeaderTerm()
    {
        Sharder &sharder = Sharder::Instance();
        sharder.leader_term_cache_[0].store(-1, std::memory_order_release);
        sharder.cc_nodes_init_.store(false, std::memory_order_release);
    }
};

/**
 * @brief One real point read of a key, with its result, issued the way the
 * local handler issues it.
 */
struct PointRead
{
    /**
     * @param table The table read.
     * @param cc_map The table's cc map. It is not registered in the shard, so
     * the read is pointed at it directly, as a read is after its first
     * lookup.
     * @param key The key read.
     * @param abort_if_oom Whether the read fails fast for memory, as a read
     * holding a range read lock does.
     */
    PointRead(const TableName &table,
              PointReadCcMap &cc_map,
              int key,
              bool abort_if_oom)
        : key_(key), tx_key_(&key_), result_(nullptr)
    {
        result_.Value().Reset();
        read_.Use();
        read_.Reset(&table,
                    /*schema_version=*/1,
                    &tx_key_,
                    /*key_shard_code=*/0,
                    &value_,
                    ReadType::Inside,
                    /*tx_number=*/1,
                    /*tx_term=*/1,
                    /*ts=*/0,
                    &result_,
                    IsolationLevel::ReadCommitted,
                    CcProtocol::OCC,
                    /*is_for_write=*/false,
                    /*is_covering_keys=*/false,
                    /*archives=*/nullptr,
                    /*is_in_recovering=*/false,
                    /*point_read_on_miss=*/false,
                    /*partition_id=*/0,
                    abort_if_oom);
        read_.ccm_ = &cc_map;
    }

    CompositeKey<int> key_;                  ///< The key read.
    TxKey tx_key_;                           ///< Borrowed view of key_.
    CompositeRecord<int> value_;             ///< Where the read puts the value.
    CcHandlerResult<ReadKeyResult> result_;  ///< The read's result.
    ReadCc read_;                            ///< The request.
};

/**
 * @brief Runs the shard until its queue is empty.
 * @param shard The shard to run.
 */
void DrainShard(CcShard &shard)
{
    // The cleaner re-enqueues itself between batches; a bound keeps a bug
    // from turning into a hang.
    for (int round = 0; round < 1000; ++round)
    {
        if (shard.ProcessRequests() == 0)
        {
            return;
        }
    }
    FAIL("the shard queue did not drain");
}

/**
 * @brief Reserves everything above the allocator's figure, so the heap is
 * full on reservations alone.
 * @param heap The shard heap.
 * @return The amount reserved, to release later.
 */
size_t FillHeap(CcShardHeap &heap)
{
    const size_t fill =
        heap.MemoryLimit() - static_cast<size_t>(heap.PhysicalAllocated());
    REQUIRE(heap.TryReserve(fill));
    REQUIRE(heap.Full());
    return fill;
}

/**
 * @brief Looks up the entry of a key without creating it.
 * @param cc_map The map.
 * @param key The key.
 * @return The entry, or null if the key has none.
 */
PointReadCcEntry *FindEntry(PointReadCcMap &cc_map, int key)
{
    auto it = cc_map.Find(CompositeKey<int>(key));
    return it->second;
}
}  // namespace

TEST_CASE(
    "A point read on a cache miss waits for memory before its record is "
    "installed",
    "[fetch-record][memory]")
{
    CatalogFactory *catalog_factories[3] = {
        &mock_catalog_factory, &mock_catalog_factory, &mock_catalog_factory};
    EloqDS::DataStoreServiceClusterManager cluster_manager;
    AnsweringStore store(catalog_factories, cluster_manager);
    FetchRecordFixture fixture(&store);
    ScopedLeaderTerm leader_term;
    CcShard &shard = fixture.shard;
    CcShardHeap *heap = shard.GetShardHeap();
    REQUIRE(heap != nullptr);

    const TableName table{std::string_view("point_read_memory"),
                          TableType::Primary,
                          TableEngine::EloqSql};
    MockTableSchema schema(table, "", 1);
    // Not every key is cached, so a read of an uncached key asks the store.
    PointReadCcMap cc_map(
        &shard, 0, table, 1, &schema, /*ccm_has_full_entries=*/false);

    SECTION("the read waits while the record does not fit, then completes")
    {
        PointRead point_read(table, cc_map, 7, /*abort_if_oom=*/false);

        // The read misses the cache and issues a store fetch.
        REQUIRE_FALSE(point_read.read_.Execute(shard));
        REQUIRE(store.fetches_.size() == 1);
        FetchRecordCc *fetch = store.fetches_.front();
        PointReadCcEntry *cce = FindEntry(cc_map, 7);
        REQUIRE(cce != nullptr);
        REQUIRE(shard.fetch_record_reqs_.contains(cce));
        REQUIRE_FALSE(point_read.result_.IsFinished());

        // The store answers while the heap is full: the fetch parks, the
        // cleaner runs a pass that finds nothing to evict and defers, and the
        // read stays blocked with nothing installed.
        const size_t fill = FillHeap(*heap);
        fetch->SetFinish(0);
        DrainShard(shard);
        REQUIRE(shard.WaitListSizeForMemory() == 1);
        REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);
        REQUIRE_FALSE(point_read.result_.IsFinished());

        // Room again. The cleaner's deferred retry releases the fetch, which
        // installs the record and resumes the read.
        heap->ReleaseReservation(fill);
        shard.FireCleanRetryIfDue(UINT64_MAX);
        DrainShard(shard);
        REQUIRE(shard.WaitListSizeForMemory() == 0);
        REQUIRE(point_read.result_.IsFinished());
        REQUIRE_FALSE(point_read.result_.IsError());
        REQUIRE(point_read.result_.Value().rec_status_ == RecordStatus::Normal);
        REQUIRE(point_read.result_.Value().ts_ == kStoredCommitTs);
        REQUIRE(std::get<0>(point_read.value_.Tuple()) == kStoredValue);
        REQUIRE(cce->PayloadStatus() == RecordStatus::Normal);
        REQUIRE_FALSE(shard.fetch_record_reqs_.contains(cce));
        REQUIRE(cce->GetKeyGapLockAndExtraData() == nullptr);
        REQUIRE(heap->ReservedBytes() == 0);
    }

    SECTION("a fail-fast read is rejected by the cleaner's pass")
    {
        PointRead point_read(table, cc_map, 8, /*abort_if_oom=*/true);

        REQUIRE_FALSE(point_read.read_.Execute(shard));
        REQUIRE(store.fetches_.size() == 1);
        FetchRecordCc *fetch = store.fetches_.front();
        PointReadCcEntry *cce = FindEntry(cc_map, 8);
        REQUIRE(cce != nullptr);

        // The store answers while the heap is full: the fetch parks, and the
        // cleaner's pass, which ends with the heap still full, rejects it
        // because the read waiting on it fails fast.
        const size_t fill = FillHeap(*heap);
        fetch->SetFinish(0);
        DrainShard(shard);
        REQUIRE(shard.WaitListSizeForMemory() == 0);
        REQUIRE(point_read.result_.IsFinished());
        REQUIRE(point_read.result_.IsError());
        REQUIRE(point_read.result_.ErrorCode() == CcErrorCode::OUT_OF_MEMORY);
        REQUIRE_FALSE(shard.fetch_record_reqs_.contains(cce));
        // Nothing was installed, and the fetch's pin and the read's are gone.
        REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);
        REQUIRE(cce->GetKeyGapLockAndExtraData() == nullptr);

        heap->ReleaseReservation(fill);
        REQUIRE(heap->ReservedBytes() == 0);
    }
}

}  // namespace txservice
