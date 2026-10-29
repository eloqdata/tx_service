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

}  // namespace txservice
