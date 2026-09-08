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

#include "cc/cc_req_misc.h"
#include "cc/cc_req_pool.h"
#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "data_store_service_client.h"
#include "include/mock/mock_catalog_factory.h"
#include "sharder.h"
#include "tx_key.h"

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
