/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under either GNU Affero General Public License, version 3, or GNU
 *    General Public License, version 2.
 */

// Production DSS-client expansion tests for docs/08-paged-objects-test-plan.md
// §4.6. These deliberately call DataStoreServiceClient::PreparePartitionBatches
// rather than mirroring it: the resulting parallel arrays are exactly what the
// local/RPC BatchWriteRecords path consumes.

#include <catch2/catch_all.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "data_store_service_client.h"
#include "data_store_service_client_closure.h"
#include "harness/test_node.h"
#include "include/mock/mock_catalog_factory.h"
#include "tx_service/include/data_sync_task.h"
#include "tx_service/include/page_key_codec.h"

namespace EloqDS
{
class PagedStoreBatchTestAccess
{
public:
    static void Prepare(
        DataStoreServiceClient &client,
        PartitionFlushState &state,
        const std::vector<std::pair<size_t, size_t>> &indices,
        const std::vector<std::unique_ptr<txservice::FlushTaskEntry>> &entries,
        const txservice::TableName &table,
        uint64_t now)
    {
        client.PreparePartitionBatches(
            state, indices, entries, table, 1, 1, now);
    }
};
}  // namespace EloqDS

namespace txservice
{
namespace
{
constexpr size_t kWriteBatchLimit = 64U * 1024U * 1024U;

class ClientFixture
{
public:
    ClientFixture()
    {
        cluster_.Initialize("127.0.0.1", 1);
        CatalogFactory *factories[NUM_EXTERNAL_ENGINES] = {
            &catalog_, &catalog_, &catalog_};
        client_ = std::make_unique<EloqDS::DataStoreServiceClient>(
            false, factories, cluster_, false, nullptr);
    }

    EloqDS::DataStoreServiceClient &Client()
    {
        return *client_;
    }

private:
    MockCatalogFactory catalog_;
    EloqDS::DataStoreServiceClusterManager cluster_;
    std::unique_ptr<EloqDS::DataStoreServiceClient> client_;
};

FlushRecord PagedRecord(int key,
                        uint64_t commit_ts,
                        RecordStatus status,
                        PagedObjectFlush paged)
{
    FlushRecord record;
    record.SetKey(test::Key(key));
    record.payload_status_ = status;
    record.commit_ts_ = commit_ts;
    record.partition_id_ = 0;
    record.SetPagedPayload(std::move(paged));
    return record;
}

std::unique_ptr<FlushTaskEntry> Entry(std::vector<FlushRecord> records)
{
    return std::make_unique<FlushTaskEntry>(
        std::make_unique<std::vector<FlushRecord>>(std::move(records)),
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        0);
}

std::vector<FlushRecord> OneRecord(FlushRecord record)
{
    std::vector<FlushRecord> records;
    records.emplace_back(std::move(record));
    return records;
}

struct PreparedBatches
{
    // PartitionBatchRequest deliberately contains zero-copy views into the
    // FlushRecords. Production keeps the FlushTaskEntries alive until every
    // batch completes, so this fixture must model the same lifetime. Keep the
    // entries before the batches so the views are destroyed first.
    std::vector<std::unique_ptr<FlushTaskEntry>> entries;
    std::vector<EloqDS::PartitionBatchRequest> batches;
};

PreparedBatches Prepare(EloqDS::DataStoreServiceClient &client,
                        std::vector<FlushRecord> records)
{
    PreparedBatches prepared;
    std::vector<std::pair<size_t, size_t>> indices;
    for (size_t i = 0; i < records.size(); ++i)
    {
        indices.emplace_back(0, i);
    }
    prepared.entries.emplace_back(Entry(std::move(records)));

    TableName table(std::string_view("paged_store_batch"),
                    TableType::Primary,
                    TableEngine::EloqKv);
    REQUIRE(table.IsHashPartitioned());
    REQUIRE(table.IsObjectTable());

    EloqDS::PartitionFlushState state;
    state.Reset(0, false);
    EloqDS::PagedStoreBatchTestAccess::Prepare(
        client, state, indices, prepared.entries, table, 1000);

    EloqDS::PartitionBatchRequest batch;
    while (state.GetNextBatch(batch))
    {
        prepared.batches.emplace_back(std::move(batch));
        batch = EloqDS::PartitionBatchRequest();
    }
    return prepared;
}

void RequireParallel(const EloqDS::PartitionBatchRequest &batch)
{
    REQUIRE(batch.key_parts.size() == batch.record_parts.size());
    REQUIRE(batch.key_parts.size() == batch.records_ts.size());
    REQUIRE(batch.key_parts.size() == batch.records_ttl.size());
    REQUIRE(batch.key_parts.size() == batch.op_types.size());
}

PagedObjectFlush SizedFlush(size_t total_bytes, size_t key_size)
{
    // PrepareObjectData accounts for key + metadata + ts + ttl + op. No page
    // rows are needed to exercise the record-boundary rule.
    const size_t overhead =
        key_size + 2 * sizeof(uint64_t) + sizeof(EloqDS::WriteOpType);
    REQUIRE(total_bytes >= overhead);
    PagedObjectFlush paged;
    paged.page_size_ = 4096;
    paged.metadata_.assign(total_bytes - overhead, 'm');
    return paged;
}
}  // namespace

TEST_CASE("Paged DSS expansion preserves row shape, TTL, and zero-copy pages",
          "[paged][store-batch]")
{
    ClientFixture fixture;
    auto page = std::shared_ptr<uint8_t[]>(new uint8_t[32]);
    for (uint8_t i = 0; i < 32; ++i)
    {
        page[i] = i;
    }

    PagedObjectFlush paged;
    paged.metadata_ = "metadata";
    paged.page_size_ = 32;
    paged.metadata_row_ttl_ = 7777;
    paged.pages_.push_back({7, PageRowKind::HashPage, page});
    paged.pages_.push_back({8, PageRowKind::HashPage, nullptr});

    auto prepared =
        Prepare(fixture.Client(),
                OneRecord(PagedRecord(
                    42, 1234, RecordStatus::Normal, std::move(paged))));
    const auto &batches = prepared.batches;
    REQUIRE(batches.size() == 1);
    const auto &batch = batches.front();
    RequireParallel(batch);
    REQUIRE(batch.key_parts.size() == 3);

    REQUIRE(batch.record_parts[0] == "metadata");
    REQUIRE(batch.records_ttl == std::vector<uint64_t>{7777, 0, 0});
    REQUIRE(batch.records_ts == std::vector<uint64_t>{1234, 1234, 1234});
    REQUIRE(batch.op_types[0] == EloqDS::WriteOpType::PUT);
    REQUIRE(batch.op_types[1] == EloqDS::WriteOpType::PUT);
    REQUIRE(batch.op_types[2] == EloqDS::WriteOpType::DELETE);
    REQUIRE(batch.record_parts[1].data() ==
            reinterpret_cast<const char *>(page.get()));
    REQUIRE(batch.record_parts[1].size() == 32);
    REQUIRE(batch.record_parts[2].empty());

    PageKeyParts first_page;
    PageKeyParts deleted_page;
    REQUIRE(DecodePageKey(batch.key_parts[1], first_page));
    REQUIRE(DecodePageKey(batch.key_parts[2], deleted_page));
    REQUIRE(first_page.page_id_ == 7);
    REQUIRE(deleted_page.page_id_ == 8);
    REQUIRE(first_page.object_key_ == deleted_page.object_key_);
}

TEST_CASE("Deleting a paged object deletes metadata and every page row",
          "[paged][store-batch]")
{
    ClientFixture fixture;
    auto page = std::shared_ptr<uint8_t[]>(new uint8_t[16]);
    PagedObjectFlush paged;
    paged.metadata_ = "must-not-be-written";
    paged.page_size_ = 16;
    paged.metadata_row_ttl_ = 9999;
    paged.pages_.push_back({1, PageRowKind::HashPage, page});
    paged.pages_.push_back({2, PageRowKind::HashPage, nullptr});

    auto prepared = Prepare(
        fixture.Client(),
        OneRecord(PagedRecord(9, 88, RecordStatus::Deleted, std::move(paged))));
    const auto &batches = prepared.batches;
    REQUIRE(batches.size() == 1);
    const auto &batch = batches.front();
    RequireParallel(batch);
    REQUIRE(batch.key_parts.size() == 3);
    REQUIRE(batch.record_parts == std::vector<std::string_view>{"", "", ""});
    REQUIRE(batch.records_ttl == std::vector<uint64_t>{0, 0, 0});
    REQUIRE(batch.op_types ==
            std::vector<EloqDS::WriteOpType>(3, EloqDS::WriteOpType::DELETE));
}

TEST_CASE("Paged objects are indivisible at every DSS batch boundary",
          "[paged][store-batch]")
{
    ClientFixture fixture;
    const size_t key_size = test::Key(1).Size();

    auto batches_for = [&](size_t first_size)
    {
        std::vector<FlushRecord> records;
        records.emplace_back(PagedRecord(
            1, 10, RecordStatus::Normal, SizedFlush(first_size, key_size)));
        PagedObjectFlush tail;
        tail.metadata_ = "tail";
        tail.page_size_ = 4096;
        records.emplace_back(
            PagedRecord(2, 11, RecordStatus::Normal, std::move(tail)));
        return Prepare(fixture.Client(), std::move(records));
    };

    SECTION("one byte below the limit keeps the next record in the batch")
    {
        auto prepared = batches_for(kWriteBatchLimit - 1);
        const auto &batches = prepared.batches;
        REQUIRE(batches.size() == 1);
        RequireParallel(batches[0]);
        REQUIRE(batches[0].key_parts.size() == 2);
    }
    SECTION("exactly at the limit cuts before the next record")
    {
        auto prepared = batches_for(kWriteBatchLimit);
        const auto &batches = prepared.batches;
        REQUIRE(batches.size() == 2);
        REQUIRE(batches[0].key_parts.size() == 1);
        REQUIRE(batches[1].key_parts.size() == 1);
    }
    SECTION("an oversized object stays whole and cuts before the next record")
    {
        auto prepared = batches_for(kWriteBatchLimit + 1);
        const auto &batches = prepared.batches;
        REQUIRE(batches.size() == 2);
        REQUIRE(batches[0].key_parts.size() == 1);
        REQUIRE(batches[0].record_parts[0].size() > kWriteBatchLimit / 2);
        REQUIRE(batches[1].key_parts.size() == 1);
    }
}
}  // namespace txservice
