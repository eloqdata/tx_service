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
#include <catch2/catch_all.hpp>
#include <cstddef>
#include <memory>
#include <string>

#include "cc/ccm_scanner.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
namespace
{
using Key = CompositeKey<std::string>;

class CountedRecord : public CompositeRecord<std::string>
{
public:
    CountedRecord() = default;
    explicit CountedRecord(size_t *destroyed)
        : CompositeRecord<std::string>(std::string(64 * 1024, 'v')),
          destroyed_(destroyed)
    {
    }

    ~CountedRecord() override
    {
        if (destroyed_ != nullptr)
        {
            ++*destroyed_;
        }
    }

private:
    size_t *destroyed_{nullptr};
};

using Cache = TemplateScanCache<Key, CountedRecord>;
}  // namespace

TEST_CASE("Idle hash scanners release memory and recycled KV records",
          "[scanner-pool]")
{
    size_t destroyed = 0;
    auto shared = std::make_shared<CountedRecord>(&destroyed);
    HashParitionCcScanner<Key, CountedRecord> scanner(
        ScanDirection::Forward, ScanIndexType::Primary, nullptr);
    auto *shard0 = scanner.GetShardCache(0);
    shard0->memory_cache_->AddScanTuple()->SetRecord(shared);
    shard0->GetOrCreateKvCache(1, &scanner, nullptr, 1)
        ->AddScanTuple()
        ->SetRecord(std::make_unique<CountedRecord>(&destroyed));
    shard0->Recycle();
    REQUIRE(shard0->free_cache_pool_.size() == 1);

    auto *shard1 = scanner.GetShardCache(1);
    shard1->GetOrCreateKvCache(2, &scanner, nullptr, 1)
        ->AddScanTuple()
        ->SetRecord(std::make_unique<CountedRecord>(&destroyed));

    // Close also occurs between live scan plans. Payload release belongs to
    // the terminal transition into the scanner pool.
    CcScanner &base = scanner;
    base.Close();
    REQUIRE(destroyed == 0);
    base.ReleaseCaches();
    REQUIRE(destroyed == 2);
    REQUIRE(shared.use_count() == 1);
    REQUIRE(scanner.ShardCount() == 0);
    REQUIRE(scanner.Status() == ScannerStatus::Closed);
    shared.reset();
    REQUIRE(destroyed == 3);

    scanner.Reset(nullptr);
    auto *reused = scanner.GetShardCache(0);
    reused->memory_cache_->AddScanTuple()->SetRecord(
        std::make_unique<CountedRecord>(&destroyed));
    scanner.Close();
    scanner.ReleaseCaches();
    REQUIRE(destroyed == 4);
}

TEST_CASE("Idle range scanners release tuple owners and remain reusable",
          "[scanner-pool]")
{
    size_t destroyed = 0;
    RangePartitionedCcmScanner<Key, CountedRecord, true> scanner(
        ScanDirection::Forward, ScanIndexType::Primary, nullptr);
    auto *cache = static_cast<Cache *>(scanner.Cache(0));
    cache->AddScanTuple()->SetRecord(
        std::make_unique<CountedRecord>(&destroyed));
    scanner.ResetCaches();
    REQUIRE(destroyed == 0);
    REQUIRE(cache->At(0)->Record() != nullptr);

    CcScanner &base = scanner;
    base.Close();
    REQUIRE(destroyed == 0);
    base.ReleaseCaches();
    REQUIRE(destroyed == 1);
    REQUIRE(cache->Size() == 0);
    REQUIRE(scanner.Status() == ScannerStatus::Closed);

    scanner.Reset(nullptr);
    scanner.SetStatus(ScannerStatus::Open);
    cache->AddScanTuple()->SetRecord(
        std::make_unique<CountedRecord>(&destroyed));
    scanner.Close();
    scanner.ReleaseCaches();
    REQUIRE(destroyed == 2);
}
}  // namespace txservice
