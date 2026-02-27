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
#include <catch2/catch_all.hpp>
#include <chrono>
#include <random>

#include "cc_entry.h"
#include "cc_shard.h"
#include "local_cc_shards.h"
#include "template_cc_map.h"
#include "tx_key.h"     // CompositeKey
#include "tx_record.h"  // CompositeRecord
#include "tx_service_common.h"
#include "type.h"

namespace txservice
{

void PrepareCcMap(TemplateCcMap<CompositeKey<std::string, int>,
                                CompositeRecord<int>,
                                true,
                                true> &cc_map,
                  size_t cnt,
                  std::string &table_name,
                  bool random = true)
{
    LOG(INFO) << "preparing ccmap of table: " << table_name;
    std::vector<CompositeKey<std::string, int>> t1_keys;
    for (size_t i = 0; i < cnt; i++)
    {
        t1_keys.emplace_back(std::make_tuple(table_name, i));
    }
    std::vector<CompositeKey<std::string, int> *> t1_key_ptrs;
    for (auto &key : t1_keys)
    {
        t1_key_ptrs.emplace_back(&key);
    }
    if (random)
    {
        auto rng = std::default_random_engine{};
        std::shuffle(t1_key_ptrs.begin(), t1_key_ptrs.end(), rng);
    }
    LOG(INFO) << "BulkEmplace into: " << table_name;
    auto start = std::chrono::high_resolution_clock::now();
    bool ok = cc_map.BulkEmplaceForTest(t1_key_ptrs);
    auto stop = std::chrono::high_resolution_clock::now();
    int64_t ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start)
            .count();
    LOG(INFO) << (random ? "randomly" : "sequentially") << " inserting " << cnt
              << " keys into ccmap takes: " << ms << " ms";
    REQUIRE(ok == true);
}

TEST_CASE("CcPage clean tests", "[cc-page]")
{
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
        {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> tx_cnf{{"node_memory_limit_mb", 1000},
                                           {"enable_key_cache", 0},
                                           {"reltime_sampling", 0},
                                           {"range_split_worker_num", 1},
                                           {"core_num", 1},
                                           {"realtime_sampling", 0},
                                           {"checkpointer_interval", 10},
                                           {"enable_shard_heap_defragment", 0},
                                           {"node_log_limit_mb", 1000}};
    LocalCcShards local_cc_shards(
        0, 0, tx_cnf, nullptr, nullptr, &ng_configs, 2, nullptr, nullptr, true);
    CcShard shard(0,
                  1,
                  10000,
                  10000,
                  false,
                  0,
                  local_cc_shards,
                  nullptr,
                  nullptr,
                  &ng_configs,
                  2);
    shard.Init();
    std::string raft_path("");
    Sharder::Instance(0,
                      &ng_configs,
                      0,
                      nullptr,
                      nullptr,
                      &local_cc_shards,
                      nullptr,
                      &raft_path);

    const size_t MAP_NUM = 20;
    const size_t MAP_SIZE = 10000;
    std::vector<std::string> tables;
    std::vector<TableName> table_names;
    std::vector<std::unique_ptr<TemplateCcMap<CompositeKey<std::string, int>,
                                              CompositeRecord<int>,
                                              true,
                                              true>>>
        ccmaps;
    std::vector<std::vector<CompositeKey<std::string, int>>> map_keys;
    std::vector<std::vector<CompositeKey<std::string, int> *>> map_key_ptrs;

    for (size_t i = 0; i < MAP_NUM; i++)
    {
        tables.emplace_back("t" + std::to_string(i));
        table_names.emplace_back(
            tables[i], TableType::Primary, TableEngine::EloqSql);
        ccmaps.emplace_back(
            std::make_unique<TemplateCcMap<CompositeKey<std::string, int>,
                                           CompositeRecord<int>,
                                           true,
                                           true>>(
                &shard, 0, table_names[i], 1, nullptr, true));
    }

    for (size_t i = 0; i < MAP_NUM; i++)
    {
        auto &cc_map = *ccmaps[i];
        PrepareCcMap(cc_map, MAP_SIZE, tables[i], i & 1);
    }

    for (auto &up : ccmaps)
    {
        auto &cc_map = *up;
        size_t size = cc_map.VerifyOrdering();
        REQUIRE(size == MAP_SIZE);
    }

    shard.VerifyLruList();

    LOG(INFO) << "clean all freeable entries...";
    size_t total_free = 0;
    while (true)
    {
        auto [free_cnt, yield] = shard.Clean();
        shard.VerifyLruList();
        if (free_cnt == 0)
        {
            break;
        }
        total_free += free_cnt;
    }
    LOG(INFO) << "total freed: " << total_free;
    shard.VerifyLruList();

    size_t total_remain = 0;
    for (size_t i = 0; i < MAP_NUM; i++)
    {
        auto &cc_map = *ccmaps.at(i);
        size_t remain = cc_map.VerifyOrdering();
        LOG(INFO) << "after clean, ccmap of table " << tables[i]
                  << " remain: " << remain;
        total_remain += remain;
    }

    local_cc_shards.Terminate();

    REQUIRE(total_remain + total_free == MAP_NUM * MAP_SIZE);
}

// A CompositeRecord<int> subclass that reports an artificially large payload
// size. Used to test the payload-size-aware cache eviction protection.
struct LargeCompositeRecord : public CompositeRecord<int>
{
    explicit LargeCompositeRecord(int val, size_t reported_size)
        : CompositeRecord<int>(val), reported_size_(reported_size)
    {
    }

    size_t Size() const override
    {
        return reported_size_;
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<LargeCompositeRecord>(*this);
    }

    size_t reported_size_;
};

TEST_CASE("Large-value eviction protection test", "[cc-page]")
{
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
        {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
    std::map<std::string, uint32_t> tx_cnf{{"node_memory_limit_mb", 1000},
                                           {"enable_key_cache", 0},
                                           {"reltime_sampling", 0},
                                           {"range_split_worker_num", 1},
                                           {"core_num", 1},
                                           {"realtime_sampling", 0},
                                           {"checkpointer_interval", 10},
                                           {"enable_shard_heap_defragment", 0},
                                           {"node_log_limit_mb", 1000}};
    LocalCcShards local_cc_shards(
        0, 0, tx_cnf, nullptr, nullptr, &ng_configs, 2, nullptr, nullptr, true);
    CcShard shard(0,
                  1,
                  10000,
                  10000,
                  false,
                  0,
                  local_cc_shards,
                  nullptr,
                  nullptr,
                  &ng_configs,
                  2);
    shard.Init();
    std::string raft_path("");
    Sharder::Instance(0,
                      &ng_configs,
                      0,
                      nullptr,
                      nullptr,
                      &local_cc_shards,
                      nullptr,
                      &raft_path);

    const size_t MAP_SIZE = 200;
    const size_t LARGE_PAYLOAD_SIZE = 1024;

    using TestCcMap = TemplateCcMap<CompositeKey<std::string, int>,
                                    CompositeRecord<int>,
                                    true,
                                    true>;

    // Small-value map – pages stay in the head (small-value) zone.
    std::string small_table = "small_val_test";
    TableName small_tname(
        small_table, TableType::Primary, TableEngine::EloqSql);
    auto small_map =
        std::make_unique<TestCcMap>(&shard, 0, small_tname, 1, nullptr, true);

    // Large-value map – pages will be re-zoned to the tail (large-value) zone.
    std::string large_table = "large_val_test";
    TableName large_tname(
        large_table, TableType::Primary, TableEngine::EloqSql);
    auto large_map =
        std::make_unique<TestCcMap>(&shard, 0, large_tname, 1, nullptr, true);

    auto make_keys = [](const std::string &tname,
                        size_t cnt,
                        std::vector<CompositeKey<std::string, int>> &storage)
        -> std::vector<CompositeKey<std::string, int> *>
    {
        for (size_t i = 0; i < cnt; i++)
        {
            storage.emplace_back(std::make_tuple(tname, static_cast<int>(i)));
        }
        std::vector<CompositeKey<std::string, int> *> ptrs;
        for (auto &k : storage)
        {
            ptrs.push_back(&k);
        }
        return ptrs;
    };

    // Insert entries for both maps.  Both maps start in the small-value zone
    // because has_large_value_ is false on insertion.
    std::vector<CompositeKey<std::string, int>> small_keys;
    REQUIRE(small_map->BulkEmplaceFreeForTest(
        make_keys(small_table, MAP_SIZE, small_keys)));
    REQUIRE(small_map->VerifyOrdering() == MAP_SIZE);

    std::vector<CompositeKey<std::string, int>> large_keys;
    REQUIRE(large_map->BulkEmplaceFreeForTest(
        make_keys(large_table, MAP_SIZE, large_keys)));
    REQUIRE(large_map->VerifyOrdering() == MAP_SIZE);

    // Assign large payloads to the large-value map entries.
    auto large_payload =
        std::make_shared<LargeCompositeRecord>(42, LARGE_PAYLOAD_SIZE);
    large_map->SetPayloadForTest(large_payload);

    txservice_large_value_threshold = LARGE_PAYLOAD_SIZE / 2;

    // -----------------------------------------------------------------------
    // PART 1: Zone-separation structure.
    // -----------------------------------------------------------------------
    // Use RezoneAsLargeValueForTest() to set has_large_value_ on large_map
    // pages and call UpdateLruList to move them into the large-value zone.
    // This simulates what happens in production when those pages are accessed
    // after CanBeCleaned has set has_large_value_ on them.
    large_map->RezoneAsLargeValueForTest();
    shard.VerifyLruList();

    // lru_large_value_zone_head_ must now point into the large-value zone.
    const LruPage *zone_head = shard.LruLargeValueZoneHead();
    REQUIRE(zone_head != nullptr);
    REQUIRE(zone_head->parent_map_ != nullptr);  // not a sentinel

    // Walk the LRU list and verify:
    //   head → [small_map pages] → zone_head → [large_map pages] → tail
    {
        bool in_large_zone = false;
        for (const LruPage *p = shard.LruHead()->lru_next_;
             p->parent_map_ != nullptr;  // sentinel tail has parent_map_==null
             p = p->lru_next_)
        {
            if (p == zone_head)
            {
                in_large_zone = true;
            }
            if (in_large_zone)
            {
                REQUIRE(p->parent_map_ == large_map.get());
            }
            else
            {
                REQUIRE(p->parent_map_ == small_map.get());
            }
        }
        // We must have entered the large zone.
        REQUIRE(in_large_zone);
    }

    // -----------------------------------------------------------------------
    // PART 2: Small-value insertion stays before the zone head.
    // -----------------------------------------------------------------------
    const LruPage *zone_head_before = shard.LruLargeValueZoneHead();
    CompositeKey<std::string, int> extra_sv_key =
        std::make_tuple(small_table, static_cast<int>(MAP_SIZE + 1));
    std::vector<CompositeKey<std::string, int> *> extra_sv_ptr = {
        &extra_sv_key};
    REQUIRE(small_map->BulkEmplaceFreeForTest(extra_sv_ptr));
    shard.VerifyLruList();
    // SV insertion must not change the zone head.
    REQUIRE(shard.LruLargeValueZoneHead() == zone_head_before);

    // -----------------------------------------------------------------------
    // PART 3: Full scan – all pages are evicted.
    // -----------------------------------------------------------------------
    size_t total_freed = 0;
    while (true)
    {
        auto [free_cnt, yield] = shard.Clean();
        shard.VerifyLruList();
        total_freed += free_cnt;
        if (free_cnt == 0)
        {
            break;
        }
    }
    REQUIRE(total_freed == MAP_SIZE + MAP_SIZE + 1 /* extra sv */);

    // Restore global defaults.
    txservice_large_value_threshold = 0;
    local_cc_shards.Terminate();
}

}  // namespace txservice

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int ret = Catch::Session().run(argc, argv);
    return ret;
}
