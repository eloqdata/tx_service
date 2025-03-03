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

namespace txservice
{

void PrepareCcMap(
    TemplateCcMap<CompositeKey<std::string, int>, CompositeRecord<int>> &cc_map,
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
    std::vector<std::unique_ptr<
        TemplateCcMap<CompositeKey<std::string, int>, CompositeRecord<int>>>>
        ccmaps;
    std::vector<std::vector<CompositeKey<std::string, int>>> map_keys;
    std::vector<std::vector<CompositeKey<std::string, int> *>> map_key_ptrs;

    for (size_t i = 0; i < MAP_NUM; i++)
    {
        tables.emplace_back("t" + std::to_string(i));
        table_names.emplace_back(tables[i], TableType::Primary);
        ccmaps.emplace_back(
            std::make_unique<TemplateCcMap<CompositeKey<std::string, int>,
                                           CompositeRecord<int>>>(
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

}  // namespace txservice

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int ret = Catch::Session().run(argc, argv);
    return ret;
}
