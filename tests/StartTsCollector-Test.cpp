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
#include <filesystem>
#include <iostream>

#include "include/mock/mock_catalog_factory.h"
#include "store/int_mem_store.h"
#include "tx_execution.h"
#include "tx_service.h"
#include "tx_start_ts_collector.h"

using namespace txservice;

static std::unique_ptr<store::IntMemoryStore> store_hd =
    std::make_unique<store::IntMemoryStore>();
static MockCatalogFactory mock_catalog_factory{};
static std::vector<std::string> ips{"127.0.0.1"};
static std::vector<std::string> tx_ips{"127.0.0.1"};
static std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs{
    {0, {NodeConfig(0, "127.0.0.1", 8600)}}};
static std::vector<uint16_t> ports{8600};
static std::vector<uint16_t> tx_ports{8602};

uint64_t cluster_config_version = 2;

TEST_CASE("TxStartTsCollector GlobalMinSiTxStartTs unit test",
          "[start-ts-collector]")
{
    std::cout << "===== TEST STARTING =====" << std::endl;

    //== Create and start TxService
    std::filesystem::path output_dir = std::filesystem::path("/tmp/");
    output_dir.append("test_output");
    std::string cluster_config_path =
        output_dir.string() + "/cluster_config.json";
    std::filesystem::create_directory(output_dir);
    // braft need: must add protocol
    std::string local_path = "local://" + output_dir.string();
    std::cout << "output_dir: " << local_path << std::endl;
    uint32_t node_id = 0;
    uint32_t ng_id = 0;
    uint32_t core_num = 3;

    std::map<std::string, uint32_t> tx_service_conf;
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("core_num", core_num));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("range_split_worker_num", 0));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("node_memory_limit_mb", 1000));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("node_log_limit_mb", 1000));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("realtime_sampling", 0));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("checkpointer_interval", 10));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("checkpointer_delay_seconds", 0));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("enable_shard_heap_defragment", 0));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("enable_key_cache", 0));

    tx_service_conf.insert(std::pair<std::string, uint32_t>(
        "collect_active_tx_ts_interval_seconds", 2));
    tx_service_conf.insert(
        std::pair<std::string, uint32_t>("rep_group_cnt", 3));
    CatalogFactory *catalog_factory[4] = {
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
        &mock_catalog_factory,
    };

    std::unique_ptr<TxService> tx_service_ =
        std::make_unique<TxService>(catalog_factory,
                                    &MockSystemHandler::Instance(),
                                    tx_service_conf,
                                    node_id,
                                    ng_id,
                                    &ng_configs,
                                    cluster_config_version,
                                    store_hd.get(),
                                    nullptr,
                                    true,
                                    true);

    tx_service_->Start(node_id,
                       ng_id,
                       &ng_configs,
                       cluster_config_version,
                       &tx_ips,
                       &tx_ports,
                       nullptr,
                       nullptr,
                       nullptr,
                       tx_service_conf,
                       nullptr,
                       local_path,
                       cluster_config_path);

    TxStartTsCollector::Instance().SetDelaySeconds(2);

    sleep(3);

    //== Init serveral transactions
    size_t tx_count = 2 * core_num;
    std::vector<TransactionExecution *> txs;
    txs.resize(tx_count);
    std::vector<InitTxRequest *> init_tx_reqs;
    init_tx_reqs.resize(tx_count);

    for (size_t i = 0; i < tx_count; i++)
    {
        init_tx_reqs[i] =
            new InitTxRequest(IsolationLevel::Snapshot, CcProtocol::OccRead);
        init_tx_reqs[i]->Reset();
        txs[i] = tx_service_->NewTx();
        // REQUIRE(txs[i]->GetStartTs() == 0U);
        txs[i]->Execute(init_tx_reqs[i]);
        sleep(3);  // to use diffrent value of ts_base_
    }

    uint64_t min_tx_ts = UINT64_MAX;
    size_t min_tx_index = 0;
    for (size_t i = 0; i < tx_count; i++)
    {
        init_tx_reqs[i]->Wait();
        REQUIRE_FALSE(init_tx_reqs[i]->IsError());
        REQUIRE(txs[i]->GetStartTs() > 0U);

        std::cout << "tx#" << i << " start_ts: " << txs[i]->GetStartTs()
                  << std::endl;

        if (min_tx_ts > txs[i]->GetStartTs())
        {
            min_tx_ts = txs[i]->GetStartTs();
            min_tx_index = i;
        }
    }

    sleep(3);  // Assure "TxStartTsCollector" has collected the min start ts.

    uint64_t collected_min_ts =
        TxStartTsCollector::Instance().GlobalMinSiTxStartTs();
    REQUIRE(min_tx_ts == collected_min_ts);

    // commit the tx which is begin first.
    CommitTxRequest commit_req1;
    txs[min_tx_index]->Execute(&commit_req1);
    commit_req1.Wait();
    size_t committed_tx_index = min_tx_index;

    min_tx_ts = UINT64_MAX;
    for (size_t i = 0; i < tx_count; i++)
    {
        if (min_tx_ts > txs[i]->GetStartTs() && i != committed_tx_index)
        {
            min_tx_ts = txs[i]->GetStartTs();
            min_tx_index = i;
        }
    }

    sleep(3);  // Assure "TxStartTsCollector" has collected the min start ts.

    collected_min_ts = TxStartTsCollector::Instance().GlobalMinSiTxStartTs();

    REQUIRE(min_tx_ts == collected_min_ts);

    for (auto *req : init_tx_reqs)
    {
        delete req;
    }

    tx_service_->Shutdown();
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int ret = Catch::Session().run(argc, argv);
    return ret;
}
