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
#include <iostream>

#include "harness/test_node.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_start_ts_collector.h"

using namespace txservice;
using namespace txservice::test;

TEST_CASE("TxStartTsCollector GlobalMinSiTxStartTs unit test",
          "[start-ts-collector]")
{
    std::cout << "===== TEST STARTING =====" << std::endl;

    uint32_t core_num = 3;
    TestNode node(TestNodeOptions{}.CoreNum(core_num));
    TxService *tx_service_ = node.Service();

    TxStartTsCollector::Instance().SetDelaySeconds(2);

    // NOTE: the sleeps in this test are intentional and semantic, not arbitrary
    // synchronization. TxStartTsCollector samples active-tx start timestamps on
    // a periodic background interval (SetDelaySeconds above); the test must let
    // wall-clock time advance both to give transactions distinct ts_base_
    // values and to let the collector run a sampling pass before asserting the
    // global min it collected. These are the StartTsCollector-specific timing
    // waits explicitly exempted from the "no fixed sleeps" guideline.
    sleep(3);

    //== Init several transactions
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
        sleep(3);  // to use different value of ts_base_
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

    // TestNode destructor handles teardown (no explicit Shutdown needed).
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
