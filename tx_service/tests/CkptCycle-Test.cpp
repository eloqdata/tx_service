/**
 *    Copyright (C) 2026 EloqData Inc.
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

/**
 * @file CkptCycle-Test.cpp
 * @brief The node's concluded checkpoint cycle count on the live
 * checkpointer: a round that finds nothing to flush concludes a cycle, and a
 * round skipped because a write lock pins the checkpoint timestamp concludes
 * one too. These are the paths the shard cleaner's park bound depends on.
 *
 * The node is store-backed so the checkpointer thread runs, and the test
 * never dirties the table: the in-memory store cannot flush the mock table's
 * record type, so a dirty entry would abort the shutdown round. A write lock
 * taken without a post-write leaves the entry with nothing to flush while
 * pinning the timestamp, which is all the skip path needs. One TestNode per
 * process.
 */

#include <catch2/catch_all.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>
#include <vector>

// The stall counter is private to the checkpointer and the lock release is
// protected on the map; the existing cleaner tests open members the same way.
#define private public
#define protected public
#include "cc/cc_map.h"
#include "cc/local_cc_shards.h"
#include "cc/template_cc_map.h"
#include "checkpointer.h"
#undef protected
#undef private

#include "cc/cc_entry.h"
#include "cc/cc_handler_result.h"
#include "cc/cc_req_misc.h"
#include "cc/cc_request.h"
#include "cc/cc_shard.h"
#include "cc/non_blocking_lock.h"
#include "cc_protocol.h"
#include "harness/test_node.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "type.h"

using namespace std::chrono_literals;
using namespace txservice;
using namespace txservice::test;

namespace
{
constexpr NodeGroupId kNodeGroup = 0;
constexpr uint32_t kShardCode = 0;

/** Runs @p task on shard 0 and waits for it. */
void RunOnShard(std::function<void(CcShard &)> task)
{
    WaitableCc cc(
        [task = std::move(task)](CcShard &shard)
        {
            task(shard);
            return true;
        });
    Sharder::Instance().GetLocalCcShards()->EnqueueToCcShard(0, &cc);
    cc.Wait();
    REQUIRE_FALSE(cc.IsError());
}

/** Polls @p done until it holds or @p budget elapses. */
bool WaitFor(const std::function<bool()> &done,
             std::chrono::milliseconds budget)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done())
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return false;
        }
        std::this_thread::sleep_for(20ms);
    }
    return true;
}

/** A write-lock acquisition on one key for a live transaction. */
struct Acquire
{
    Acquire(const TableName &table,
            uint64_t schema_version,
            int key,
            TransactionExecution *txm)
        : key_(Key(key)), result_(nullptr)
    {
        result_.Value().resize(1);
        result_.Value()[0].Reset();
        req_.Use();
        req_.Reset(&table,
                   schema_version,
                   &key_,
                   kShardCode,
                   txm->TxNumber(),
                   txm->TxTerm(),
                   /*ts=*/1,
                   /*is_insert=*/false,
                   &result_,
                   /*hd_res_idx=*/0,
                   CcProtocol::Locking,
                   IsolationLevel::RepeatableRead,
                   /*abort_if_oom=*/false,
                   /*reserve_bytes=*/0);
    }

    LruEntry *Entry() const
    {
        return result_.Value()[0].cce_addr_.ExtractCce();
    }

    TxKey key_;
    CcHandlerResult<std::vector<AcquireKeyResult>> result_;
    AcquireCc req_;
};
}  // namespace

TEST_CASE("checkpoint cycles conclude on the live checkpointer", "[ckpt-cycle]")
{
    TestNode node(TestNodeOptions{}.CoreNum(1).StoreBacked(true));
    Checkpointer *ckpter = Sharder::Instance().GetCheckpointer();
    REQUIRE(ckpter != nullptr);

    // Nothing to flush: a requested round over the node's one group concludes
    // a cycle through the real run and its status.
    {
        const uint64_t before =
            Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles();
        ckpter->Notify(true, "ckpt_cycle_test");
        REQUIRE(WaitFor(
            [&]
            {
                return Sharder::Instance()
                           .GetCheckpointer()
                           ->ConcludedCkptCycles() > before;
            },
            30s));
    }

    // Create the table's cc map on the shard without dirtying anything: a read
    // of an absent key resolves in memory.
    {
        auto t = node.BeginTx();
        int v = 0;
        REQUIRE_FALSE(t.Read(42, v));
        REQUIRE(t.Commit());
    }

    // A granted write lock with no post-write pins the checkpoint timestamp
    // below its grant and leaves the entry with nothing to flush. The first
    // round under it advances the timestamp to just below the grant and
    // concludes; every later round cannot advance, is skipped before any
    // flush, and concludes too. Two concluded cycles under the lock therefore
    // prove the skip path.
    auto tx = node.BeginTx();
    Acquire held(node.Table(), node.SchemaVersion(), 7, tx.Txm());
    CcMap *map = nullptr;
    RunOnShard(
        [&](CcShard &s)
        {
            map = s.GetCcm(node.Table(), kNodeGroup);
            REQUIRE(map != nullptr);
            REQUIRE(map->Execute(held.req_));
            REQUIRE_FALSE(held.result_.IsError());
            REQUIRE(held.Entry()->GetKeyLock()->HasWriteLock());
        });
    {
        const uint64_t before =
            Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles();
        const uint32_t stalled_before =
            ckpter->ckpt_stall_states_[kNodeGroup].stall_rounds_;
        // Rounds come from the periodic timer (10 s in the TestNode
        // configuration) and from requests, which the checkpointer accepts at
        // most once per its minimum interval.
        REQUIRE(WaitFor(
            [&]
            {
                ckpter->Notify(true, "ckpt_cycle_test");
                return Sharder::Instance()
                           .GetCheckpointer()
                           ->ConcludedCkptCycles() >= before + 2;
            },
            60s));
        REQUIRE(ckpter->ckpt_stall_states_[kNodeGroup].stall_rounds_ >
                stalled_before);
    }

    RunOnShard(
        [&](CcShard &)
        {
            LruEntry *entry = held.Entry();
            map->ReleaseCceLock(entry->GetKeyLock(),
                                entry,
                                tx.Txm()->TxNumber(),
                                kNodeGroup,
                                LockType::WriteLock);
        });
    REQUIRE(tx.Abort());

    // Unpinned again: the next round advances and concludes.
    {
        const uint64_t before =
            Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles();
        REQUIRE(WaitFor(
            [&]
            {
                ckpter->Notify(true, "ckpt_cycle_test");
                return Sharder::Instance()
                           .GetCheckpointer()
                           ->ConcludedCkptCycles() > before;
            },
            30s));
    }
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
