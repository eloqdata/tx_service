#include <atomic>
#include <catch2/catch_all.hpp>
#include <string>
#include <vector>

#include "cc/cc_entry.h"
#include "cc/cc_req_misc.h"
#include "cc/local_cc_shards.h"
#include "harness/test_node.h"
#include "read_write_set.h"
#include "sharder.h"
#include "tx_request.h"

using namespace txservice;
using namespace txservice::test;

// IMPORTANT: at most one TestNode may be constructed per process (the engine's
// Sharder is a process-global singleton whose brpc servers register their
// services once; a second TestNode fails to re-Start). Catch2 re-runs a
// TEST_CASE body once per leaf SECTION, which would reconstruct the TestNode,
// so this file uses a SINGLE TestNode and drives every scenario as a sequential
// scoped block (distinct keys per scenario), with NO SECTIONs.

namespace
{
LruEntry *CceOwner(const CcEntryAddr &cce_addr)
{
    std::atomic<LruEntry *> owner{nullptr};
    WaitableCc check_owner(
        [&cce_addr, &owner](CcShard &)
        {
            owner.store(cce_addr.ExtractCce(), std::memory_order_release);
            return true;
        });

    Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(cce_addr.CoreId(),
                                                             &check_owner);
    check_owner.Wait();
    REQUIRE_FALSE(check_owner.IsError());
    return owner.load(std::memory_order_acquire);
}

struct ClosedScanRead
{
    CcEntryAddr cce_addr_;
    LruEntry *cce_;
    TxNumber txn_;
};

bool TxOwnsScanRead(const ClosedScanRead &read)
{
    std::atomic<bool> owns_read{false};
    WaitableCc check_ownership(
        [&read, &owns_read](CcShard &)
        {
            NonBlockingLock *key_lock = read.cce_->GetKeyLock();
            if (key_lock != nullptr)
            {
                const auto &read_locks = key_lock->ReadLocks();
                const auto &read_intents = key_lock->ReadIntents();
                owns_read.store(
                    read_locks.find(read.txn_) != read_locks.end() ||
                        read_intents.find(read.txn_) != read_intents.end(),
                    std::memory_order_release);
            }
            return true;
        });

    Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(
        read.cce_addr_.CoreId(), &check_ownership);
    check_ownership.Wait();
    REQUIRE_FALSE(check_ownership.IsError());
    return owns_read.load(std::memory_order_acquire);
}

ClosedScanRead ScanOneAndClose(TestNode &node, TxHandle &tx, int key)
{
    TxKey start_key(CompositeKey<int>::NegativeInfinity());
    TxKey end_key(CompositeKey<int>::PositiveInfinity());
    TxKey target_key = Key(key);

    BucketScanSavePoint save_point;
    ScanOpenTxRequest open_req(&node.Table(),
                               node.SchemaVersion(),
                               ScanIndexType::Primary,
                               &start_key,
                               true,
                               &end_key,
                               true,
                               ScanDirection::Forward);
    open_req.bucket_scan_save_point_ = &save_point;
    uint64_t alias = tx.Txm()->OpenTxScan(open_req);
    open_req.Wait();
    REQUIRE_FALSE(open_req.IsError());

    std::vector<ScanBatchTuple> batch;
    size_t target_idx = 0;
    bool found = false;
    for (size_t plan_idx = 0; plan_idx < save_point.PlanSize() && !found;
         ++plan_idx)
    {
        BucketScanPlan plan = save_point.PickPlan(plan_idx);
        bool plan_finished = false;
        while (!found && !plan_finished)
        {
            ScanBatchTxRequest batch_req(alias, node.Table(), &batch);
            batch_req.bucket_scan_plan_ = &plan;
            tx.Txm()->Execute(&batch_req);
            batch_req.Wait();
            REQUIRE_FALSE(batch_req.IsError());
            plan_finished = batch_req.Result();

            for (target_idx = 0; target_idx < batch.size(); ++target_idx)
            {
                if (batch[target_idx].key_ == target_key)
                {
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                batch.clear();
            }
        }
    }
    REQUIRE(found);
    REQUIRE_FALSE(batch[target_idx].cce_addr_.Empty());

    CcEntryAddr cce_addr = batch[target_idx].cce_addr_;
    ClosedScanRead read{cce_addr, CceOwner(cce_addr), tx.Txm()->TxNumber()};
    REQUIRE(read.cce_ != nullptr);
    REQUIRE(TxOwnsScanRead(read));

    ScanCloseTxRequest close_req(batch, target_idx, alias, node.Table());
    tx.Txm()->Execute(&close_req);
    close_req.Wait();
    REQUIRE_FALSE(close_req.IsError());
    return read;
}
}  // namespace

TEST_CASE("transaction consistency on TestNode", "[tx]")
{
    TestNode node(TestNodeOptions{}.CoreNum(2));

    // Scanner-only reads must not retain a view into a pooled request's table
    // name. Short names exercise the in-object string buffer deterministically:
    // reassigning pooled_name overwrites that buffer rather than merely making
    // a dangling heap view.
    {
        ReadWriteSet read_set;
        CcEntryAddr release_addr;
        release_addr.SetCceLock(1, 1, 1, 1);

        TableName pooled_name(
            std::string("a"), TableType::Primary, node.Table().Engine());
        TableName original_name(pooled_name);
        read_set.AddReadForRelease(release_addr, pooled_name);
        REQUIRE(read_set.DataReadSetSize() == 1);

        pooled_name = TableName(
            std::string("b"), TableType::Primary, node.Table().Engine());
        REQUIRE(read_set.DataReadSet().begin()->second.second ==
                original_name.StringView());

        read_set.ClearReadSet(original_name);
        REQUIRE(read_set.DataReadSetSize() == 0);
    }

    // Scenario 1: an aborted write is invisible (key 10). A committed reader
    // started after the abort must not observe the rolled-back value.
    {
        auto t = node.BeginTx();
        REQUIRE(t.Upsert(10, 200));
        REQUIRE(t.Abort());
    }
    {
        auto t = node.BeginTx();
        int v = 0;
        REQUIRE_FALSE(t.Read(10, v));  // absent after abort
        REQUIRE(t.Commit());
    }

    // Scenario 2: a committed write IS visible to a later transaction (key 12).
    {
        auto t = node.BeginTx();
        REQUIRE(t.Upsert(12, 120));
        REQUIRE(t.Commit());
    }
    {
        auto t = node.BeginTx();
        int v = 0;
        REQUIRE(t.Read(12, v));
        REQUIRE(v == 120);
        REQUIRE(t.Commit());
    }

    // Scenario 3: an OCC read-write conflict aborts exactly one writer
    // (key 11). Two concurrent transactions each read-then-write key 11.
    //
    // The conflict is detected by OCC *read* validation, not by write-lock
    // contention: the helper's Upsert only buffers into the write set (write
    // locks are taken during commit), and because this driver commits the two
    // transactions sequentially, t1 fully commits and releases its locks before
    // t2's commit begins -- so there is never write-lock contention. What makes
    // this a real conflict is the read: under RepeatableRead + OccRead a read
    // takes a ReadIntent and is recorded in the read set (under Snapshot the
    // read is LockType::NoLock and leaves no validatable footprint, so both
    // writers would commit). Both txns read key 11 at the same version; t1
    // commits and bumps that version; t2's commit-time validation sees its
    // read-set entry for key 11 is now stale and aborts t2.
    //
    // Key 11 is pre-populated so the read returns a concrete version to
    // validate against.
    {
        auto t = node.BeginTx();
        REQUIRE(t.Upsert(11, 0));
        REQUIRE(t.Commit());
    }
    {
        auto t1 =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        auto t2 =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);

        // Each tx reads key 11 (ReadIntent -> read set), establishing the
        // version it expects to still hold at commit time.
        int v1 = 0;
        int v2 = 0;
        REQUIRE(t1.Read(11, v1));
        REQUIRE(t2.Read(11, v2));

        REQUIRE(t1.Upsert(11, 1));
        REQUIRE(t2.Upsert(11, 2));

        bool c1 = t1.Commit();
        bool c2 = t2.Commit();
        REQUIRE(c1 != c2);  // exactly one commits; the other fails validation

        // The winning writer's value must be visible afterwards (t1 wrote 1,
        // t2 wrote 2). This confirms the aborted writer's post-abort cleanup
        // left the correct value in place rather than its own buffered write.
        {
            auto t = node.BeginTx();
            int v = 0;
            REQUIRE(t.Read(11, v));
            REQUIRE(v == (c1 ? 1 : 2));
            REQUIRE(t.Commit());
        }
    }

    bool commit_scan_retained = false;
    bool abort_scan_retained = false;

    // Scenario 4: scan close retains the transaction's read ownership until
    // commit validation releases it. Lock state is observed only on the CCE's
    // owner shard.
    {
        auto seed = node.BeginTx();
        REQUIRE(seed.Upsert(20, 200));
        REQUIRE(seed.Upsert(22, 220));
        REQUIRE(seed.Commit());

        auto tx =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        ClosedScanRead scanned = ScanOneAndClose(node, tx, 20);

        // On pre-fix code, the next tx request cannot finish until the
        // scan-close release operation has completed. Inspecting the captured
        // CCE avoids ambiguity if its pooled lock is reused by this barrier
        // read.
        int barrier_value = 0;
        REQUIRE(tx.Read(22, barrier_value));
        REQUIRE(barrier_value == 220);

        commit_scan_retained = TxOwnsScanRead(scanned);
        REQUIRE(tx.Commit());
        REQUIRE_FALSE(TxOwnsScanRead(scanned));
    }

    // Scenario 5: abort uses the same final read-set cleanup path.
    {
        auto seed = node.BeginTx();
        REQUIRE(seed.Upsert(21, 210));
        REQUIRE(seed.Upsert(23, 230));
        REQUIRE(seed.Commit());

        auto tx =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        ClosedScanRead scanned = ScanOneAndClose(node, tx, 21);

        int barrier_value = 0;
        REQUIRE(tx.Read(23, barrier_value));
        REQUIRE(barrier_value == 230);

        abort_scan_retained = TxOwnsScanRead(scanned);
        REQUIRE(tx.Abort());
        REQUIRE_FALSE(TxOwnsScanRead(scanned));
    }

    REQUIRE(commit_scan_retained);
    REQUIRE(abort_scan_retained);
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
