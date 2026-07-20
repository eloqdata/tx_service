#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "catch2/catch_all.hpp"
#include "cc/cc_entry.h"
#include "cc/cc_req_misc.h"
#include "cc/cc_request.h"
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

    // Pooled scan-close requests preserve the same long-lived table-name
    // backing used by regular scan reads.
    {
        ReadWriteSet read_set;
        CcEntryAddr release_addr;
        release_addr.SetCceLock(1, 1, 1, 1);

        TableName stable_name(
            std::string("a"), TableType::Primary, node.Table().Engine());
        ScanCloseTxRequest close_req(1, stable_name);
        REQUIRE_FALSE(close_req.table_name_.IsStringOwner());
        REQUIRE(close_req.table_name_.StringView().data() ==
                stable_name.StringView().data());

        REQUIRE(read_set.AddRead(release_addr, 0, &close_req.table_name_));
        REQUIRE(read_set.DataReadSetSize() == 1);

        TableName next_stable_name(
            std::string("b"), TableType::Primary, node.Table().Engine());
        close_req.in_use_.store(false, std::memory_order_relaxed);
        close_req.Reset(2, next_stable_name);
        REQUIRE_FALSE(close_req.table_name_.IsStringOwner());
        REQUIRE(read_set.DataReadSet().begin()->second.second ==
                stable_name.StringView());

        read_set.ClearReadSet(stable_name);
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

    // Scenario 6: when scan post-processing detects a repeatable-read version
    // mismatch, scanner-only locks must move into the read set so Abort can
    // release them.
    {
        int conflict_key = -1;
        int scanner_only_key = -1;
        absl::flat_hash_map<uint16_t, int> first_key_by_bucket;
        for (int key = 1000; scanner_only_key < 0; ++key)
        {
            CompositeKey<int> composite_key{int{key}};
            uint16_t bucket_id =
                Sharder::MapKeyHashToBucketId(composite_key.Hash());
            auto [it, inserted] =
                first_key_by_bucket.try_emplace(bucket_id, key);
            if (!inserted)
            {
                conflict_key = it->second;
                scanner_only_key = key;
            }
        }
        REQUIRE(conflict_key < scanner_only_key);

        auto seed = node.BeginTx();
        REQUIRE(seed.Upsert(conflict_key, conflict_key));
        REQUIRE(seed.Upsert(scanner_only_key, scanner_only_key));
        REQUIRE(seed.Commit());

        auto keeper =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        ClosedScanRead keeper_read =
            ScanOneAndClose(node, keeper, scanner_only_key);

        auto failing_tx =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        const TxNumber failing_txn = failing_tx.Txm()->TxNumber();
        ClosedScanRead failing_read{
            keeper_read.cce_addr_, keeper_read.cce_, failing_txn};
        REQUIRE_FALSE(TxOwnsScanRead(failing_read));

        int old_value = 0;
        REQUIRE(failing_tx.Read(conflict_key, old_value));
        REQUIRE(old_value == conflict_key);

        auto updater = node.BeginTx();
        REQUIRE(updater.Upsert(conflict_key, conflict_key + 1));
        REQUIRE(updater.Commit());

        TxKey start_key = Key(conflict_key);
        TxKey end_key(CompositeKey<int>::PositiveInfinity());
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
        uint64_t alias = failing_tx.Txm()->OpenTxScan(open_req);
        open_req.Wait();
        REQUIRE_FALSE(open_req.IsError());

        bool mismatch_observed = false;
        TxErrorCode mismatch_error = TxErrorCode::NO_ERROR;
        std::vector<ScanBatchTuple> batch;
        for (size_t plan_idx = 0;
             plan_idx < save_point.PlanSize() && !mismatch_observed;
             ++plan_idx)
        {
            BucketScanPlan plan = save_point.PickPlan(plan_idx);
            bool plan_finished = false;
            while (!plan_finished && !mismatch_observed)
            {
                ScanBatchTxRequest batch_req(alias, node.Table(), &batch);
                batch_req.bucket_scan_plan_ = &plan;
                failing_tx.Txm()->Execute(&batch_req);
                batch_req.Wait();
                if (batch_req.IsError())
                {
                    mismatch_observed = true;
                    mismatch_error = batch_req.ErrorCode();
                }
                else
                {
                    plan_finished = batch_req.Result();
                    batch.clear();
                }
            }
        }
        REQUIRE(mismatch_observed);
        REQUIRE(mismatch_error == TxErrorCode::OCC_BREAK_REPEATABLE_READ);
        REQUIRE(TxOwnsScanRead(failing_read));

        REQUIRE(failing_tx.Abort());
        bool scanner_only_lock_left = TxOwnsScanRead(failing_read);

        WaitableCc cleanup(
            [failing_txn](CcShard &ccs)
            {
                ccs.ClearTx(failing_txn);
                return true;
            });
        Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(
            failing_read.cce_addr_.CoreId(), &cleanup);
        cleanup.Wait();
        REQUIRE_FALSE(cleanup.IsError());

        REQUIRE(keeper.Abort());
        REQUIRE_FALSE(TxOwnsScanRead(keeper_read));
        REQUIRE_FALSE(scanner_only_lock_left);
    }

    // Scenario 7: a later batch must not rescan a memory source that Merge
    // already marked finished or acquire another ReadIntent from that source.
    {
        constexpr int scan_key = 30;
        auto seed = node.BeginTx();
        REQUIRE(seed.Upsert(scan_key, scan_key));
        REQUIRE(seed.Commit());

        auto tx =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        ClosedScanRead scanned = ScanOneAndClose(node, tx, scan_key);
        const TxNumber txn = tx.Txm()->TxNumber();
        const NodeGroupId node_group_id = Sharder::Instance().NativeNodeGroup();
        const uint16_t core_id = scanned.cce_addr_.CoreId();
        const uint16_t bucket_id = Sharder::MapKeyHashToBucketId(
            CompositeKey<int>{int{scan_key}}.Hash());

        absl::flat_hash_map<NodeGroupId, std::vector<uint16_t>> buckets;
        buckets[node_group_id].push_back(bucket_id);
        absl::flat_hash_map<NodeGroupId,
                            absl::flat_hash_map<uint16_t, BucketScanProgress>>
            saved_progress;
        auto [ng_progress_it, inserted] =
            saved_progress.try_emplace(node_group_id);
        REQUIRE(inserted);
        auto [core_progress_it, core_inserted] =
            ng_progress_it->second.try_emplace(
                core_id, TxKey(CompositeKey<int>::NegativeInfinity()), true);
        REQUIRE(core_inserted);
        core_progress_it->second.memory_scan_is_finished_ = true;
        core_progress_it->second.scan_buckets_[bucket_id] = false;

        BucketScanPlan plan(0, &buckets, saved_progress);
        HashParitionCcScanner<CompositeKey<int>, CompositeRecord<int>> scanner(
            ScanDirection::Forward, ScanIndexType::Primary, nullptr);
        CcHandlerResult<ScanNextResult> scan_result(nullptr);
        scan_result.Value().current_scan_plan_ = &plan;
        scan_result.Value().ccm_scanner_ = &scanner;
        TxKey end_key(CompositeKey<int>::PositiveInfinity());
        ScanNextBatchCc scan_req;
        scan_req.Reset(node.Table(),
                       node_group_id,
                       plan.GetNodeGroupTerm(node_group_id),
                       txn,
                       tx.Txm()->GetStartTs(),
                       end_key,
                       true,
                       &plan,
                       nullptr,
                       tx.Txm()->TxTerm(),
                       &scan_result,
                       IsolationLevel::RepeatableRead,
                       CcProtocol::OccRead,
                       false,
                       false,
                       false,
                       true,
                       true);

        auto read_intent_count = [txn, cce = scanned.cce_]
        {
            NonBlockingLock *key_lock = cce->GetKeyLock();
            if (key_lock == nullptr)
            {
                return uint32_t{0};
            }
            auto it = key_lock->ReadIntents().find(txn);
            return it == key_lock->ReadIntents().end() ? uint32_t{0}
                                                       : it->second;
        };

        const uint32_t shard_code = (node_group_id << 10) + core_id;
        std::atomic<uint32_t> initial_count{0};
        std::atomic<uint32_t> post_request_count{0};
        std::atomic<uint32_t> post_commit_count{0};
        WaitableCc inspect_after_request(
            [&](CcShard &)
            {
                post_request_count.store(read_intent_count(),
                                         std::memory_order_release);
                return true;
            });
        WaitableCc enqueue_scan(
            [&](CcShard &ccs)
            {
                initial_count.store(read_intent_count(),
                                    std::memory_order_release);
                ccs.Enqueue(&scan_req);
                ccs.Enqueue(&inspect_after_request);
                return true;
            });
        Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(core_id,
                                                                 &enqueue_scan);
        enqueue_scan.Wait();
        inspect_after_request.Wait();
        REQUIRE_FALSE(enqueue_scan.IsError());
        REQUIRE_FALSE(inspect_after_request.IsError());
        REQUIRE(scan_result.IsFinished());
        REQUIRE_FALSE(scan_result.IsError());

        REQUIRE(tx.Commit());
        WaitableCc inspect_after_commit(
            [&](CcShard &)
            {
                post_commit_count.store(read_intent_count(),
                                        std::memory_order_release);
                return true;
            });
        Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(
            core_id, &inspect_after_commit);
        inspect_after_commit.Wait();
        REQUIRE_FALSE(inspect_after_commit.IsError());

        REQUIRE(initial_count.load(std::memory_order_acquire) == 1);
        REQUIRE(scanner.Cache(shard_code)->Size() == 0);
        REQUIRE(post_request_count.load(std::memory_order_acquire) == 1);
        REQUIRE(post_commit_count.load(std::memory_order_acquire) == 0);
    }

    // Scenario 8: orphan-lock recovery for locally-coordinated transactions.
    // Fabricate the residue shapes a lock leak leaves behind — a real
    // ReadIntent owned by a tx that no longer exists, and registry
    // bookkeeping claiming a write lock with no backing lock — verify the
    // bookkeeping pins the checkpoint timestamp and CkptTsCc names it, then
    // verify the CheckRecoverTx-launched probe (RecoverDeadTxCc) clears both
    // while leaving a live transaction's intent alone. Recovery is driven by
    // issuing CkptTsCc rounds, the same trigger production uses.
    {
        auto seed = node.BeginTx();
        REQUIRE(seed.Upsert(50, 500));
        REQUIRE(seed.Commit());

        auto scan_tx =
            node.BeginTx(IsolationLevel::RepeatableRead, CcProtocol::OccRead);
        ClosedScanRead scanned = ScanOneAndClose(node, scan_tx, 50);

        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        const NodeGroupId ng = Sharder::Instance().NativeNodeGroup();
        const uint16_t core_id = scanned.cce_addr_.CoreId();
        const int64_t ng_term = Sharder::Instance().LeaderTerm(ng);
        REQUIRE(ng_term >= 0);

        const TxNumber core_prefix = static_cast<TxNumber>((ng << 10) | core_id)
                                     << 32L;
        // Idents far above anything this test allocates: LocateTx must not
        // find them, so recovery treats their owners as finished.
        const TxNumber dead_intent_txn = core_prefix | 0xFEED0001;
        const TxNumber dead_book_txn = core_prefix | 0xFEED0002;

        auto live_tx = node.BeginTx();
        const TxNumber live_txn = live_tx.Txm()->TxNumber();

        // Plant while scan_tx's retained read keeps the cce's lock struct
        // alive. Mirrors CcMap::AcquireReadIntent: acquire on the lock, then
        // register the first acquisition in the shard's registry.
        std::atomic<bool> planted{false};
        WaitableCc plant(
            [&](CcShard &ccs)
            {
                NonBlockingLock *lock = scanned.cce_->GetKeyLock();
                if (lock == nullptr)
                {
                    return true;
                }
                // The scan-leak shape: a real ReadIntent registered under a
                // tx that no longer exists.
                if (lock->AcquireReadIntent(dead_intent_txn))
                {
                    ccs.UpsertLockHoldingTx(dead_intent_txn,
                                            ng_term,
                                            scanned.cce_,
                                            false,
                                            ng,
                                            TableType::Primary);
                }
                // The ident-wrap-adoption shape: bookkeeping claiming a
                // write lock that does not exist on the entry.
                ccs.UpsertLockHoldingTx(dead_book_txn,
                                        ng_term,
                                        scanned.cce_,
                                        true,
                                        ng,
                                        TableType::Primary);
                // A live tx's intent must survive recovery.
                if (lock->AcquireReadIntent(live_txn))
                {
                    ccs.UpsertLockHoldingTx(live_txn,
                                            ng_term,
                                            scanned.cce_,
                                            false,
                                            ng,
                                            TableType::Primary);
                }
                planted.store(true, std::memory_order_release);
                return true;
            });
        shards->EnqueueCcRequest(core_id, &plant);
        plant.Wait();
        REQUIRE_FALSE(plant.IsError());
        REQUIRE(planted.load(std::memory_order_acquire));

        // Release the scan tx's own ownership; the fabricated intents keep
        // the lock struct alive.
        REQUIRE(scan_tx.Commit());
        REQUIRE_FALSE(TxOwnsScanRead(scanned));

        auto drive_recovery_round = [&]() -> TxNumber
        {
            CkptTsCc ckpt_req(shards->Count(), ng);
            for (size_t core = 0; core < shards->Count(); ++core)
            {
                shards->EnqueueCcRequest(core, &ckpt_req);
            }
            ckpt_req.Wait();
            return ckpt_req.GetPinningTx().txn_;
        };
        auto lock_cleared = [&](TxNumber txn)
        {
            return !TxOwnsScanRead(
                ClosedScanRead{scanned.cce_addr_, scanned.cce_, txn});
        };

        // Within the 5s recovery gate no probe has launched yet, and the
        // fabricated write-lock bookkeeping pins the checkpoint ts.
        REQUIRE(drive_recovery_round() == dead_book_txn);

        // Keep driving rounds: once the gate elapses, CheckRecoverTx probes
        // the dead txns and their residue is cleared.
        bool recovered = false;
        for (int i = 0; i < 300 && !recovered; ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            TxNumber pinning_txn = drive_recovery_round();
            recovered = lock_cleared(dead_intent_txn) && pinning_txn == 0;
        }
        REQUIRE(recovered);
        // The live tx's intent survived recovery.
        REQUIRE_FALSE(lock_cleared(live_txn));

        // Once the live tx finishes, its (fabricated, hence never released)
        // intent becomes recoverable residue too.
        REQUIRE(live_tx.Commit());
        recovered = false;
        for (int i = 0; i < 300 && !recovered; ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            drive_recovery_round();
            recovered = lock_cleared(live_txn);
        }
        REQUIRE(recovered);
    }

    REQUIRE(commit_scan_retained);
    REQUIRE(abort_scan_retained);
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
