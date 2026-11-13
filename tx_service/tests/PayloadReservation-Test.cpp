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
 * @file PayloadReservation-Test.cpp
 * @brief Payload reservations before write-lock acquisition: the shard
 * heap's ledger, the acquire request's ownership of its charge through
 * success, conflict, waiting, cancellation and bulk reset, the cleaner's
 * K-cycle rejection, the checkpoint cycle count that drives it, and the
 * blocked-request probe handling.
 *
 * One TestNode per process (see TestNodeSmoke-Test): every scenario runs in
 * one TEST_CASE against shard 0 of that node, inside closures executed on the
 * shard so the ledger is read and written under its ownership contract.
 */

#include <gflags/gflags.h>

#include <atomic>
#include <catch2/catch_all.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// The lock release and the wait-list links are protected and private members
// of the map and the shard; the existing cleaner tests open them the same way.
#define private public
#define protected public
#include "cc/cc_map.h"
#include "cc/local_cc_shards.h"
#include "cc/template_cc_map.h"
#undef protected
#undef private

#include "cc/cc_entry.h"
#include "cc/cc_handler_result.h"
#include "cc/cc_req_base.h"
#include "cc/cc_req_misc.h"
#include "cc/cc_request.h"
#include "cc/cc_shard.h"
#include "cc/non_blocking_lock.h"
#include "cc_protocol.h"
#include "checkpointer.h"
#include "data_sync_task.h"
#include "error_messages.h"
#include "fault/log_replay_service.h"
#include "harness/test_node.h"
#include "remote/cc_stream_receiver.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "type.h"

DECLARE_uint64(shard_memory_wait_ckpt_cycles);

using namespace std::chrono_literals;
using namespace txservice;
using namespace txservice::test;

namespace
{
/** The primary cc map type the mock catalog factory builds for the test
 *  table. */
using PkMap =
    TemplateCcMap<CompositeKey<int>, CompositeRecord<int>, true, false>;

/** Node group of the single-group test node. */
constexpr NodeGroupId kNodeGroup = 0;
/** Shard code for node group 0, core 0: the group in the high bits. */
constexpr uint32_t kShardCode = 0;

/**
 * @brief Runs @p task on shard 0 and waits for it.
 * @param task The work; it runs on the shard's own thread.
 */
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

/**
 * @brief Polls @p done until it holds or @p budget elapses.
 * @return Whether @p done held before the budget ran out.
 */
bool WaitFor(const std::function<bool()> &done,
             std::chrono::milliseconds budget = 10s)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (!done())
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return false;
        }
        std::this_thread::sleep_for(2ms);
    }
    return true;
}

/**
 * @brief One acquire request with its own result, built the way the local
 * handler builds them, with the reservation budget chosen by the test.
 */
struct Acquire
{
    /**
     * @param table The test table.
     * @param schema_version The table's schema version.
     * @param key The key to lock; kept alive by this object.
     * @param txm The live transaction the request acts for, so the lock
     * registry's recovery checks find an owner.
     * @param reserve_bytes The payload estimate to reserve.
     * @param proto Locking queues on conflict; OCC fails at once.
     * @param abort_if_oom The transaction's fail-fast flag, which it sets
     * while it holds a range read lock (AcquireCc::MemoryWaitPolicy()).
     */
    Acquire(const TableName &table,
            uint64_t schema_version,
            int key,
            TransactionExecution *txm,
            size_t reserve_bytes,
            CcProtocol proto,
            bool abort_if_oom = false)
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
                   proto,
                   IsolationLevel::RepeatableRead,
                   abort_if_oom,
                   reserve_bytes);
    }

    /** @return The entry the granted lock lives on. */
    LruEntry *Entry() const
    {
        return result_.Value()[0].cce_addr_.ExtractCce();
    }

    /** @return The entry's lock, or null once it was recycled. */
    NonBlockingLock *Lock() const
    {
        LruEntry *entry = Entry();
        return entry != nullptr ? entry->GetKeyLock() : nullptr;
    }

    TxKey key_;                                              ///< Locked key.
    CcHandlerResult<std::vector<AcquireKeyResult>> result_;  ///< Its result.
    AcquireCc req_;                                          ///< The request.
};

/**
 * @brief One log replay request that updates a single key of the test table,
 * with the state the log replay service keeps around such a request.
 */
struct Replay
{
    /**
     * @param table The test table.
     * @param key The key the log record updates.
     * @param value The value the log record carries.
     * @param commit_ts The log record's commit timestamp.
     * @param txn The committed transaction, which orphan lock recovery
     * matches against the key's lock holder.
     * @param lock_recovery Whether the request recovers an orphaned lock, as
     * opposed to replaying the log after a failover.
     */
    Replay(const TableName &table,
           int key,
           int value,
           uint64_t commit_ts,
           TxNumber txn,
           bool lock_recovery)
    {
        CompositeKey<int>(key).Serialize(blob_);
        blob_.push_back(static_cast<char>(OperationType::Update));
        Rec(value)->Serialize(blob_);
        req_.Reset(kNodeGroup,
                   Sharder::Instance().LeaderTerm(kNodeGroup),
                   table.StringView(),
                   table.Type(),
                   table.Engine(),
                   blob_,
                   commit_ts,
                   txn,
                   mux_,
                   status_,
                   on_fly_cnt_,
                   recovery_error_,
                   lock_recovery);
    }

    /** @return Whether the request finished, applied or skipped. */
    bool Finished() const
    {
        return on_fly_cnt_.load(std::memory_order_relaxed) == 0;
    }

    std::string blob_;    ///< The log record: key, operation type, value.
    bthread::Mutex mux_;  ///< Guards recovery_error_.
    /** The replay stream's status, unused by a single request. */
    std::atomic<fault::RecoveryService::WaitingStatus> status_{
        fault::RecoveryService::WaitingStatus::Active};
    std::atomic<size_t> on_fly_cnt_{1};  ///< Drops to 0 when req_ finishes.
    bool recovery_error_{false};         ///< Set if req_ is aborted.
    ReplayLogCc req_;                    ///< The request.
};

/** A parked stub that records its abort. */
class StubRequest : public CcRequestBase
{
public:
    /**
     * @param txn The transaction the stub belongs to.
     * @param policy How long it may wait for memory
     * (CcRequestBase::MemoryWaitPolicy()).
     */
    explicit StubRequest(TxNumber txn = 0,
                         MemoryWait policy = MemoryWait::kBounded)
        : policy_(policy)
    {
        tx_number_ = txn;
    }

    bool Execute(CcShard &) override
    {
        ++executed_;
        return true;
    }

    /** @return How many times the stub was executed. */
    int Executed() const
    {
        return executed_;
    }

    MemoryWait MemoryWaitPolicy() const override
    {
        return policy_;
    }

    void AbortCcRequest(CcErrorCode error) override
    {
        abort_error_.store(error, std::memory_order_release);
        Free();
    }

    CcErrorCode AbortError() const
    {
        return abort_error_.load(std::memory_order_acquire);
    }

private:
    const MemoryWait policy_;  ///< See MemoryWaitPolicy().
    int executed_{0};          ///< Executions, counted on the shard.
    std::atomic<CcErrorCode> abort_error_{CcErrorCode::NO_ERROR};
};

/** Puts the K flag back when a scope ends. */
struct RestoreWaitCycles
{
    uint64_t previous = FLAGS_shard_memory_wait_ckpt_cycles;
    ~RestoreWaitCycles()
    {
        FLAGS_shard_memory_wait_ckpt_cycles = previous;
    }
};

/** Advances the node's concluded checkpoint cycle count by @p n. */
void BumpCycles(uint64_t n)
{
    for (uint64_t i = 0; i < n; ++i)
    {
        Sharder::Instance().GetCheckpointer()->OnCkptCycleConcluded();
    }
}
}  // namespace

TEST_CASE("payload reservation before write-lock acquisition",
          "[payload-reservation]")
{
    // Not store-backed: this test commits data, and the in-memory store cannot
    // flush the mock table's record type, so a live checkpointer would abort
    // at shutdown. The live checkpointer's cycle wiring is covered by
    // CkptCycle-Test on a node that never dirties the table.
    TestNode node(TestNodeOptions{}.CoreNum(1));
    const TableName &table = node.Table();
    const uint64_t schema_version = node.SchemaVersion();
    constexpr size_t kMiB = size_t(1) << 20;

    // A committed write creates the table's cc map on the shard and leaves
    // key 1 as an existing entry.
    {
        auto t = node.BeginTx();
        REQUIRE(t.Upsert(1, 100));
        REQUIRE(t.Commit());
    }

    CcShard *shard = nullptr;
    PkMap *map = nullptr;
    CcShardHeap *heap = nullptr;
    RunOnShard(
        [&](CcShard &s)
        {
            shard = &s;
            map = static_cast<PkMap *>(s.GetCcm(table, kNodeGroup));
            heap = s.GetShardHeap();
        });
    REQUIRE(shard != nullptr);
    REQUIRE(map != nullptr);
    REQUIRE(heap != nullptr);
    const size_t limit = heap->MemoryLimit();
    REQUIRE(limit > 64 * kMiB);

    // Two live transactions whose numbers the requests below act for. The
    // locks they take are released by hand before the transactions end.
    auto tx_a = node.BeginTx();
    auto tx_b = node.BeginTx();
    TransactionExecution *txm_a = tx_a.Txm();
    TransactionExecution *txm_b = tx_b.Txm();

    // --- Heap ledger -------------------------------------------------------
    RunOnShard(
        [&](CcShard &)
        {
            REQUIRE(heap->ReservedBytes() == 0);
            REQUIRE(heap->TryReserve(0));
            REQUIRE(heap->ReservedBytes() == 0);
            REQUIRE_FALSE(heap->TryReserve(limit + 1));
            REQUIRE(heap->ReservedBytes() == 0);

            const int64_t physical = heap->PhysicalAllocated();
            REQUIRE(physical > 0);
            REQUIRE(static_cast<size_t>(physical) + 4 * kMiB < limit);
            // Leave 2 MiB of room: a 3 MiB charge is refused and leaves the
            // ledger as it was, a 1 MiB charge fits.
            const size_t big = limit - static_cast<size_t>(physical) - 2 * kMiB;
            REQUIRE(heap->TryReserve(big));
            REQUIRE(heap->ReservedBytes() == big);
            REQUIRE_FALSE(heap->Full());
            REQUIRE_FALSE(heap->TryReserve(3 * kMiB));
            REQUIRE(heap->ReservedBytes() == big);
            REQUIRE(heap->TryReserve(kMiB));
            REQUIRE(heap->ReservedBytes() == big + kMiB);
            // The reservation alone counts as used: with about 1 MiB of room
            // left, a 1 MiB entry-sized request is at the limit's edge, while
            // the physical figure is far below it.
            REQUIRE(heap->PhysicalAllocated() + 2 * kMiB <
                    static_cast<int64_t>(limit));
            heap->ReleaseReservation(kMiB);
            heap->ReleaseReservation(big);
            REQUIRE(heap->ReservedBytes() == 0);
        });

    // --- Defragmentation condition a reservation checks --------------------
    RunOnShard(
        [&](CcShard &s)
        {
            // Fragmented means committed memory above 80% of the limit with
            // at most 80% of it allocated. The figures are passed in, as
            // TryReserve() passes the ones it read for its capacity check.
            const int64_t tenth = static_cast<int64_t>(limit / 10);
            // Committed at 70% of the limit: below the mark, however little
            // of it is allocated.
            REQUIRE_FALSE(heap->IsFragmented(1 * tenth, 7 * tenth));
            // Committed at 90% of the limit, 5/9 of it allocated.
            REQUIRE(heap->IsFragmented(5 * tenth, 9 * tenth));
            // Committed over the limit, which also makes the heap full when
            // defragmentation is enabled, half of it allocated.
            REQUIRE(heap->IsFragmented(6 * tenth, 12 * tenth));
            // Committed at 90% of the limit, but 8.5/9 of it allocated: the
            // heap is dense, and defragmenting it would gain nothing.
            REQUIRE_FALSE(heap->IsFragmented(tenth * 17 / 2, 9 * tenth));

            // The test node runs with defragmentation disabled, so fragmented
            // figures start nothing.
            REQUIRE_FALSE(s.EnableDefragment());
            REQUIRE_FALSE(heap->StartDefragmentIfNeeded(5 * tenth, 9 * tenth));
            REQUIRE_FALSE(heap->IsDefragHeapCcOnFly());
        });

    // --- FindEmplace on a full heap: only a missing key is refused ----------
    RunOnShard(
        [&](CcShard &)
        {
            // Reserve everything above the physical figure so the heap is
            // full on reservations alone.
            const int64_t physical = heap->PhysicalAllocated();
            const size_t fill = limit - static_cast<size_t>(physical);
            REQUIRE(heap->TryReserve(fill));
            REQUIRE(heap->Full());

            // The existing entry of key 1 is returned to a write and to a
            // read-only request alike: finding it allocates nothing.
            bool emplaced = false;
            auto write_hit = map->FindEmplace(CompositeKey<int>(1), &emplaced);
            REQUIRE(write_hit->second != nullptr);
            REQUIRE_FALSE(emplaced);
            auto read_hit = map->FindEmplace(CompositeKey<int>(1),
                                             &emplaced,
                                             /*force_emplace=*/false,
                                             /*read_only_req=*/true);
            REQUIRE(read_hit->second != nullptr);
            REQUIRE_FALSE(emplaced);

            // A key that is absent (777) is refused: the new entry itself
            // needs memory.
            auto missing = map->FindEmplace(CompositeKey<int>(777), &emplaced);
            REQUIRE(missing->second == nullptr);
            REQUIRE_FALSE(emplaced);
            REQUIRE(map->Find(CompositeKey<int>(777))->second == nullptr);

            // Whether a payload may be installed into the entry is a separate
            // question, asked by the writes that reserve nothing: on a full
            // heap they must wait.
            REQUIRE_FALSE(map->ExemptFromMemoryLimit());
            REQUIRE(map->UnreservedWriteMustWait());

            heap->ReleaseReservation(fill);
            REQUIRE_FALSE(heap->Full());
            REQUIRE(heap->ReservedBytes() == 0);

            // With room again they need not.
            REQUIRE_FALSE(map->UnreservedWriteMustWait());
        });

    // --- Orphan lock recovery and log replay on a full heap ----------------
    {
        // A committed write leaves key 4242 as an existing entry, apart from
        // key 1 whose versions later scenarios count.
        constexpr int kKey = 4242;
        {
            auto t = node.BeginTx();
            REQUIRE(t.Upsert(kKey, 200));
            REQUIRE(t.Commit());
        }
        // The key's entry commit timestamp, read on the shard.
        auto entry_commit_ts = [&]()
        {
            auto it = map->Find(CompositeKey<int>(kKey));
            REQUIRE(it->second != nullptr);
            return it->second->CommitTs();
        };

        // Transaction A takes the key's write lock with a 4 MiB reservation
        // and plays the committed transaction whose coordinator died before
        // it installed the value and released the lock.
        Acquire orphan(
            table, schema_version, kKey, txm_a, 4 * kMiB, CcProtocol::Locking);
        uint64_t entry_ts = 0;
        size_t fill = 0;
        size_t refill = 0;
        RunOnShard(
            [&](CcShard &)
            {
                REQUIRE(map->Execute(orphan.req_));
                REQUIRE_FALSE(orphan.result_.IsError());
                REQUIRE(orphan.Lock()->WriteLockTx() == txm_a->TxNumber());
                entry_ts = entry_commit_ts();

                // Fill the heap: the orphaned lock's reservation is part of
                // what keeps it full.
                const int64_t physical = heap->PhysicalAllocated();
                fill = limit - static_cast<size_t>(physical) - 4 * kMiB;
                REQUIRE(heap->TryReserve(fill));
                REQUIRE(heap->Full());
                REQUIRE(heap->ReservedBytes() == fill + 4 * kMiB);
            });
        REQUIRE(entry_ts > 0);

        // Recovery of a transaction that does not hold the lock (B) is
        // skipped: the entry and the lock stay as they are.
        Replay not_holder(table,
                          kKey,
                          201,
                          entry_ts + 10,
                          txm_b->TxNumber(),
                          /*lock_recovery=*/true);
        // Recovery of a key that is not in the map (778) is skipped without
        // creating an entry: no lock can sit on a missing entry.
        Replay missing_key(table,
                           778,
                           202,
                           entry_ts + 10,
                           txm_a->TxNumber(),
                           /*lock_recovery=*/true);
        // Recovery of the lock holder (A).
        Replay holder(table,
                      kKey,
                      203,
                      entry_ts + 10,
                      txm_a->TxNumber(),
                      /*lock_recovery=*/true);
        // Plain log replay of a later record of the key, which nobody locks
        // by then.
        Replay plain(table,
                     kKey,
                     204,
                     entry_ts + 20,
                     /*txn=*/0,
                     /*lock_recovery=*/false);
        RunOnShard(
            [&](CcShard &s)
            {
                REQUIRE(map->Execute(not_holder.req_));
                REQUIRE(not_holder.Finished());
                REQUIRE(entry_commit_ts() == entry_ts);
                REQUIRE(orphan.Lock()->WriteLockTx() == txm_a->TxNumber());

                REQUIRE(map->Execute(missing_key.req_));
                REQUIRE(missing_key.Finished());
                REQUIRE(map->Find(CompositeKey<int>(778))->second == nullptr);

                // The holder's record is installed and its lock released
                // although the heap is full, and nothing parks: waiting for
                // memory would wait on the checkpoint this lock holds back.
                // The lock's reservation is returned with it.
                REQUIRE(heap->Full());
                REQUIRE(map->Execute(holder.req_));
                REQUIRE(holder.Finished());
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(entry_commit_ts() == entry_ts + 10);
                NonBlockingLock *lock = orphan.Lock();
                REQUIRE((lock == nullptr || !lock->HasWriteLock()));
                REQUIRE(heap->ReservedBytes() == fill);

                // Releasing the lock's 4 MiB left the heap short of full; fill
                // it again.
                REQUIRE_FALSE(heap->Full());
                refill = limit -
                         static_cast<size_t>(heap->PhysicalAllocated()) - fill;
                REQUIRE(heap->TryReserve(refill));
                REQUIRE(heap->Full());

                // Plain replay has no reservation, so it waits for memory
                // although the entry exists, and installs nothing meanwhile.
                REQUIRE_FALSE(map->Execute(plain.req_));
                REQUIRE_FALSE(plain.Finished());
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(plain.req_.MemoryParkStamp() != 0);
                REQUIRE(entry_commit_ts() == entry_ts + 10);

                // Free the memory. The cleaner woken by the park finds room
                // and releases the request.
                heap->ReleaseReservation(fill + refill);
                REQUIRE(heap->ReservedBytes() == 0);
            });

        REQUIRE(WaitFor([&] { return plain.Finished(); }));
        RunOnShard(
            [&](CcShard &s)
            {
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(entry_commit_ts() == entry_ts + 20);
            });
        REQUIRE_FALSE(not_holder.recovery_error_);
        REQUIRE_FALSE(missing_key.recovery_error_);
        REQUIRE_FALSE(holder.recovery_error_);
        REQUIRE_FALSE(plain.recovery_error_);

        // The replayed value is what a transaction now reads.
        auto reader = node.BeginTx();
        int value = 0;
        REQUIRE(reader.Read(kKey, value));
        REQUIRE(value == 204);
        REQUIRE(reader.Commit());
    }

    // --- Immediate success, held-lock re-send, release ---------------------
    {
        Acquire first(
            table, schema_version, 1, txm_a, 4 * kMiB, CcProtocol::Locking);
        Acquire resent(
            table, schema_version, 1, txm_a, 4 * kMiB, CcProtocol::Locking);
        RunOnShard(
            [&](CcShard &s)
            {
                REQUIRE(map->Execute(first.req_));
                REQUIRE(first.result_.IsFinished());
                REQUIRE_FALSE(first.result_.IsError());
                REQUIRE(first.req_.Phase() ==
                        AcquireCc::ReservePhase::kLockOwned);
                REQUIRE(first.req_.OwnedBytes() == 0);
                REQUIRE(heap->ReservedBytes() == 4 * kMiB);
                NonBlockingLock *lock = first.Lock();
                REQUIRE(lock != nullptr);
                REQUIRE(lock->HasWriteLock());
                REQUIRE(lock->WriteLockTx() == txm_a->TxNumber());
                REQUIRE(lock->ReservedBytes() == 4 * kMiB);

                // The same transaction acquires the same key again, as the
                // re-run after a leadership change does: no second charge.
                REQUIRE(map->Execute(resent.req_));
                REQUIRE(resent.result_.IsFinished());
                REQUIRE_FALSE(resent.result_.IsError());
                REQUIRE(resent.req_.Phase() ==
                        AcquireCc::ReservePhase::kLockOwned);
                REQUIRE(heap->ReservedBytes() == 4 * kMiB);
                REQUIRE(lock->ReservedBytes() == 4 * kMiB);

                map->ReleaseCceLock(lock,
                                    first.Entry(),
                                    txm_a->TxNumber(),
                                    kNodeGroup,
                                    LockType::WriteLock);
                REQUIRE(heap->ReservedBytes() == 0);
            });
    }

    // --- Immediate OCC conflict returns the charge -------------------------
    {
        Acquire holder(
            table, schema_version, 1, txm_a, 4 * kMiB, CcProtocol::Locking);
        Acquire loser(
            table, schema_version, 1, txm_b, 2 * kMiB, CcProtocol::OCC);
        RunOnShard(
            [&](CcShard &)
            {
                REQUIRE(map->Execute(holder.req_));
                REQUIRE(heap->ReservedBytes() == 4 * kMiB);

                REQUIRE(map->Execute(loser.req_));
                REQUIRE(loser.result_.IsFinished());
                REQUIRE(loser.result_.IsError());
                REQUIRE(loser.result_.ErrorCode() ==
                        CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT);
                REQUIRE(loser.req_.Phase() ==
                        AcquireCc::ReservePhase::kUncharged);
                REQUIRE(loser.req_.OwnedBytes() == 0);
                REQUIRE(heap->ReservedBytes() == 4 * kMiB);

                map->ReleaseCceLock(holder.Lock(),
                                    holder.Entry(),
                                    txm_a->TxNumber(),
                                    kNodeGroup,
                                    LockType::WriteLock);
                REQUIRE(heap->ReservedBytes() == 0);
            });
    }

    // --- Blocked acquisition keeps its charge, transfers on grant ----------
    {
        Acquire holder(
            table, schema_version, 1, txm_a, 4 * kMiB, CcProtocol::Locking);
        Acquire waiter(
            table, schema_version, 1, txm_b, 2 * kMiB, CcProtocol::Locking);
        RunOnShard(
            [&](CcShard &)
            {
                REQUIRE(map->Execute(holder.req_));
                REQUIRE_FALSE(map->Execute(waiter.req_));
                REQUIRE_FALSE(waiter.result_.IsFinished());
                REQUIRE(waiter.req_.Phase() ==
                        AcquireCc::ReservePhase::kRequestOwned);
                REQUIRE(waiter.req_.OwnedBytes() == 2 * kMiB);
                REQUIRE(heap->ReservedBytes() == 6 * kMiB);
                REQUIRE(holder.Lock()->FindQueueRequest(txm_b->TxNumber()));

                // Releasing the holder grants the waiter and re-enqueues it
                // on the shard; the resumed execution transfers the charge.
                map->ReleaseCceLock(holder.Lock(),
                                    holder.Entry(),
                                    txm_a->TxNumber(),
                                    kNodeGroup,
                                    LockType::WriteLock);
                REQUIRE(heap->ReservedBytes() == 2 * kMiB);
            });
        REQUIRE(WaitFor([&] { return waiter.result_.IsFinished(); }));
        REQUIRE_FALSE(waiter.result_.IsError());
        RunOnShard(
            [&](CcShard &)
            {
                REQUIRE(waiter.req_.Phase() ==
                        AcquireCc::ReservePhase::kLockOwned);
                REQUIRE(waiter.req_.OwnedBytes() == 0);
                REQUIRE(heap->ReservedBytes() == 2 * kMiB);
                NonBlockingLock *lock = waiter.Lock();
                REQUIRE(lock != nullptr);
                REQUIRE(lock->WriteLockTx() == txm_b->TxNumber());
                REQUIRE(lock->ReservedBytes() == 2 * kMiB);
                map->ReleaseCceLock(lock,
                                    waiter.Entry(),
                                    txm_b->TxNumber(),
                                    kNodeGroup,
                                    LockType::WriteLock);
                REQUIRE(heap->ReservedBytes() == 0);
            });
    }

    // --- Cancellation while queued returns the charge once -----------------
    {
        Acquire holder(
            table, schema_version, 1, txm_a, 4 * kMiB, CcProtocol::Locking);
        Acquire victim(
            table, schema_version, 1, txm_b, 2 * kMiB, CcProtocol::Locking);
        RunOnShard(
            [&](CcShard &)
            {
                REQUIRE(map->Execute(holder.req_));
                REQUIRE_FALSE(map->Execute(victim.req_));
                REQUIRE(heap->ReservedBytes() == 6 * kMiB);

                holder.Lock()->AbortQueueRequest(txm_b->TxNumber(),
                                                 CcErrorCode::DEAD_LOCK_ABORT);
                REQUIRE(victim.result_.IsFinished());
                REQUIRE(victim.result_.ErrorCode() ==
                        CcErrorCode::DEAD_LOCK_ABORT);
                REQUIRE(victim.req_.Phase() ==
                        AcquireCc::ReservePhase::kUncharged);
                REQUIRE(heap->ReservedBytes() == 4 * kMiB);
                // Idempotent: a second rollback changes nothing.
                victim.req_.ReleaseRequestReservation();
                REQUIRE(heap->ReservedBytes() == 4 * kMiB);

                map->ReleaseCceLock(holder.Lock(),
                                    holder.Entry(),
                                    txm_a->TxNumber(),
                                    kNodeGroup,
                                    LockType::WriteLock);
                REQUIRE(heap->ReservedBytes() == 0);
            });
    }

    // --- Dispatcher error on a resumed waiter returns the charge -----------
    {
        Acquire holder(
            table, schema_version, 1, txm_a, 4 * kMiB, CcProtocol::Locking);
        Acquire stale(
            table, schema_version, 1, txm_b, 2 * kMiB, CcProtocol::Locking);
        RunOnShard(
            [&](CcShard &s)
            {
                REQUIRE(map->Execute(holder.req_));
                REQUIRE_FALSE(map->Execute(stale.req_));
                REQUIRE(heap->ReservedBytes() == 6 * kMiB);

                // The waiter resumes through the dispatcher after the group's
                // term moved on: the term check fails before the map sees it.
                stale.req_.ng_term_ =
                    Sharder::Instance().LeaderTerm(kNodeGroup) + 1;
                REQUIRE(stale.req_.Execute(s));
                REQUIRE(stale.result_.IsFinished());
                REQUIRE(stale.result_.ErrorCode() ==
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                REQUIRE(stale.req_.Phase() ==
                        AcquireCc::ReservePhase::kUncharged);
                REQUIRE(heap->ReservedBytes() == 4 * kMiB);

                // Drop the dead entry from the queue, then the holder.
                holder.Lock()->AbortQueueRequest(txm_b->TxNumber());
                map->ReleaseCceLock(holder.Lock(),
                                    holder.Entry(),
                                    txm_a->TxNumber(),
                                    kNodeGroup,
                                    LockType::WriteLock);
                REQUIRE(heap->ReservedBytes() == 0);
            });
    }

    // --- Bulk reset returns the granted and the queued charge --------------
    {
        Acquire holder(
            table, schema_version, 1, txm_a, 4 * kMiB, CcProtocol::Locking);
        Acquire queued(
            table, schema_version, 1, txm_b, 2 * kMiB, CcProtocol::Locking);
        RunOnShard(
            [&](CcShard &s)
            {
                REQUIRE(map->Execute(holder.req_));
                REQUIRE_FALSE(map->Execute(queued.req_));
                REQUIRE(heap->ReservedBytes() == 6 * kMiB);

                LruEntry *entry = holder.Entry();
                entry->ClearLocks(s, kNodeGroup);
                REQUIRE(heap->ReservedBytes() == 0);
                REQUIRE(entry->GetKeyLock() == nullptr);
                REQUIRE(queued.result_.IsFinished());
                REQUIRE(queued.result_.ErrorCode() ==
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                REQUIRE(queued.req_.Phase() ==
                        AcquireCc::ReservePhase::kUncharged);
            });
    }

    // --- Oversized payload fails before locking ----------------------------
    {
        Acquire oversized(
            table, schema_version, 1, txm_a, limit + 1, CcProtocol::Locking);
        RunOnShard(
            [&](CcShard &s)
            {
                REQUIRE(map->Execute(oversized.req_));
                REQUIRE(oversized.result_.ErrorCode() ==
                        CcErrorCode::OUT_OF_MEMORY);
                REQUIRE(oversized.req_.Phase() ==
                        AcquireCc::ReservePhase::kUncharged);
                REQUIRE(heap->ReservedBytes() == 0);
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(oversized.Lock() == nullptr);
            });
    }

    // --- Reservation that does not fit parks; the K-cycle rule rejects -----
    {
        RestoreWaitCycles restore;
        FLAGS_shard_memory_wait_ckpt_cycles = 3;
        Acquire parked(
            table, schema_version, 9, txm_b, 4 * kMiB, CcProtocol::Locking);
        Acquire fail_fast(table,
                          schema_version,
                          10,
                          txm_b,
                          4 * kMiB,
                          CcProtocol::Locking,
                          /*abort_if_oom=*/true);
        Acquire zero_existing(
            table, schema_version, 1, txm_a, 0, CcProtocol::Locking);
        RunOnShard(
            [&](CcShard &s)
            {
                // Outstanding reservations leave 2 MiB of room: a new entry
                // still fits, a 4 MiB payload does not.
                const int64_t physical = heap->PhysicalAllocated();
                const size_t fill =
                    limit - static_cast<size_t>(physical) - 2 * kMiB;
                REQUIRE(heap->TryReserve(fill));
                const uint64_t cycles_at_park = Sharder::Instance()
                                                    .GetCheckpointer()
                                                    ->ConcludedCkptCycles();

                REQUIRE_FALSE(map->Execute(parked.req_));
                REQUIRE_FALSE(parked.result_.IsFinished());
                REQUIRE(parked.req_.Phase() ==
                        AcquireCc::ReservePhase::kUncharged);
                // Parked without a published entry pointer.
                REQUIRE(parked.req_.CcePtr() == nullptr);
                REQUIRE(parked.req_.MemoryParkStamp() == cycles_at_park + 1);
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(heap->ReservedBytes() == fill);

                // A zero-byte acquisition of an existing key is admitted on a
                // heap this close to its limit: it needs no new memory.
                REQUIRE(map->Execute(zero_existing.req_));
                REQUIRE_FALSE(zero_existing.result_.IsError());
                REQUIRE(zero_existing.Lock()->ReservedBytes() == 0);
                REQUIRE(heap->ReservedBytes() == fill);
                map->ReleaseCceLock(zero_existing.Lock(),
                                    zero_existing.Entry(),
                                    txm_a->TxNumber(),
                                    kNodeGroup,
                                    LockType::WriteLock);

                // Fewer than K concluded cycles: still parked.
                BumpCycles(2);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/false);
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE_FALSE(parked.result_.IsFinished());

                // A fail-fast request parks beside it and is rejected at the
                // first full or unproductive pass end, regardless of the
                // cycle count.
                REQUIRE(fail_fast.req_.MemoryWaitPolicy() ==
                        MemoryWait::kFailFast);
                REQUIRE(parked.req_.MemoryWaitPolicy() == MemoryWait::kBounded);
                REQUIRE_FALSE(map->Execute(fail_fast.req_));
                REQUIRE(s.WaitListSizeForMemory() == 2);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/true);
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(fail_fast.result_.ErrorCode() ==
                        CcErrorCode::OUT_OF_MEMORY);
                REQUIRE_FALSE(parked.result_.IsFinished());

                // The K-th cycle concludes: rejected with OUT_OF_MEMORY, the
                // ledger untouched.
                BumpCycles(1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/false);
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(parked.result_.IsFinished());
                REQUIRE(parked.result_.ErrorCode() ==
                        CcErrorCode::OUT_OF_MEMORY);
                REQUIRE(parked.req_.Phase() ==
                        AcquireCc::ReservePhase::kUncharged);
                REQUIRE(heap->ReservedBytes() == fill);

                heap->ReleaseReservation(fill);
                REQUIRE(heap->ReservedBytes() == 0);
            });
    }

    // --- Park stamp survives release and re-park; a new use clears it ------
    {
        RestoreWaitCycles restore;
        FLAGS_shard_memory_wait_ckpt_cycles = 2;
        StubRequest stub(/*txn=*/0);
        StubRequest disabled(/*txn=*/0);
        // A request exempt from the cycle rule, as log replay is, and one that
        // is not.
        StubRequest exempt(/*txn=*/0, MemoryWait::kUnbounded);
        StubRequest bounded(/*txn=*/0);
        REQUIRE(ReplayLogCc().MemoryWaitPolicy() == MemoryWait::kUnbounded);
        // A write forwarded to a standby is exempt too: its abort hook puts
        // it back on the shard's queue instead of ending it, so a rejection
        // would only make it run and park again.
        REQUIRE(KeyObjectStandbyForwardCc().MemoryWaitPolicy() ==
                MemoryWait::kUnbounded);
        RunOnShard(
            [&](CcShard &s)
            {
                const uint64_t base = Sharder::Instance()
                                          .GetCheckpointer()
                                          ->ConcludedCkptCycles();
                stub.Use();
                REQUIRE(stub.MemoryParkStamp() == 0);
                s.EnqueueWaitListIfMemoryFull(&stub);
                REQUIRE(stub.MemoryParkStamp() == base + 1);

                // Released by a batch and parked again: the stamp is kept, so
                // the wait is measured from the first park.
                BumpCycles(1);
                s.cc_wait_list_for_memory_.Remove(&stub);
                s.EnqueueWaitListIfMemoryFull(&stub);
                REQUIRE(stub.MemoryParkStamp() == base + 1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/false);
                REQUIRE(s.WaitListSizeForMemory() == 1);

                // A request with no transaction is rejected like any other.
                BumpCycles(1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/false);
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(stub.AbortError() == CcErrorCode::OUT_OF_MEMORY);

                // Taken into use again: a fresh attempt starts unstamped.
                stub.Use();
                REQUIRE(stub.MemoryParkStamp() == 0);

                // An exempt request is stamped like any other but never
                // rejected by the cycle rule, however many cycles conclude. A
                // request parked alongside that is not exempt keeps the rule.
                exempt.Use();
                s.EnqueueWaitListIfMemoryFull(&exempt);
                bounded.Use();
                s.EnqueueWaitListIfMemoryFull(&bounded);
                REQUIRE(exempt.MemoryParkStamp() != 0);
                REQUIRE(bounded.MemoryParkStamp() != 0);
                BumpCycles(10);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/true);
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(exempt.AbortError() == CcErrorCode::NO_ERROR);
                REQUIRE(bounded.AbortError() == CcErrorCode::OUT_OF_MEMORY);
                s.cc_wait_list_for_memory_.Remove(&exempt);
                exempt.Free();

                // K = 0 disables the rule.
                FLAGS_shard_memory_wait_ckpt_cycles = 0;
                disabled.Use();
                s.EnqueueWaitListIfMemoryFull(&disabled);
                BumpCycles(5);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/false);
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(disabled.AbortError() == CcErrorCode::NO_ERROR);
                s.cc_wait_list_for_memory_.Remove(&disabled);
                disabled.Free();
            });
    }

    // --- A checkpoint cycle concludes once every node group's flush has -----
    {
        const int64_t term = Sharder::Instance().LeaderTerm(kNodeGroup);
        REQUIRE(term >= 0);
        // The coordinator has signalled that all tasks are enqueued, and
        // every task has ended.
        auto finalize = [](DataSyncStatus &status)
        {
            std::lock_guard<std::mutex> lk(status.mux_);
            status.unfinished_tasks_ = 0;
            return status.TryFinalizeCheckpointLocked();
        };

        // A run with no node group to visit concludes at once.
        {
            const uint64_t before =
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles();
            auto cycle = std::make_shared<CkptCycle>();
            REQUIRE(cycle->ReleaseCheckpointerHold());
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before + 1);
        }

        // Two groups: the cycle waits for both, whatever their outcome, and
        // finalizing twice counts once.
        {
            const uint64_t before =
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles();
            auto cycle = std::make_shared<CkptCycle>();
            auto flushed = std::make_shared<DataSyncStatus>(
                kNodeGroup, term, true, DataSyncStatus::Origin::Checkpoint);
            auto failed = std::make_shared<DataSyncStatus>(
                kNodeGroup, term, true, DataSyncStatus::Origin::Checkpoint);
            flushed->JoinCkptCycle(cycle);
            failed->JoinCkptCycle(cycle);
            REQUIRE_FALSE(cycle->ReleaseCheckpointerHold());
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before);

            REQUIRE(finalize(*flushed).has_value());
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before);
            REQUIRE_FALSE(finalize(*flushed).has_value());

            failed->err_code_ = CcErrorCode::DATA_STORE_ERR;
            auto outcome = finalize(*failed);
            REQUIRE(outcome.has_value());
            REQUIRE(outcome->outcome_ ==
                    DataSyncStatus::CheckpointOutcome::Failure);
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before + 1);
        }

        // A data sync operation the checkpointer abandons concludes its node
        // group's flush by hand.
        {
            const uint64_t before =
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles();
            auto cycle = std::make_shared<CkptCycle>();
            auto abandoned = std::make_shared<DataSyncStatus>(
                kNodeGroup, term, true, DataSyncStatus::Origin::Checkpoint);
            abandoned->JoinCkptCycle(cycle);
            REQUIRE_FALSE(cycle->ReleaseCheckpointerHold());
            abandoned->LeaveCkptCycle();
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before + 1);
            // Already concluded: finalizing later does not count again.
            REQUIRE(finalize(*abandoned).has_value());
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before + 1);
        }

        // Later cycles' data sync operations merged into a checkpoint flush
        // task an earlier cycle has not started attach to that task, as the
        // enqueue path does: each counts the task as one more unfinished task
        // of its own, so none concludes before the task's flush lands, and
        // every one concludes when it does.
        {
            const uint64_t before =
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles();
            auto slow_cycle = std::make_shared<CkptCycle>();
            // No log truncation: the test node has no log to notify.
            auto slow = std::make_shared<DataSyncStatus>(
                kNodeGroup, term, false, DataSyncStatus::Origin::Checkpoint);
            slow->JoinCkptCycle(slow_cycle);
            REQUIRE_FALSE(slow_cycle->ReleaseCheckpointerHold());
            auto owner = std::make_shared<DataSyncTask>(
                table, 0, 1, kNodeGroup, term, 100, slow, true, true, nullptr);
            slow->RegisterTasks();
            {
                std::lock_guard<std::mutex> lk(slow->mux_);
                REQUIRE_FALSE(slow->EndEnqueuingLocked());
                // Its own task is still scanning, so nothing waits for the
                // flush buffer yet.
                REQUIRE_FALSE(slow->WaitsForFlushBufferLocked());
            }
            std::vector<std::shared_ptr<DataSyncStatus>> folded;
            for (int round = 0; round < 5; ++round)
            {
                auto cycle = std::make_shared<CkptCycle>();
                auto dedup = std::make_shared<DataSyncStatus>(
                    kNodeGroup, term, true, DataSyncStatus::Origin::Checkpoint);
                dedup->JoinCkptCycle(cycle);
                dedup->SetNoTruncateLog();
                owner->AbsorbStatus(dedup);
                REQUIRE_FALSE(cycle->ReleaseCheckpointerHold());
                {
                    // The coordinator signals that all tasks are enqueued
                    // (none of its own), but the task it was merged into
                    // still counts: not finalized.
                    std::lock_guard<std::mutex> lk(dedup->mux_);
                    REQUIRE_FALSE(dedup->EndEnqueuingLocked());
                    REQUIRE(dedup->unfinished_tasks_ == 1);
                    REQUIRE(dedup->absorbed_tasks_ == 1);
                    // It has no scan left and one unfinished task, but that
                    // task is the earlier cycle's, not its own: it put
                    // nothing in the flush buffer, so the buffer is not
                    // flushed on its account.
                    REQUIRE(dedup->unfinished_scan_tasks_ == 0);
                    REQUIRE_FALSE(dedup->WaitsForFlushBufferLocked());
                    REQUIRE_FALSE(
                        dedup->TryFinalizeCheckpointLocked().has_value());
                }
                REQUIRE(Sharder::Instance()
                            .GetCheckpointer()
                            ->ConcludedCkptCycles() == before);
                folded.push_back(std::move(dedup));
            }
            // The flush lands: the task's own data sync operation and every
            // data sync operation merged into it conclude, one each.
            owner->SetFinish();
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before + 6);
            for (const auto &dedup : folded)
            {
                std::lock_guard<std::mutex> lk(dedup->mux_);
                REQUIRE(dedup->unfinished_tasks_ == 0);
                REQUIRE(dedup->absorbed_tasks_ == 0);
                REQUIRE(dedup->checkpoint_finalized_);
            }
        }

        // The flush buffer is flushed on a data sync operation's account
        // only while a task of its own has finished scanning and waits for
        // the buffer.
        {
            auto own = std::make_shared<DataSyncStatus>(
                kNodeGroup, term, false, DataSyncStatus::Origin::FlushData);
            own->RegisterTasks();
            std::lock_guard<std::mutex> lk(own->mux_);
            REQUIRE_FALSE(own->EndEnqueuingLocked());
            // Its task is still scanning.
            REQUIRE_FALSE(own->WaitsForFlushBufferLocked());
            // The scan finishes, as DataSyncTask::SetScanTaskFinished()
            // records: the task now waits for the buffer.
            own->unfinished_scan_tasks_--;
            REQUIRE(own->WaitsForFlushBufferLocked());
            // The flush lands and the task ends.
            REQUIRE(own->EndTaskLocked());
            REQUIRE_FALSE(own->WaitsForFlushBufferLocked());
        }

        // A data sync operation whose coordinator is still enqueuing: it is
        // merged into another task for one range, registers a task of its own
        // for another, and both tasks finish before the coordinator signals
        // that all tasks are enqueued. The coordinator's count keeps the data
        // sync operation's count above zero, and the data sync operation
        // concludes only after that signal.
        {
            const uint64_t before =
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles();
            auto owner_cycle = std::make_shared<CkptCycle>();
            auto owner_status = std::make_shared<DataSyncStatus>(
                kNodeGroup, term, false, DataSyncStatus::Origin::Checkpoint);
            owner_status->JoinCkptCycle(owner_cycle);
            REQUIRE_FALSE(owner_cycle->ReleaseCheckpointerHold());
            owner_status->RegisterTasks();
            {
                std::lock_guard<std::mutex> lk(owner_status->mux_);
                REQUIRE_FALSE(owner_status->EndEnqueuingLocked());
            }
            auto owner = std::make_shared<DataSyncTask>(table,
                                                        0,
                                                        1,
                                                        kNodeGroup,
                                                        term,
                                                        100,
                                                        owner_status,
                                                        true,
                                                        true,
                                                        nullptr);

            auto round_cycle = std::make_shared<CkptCycle>();
            auto round = std::make_shared<DataSyncStatus>(
                kNodeGroup, term, false, DataSyncStatus::Origin::Checkpoint);
            round->JoinCkptCycle(round_cycle);
            REQUIRE_FALSE(round_cycle->ReleaseCheckpointerHold());
            // Deduplicated for the owner's range.
            round->SetNoTruncateLog();
            owner->AbsorbStatus(round);
            // A task of its own for another range, registered, then run.
            round->RegisterTasks();
            auto own = std::make_shared<DataSyncTask>(
                table, 1, 1, kNodeGroup, term, 200, round, true, true, nullptr);
            own->SetFinish();
            {
                // The task it was merged into and the coordinator's count
                // remain.
                std::lock_guard<std::mutex> lk(round->mux_);
                REQUIRE(round->unfinished_tasks_ == 2);
                REQUIRE_FALSE(round->checkpoint_finalized_);
            }
            // The owner's flush lands: the owner's own data sync operation
            // concludes; the other is down to its coordinator's count, not
            // finalized yet.
            owner->SetFinish();
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before + 1);
            {
                std::lock_guard<std::mutex> lk(round->mux_);
                REQUIRE(round->unfinished_tasks_ == 1);
                REQUIRE_FALSE(round->checkpoint_finalized_);
                // The coordinator signals that all tasks are enqueued,
                // reaches zero and finishes the data sync operation.
                REQUIRE(round->EndEnqueuingLocked());
                REQUIRE(round->FinishLocked(nullptr).has_value());
            }
            REQUIRE(
                Sharder::Instance().GetCheckpointer()->ConcludedCkptCycles() ==
                before + 2);
        }

        // A task fails before its coordinator (range split, index creation,
        // bucket migration) signals that all tasks are enqueued. The
        // coordinator then brings the count to zero, and the result handle
        // gets the task's error, not success.
        {
            CcHandlerResult<Void> hres(nullptr);
            auto status = std::make_shared<DataSyncStatus>(
                kNodeGroup, term, false, DataSyncStatus::Origin::RangeSplit);
            status->RegisterTasks();
            auto task = std::make_shared<DataSyncTask>(
                table, 0, 1, kNodeGroup, term, 100, status, true, false, &hres);
            task->SetError(CcErrorCode::DATA_STORE_ERR);
            REQUIRE_FALSE(hres.IsFinished());
            {
                std::lock_guard<std::mutex> lk(status->mux_);
                REQUIRE(status->EndEnqueuingLocked());
                REQUIRE_FALSE(status->FinishLocked(&hres).has_value());
            }
            REQUIRE(hres.IsFinished());
            REQUIRE(hres.ErrorCode() == CcErrorCode::DATA_STORE_ERR);
        }

        // The live checkpointer's wiring (its hold, one flush per node group,
        // skipped node groups adding none) is exercised by CkptCycle-Test
        // against a store-backed node; a held live checkpoint flush task with
        // rounds merged into it, and a node group's flush abandoned on a
        // leadership change, by EloqDoc's cmake/tests/multinode/ckpt_cycles.py.
    }

    // --- Blocked-request probe: only a terminal status closes the slot -----
    // Each result is counted as remote first, as LocalCcHandler::AcquireWrite
    // does when it sends an acquisition to another node.
    {
        CcHandlerResult<std::vector<AcquireKeyResult>> result(nullptr);
        result.Value().resize(1);
        AcquireKeyResult &slot = result.Value()[0];
        slot.Reset();
        result.IncreaseRemoteRef();

        remote::ApplyAcquireBlockCheckStatus(
            slot, result, AckStatus::BlockQueue);
        REQUIRE_FALSE(slot.IsRemoteHdResultSet(std::memory_order_acquire));
        REQUIRE_FALSE(result.IsFinished());
        REQUIRE_FALSE(result.IsError());
        REQUIRE(result.RemoteRefCnt() == 1);

        remote::ApplyAcquireBlockCheckStatus(slot, result, AckStatus::Finished);
        REQUIRE(slot.IsRemoteHdResultSet(std::memory_order_acquire));
        REQUIRE(result.IsError());
        REQUIRE(result.ErrorCode() == CcErrorCode::REQUEST_LOST);
        REQUIRE(result.RemoteRefCnt() == 0);

        CcHandlerResult<std::vector<AcquireKeyResult>> term_result(nullptr);
        term_result.Value().resize(1);
        AcquireKeyResult &term_slot = term_result.Value()[0];
        term_slot.Reset();
        term_result.IncreaseRemoteRef();
        remote::ApplyAcquireBlockCheckStatus(
            term_slot, term_result, AckStatus::ErrorTerm);
        REQUIRE(term_slot.IsRemoteHdResultSet(std::memory_order_acquire));
        REQUIRE(term_result.ErrorCode() == CcErrorCode::NG_TERM_CHANGED);
        REQUIRE(term_result.RemoteRefCnt() == 0);
    }

    // --- Probe of one of two remote acquisitions: counts stay consistent ----
    // Two remote acquisitions, A and B, and nothing local. A's normal
    // response is lost and its probe reports Finished: the total and the
    // remote count both drop to 1, so the local count stays 0 and a later
    // timeout still probes B instead of only asking for a deadlock check.
    // B's normal response then finishes the result.
    {
        CcHandlerResult<std::vector<AcquireKeyResult>> result(nullptr);
        result.Value().resize(2);
        result.Value()[0].Reset();
        result.Value()[1].Reset();
        result.SetRefCnt(2);
        result.IncreaseRemoteRef();
        result.IncreaseRemoteRef();
        REQUIRE(result.LocalRefCnt() == 0);

        remote::ApplyAcquireBlockCheckStatus(
            result.Value()[0], result, AckStatus::Finished);
        REQUIRE(
            result.Value()[0].IsRemoteHdResultSet(std::memory_order_acquire));
        REQUIRE_FALSE(result.IsFinished());
        REQUIRE(result.RefCnt() == 1);
        REQUIRE(result.RemoteRefCnt() == 1);
        REQUIRE(result.LocalRefCnt() == 0);

        // B is still queued on its remote node: nothing changes.
        remote::ApplyAcquireBlockCheckStatus(
            result.Value()[1], result, AckStatus::BlockQueue);
        REQUIRE(result.RefCnt() == 1);
        REQUIRE(result.RemoteRefCnt() == 1);

        // B's response arrives, as the AcquireResponse handler finishes it.
        result.Value()[1].SetRemoteHdResult(true, std::memory_order_release);
        result.SetRemoteFinished();
        REQUIRE(result.IsFinished());
        REQUIRE(result.ErrorCode() == CcErrorCode::REQUEST_LOST);
        REQUIRE(result.RemoteRefCnt() == 0);
    }

    // --- The local count reads 0, never a wrapped value, when the remote
    // count is not below the total ------------------------------------------
    {
        CcHandlerResult<std::vector<AcquireKeyResult>> result(nullptr);
        result.SetRefCnt(1);
        result.IncreaseRemoteRef();
        result.IncreaseRemoteRef();
        REQUIRE(result.RefCnt() == 1);
        REQUIRE(result.RemoteRefCnt() == 2);
        REQUIRE(result.LocalRefCnt() == 0);
        result.SetRefCnt(3);
        result.IncreaseRemoteRef();
        REQUIRE(result.LocalRefCnt() == 2);
    }

    // --- A store fetch checks capacity before it installs its record -------
    {
        RestoreWaitCycles restore;
        FLAGS_shard_memory_wait_ckpt_cycles = 2;

        using PkEntry =
            CcEntry<CompositeKey<int>, CompositeRecord<int>, true, false>;
        /** The store's answer a fetch carries in these scenarios. */
        constexpr uint64_t kFetchedCommitTs = 5;

        // Registers a fetch for a key that is not cached, as
        // CcShard::FetchRecord() does, but without asking the store: the
        // answer is filled in here. `waiter` is the request waiting on the
        // fetch, or null for a fetch nobody waits on, as log replay issues.
        // `status` is the status of the record the store returned.
        auto start_fetch =
            [&](CcShard &s, int key, CcRequestBase *waiter, RecordStatus status)
            -> std::pair<FetchRecordCc *, PkEntry *>
        {
            bool emplaced = false;
            auto it = map->FindEmplace(CompositeKey<int>(key), &emplaced);
            REQUIRE(emplaced);
            PkEntry *cce = it->second;
            auto *ccp = it.GetPage();
            REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);
            cce->GetOrCreateKeyLock(&s, map, ccp);

            FetchRecordCc::uptr owner(new FetchRecordCc(),
                                      FetchRecordCc::Deleter{false});
            owner->Use();
            FetchRecordCc *fetch = owner.get();
            fetch->Reset(&table,
                         map->GetTableSchema(),
                         TxKey(ccp->KeyOfEntry(cce)),
                         cce,
                         s,
                         kNodeGroup,
                         Sharder::Instance().LeaderTerm(kNodeGroup),
                         /*partition_id=*/0);
            REQUIRE(
                s.fetch_record_reqs_.try_emplace(cce, std::move(owner)).second);
            fetch->AddRequester(waiter);
            // One pin for the fetch, one for the waiting request.
            cce->GetKeyGapLockAndExtraData()->AddPin();
            if (waiter != nullptr)
            {
                cce->GetKeyGapLockAndExtraData()->AddPin();
            }

            fetch->rec_status_ = status;
            fetch->rec_ts_ = kFetchedCommitTs;
            if (status == RecordStatus::Normal)
            {
                CompositeRecord<int>(key * 10).Serialize(fetch->rec_str_);
            }
            return {fetch, cce};
        };
        // Fills the heap with reservations and returns the amount to release.
        auto fill_heap = [&]() -> size_t
        {
            const size_t fill =
                limit - static_cast<size_t>(heap->PhysicalAllocated());
            REQUIRE(heap->TryReserve(fill));
            REQUIRE(heap->Full());
            return fill;
        };
        // A real read releases its own pin when the fetch resumes it; the
        // stub does not, so the test releases it in its place.
        auto release_waiter_pin = [&](CcShard &s, PkEntry *cce)
        {
            cce->GetKeyGapLockAndExtraData()->ReleasePin();
            cce->RecycleKeyLock(s);
            REQUIRE(cce->GetKeyGapLockAndExtraData() == nullptr);
        };

        // A full heap parks the fetch with nothing installed; once there is
        // room it installs the record and resumes the waiting request.
        StubRequest reader(/*txn=*/0);
        RunOnShard(
            [&](CcShard &s)
            {
                reader.Use();
                auto [fetch, cce] =
                    start_fetch(s, 601, &reader, RecordStatus::Normal);
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kBounded);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);
                REQUIRE(reader.Executed() == 0);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 1);

                // Room again: the cleaner would re-enqueue the fetch; it is
                // executed here in its place.
                heap->ReleaseReservation(fill);
                s.cc_wait_list_for_memory_.Remove(fetch);
                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(cce->PayloadStatus() == RecordStatus::Normal);
                REQUIRE(cce->CommitTs() == kFetchedCommitTs);
                REQUIRE(reader.Executed() == 1);
                REQUIRE(reader.AbortError() == CcErrorCode::NO_ERROR);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 0);
                release_waiter_pin(s, cce);
            });

        // The check is whether the heap is already full, not whether the
        // fetched record fits: with 1 MiB of room left, a 2 MiB record is
        // installed at once, since the heap is below its limit.
        StubRequest big_reader(/*txn=*/0);
        RunOnShard(
            [&](CcShard &s)
            {
                big_reader.Use();
                auto [fetch, cce] =
                    start_fetch(s, 609, &big_reader, RecordStatus::Normal);
                // Deserializing reads only the record's own bytes, so padding
                // after them only makes the fetched record large.
                fetch->rec_str_.resize(2 * kMiB, 'x');
                const size_t fill =
                    limit - static_cast<size_t>(heap->PhysicalAllocated()) -
                    kMiB;
                REQUIRE(heap->TryReserve(fill));
                REQUIRE_FALSE(heap->Full());

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Normal);
                REQUIRE(big_reader.Executed() == 1);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 0);
                release_waiter_pin(s, cce);

                heap->ReleaseReservation(fill);
            });

        // A fetch still parked after the configured number of checkpoint
        // cycles is rejected: the waiting request fails with OUT_OF_MEMORY,
        // nothing is installed, and both pins are released.
        StubRequest waited_out(/*txn=*/0);
        RunOnShard(
            [&](CcShard &s)
            {
                waited_out.Use();
                auto [fetch, cce] =
                    start_fetch(s, 602, &waited_out, RecordStatus::Normal);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 1);
                BumpCycles(1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/true);
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(waited_out.AbortError() == CcErrorCode::NO_ERROR);

                BumpCycles(1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/false);
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(waited_out.AbortError() == CcErrorCode::OUT_OF_MEMORY);
                REQUIRE(waited_out.Executed() == 0);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 0);
                REQUIRE(cce->GetKeyGapLockAndExtraData() == nullptr);

                heap->ReleaseReservation(fill);
            });

        // A fetch that a fail-fast request waits on fails fast itself.
        StubRequest hurried(/*txn=*/0, MemoryWait::kFailFast);
        RunOnShard(
            [&](CcShard &s)
            {
                hurried.Use();
                auto [fetch, cce] =
                    start_fetch(s, 603, &hurried, RecordStatus::Normal);
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kFailFast);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/true);
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(hurried.AbortError() == CcErrorCode::OUT_OF_MEMORY);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);
                REQUIRE(cce->GetKeyGapLockAndExtraData() == nullptr);

                heap->ReleaseReservation(fill);
            });

        // A fetch that survives a rejection with only issuers that cannot be
        // rejected is re-enqueued on the shard, so it runs after the closure
        // that rejected it. Waits for that run, then checks that the record
        // of `key` was installed. `cce` is only compared, never dereferenced.
        auto expect_installed_later = [&](int key, PkEntry *cce)
        {
            REQUIRE(WaitFor(
                [&]
                {
                    bool done = false;
                    RunOnShard(
                        [&](CcShard &s)
                        { done = s.fetch_record_reqs_.count(cce) == 0; });
                    return done;
                }));
            RunOnShard(
                [&](CcShard &)
                {
                    auto it = map->Find(CompositeKey<int>(key));
                    REQUIRE(it->second != nullptr);
                    REQUIRE(it->second->PayloadStatus() ==
                            RecordStatus::Normal);
                    REQUIRE(it->second->GetKeyGapLockAndExtraData() == nullptr);
                });
        };

        // A rejection applies to each waiting request by its own policy. A
        // fail-fast request sharing a fetch with an issuer that waits on
        // nothing, as log replay does, makes the fetch fail fast, but only
        // that request is rejected: the fetch survives and installs its
        // record.
        StubRequest shared_hurried(/*txn=*/0, MemoryWait::kFailFast);
        PkEntry *replay_shared_cce = nullptr;
        RunOnShard(
            [&](CcShard &s)
            {
                shared_hurried.Use();
                auto [fetch, cce] =
                    start_fetch(s, 606, &shared_hurried, RecordStatus::Normal);
                replay_shared_cce = cce;
                fetch->AddRequester(nullptr);
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kFailFast);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/true);
                REQUIRE(shared_hurried.AbortError() ==
                        CcErrorCode::OUT_OF_MEMORY);
                // Kept for the issuer that cannot be rejected: no longer
                // waiting for memory, and queued to install.
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 1);
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kUnbounded);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);

                heap->ReleaseReservation(fill);
            });
        expect_installed_later(606, replay_shared_cce);

        // A fail-fast request sharing a fetch with a bounded one is rejected
        // alone. The bounded request keeps waiting on the same fetch, which
        // goes back on the wait list with its park stamp and installs once
        // there is room.
        StubRequest paired_hurried(/*txn=*/0, MemoryWait::kFailFast);
        StubRequest paired_bounded(/*txn=*/0);
        RunOnShard(
            [&](CcShard &s)
            {
                paired_hurried.Use();
                paired_bounded.Use();
                auto [fetch, cce] =
                    start_fetch(s, 607, &paired_hurried, RecordStatus::Normal);
                fetch->AddRequester(&paired_bounded);
                cce->GetKeyGapLockAndExtraData()->AddPin();
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kFailFast);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                const uint64_t stamp = fetch->MemoryParkStamp();
                REQUIRE(stamp != 0);
                BumpCycles(1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/true);
                REQUIRE(paired_hurried.AbortError() ==
                        CcErrorCode::OUT_OF_MEMORY);
                REQUIRE(paired_bounded.AbortError() == CcErrorCode::NO_ERROR);
                REQUIRE(paired_bounded.Executed() == 0);
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kBounded);
                REQUIRE(fetch->MemoryParkStamp() == stamp);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Unknown);

                heap->ReleaseReservation(fill);
                s.cc_wait_list_for_memory_.Remove(fetch);
                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(cce->PayloadStatus() == RecordStatus::Normal);
                REQUIRE(paired_bounded.Executed() == 1);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 0);
                release_waiter_pin(s, cce);
            });

        // A bounded request sharing a fetch with an issuer that waits on
        // nothing is rejected once the fetch has waited out the cycle bound;
        // the fetch survives for that issuer and installs its record.
        StubRequest shared_bounded(/*txn=*/0);
        PkEntry *waited_out_shared_cce = nullptr;
        RunOnShard(
            [&](CcShard &s)
            {
                shared_bounded.Use();
                auto [fetch, cce] =
                    start_fetch(s, 608, &shared_bounded, RecordStatus::Normal);
                waited_out_shared_cce = cce;
                fetch->AddRequester(nullptr);
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kBounded);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                BumpCycles(1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/true);
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(shared_bounded.AbortError() == CcErrorCode::NO_ERROR);

                BumpCycles(1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/false);
                REQUIRE(shared_bounded.AbortError() ==
                        CcErrorCode::OUT_OF_MEMORY);
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 1);
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kUnbounded);

                heap->ReleaseReservation(fill);
            });
        expect_installed_later(608, waited_out_shared_cce);

        // A fetch nobody waits on has nobody to reject, so it does not wait:
        // it installs its record on a full heap.
        RunOnShard(
            [&](CcShard &s)
            {
                auto [fetch, cce] =
                    start_fetch(s, 604, nullptr, RecordStatus::Normal);
                REQUIRE(fetch->MemoryWaitPolicy() == MemoryWait::kUnbounded);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Normal);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 0);
                REQUIRE(cce->GetKeyGapLockAndExtraData() == nullptr);

                heap->ReleaseReservation(fill);
            });

        // A fetch that found the key deleted installs no value, so it does
        // not wait either.
        StubRequest tombstone_reader(/*txn=*/0);
        RunOnShard(
            [&](CcShard &s)
            {
                tombstone_reader.Use();
                auto [fetch, cce] = start_fetch(
                    s, 605, &tombstone_reader, RecordStatus::Deleted);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Deleted);
                REQUIRE(tombstone_reader.Executed() == 1);
                release_waiter_pin(s, cce);

                heap->ReleaseReservation(fill);
            });

        // --- Historical versions -------------------------------------------
        // A snapshot read that finds its key cached, but needs a version
        // older than the cached one, fetches historical versions only. Key 1
        // is cached with a commit timestamp far above the ones used here.
        using ArchiveRecords =
            std::vector<std::tuple<uint64_t, RecordStatus, std::string>>;
        /** Commit timestamp of the historical version the store returns. */
        constexpr uint64_t kOldVersionTs = 2;
        /** The snapshot the reads below run at: it sees that version. */
        constexpr uint64_t kSnapshotTs = 3;

        // Registers a fetch of historical versions only for the cached entry
        // of key 1, with the store's answer filled in: one version committed
        // at kOldVersionTs, with `status`.
        auto start_archive_fetch =
            [&](CcShard &s,
                CcRequestBase *waiter,
                RecordStatus status) -> std::pair<FetchRecordCc *, PkEntry *>
        {
            auto it = map->Find(CompositeKey<int>(1));
            PkEntry *cce = it->second;
            REQUIRE(cce != nullptr);
            REQUIRE(cce->PayloadStatus() == RecordStatus::Normal);
            REQUIRE(cce->CommitTs() > kSnapshotTs);
            auto *ccp = it.GetPage();
            cce->GetOrCreateKeyLock(&s, map, ccp);

            FetchRecordCc::uptr owner(new FetchRecordCc(),
                                      FetchRecordCc::Deleter{false});
            owner->Use();
            FetchRecordCc *fetch = owner.get();
            fetch->Reset(&table,
                         map->GetTableSchema(),
                         TxKey(ccp->KeyOfEntry(cce)),
                         cce,
                         s,
                         kNodeGroup,
                         Sharder::Instance().LeaderTerm(kNodeGroup),
                         /*partition_id=*/0,
                         /*fetch_from_primary=*/false,
                         /*snapshot_read_ts=*/kSnapshotTs,
                         /*only_fetch_archives=*/true);
            REQUIRE(
                s.fetch_record_reqs_.try_emplace(cce, std::move(owner)).second);
            fetch->AddRequester(waiter);
            // One pin for the fetch, one for the waiting request.
            cce->GetKeyGapLockAndExtraData()->AddPin();
            if (waiter != nullptr)
            {
                cce->GetKeyGapLockAndExtraData()->AddPin();
            }

            std::string version;
            if (status == RecordStatus::Normal)
            {
                CompositeRecord<int>(77).Serialize(version);
            }
            fetch->archive_records_ = std::make_unique<ArchiveRecords>();
            fetch->archive_records_->emplace_back(
                kOldVersionTs, status, std::move(version));
            return {fetch, cce};
        };
        // The number of historical versions an entry holds.
        auto archive_count = [](PkEntry *cce) -> size_t
        {
            auto *archives = cce->payload_.GetArchives();
            return archives == nullptr ? 0 : archives->size();
        };
        // The status of the version of an entry a snapshot at kSnapshotTs
        // sees.
        auto snapshot_status = [&](PkEntry *cce) -> RecordStatus
        {
            VersionResultRecord<CompositeRecord<int>> version;
            cce->MvccGet(kSnapshotTs, version);
            return version.payload_status_;
        };

        // Historical versions are admitted like the current value: on a full
        // heap the fetch parks with no version installed, and once there is
        // room it installs the version and resumes the read, which then finds
        // the version its snapshot needs.
        StubRequest snapshot_reader(/*txn=*/0);
        RunOnShard(
            [&](CcShard &s)
            {
                snapshot_reader.Use();
                auto [fetch, cce] = start_archive_fetch(
                    s, &snapshot_reader, RecordStatus::Normal);
                const size_t versions_before = archive_count(cce);
                REQUIRE(snapshot_status(cce) != RecordStatus::Normal);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 1);
                REQUIRE(archive_count(cce) == versions_before);
                REQUIRE(snapshot_reader.Executed() == 0);

                heap->ReleaseReservation(fill);
                s.cc_wait_list_for_memory_.Remove(fetch);
                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(archive_count(cce) == versions_before + 1);
                REQUIRE(snapshot_status(cce) == RecordStatus::Normal);
                REQUIRE(snapshot_reader.Executed() == 1);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 0);
                release_waiter_pin(s, cce);
            });

        // A fetch of historical versions is rejected like any other: the
        // fail-fast read fails with OUT_OF_MEMORY, no version is installed,
        // and both pins are released. The version fetched here has a commit
        // timestamp the entry does not hold yet, so installing it would show
        // in the count.
        StubRequest hurried_snapshot_reader(/*txn=*/0, MemoryWait::kFailFast);
        RunOnShard(
            [&](CcShard &s)
            {
                hurried_snapshot_reader.Use();
                auto [fetch, cce] = start_archive_fetch(
                    s, &hurried_snapshot_reader, RecordStatus::Normal);
                std::get<0>(fetch->archive_records_->front()) = 1;
                const size_t versions_before = archive_count(cce);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 1);
                s.AbortRequestsAfterMemoryFree(/*reject_fail_fast=*/true);
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(hurried_snapshot_reader.AbortError() ==
                        CcErrorCode::OUT_OF_MEMORY);
                REQUIRE(archive_count(cce) == versions_before);
                REQUIRE(cce->PayloadStatus() == RecordStatus::Normal);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 0);
                REQUIRE(cce->GetKeyGapLockAndExtraData() == nullptr);

                heap->ReleaseReservation(fill);
            });

        // Historical versions that are all tombstones carry no value, so the
        // fetch does not wait on a full heap.
        StubRequest tombstone_snapshot_reader(/*txn=*/0);
        RunOnShard(
            [&](CcShard &s)
            {
                tombstone_snapshot_reader.Use();
                auto [fetch, cce] = start_archive_fetch(
                    s, &tombstone_snapshot_reader, RecordStatus::Deleted);
                const size_t fill = fill_heap();

                REQUIRE_FALSE(fetch->Execute(s));
                REQUIRE(s.WaitListSizeForMemory() == 0);
                REQUIRE(tombstone_snapshot_reader.Executed() == 1);
                REQUIRE(s.fetch_record_reqs_.count(cce) == 0);
                release_waiter_pin(s, cce);

                heap->ReleaseReservation(fill);
            });

        RunOnShard([&](CcShard &) { REQUIRE(heap->ReservedBytes() == 0); });
    }

    // --- Real transactions: the ledger is empty at every quiescent point ---
    {
        REQUIRE(tx_a.Abort());
        REQUIRE(tx_b.Abort());
        RunOnShard([&](CcShard &) { REQUIRE(heap->ReservedBytes() == 0); });

        auto t = node.BeginTx();
        REQUIRE(t.Upsert(2, 200));
        REQUIRE(t.Upsert(3, 300));
        REQUIRE(t.Commit());
        auto u = node.BeginTx();
        REQUIRE(u.Upsert(2, 201));
        REQUIRE(u.Abort());
        RunOnShard([&](CcShard &) { REQUIRE(heap->ReservedBytes() == 0); });

        auto r = node.BeginTx();
        int v = 0;
        REQUIRE(r.Read(2, v));
        REQUIRE(v == 200);
        REQUIRE(r.Read(3, v));
        REQUIRE(v == 300);
        REQUIRE(r.Commit());
    }
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
