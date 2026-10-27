#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "catch2/catch_all.hpp"
#include "cc/cc_req_misc.h"
#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "error_messages.h"
#include "harness/test_node.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_record.h"
#include "tx_request.h"

using namespace txservice;
using namespace txservice::test;

namespace
{
/**
 * Inject a declared payload footprint without allocating a heap-sized buffer.
 * The real coordinator, AcquireCc, aggregation and abort tail still run. This
 * controls admission only; it is not an allocator-peak or durable-WAL test.
 */
class BudgetedRecord : public CompositeRecord<int>
{
public:
    BudgetedRecord(int value, size_t budget)
        : CompositeRecord<int>(std::move(value)), budget_(budget)
    {
    }

    size_t MemUsage() const override
    {
        return budget_;
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<BudgetedRecord>(*this);
    }

private:
    size_t budget_;
};

struct HeapSnapshot
{
    size_t limit{0};
    int64_t reserved{0};
    bool full{true};
};

std::vector<HeapSnapshot> SnapshotHeaps()
{
    LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
    std::vector<HeapSnapshot> result(shards->Count());
    for (size_t core = 0; core < result.size(); ++core)
    {
        WaitableCc snapshot(
            [&, core](CcShard &ccs)
            {
                CcShardHeap *heap = ccs.GetShardHeap();
                if (heap != nullptr)
                {
                    result[core] = {heap->MemoryLimit(),
                                    heap->ReservedBytes(),
                                    heap->FullWithReservation()};
                }
                return true;
            });
        shards->EnqueueCcRequest(core, &snapshot);
        snapshot.Wait();
        REQUIRE_FALSE(snapshot.IsError());
        REQUIRE(result[core].limit > 0);
    }
    return result;
}
}  // namespace

TEST_CASE("payload OOM aborts a complete transaction before retry",
          "[tx][payload]")
{
    // Sharder is process-global: construct exactly one node, without SECTIONs.
    TestNode node(TestNodeOptions{}.CoreNum(2));
    const auto baseline = SnapshotHeaps();
    REQUIRE(baseline[0].limit == baseline[1].limit);

    struct ReadMode
    {
        CcProtocol protocol;
        bool for_write;
    };
    const std::array<ReadMode, 3> modes{{{CcProtocol::OCC, false},
                                         {CcProtocol::Locking, false},
                                         {CcProtocol::Locking, true}}};
    int key = 100;
    for (const ReadMode &mode : modes)
    {
        auto seed = node.BeginTx();
        REQUIRE(seed.Upsert(key, 10));
        REQUIRE(seed.Upsert(key + 1, 20));
        REQUIRE(seed.Commit());

        auto tx = node.BeginTx(IsolationLevel::RepeatableRead, mode.protocol);
        TxKey read_key = Key(key);
        CompositeRecord<int> old_value;
        ReadTxRequest read(&node.Table(),
                           node.SchemaVersion(),
                           &read_key,
                           &old_value,
                           mode.for_write);
        tx.Txm()->Execute(&read);
        read.Wait();
        REQUIRE_FALSE(read.IsError());
        REQUIRE(read.Result().first == RecordStatus::Normal);

        // This request is <= the heap limit, but cannot fit alongside existing
        // allocations. It must fail at reservation, not enter the memory queue.
        for (const HeapSnapshot &heap : SnapshotHeaps())
        {
            REQUIRE_FALSE(heap.full);
        }
        UpsertTxRequest large(
            &node.Table(),
            Key(key),
            std::make_unique<BudgetedRecord>(11, baseline[0].limit),
            OperationType::Upsert);
        tx.Txm()->Execute(&large);
        large.Wait();
        REQUIRE_FALSE(large.IsError());
        REQUIRE(tx.Upsert(key + 1, 21));
        CommitTxRequest commit;
        tx.Txm()->Execute(&commit);
        commit.Wait();
        // A failed commit finishes its result with Result() == false and the
        // error code, but not with the TxResultStatus::Error state.
        REQUIRE_FALSE(commit.Result());
        REQUIRE(commit.ErrorCode() == TxErrorCode::OUT_OF_MEMORY);

        const auto after_abort = SnapshotHeaps();
        for (size_t core = 0; core < baseline.size(); ++core)
        {
            REQUIRE(after_abort[core].reserved == baseline[core].reserved);
        }
        auto verify_abort = node.BeginTx();
        int value = 0;
        REQUIRE(verify_abort.Read(key, value));
        REQUIRE(value == 10);
        REQUIRE(verify_abort.Read(key + 1, value));
        REQUIRE(value == 20);
        REQUIRE(verify_abort.Commit());

        // Retry the entire transaction, not the failed participant request.
        auto retry =
            node.BeginTx(IsolationLevel::RepeatableRead, mode.protocol);
        REQUIRE(retry.Read(key, value));
        REQUIRE(retry.Upsert(key, 11));
        REQUIRE(retry.Upsert(key + 1, 21));
        REQUIRE(retry.Commit());
        auto verify_commit = node.BeginTx();
        REQUIRE(verify_commit.Read(key, value));
        REQUIRE(value == 11);
        REQUIRE(verify_commit.Read(key + 1, value));
        REQUIRE(value == 21);
        REQUIRE(verify_commit.Commit());
        const auto after_commit = SnapshotHeaps();
        for (size_t core = 0; core < baseline.size(); ++core)
        {
            REQUIRE(after_commit[core].reserved == baseline[core].reserved);
        }
        key += 2;
    }
}
