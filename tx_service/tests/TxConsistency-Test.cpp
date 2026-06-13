#include <catch2/catch_all.hpp>

#include "harness/test_node.h"

using namespace txservice;
using namespace txservice::test;

// IMPORTANT: at most one TestNode may be constructed per process (the engine's
// Sharder is a process-global singleton whose brpc servers register their
// services once; a second TestNode fails to re-Start). Catch2 re-runs a
// TEST_CASE body once per leaf SECTION, which would reconstruct the TestNode,
// so this file uses a SINGLE TestNode and drives every scenario as a sequential
// scoped block (distinct keys per scenario), with NO SECTIONs.

TEST_CASE("transaction consistency on TestNode", "[tx]")
{
    TestNode node(TestNodeOptions{}.CoreNum(2));

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
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
