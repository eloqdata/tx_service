#include <catch2/catch_all.hpp>

#include "harness/test_node.h"

using namespace txservice;
using namespace txservice::test;

// IMPORTANT: at most one TestNode may be constructed per process. The engine's
// Sharder is a process-global singleton whose brpc servers (cc-node,
// log-replay) register their services once; a second TestNode in the same
// process fails to re-Start ("service ... already exists"). Catch2 runs every
// TEST_CASE in the same process, so all of a binary's assertions must share a
// single TestNode. Hence one TEST_CASE that drives the full round-trip through
// a sequence of transactions, rather than one TestNode per case.

TEST_CASE("TestNode write/read/delete round-trip", "[testnode]")
{
    TestNode node(TestNodeOptions{}.CoreNum(2));

    // An empty transaction commits (bring-up + commit path).
    {
        auto t = node.BeginTx();
        REQUIRE(t.Commit());
    }

    // Write 1 -> 100 and commit.
    {
        auto t = node.BeginTx();
        REQUIRE(t.Upsert(1, 100));
        REQUIRE(t.Commit());
    }

    // A fresh transaction reads back the committed value.
    {
        auto t = node.BeginTx();
        int v = 0;
        REQUIRE(t.Read(1, v));
        REQUIRE(v == 100);
        REQUIRE(t.Commit());
    }

    // A key that was never written reads as absent (Read returns false).
    {
        auto t = node.BeginTx();
        int v = 0;
        REQUIRE_FALSE(t.Read(999, v));
        REQUIRE(t.Commit());
    }

    // Overwrite an existing key, then read the new value back.
    {
        auto t = node.BeginTx();
        REQUIRE(t.Upsert(1, 200));
        REQUIRE(t.Commit());
    }
    {
        auto t = node.BeginTx();
        int v = 0;
        REQUIRE(t.Read(1, v));
        REQUIRE(v == 200);
        REQUIRE(t.Commit());
    }

    // Delete the key, then it reads as absent.
    {
        auto t = node.BeginTx();
        REQUIRE(t.Delete(1));
        REQUIRE(t.Commit());
    }
    {
        auto t = node.BeginTx();
        int v = 0;
        REQUIRE_FALSE(t.Read(1, v));
        REQUIRE(t.Commit());
    }
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
