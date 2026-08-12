#include <catch2/catch_all.hpp>

#include "catalog_key_record.h"
#include "harness/test_node.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_request.h"
#include "type.h"

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

    // A metadata-only transaction must use the caller-owned close request so
    // CommitTx does not return before metadata post-processing has completed.
    constexpr bool close_modes[] = {true, false};
    for (bool to_commit : close_modes)
    {
        auto t = node.BeginTx();
        CatalogKey catalog_key(node.Table());
        TxKey tx_key(&catalog_key);
        CatalogRecord catalog_record;
        ReadTxRequest read_req(
            &catalog_ccm_name, 0, &tx_key, &catalog_record, false, false, true);
        t.Txm()->Execute(&read_req);
        read_req.Wait();
        REQUIRE_FALSE(read_req.IsError());

        CommitTxRequest close_req(to_commit);
        const bool closed = t.Txm()->CommitTx(close_req);
        CAPTURE(to_commit,
                closed,
                close_req.IsFinished(),
                close_req.IsError(),
                close_req.ErrorCode());
        REQUIRE(closed == to_commit);
        REQUIRE(close_req.IsFinished());
        REQUIRE_FALSE(close_req.IsError());
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
