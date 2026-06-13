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

// Phase 2, Task D1: the first end-to-end multi-process cluster test. It spins
// up a real 2-node cluster (node0 -> ng0, node1 -> ng1) out of process via
// TestCluster and exercises cross-node-group WRITE coordination (cc-stream
// established at bring-up, leaders driven by the test driver, leader caches
// propagated via NotifyNewLeaderStart).

// brpc / bvar headers (pulled in by test_cluster.h and the generated workload
// stub) MUST be fully included and their templates instantiated BEFORE Catch2's
// decomposer operators become visible: bvar's Reducer::get_value contains an
// `||` expression that, if Catch2's global operator||(ExprLhs&&, RhsT&&) is in
// scope, gets mis-selected and trips a static_assert ("operator|| is not
// supported inside assertions"). So include the engine/brpc headers first and
// Catch2 last.
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <cstdint>

#include "cluster/test_cluster.h"
#include "txnode_workload.pb.h"

// Catch2 last (see note above).
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

using txservice::test::ClusterOptions;
using txservice::test::TestCluster;
using WorkloadStub = txnode_workload::WorkloadService_Stub;

namespace
{
// --- Workload-RPC helper wrappers over a WorkloadService_Stub. Each opens a
// fresh brpc::Controller (a Controller is single-use) and asserts the RPC did
// not fail at the transport level before inspecting the reply. ---

// Query the leader term `stub`'s node reports for `ng_id`. A driven leader
// reports leader_term > 0; a follower (or a node that never won the term)
// reports 0. The RPC itself must not fail at the transport level.
int64_t LeaderTerm(WorkloadStub &stub, uint32_t ng_id)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    txnode_workload::NodeInfoReq req;
    req.set_ng_id(ng_id);
    txnode_workload::NodeInfoResp resp;
    stub.NodeInfo(&cntl, &req, &resp, nullptr);
    REQUIRE_FALSE(cntl.Failed());
    return resp.leader_term();
}

// The node id `stub`'s node reports for itself. Used to confirm the two stubs
// reach two DISTINCT nodes -- otherwise a topology/port misconfiguration could
// silently degenerate the "cross-NG" test into two transactions on one node.
uint32_t NodeId(WorkloadStub &stub)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    txnode_workload::NodeInfoReq req;
    req.set_ng_id(0);
    txnode_workload::NodeInfoResp resp;
    stub.NodeInfo(&cntl, &req, &resp, nullptr);
    REQUIRE_FALSE(cntl.Failed());
    return resp.node_id();
}

// Begin a transaction with the Phase-1-proven defaults: isolation=1 (Snapshot),
// protocol=1 (OccRead). Returns the tx handle.
uint64_t Begin(WorkloadStub &stub)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    txnode_workload::BeginTxReq req;
    req.set_isolation(1);  // Snapshot
    req.set_protocol(1);   // OccRead
    txnode_workload::BeginTxResp resp;
    stub.BeginTx(&cntl, &req, &resp, nullptr);
    REQUIRE_FALSE(cntl.Failed());
    REQUIRE_FALSE(resp.error());
    return resp.tx_handle();
}

bool Upsert(WorkloadStub &stub, uint64_t handle, int key, int value)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    txnode_workload::UpsertReq req;
    req.set_tx_handle(handle);
    req.set_key(key);
    req.set_value(value);
    req.set_is_delete(false);
    txnode_workload::UpsertResp resp;
    stub.Upsert(&cntl, &req, &resp, nullptr);
    REQUIRE_FALSE(cntl.Failed());
    return resp.ok();
}

bool Commit(WorkloadStub &stub, uint64_t handle)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    txnode_workload::CommitReq req;
    req.set_tx_handle(handle);
    txnode_workload::CommitResp resp;
    stub.Commit(&cntl, &req, &resp, nullptr);
    REQUIRE_FALSE(cntl.Failed());
    return resp.committed();
}
}  // namespace

// What this test proves (and what it deliberately does NOT):
//
//   PROVES (green, meaningful):
//     * The multi-process cluster harness works end to end: TestCluster spawns
//       two real `txnode` processes, the scripted host manager brings them up,
//       leadership is scripted (OnLeaderStart + NotifyNewLeaderStart), leader
//       caches propagate, and the cc-stream between the two node groups is
//       established.
//     * Both NGs are led: node 0 reports leader_term > 0 for ng0, node 1
//       reports leader_term > 0 for ng1.
//     * Cross-NG WRITE commit works from EITHER node. Each node upserts 20 keys
//       that hash across BOTH node groups, so a successful commit exercises the
//       full cross-NG write path: remote write-lock acquisition on the peer
//       NG's leader plus 2PC coordination (the coordinating node prepares and
//       commits against the remote NG). 20 keys spanning both buckets is enough
//       to guarantee at least one remote write per transaction.
//
//   INTENTIONALLY NOT ASSERTED (engine follow-up, not a harness gap):
//     * Cross-NG READ-BACK. Reading a key whose bucket is owned by the peer NG
//       takes the bucket-meta lock path (ReadOperation -> lock_bucket_op_, a
//       ReadLocal on the RangeBucket ccm). On a freshly-promoted skip_kv
//       leader, InitRangeBuckets seeds bucket *info* (enough for write routing)
//       but does NOT seed the RangeBucket CcMap entries that a read pins -- see
//       the `// TODO: HARDCORE SEED` note in cc_node.cpp. So the bucket-meta
//       lock never resolves (there is no KV to fetch it from under skip_kv) and
//       the read hangs / trips an engine assertion. This is a tracked engine
//       gap in the cross-NG hash-partition read path under skip_kv, NOT a
//       defect in this harness, and fixing it requires core tx_execution /
//       InitRangeBuckets changes that are out of scope here. Do NOT "fix" this
//       test by reading keys back, collapsing to a single NG, or otherwise
//       weakening the cross-NG nature of the writes.
TEST_CASE("two-node cluster: bring-up + cross-NG write commit", "[cluster]")
{
    // Default options: 2 NGs, node0 -> ng0, node1 -> ng1, 2 cores each.
    TestCluster cluster(ClusterOptions{});
    cluster.Start();

    WorkloadStub &c0 = cluster.Client(0);
    WorkloadStub &c1 = cluster.Client(1);

    // The two stubs reach two distinct nodes (guards against a topology/port
    // mix-up that would make this a single-node test in disguise).
    REQUIRE(NodeId(c0) == 0);
    REQUIRE(NodeId(c1) == 1);

    // Both node groups are led after bring-up: node 0 leads ng0, node 1 leads
    // ng1, each with a positive leader term. (NodeInfo reports the LOCAL leader
    // term, so this only goes positive on each node's OWN ng; cross-NG leader
    // resolution is proven instead by the cross-NG commits below succeeding.)
    REQUIRE(LeaderTerm(c0, /*ng_id=*/0) > 0);
    REQUIRE(LeaderTerm(c1, /*ng_id=*/1) > 0);

    // Cross-NG write from node 0: 20 keys (k=1..20, value k*10) hashing across
    // BOTH NGs. Upsert only buffers into the tx write set locally (it cannot
    // fail for a resolvable table), so the real cross-NG work is at Commit: it
    // succeeds only if node 0 acquires write locks on ng1's leader (node 1) for
    // the remote keys and 2PC-coordinates the commit across both NGs.
    uint64_t t0 = Begin(c0);
    for (int k = 1; k <= 20; ++k)
    {
        REQUIRE(Upsert(c0, t0, k, k * 10));
    }
    REQUIRE(Commit(c0, t0));

    // Cross-NG write from node 1: a DIFFERENT span of 20 keys (k=101..120).
    // With node 1 as coordinator these keys hash predominantly to ng0, so the
    // commit exercises the remote write-lock + 2PC path against ng0's leader
    // (node 0) -- the mirror of the node-0 case, with the coordinator/remote
    // roles swapped.
    uint64_t t1 = Begin(c1);
    for (int k = 101; k <= 120; ++k)
    {
        REQUIRE(Upsert(c1, t1, k, k * 10));
    }
    REQUIRE(Commit(c1, t1));
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
