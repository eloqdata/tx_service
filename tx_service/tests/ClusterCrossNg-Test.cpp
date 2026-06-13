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
// TestCluster, writes a span of keys through node 0 -- some owned by ng1, hence
// REMOTE writes -- and reads them all back through node 1 -- some owned by ng0,
// hence REMOTE reads. A pass proves cross-node-group request routing works
// (cc-stream established at bring-up, leaders driven by the test driver, leader
// caches propagated via NotifyNewLeaderStart).

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

// Reads `key` into `value_out`. Returns true iff the key is present (Normal).
// A transport failure or engine-level error fails the test outright.
bool Read(WorkloadStub &stub, uint64_t handle, int key, int &value_out)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    txnode_workload::ReadReq req;
    req.set_tx_handle(handle);
    req.set_key(key);
    txnode_workload::ReadResp resp;
    stub.Read(&cntl, &req, &resp, nullptr);
    REQUIRE_FALSE(cntl.Failed());
    REQUIRE_FALSE(resp.error());
    if (resp.found())
    {
        value_out = resp.value();
    }
    return resp.found();
}
}  // namespace

// STATUS (currently failing -- BLOCKED on an engine issue, NOT the harness):
// the cluster bring-up (TestCluster::Start) works end to end -- both nodes come
// up, leaders are driven, leader caches propagate, an empty tx commits on every
// node, and the cross-NG WRITES below commit successfully. What does NOT work
// yet is the cross-NG READ: when node 1 serves a read of a key whose bucket is
// owned by ng0, the engine takes the bucket-meta lock path (ReadOperation ->
// lock_bucket_op_, a ReadLocal on the RangeBucket ccm) which, on a
// freshly-promoted skip_kv leader, does not complete synchronously. This
// surfaces as the read RPC failing (lock_bucket_op_ never finishes -- there is
// no KV to fetch the bucket meta from under skip_kv) or, in a rarer
// interleaving, an engine assertion firing on node 1:
//
//   tx_operation.cpp: ReadOperation::Forward:
//     assert(lock_range_bucket_result_->IsFinished())
//
// (observed with is_error=0, err_code=0 -- the bucket-lock result is simply not
// ready). This is a pre-existing engine limitation in the cross-NG
// hash-partition read path under the skip_kv/skip_wal test bring-up, which this
// is the first test to exercise; it is not a defect in the cluster harness. The
// test is intentionally left active and genuine (it exercises real cross-NG
// routing -- write via node 0, read via node 1, 20 keys spanning both NGs); it
// will pass once the engine resolves the bucket-meta lock on a cold skip_kv
// leader. Do NOT "fix" it by reading only local keys or collapsing both nodes
// into one NG -- that would defeat the point of the test.
TEST_CASE("two-node cluster: write on one node is visible from the other",
          "[cluster]")
{
    // Default options: 2 NGs, node0 -> ng0, node1 -> ng1, 2 cores each.
    TestCluster cluster(ClusterOptions{});
    cluster.Start();

    WorkloadStub &c0 = cluster.Client(0);
    WorkloadStub &c1 = cluster.Client(1);

    // Write 20 keys via node 0. Keys hash across both node groups, so some of
    // these are REMOTE writes routed from node 0 to ng1's leader (node 1).
    uint64_t t0 = Begin(c0);
    for (int k = 1; k <= 20; ++k)
    {
        REQUIRE(Upsert(c0, t0, k, k * 10));
    }
    REQUIRE(Commit(c0, t0));

    // Read them all back via node 1. Keys owned by ng0 are REMOTE reads routed
    // from node 1 to ng0's leader (node 0). Every key must be present with its
    // written value.
    uint64_t t1 = Begin(c1);
    for (int k = 1; k <= 20; ++k)
    {
        int v = 0;
        REQUIRE(Read(c1, t1, k, v));
        REQUIRE(v == k * 10);
    }
    REQUIRE(Commit(c1, t1));
}

int main(int argc, char **argv)
{
    return Catch::Session().run(argc, argv);
}
