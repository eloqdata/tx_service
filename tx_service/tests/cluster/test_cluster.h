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
#pragma once

#include <brpc/channel.h>
#include <sys/types.h>  // pid_t

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "scripted_host_manager.h"
#include "txnode_workload.pb.h"

namespace txservice
{
namespace test
{
// Configuration for a TestCluster. The foundation maps each node-group to a
// single member node (1 node/NG), since the engine's process-global singletons
// (Sharder, etc.) force one node per OS process.
struct ClusterOptions
{
    // ng_id -> the single member node_id leading it (foundation: 1 node/NG).
    std::vector<std::pair<uint32_t, uint32_t>> ng_to_node{{0, 0}, {1, 1}};
    uint32_t core_num{2};
};

// One spawned `txnode` OS process and the driver-side state for it: the
// reserved tx-port window base, its workload-service port, its pid, its data
// dir, and the brpc channel/stub the driver uses to send workload RPCs.
struct NodeProc
{
    uint32_t node_id{0};
    uint32_t ng_id{0};
    uint16_t tx_port{0};  // base of the reserved +0..+3 window
    uint16_t workload_port{0};
    pid_t pid{-1};
    std::string data_dir;
    std::unique_ptr<brpc::Channel> channel;
    std::unique_ptr<txnode_workload::WorkloadService_Stub> stub;
};

// Out-of-process cluster test driver. Runs IN the Catch test process: it
// reserves ports, spawns N `txnode` subprocesses (one node each), runs an
// in-driver ScriptedHostManager, drives leadership, and exposes a per-node
// workload-RPC stub. Each `txnode` brings up one real TxService node and serves
// the WorkloadService so the driver can run transactions against it.
//
// Lifecycle (Task split):
//   - ctor (C1): set up base_dir_, options, reserve a tx-port window + a
//     workload port + a data dir for each node. Does NOT spawn or start the HM.
//   - Start() (C2): start the HM, spawn all nodes, build stubs, drive
//     leadership, and wait until the cluster is ready. (Stubbed in C1.)
//   - dtor: SIGKILL any live node, stop the HM, remove the temp dir.
class TestCluster
{
public:
    explicit TestCluster(const ClusterOptions &opts = {});
    ~TestCluster();  // SIGKILL live nodes, stop HM, rm temp dirs
    TestCluster(const TestCluster &) = delete;
    TestCluster &operator=(const TestCluster &) = delete;

    // Start the HM, spawn all nodes, drive leadership and wait until ready.
    // Implemented in Task C2; throws "not implemented" in C1.
    void Start();

    // SIGKILL one node and reap it (no zombie). No-op if already dead.
    void Kill(uint32_t node_id);

    // Workload-RPC stub for a node. Throws if the node is unknown or its stub
    // has not been built yet (i.e. before Start()).
    txnode_workload::WorkloadService_Stub &Client(uint32_t node_id);

private:
    // Resolve the `txnode` binary path from /proc/self/exe (the test binary and
    // `txnode` build into the same tests/ dir). Throws if it is missing.
    std::string LocateTxnodeBinary() const;

    // Build the engine topology string the driver and txnode agree on:
    // ';'-separated "ng:node@127.0.0.1:tx_port" members covering every node.
    std::string BuildTopologyString() const;

    // posix_spawn one `txnode` for `node`, redirecting its stdout+stderr to
    // <base_dir_>/node_<id>.log, and record its pid. C2 calls this from
    // Start().
    void SpawnNode(NodeProc &node, const std::string &topology);

    // Build the brpc channel + WorkloadService stub to 127.0.0.1:workload_port
    // for `node` (brpc connects lazily; connectivity is verified by an actual
    // RPC in C2). C2 calls this from Start().
    void BuildClient(NodeProc &node);

    // SIGKILL + waitpid one NodeProc; marks pid = -1. Safe if already reaped.
    void KillNode(NodeProc &node);

    // Find the NodeProc for node_id, or nullptr.
    NodeProc *FindNode(uint32_t node_id);

    ClusterOptions opts_;
    ScriptedHostManager hm_;
    uint16_t hm_port_{0};
    std::string base_dir_;
    std::vector<NodeProc> nodes_;
};
}  // namespace test
}  // namespace txservice
