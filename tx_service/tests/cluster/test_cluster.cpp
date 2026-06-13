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
#include "cluster/test_cluster.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <fcntl.h>     // O_WRONLY, O_CREAT, O_TRUNC
#include <signal.h>    // ::kill, SIGKILL
#include <spawn.h>     // posix_spawn, posix_spawn_file_actions_*
#include <sys/wait.h>  // ::waitpid
#include <unistd.h>    // ::readlink, STDOUT_FILENO, STDERR_FILENO

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "bthread/bthread.h"    // bthread_usleep
#include "cc_request.pb.h"      // CcRpcService_Stub, OnLeaderStartRequest, ...
#include "harness/port_util.h"  // BindEphemeralPort, ReserveTxPortWindow
#include "sharder.h"            // GET_CCNODE_RPC_PORT

// posix_spawn needs the process environment to inherit; declared by the C
// runtime, not in a public header.
extern char **environ;

namespace txservice
{
namespace test
{
namespace
{
// The cluster_config_version every node is brought up with (txnode_bringup.cpp
// uses a fixed `cluster_config_version = 2`). OnLeaderStart carries this same
// version so Sharder::UpdateInMemoryClusterConfig early-returns (version not
// advanced) and the empty cluster_config/node_configs we send do not clobber
// the topology each node already built from its own ng_configs.
constexpr uint64_t kClusterConfigVersion = 2;

// The leader term the driver assigns every NG (single, never-changing term in
// this foundation: nodes do not fail over).
constexpr int64_t kLeaderTerm = 1;

// Hard, build-mode-independent precondition check for cluster bring-up (mirrors
// the Phase 1 fixture's FailBringup). assert() would compile out under NDEBUG
// and silently proceed; throwing lets Catch2 report with context.
[[noreturn]] void FailCluster(const char *what)
{
    throw std::runtime_error(std::string("TestCluster failed: ") + what);
}
}  // namespace

TestCluster::TestCluster(const ClusterOptions &opts) : opts_(opts)
{
    if (opts_.ng_to_node.empty())
    {
        FailCluster("ClusterOptions::ng_to_node is empty");
    }

    // 1. Unique temp dir to hold every node's data dir and per-node log file.
    //    The dtor removes it. (Mirrors TestNode's temp-dir scheme.)
    base_dir_ = (std::filesystem::temp_directory_path() /
                 ("txcluster_" + std::to_string(::getpid()) + "_" +
                  std::to_string(reinterpret_cast<uintptr_t>(this))))
                    .string();
    std::error_code ec;
    std::filesystem::remove_all(base_dir_, ec);
    std::filesystem::create_directories(base_dir_, ec);
    if (ec)
    {
        FailCluster("create base temp dir");
    }

    // 2. Build the NodeProc list from the ng->node mapping. Reserve a 4-wide tx
    //    port window and an ephemeral workload port for each node, and assign a
    //    per-node data dir under base_dir_. Spawning + HM start happen in
    //    Start() (Task C2), so we only set up state here.
    nodes_.reserve(opts_.ng_to_node.size());
    for (const auto &[ng_id, node_id] : opts_.ng_to_node)
    {
        NodeProc node;
        node.ng_id = ng_id;
        node.node_id = node_id;
        node.tx_port = ReserveTxPortWindow();
        {
            // Reserve an ephemeral workload port (closed immediately; the child
            // re-binds it -- a TOCTOU race the same as the tx window, made
            // unlikely by SO-reuse-free ephemeral allocation).
            auto [fd, port] = BindEphemeralPort();
            ::close(fd);
            node.workload_port = port;
        }
        node.data_dir = (std::filesystem::path(base_dir_) /
                         ("node_" + std::to_string(node_id)))
                            .string();
        std::filesystem::create_directories(node.data_dir, ec);
        if (ec)
        {
            FailCluster("create node data dir");
        }
        nodes_.push_back(std::move(node));
    }

    // 3. Reserve an ephemeral port for the in-driver ScriptedHostManager. The
    //    HM is started in Start() (C2); reserve the port now so the topology /
    //    spawn flags can reference it.
    {
        auto [fd, port] = BindEphemeralPort();
        ::close(fd);
        hm_port_ = port;
    }
}

TestCluster::~TestCluster()
{
    // Robust even if Start() was never called or threw midway: KillNode is a
    // no-op for pid <= 0, hm_.Stop() is safe if Start was never called, and
    // remove_all swallows errors.
    for (NodeProc &node : nodes_)
    {
        KillNode(node);
    }
    hm_.Stop();
    std::error_code ec;
    // Keep the per-node logs for post-mortem when TXCLUSTER_KEEP_LOGS is set;
    // otherwise remove the temp dir (the common case). Useful for debugging a
    // failed bring-up, since the FailCluster messages point at base_dir_.
    if (const char *keep = ::getenv("TXCLUSTER_KEEP_LOGS"); keep && *keep)
    {
        return;
    }
    std::filesystem::remove_all(base_dir_, ec);
}

void TestCluster::Start()
{
    using namespace std::chrono_literals;

    // 1. Start the scripted host manager FIRST. A node's TxService::Start
    //    retries StartNode against the HM for ~10s, so the HM must be listening
    //    before any node is spawned or the bring-up RPC may exhaust its
    //    retries.
    if (!hm_.Start("127.0.0.1", hm_port_))
    {
        FailCluster("host manager start");
    }

    // 2. Spawn every node with the shared topology, then build its workload
    //    stub. brpc connects lazily; step 3 verifies connectivity with a real
    //    RPC.
    const std::string topology = BuildTopologyString();
    for (NodeProc &node : nodes_)
    {
        SpawnNode(node, topology);
        BuildClient(node);
    }

    // 3. Wait until each node's workload server answers NodeInfo -- i.e.
    //    TxService::Start returned (the node registered with the HM and its
    //    cc-node / workload servers are listening). Only then is OnLeaderStart
    //    safe to send.
    AwaitWorkloadServers(30s);

    // 4. Drive leadership for every NG: send CcRpcService.OnLeaderStart to the
    //    member node and record it on the HM so GetLeader resolves. This sets
    //    each node's *candidate* leader term for its native NG (the actual
    //    candidate->leader promotion is finished in step 6 via WaitReady, since
    //    skip_wal leaves no log service to do it).
    for (const auto &[ng_id, node_id] : opts_.ng_to_node)
    {
        const NodeProc *node = FindNode(node_id);
        if (node == nullptr)
        {
            FailCluster("ng_to_node references unknown node_id");
        }
        DriveLeader(ng_id, *node, 30s);
        hm_.SetLeader(ng_id, node_id);
    }

    // 5. Propagate every NG's leader into every node's leader cache via
    //    CcRpcService.NotifyNewLeaderStart, so LeaderNodeId(ng) resolves on
    //    each node and cross-NG cc requests route to the correct remote leader.
    //    A node's own NG is harmless to (re)notify.
    for (const NodeProc &target : nodes_)
    {
        for (const auto &[ng_id, leader_node_id] : opts_.ng_to_node)
        {
            NotifyLeader(target, ng_id, leader_node_id, 10s);
        }
    }

    // 6. Finish recovery on every node (WaitReady) and verify the cluster is
    //    usable end-to-end: an empty tx commits and a cross-NG read returns
    //    without an RPC error.
    AwaitClusterReady(30s);
}

void TestCluster::AwaitWorkloadServers(std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (NodeProc &node : nodes_)
    {
        bool up = false;
        while (std::chrono::steady_clock::now() < deadline)
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(1000);
            txnode_workload::NodeInfoReq req;
            req.set_ng_id(node.ng_id);
            txnode_workload::NodeInfoResp resp;
            node.stub->NodeInfo(&cntl, &req, &resp, nullptr);
            if (!cntl.Failed())
            {
                up = true;
                break;
            }
            bthread_usleep(100 * 1000);  // 100 ms
        }
        if (!up)
        {
            throw std::runtime_error(
                "TestCluster failed: node " + std::to_string(node.node_id) +
                " workload server never came up; see logs under " + base_dir_);
        }
    }
}

void TestCluster::DriveLeader(uint32_t ng_id,
                              const NodeProc &node,
                              std::chrono::milliseconds timeout)
{
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 5000;
    brpc::Channel channel;
    const std::string addr =
        "127.0.0.1:" + std::to_string(GET_CCNODE_RPC_PORT(node.tx_port));
    if (channel.Init(addr.c_str(), &options) != 0)
    {
        FailCluster("brpc::Channel::Init to cc-node service");
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        ::txservice::remote::CcRpcService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(5000);
        ::txservice::remote::OnLeaderStartRequest req;
        req.set_node_group_id(ng_id);
        req.set_node_group_term(kLeaderTerm);
        // Empty cluster_config / node_configs: the node already holds the full
        // topology and config_version matches what it was started with, so the
        // handler's UpdateInMemoryClusterConfig is a no-op (see header note).
        req.set_config_version(kClusterConfigVersion);
        ::txservice::remote::OnLeaderStartResponse resp;
        stub.OnLeaderStart(&cntl, &req, &resp, nullptr);
        if (!cntl.Failed() && !resp.error())
        {
            return;
        }
        bthread_usleep(100 * 1000);  // 100 ms
    }
    throw std::runtime_error(
        "TestCluster failed: OnLeaderStart never succeeded for ng " +
        std::to_string(ng_id) + " on node " + std::to_string(node.node_id) +
        "; see logs under " + base_dir_);
}

void TestCluster::NotifyLeader(const NodeProc &target,
                               uint32_t ng_id,
                               uint32_t leader_node_id,
                               std::chrono::milliseconds timeout)
{
    const NodeProc *leader = nullptr;
    for (const NodeProc &n : nodes_)
    {
        if (n.node_id == leader_node_id)
        {
            leader = &n;
            break;
        }
    }
    if (leader == nullptr)
    {
        FailCluster("NotifyLeader: unknown leader node_id");
    }

    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 2000;
    brpc::Channel channel;
    const std::string addr =
        "127.0.0.1:" + std::to_string(GET_CCNODE_RPC_PORT(target.tx_port));
    if (channel.Init(addr.c_str(), &options) != 0)
    {
        FailCluster("brpc::Channel::Init to cc-node service (notify)");
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        ::txservice::remote::CcRpcService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(2000);
        ::txservice::remote::NotifyNewLeaderStartRequest req;
        req.set_ng_id(ng_id);
        req.set_node_id(leader_node_id);
        req.set_term(kLeaderTerm);
        ::txservice::remote::NotifyNewLeaderStartResponse resp;
        stub.NotifyNewLeaderStart(&cntl, &req, &resp, nullptr);
        if (!cntl.Failed() && !resp.error())
        {
            return;
        }
        bthread_usleep(100 * 1000);  // 100 ms
    }
    throw std::runtime_error(
        "TestCluster failed: NotifyNewLeaderStart never succeeded (ng " +
        std::to_string(ng_id) + " -> node " + std::to_string(leader_node_id) +
        ") on node " + std::to_string(target.node_id) + "; see logs under " +
        base_dir_);
}

void TestCluster::AwaitClusterReady(std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    // 6a. WaitReady on every node, CONCURRENTLY. WaitReady finishes the node's
    //     native-NG recovery (LeaderTerm > 0) and then drives WaitClusterReady,
    //     which for each REMOTE NG sends a RecoverStateCheck over the cc-stream
    //     and blocks until that NG's leader answers LeaderTerm > 0. Because
    //     each node's WaitClusterReady waits on the OTHER node's native leader
    //     term, the calls MUST overlap: if we drove them one at a time, node
    //     0's WaitClusterReady would block on node 1's leader term, which node
    //     1's (not-yet-issued) WaitReady has not set -- a bring-up deadlock.
    //     Issuing them in parallel lets every node finish its own native
    //     recovery first, after which all the cross-NG handshakes resolve.
    //     Mirrors production, where every node runs WaitClusterReady
    //     concurrently.
    std::vector<std::thread> ready_threads;
    std::vector<std::string> ready_errors(nodes_.size());
    ready_threads.reserve(nodes_.size());
    for (size_t i = 0; i < nodes_.size(); ++i)
    {
        ready_threads.emplace_back(
            [this, i, deadline, &ready_errors]()
            {
                const NodeProc &node = nodes_[i];
                while (std::chrono::steady_clock::now() < deadline)
                {
                    brpc::Controller cntl;
                    // WaitClusterReady can take a few 1s rounds; allow ample
                    // time per RPC.
                    cntl.set_timeout_ms(15000);
                    txnode_workload::WaitReadyReq req;
                    txnode_workload::WaitReadyResp resp;
                    node.stub->WaitReady(&cntl, &req, &resp, nullptr);
                    if (!cntl.Failed() && resp.ready())
                    {
                        return;
                    }
                    // A non-failed reply with ready=false means OnLeaderStart
                    // was never driven for this node (a driver bug); retrying
                    // will not help, but the deadline below bounds it anyway.
                    bthread_usleep(200 * 1000);  // 200 ms
                }
                ready_errors[i] = "node " + std::to_string(node.node_id) +
                                  " never reached cluster-ready";
            });
    }
    for (std::thread &t : ready_threads)
    {
        t.join();
    }
    for (const std::string &err : ready_errors)
    {
        if (!err.empty())
        {
            throw std::runtime_error("TestCluster failed: " + err +
                                     "; see logs under " + base_dir_);
        }
    }

    // 6b. Native-NG leader-term check on every node: NodeInfo for the node's
    // own
    //     NG must report leader_term > 0.
    for (const NodeProc &node : nodes_)
    {
        brpc::Controller cntl;
        cntl.set_timeout_ms(2000);
        txnode_workload::NodeInfoReq req;
        req.set_ng_id(node.ng_id);
        txnode_workload::NodeInfoResp resp;
        node.stub->NodeInfo(&cntl, &req, &resp, nullptr);
        if (cntl.Failed() || resp.leader_term() <= 0)
        {
            throw std::runtime_error(
                "TestCluster failed: node " + std::to_string(node.node_id) +
                " native ng " + std::to_string(node.ng_id) +
                " not leader after WaitReady; see logs under " + base_dir_);
        }
    }

    // 6c. Per-node liveness check: an empty tx commits on every node, proving
    //     the node is a working leader that can run the tx state machine end to
    //     end. We deliberately do NOT issue a cross-NG read here: a cold
    //     cross-NG hash-partition read on a freshly-promoted skip_kv leader can
    //     trip an engine assertion in the bucket-meta lock path (see the
    //     ClusterCrossNg-Test notes), so the readiness gate stays on the
    //     reliable empty-tx path and the cross-NG read itself is exercised by
    //     the test body, not the bring-up.
    for (const NodeProc &node : nodes_)
    {
        uint64_t handle = 0;
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(5000);
            txnode_workload::BeginTxReq req;
            req.set_isolation(1);  // Snapshot
            req.set_protocol(1);   // OccRead
            txnode_workload::BeginTxResp resp;
            node.stub->BeginTx(&cntl, &req, &resp, nullptr);
            if (cntl.Failed() || resp.error())
            {
                throw std::runtime_error(
                    "TestCluster failed: node " + std::to_string(node.node_id) +
                    " BeginTx failed in readiness check; see logs under " +
                    base_dir_);
            }
            handle = resp.tx_handle();
        }
        {
            brpc::Controller cntl;
            cntl.set_timeout_ms(5000);
            txnode_workload::CommitReq req;
            req.set_tx_handle(handle);
            txnode_workload::CommitResp resp;
            node.stub->Commit(&cntl, &req, &resp, nullptr);
            if (cntl.Failed() || !resp.committed())
            {
                throw std::runtime_error(
                    "TestCluster failed: node " + std::to_string(node.node_id) +
                    " Commit failed in readiness check; see logs under " +
                    base_dir_);
            }
        }
    }
}

void TestCluster::Kill(uint32_t node_id)
{
    NodeProc *node = FindNode(node_id);
    if (node == nullptr)
    {
        FailCluster("Kill: unknown node_id");
    }
    KillNode(*node);
}

txnode_workload::WorkloadService_Stub &TestCluster::Client(uint32_t node_id)
{
    NodeProc *node = FindNode(node_id);
    if (node == nullptr)
    {
        FailCluster("Client: unknown node_id");
    }
    if (!node->stub)
    {
        FailCluster("Client: node not started (no stub)");
    }
    return *node->stub;
}

std::string TestCluster::LocateTxnodeBinary() const
{
    // The test binary and `txnode` build into the same tests/ dir; derive the
    // path from this process's own executable.
    char buf[4096];
    ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n <= 0)
    {
        FailCluster("readlink /proc/self/exe");
    }
    buf[n] = '\0';
    std::filesystem::path self(buf);
    std::filesystem::path txnode = self.parent_path() / "txnode";
    if (!std::filesystem::exists(txnode))
    {
        FailCluster("txnode binary not found next to test binary");
    }
    return txnode.string();
}

std::string TestCluster::BuildTopologyString() const
{
    // ';'-separated "ng:node@127.0.0.1:tx_port" members for every node. This
    // round-trips ParseTopology() in txnode_main.cpp; the port is each node's
    // reserved tx base port (cc-node = base+1, log-replay = base+3).
    std::string topology;
    for (const NodeProc &node : nodes_)
    {
        if (!topology.empty())
        {
            topology += ';';
        }
        topology += std::to_string(node.ng_id);
        topology += ':';
        topology += std::to_string(node.node_id);
        topology += "@127.0.0.1:";
        topology += std::to_string(node.tx_port);
    }
    return topology;
}

void TestCluster::SpawnNode(NodeProc &node, const std::string &topology)
{
    const std::string bin = LocateTxnodeBinary();

    // Build the flag strings (owned for the duration of posix_spawn; argv holds
    // pointers into them).
    const std::string arg_node_id = "--node_id=" + std::to_string(node.node_id);
    const std::string arg_ng_id = "--ng_id=" + std::to_string(node.ng_id);
    const std::string arg_core_num =
        "--core_num=" + std::to_string(opts_.core_num);
    const std::string arg_topology = "--topology=" + topology;
    const std::string arg_hm_ip = "--hm_ip=127.0.0.1";
    const std::string arg_hm_port = "--hm_port=" + std::to_string(hm_port_);
    const std::string arg_workload_port =
        "--workload_port=" + std::to_string(node.workload_port);
    const std::string arg_data_dir = "--data_dir=" + node.data_dir;
    // Direct glog's own file output into the node dir as well, alongside the
    // stdout/stderr capture below.
    const std::string arg_log_dir = "--log_dir=" + node.data_dir;

    // argv[0] is the binary path; the array MUST be null-terminated.
    char *argv[] = {const_cast<char *>(bin.c_str()),
                    const_cast<char *>(arg_node_id.c_str()),
                    const_cast<char *>(arg_ng_id.c_str()),
                    const_cast<char *>(arg_core_num.c_str()),
                    const_cast<char *>(arg_topology.c_str()),
                    const_cast<char *>(arg_hm_ip.c_str()),
                    const_cast<char *>(arg_hm_port.c_str()),
                    const_cast<char *>(arg_workload_port.c_str()),
                    const_cast<char *>(arg_data_dir.c_str()),
                    const_cast<char *>(arg_log_dir.c_str()),
                    nullptr};

    // Redirect the child's stdout (fd 1) and stderr (fd 2) to a per-node log
    // file so a failed bring-up leaves a diagnosable trail.
    const std::string log_path =
        (std::filesystem::path(base_dir_) /
         ("node_" + std::to_string(node.node_id) + ".log"))
            .string();
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    // Open the log file as fd; then dup2 it onto 1 and 2 and close the
    // original.
    posix_spawn_file_actions_addopen(&actions,
                                     STDOUT_FILENO,
                                     log_path.c_str(),
                                     O_WRONLY | O_CREAT | O_TRUNC,
                                     0644);
    posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);

    pid_t pid = -1;
    int ret = posix_spawn(&pid, bin.c_str(), &actions, nullptr, argv, environ);
    posix_spawn_file_actions_destroy(&actions);
    if (ret != 0)
    {
        FailCluster("posix_spawn txnode");
    }
    node.pid = pid;
}

void TestCluster::BuildClient(NodeProc &node)
{
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 5000;
    options.max_retry = 3;

    auto channel = std::make_unique<brpc::Channel>();
    const std::string addr = "127.0.0.1:" + std::to_string(node.workload_port);
    if (channel->Init(addr.c_str(), &options) != 0)
    {
        FailCluster("brpc::Channel::Init to workload service");
    }
    // The stub does not own the channel, so keep the channel alive in NodeProc.
    node.stub =
        std::make_unique<txnode_workload::WorkloadService_Stub>(channel.get());
    node.channel = std::move(channel);
}

void TestCluster::KillNode(NodeProc &node)
{
    if (node.pid > 0)
    {
        ::kill(node.pid, SIGKILL);
        int status = 0;
        ::waitpid(node.pid, &status, 0);
        node.pid = -1;
    }
}

NodeProc *TestCluster::FindNode(uint32_t node_id)
{
    for (NodeProc &node : nodes_)
    {
        if (node.node_id == node_id)
        {
            return &node;
        }
    }
    return nullptr;
}
}  // namespace test
}  // namespace txservice
