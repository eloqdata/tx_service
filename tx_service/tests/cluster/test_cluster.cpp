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

#include <fcntl.h>     // O_WRONLY, O_CREAT, O_TRUNC
#include <signal.h>    // ::kill, SIGKILL
#include <spawn.h>     // posix_spawn, posix_spawn_file_actions_*
#include <sys/wait.h>  // ::waitpid
#include <unistd.h>    // ::readlink, STDOUT_FILENO, STDERR_FILENO

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "harness/port_util.h"  // BindEphemeralPort, ReserveTxPortWindow

// posix_spawn needs the process environment to inherit; declared by the C
// runtime, not in a public header.
extern char **environ;

namespace txservice
{
namespace test
{
namespace
{
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
    std::filesystem::remove_all(base_dir_, ec);
}

void TestCluster::Start()
{
    // Task C2: start the HM, spawn all nodes (SpawnNode), build per-node stubs
    // (BuildClient), then drive leadership (ScriptedHostManager::SetLeader +
    // each node's CcRpcService.OnLeaderStart) and wait until every node reports
    // cluster_ready. The C1 plumbing (port reservation, topology, spawn, stub
    // construction) is in place; only the orchestration is deferred.
    throw std::runtime_error("TestCluster::Start not implemented (Task C2)");
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
