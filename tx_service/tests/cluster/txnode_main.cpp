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

// Standalone tx-service node binary used by the Phase 2 cluster test driver
// (TestCluster, Task C1). Each `txnode` process brings up one real TxService
// node via txservice::test::TxNode and exposes a brpc WorkloadService so the
// out-of-process driver can begin/commit/abort transactions and issue
// read/upsert/delete against the prebuilt test table on this node. The node
// registers with the driver's scripted host manager (--hm_ip/--hm_port) and
// does NOT self-promote -- the driver drives leadership.

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <signal.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "bthread/bthread.h"
#include "cc_protocol.h"  // CcProtocol, IsolationLevel
#include "cluster/txnode_bringup.h"
#include "sharder.h"  // Sharder
#include "tx_execution.h"
#include "tx_key.h"     // TxKey, CompositeKey
#include "tx_record.h"  // CompositeRecord
#include "tx_request.h"
#include "tx_service.h"
#include "txnode_workload.pb.h"
#include "type.h"  // TableName, RecordStatus, OperationType

DEFINE_uint32(node_id, 0, "This node's id.");
DEFINE_uint32(ng_id, 0, "This node's native node-group id.");
DEFINE_uint32(core_num, 2, "Number of cores (TxProcessors) for this node.");
// Topology string format (kept consistent with the driver, Task C1):
//   semicolon-separated members, each "ng:node@host:port".
//   e.g. for two single-member node-groups:
//     0:0@127.0.0.1:31000;1:1@127.0.0.1:31010
// `port` is the tx base port; cc-stream binds it, cc-node binds base+1,
// log-replay binds base+3 (the driver reserves a 4-wide window per node).
DEFINE_string(topology,
              "",
              "Cluster topology: 'ng:node@host:port' members, ';'-separated.");
// hm_ip / hm_port are already defined by data_substrate (core/src/
// tx_service_init.cpp); reuse those gflags rather than redefining them (a
// second DEFINE_ is a multiple-definition link error). The driver still passes
// --hm_ip/--hm_port on the command line and gflags routes them to these.
DECLARE_string(hm_ip);
DECLARE_int32(hm_port);
DEFINE_int32(workload_port, 0, "Port for this node's WorkloadService.");
DEFINE_string(data_dir, "", "Unique data directory for this node.");

namespace
{
using txservice::CcProtocol;
using txservice::CompositeKey;
using txservice::CompositeRecord;
using txservice::IsolationLevel;
using txservice::OperationType;
using txservice::RecordStatus;
using txservice::Sharder;
using txservice::TableName;
using txservice::TransactionExecution;
using txservice::TxKey;
using txservice::TxService;
using txservice::test::TxNode;
using txservice::test::TxNodeConfig;

// Build a TxKey wrapping a CompositeKey<int> / a CompositeRecord<int> holding a
// single int field. Duplicated from the harness (test_node.cpp) so this binary
// links only cluster_harness, avoiding a test_harness/cluster_harness link
// cycle. Must stay in lock-step with the prebuilt CompositeKey<int> /
// CompositeRecord<int> table.
TxKey MakeKey(int k)
{
    return TxKey(std::make_unique<CompositeKey<int>>(k));
}

std::unique_ptr<CompositeRecord<int>> MakeRec(int v)
{
    return std::make_unique<CompositeRecord<int>>(v);
}

// Parses the --topology string into a TxNodeConfig::ng_members map.
// Round-trips the "ng:node@host:port;..." format the driver writes. Throws on
// malformed input so main() exits non-zero with a clear message rather than
// bringing up a half-formed topology.
std::map<uint32_t, std::vector<std::tuple<uint32_t, std::string, uint16_t>>>
ParseTopology(const std::string &topology)
{
    std::map<uint32_t, std::vector<std::tuple<uint32_t, std::string, uint16_t>>>
        ng_members;
    if (topology.empty())
    {
        throw std::runtime_error("--topology is empty");
    }

    size_t start = 0;
    while (start < topology.size())
    {
        size_t semi = topology.find(';', start);
        std::string member = (semi == std::string::npos)
                                 ? topology.substr(start)
                                 : topology.substr(start, semi - start);
        start = (semi == std::string::npos) ? topology.size() : semi + 1;
        if (member.empty())
        {
            // Tolerate a trailing ';'.
            continue;
        }

        // member == "ng:node@host:port"
        size_t colon1 = member.find(':');
        size_t at = member.find('@');
        if (colon1 == std::string::npos || at == std::string::npos ||
            at < colon1)
        {
            throw std::runtime_error("malformed topology member: " + member);
        }
        size_t colon2 = member.rfind(':');
        if (colon2 == std::string::npos || colon2 <= at)
        {
            throw std::runtime_error("malformed topology member: " + member);
        }

        std::string ng_str = member.substr(0, colon1);
        std::string node_str = member.substr(colon1 + 1, at - colon1 - 1);
        std::string host = member.substr(at + 1, colon2 - at - 1);
        std::string port_str = member.substr(colon2 + 1);
        if (ng_str.empty() || node_str.empty() || host.empty() ||
            port_str.empty())
        {
            throw std::runtime_error("malformed topology member: " + member);
        }

        try
        {
            uint32_t ng_id = static_cast<uint32_t>(std::stoul(ng_str));
            uint32_t node_id = static_cast<uint32_t>(std::stoul(node_str));
            uint16_t port = static_cast<uint16_t>(std::stoul(port_str));
            ng_members[ng_id].emplace_back(node_id, host, port);
        }
        catch (const std::exception &)
        {
            throw std::runtime_error("malformed topology member: " + member);
        }
    }

    if (ng_members.empty())
    {
        throw std::runtime_error("--topology parsed to no members");
    }
    return ng_members;
}

// brpc WorkloadService over a single in-process TxService. Each RPC drives the
// same TxRequest / TransactionExecution flow as the harness's TxHandle
// (test_node.cpp), so the driver gets identical semantics out of process. Tx
// handles are integer tokens mapping to live TransactionExecution* owned by the
// engine pool; the map is guarded by mutex_ because brpc dispatches RPCs on
// multiple worker threads.
class WorkloadServiceImpl : public txnode_workload::WorkloadService
{
public:
    WorkloadServiceImpl(TxService *svc,
                        const TableName *table,
                        uint64_t schema_version,
                        uint32_t native_ng_id,
                        std::atomic<bool> *shutdown_flag)
        : svc_(svc),
          table_(table),
          schema_version_(schema_version),
          native_ng_id_(native_ng_id),
          shutdown_flag_(shutdown_flag)
    {
    }

    void BeginTx(::google::protobuf::RpcController *,
                 const txnode_workload::BeginTxReq *req,
                 txnode_workload::BeginTxResp *resp,
                 ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        TransactionExecution *txm = svc_->NewTx();
        txservice::InitTxRequest init(
            static_cast<IsolationLevel>(req->isolation()),
            static_cast<CcProtocol>(req->protocol()));
        init.Reset();
        txm->Execute(&init);
        init.Wait();
        if (init.IsError())
        {
            resp->set_error(true);
            return;
        }
        uint64_t handle;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            handle = ++next_handle_;
            txns_[handle] = txm;
        }
        resp->set_tx_handle(handle);
        resp->set_error(false);
    }

    void Upsert(::google::protobuf::RpcController *,
                const txnode_workload::UpsertReq *req,
                txnode_workload::UpsertResp *resp,
                ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        TransactionExecution *txm = Lookup(req->tx_handle());
        if (txm == nullptr)
        {
            resp->set_ok(false);
            return;
        }
        // Mirror TxHandle::Upsert / TxHandle::Delete: Delete carries a null rec
        // with OperationType::Delete; Upsert writes Rec(value).
        txservice::UpsertTxRequest write(
            table_,
            MakeKey(req->key()),
            req->is_delete() ? nullptr : MakeRec(req->value()),
            req->is_delete() ? OperationType::Delete : OperationType::Upsert);
        txm->Execute(&write);
        write.Wait();
        resp->set_ok(!write.IsError());
    }

    void Read(::google::protobuf::RpcController *,
              const txnode_workload::ReadReq *req,
              txnode_workload::ReadResp *resp,
              ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        TransactionExecution *txm = Lookup(req->tx_handle());
        if (txm == nullptr)
        {
            resp->set_error(true);
            return;
        }
        // Mirror TxHandle::Read.
        TxKey tx_key = MakeKey(req->key());
        CompositeRecord<int> rec;
        txservice::ReadTxRequest read(table_, schema_version_, &tx_key, &rec);
        txm->Execute(&read);
        read.Wait();
        if (read.IsError())
        {
            resp->set_error(true);
            return;
        }
        resp->set_error(false);
        // Result is a pair<RecordStatus, commit_ts>; only Normal is present.
        bool found = read.Result().first == RecordStatus::Normal;
        resp->set_found(found);
        if (found)
        {
            resp->set_value(std::get<0>(rec.Tuple()));
        }
    }

    void Commit(::google::protobuf::RpcController *,
                const txnode_workload::CommitReq *req,
                txnode_workload::CommitResp *resp,
                ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        TransactionExecution *txm = LookupAndErase(req->tx_handle());
        if (txm == nullptr)
        {
            resp->set_committed(false);
            return;
        }
        txservice::CommitTxRequest commit;
        txm->Execute(&commit);
        commit.Wait();
        resp->set_committed(!commit.IsError() && commit.Result());
    }

    void Abort(::google::protobuf::RpcController *,
               const txnode_workload::AbortReq *req,
               txnode_workload::AbortResp *resp,
               ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        TransactionExecution *txm = LookupAndErase(req->tx_handle());
        if (txm == nullptr)
        {
            resp->set_ok(false);
            return;
        }
        txservice::AbortTxRequest abort;
        txm->Execute(&abort);
        abort.Wait();
        resp->set_ok(!abort.IsError());
    }

    void NodeInfo(::google::protobuf::RpcController *,
                  const txnode_workload::NodeInfoReq *req,
                  txnode_workload::NodeInfoResp *resp,
                  ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        resp->set_node_id(Sharder::Instance().NodeId());
        resp->set_leader_term(Sharder::Instance().LeaderTerm(req->ng_id()));
        // Non-blocking readiness proxy: this node is "ready" once it has become
        // leader of its native ng. Do NOT call WaitClusterReady here (it
        // blocks).
        resp->set_cluster_ready(Sharder::Instance().LeaderTerm(native_ng_id_) >
                                0);
    }

    void Shutdown(::google::protobuf::RpcController *,
                  const txnode_workload::ShutdownReq *,
                  txnode_workload::ShutdownResp *,
                  ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        shutdown_flag_->store(true, std::memory_order_release);
    }

private:
    // Returns the live txm for `handle`, or nullptr if unknown.
    TransactionExecution *Lookup(uint64_t handle)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = txns_.find(handle);
        return it == txns_.end() ? nullptr : it->second;
    }

    // Returns and removes the txm for `handle` (used by Commit/Abort, which end
    // the transaction's lifetime), or nullptr if unknown.
    TransactionExecution *LookupAndErase(uint64_t handle)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = txns_.find(handle);
        if (it == txns_.end())
        {
            return nullptr;
        }
        TransactionExecution *txm = it->second;
        txns_.erase(it);
        return txm;
    }

    TxService *svc_;
    const TableName *table_;
    uint64_t schema_version_;
    uint32_t native_ng_id_;
    std::atomic<bool> *shutdown_flag_;

    std::mutex mutex_;
    std::unordered_map<uint64_t, TransactionExecution *> txns_;
    uint64_t next_handle_{0};
};

// SIGTERM/SIGINT set this so main() can drain and shut down cleanly. A process
// signal handler must touch only async-signal-safe state, hence a global
// atomic.
std::atomic<bool> g_shutdown{false};

void SignalHandler(int)
{
    g_shutdown.store(true, std::memory_order_release);
}
}  // namespace

int main(int argc, char *argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // 1. Parse --topology into the per-node config.
    TxNodeConfig cfg;
    cfg.node_id = FLAGS_node_id;
    cfg.native_ng_id = FLAGS_ng_id;
    cfg.core_num = FLAGS_core_num;
    // hm_ip/hm_port are the shared data_substrate gflags (default hm_ip="").
    // Default an empty hm_ip to loopback so a bare invocation matches the
    // driver's localhost topology.
    cfg.hm_ip = FLAGS_hm_ip.empty() ? "127.0.0.1" : FLAGS_hm_ip;
    cfg.hm_port = static_cast<uint16_t>(FLAGS_hm_port);
    cfg.data_dir = FLAGS_data_dir;
    try
    {
        cfg.ng_members = ParseTopology(FLAGS_topology);
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "txnode: bad --topology: %s\n", e.what());
        return 1;
    }

    // Install signal handlers BEFORE bring-up so a kill during the (retrying)
    // StartNode RPC still terminates the process.
    struct sigaction sa;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sa.sa_handler = SignalHandler;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);

    // 2. Bring up the engine (registers with the external HM; throws on
    // failure). Keep the node on the stack so its dtor (TxService::Shutdown)
    // runs on the normal exit path below.
    std::unique_ptr<TxNode> node;
    try
    {
        node = std::make_unique<TxNode>(cfg);
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "txnode: bring-up failed: %s\n", e.what());
        return 1;
    }

    // 3. Workload service over this node's TxService + prebuilt table. The
    // Shutdown RPC and SIGTERM/SIGINT both flip g_shutdown.
    WorkloadServiceImpl workload(node->Service(),
                                 &node->Table(),
                                 node->SchemaVersion(),
                                 cfg.native_ng_id,
                                 &g_shutdown);

    brpc::Server server;
    if (server.AddService(&workload, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        std::fprintf(stderr, "txnode: failed to add WorkloadService\n");
        return 1;
    }
    brpc::ServerOptions opts;
    // 0 means use the (already capped) default bthread worker count.
    opts.num_threads = 0;
    if (server.Start(FLAGS_workload_port, &opts) != 0)
    {
        std::fprintf(stderr,
                     "txnode: failed to start WorkloadService on port %d\n",
                     FLAGS_workload_port);
        return 1;
    }

    // 4. Serve until the Shutdown RPC or a SIGTERM/SIGINT flips the flag. Poll
    // with a short bthread sleep so we don't burn a worker.
    while (!g_shutdown.load(std::memory_order_acquire))
    {
        bthread_usleep(50 * 1000);  // 50 ms
    }

    // 5. Clean shutdown: stop serving, then tear the engine down (TxNode dtor
    // calls TxService::Shutdown).
    server.Stop(0);
    server.Join();
    node.reset();
    return 0;
}
