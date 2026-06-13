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
#include "harness/test_node.h"

#include <gflags/gflags.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store_service_client.h"
#include "eloq_data_store_service/data_store_service.h"
#include "eloq_data_store_service/data_store_service_config.h"
#include "harness/mem_data_store_factory.h"
#include "include/mock/mock_catalog_factory.h"  // MockCatalogFactory, MockSystemHandler
#include "sharder.h"                            // NodeConfig
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"

namespace txservice
{
namespace test
{
namespace
{
// Binds an ephemeral TCP port on loopback and returns (fd, port) without
// closing the socket, so the kernel will not hand the same port to a later
// call. Caller must close the fd once it has claimed the port.
std::pair<int, uint16_t> BindEphemeralPort()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    // Probe on INADDR_ANY (0.0.0.0), matching how brpc binds the DSS/cc-node/
    // log-replay servers. Probing loopback-only would not accurately reserve
    // what the servers need (a port free on 127.0.0.1 can still be unbindable
    // on 0.0.0.0), causing spurious "Fail to listen" under port pressure.
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = 0;  // let the kernel choose
    [[maybe_unused]] int rc =
        ::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    assert(rc == 0);
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len);
    return {fd, ntohs(addr.sin_port)};
}

// Tries to bind a specific port on INADDR_ANY (matching the servers; see
// BindEphemeralPort). Returns the open fd on success or -1.
int TryBindPort(uint16_t port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0)
    {
        ::close(fd);
        return -1;
    }
    return fd;
}

// The TxService derives several ports from a single base: cc-stream = base,
// cc-node = base + 1, log-replay = base + 3 (see GET_CCNODE_RPC_PORT /
// GET_LOG_REPLAY_RPC_PORT in sharder.h). A free-port picker that only reserves
// the base is not enough -- base+1 / base+3 must be free too, or
// TxService::Start() fails to listen. This finds a base whose whole +0..+3
// window is simultaneously bindable, holding every socket open while probing so
// the kernel cannot reissue one of them mid-window.
uint16_t ReserveTxPortWindow()
{
    constexpr int kWindow = 4;  // base .. base+3
    constexpr int kMaxAttempts = 64;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt)
    {
        auto [base_fd, base] = BindEphemeralPort();
        // Avoid wrapping uint16 at the top of the range.
        if (base > 65535 - kWindow)
        {
            ::close(base_fd);
            continue;
        }
        std::vector<int> fds{base_fd};
        bool ok = true;
        for (int off = 1; off < kWindow; ++off)
        {
            int fd = TryBindPort(static_cast<uint16_t>(base + off));
            if (fd < 0)
            {
                ok = false;
                break;
            }
            fds.push_back(fd);
        }
        for (int fd : fds)
        {
            ::close(fd);
        }
        if (ok)
        {
            return base;
        }
    }
    // Extremely unlikely; let the caller's Start() surface the failure.
    auto [fd, port] = BindEphemeralPort();
    ::close(fd);
    return port;
}
}  // namespace

TxKey Key(int k)
{
    return TxKey(std::make_unique<CompositeKey<int>>(k));
}

std::unique_ptr<CompositeRecord<int>> Rec(int v)
{
    return std::make_unique<CompositeRecord<int>>(v);
}

struct TestNode::Impl
{
    explicit Impl(const TestNodeOptions &opts) : options(opts)
    {
    }

    TestNodeOptions options;

    std::filesystem::path dir;

    // Catalog factory shared across the engine. Lives for the whole node.
    MockCatalogFactory catalog_factory;

    // In-process data store service + its client (storage backend).
    EloqDS::DataStoreServiceClusterManager cluster_mgr;
    std::unique_ptr<EloqDS::DataStoreService> dss;
    std::unique_ptr<EloqDS::DataStoreServiceClient> store_hd;

    // The engine.
    std::unique_ptr<TxService> tx_service;

    // The (valid) test table. Read/write resolution against it is a later task.
    TableName table{std::string_view("test_table"),
                    TableType::Primary,
                    TableEngine::EloqKv};
    uint64_t schema_version{1};

    // Owning storage for the Start() arguments that take pointers.
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs;
    std::vector<std::string> tx_ips;
    std::vector<uint16_t> tx_ports;
    std::map<std::string, uint32_t> conf;

    // Prebuilt-tables map handed to the TxService ctor. With skip_kv=true the
    // engine installs each entry's schema into table_catalogs_ at leader start
    // (LocalCcShards::InitPrebuiltTables), so the table resolves without a KV
    // catalog fetch. The image value is opaque to MockCatalogFactory (it stores
    // it verbatim on MockTableSchema), so the table name itself is enough.
    std::unordered_map<TableName, std::string> prebuilt_tables;
};

TestNode::TestNode(const TestNodeOptions &options)
    : impl_(std::make_unique<Impl>(options))
{
    Impl &impl = *impl_;

    // 0. CRITICAL for ELOQ_MODULE_ENABLED builds (this build): once
    // TxService::Start calls eloq::register_module(), every brpc worker thread
    // N drives TxProcessor N as an "external" processor and calls
    // TxServiceModule::ExtThdStart(N), which asserts N < coordinators_.size()
    // (== core_num). brpc otherwise spins up ~NUM_VCPU workers, so a worker
    // with index >= core_num trips the assertion and SIGABRTs
    // deterministically. Production caps bthread_concurrency to core_num before
    // any brpc server starts (data_substrate.cpp ~L880); we mirror that. brpc
    // reads this gflag when it first lazily creates its global worker pool --
    // which is on the first brpc server start, i.e. the DSS below -- so it MUST
    // be set here, before DSS StartService, not just before TxService::Start.
    // It is a process-global gflag set once. We do NOT call
    // gflags::ParseCommandLineFlags (that breaks catch_discover_tests);
    // SetCommandLineOption is independent of parsing.
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "bthread_concurrency", std::to_string(impl.options.core_num).c_str());
#ifdef ELOQ_MODULE_ENABLED
    GFLAGS_NAMESPACE::SetCommandLineOption("brpc_worker_as_ext_processor",
                                           "true");
    GFLAGS_NAMESPACE::SetCommandLineOption("worker_polling_time_us", "100000");
#endif

    // 1. Unique temp dir for the DSS config + migration log + cluster config.
    impl.dir = std::filesystem::temp_directory_path() /
               ("testnode_" + std::to_string(::getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(this)));
    std::filesystem::remove_all(impl.dir);
    std::filesystem::create_directories(impl.dir);

    // 2. In-process DataStoreService FIRST, on an ephemeral port. Ordering
    // matters: brpc (the DSS server and its client below) lazily allocates
    // ephemeral sockets when it first starts, and any of those could grab a
    // port inside the TxService's +0..+3 window if that window were already
    // reserved and closed. So we bring the DSS/client fully up first and
    // reserve the tx window LAST (step 5), leaving only the socket-free
    // TxService ctor between the probe and TxService::Start's bind.
    // Initialize() creates a single-node topology that owns shard 0 in
    // ReadWrite; StartService binds brpc to the port. Because a probed-free
    // port can still be lost to a concurrent process between probe and bind
    // (TOCTOU), retry StartService with a fresh port.
    constexpr int kMaxBindRetries = 16;
    uint16_t dss_port = 0;
    bool dss_ok = false;
    for (int attempt = 0; attempt < kMaxBindRetries && !dss_ok; ++attempt)
    {
        {
            auto [dss_fd, port] = BindEphemeralPort();
            ::close(dss_fd);
            dss_port = port;
        }
        impl.cluster_mgr.Initialize("127.0.0.1", dss_port);
        auto factory = std::make_unique<EloqDS::MemDataStoreFactory>();
        impl.dss = std::make_unique<EloqDS::DataStoreService>(
            impl.cluster_mgr,
            (impl.dir / "dss_config.ini").string(),
            (impl.dir / "DSMigrateLog").string(),
            std::move(factory));
        dss_ok = impl.dss->StartService(/*create_db_if_missing=*/true);
        if (!dss_ok)
        {
            impl.dss.reset();
        }
    }
    assert(dss_ok);
    assert(impl.dss->GetClusterManager().IsOwnerOfShard(0));

    // 3. DataStoreServiceClient over the local DSS. The client ctor wants a
    // CatalogFactory*[3]; all engines share the mock factory in tests.
    CatalogFactory *catalog_factory_arr[NUM_EXTERNAL_ENGINES] = {
        &impl.catalog_factory,
        &impl.catalog_factory,
        &impl.catalog_factory,
    };
    // bind_data_shard_with_ng MUST be false here. When true, the leader-start
    // path (CcNode::OnLeaderStart -> DataStoreServiceClient::OnLeaderStart)
    // calls ClearStandbySnapshotPayloadForEloqStore, which under
    // DATA_STORE_TYPE_ELOQDSS_ELOQSTORE hard-casts data_store_factory_ to
    // EloqStoreDataStoreFactory* and dereferences it -- but our factory is a
    // MemDataStoreFactory, so that static_cast is undefined behavior and
    // segfaults. With false, that whole ng-bound standby/migration path is
    // skipped; the DSS shard 0 that StartService already opened stays open and
    // owned, so reads/writes still route through it.
    impl.store_hd = std::make_unique<EloqDS::DataStoreServiceClient>(
        /*is_bootstrap=*/true,
        catalog_factory_arr,
        impl.cluster_mgr,
        /*bind_data_shard_with_ng=*/false,
        impl.dss.get());
    [[maybe_unused]] bool connected = impl.store_hd->Connect();
    assert(connected);

    // 4. Conf / catalog / prebuilt table -- all port-independent, built before
    // the tx port window is reserved (step 5) so nothing allocates sockets
    // between that reservation and TxService::Start.

    // Conf map mirrors StartTsCollector-Test's working hm-less configuration.
    impl.conf = {
        {"core_num", impl.options.core_num},
        {"range_split_worker_num", 0},
        {"range_slice_memory_limit_percent", 20},
        {"node_memory_limit_mb", 1000},
        {"node_log_limit_mb", 1000},
        {"realtime_sampling", 0},
        {"checkpointer_interval", 10},
        {"checkpointer_delay_seconds", 0},
        {"checkpointer_min_ckpt_request_interval", 5},
        {"enable_shard_heap_defragment", 0},
        {"enable_key_cache", 0},
        {"collect_active_tx_ts_interval_seconds", 2},
        {"rep_group_cnt", 1},
    };

    const uint64_t cluster_config_version = 2;
    const uint32_t node_id = 0;
    const uint32_t ng_id = 0;

    CatalogFactory *tx_catalog_factory[NUM_EXTERNAL_ENGINES] = {
        &impl.catalog_factory,
        &impl.catalog_factory,
        &impl.catalog_factory,
    };

    // Register the test table as a prebuilt table. With skip_kv=true the engine
    // installs this schema into every shard's catalog at leader start, so
    // Read/Upsert against impl.table resolve a CcMap without a KV catalog fetch
    // (the MockCatalogFactory has no real catalog backing in the store). The
    // image string is opaque to MockCatalogFactory::CreateTableSchema, so the
    // table's own name serves as a self-describing placeholder.
    impl.prebuilt_tables.try_emplace(impl.table, impl.table.String());

    // 5. Reserve the tx port window LAST -- after every DSS/client brpc socket
    // is already allocated -- so the only thing between the probe and
    // TxService::Start binding the window is the socket-free TxService ctor.
    // The window must avoid the DSS port. We deliberately do NOT retry Start on
    // failure: it initializes the process-wide Sharder singleton, which cannot
    // be cleanly re-Init'd, so reserving the window this late (rather than
    // retrying) is what makes a lost-race bind vanishingly unlikely outside
    // truly-concurrent runs. cc-stream binds tx_port, cc-node tx_port+1,
    // log-replay tx_port+3; no log agent is wired in (hm-less), so the txlog
    // port is never actually bound.
    uint16_t tx_port = 0;
    do
    {
        tx_port = ReserveTxPortWindow();
    } while (dss_port >= tx_port && dss_port <= tx_port + 3);
    impl.ng_configs = {{0, {NodeConfig(0, "127.0.0.1", tx_port)}}};
    impl.tx_ips = {"127.0.0.1"};
    impl.tx_ports = {tx_port};

    // store_hd is passed as nullptr here even though the DSS/MemDataStore is
    // brought up above. With skip_kv=true the engine keeps full entries in the
    // cc maps and never routes data through the KV store, so a store handler is
    // unnecessary for the data path. Crucially, the checkpointer's main loop
    // early-returns when store_hd_ == nullptr (checkpointer.cpp): if we instead
    // passed the real handler, the periodic checkpoint would try to data-sync
    // the in-memory-only prebuilt table and InitCcm would abort (it has no
    // store-side range/statistics to flush). The DSS bring-up is retained as
    // scaffolding for a future storage-backed (skip_kv=false) fixture.
    impl.tx_service =
        std::make_unique<TxService>(tx_catalog_factory,
                                    &MockSystemHandler::Instance(),
                                    impl.conf,
                                    node_id,
                                    ng_id,
                                    &impl.ng_configs,
                                    cluster_config_version,
                                    /*store_hd=*/nullptr,
                                    /*log_hd=*/nullptr,
                                    impl.options.enable_mvcc,
                                    impl.options.skip_wal,
                                    /*skip_kv=*/true,
                                    /*enable_cache_replacement=*/false,
                                    /*auto_redirect=*/true,
                                    /*metrics_registry=*/nullptr,
                                    /*common_labels=*/metrics::CommonLabels{},
                                    &impl.prebuilt_tables);

    // braft needs a protocol prefix on the local path.
    const std::string local_path = "local://" + impl.dir.string();
    const std::string cluster_config_path =
        (impl.dir / "cluster_config.json").string();

    // hm-less Start: pass nullptr for hm_ip/hm_port/hm_bin_path and the log
    // agent. In the test env Sharder self-promotes node 0 to ng-0 leader and
    // finishes log replay directly (see sharder.cpp OnLeaderStart path).
    [[maybe_unused]] int started =
        impl.tx_service->Start(node_id,
                               ng_id,
                               &impl.ng_configs,
                               cluster_config_version,
                               &impl.tx_ips,
                               &impl.tx_ports,
                               /*hm_ip=*/nullptr,
                               /*hm_port=*/nullptr,
                               /*hm_bin_path=*/nullptr,
                               impl.conf,
                               /*log_agent=*/nullptr,
                               local_path,
                               cluster_config_path);
    assert(started == 0);

    impl.tx_service->WaitClusterReady();
}

TestNode::~TestNode()
{
    Impl &impl = *impl_;
    if (impl.tx_service)
    {
        impl.tx_service->Shutdown();
        impl.tx_service.reset();
    }
    // No public DataStoreService::Shutdown(); teardown is the destructor. Drop
    // the client first (it references the service), then the service.
    impl.store_hd.reset();
    impl.dss.reset();

    std::error_code ec;
    std::filesystem::remove_all(impl.dir, ec);
}

TxHandle TestNode::BeginTx()
{
    return BeginTx(impl_->options.isolation, impl_->options.protocol);
}

TxHandle TestNode::BeginTx(IsolationLevel isolation, CcProtocol protocol)
{
    Impl &impl = *impl_;
    TransactionExecution *txm = impl.tx_service->NewTx();
    InitTxRequest init(isolation, protocol);
    init.Reset();
    txm->Execute(&init);
    init.Wait();
    // Fail fast and locally: never hand back a TxHandle over a txm whose
    // initialization failed, which would surface as confusing downstream
    // errors.
    assert(!init.IsError());
    return TxHandle(txm, &impl.table, impl.schema_version);
}

const TableName &TestNode::Table() const
{
    return impl_->table;
}

uint64_t TestNode::SchemaVersion() const
{
    return impl_->schema_version;
}

TxService *TestNode::Service()
{
    return impl_->tx_service.get();
}

// ---- TxHandle ----

bool TxHandle::Commit()
{
    CommitTxRequest req;
    txm_->Execute(&req);
    req.Wait();
    return !req.IsError() && req.Result();
}

bool TxHandle::Abort()
{
    AbortTxRequest req;
    txm_->Execute(&req);
    req.Wait();
    // AbortTxRequest's result is true once the abort completes. Treat a
    // non-errored abort as success.
    return !req.IsError();
}

// The test table is registered as a prebuilt table (see the TxService ctor in
// TestNode), so these requests resolve a CcMap and run to completion. The
// schema_version passed must match what the cc layer derives from the table's
// KeySchema (MockKeySchema::SchemaTs() == 1, mirrored by TestNode's
// schema_version), or InitCcm rejects the request as a schema mismatch.

bool TxHandle::Upsert(int key, int value)
{
    // OperationType::Upsert writes the record whether or not the key already
    // exists, which is the behavior callers expect from a generic write helper.
    UpsertTxRequest req(table_, Key(key), Rec(value), OperationType::Upsert);
    txm_->Execute(&req);
    req.Wait();
    return !req.IsError();
}

bool TxHandle::Delete(int key)
{
    UpsertTxRequest req(
        table_, Key(key), /*rec=*/nullptr, OperationType::Delete);
    txm_->Execute(&req);
    req.Wait();
    return !req.IsError();
}

bool TxHandle::Read(int key, int &value_out)
{
    TxKey tx_key = Key(key);
    CompositeRecord<int> rec;
    ReadTxRequest req(table_, schema_version_, &tx_key, &rec);
    txm_->Execute(&req);
    req.Wait();
    if (req.IsError())
    {
        return false;
    }
    // Result is a pair<RecordStatus, commit_ts>. Only Normal means the key is
    // present; Deleted / Unknown mean it is absent for the caller's purposes.
    if (req.Result().first != RecordStatus::Normal)
    {
        return false;
    }
    value_out = std::get<0>(rec.Tuple());
    return true;
}

}  // namespace test
}  // namespace txservice
