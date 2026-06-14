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
#include "cluster/txnode_bringup.h"

#include <gflags/gflags.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store_service_client.h"
#include "eloq_data_store_service/data_store_service.h"
#include "eloq_data_store_service/data_store_service_config.h"
#include "harness/mem_data_store_factory.h"
#include "harness/port_util.h"  // BindEphemeralPort (shared with test_node.cpp)
#include "include/mock/mock_catalog_factory.h"  // MockCatalogFactory, MockSystemHandler
#include "sharder.h"                            // NodeConfig
#include "tx_service.h"

namespace txservice
{
namespace test
{
namespace
{
// Hard, build-mode-independent precondition check for fixture bring-up.
// (assert() would compile out under NDEBUG and silently proceed past a failed
// bring-up; the caller catches the exception and reports it with context.)
[[noreturn]] void FailBringup(const char *what)
{
    throw std::runtime_error(std::string("TxNode bring-up failed: ") + what);
}
}  // namespace

struct TxNode::Impl
{
    explicit Impl(const TxNodeConfig &c) : cfg(c)
    {
    }

    TxNodeConfig cfg;

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

TxNode::TxNode(const TxNodeConfig &cfg) : impl_(std::make_unique<Impl>(cfg))
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
        "bthread_concurrency", std::to_string(impl.cfg.core_num).c_str());
#ifdef ELOQ_MODULE_ENABLED
    GFLAGS_NAMESPACE::SetCommandLineOption("brpc_worker_as_ext_processor",
                                           "true");
    GFLAGS_NAMESPACE::SetCommandLineOption("worker_polling_time_us", "100000");
#endif

    // 1. Unique temp dir (per node) for the DSS config + migration log +
    // cluster config.
    impl.dir = std::filesystem::path(impl.cfg.data_dir);
    std::filesystem::remove_all(impl.dir);
    std::filesystem::create_directories(impl.dir);

    // 2. In-process DataStoreService FIRST, on an ephemeral port. Initialize()
    // creates a single-node topology that owns shard 0 in ReadWrite;
    // StartService binds brpc to the port. Because a probed-free port can still
    // be lost to a concurrent process between probe and bind (TOCTOU), retry
    // StartService with a fresh port. Unchanged from Phase 1 TestNode.
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
    if (!dss_ok)
    {
        FailBringup("DataStoreService StartService");
    }
    if (!impl.dss->GetClusterManager().IsOwnerOfShard(0))
    {
        FailBringup("DataStoreService not owner of shard 0");
    }

    // 3. DataStoreServiceClient over the local DSS. Both the client ctor and
    // the TxService ctor (below) want a CatalogFactory*[3]; all engines share
    // the mock factory in tests, so one array serves both.
    CatalogFactory *catalog_factory_arr[NUM_EXTERNAL_ENGINES] = {
        &impl.catalog_factory,
        &impl.catalog_factory,
        &impl.catalog_factory,
    };
    // bind_data_shard_with_ng=false keeps the DSS client out of the ng-bound
    // leader/standby path. With skip_kv=true and store_hd passed as nullptr to
    // the TxService ctor below, that path is unreached regardless. Keeping
    // bind_data_shard_with_ng=false leaves the DSS shard 0 that StartService
    // already opened open and owned. Unchanged from Phase 1 TestNode.
    impl.store_hd = std::make_unique<EloqDS::DataStoreServiceClient>(
        /*is_bootstrap=*/true,
        catalog_factory_arr,
        impl.cluster_mgr,
        /*bind_data_shard_with_ng=*/false,
        impl.dss.get());
    if (!impl.store_hd->Connect())
    {
        FailBringup("DataStoreServiceClient Connect");
    }

    // 4. Conf map. Mirrors the Phase 1 hm-less configuration; rep_group_cnt = 1
    // (foundation: 1 candidate per ng, no standbys).
    impl.conf = {
        {"core_num", impl.cfg.core_num},
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

    const uint64_t cluster_config_version = kClusterConfigVersion;
    const uint32_t node_id = impl.cfg.node_id;
    const uint32_t ng_id = impl.cfg.native_ng_id;

    // Register the test table as a prebuilt table. With skip_kv=true the engine
    // installs this schema into every shard's catalog at leader start, so
    // Read/Upsert against impl.table resolve a CcMap without a KV catalog fetch
    // (the MockCatalogFactory has no real catalog backing in the store). The
    // image string is opaque to MockCatalogFactory::CreateTableSchema, so the
    // table's own name serves as a self-describing placeholder.
    impl.prebuilt_tables.try_emplace(impl.table, impl.table.String());

    // 4b. Build the FULL multi-node topology from cfg.ng_members: every node's
    // ng_configs describes ALL node-groups and ALL members. The ports were
    // assigned by the driver (it reserved a 4-wide window per node: cc-stream =
    // base, cc-node = base+1, log-group = base+2, log-replay = base+3); we just
    // plumb the assigned base port through as each member's NodeConfig port.
    // tx_ips/tx_ports reflect THIS node's own (host, base port) entry.
    for (const auto &[member_ng_id, members] : impl.cfg.ng_members)
    {
        std::vector<NodeConfig> group_config;
        group_config.reserve(members.size());
        for (const auto &[member_node_id, host, base_port] : members)
        {
            // is_candidate=true: in this 1-member-per-NG foundation each member
            // is its NG's sole leader-capable node. NodeConfig defaults
            // is_candidate=false (a non-candidate voter), which would make the
            // node ineligible for leadership -- CcNode rejects a leader target
            // that is not a candidate (cc_node.cpp). Production marks each NG's
            // primary as a candidate the same way (util.h ParseNgConfig).
            group_config.emplace_back(member_node_id,
                                      host,
                                      base_port,
                                      /*is_candidate=*/true);
            if (member_node_id == node_id)
            {
                impl.tx_ips.push_back(host);
                impl.tx_ports.push_back(base_port);
            }
        }
        impl.ng_configs.try_emplace(member_ng_id, std::move(group_config));
    }
    if (impl.tx_ports.empty())
    {
        FailBringup("node_id not found in ng_members");
    }

    // store_hd is passed as nullptr here even though the DSS/MemDataStore is
    // brought up above. With skip_kv=true the engine keeps full entries in the
    // cc maps and never routes data through the KV store, so a store handler is
    // unnecessary for the data path. Crucially, the checkpointer thread is
    // never spawned when store_hd_ == nullptr (Checkpointer ctor guard,
    // checkpointer.cpp), so no periodic data-sync runs against the
    // in-memory-only prebuilt table. The DSS bring-up is retained as
    // scaffolding for a future storage-backed (skip_kv=false) fixture.
    impl.tx_service =
        std::make_unique<TxService>(catalog_factory_arr,
                                    &MockSystemHandler::Instance(),
                                    impl.conf,
                                    node_id,
                                    ng_id,
                                    &impl.ng_configs,
                                    cluster_config_version,
                                    /*store_hd=*/nullptr,
                                    /*log_hd=*/nullptr,
                                    /*enable_mvcc=*/true,
                                    /*skip_wal=*/true,
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

    // Multi-node Start: pass a REAL hm_ip/hm_port (the driver's scripted host
    // manager) and fork_host_manager=false, so Sharder::Init registers this
    // node via a StartNode RPC to that external HM instead of forking one and
    // does NOT self-promote to leader (the driver drives OnLeaderStart in a
    // later task). The StartNode RPC retries until the HM is reachable, so the
    // driver must have the HM up before starting this node; if it is not up,
    // Start() returns non-0 after the retries and we FailBringup. Start() does
    // NOT wait on cluster readiness internally (WaitClusterReady is a separate
    // method); we deliberately do NOT call it here -- the node is not leader
    // yet, and the driver waits for readiness separately.
    int started = impl.tx_service->Start(node_id,
                                         ng_id,
                                         &impl.ng_configs,
                                         cluster_config_version,
                                         &impl.tx_ips,
                                         &impl.tx_ports,
                                         &impl.cfg.hm_ip,
                                         &impl.cfg.hm_port,
                                         /*hm_bin_path=*/nullptr,
                                         impl.conf,
                                         /*log_agent=*/nullptr,
                                         local_path,
                                         cluster_config_path,
                                         /*fork_host_manager=*/false);
    if (started != 0)
    {
        FailBringup("TxService Start");
    }

    // Intentionally NO WaitClusterReady(): the node is not leader yet; the
    // driver drives leadership and waits for readiness separately.
}

TxNode::~TxNode()
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

TxService *TxNode::Service()
{
    return impl_->tx_service.get();
}

const TableName &TxNode::Table() const
{
    return impl_->table;
}

uint64_t TxNode::SchemaVersion() const
{
    return impl_->schema_version;
}

}  // namespace test
}  // namespace txservice
