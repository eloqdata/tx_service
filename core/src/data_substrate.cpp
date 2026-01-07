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

#include <gflags/gflags.h>
#include <sys/resource.h>
#include <sys/sysinfo.h>

#include <filesystem>
#include <iostream>
#include <mutex>

#include "INIReader.h"
// clang-format off
#include "tx_service.h"
// clang-format on
#include "data_substrate.h"
#include "log_server.h"
#include "sequences/sequences.h"
#include "store/data_store_handler.h"
#include "util.h"
#ifdef ELOQDS
#include "eloq_data_store_service/data_store_service.h"
#endif

// Data Substrate gflags definitions
DEFINE_string(eloq_data_path, "/tmp/eloq_data", "Data substrate data path");
DEFINE_int32(core_number, 8, "Number of cores for data substrate");
DEFINE_bool(enable_heap_defragment, false, "Enable heap defragmentation");
DEFINE_bool(enable_wal, true, "Enable WAL");
DEFINE_bool(enable_data_store, true, "Enable data store");
DEFINE_bool(enable_io_uring, false, "Enable io_uring as the IO engine");
DEFINE_bool(raft_log_async_fsync,
            false,
            "Whether raft log fsync is performed asynchronously (blocking the "
            "bthread) instead of blocking the worker thread");
DEFINE_bool(bootstrap, false, "init system tables and exit");
DEFINE_bool(enable_cache_replacement, true, "Enable cache replacement");
DEFINE_bool(enable_mvcc, false, "Enable mvcc");

// Network configuration gflags
DEFINE_string(tx_ip, "127.0.0.1", "Local tx service IP address");
DEFINE_int32(tx_port, 16379, "Local tx service port");
DEFINE_string(tx_ip_port_list, "", "IP list for cluster");
DEFINE_string(tx_standby_ip_port_list, "", "Standby IP list");
DEFINE_string(tx_voter_ip_port_list, "", "Voter IP list");

DEFINE_string(cluster_config_file, "", "Cluster configuration file");
DEFINE_int32(node_group_replica_num, 3, "Node group replica number");
DEFINE_bool(
    bind_all,
    false,
    "Listen on all interfaces if enabled, otherwise listen on local.ip");
DEFINE_uint32(maxclients, 500000, "maxclients");
DEFINE_string(log_file_name_prefix,
              "eloqdb.log",
              "Sets the prefix for log files. Default is 'eloqdb.log'");

namespace brpc
{
DECLARE_int32(event_dispatcher_num);
}
const auto NUM_VCPU = std::thread::hardware_concurrency();

// Static mutex for protecting initialization state
static std::mutex instance_mutex_;

DataSubstrate &DataSubstrate::Instance()
{
    static DataSubstrate instance_;
    return instance_;
}

bool DataSubstrate::Init(const std::string &config_file_path)
{
    std::lock_guard<std::mutex> lock(instance_mutex_);

    DataSubstrate &instance = Instance();

    if (instance.init_state_ != InitState::NotInitialized)
    {
        LOG(WARNING) << "DataSubstrate already initialized";
        return true;
    }

    // Save config file path for use in Start()
    instance.config_file_path_ = config_file_path;

    INIReader config_file_reader(config_file_path);
    if (!config_file_path.empty() && config_file_reader.ParseError() != 0)
    {
        LOG(ERROR) << "Failed to parse config file: " << config_file_path;
        instance.init_state_ = InitState::NotInitialized;
        return false;
    }

    // Phase 1: Load core and network configuration only
    if (!instance.LoadCoreAndNetworkConfig(config_file_reader))
    {
        instance.init_state_ = InitState::NotInitialized;
        return false;
    }

    // Add Sequences table (system table, not engine-specific)
    instance.prebuilt_tables_.try_emplace(
        txservice::Sequences::table_name_,
        std::string(txservice::Sequences::table_name_sv_));

    // Mark as initialized (but not started)
    instance.init_state_ = InitState::ConfigLoaded;

    LOG(INFO) << "DataSubstrate initialized (config loaded)";
    return true;
}

bool DataSubstrate::Start()
{
    std::lock_guard<std::mutex> lock(instance_mutex_);

    DataSubstrate &instance = Instance();

    txservice::InitializeTscFrequency();
    struct sysinfo meminfo;
    if (sysinfo(&meminfo))
    {
        LOG(ERROR) << "Failed to get system memory info: " << strerror(errno)
                   << " when node_memory_limit_mb is not set";
        return false;
    }
    uint32_t mem_mib = meminfo.totalram * meminfo.mem_unit / (1024 * 1024);
    instance.remaining_node_memory_mb_ = mem_mib * 4 / 5;

    if (instance.init_state_ == InitState::NotInitialized)
    {
        LOG(ERROR) << "Cannot start: DataSubstrate not initialized. Call "
                      "Init() first.";
        return false;
    }

    if (instance.init_state_ == InitState::Started)
    {
        LOG(WARNING) << "DataSubstrate already started";
        return true;
    }

    // Use config file path saved during Init()
    INIReader config_file_reader(instance.config_file_path_);
    if (!instance.config_file_path_.empty() &&
        config_file_reader.ParseError() != 0)
    {
        LOG(ERROR) << "Failed to parse config file: "
                   << instance.config_file_path_;
        return false;
    }

    // Phase 1: Initialize log service
    if (!instance.InitializeLogService(config_file_reader))
    {
        LOG(ERROR) << "Failed to initialize log service";
        return false;
    }

    // Phase 2: Initialize storage handler
    if (instance.GetCoreConfig().enable_data_store &&
        !instance.InitializeStorageHandler(config_file_reader))
    {
        LOG(ERROR) << "Failed to initialize storage handler";
        return false;
    }

    // Phase 3: Initialize metrics
    if (!instance.InitializeMetrics(config_file_reader))
    {
        LOG(ERROR) << "Failed to initialize metrics";
        return false;
    }

    // Phase 4: Initialize TxService
    if (!instance.InitializeTxService(config_file_reader))
    {
        LOG(ERROR) << "Failed to initialize TxService";
        return false;
    }

    instance.init_state_ = InitState::Started;
    LOG(INFO) << "DataSubstrate started successfully";

    // Notify all waiting engine threads that DataSubstrate has started
    // (instance_mutex_ is already held, which is what
    // data_substrate_started_cv_ uses)
    instance.data_substrate_started_cv_.notify_all();

    return true;
}

DataSubstrate::~DataSubstrate()
{
    // Note: We can't call Shutdown() here because TxService is an incomplete
    // type The actual shutdown will be handled by the component that owns the
    // TxService For now, just reset the pointers
    tx_service_ = nullptr;
    store_hd_ = nullptr;
#ifdef ELOQDS
    data_store_service_ = nullptr;
#endif
    log_server_ = nullptr;
    metrics_registry_ = nullptr;
}

void DataSubstrate::Shutdown()
{
#ifdef ELOQ_MODULE_ELOQSQL
    if (system_handler_ != nullptr)
    {
        system_handler_->Shutdown();
        system_handler_ = nullptr;
    }
#endif
    if (tx_service_ != nullptr)
    {
        LOG(INFO) << "Shutting down the tx service.";
        tx_service_->Shutdown();
        LOG(INFO) << "Tx service shut down.";
    }

    if (store_hd_ != nullptr)
    {
        LOG(INFO) << "Shutting down the storage handler.";
        store_hd_ = nullptr;  // Wait for all in-fight requests complete.
#if ELOQDS
        if (data_store_service_ != nullptr)
        {
            data_store_service_ = nullptr;
        }
#endif
        LOG(INFO) << "Storage handler shut down.";
    }

#if (WITH_LOG_SERVICE)
    if (log_server_ != nullptr)
    {
        LOG(INFO) << "Shutting down the internal logservice.";
        std::string storage_path = log_server_->GetStoragePath();
#if defined(LOG_STATE_TYPE_RKDB_ALL) && !defined(OPEN_LOG_SERVICE)
        std::string rocksdb_storage_path = log_server_->GetRocksDBStoragePath();
#endif
        log_server_ = nullptr;
        if (core_config_.bootstrap)
        {
            std::error_code ec;
#if defined(LOG_STATE_TYPE_RKDB_ALL) && !defined(OPEN_LOG_SERVICE)
            // Remove rocksdb storage path for bootstrap mode
            LOG(INFO) << "Removing rocksdb storage path after bootstrap: "
                      << rocksdb_storage_path;
            std::filesystem::remove_all(rocksdb_storage_path, ec);
            if (ec)
            {
                LOG(ERROR) << "Failed to remove rocksdb storage path: "
                           << ec.message();
            }
#endif
            // Remove log storage path for bootstrap mode
            LOG(INFO) << "Removing log storage path after bootstrap: "
                      << storage_path;
            if (storage_path.find("local://") == 0)
            {
                storage_path = storage_path.substr(8);
            }
            std::filesystem::remove_all(storage_path, ec);
            if (ec)
            {
                LOG(ERROR) << "Failed to remove log storage path: "
                           << ec.message();
            }
        }
        LOG(INFO) << "Internal logservice shut down.";
    }
#endif
    txservice::Sequences::Destory();

#if defined(DATA_STORE_TYPE_DYNAMODB) ||                 \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) || \
    defined(LOG_STATE_TYPE_RKDB_S3)
    Aws::ShutdownAPI(aws_options_);
#endif

    tx_service_ = nullptr;
}

bool DataSubstrate::RegisterEngine(
    txservice::TableEngine engine_type,
    txservice::CatalogFactory *factory,
    txservice::SystemHandler *system_handler,
    std::vector<std::pair<txservice::TableName, std::string>> &&prebuilt_tables,
    std::vector<std::tuple<metrics::Name,
                           metrics::Type,
                           std::vector<metrics::LabelGroup>>> &&engine_metrics,
    std::function<void(std::string_view, std::string_view)> publish_func)
{
    // Acquire mutex to protect RegisterEngine() and init_state_ access
    // This is thread-safe as RegisterEngine() may be called from different
    // threads Note: instance_mutex_ is a static mutex declared in the .cpp file
    std::lock_guard<std::mutex> lock(instance_mutex_);

    DataSubstrate &instance = Instance();

    // Check init_state_ (protected by mutex) - must be ConfigLoaded
    if (instance.init_state_ != InitState::ConfigLoaded)
    {
        if (instance.init_state_ == InitState::NotInitialized)
        {
            LOG(ERROR)
                << "Cannot register engine: DataSubstrate not initialized. "
                   "Call Init() first.";
        }
        else if (instance.init_state_ == InitState::Started)
        {
            LOG(ERROR)
                << "Cannot register engine: DataSubstrate already started. "
                   "Register engines before calling Start().";
        }
        return false;
    }

    // Validate engine type (must be one of the external engines: EloqSql=1,
    // EloqKv=2, EloqDoc=3)
    if (engine_type == txservice::TableEngine::None ||
        engine_type < txservice::TableEngine::EloqSql ||
        engine_type > txservice::TableEngine::EloqDoc)
    {
        LOG(ERROR) << "Invalid engine type: " << static_cast<int>(engine_type);
        return false;
    }

    // Find engine slot (EloqSql=1 -> index 0, EloqKv=2 -> index 1, EloqDoc=3 ->
    // index 2)
    size_t engine_index = static_cast<size_t>(engine_type) - 1;
    if (engine_index >= NUM_EXTERNAL_ENGINES)
    {
        LOG(ERROR) << "Engine type out of range: "
                   << static_cast<int>(engine_type);
        return false;
    }

    // Check for duplicate registration by checking if engine is already enabled
    // Use engine_registration_mutex_ to protect engine_registered flag
    std::lock_guard<std::mutex> reg_lock(instance.engine_registration_mutex_);
    if (instance.engines_[engine_index].engine_registered)
    {
        LOG(ERROR) << "Engine " << static_cast<int>(engine_type)
                   << " already registered";
        return false;
    }

    // Register the engine
    instance.engines_[engine_index].engine = engine_type;
    instance.engines_[engine_index].catalog_factory = factory;

    // Store system handler if provided (only one system handler is allowed)
    if (system_handler != nullptr)
    {
        if (instance.system_handler_ != nullptr &&
            instance.system_handler_ != system_handler)
        {
            LOG(WARNING)
                << "SystemHandler already set, replacing with new handler";
        }
        instance.system_handler_ = system_handler;
    }

    // Store prebuilt tables
    for (auto &[table_name, table_string] : prebuilt_tables)
    {
        instance.prebuilt_tables_.try_emplace(std::move(table_name),
                                              std::move(table_string));
    }

    // Note: The Sequences table is a special prebuilt table that was previously
    // added in RegisterEngines(). It should be added separately if needed, or
    // engines can include it in their prebuilt_tables vector.

    // Store engine metrics
    for (auto &metric : engine_metrics)
    {
        instance.external_metrics_.push_back(std::move(metric));
    }

    instance.publish_func_ = publish_func;

    LOG(INFO) << "Engine " << static_cast<int>(engine_type)
              << " registered successfully";
    // Note: instance_mutex_ is released here when lock goes out of scope

    // Set engine_registered and notify waiting threads
    instance.engines_[engine_index].engine_registered = true;
    instance.engine_registration_cv_.notify_all();

    return true;
}

void DataSubstrate::EnableEngine(txservice::TableEngine engine_type)
{
    // Validate engine type
    if (engine_type == txservice::TableEngine::None ||
        engine_type < txservice::TableEngine::EloqSql ||
        engine_type > txservice::TableEngine::EloqDoc)
    {
        LOG(ERROR) << "Invalid engine type for EnableEngine: "
                   << static_cast<int>(engine_type);
        return;
    }

    // Find engine slot
    size_t engine_index = static_cast<size_t>(engine_type) - 1;
    if (engine_index >= NUM_EXTERNAL_ENGINES)
    {
        LOG(ERROR) << "Engine type out of range: "
                   << static_cast<int>(engine_type);
        return;
    }

    DataSubstrate &instance = Instance();
    std::lock_guard<std::mutex> reg_lock(instance.engine_registration_mutex_);
    instance.engines_[engine_index].enable_engine = true;
    instance.engines_[engine_index].engine_registered =
        false;  // Initialize to false
    instance.engines_[engine_index].engine = engine_type;
}

bool DataSubstrate::WaitForEnabledEnginesRegistered(
    std::chrono::milliseconds timeout)
{
    DataSubstrate &instance = Instance();
    std::unique_lock<std::mutex> lock(instance.engine_registration_mutex_);

    auto deadline = std::chrono::steady_clock::now() + timeout;

    while (true)
    {
        // Check if all enabled engines are registered
        bool all_registered = true;
        for (size_t i = 0; i < NUM_EXTERNAL_ENGINES; i++)
        {
            if (instance.engines_[i].enable_engine &&
                !instance.engines_[i].engine_registered)
            {
                all_registered = false;
                break;
            }
        }

        if (all_registered)
        {
            return true;
        }

        // Wait with timeout
        if (instance.engine_registration_cv_.wait_until(lock, deadline) ==
            std::cv_status::timeout)
        {
            // Timeout - check one more time
            all_registered = true;
            for (size_t i = 0; i < NUM_EXTERNAL_ENGINES; i++)
            {
                if (instance.engines_[i].enable_engine &&
                    !instance.engines_[i].engine_registered)
                {
                    all_registered = false;
                    break;
                }
            }
            return all_registered;
        }
    }
}

bool DataSubstrate::WaitForDataSubstrateStarted(
    std::chrono::milliseconds timeout)
{
    DataSubstrate &instance = Instance();
    std::unique_lock<std::mutex> lock(instance_mutex_);

    // Check if already started
    if (instance.init_state_ == InitState::Started)
    {
        return true;
    }

    // Wait for Start() to complete
    auto deadline = std::chrono::steady_clock::now() + timeout;
    bool started = instance.data_substrate_started_cv_.wait_until(
        lock,
        deadline,
        [&instance] { return instance.init_state_ == InitState::Started; });

    return started;
}

bool DataSubstrate::LoadNetworkConfig(bool is_bootstrap,
                                      const INIReader &config_reader,
                                      const std::string &default_data_path,
                                      NetworkConfig &network_config)
{
    // Load network and cluster configuration with gflags priority
    network_config.local_ip =
        !CheckCommandLineFlagIsDefault("tx_ip")
            ? FLAGS_tx_ip
            : config_reader.Get("local", "tx_ip", FLAGS_tx_ip);

    network_config.local_port =
        !CheckCommandLineFlagIsDefault("tx_port")
            ? FLAGS_tx_port
            : config_reader.GetInteger("local", "tx_port", FLAGS_tx_port);
    std::string local_ip_port = network_config.local_ip + ":" +
                                std::to_string(network_config.local_port);
    DLOG(INFO) << "Local tx service ip port: " << local_ip_port;

    if (is_bootstrap)
    {
        network_config.ip_list = local_ip_port;
    }
    else
    {
        network_config.ip_list =
            !CheckCommandLineFlagIsDefault("tx_ip_port_list")
                ? FLAGS_tx_ip_port_list
                : config_reader.Get(
                      "cluster", "tx_ip_port_list", local_ip_port);

        network_config.standby_ip_list =
            !CheckCommandLineFlagIsDefault("tx_standby_ip_port_list")
                ? FLAGS_tx_standby_ip_port_list
                : config_reader.Get("cluster", "tx_standby_ip_port_list", "");

        network_config.voter_ip_list =
            !CheckCommandLineFlagIsDefault("tx_voter_ip_port_list")
                ? FLAGS_tx_voter_ip_port_list
                : config_reader.Get("cluster", "tx_voter_ip_port_list", "");
    }
    network_config.cluster_config_file_path =
        !CheckCommandLineFlagIsDefault("cluster_config_file")
            ? FLAGS_cluster_config_file
            : config_reader.Get(
                  "cluster", "cluster_config_file", FLAGS_cluster_config_file);

    network_config.node_group_replica_num =
        !CheckCommandLineFlagIsDefault("node_group_replica_num")
            ? FLAGS_node_group_replica_num
            : config_reader.GetInteger("cluster",
                                       "node_group_replica_num",
                                       FLAGS_node_group_replica_num);

    network_config.bind_all =
        !CheckCommandLineFlagIsDefault("bind_all")
            ? FLAGS_bind_all
            : config_reader.GetBoolean("local", "bind_all", FLAGS_bind_all);

    // Try to read cluster config from file. Cluster config file is written by
    // host manager when cluster config updates.
    if (network_config.cluster_config_file_path.empty())
    {
        network_config.cluster_config_file_path =
            default_data_path + "/tx_service/cluster_config";
    }

    network_config.cluster_config_version = 2;
    if (!txservice::ReadClusterConfigFile(
            network_config.cluster_config_file_path,
            network_config.ng_configs,
            network_config.cluster_config_version))
    {
        // Read cluster topology from general config file in this case
        auto parse_res =
            txservice::ParseNgConfig(network_config.ip_list,
                                     network_config.standby_ip_list,
                                     network_config.voter_ip_list,
                                     network_config.ng_configs,
                                     network_config.node_group_replica_num,
                                     0);
        if (!parse_res)
        {
            LOG(ERROR)
                << "Failed to extract cluster configs from tx_ip_port_list.";
            return false;
        }
    }

    // check whether this node is in cluster.
    bool found = false;
    network_config.node_id = 0;
    network_config.native_ng_id = 0;
    for (auto &pair : network_config.ng_configs)
    {
        auto &ng_nodes = pair.second;
        for (size_t i = 0; i < ng_nodes.size(); i++)
        {
            if (ng_nodes[i].host_name_ == network_config.local_ip &&
                ng_nodes[i].port_ == network_config.local_port)
            {
                network_config.node_id = ng_nodes[i].node_id_;
                found = true;
                if (ng_nodes[i].is_candidate_)
                {
                    // found native_ng_id.
                    network_config.native_ng_id = pair.first;
                    break;
                }
            }
        }
    }

    if (!found)
    {
        LOG(ERROR) << "!!!!!!!! Current node does not belong to the "
                      "cluster, startup is terminated !!!!!!!!";
        return false;
    }

    return true;
}

bool DataSubstrate::LoadCoreAndNetworkConfig(const INIReader &config_reader)
{
    // Load core data substrate configuration with gflags priority
    core_config_.data_path =
        !CheckCommandLineFlagIsDefault("eloq_data_path")
            ? FLAGS_eloq_data_path
            : config_reader.Get(
                  "local", "eloq_data_path", FLAGS_eloq_data_path);

    core_config_.enable_heap_defragment =
        !CheckCommandLineFlagIsDefault("enable_heap_defragment")
            ? FLAGS_enable_heap_defragment
            : config_reader.GetBoolean("local",
                                       "enable_heap_defragment",
                                       FLAGS_enable_heap_defragment);

    core_config_.enable_cache_replacement =
        !CheckCommandLineFlagIsDefault("enable_cache_replacement")
            ? FLAGS_enable_cache_replacement
            : config_reader.GetBoolean("local",
                                       "enable_cache_replacement",
                                       FLAGS_enable_cache_replacement);

    core_config_.maxclients =
        !CheckCommandLineFlagIsDefault("maxclients")
            ? FLAGS_maxclients
            : config_reader.GetInteger("local", "maxclients", FLAGS_maxclients);
    struct rlimit ulimit
    {
    };
    ulimit.rlim_cur = core_config_.maxclients;
    ulimit.rlim_max = core_config_.maxclients;
    if (setrlimit(RLIMIT_NOFILE, &ulimit) == -1)
    {
        LOG(WARNING) << "Failed to set maxclients.";
    }
    core_config_.bootstrap =
        !CheckCommandLineFlagIsDefault("bootstrap")
            ? FLAGS_bootstrap
            : config_reader.GetBoolean("local", "bootstrap", FLAGS_bootstrap);

    core_config_.enable_wal =
        !core_config_.bootstrap &&
        (!CheckCommandLineFlagIsDefault("enable_wal")
             ? FLAGS_enable_wal
             : config_reader.GetBoolean(
                   "local", "enable_wal", FLAGS_enable_wal));

    core_config_.enable_data_store =
        !CheckCommandLineFlagIsDefault("enable_data_store")
            ? FLAGS_enable_data_store
            : config_reader.GetBoolean(
                  "local", "enable_data_store", FLAGS_enable_data_store);
    if (core_config_.enable_wal && !core_config_.enable_data_store)
    {
        LOG(ERROR) << "When set enable_wal, should also set enable_data_store";
        return false;
    }

    if (!core_config_.enable_data_store &&
        core_config_.enable_cache_replacement)
    {
        LOG(WARNING) << "When set enable_cache_replacement, should also set "
                        "enable_data_store, reset to false";
        core_config_.enable_cache_replacement = false;
    }

    const char *field_core = "core_number";
    core_config_.core_num = FLAGS_core_number;
    if (CheckCommandLineFlagIsDefault(field_core))
    {
        if (config_reader.HasValue("local", field_core))
        {
            core_config_.core_num =
                config_reader.GetInteger("local", field_core, 0);
            assert(core_config_.core_num);
        }
        else
        {
            if (!NUM_VCPU)
            {
                LOG(ERROR) << "config is missing: " << field_core;
                return false;
            }
            core_config_.core_num = std::max(1u, (NUM_VCPU * 9) / 10);
            LOG(INFO) << "config is automatically set: " << field_core << "="
                      << core_config_.core_num << ", vcpu=" << NUM_VCPU;
        }
    }

    core_config_.enable_mvcc =
        !CheckCommandLineFlagIsDefault("enable_mvcc")
            ? FLAGS_enable_mvcc
            : config_reader.GetBoolean(
                  "local", "enable_mvcc", FLAGS_enable_mvcc);

    // Load network configuration
    if (!LoadNetworkConfig(core_config_.bootstrap,
                           config_reader,
                           core_config_.data_path,
                           network_config_))
    {
        LOG(ERROR) << "Failed to load network configuration.";
        return false;
    }

    // print out ng_configs
    LOG(INFO) << "Cluster Node Group Configurations:";
    for (auto &pair : network_config_.ng_configs)
    {
        DLOG(INFO) << "ng_id: " << pair.first;
        for (auto &node : pair.second)
        {
            DLOG(INFO) << "node_id: " << node.node_id_
                       << ", host_name: " << node.host_name_
                       << ", port: " << node.port_;
        }
    }

    const char *field_ed = "event_dispatcher_num";
    uint num_iothreads = brpc::FLAGS_event_dispatcher_num;
    if (CheckCommandLineFlagIsDefault(field_ed))
    {
        if (config_reader.HasValue("local", field_ed))
        {
            num_iothreads = config_reader.GetInteger("local", field_ed, 0);
            assert(num_iothreads);
        }
        else
        {
            if (!NUM_VCPU)
            {
                LOG(ERROR) << "config is missing: " << field_ed;
                return false;
            }
            num_iothreads = std::max(uint(1), (core_config_.core_num + 5) / 6);
            LOG(INFO) << "config is automatically set: " << field_ed << "="
                      << num_iothreads << ", vcpu=" << NUM_VCPU;
        }
    }
    GFLAGS_NAMESPACE::SetCommandLineOption(
        field_ed, std::to_string(num_iothreads).c_str());

    // Set bthread concurrency
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "bthread_concurrency", std::to_string(core_config_.core_num).c_str());
#ifdef ELOQ_MODULE_ENABLED
    GFLAGS_NAMESPACE::SetCommandLineOption("worker_polling_time_us", "100000");
    GFLAGS_NAMESPACE::SetCommandLineOption("brpc_worker_as_ext_processor",
                                           "true");
#endif
    GFLAGS_NAMESPACE::SetCommandLineOption("use_pthread_event_dispatcher",
                                           "true");
    GFLAGS_NAMESPACE::SetCommandLineOption("max_body_size", "536870912");
    GFLAGS_NAMESPACE::SetCommandLineOption("graceful_quit_on_sigterm", "true");
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "eloq_store_worker_num", std::to_string(core_config_.core_num).c_str());
#endif

    bool enable_io_uring =
        !CheckCommandLineFlagIsDefault("enable_io_uring")
            ? FLAGS_enable_io_uring
            : config_reader.GetBoolean(
                  "local", "enable_io_uring", FLAGS_enable_io_uring);

    bool raft_log_async_fsync =
        !CheckCommandLineFlagIsDefault("raft_log_async_fsync")
            ? FLAGS_raft_log_async_fsync
            : config_reader.GetBoolean(
                  "local", "raft_log_async_fsync", FLAGS_raft_log_async_fsync);
    if (raft_log_async_fsync && !enable_io_uring)
    {
        LOG(ERROR) << "Invalid config: when set `enable_io_uring`, "
                      "should also set `enable_io_uring`.";
        return false;
    }
    GFLAGS_NAMESPACE::SetCommandLineOption("use_io_uring",
                                           enable_io_uring ? "true" : "false");
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "raft_use_bthread_fsync", raft_log_async_fsync ? "true" : "false");
#if defined(DATA_STORE_TYPE_DYNAMODB) ||                 \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) || \
    defined(LOG_STATE_TYPE_RKDB_S3)
    aws_options_.httpOptions.installSigPipeHandler = true;
    aws_options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    if (!FLAGS_log_dir.empty())
    {
        static std::string logprefix = FLAGS_log_dir + std::string("/aws_sdk_");
        aws_options_.loggingOptions.defaultLogPrefix = logprefix.c_str();
    }
    Aws::InitAPI(aws_options_);
#endif

    return true;
}
