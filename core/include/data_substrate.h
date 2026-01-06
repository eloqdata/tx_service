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

#include <gflags/gflags.h>

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#if (defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||  \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS) || \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB) ||           \
     defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE))
#define ELOQDS 1
#endif

// Log state type
#if !defined(LOG_STATE_TYPE_RKDB_CLOUD)

// Only if LOG_STATE_TYPE_RKDB_CLOUD undefined
#if ((defined(LOG_STATE_TYPE_RKDB_S3) || defined(LOG_STATE_TYPE_RKDB_GCS)) && \
     !defined(LOG_STATE_TYPE_RKDB))
#define LOG_STATE_TYPE_RKDB_CLOUD 1
#endif

#endif

#if !defined(LOG_STATE_TYPE_RKDB_ALL)

// Only if LOG_STATE_TYPE_RKDB_ALL undefined
#if (defined(LOG_STATE_TYPE_RKDB_S3) || defined(LOG_STATE_TYPE_RKDB_GCS) || \
     defined(LOG_STATE_TYPE_RKDB))
#define LOG_STATE_TYPE_RKDB_ALL 1
#endif

#endif

#if (defined(DATA_STORE_TYPE_DYNAMODB) || defined(LOG_STATE_TYPE_RKDB_S3) || \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3))
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#endif
#include "meter.h"
#include "metrics.h"
#include "type.h"

// Forward declaration for INIReader
class INIReader;

// Forward declarations
namespace txservice
{
class TxService;
struct NodeConfig;
class CatalogFactory;
class SystemHandler;
namespace store
{
class DataStoreHandler;
}
}  // namespace txservice

#ifdef ELOQDS
namespace EloqDS
{
class DataStoreService;
}  // namespace EloqDS
#endif

namespace txlog
{
class LogServer;
class LogAgent;
}  // namespace txlog

namespace metrics
{
class MetricsRegistry;
}  // namespace metrics

class DataSubstrate
{
public:
    enum class InitState
    {
        NotInitialized,
        ConfigLoaded,
        Started
    };

    struct EngineConfig
    {
        txservice::CatalogFactory *catalog_factory{nullptr};
        txservice::TableEngine engine{txservice::TableEngine::None};
        bool enable_engine{false};  // Set by main thread before engine init
        bool engine_registered{
            false};  // Set true when engine calls RegisterEngine()
    };

    // Core data substrate configuration
    struct CoreConfig
    {
        std::string data_path;
        uint32_t core_num;
        bool enable_heap_defragment;
        bool enable_wal;
        bool enable_data_store;
        bool bootstrap;
        bool enable_cache_replacement;
        bool enable_mvcc;
        uint32_t maxclients;
    };

    // Network and cluster configuration
    struct NetworkConfig
    {
        std::string local_ip;
        uint32_t local_port;
        std::string ip_list;
        std::string standby_ip_list;
        std::string voter_ip_list;
        uint32_t node_group_replica_num;
        bool bind_all;
        // Parsed service lists
        std::unordered_map<uint32_t, std::vector<txservice::NodeConfig>>
            ng_configs;
        uint64_t cluster_config_version;
        uint32_t node_id;
        uint32_t native_ng_id;
        std::string cluster_config_file_path;

        bool IsSingleNode() const
        {
            return (standby_ip_list.empty() && voter_ip_list.empty() &&
                    ip_list.find(',') == ip_list.npos);
        }
    };

    struct LogServiceConfig
    {
        uint32_t txlog_group_replica_num;
        std::vector<std::string> txlog_ips;
        std::vector<uint16_t> txlog_ports;
    };

    // New singleton access methods
    static DataSubstrate &Instance();
    static bool Init(const std::string &config_file_path);

    // Complete initialization by starting all services
    // Must be called after Init() and all engines have registered
    // Uses config file path saved during Init()
    bool Start();

    // Register an engine with data substrate
    // Returns true on success, false on failure (e.g., duplicate registration)
    bool RegisterEngine(
        txservice::TableEngine engine_type,
        txservice::CatalogFactory *factory,
        txservice::SystemHandler *system_handler,  // Optional, can be nullptr
        std::vector<std::pair<txservice::TableName, std::string>>
            &&prebuilt_tables,
        std::vector<std::tuple<metrics::Name,
                               metrics::Type,
                               std::vector<metrics::LabelGroup>>>
            &&engine_metrics,
        std::function<void(std::string_view, std::string_view)> publish_func =
            nullptr);

    // Called in main thread before starting engine init threads
    void EnableEngine(txservice::TableEngine engine_type);

    // Wait until all enabled engines have registered (or timeout)
    bool WaitForEnabledEnginesRegistered(std::chrono::milliseconds timeout);

    // Called by engine threads to wait until DataSubstrate has fully started
    bool WaitForDataSubstrateStarted(std::chrono::milliseconds timeout);

    // Shutdown DataSubstrate instance
    void Shutdown();

    // Component accessors
    txservice::TxService *GetTxService() const
    {
        return tx_service_.get();
    }
    txservice::store::DataStoreHandler *GetStoreHandler() const
    {
        return store_hd_.get();
    }
    txlog::LogServer *GetLogServer() const
    {
        return log_server_.get();
    }
    metrics::MetricsRegistry *GetMetricsRegistry() const
    {
        return metrics_registry_.get();
    }
    // Configuration accessors
    const CoreConfig &GetCoreConfig() const
    {
        return core_config_;
    }
    const NetworkConfig &GetNetworkConfig() const
    {
        return network_config_;
    }

    ~DataSubstrate();

private:
    // Make constructor private for singleton pattern
    DataSubstrate() = default;

    DataSubstrate(const DataSubstrate &) = delete;
    DataSubstrate &operator=(const DataSubstrate &) = delete;
    DataSubstrate(DataSubstrate &&) = delete;
    DataSubstrate &operator=(DataSubstrate &&) = delete;

    // Initialization state tracking
    InitState init_state_ = InitState::NotInitialized;

    // Config file path saved during Init() for use in Start()
    std::string config_file_path_;

    // Private methods that call component initialization files
    bool LoadNetworkConfig(bool is_bootstrap,
                           const INIReader &config_file_reader,
                           const std::string &default_data_path,
                           NetworkConfig &network_config);
    bool LoadCoreAndNetworkConfig(const INIReader &config_file_reader);
    bool InitializeStorageHandler(const INIReader &config_file_reader);
    bool InitializeLogService(const INIReader &config_file_reader);
    bool InitializeTxService(const INIReader &config_file_reader);
    bool InitializeMetrics(const INIReader &config_file_reader);

    uint32_t remaining_node_memory_mb_{};

    // Configuration storage
    CoreConfig core_config_;
    NetworkConfig network_config_;
    LogServiceConfig log_service_config_;
    metrics::CommonLabels tx_service_common_labels_{};
    std::vector<std::tuple<metrics::Name,
                           metrics::Type,
                           std::vector<metrics::LabelGroup>>>
        external_metrics_ = {};
    // TODO(liunyl): system handler is used to refresh auth related cache. In
    // converged db, there should only be one system handler.
    txservice::SystemHandler *system_handler_{nullptr};

    // This is for publish subscribe feature in eloqkv.
    std::function<void(std::string_view, std::string_view)> publish_func_{
        nullptr};

    // Component instances
    std::unique_ptr<txservice::TxService> tx_service_;
#if defined(DATA_STORE_TYPE_DYNAMODB) ||                 \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) || \
    defined(LOG_STATE_TYPE_RKDB_S3)
    Aws::SDKOptions aws_options_;
#endif

    std::unique_ptr<txservice::store::DataStoreHandler> store_hd_;
#ifdef ELOQDS
    std::unique_ptr<EloqDS::DataStoreService> data_store_service_;
#endif
    std::unique_ptr<txlog::LogServer> log_server_;
    std::unique_ptr<metrics::MetricsRegistry> metrics_registry_{nullptr};

    // Engine registry
    EngineConfig engines_[NUM_EXTERNAL_ENGINES];

    std::unordered_map<txservice::TableName, std::string> prebuilt_tables_;

    // Synchronization for engine registration (Option A: condition_variable)
    std::condition_variable engine_registration_cv_;
    std::mutex engine_registration_mutex_;

    // Synchronization for DataSubstrate::Start() completion (engines wait on
    // this) Uses instance_mutex_ and init_state_ instead of separate mutex/flag
    std::condition_variable data_substrate_started_cv_;
};

// Helper function to check if a gflag was set from command line
static inline bool CheckCommandLineFlagIsDefault(const char *name)
{
    gflags::CommandLineFlagInfo flag_info;
    bool flag_found = gflags::GetCommandLineFlagInfo(name, &flag_info);
    // Make sure the flag is declared.
    assert(flag_found);
    (void) flag_found;

    // Return `true` if the flag has the default value and has not been set
    // explicitly from the cmdline or via SetCommandLineOption
    return flag_info.is_default;
}
