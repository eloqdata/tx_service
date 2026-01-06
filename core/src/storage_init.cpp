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

#include <memory>

#include "INIReader.h"
#include "data_store_handler.h"
#include "data_substrate.h"
#include "kv_store.h"
#if ELOQDS
#include <filesystem>

#include "data_store_service_client.h"
#include "eloq_data_store_service/data_store_service.h"
#include "eloq_data_store_service/data_store_service_config.h"
#endif
#if defined(DATA_STORE_TYPE_ROCKSDB)
#include "store_handler/rocksdb_handler.h"
#endif

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) || \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
#include "eloq_data_store_service/rocksdb_cloud_data_store_factory.h"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
#include "eloq_data_store_service/rocksdb_data_store_factory.h"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
#include "eloq_data_store_service/eloq_store_data_store_factory.h"
#endif

#ifdef DATA_STORE_TYPE_DYNAMODB
#include "store_handler/dynamo_handler.h"
#endif

#ifdef DATA_STORE_TYPE_BIGTABLE
#include "store_handler/bigtable_handler.h"
#endif

#if defined(DATA_STORE_TYPE_DYNAMODB)
DEFINE_string(dynamodb_endpoint, "", "Endpoint of KvStore Dynamodb");
DEFINE_string(dynamodb_keyspace, "eloq", "KeySpace of Dynamodb KvStore");
DEFINE_string(dynamodb_region,
              "ap-northeast-1",
              "Region of the used trable in DynamoDB");
#endif

#ifdef DATA_STORE_TYPE_BIGTABLE
DEFINE_string(bigtable_project_id, "", "Project id of BigTable");
DEFINE_string(bigtable_instance_id, "", "Instance id of BigTable");
DEFINE_string(bigtable_keyspace, "eloq", "KeySpace of BigTable");
#endif
// aws_secret_key
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
DECLARE_string(aws_access_key_id);
DECLARE_string(aws_secret_key);
#elif defined(DATA_STORE_TYPE_DYNAMODB) || defined(LOG_STATE_TYPE_RKDB_S3)
DEFINE_string(aws_access_key_id, "", "AWS SDK access key id");
DEFINE_string(aws_secret_key, "", "AWS SDK secret key");
#endif

#if ELOQDS
DEFINE_string(eloq_dss_peer_node,
              "",
              "EloqDataStoreService peer node address. Used to fetch eloq-dss "
              "topology from a working eloq-dss server.");
DEFINE_string(eloq_dss_branch_name,
              "development",
              "Branch name of EloqDataStore");
DEFINE_string(eloq_dss_config_file_path,
              "",
              "EloqDataStoreService config file path. Used to load eloq-dss "
              "config from a file.");
#endif

DEFINE_uint32(snapshot_sync_worker_num, 0, "Snapshot sync worker num");

bool DataSubstrate::InitializeStorageHandler(const INIReader &config_reader)
{
    if (CheckCommandLineFlagIsDefault("snapshot_sync_worker_num") &&
        !config_reader.HasValue("store", "snapshot_sync_worker_num"))
    {
        FLAGS_snapshot_sync_worker_num =
            std::max(core_config_.core_num / 4, static_cast<uint32_t>(1));
    }
#if defined(DATA_STORE_TYPE_DYNAMODB)
    std::string dynamodb_endpoint =
        !CheckCommandLineFlagIsDefault("dynamodb_endpoint")
            ? FLAGS_dynamodb_endpoint
            : config_reader.GetString(
                  "store", "dynamodb_endpoint", FLAGS_dynamodb_endpoint);
    std::string dynamodb_keyspace =
        !CheckCommandLineFlagIsDefault("dynamodb_keyspace")
            ? FLAGS_dynamodb_keyspace
            : config_reader.GetString(
                  "store", "dynamodb_keyspace", FLAGS_dynamodb_keyspace);
    std::string dynamodb_region =
        !CheckCommandLineFlagIsDefault("dynamodb_region")
            ? FLAGS_dynamodb_region
            : config_reader.GetString(
                  "store", "dynamodb_region", FLAGS_dynamodb_region);
    std::string aws_access_key_id =
        !CheckCommandLineFlagIsDefault("aws_access_key_id")
            ? FLAGS_aws_access_key_id
            : config_reader.GetString(
                  "store", "aws_access_key_id", FLAGS_aws_access_key_id);
    std::string aws_secret_key =
        !CheckCommandLineFlagIsDefault("aws_secret_key")
            ? FLAGS_aws_secret_key
            : config_reader.GetString(
                  "store", "aws_secret_key", FLAGS_aws_secret_key);
    bool ddl_skip_kv = false;
    uint16_t worker_pool_size = core_num_ * 2;

    store_hd_ = std::make_unique<EloqDS::DynamoHandler>(dynamodb_keyspace,
                                                        dynamodb_endpoint,
                                                        dynamodb_region,
                                                        aws_access_key_id,
                                                        aws_secret_key,
                                                        core_config_.bootstrap,
                                                        ddl_skip_kv,
                                                        worker_pool_size,
                                                        false);

#elif defined(DATA_STORE_TYPE_BIGTABLE)
    std::string bigtable_keyspace =
        !CheckCommandLineFlagIsDefault("bigtable_keyspace")
            ? FLAGS_bigtable_keyspace
            : config_reader.GetString(
                  "store", "bigtable_keyspace", FLAGS_bigtable_keyspace);
    std::string bigtable_project_id =
        !CheckCommandLineFlagIsDefault("bigtable_project_id")
            ? FLAGS_bigtable_project_id
            : config_reader.GetString(
                  "store", "bigtable_project_id", FLAGS_bigtable_project_id);
    std::string bigtable_instance_id =
        !CheckCommandLineFlagIsDefault("bigtable_instance_id")
            ? FLAGS_bigtable_instance_id
            : config_reader.GetString(
                  "store", "bigtable_instance_id", FLAGS_bigtable_instance_id);
    bool ddl_skip_kv = false;
    store_hd_ =
        std::make_unique<EloqDS::BigTableHandler>(bigtable_keyspace,
                                                  bigtable_project_id,
                                                  bigtable_instance_id,
                                                  core_config_.bootstrap,
                                                  ddl_skip_kv);

#elif defined(DATA_STORE_TYPE_ROCKSDB)
    bool is_single_node = network_config_.IsSingleNode();

    EloqShare::RocksDBConfig rocksdb_config(config_reader,
                                            core_config_.data_path);

    store_hd_ = std::make_unique<EloqKV::RocksDBHandlerImpl>(
        rocksdb_config,
        true,  // for non shared storage, always pass
               // create_if_missing=true, since no confilcts will happen
               // under
        core_config_.enable_cache_replacement);

#elif ELOQDS
    NetworkConfig *network_config_ptr = &network_config_;
#if !defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    // if this is bootstrap, we need to get the complete network config,
    // instead of the local node only.
    NetworkConfig full_network_config;
    if (core_config_.bootstrap)
    {
        if (!LoadNetworkConfig(false,
                               config_reader,
                               core_config_.data_path,
                               full_network_config))
        {
            LOG(ERROR) << "Failed to load full network configuration for "
                          "eloq dss bootstrap.";
            return false;
        }
        network_config_ptr = &full_network_config;
    }
#endif

    bool is_single_node = network_config_ptr->IsSingleNode();

    std::string eloq_dss_peer_node =
        !CheckCommandLineFlagIsDefault("eloq_dss_peer_node")
            ? FLAGS_eloq_dss_peer_node
            : config_reader.GetString(
                  "store", "eloq_dss_peer_node", FLAGS_eloq_dss_peer_node);

    std::string eloq_dss_data_path = core_config_.data_path + "/eloq_dss";
    // Normalize to absolute path first
    if (!std::filesystem::path(eloq_dss_data_path).is_absolute())
    {
        try
        {
            eloq_dss_data_path =
                std::filesystem::absolute(eloq_dss_data_path).string();
            LOG(INFO) << "The absolute path of data store path is: "
                      << eloq_dss_data_path;
        }
        catch (const std::filesystem::filesystem_error &e)
        {
            LOG(ERROR) << "Failed to get absolute path for data store path ("
                       << eloq_dss_data_path << "), error: " << e.what();
            return false;
        }
    }
    if (!std::filesystem::exists(eloq_dss_data_path))
    {
        std::filesystem::create_directories(eloq_dss_data_path);
    }

    std::string dss_config_file_path = "";
    EloqDS::DataStoreServiceClusterManager ds_config;
    uint32_t dss_leader_id = EloqDS::UNKNOWN_DSS_LEADER_NODE_ID;

    // use tx node id as the dss node id
    // since they are deployed together
    uint32_t dss_node_id = network_config_ptr->node_id;
    if (core_config_.bootstrap || is_single_node)
    {
        dss_leader_id = network_config_ptr->node_id;
    }

    if (!eloq_dss_peer_node.empty())
    {
        ds_config.SetThisNode(network_config_ptr->local_ip,
                              EloqDS::DataStoreServiceClient::TxPort2DssPort(
                                  network_config_ptr->local_port));
        // Fetch ds topology from peer node
        if (!EloqDS::DataStoreService::FetchConfigFromPeer(eloq_dss_peer_node,
                                                           ds_config))
        {
            LOG(ERROR) << "Failed to fetch config from peer node: "
                       << eloq_dss_peer_node;
            return false;
        }
    }
    else
    {
        // Single node or bootstraping multi-node cluster
        std::unordered_map<uint32_t, uint32_t> ng_leaders;
        // If dss_leader_id is UNKNOWN_DSS_LEADER_NODE_ID, skip setting
        // ng_leaders
        if (dss_leader_id != EloqDS::UNKNOWN_DSS_LEADER_NODE_ID)
        {
            for (const auto &[ng_id, ng_config] :
                 network_config_ptr->ng_configs)
            {
                ng_leaders.emplace(ng_id, dss_leader_id);
            }
        }
        EloqDS::DataStoreServiceClient::TxConfigsToDssClusterConfig(
            dss_node_id, network_config_ptr->ng_configs, ng_leaders, ds_config);
    }
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) || \
    defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
    EloqDS::RocksDBConfig rocksdb_config(config_reader, eloq_dss_data_path);
    EloqDS::RocksDBCloudConfig rocksdb_cloud_config(config_reader);
    rocksdb_cloud_config.branch_name_ = FLAGS_eloq_dss_branch_name;
    auto ds_factory = std::make_unique<EloqDS::RocksDBCloudDataStoreFactory>(
        rocksdb_config,
        rocksdb_cloud_config,
        core_config_.enable_cache_replacement);

#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
    EloqDS::RocksDBConfig rocksdb_config(config_reader, eloq_dss_data_path);
    auto ds_factory = std::make_unique<EloqDS::RocksDBDataStoreFactory>(
        rocksdb_config, core_config_.enable_cache_replacement);

#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    EloqDS::EloqStoreConfig eloq_store_config(
        config_reader, eloq_dss_data_path, core_config_.node_memory_limit_mb);
    auto ds_factory = std::make_unique<EloqDS::EloqStoreDataStoreFactory>(
        std::move(eloq_store_config));
#endif

    data_store_service_ = std::make_unique<EloqDS::DataStoreService>(
        ds_config,
        dss_config_file_path,
        eloq_dss_data_path + "/DSMigrateLog",
        std::move(ds_factory));
    // setup local data store service
    bool ret = true;
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
    // For non shared storage like rocksdb,
    // we always set create_if_missing to true
    // since non conflicts will happen under
    // multi-node deployment.
    ret = data_store_service_->StartService(true);
#else
    ret = data_store_service_->StartService(
        (core_config_.bootstrap || is_single_node));
#endif
    if (!ret)
    {
        LOG(ERROR) << "Failed to start data store service";
        return false;
    }
    // setup data store service client
    txservice::CatalogFactory *catalog_factory[NUM_EXTERNAL_ENGINES] = {
        nullptr, nullptr, nullptr};

    for (size_t i = 0; i < NUM_EXTERNAL_ENGINES; i++)
    {
        catalog_factory[i] = engines_[i].catalog_factory;
    }

    store_hd_ = std::make_unique<EloqDS::DataStoreServiceClient>(
        core_config_.bootstrap || is_single_node,
        catalog_factory,
        ds_config,
        eloq_dss_peer_node.empty(),
        data_store_service_.get());

#endif

    for (const auto &[table_name, _] : prebuilt_tables_)
    {
        store_hd_->AppendPreBuiltTable(table_name);
    }

    if (!store_hd_->Connect())
    {
        LOG(ERROR) << "!!!!!!!! Failed to connect to kvstore, startup is "
                      "terminated !!!!!!!!";
        return false;
    }
    return true;
}
