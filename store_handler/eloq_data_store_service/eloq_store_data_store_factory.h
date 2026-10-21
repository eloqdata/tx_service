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

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "data_store_factory.h"
#include "eloq_store_data_store.h"

namespace EloqDS
{
/**
 * @brief Makes the EloqStore-backed data stores, and owns the one EloqStore
 * they share.
 *
 * The process runs one EloqStore, with one buffer pool, fd budget and local
 * cache quota, and that store hosts every data shard this node leads: each
 * EloqStoreDataStore attaches its data shard to the store when the shard is
 * opened (EloqStore::AttachShard) and detaches it when the shard is closed.
 * The store starts with the first data store made and stops with the factory.
 */
class EloqStoreDataStoreFactory : public DataStoreFactory
{
public:
    explicit EloqStoreDataStoreFactory(EloqStoreConfig &&configs)
        : eloq_store_configs_(std::move(configs))
    {
    }

    ~EloqStoreDataStoreFactory() override
    {
        if (store_ != nullptr && !store_->IsStopped())
        {
            store_->Stop();
        }
    }

    std::unique_ptr<DataStore> CreateDataStore(
        bool create_if_missing,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true,
        int64_t term = 0) override
    {
        if (!EnsureStoreStarted())
        {
            return nullptr;
        }
        auto ds =
            std::make_unique<EloqStoreDataStore>(shard_id, data_store_service);
        ds->Initialize();
        if (start_db)
        {
            if (!ds->StartDB(term, shard_id))
            {
                return nullptr;
            }
        }
        return ds;
    }

    DataStoreFactoryType DataStoreType() const override
    {
        return DataStoreFactoryType::ELOQSTORE_FACTORY;
    }

    std::string GetStoragePath() const override
    {
        // EloqStore uses multiple paths, return first one or empty
        if (eloq_store_configs_.eloqstore_configs_.store_path.empty())
        {
            return "";
        }
        return eloq_store_configs_.eloqstore_configs_.store_path[0];
    }

    std::string GetS3BucketName() const override
    {
        return "";  // Not applicable for EloqStore
    }

    std::string GetS3ObjectPath() const override
    {
        return "";  // Not applicable for EloqStore
    }

    std::string GetS3Region() const override
    {
        return "";  // Not applicable for EloqStore
    }

    std::string GetS3EndpointUrl() const override
    {
        return "";  // Not applicable for EloqStore
    }

    std::string GetAwsAccessKeyId() const override
    {
        return "";  // Not applicable for EloqStore
    }

    std::string GetAwsSecretKey() const override
    {
        return "";  // Not applicable for EloqStore
    }

    uint64_t GetSstFileCacheSize() const override
    {
        return 0;  // Not applicable for EloqStore
    }

    bool IsCloudMode() const override
    {
        return !eloq_store_configs_.eloqstore_configs_.cloud_store_path.empty();
    }

    void UpdateStandbyMasterStorePaths(
        const std::vector<std::string> &store_paths,
        const std::vector<uint64_t> &store_path_weights)
    {
        eloq_store_configs_.eloqstore_configs_.standby_master_store_paths =
            store_paths;
        eloq_store_configs_.eloqstore_configs_
            .standby_master_store_path_weights = store_path_weights;
    }

    void UpdateStandbyMasterAddr(const std::string &standby_master_addr)
    {
        eloq_store_configs_.eloqstore_configs_.standby_master_addr =
            standby_master_addr;
    }

    void SetEnableLocalStandby(bool enable)
    {
        eloq_store_configs_.eloqstore_configs_.enable_local_standby = enable;
    }

    /**
     * @brief The one EloqStore every data shard's data store sends to.
     * @return The store; null until the first data store is made.
     */
    ::eloqstore::EloqStore *Store() const
    {
        return store_.get();
    }

private:
    /**
     * @brief Builds and starts the store on the first call (StartInstance:
     * the store paths are opened and the worker shards built, with no data
     * shard attached yet). The options are fixed then, including the standby
     * settings; later changes to them reach the running store through
     * EloqStoreDataStore::UpdateStandbyMaster*.
     * @return Whether the store is running.
     */
    bool EnsureStoreStarted();

    /**
     * @brief The store's partition filter: whether a partition belongs to a
     * data shard this node hosts, answered by that shard's data store
     * (EloqStoreDataStore::OwnsPartition) from its buckets.
     *
     * Called from the store's threads when a sweep (archive, reopen, branch,
     * prewarm) runs for a shard; a shard without a data store yet owns
     * nothing.
     *
     * @param ds_shard_id The data shard the sweep runs for.
     * @param table_ident The partition.
     * @return Whether the partition is the shard's.
     */
    bool OwnsPartition(::eloqstore::DsShardId ds_shard_id,
                       const ::eloqstore::TableIdent &table_ident) const;

    /**
     * @brief Lets a data shard's data store answer the filter for its id.
     * @param shard_id The data shard.
     * @param data_store Its data store; null to withdraw it.
     */
    void RegisterShardStore(uint32_t shard_id, EloqStoreDataStore *data_store);

    EloqStoreConfig eloq_store_configs_;
    /** Serializes the store's lazy start. */
    std::mutex store_mux_;
    std::unique_ptr<::eloqstore::EloqStore> store_{nullptr};
    /**
     * The data shards' data stores, for the partition filter; each registers
     * itself for its lifetime. Indexed by data shard id.
     */
    std::array<std::atomic<EloqStoreDataStore *>,
               DataStoreService::kMaxShardCount>
        shard_stores_{};

    friend class EloqStoreDataStore;
};
}  // namespace EloqDS
