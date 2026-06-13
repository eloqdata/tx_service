#pragma once

#include <memory>
#include <string>

#include "eloq_data_store_service/data_store_factory.h"
#include "mem_data_store.h"

namespace EloqDS
{
class MemDataStoreFactory : public DataStoreFactory
{
public:
    std::unique_ptr<DataStore> CreateDataStore(
        bool /*create_if_missing*/,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true,
        int64_t term = 0) override
    {
        auto ds = std::make_unique<MemDataStore>(shard_id, data_store_service);
        ds->Initialize();
        if (start_db)
        {
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
            ds->StartDB(term, shard_id);
#else
            ds->StartDB(term);
#endif
        }
        return ds;
    }

    DataStoreFactoryType DataStoreType() const override
    {
        return DataStoreFactoryType::ROCKSDB_FACTORY;
    }

    std::string GetStoragePath() const override
    {
        return "";
    }
    std::string GetS3BucketName() const override
    {
        return "";
    }
    std::string GetS3ObjectPath() const override
    {
        return "";
    }
    std::string GetS3Region() const override
    {
        return "";
    }
    std::string GetS3EndpointUrl() const override
    {
        return "";
    }
    std::string GetAwsAccessKeyId() const override
    {
        return "";
    }
    std::string GetAwsSecretKey() const override
    {
        return "";
    }
    uint64_t GetSstFileCacheSize() const override
    {
        return 0;
    }
    bool IsCloudMode() const override
    {
        return false;
    }
};
}  // namespace EloqDS
