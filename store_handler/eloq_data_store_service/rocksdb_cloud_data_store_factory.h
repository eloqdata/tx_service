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

#include <memory>
#include <string>

#include "data_store.h"
#include "data_store_factory.h"
#include "rocksdb_cloud_data_store.h"
#include "rocksdb_config.h"

namespace EloqDS
{

class RocksDBCloudDataStoreFactory : public DataStoreFactory
{
public:
    RocksDBCloudDataStoreFactory(
        const ::EloqDS::RocksDBConfig &config,
        const ::EloqDS::RocksDBCloudConfig &cloud_config,
        bool tx_enable_cache_replacement)
        : config_(config),
          cloud_config_(cloud_config),
          tx_enable_cache_replacement_(tx_enable_cache_replacement)
    {
    }

    std::unique_ptr<DataStore> CreateDataStore(
        bool create_if_missing,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true) override
    {
        // Add shard_id to object_path_ only for legacy configuration
        // When oss_url is configured, the user can include shard path in the
        // URL themselves
        auto shard_cloud_config = cloud_config_;
        if (!shard_cloud_config.IsOssUrlConfigured())
        {
            // Legacy configuration: append shard_id for shard isolation
            if (shard_cloud_config.object_path_.empty())
            {
                shard_cloud_config.object_path_ = "rocksdb_cloud_object_path/";
            }
            else if (shard_cloud_config.object_path_.back() != '/')
            {
                shard_cloud_config.object_path_.append("/");
            }
            shard_cloud_config.object_path_.append("ds_");
            shard_cloud_config.object_path_.append(std::to_string(shard_id));
        }
        else
        {
            if (shard_cloud_config.oss_url_.back() == '/')
            {
                shard_cloud_config.oss_url_.pop_back();
            }
            shard_cloud_config.oss_url_.append("/ds_");
            shard_cloud_config.oss_url_.append(std::to_string(shard_id));
        }

        auto ds = std::make_unique<RocksDBCloudDataStore>(
            shard_cloud_config,
            config_,
            create_if_missing,
            tx_enable_cache_replacement_,
            shard_id,
            data_store_service);

        ds->Initialize();

        if (start_db)
        {
            bool ret = ds->StartDB();
            if (!ret)
            {
                LOG(ERROR)
                    << "Failed to start db instance in data store service";
                return nullptr;
            }
        }
        return ds;
    }

    DataStoreFactoryType DataStoreType() const override
    {
        return DataStoreFactoryType::ROCKSDB_CLOUD_FACTORY;
    }

    std::string GetStoragePath() const override
    {
        return config_.storage_path_;
    }

    std::string GetS3BucketName() const override
    {
        return cloud_config_.bucket_prefix_ + cloud_config_.bucket_name_;
    }

    std::string GetS3ObjectPath() const override
    {
        return cloud_config_.object_path_;
    }

    std::string GetS3Region() const override
    {
        return cloud_config_.region_;
    }

    std::string GetS3EndpointUrl() const override
    {
        return cloud_config_.s3_endpoint_url_;
    }

    std::string GetAwsAccessKeyId() const override
    {
        return cloud_config_.aws_access_key_id_;
    }

    std::string GetAwsSecretKey() const override
    {
        return cloud_config_.aws_secret_key_;
    }

    uint64_t GetSstFileCacheSize() const override
    {
        return cloud_config_.sst_file_cache_size_;
    }

    bool IsOssUrlConfigured() const override
    {
        return cloud_config_.IsOssUrlConfigured();
    }

private:
    ::EloqDS::RocksDBConfig config_;
    ::EloqDS::RocksDBCloudConfig cloud_config_;
    bool tx_enable_cache_replacement_;
};

}  // namespace EloqDS
