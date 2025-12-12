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

namespace EloqDS
{

enum class DataStoreFactoryType
{
    ROCKSDB_FACTORY = 0,
    ROCKSDB_CLOUD_FACTORY = 2,
    ELOQSTORE_FACTORY = 3,
};

class DataStoreFactory
{
public:
    virtual ~DataStoreFactory() = default;

    virtual std::unique_ptr<DataStore> CreateDataStore(
        bool create_if_missing,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true) = 0;

    virtual DataStoreFactoryType DataStoreType() const = 0;

    /**
     * @brief Get storage path for the data store
     * @return Storage path string, empty if not applicable
     */
    virtual std::string GetStoragePath() const = 0;

    /**
     * @brief Get S3 bucket name (with prefix) for constructing S3 URLs
     * @return S3 bucket name with prefix, empty if not applicable
     */
    virtual std::string GetS3BucketName() const = 0;

    /**
     * @brief Get S3 object path for constructing S3 URLs
     * @return S3 object path, empty if not applicable
     */
    virtual std::string GetS3ObjectPath() const = 0;

    /**
     * @brief Get S3 region
     * @return S3 region, empty if not applicable
     */
    virtual std::string GetS3Region() const = 0;

    /**
     * @brief Get S3 endpoint URL (for custom endpoints like MinIO)
     * @return S3 endpoint URL, empty if using default AWS endpoint
     */
    virtual std::string GetS3EndpointUrl() const = 0;

    /**
     * @brief Get AWS access key ID for S3 authentication
     * @return AWS access key ID, empty if not applicable or using default
     * credentials
     */
    virtual std::string GetAwsAccessKeyId() const = 0;

    /**
     * @brief Get AWS secret key for S3 authentication
     * @return AWS secret key, empty if not applicable or using default
     * credentials
     */
    virtual std::string GetAwsSecretKey() const = 0;

    /**
     * @brief Get SST file cache size limit in bytes
     * @return Cache size limit in bytes, 0 if not applicable
     */
    virtual uint64_t GetSstFileCacheSize() const = 0;

    /**
     * @brief Check if S3 URL configuration is being used (instead of legacy config)
     * @return true if s3_url is configured, false otherwise
     */
    virtual bool IsS3UrlConfigured() const { return false; }
};

}  // namespace EloqDS
