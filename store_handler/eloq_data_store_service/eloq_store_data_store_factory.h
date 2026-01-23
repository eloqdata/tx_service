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
#include <unordered_set>
#include <vector>

#include "data_store_factory.h"
#include "eloq_store_data_store.h"

namespace EloqDS
{
class EloqStoreDataStoreFactory : public DataStoreFactory
{
public:
    explicit EloqStoreDataStoreFactory(EloqStoreConfig &&configs)
        : eloq_store_configs_(std::move(configs))
    {
    }

    std::unique_ptr<DataStore> CreateDataStore(
        bool create_if_missing,
        uint32_t shard_id,
        DataStoreService *data_store_service,
        bool start_db = true,
        int64_t term = 0) override
    {
        auto ds =
            std::make_unique<EloqStoreDataStore>(shard_id, data_store_service);
        ds->Initialize();
        if (start_db)
        {
            if (!ds->StartDB(term))
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

    void InitializePartitionFilter(
        uint32_t ng_id, std::unordered_set<uint16_t> &&bucket_ids) override
    {
        eloq_store_configs_.eloqstore_configs_.partition_filter =
            [bucket_ids = std::move(bucket_ids)](
                const eloqstore::TableIdent &table_ident) -> bool
        {
            const std::string &table_name = table_ident.tbl_name_;
            uint16_t bucket_id;
            if (table_name.compare(0, 8, "eloqdoc_") == 0 ||
                table_name.compare(0, 8, "eloqsql_") == 0 ||
                table_name.compare(0, 7, "irange_") == 0)
            {
                // this table engine use RangePartition.
                uint32_t hash_val;
                butil::MurmurHash3_x86_32(&table_ident.partition_id_,
                                          sizeof(table_ident.partition_id_),
                                          9001,
                                          &hash_val);
                bucket_id = hash_val % 1024;
            }
            else
            {
                // use HashPartition, hash_partition_id equals to bucket_id
                bucket_id = static_cast<uint16_t>(table_ident.partition_id_);
            }

            return bucket_ids.find(bucket_id) != bucket_ids.end();
        };
    }

private:
    EloqStoreConfig eloq_store_configs_;

    friend class EloqStoreDataStore;
};
}  // namespace EloqDS
