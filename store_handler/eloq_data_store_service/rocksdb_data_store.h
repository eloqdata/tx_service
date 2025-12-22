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

#include <rocksdb/db.h>

#include <string>

#include "data_store.h"
#include "data_store_service.h"
#include "rocksdb_config.h"
#include "rocksdb_data_store_common.h"

namespace EloqDS
{
class RocksDBDataStore : public RocksDBDataStoreCommon
{
public:
    RocksDBDataStore(const EloqDS::RocksDBConfig &config,
                     bool create_if_missing,
                     bool tx_enable_cache_replacement,
                     uint32_t shard_id,
                     DataStoreService *data_store_service);

    ~RocksDBDataStore();

    /**
     * @brief Open the cloud database.
     * @param term The term value to use when starting the database.
     * @return True if open successfully, otherwise false.
     */
    bool StartDB(int64_t term) override;

    /**
     * @brief Close the cloud database.
     */
    void Shutdown() override;

    void CreateSnapshotForBackup(CreateSnapshotForBackupRequest *req) override
    {
        assert(false);
    }

protected:
    /**
     * @brief Get the RocksDB pointer.
     */
    rocksdb::DB *GetDBPtr() override;

private:
    rocksdb::DB *db_{nullptr};
};

}  // namespace EloqDS
