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

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
namespace EloqDS
{

class DataStoreService;
class FetchRecordsRequest;
class WriteRecordsRequest;
class FlushDataRequest;
class DeleteRangeRequest;
class ReadRequest;
class CreateTableRequest;
class DropTableRequest;
class ScanRequest;
class CreateSnapshotForBackupRequest;

class DataStore
{
public:
    DataStore(uint32_t shard_id, DataStoreService *data_store_service)
        : shard_id_(shard_id), data_store_service_(data_store_service)
    {
    }

    virtual ~DataStore() = default;

    /**
     * @brief Initialize the data store.
     * @return True if connect successfully, otherwise false.
     */
    virtual bool Initialize() = 0;

    /**
     * @brief Start the data store.
     * @return True if start successfully, otherwise false.
     */
    virtual bool StartDB() = 0;

    /**
     * @brief Close the data store.
     */
    virtual void Shutdown() = 0;

    /**
     * @brief Read record from data store.
     * @param read_req The pointer of the request.
     */
    virtual void Read(ReadRequest *read_req) = 0;

    /**
     * @brief flush entries in \@param batch to base table or skindex table in
     * data store, stop and return false if node_group is not longer leader.
     * @param batch
     * @param table_name base table name or sk index name
     * @param table_schema
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual void BatchWriteRecords(WriteRecordsRequest *batch_write_req) = 0;

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param batch
     * to base table or skindex table in data store, stop and return false if
     * node_group is not longer leader.
     * @param flush_data_req The pointer of the request.
     */
    virtual void FlushData(FlushDataRequest *flush_data_req) = 0;

    /**
     * @brief Delete records in a range from data store.
     * @param delete_range_req The pointer of the request.
     */
    virtual void DeleteRange(DeleteRangeRequest *delete_range_req) = 0;

    /**
     * @brief Create kv table.
     * @param create_table_req The pointer of the request.
     */
    virtual void CreateTable(CreateTableRequest *create_table_req) = 0;

    /**
     * @brief Drop kv table.
     * @param drop_talbe_req The pointer of the request.
     */
    virtual void DropTable(DropTableRequest *drop_table_req) = 0;

    /**
     * @brief Fetch next scan result.
     * @param scan_req Scan request.
     */
    virtual void ScanNext(ScanRequest *scan_req) = 0;

    /**
     * @brief Close scan operation.
     * @param req_shard_id Requested shard id.
     */
    virtual void ScanClose(ScanRequest *scan_req) = 0;

    /**
     * @brief Create a snapshot of the data store.
     * @param req The pointer of the request.
     */
    virtual void CreateSnapshotForBackup(CreateSnapshotForBackupRequest *req) = 0;

    /**
     * @brief Switch the data store to read only mode.
     */
    virtual void SwitchToReadOnly() = 0;

    /**
     * @brief Switch the data store to read write mode.
     */
    virtual void SwitchToReadWrite() = 0;

protected:
    uint32_t shard_id_;
    DataStoreService *data_store_service_;
};

}  // namespace EloqDS
