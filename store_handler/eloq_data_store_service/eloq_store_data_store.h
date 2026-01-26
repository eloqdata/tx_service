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

#include <unordered_map>
#include <vector>

#include "data_store.h"
#include "data_store_service.h"
#include "eloq_store.h"
#include "eloq_store_config.h"
#include "object_pool.h"

namespace EloqDS
{
class EloqStoreDataStore;

template <typename ReqT>
struct EloqStoreOperationData : public Poolable
{
    EloqStoreOperationData() = default;
    EloqStoreOperationData(const EloqStoreOperationData &rhs) = delete;
    EloqStoreOperationData(EloqStoreOperationData &&rhs) = delete;

    void Reset(Poolable *ds_req_ptr)
    {
        data_store_request_ptr_ = ds_req_ptr;
    }

    void Clear() override
    {
        data_store_request_ptr_->Clear();
        data_store_request_ptr_->Free();
        data_store_request_ptr_ = nullptr;
    }

    ReqT &EloqStoreRequest()
    {
        return eloq_store_request_;
    }

    Poolable *DataStoreRequest() const
    {
        return data_store_request_ptr_;
    }

private:
    ReqT eloq_store_request_;
    Poolable *data_store_request_ptr_{nullptr};
};

struct ScanDeleteOperationData : public Poolable
{
private:
    enum struct Stage
    {
        SCAN = 0,
        DELETE = 1,
    };

public:
    ScanDeleteOperationData() = default;
    ScanDeleteOperationData(const ScanDeleteOperationData &rhs) = delete;
    ScanDeleteOperationData(ScanDeleteOperationData &&rhs) = delete;

    void Reset(Poolable *ds_req_ptr, ::eloqstore::EloqStore *store)
    {
        data_store_request_ptr_ = ds_req_ptr;
        op_ts_ =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        op_stage_ = Stage::SCAN;
        eloq_store_ = store;
    }

    void Clear() override
    {
        data_store_request_ptr_->Clear();
        data_store_request_ptr_->Free();
        data_store_request_ptr_ = nullptr;
        entries_.clear();
    }

    ::eloqstore::ScanRequest &EloqStoreScanRequest()
    {
        return kv_scan_req_;
    }

    ::eloqstore::BatchWriteRequest &EloqStoreWriteRequest()
    {
        return kv_write_req_;
    }

    Poolable *DataStoreRequest() const
    {
        return data_store_request_ptr_;
    }

    const std::string &LastScanEndKey() const
    {
        return last_scan_end_key_;
    }

    void UpdateLastScanEndKey(const std::string &key_str, bool scan_drained)
    {
        last_scan_end_key_ = scan_drained ? "" : key_str;
    }

    uint64_t OpTs() const
    {
        return op_ts_;
    }

    void UpdateOperationStage(Stage next_stage)
    {
        op_stage_ = next_stage;
    }

    Stage OperationStage() const
    {
        return op_stage_;
    }

    ::eloqstore::EloqStore *EloqStoreService() const
    {
        return eloq_store_;
    }

private:
    ::eloqstore::ScanRequest kv_scan_req_;
    ::eloqstore::BatchWriteRequest kv_write_req_;
    Poolable *data_store_request_ptr_{nullptr};
    uint64_t op_ts_{0};
    std::vector<::eloqstore::WriteDataEntry> entries_;
    std::string last_scan_end_key_{""};
    Stage op_stage_{Stage::SCAN};
    ::eloqstore::EloqStore *eloq_store_{nullptr};

    friend class EloqStoreDataStore;
};

class EloqStoreDataStore : public DataStore
{
public:
    EloqStoreDataStore(uint32_t shard_id, DataStoreService *data_store_service);

    ~EloqStoreDataStore() override = default;

    bool Initialize() override;

    bool StartDB(int64_t term) override
    {
        ::eloqstore::KvError res = eloq_store_service_->Start(term);
        if (res != ::eloqstore::KvError::NoError)
        {
            LOG(ERROR) << "EloqStore start failed with error code: "
                       << static_cast<uint32_t>(res);
        }
        return res == ::eloqstore::KvError::NoError;
    }

    void Shutdown() override
    {
        eloq_store_service_->Stop();
    }

    /**
     * @brief Read record from data store.
     * @param read_req The pointer of the request.
     */
    void Read(ReadRequest *read_req) override;

    /**
     * @brief flush entries in \@param batch to base table or skindex table in
     * data store, stop and return false if node_group is not longer leader.
     * @param batch
     * @param table_name base table name or sk index name
     * @param table_schema
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    void BatchWriteRecords(WriteRecordsRequest *batch_write_req) override;

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param batch
     * to base table or skindex table in data store, stop and return false if
     * node_group is not longer leader.
     * @param flush_data_req The pointer of the request.
     */
    void FlushData(FlushDataRequest *flush_data_req) override;

    /**
     * @brief Delete records in a range from data store.
     * @param delete_range_req The pointer of the request.
     */
    void DeleteRange(DeleteRangeRequest *delete_range_req) override;

    /**
     * @brief Create kv table.
     * @param create_table_req The pointer of the request.
     */
    void CreateTable(CreateTableRequest *create_table_req) override;

    /**
     * @brief Drop kv table.
     * @param drop_talbe_req The pointer of the request.
     */
    void DropTable(DropTableRequest *drop_table_req) override;

    /**
     * @brief Fetch next scan result.
     * @param scan_req Scan request.
     */
    void ScanNext(ScanRequest *scan_req) override;

    /**
     * @brief Close scan operation.
     * @param req_shard_id Requested shard id.
     */
    void ScanClose(ScanRequest *scan_req) override;

    /**
     * @brief Switch the data store to read only mode.
     */
    void SwitchToReadOnly() override;

    /**
     * @brief Switch the data store to read write mode.
     */
    void SwitchToReadWrite() override;

    void CreateSnapshotForBackup(CreateSnapshotForBackupRequest *req) override;

#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    /**
     * @brief Standby node sync file cache operation.
     * Executes prewarm operation and cleans up files after completion.
     * Only implemented for EloqStoreDataStore.
     */
    void StandbySyncFileCache() override;

    /**
     * @brief Stop standby sync file cache operation.
     * Stops the ongoing prewarm operation gracefully.
     * Only implemented for EloqStoreDataStore.
     */
    void StopStandbySyncFileCache() override;
#endif

private:
    static void OnRead(::eloqstore::KvRequest *req);
    static void OnBatchWrite(::eloqstore::KvRequest *req);
    static void OnDropTable(::eloqstore::KvRequest *req);
    static void OnDeleteRange(::eloqstore::KvRequest *req);
    static void OnScanNext(::eloqstore::KvRequest *req);
    static void OnScanDelete(::eloqstore::KvRequest *req);
    static void OnFloor(::eloqstore::KvRequest *req);

    void ScanDelete(DeleteRangeRequest *delete_range_req);
    void Floor(ScanRequest *scan_req);

#if defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    /**
     * @brief Clean up non-data files after prewarm completion.
     * Deletes all non-data files (manifest, etc.) and the data file with
     * maximum file_id.
     */
    void CleanupNonDataFilesAfterPrewarm();
#endif

    std::unique_ptr<::eloqstore::EloqStore> eloq_store_service_{nullptr};
};
}  // namespace EloqDS
