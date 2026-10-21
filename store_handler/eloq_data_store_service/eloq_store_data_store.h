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
        if (data_store_request_ptr_)
        {
            data_store_request_ptr_->Clear();
            data_store_request_ptr_->Free();
            data_store_request_ptr_ = nullptr;
        }
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
    struct ArchiveEntry
    {
        uint64_t term{0};
        std::string tag;
    };

    /**
     * @brief Makes the data shard's data store on the factory's store, which
     * must be running (EloqStoreDataStoreFactory::CreateDataStore).
     * @param shard_id The data shard.
     * @param data_store_service The service, which holds the factory.
     */
    EloqStoreDataStore(uint32_t shard_id, DataStoreService *data_store_service);

    ~EloqStoreDataStore() override;

    bool Initialize() override;

    /**
     * @brief Attaches the data shard to the store at its term
     * (EloqStore::AttachShard): its partitions are served in that term from
     * now on, in cloud mode after the shard's term file has been raised.
     *
     * @param term The data shard's term.
     * @param data_shard_id The data shard, the store's data store shard id.
     * @return Whether the shard is attached; false on ExpiredTerm, when a
     * newer term owns the shard.
     */
    bool StartDB(int64_t term, uint32_t data_shard_id) override
    {
        LOG(INFO) << "Attaching data shard " << data_shard_id
                  << " to the EloqStore at term " << term << ", owning "
                  << partition_buckets_.size()
                  << " buckets (0: every partition)";
        ::eloqstore::KvError res = store_->AttachShard(data_shard_id, term);
        if (res != ::eloqstore::KvError::NoError)
        {
            LOG(ERROR) << "Attaching data shard " << data_shard_id
                       << " to the EloqStore failed with error code: "
                       << static_cast<uint32_t>(res);
        }
        return res == ::eloqstore::KvError::NoError;
    }

    /**
     * @brief Detaches the data shard from the store (EloqStore::DetachShard):
     * its requests are refused, the ones in flight finish, and the store
     * drops what it holds for its partitions. The store itself keeps running
     * for the other data shards.
     */
    void Shutdown() override
    {
        ::eloqstore::KvError res = store_->DetachShard(shard_id_);
        if (res != ::eloqstore::KvError::NoError)
        {
            LOG(ERROR) << "Detaching data shard " << shard_id_
                       << " from the EloqStore failed with error code: "
                       << static_cast<uint32_t>(res);
        }
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

    /**
     * @brief Sets the buckets of this data shard, which the store's partition
     * filter checks for it (OwnsPartition).
     *
     * Every data shard's partitions share the store's storage: one cloud
     * store path for the cluster, and in local mode one store path per node.
     * So the operations that sweep it for a data shard (prewarm, archives for
     * backups and standby snapshots, branches, reopen) must pick that shard's
     * partitions. The set belongs to the data shard, not the process: the one
     * store hosts a shard for each node group the node leads.
     *
     * @param bucket_ids The data shard's buckets; empty accepts every
     * partition.
     * @warning Call only while the data shard is detached (Starting, before
     * StartDB), since the store reads the set without a lock once the shard
     * is attached.
     */
    void SetPartitionBuckets(
        const std::unordered_set<uint16_t> &bucket_ids) override;

    void CreateSnapshotForBackup(CreateSnapshotForBackupRequest *req) override;
    bool CreateSnapshotForStandby(uint32_t ng_id, std::string_view tag);
    void DeleteStandbySnapshot(uint64_t term, std::string_view tag) override;
    bool ListArchiveTags(std::string_view prefix,
                         std::vector<ArchiveEntry> *entries);

    bool ReloadData(int64_t term,
                    uint64_t snapshot_ts,
                    bool from_snapshot) override;

    void UpdateStandbyMasterStorePaths(
        const std::vector<std::string> &store_paths,
        const std::vector<uint64_t> &store_path_weights) override;
    void UpdateStandbyMasterAddr(
        const std::string &standby_master_addr) override;

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

    /**
     * @brief Whether a partition belongs to this data shard; the store's
     * partition filter asks the factory, which asks the shard's data store.
     *
     * Maps the partition to its bucket as the tx service does
     * (Sharder::MapRangeIdToBucketId for range-partitioned tables, the hash
     * partition itself for hash-partitioned ones) and looks the bucket up in
     * partition_buckets_.
     *
     * @param table_ident The partition.
     * @return true if it belongs to this data shard, or if no buckets are set.
     */
    bool OwnsPartition(const ::eloqstore::TableIdent &table_ident) const;

    /**
     * @brief Buckets of this data shard (SetPartitionBuckets); empty accepts
     * every partition.
     *
     * Written only while the data shard is detached and read by the store
     * through its partition filter while it is attached, so it needs no lock.
     */
    std::unordered_set<uint16_t> partition_buckets_;

    /**
     * The store every data shard's data store shares, owned by the factory,
     * which outlives this object's use of it (DataStoreService shuts the
     * data shards down before it destroys the factory).
     */
    ::eloqstore::EloqStore *store_{nullptr};

    friend class EloqStoreDataStoreFactory;
};
}  // namespace EloqDS
