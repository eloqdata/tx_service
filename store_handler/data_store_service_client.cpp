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
#include "data_store_service_client.h"

#include <glog/logging.h>

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store_service_client_closure.h"
#include "data_store_service_scanner.h"
#include "eloq_data_store_service/object_pool.h"  // ObjectPool
#include "eloq_data_store_service/thread_worker_pool.h"
#include "metrics.h"
#include "store_util.h"  // host_to_big_endian
#include "tx_service/include/cc/local_cc_shards.h"
#include "tx_service/include/error_messages.h"
#include "tx_service/include/sequences/sequences.h"

namespace EloqDS
{

thread_local ObjectPool<BatchWriteRecordsClosure> batch_write_closure_pool_;
thread_local ObjectPool<FlushDataClosure> flush_data_closure_pool_;
thread_local ObjectPool<DeleteRangeClosure> delete_range_closure_pool_;
thread_local ObjectPool<ReadClosure> read_closure_pool_;
thread_local ObjectPool<DropTableClosure> drop_table_closure_pool_;
thread_local ObjectPool<ScanNextClosure> scan_next_closure_pool_;
thread_local ObjectPool<CreateSnapshotForBackupClosure>
    create_snapshot_for_backup_closure_pool_;
thread_local ObjectPool<CreateSnapshotForBackupCallbackData>
    create_snapshot_for_backup_callback_data_pool_;

thread_local ObjectPool<SyncCallbackData> sync_callback_data_pool_;
thread_local ObjectPool<FetchTableCallbackData> fetch_table_callback_data_pool_;
thread_local ObjectPool<FetchDatabaseCallbackData> fetch_db_callback_data_pool_;
thread_local ObjectPool<FetchAllDatabaseCallbackData>
    fetch_all_dbs_callback_data_pool_;
thread_local ObjectPool<DiscoverAllTableNamesCallbackData>
    discover_all_tables_callback_data_pool_;
thread_local ObjectPool<SyncPutAllData> sync_putall_data_pool_;
thread_local ObjectPool<SyncConcurrentRequest> sync_concurrent_request_pool_;
thread_local ObjectPool<PartitionFlushState> partition_flush_state_pool_;
thread_local ObjectPool<PartitionCallbackData> partition_callback_data_pool_;

static const uint64_t MAX_WRITE_BATCH_SIZE = 64 * 1024 * 1024;  // 64MB

static const std::string_view kv_table_catalogs_name("table_catalogs");
static const std::string_view kv_database_catalogs_name("db_catalogs");
static const std::string_view kv_range_table_name("table_ranges");
static const std::string_view kv_range_slices_table_name("table_range_slices");
static const std::string_view kv_last_range_id_name(
    "table_last_range_partition_id");
static const std::string_view kv_table_statistics_name("table_statistics");
static const std::string_view kv_table_statistics_version_name(
    "table_statistics_version");
static const std::string_view kv_mvcc_archive_name("mvcc_archives");
static const std::string_view KEY_SEPARATOR("\\");

DataStoreServiceClient::~DataStoreServiceClient()
{
    upsert_table_worker_.Shutdown();
}

/**
 * @brief Configures the data store service client with cluster manager
 * information.
 *
 * Initializes the client with cluster configuration including node hostnames
 * and ports. Logs all node information for debugging purposes and stores the
 * cluster manager reference for future use.
 *
 * @param cluster_manager Reference to the cluster manager containing shard and
 * node information.
 */
void DataStoreServiceClient::SetupConfig(
    const DataStoreServiceClusterManager &cluster_manager)
{
    assert(cluster_manager.GetShardCount() == 1);
    auto current_version =
        dss_topology_version_.load(std::memory_order_acquire);
    auto new_version = cluster_manager.GetTopologyVersion();
    if (current_version <= cluster_manager.GetTopologyVersion() &&
        dss_topology_version_.compare_exchange_strong(current_version,
                                                      new_version))
    {
        for (const auto &[_, group] : cluster_manager.GetAllShards())
        {
            for (const auto &node : group.nodes_)
            {
                LOG(INFO) << "Node Hostname: " << node.host_name_
                          << ", Port: " << node.port_;
            }
            // The first node is the owner of shard.
            assert(group.nodes_.size() > 0);
            while (!UpgradeShardVersion(group.shard_id_,
                                        group.version_,
                                        group.nodes_[0].host_name_,
                                        group.nodes_[0].port_))
            {
                LOG(INFO) << "UpgradeShardVersion failed, retry";
                bthread_usleep(1000000);
            }
            LOG(INFO) << "UpgradeShardVersion success, shard_id:"
                      << group.shard_id_ << ", version:" << group.version_;
        }
    }
}

/**
 * @brief Establishes connection to the data store service.
 *
 * Attempts to connect to the data store service with retry logic. Initializes
 * pre-built tables and retries up to 5 times with 1-second delays between
 * attempts. Returns true if connection succeeds, false otherwise.
 *
 * @return true if connection is successful, false if all retry attempts fail.
 */
bool DataStoreServiceClient::Connect()
{
    bool succeed = false;
    for (int retry = 1; retry <= 5 && !succeed; retry++)
    {
        if (!InitPreBuiltTables())
        {
            succeed = false;
            bthread_usleep(1000000);
        }
        else
        {
            succeed = true;
        }
    }
    return succeed;
}

/**
 * @brief Schedules timer-based tasks for the data store service.
 *
 * Currently not implemented. This method is a placeholder for future
 * timer-based functionality such as periodic cleanup, health checks, or
 * maintenance tasks. Will assert and log an error if called.
 */
void DataStoreServiceClient::ScheduleTimerTasks()
{
    LOG(ERROR) << "ScheduleTimerTasks not implemented";
    assert(false);
}

/**
 * @brief Batch-writes a set of flush tasks into KV tables using concurrent
 * partition processing.
 *
 * Processes the provided flush tasks grouped by table and partition, serializes
 * each record (object tables use raw encoded blobs; non-object tables encode
 * tx-records with unpack info), and issues batched PUT/DELETE operations via
 * BatchWriteRecords. The method uses a concurrent approach where different
 * partitions can flush simultaneously, but each partition maintains
 * serialization (only one request in-flight per partition at a time).
 *
 * Key features:
 * - Concurrent processing across different partitions
 * - Per-partition serialization to respect KV store constraints
 * - Automatic batching based on MAX_WRITE_BATCH_SIZE (64MB)
 * - Chained callbacks within each partition for sequential processing
 * - Global coordination to wait for all partitions to complete
 *
 * The function distinguishes hash- and range-partitioned tables, computes
 * per-partition batches, and updates per-record timestamps/TTLs and operation
 * types. On any partition-level error, the function logs the failure and
 * returns false.
 *
 * @param flush_task Mapping from KV table name to a vector of flush task
 *                   entries containing the records to write. Each entry's
 *                   data_sync_vec_ provides the sequence of records for that
 *                   flush task.
 * @return true if all partitions completed successfully; false if any partition
 *         reported an error.
 */
bool DataStoreServiceClient::PutAll(
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
        &flush_task)
{
    uint64_t now = txservice::LocalCcShards::ClockTsInMillseconds();

    // Process each table
    for (auto &[kv_table_name, entries] : flush_task)
    {
        auto &table_name = entries.front()->data_sync_task_->table_name_;

        // Group records by partition
        std::unordered_map<uint32_t, std::vector<std::pair<size_t, size_t>>>
            hash_partitions_map;
        std::unordered_map<uint32_t, std::vector<size_t>> range_partitions_map;

        size_t flush_task_entry_idx = 0;
        for (auto &entry : entries)
        {
            auto &batch = *entry->data_sync_vec_;
            if (batch.empty())
            {
                continue;
            }

            if (table_name.IsHashPartitioned())
            {
                for (size_t i = 0; i < batch.size(); ++i)
                {
                    int32_t kv_partition_id =
                        KvPartitionIdOf(batch[i].partition_id_, false);
                    auto [it, inserted] =
                        hash_partitions_map.try_emplace(kv_partition_id);
                    if (inserted)
                    {
                        it->second.reserve(batch.size() / 1024 * 2 *
                                           entries.size());
                    }
                    it->second.emplace_back(
                        std::make_pair(flush_task_entry_idx, i));
                }
            }
            else
            {
                // All records in the batch are in the same partition for range
                // table
                uint32_t parition_id =
                    KvPartitionIdOf(batch[0].partition_id_, true);
                auto [it, inserted] =
                    range_partitions_map.try_emplace(parition_id);
                it->second.emplace_back(flush_task_entry_idx);
            }
            flush_task_entry_idx++;
        }

        // Create global coordinator
        SyncPutAllData *sync_putall = sync_putall_data_pool_.NextObject();
        PoolableGuard sync_putall_guard(sync_putall);
        sync_putall->Reset();

        uint16_t parts_cnt_per_key = 1;
        uint16_t parts_cnt_per_record = table_name.IsObjectTable() ? 1 : 5;

        // Create partition states and prepare batches
        std::vector<PartitionCallbackData *> callback_data_list;

        // Process hash partitions
        for (auto &[partition_id, flush_recs] : hash_partitions_map)
        {
            auto partition_state = partition_flush_state_pool_.NextObject();
            partition_state->Reset(partition_id);
            auto callback_data = partition_callback_data_pool_.NextObject();
            callback_data->Reset(partition_state, sync_putall, kv_table_name);

            // Prepare batches for this partition
            PreparePartitionBatches(*partition_state,
                                    flush_recs,
                                    entries,
                                    table_name,
                                    parts_cnt_per_key,
                                    parts_cnt_per_record,
                                    now);

            sync_putall->partition_states_.push_back(partition_state);
            callback_data_list.push_back(callback_data);
        }

        // Process range partitions
        for (auto &[partition_id, flush_recs] : range_partitions_map)
        {
            auto partition_state = partition_flush_state_pool_.NextObject();
            partition_state->Reset(partition_id);
            auto callback_data = partition_callback_data_pool_.NextObject();
            callback_data->Reset(partition_state, sync_putall, kv_table_name);

            // Prepare batches for this partition
            PrepareRangePartitionBatches(*partition_state,
                                         flush_recs,
                                         entries,
                                         table_name,
                                         parts_cnt_per_key,
                                         parts_cnt_per_record,
                                         now);

            sync_putall->partition_states_.push_back(partition_state);
            callback_data_list.push_back(callback_data);
        }

        // Set up global coordinator
        sync_putall->total_partitions_ = sync_putall->partition_states_.size();

        // Start concurrent processing for each partition
        for (size_t i = 0; i < callback_data_list.size(); ++i)
        {
            auto *partition_state = sync_putall->partition_states_[i];
            auto *callback_data = callback_data_list[i];

            // Start the first batch for this partition
            auto &first_batch = callback_data->inflight_batch;
            if (partition_state->GetNextBatch(first_batch))
            {
                BatchWriteRecords(callback_data->table_name,
                                  partition_state->partition_id,
                                  std::move(first_batch.key_parts),
                                  std::move(first_batch.record_parts),
                                  std::move(first_batch.records_ts),
                                  std::move(first_batch.records_ttl),
                                  std::move(first_batch.op_types),
                                  true,  // skip_wal
                                  callback_data,
                                  PartitionBatchCallback,
                                  first_batch.parts_cnt_per_key,
                                  first_batch.parts_cnt_per_record);
            }
            else
            {
                // No batches for this partition, mark as completed
                sync_putall->OnPartitionCompleted();
            }
        }

        // Wait for all partitions to complete
        {
            std::unique_lock<bthread::Mutex> lk(sync_putall->mux_);
            while (sync_putall->completed_partitions_ <
                   sync_putall->total_partitions_)
            {
                sync_putall->cv_.wait(lk);
            }
        }

        // Check for errors
        for (auto &partition_state : sync_putall->partition_states_)
        {
            if (partition_state->IsFailed())
            {
                LOG(ERROR) << "PutAll failed for partition "
                           << partition_state->partition_id << " with error: "
                           << partition_state->result.error_msg();
                return false;
            }
        }

        for (auto &callback_data : callback_data_list)
        {
            callback_data->Clear();
            callback_data->Free();
        }
    }
    return true;
}

/**
 * @brief Persists data from specified KV tables to storage.
 *
 * Flushes data from the provided KV table names to persistent storage using
 * asynchronous flush operations. Waits for completion and returns
 * success/failure status. Logs warnings on failure and debug info on success.
 *
 * @param kv_table_names Vector of KV table names to persist.
 * @return true if all tables are persisted successfully, false if any operation
 * fails.
 */
bool DataStoreServiceClient::PersistKV(
    const std::vector<std::string> &kv_table_names)
{
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();

    FlushData(kv_table_names, callback_data, &SyncCallback);
    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "DataStoreHandler: Failed to do PersistKV. Error: "
                     << callback_data->Result().error_msg();
        return false;
    }
    DLOG(INFO) << "DataStoreHandler::PersistKV success.";

    return true;
}

/**
 * @brief Upserts table schema information to the data store.
 *
 * Handles table creation, modification, and deletion operations by updating
 * table schema information in the data store. Validates leadership, processes
 * the operation asynchronously, and sets appropriate error codes on failure.
 * Supports various operation types including CREATE, DROP, and ALTER
 * operations.
 *
 * @param old_table_schema Pointer to the existing table schema (nullptr for
 * CREATE).
 * @param new_table_schema Pointer to the new table schema.
 * @param op_type Type of operation (CREATE, DROP, ALTER, etc.).
 * @param commit_ts Commit timestamp for the operation.
 * @param ng_id Node group ID for the operation.
 * @param tx_term Transaction term for consistency.
 * @param hd_res Handler result object to store operation outcome.
 * @param alter_table_info Information about table alterations (nullptr if not
 * applicable).
 * @param cc_req CC request base object.
 * @param ccs CC shard reference.
 * @param err_code Error code output parameter.
 */
void DataStoreServiceClient::UpsertTable(
    const txservice::TableSchema *old_table_schema,
    const txservice::TableSchema *new_table_schema,
    txservice::OperationType op_type,
    uint64_t commit_ts,
    txservice::NodeGroupId ng_id,
    int64_t tx_term,
    txservice::CcHandlerResult<txservice::Void> *hd_res,
    const txservice::AlterTableInfo *alter_table_info,
    txservice::CcRequestBase *cc_req,
    txservice::CcShard *ccs,
    txservice::CcErrorCode *err_code)
{
    int64_t leader_term =
        txservice::Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (leader_term < 0)
    {
        hd_res->SetError(txservice::CcErrorCode::TX_NODE_NOT_LEADER);
        return;
    }

    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id](void *)
        { txservice::Sharder::Instance().UnpinNodeGroupData(ng_id); });

    if (leader_term != tx_term)
    {
        hd_res->SetError(txservice::CcErrorCode::NG_TERM_CHANGED);
        return;
    }

    // Use old schema for drop table as the new schema would be null.
    UpsertTableData *table_data = new UpsertTableData(old_table_schema,
                                                      new_table_schema,
                                                      op_type,
                                                      commit_ts,
                                                      defer_unpin,
                                                      ng_id,
                                                      tx_term,
                                                      hd_res,
                                                      alter_table_info,
                                                      cc_req,
                                                      ccs,
                                                      err_code);

    upsert_table_worker_.SubmitWork([this, table_data]()
                                    { this->UpsertTable(table_data); });
}

/**
 * @brief Fetches table catalog information from the data store.
 *
 * Retrieves catalog information for the specified table by reading from the
 * KV table catalogs storage. Uses partition ID 0 and the catalog name as the
 * key. The operation is performed asynchronously with a callback for completion
 * handling.
 *
 * @param ccm_table_name The table name to fetch catalog information for.
 * @param fetch_cc Fetch catalog CC object to store the result and handle
 * completion.
 */
void DataStoreServiceClient::FetchTableCatalog(
    const txservice::TableName &ccm_table_name,
    txservice::FetchCatalogCc *fetch_cc)
{
    int32_t kv_partition_id = 0;
    std::string_view key = fetch_cc->CatalogName().StringView();
    Read(kv_table_catalogs_name,
         kv_partition_id,
         key,
         fetch_cc,
         &FetchTableCatalogCallback);
}

/**
 * @brief Fetches current table statistics from the data store.
 *
 * Retrieves the current version of table statistics for the specified table.
 * Determines the appropriate KV partition ID and reads from the table
 * statistics version storage. The operation is performed asynchronously with
 * callback handling.
 *
 * @param ccm_table_name The table name to fetch statistics for.
 * @param fetch_cc Fetch table statistics CC object to store the result and
 * handle completion.
 */
void DataStoreServiceClient::FetchCurrentTableStatistics(
    const txservice::TableName &ccm_table_name,
    txservice::FetchTableStatisticsCc *fetch_cc)
{
    std::string_view sv = ccm_table_name.StringView();
    fetch_cc->kv_partition_id_ = KvPartitionIdOf(ccm_table_name);

    fetch_cc->SetStoreHandler(this);
    Read(kv_table_statistics_version_name,
         fetch_cc->kv_partition_id_,
         sv,
         fetch_cc,
         &FetchCurrentTableStatsCallback);
}

/**
 * @brief Fetches table statistics for a specific version from the data store.
 *
 * Retrieves table statistics for a specific version by constructing key ranges
 * based on the table name and version number. Clears previous key ranges and
 * session information, then constructs start and end keys for the
 * version-specific statistics. The operation is performed asynchronously with
 * callback handling.
 *
 * @param ccm_table_name The table name to fetch statistics for.
 * @param fetch_cc Fetch table statistics CC object containing version
 * information and result storage.
 */
void DataStoreServiceClient::FetchTableStatistics(
    const txservice::TableName &ccm_table_name,
    txservice::FetchTableStatisticsCc *fetch_cc)
{
    fetch_cc->kv_start_key_.clear();
    fetch_cc->kv_end_key_.clear();
    fetch_cc->kv_session_id_.clear();

    uint64_t version = fetch_cc->CurrentVersion();
    uint64_t be_version = EloqShare::host_to_big_endian(version);
    fetch_cc->kv_start_key_.append(ccm_table_name.StringView());
    fetch_cc->kv_start_key_.append(reinterpret_cast<const char *>(&be_version),
                                   sizeof(uint64_t));
    fetch_cc->kv_end_key_ = fetch_cc->kv_start_key_;
    fetch_cc->kv_end_key_.back()++;

    fetch_cc->kv_partition_id_ = KvPartitionIdOf(ccm_table_name);

    // NOTICE: here batch_size is 1, because the size of item in
    // {kv_table_statistics_name} may be more than MAX_WRITE_BATCH_SIZE.
    ScanNext(kv_table_statistics_name,
             fetch_cc->kv_partition_id_,
             fetch_cc->kv_start_key_,
             fetch_cc->kv_end_key_,
             fetch_cc->kv_session_id_,
             false,
             false,
             true,
             1,
             nullptr,
             fetch_cc,
             &FetchTableStatsCallback);
}

// Each node group contains a sample pool, when write them to storage,
// we merge them together. The merged sample pool may be too large to store
// in one row. Therefore, we have to store table statistics segmentally.
//
// (1) We store sample keys of table statistics in
// {kv_table_statistics_name} table using the following format:
//
// segment_key: [table_name + version + segment_id + index_name];
// segment_record: [index_type + records_count + (key_size +
// key) + (key_size + key) + ... ];
//
// (2) We store the ckpt version of each table  statistics version in
// {kv_table_statistics_version_name} table using the following format:
//
// key: [table_name]; record: [ckpt_version];

std::string EncodeTableStatsKey(const txservice::TableName &base_table_name,
                                const txservice::TableName &index_name,
                                uint64_t version,
                                uint32_t segment_id)
{
    std::string key;
    std::string_view table_sv = base_table_name.StringView();
    std::string_view index_sv = index_name.StringView();
    uint64_t be_version = EloqShare::host_to_big_endian(version);
    uint32_t be_segment_id = EloqShare::host_to_big_endian(segment_id);

    key.reserve(table_sv.size() + sizeof(be_version) + sizeof(be_segment_id) +
                index_sv.size());

    key.append(table_sv);
    key.append(reinterpret_cast<const char *>(&be_version), sizeof(uint64_t));
    key.append(reinterpret_cast<const char *>(&be_segment_id),
               sizeof(uint32_t));
    key.append(index_sv);
    return key;
}

/**
 * @brief Upserts table statistics to the data store.
 *
 * Stores table statistics by splitting sample keys into segments and writing
 * them to the KV storage. Each segment contains index type, record count, and
 * sample keys. Also updates the checkpoint version for the table statistics.
 * Uses batch write operations for efficiency and handles both local and remote
 * storage paths.
 *
 * @param ccm_table_name The table name to store statistics for.
 * @param sample_pool_map Map of index names to sample pools containing record
 * counts and sample keys.
 * @param version The version number for the statistics.
 * @return true if all statistics are stored successfully, false if any
 * operation fails.
 */
bool DataStoreServiceClient::UpsertTableStatistics(
    const txservice::TableName &ccm_table_name,
    const std::unordered_map<txservice::TableName,
                             std::pair<uint64_t, std::vector<txservice::TxKey>>>
        &sample_pool_map,
    uint64_t version)
{
    // 1- split the sample keys into segments

    std::vector<std::string> segment_keys;
    std::vector<std::string> segment_records;

    for (const auto &[indexname, sample_pool] : sample_pool_map)
    {
        uint64_t records_count = sample_pool.first;
        auto &sample_keys = sample_pool.second;

        uint32_t segment_id = 0;
        std::string segment_key =
            EncodeTableStatsKey(ccm_table_name, indexname, version, segment_id);
        size_t batch_size = segment_key.size();

        std::string segment_record;
        segment_record.reserve(MAX_WRITE_BATCH_SIZE - batch_size);
        // index-type
        uint8_t index_type_int = static_cast<uint8_t>(indexname.Type());
        segment_record.append(reinterpret_cast<const char *>(&index_type_int),
                              sizeof(uint8_t));
        // records-count
        segment_record.append(reinterpret_cast<const char *>(&records_count),
                              sizeof(uint64_t));

        for (size_t i = 0; i < sample_keys.size(); ++i)
        {
            uint32_t key_size = sample_keys[i].Size();
            segment_record.append(reinterpret_cast<const char *>(&key_size),
                                  sizeof(uint32_t));
            batch_size += sizeof(uint32_t);
            segment_record.append(sample_keys[i].Data(), sample_keys[i].Size());
            batch_size += key_size;

            if (batch_size >= MAX_WRITE_BATCH_SIZE)
            {
                segment_keys.emplace_back(std::move(segment_key));
                segment_records.emplace_back(std::move(segment_record));
                // segment_size = 0;
                ++segment_id;

                segment_key = EncodeTableStatsKey(
                    ccm_table_name, indexname, version, segment_id);

                batch_size = segment_key.size();

                segment_record.clear();
                segment_record.reserve(MAX_WRITE_BATCH_SIZE - batch_size);
                // index-type
                uint8_t index_type_int = static_cast<uint8_t>(indexname.Type());
                segment_record.append(
                    reinterpret_cast<const char *>(&index_type_int),
                    sizeof(uint8_t));
                // records-count
                segment_record.append(
                    reinterpret_cast<const char *>(&records_count),
                    sizeof(uint64_t));
            }
        }

        if (segment_record.size() > 0)
        {
            segment_keys.emplace_back(std::move(segment_key));
            segment_records.emplace_back(std::move(segment_record));
        }
    }

    // 2- write the segments to storage
    int32_t kv_partition_id = KvPartitionIdOf(ccm_table_name);
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();

    for (size_t i = 0; i < segment_keys.size(); ++i)
    {
        keys.emplace_back(segment_keys[i]);
        records.emplace_back(segment_records[i]);
        records_ts.emplace_back(version);
        records_ttl.emplace_back(0);  // no ttl
        op_types.emplace_back(WriteOpType::PUT);

        // For segments are splitted based on MAX_WRITE_BATCH_SIZE, execute
        // one write request for each segment record.

        callback_data->Reset();
        BatchWriteRecords(kv_table_statistics_name,
                          kv_partition_id,
                          std::move(keys),
                          std::move(records),
                          std::move(records_ts),
                          std::move(records_ttl),
                          std::move(op_types),
                          true,
                          callback_data,
                          &SyncCallback);
        callback_data->Wait();

        if (callback_data->Result().error_code() !=
            EloqDS::remote::DataStoreError::NO_ERROR)
        {
            LOG(WARNING) << "UpdatetableStatistics: Failed to write segments.";

            return false;
        }
    }

    // 3- Update the ckpt version of the table statistics
    callback_data->Reset();
    keys.emplace_back(ccm_table_name.StringView());
    std::string version_str = std::to_string(version);
    records.emplace_back(version_str);
    records_ts.emplace_back(version);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);
    BatchWriteRecords(kv_table_statistics_version_name,
                      kv_partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      true,
                      callback_data,
                      &SyncCallback);
    callback_data->Wait();

    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "UpdatetableStatistics: Failed to write segments.";

        return false;
    }

    // 4- Delete old version data of the table statistics
    uint64_t version0 = 0;
    std::string start_key = ccm_table_name.String();
    // The big endian and small endian encoding of 0 is same.
    start_key.append(reinterpret_cast<const char *>(&version0),
                     sizeof(uint64_t));

    std::string end_key = ccm_table_name.String();
    uint64_t be_version = EloqShare::host_to_big_endian(version);
    end_key.append(reinterpret_cast<const char *>(&be_version),
                   sizeof(uint64_t));

    callback_data->Reset();
    DeleteRange(kv_table_statistics_name,
                kv_partition_id,
                start_key,
                end_key,
                true,
                callback_data,
                &SyncCallback);
    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "UpdatetableStatistics: Failed to write ckpt version.";
        return false;
    }

    return true;
}

/**
 * @brief Fetches table ranges from the data store.
 *
 * Retrieves range information for the specified table by scanning the range
 * table storage. Constructs start and end keys based on the table name and
 * performs a scan operation with pagination support. The operation is performed
 * asynchronously with callback handling for completion.
 *
 * @param fetch_cc Fetch table ranges CC object containing table name and result
 * storage.
 */
void DataStoreServiceClient::FetchTableRanges(
    txservice::FetchTableRangesCc *fetch_cc)
{
    fetch_cc->kv_partition_id_ = KvPartitionIdOf(fetch_cc->table_name_);

    fetch_cc->kv_start_key_ = fetch_cc->table_name_.String();
    fetch_cc->kv_end_key_ = fetch_cc->table_name_.String();
    fetch_cc->kv_end_key_.back()++;
    fetch_cc->kv_session_id_.clear();

    ScanNext(kv_range_table_name,
             fetch_cc->kv_partition_id_,
             fetch_cc->kv_start_key_,
             fetch_cc->kv_end_key_,
             fetch_cc->kv_session_id_,
             true,
             false,
             true,
             100,
             nullptr,
             fetch_cc,
             &FetchTableRangesCallback);
}

/**
 * @brief Fetches range slices from the data store.
 *
 * Retrieves range slice information for the specified table and range entry.
 * Validates node group term consistency and constructs the appropriate key
 * for reading range information. The operation is performed asynchronously
 * with callback handling for completion.
 *
 * @param fetch_cc Fetch range slices request object containing table name,
 * range entry, and result storage.
 */
void DataStoreServiceClient::FetchRangeSlices(
    txservice::FetchRangeSlicesReq *fetch_cc)
{
    // 1- fetch range info from {kv_range_table_name}
    // 2- fetch range slices from {kv_range_slices_table_name}

    if (txservice::Sharder::Instance().TryPinNodeGroupData(
            fetch_cc->cc_ng_id_) != fetch_cc->cc_ng_term_)
    {
        fetch_cc->SetFinish(txservice::CcErrorCode::NG_TERM_CHANGED);
        return;
    }
    fetch_cc->kv_partition_id_ = KvPartitionIdOf(fetch_cc->table_name_);
    // Also use segment_cnt to identify the step is fetch range or fetch slices.
    fetch_cc->SetSegmentCnt(0);

    txservice::TxKey start_key =
        fetch_cc->range_entry_->GetRangeInfo()->StartTxKey();

    auto catalog_factory = GetCatalogFactory(fetch_cc->table_name_.Engine());
    assert(catalog_factory != nullptr);
    fetch_cc->kv_start_key_ =
        EncodeRangeKey(catalog_factory, fetch_cc->table_name_, start_key);

    Read(kv_range_table_name,
         fetch_cc->kv_partition_id_,
         fetch_cc->kv_start_key_,
         fetch_cc,
         &FetchRangeSlicesCallback);
}

/**
 * @brief Deletes data that is out of the specified range.
 *
 * Removes data from the KV table that falls outside the specified range.
 * Constructs the appropriate start key based on the provided parameters and
 * performs a delete range operation. Handles special cases for negative
 * infinity keys and constructs proper key boundaries for the deletion.
 *
 * @param table_name The table name to delete data from.
 * @param partition_id The partition ID for the operation.
 * @param start_key The start key for the range (nullptr for negative infinity).
 * @param table_schema The table schema containing KV catalog information.
 * @return true if the deletion operation succeeds, false otherwise.
 */
bool DataStoreServiceClient::DeleteOutOfRangeData(
    const txservice::TableName &table_name,
    int32_t partition_id,
    const txservice::TxKey *start_key,
    const txservice::TableSchema *table_schema)
{
    const std::string &kv_table_name =
        table_schema->GetKVCatalogInfo()->GetKvTableName(table_name);
    std::string start_key_str;

    auto catalog_factory = GetCatalogFactory(table_name.Engine());
    assert(catalog_factory != nullptr);

    if (start_key->Type() == txservice::KeyType::NegativeInf)
    {
        const txservice::TxKey *neg_key =
            catalog_factory->PackedNegativeInfinity();
        start_key_str = std::string(neg_key->Data(), neg_key->Size());
    }
    else
    {
        start_key_str = std::string(start_key->Data(), start_key->Size());
    }

    std::string end_key_str = "";

    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();
    DeleteRange(kv_table_name,
                KvPartitionIdOf(partition_id, true),
                start_key_str,
                end_key_str,
                false,
                callback_data,
                &SyncCallback);
    callback_data->Wait();

    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DataStoreHandler: Failed to do DeleteOutOfRangeData. "
                   << callback_data->Result().error_msg();
        return false;
    }

    return true;
}

/**
 * @brief Reads a record from the data store synchronously.
 *
 * Currently not implemented. This method is a placeholder for synchronous
 * record reading functionality. Will log an error and return true.
 *
 * @param table_name The table name to read from.
 * @param key The key to read.
 * @param rec The record object to store the result.
 * @param found Output parameter indicating if the record was found.
 * @param version_ts Output parameter for the version timestamp.
 * @param table_schema The table schema information.
 * @return true (placeholder implementation).
 */
bool DataStoreServiceClient::Read(const txservice::TableName &table_name,
                                  const txservice::TxKey &key,
                                  txservice::TxRecord &rec,
                                  bool &found,
                                  uint64_t &version_ts,
                                  const txservice::TableSchema *table_schema)
{
    LOG(ERROR) << "Read not implemented";
    return true;
}

/**
 * @brief Creates a scanner for forward or backward scanning of table data.
 *
 * Creates and initializes a data store scanner for iterating over records in a
 * table. Supports both forward and backward scanning with configurable search
 * conditions. The scanner is initialized before returning.
 *
 * @param table_name The table name to scan.
 * @param ng_id Node group ID for the operation.
 * @param start_key The starting key for the scan.
 * @param inclusive Whether the start key should be included in the scan.
 * @param key_parts Number of key parts to consider.
 * @param search_cond Vector of search conditions for filtering results.
 * @param key_schema Schema information for the keys.
 * @param rec_schema Schema information for the records.
 * @param kv_info KV catalog information for the table.
 * @param scan_forward Whether to scan forward (true) or backward (false).
 * @return Unique pointer to the initialized scanner.
 */
std::unique_ptr<txservice::store::DataStoreScanner>
DataStoreServiceClient::ScanForward(
    const txservice::TableName &table_name,
    uint32_t ng_id,
    const txservice::TxKey &start_key,
    bool inclusive,
    uint8_t key_parts,
    const std::vector<txservice::store::DataStoreSearchCond> &search_cond,
    const txservice::KeySchema *key_schema,
    const txservice::RecordSchema *rec_schema,
    const txservice::KVCatalogInfo *kv_info,
    bool scan_forward)
{
    auto *catalog_factory = GetCatalogFactory(table_name.Engine());
    if (scan_forward)
    {
        auto scanner =
            std::make_unique<DataStoreServiceHashPartitionScanner<true>>(
                this,
                catalog_factory,
                key_schema,
                rec_schema,
                table_name,
                kv_info,
                start_key,
                inclusive,
                search_cond,
                100);

        // Call Init() before returning the scanner
        scanner->Init();

        return scanner;
    }
    else
    {
        auto scanner =
            std::make_unique<DataStoreServiceHashPartitionScanner<false>>(
                this,
                catalog_factory,
                key_schema,
                rec_schema,
                table_name,
                kv_info,
                start_key,
                inclusive,
                search_cond,
                100);

        // Call Init() before returning the scanner
        scanner->Init();

        return scanner;
    }
}

txservice::store::DataStoreHandler::DataStoreOpStatus
DataStoreServiceClient::LoadRangeSlice(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    uint32_t range_partition_id,
    txservice::FillStoreSliceCc *load_slice_req)
{
    int64_t leader_term = txservice::Sharder::Instance().TryPinNodeGroupData(
        load_slice_req->NodeGroup());
    if (leader_term < 0)
    {
        return txservice::store::DataStoreHandler::DataStoreOpStatus::Error;
    }
    // NOTICE: must unpin node group on calling load_slice_req->SetKvFinish().

    auto catalog_factory = GetCatalogFactory(table_name.Engine());
    assert(catalog_factory != nullptr);

    const txservice::TxKey &start_key = load_slice_req->StartKey();
    if (start_key.Type() == txservice::KeyType::NegativeInf)
    {
        const txservice::TxKey *neg_key =
            catalog_factory->PackedNegativeInfinity();
        load_slice_req->kv_start_key_ =
            std::string_view(neg_key->Data(), neg_key->Size());
    }
    else
    {
        load_slice_req->kv_start_key_ =
            std::string_view(start_key.Data(), start_key.Size());
    }

    const txservice::TxKey &end_key = load_slice_req->EndKey();
    if (end_key.Type() == txservice::KeyType::PositiveInf)
    {
        // end_key of empty string indicates the positive infinity in the
        // ScanNext
        load_slice_req->kv_end_key_ = "";
    }
    else
    {
        load_slice_req->kv_end_key_ =
            std::string_view(end_key.Data(), end_key.Size());
    }

    load_slice_req->kv_table_name_ = &(kv_info->GetKvTableName(table_name));
    load_slice_req->kv_partition_id_ =
        KvPartitionIdOf(range_partition_id, true);
    load_slice_req->kv_session_id_.clear();

    ScanNext(*load_slice_req->kv_table_name_,
             load_slice_req->kv_partition_id_,
             load_slice_req->kv_start_key_,
             load_slice_req->kv_end_key_,
             "",       // session_id
             true,     // include start_key
             false,    // include end_key
             true,     // scan forward
             1000,     // batch size
             nullptr,  // search condition
             load_slice_req,
             &LoadRangeSliceCallback);

    return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
}

// Range contains two parts info : range and slices.
// Then we store the range info and slices info in two tables.
//
// (1) We store range info in {kv_range_table_name} table using the
// following format:
//
// range_key: [table_name + range_start_key];
// range_record: [range_id + range_version + version +
//                  segment_cnt_of_slices]
//
// (2) We store slices info in {kv_range_slices_table_name} table.
// For each range contains much(about 16384) slices, to avoid a item too
// large, we store the range slices info segmentally.
//
// segment_key: [table_name + range_id + segment_id];
// segment_record: [version + (slice_key+slice_size) +
//                          (slice_key+slice_size) +...];
// Notice: segment_id starts from 0.

std::string DataStoreServiceClient::EncodeRangeKey(
    const txservice::CatalogFactory *catalog_factory,
    const txservice::TableName &table_name,
    const txservice::TxKey &range_start_key)
{
    std::string key;
    auto table_sv = table_name.StringView();
    key.reserve(table_sv.size() + range_start_key.Size());
    key.append(table_sv);
    if (range_start_key.Type() == txservice::KeyType::NegativeInf)
    {
        const txservice::TxKey *packed_neginf =
            catalog_factory->PackedNegativeInfinity();
        key.append(packed_neginf->Data(), packed_neginf->Size());
    }
    else
    {
        key.append(range_start_key.Data(), range_start_key.Size());
    }

    return key;
}

/**
 * @brief Encodes range information into a binary value format.
 *
 * Serializes range metadata including range ID, range version, general version,
 * and segment count into a binary string format for storage in the KV system.
 * Uses little-endian encoding for all numeric values.
 *
 * @param range_id The range identifier.
 * @param range_version The version of the range.
 * @param version The general version number.
 * @param segment_cnt The number of segments in the range.
 * @return Binary string containing the encoded range value.
 */
std::string DataStoreServiceClient::EncodeRangeValue(int32_t range_id,
                                                     uint64_t range_version,
                                                     uint64_t version,
                                                     uint32_t segment_cnt)
{
    std::string kv_range_record;
    kv_range_record.reserve(sizeof(int32_t) + sizeof(uint64_t) +
                            sizeof(uint64_t) + sizeof(uint32_t));
    kv_range_record.append(reinterpret_cast<const char *>(&range_id),
                           sizeof(int32_t));
    kv_range_record.append(reinterpret_cast<const char *>(&range_version),
                           sizeof(uint64_t));
    kv_range_record.append(reinterpret_cast<const char *>(&version),
                           sizeof(uint64_t));
    // segment_cnt of slices
    kv_range_record.append(reinterpret_cast<const char *>(&segment_cnt),
                           sizeof(uint32_t));
    return kv_range_record;
}

/**
 * @brief Encodes a range slice key for storage in the KV system.
 *
 * Creates a composite key by combining table name, range ID, and segment ID.
 * Uses little-endian encoding for numeric values since range slice operations
 * are point reads rather than scans, optimizing for direct key lookup
 * performance.
 *
 * @param table_name The table name for the range slice.
 * @param range_id The range identifier.
 * @param segment_id The segment identifier within the range.
 * @return Binary string containing the encoded range slice key.
 */
std::string DataStoreServiceClient::EncodeRangeSliceKey(
    const txservice::TableName &table_name,
    int32_t range_id,
    uint32_t segment_id)
{
    std::string key;
    auto table_sv = table_name.StringView();
    key.reserve(table_sv.size() + sizeof(range_id) + sizeof(segment_id));
    key.append(table_sv);
    // Due to all read operations of range slices are point reads not scan,
    // we just small endian encoding value of range_id and segment_id instead of
    // big endian encoding.
    key.append(reinterpret_cast<const char *>(&range_id), sizeof(range_id));
    key.append(reinterpret_cast<const char *>(&segment_id), sizeof(segment_id));
    return key;
}

/**
 * @brief Updates the segment ID in an encoded range slice key.
 *
 * Modifies an existing range slice key by replacing the segment ID portion
 * with a new segment ID value. This is used for updating range slice keys
 * without recreating the entire key structure.
 *
 * @param range_slice_key The range slice key to update (modified in place).
 * @param new_segment_id The new segment ID to use.
 */
void DataStoreServiceClient::UpdateEncodedRangeSliceKey(
    std::string &range_slice_key, uint32_t new_segment_id)
{
    range_slice_key.replace(range_slice_key.size() - sizeof(new_segment_id),
                            sizeof(new_segment_id),
                            reinterpret_cast<const char *>(&new_segment_id),
                            sizeof(new_segment_id));
}

/**
 * @brief Updates range slices for a table partition.
 *
 * Stores range slice information by segmenting the slices into manageable
 * chunks and writing them to the KV storage system. Handles slice serialization
 * with proper key encoding and batch size management. Also updates the range
 * information with the new version and segment count. Uses both local and
 * remote storage paths based on configuration.
 *
 * @param table_name The table name for the range slices.
 * @param version The version number for the slices.
 * @param range_start_key The starting key for the range.
 * @param slices Vector of store slices to update.
 * @param partition_id The partition ID for the range.
 * @param range_version The version of the range.
 * @return true if all slices are updated successfully, false if any operation
 * fails.
 */
bool DataStoreServiceClient::UpdateRangeSlices(
    const txservice::TableName &table_name,
    uint64_t version,
    txservice::TxKey range_start_key,
    std::vector<const txservice::StoreSlice *> slices,
    int32_t partition_id,
    uint64_t range_version)
{
    auto catalog_factory = GetCatalogFactory(table_name.Engine());
    assert(catalog_factory != nullptr);

    // 1- store range_slices info into {kv_range_slices_table_name}
    std::vector<std::string> segment_keys;
    std::vector<std::string> segment_records;
    uint32_t segment_cnt = 0;

    std::string segment_key =
        EncodeRangeSliceKey(table_name, partition_id, segment_cnt);
    std::string segment_record;
    size_t batch_size = segment_key.size() + sizeof(uint64_t);
    size_t max_segment_size = 1024 * 1024;
    segment_record.reserve(max_segment_size - segment_key.size());
    segment_record.append(reinterpret_cast<const char *>(&version),
                          sizeof(uint64_t));
    batch_size += sizeof(uint64_t);

    for (size_t i = 0; i < slices.size(); ++i)
    {
        txservice::TxKey slice_start_key = slices[i]->StartTxKey();
        if (slice_start_key.Type() == txservice::KeyType::NegativeInf)
        {
            slice_start_key =
                catalog_factory->PackedNegativeInfinity()->GetShallowCopy();
        }
        uint32_t key_size = static_cast<uint32_t>(slice_start_key.Size());
        batch_size += sizeof(uint32_t);
        batch_size += key_size;

        if (batch_size >= max_segment_size)
        {
            segment_keys.emplace_back(std::move(segment_key));
            segment_records.emplace_back(std::move(segment_record));

            segment_cnt++;
            segment_key =
                EncodeRangeSliceKey(table_name, partition_id, segment_cnt);
            batch_size = segment_key.size();

            segment_record.clear();
            segment_record.reserve(max_segment_size - segment_key.size());
            segment_record.append(reinterpret_cast<const char *>(&version),
                                  sizeof(uint64_t));
            batch_size += sizeof(uint64_t);
        }

        segment_record.append(reinterpret_cast<const char *>(&key_size),
                              sizeof(uint32_t));
        segment_record.append(slice_start_key.Data(), key_size);
        uint32_t slice_size = static_cast<uint32_t>(slices[i]->Size());
        segment_record.append(reinterpret_cast<const char *>(&slice_size),
                              sizeof(uint32_t));
    }
    if (segment_record.size() > 0)
    {
        segment_keys.emplace_back(std::move(segment_key));
        segment_records.emplace_back(std::move(segment_record));
        segment_cnt++;
    }

    assert(segment_keys.size() == segment_cnt);

    // 2- write the segments to storage
    // Calculate kv_partition_id based on table_name.
    int32_t kv_partition_id = KvPartitionIdOf(table_name);
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();

    for (size_t i = 0; i < segment_keys.size(); ++i)
    {
        keys.emplace_back(segment_keys[i]);
        records.emplace_back(segment_records[i]);
        records_ts.emplace_back(version);
        records_ttl.emplace_back(0);  // no ttl
        op_types.emplace_back(WriteOpType::PUT);

        // For segments are splitted based on MAX_WRITE_BATCH_SIZE, execute
        // one write request for each segment record.
        callback_data->Reset();
        BatchWriteRecords(kv_range_slices_table_name,
                          kv_partition_id,
                          std::move(keys),
                          std::move(records),
                          std::move(records_ts),
                          std::move(records_ttl),
                          std::move(op_types),
                          true,
                          callback_data,
                          &SyncCallback);
        callback_data->Wait();
        keys.clear();
        records.clear();
        records_ts.clear();
        records_ttl.clear();
        op_types.clear();

        if (callback_data->Result().error_code() !=
            EloqDS::remote::DataStoreError::NO_ERROR)
        {
            LOG(WARNING) << "UpdateRangeSlices: Failed to write segments.";
            return false;
        }
    }

    // 3- store range info into {kv_range_table_name}
    callback_data->Reset();

    std::string key_str =
        EncodeRangeKey(catalog_factory, table_name, range_start_key);
    std::string rec_str =
        EncodeRangeValue(partition_id, range_version, version, segment_cnt);

    keys.emplace_back(key_str);
    records.emplace_back(rec_str);

    records_ts.emplace_back(version);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);
    BatchWriteRecords(kv_range_table_name,
                      kv_partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      true,
                      callback_data,
                      &SyncCallback);
    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "UpdateRangeSlices: Failed to write range info.";
        return false;
    }

    return true;
}

/**
 * @brief Upserts range information for a table.
 *
 * Updates range slices for multiple ranges by calling UpdateRangeSlices for
 * each range in the provided vector. After updating all ranges, flushes the
 * range table data to ensure persistence. Validates that the table name is not
 * empty and handles errors from individual range updates.
 *
 * @param table_name The table name for the ranges.
 * @param range_info Vector of split range information to upsert.
 * @param version The version number for the ranges.
 * @return true if all ranges are updated and flushed successfully, false if any
 * operation fails.
 */
bool DataStoreServiceClient::UpsertRanges(
    const txservice::TableName &table_name,
    std::vector<txservice::SplitRangeInfo> range_info,
    uint64_t version)
{
    assert(table_name.StringView() != txservice::empty_sv);

    for (auto &range : range_info)
    {
        if (!UpdateRangeSlices(table_name,
                               version,
                               std::move(range.start_key_),
                               std::move(range.slices_),
                               range.partition_id_,
                               version))
        {
            return false;
        }
    }

    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();
    std::vector<std::string> kv_range_table_names;
    kv_range_table_names.emplace_back(kv_range_table_name);
    FlushData(kv_range_table_names, callback_data, &SyncCallback);
    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "UpsertRanges: Failed to flush ranges. Error: "
                     << callback_data->Result().error_msg();
        return false;
    }

    return true;
}

/**
 * @brief Fetches table schema information synchronously.
 *
 * Retrieves table schema information from the data store using asynchronous
 * operations with synchronous waiting. Uses FetchTableCatalog internally and
 * waits for completion before returning the result. Provides schema image,
 * found status, and version timestamp.
 *
 * @param table_name The table name to fetch schema for.
 * @param schema_image Output parameter for the schema image data.
 * @param found Output parameter indicating if the table was found.
 * @param version_ts Output parameter for the version timestamp.
 * @return true if the fetch operation completes successfully, false otherwise.
 */
bool DataStoreServiceClient::FetchTable(const txservice::TableName &table_name,
                                        std::string &schema_image,
                                        bool &found,
                                        uint64_t &version_ts)
{
    FetchTableCallbackData *callback_data =
        fetch_table_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset(schema_image, found, version_ts);
    Read(kv_table_catalogs_name,
         0,
         table_name.StringView(),
         callback_data,
         &FetchTableCallback);
    callback_data->Wait();

    if (callback_data->HasError())
    {
        LOG(WARNING) << "FetchTable error: "
                     << callback_data->Result().error_msg();
    }

    return !callback_data->HasError();
}

/**
 * @brief Discovers all table names in the data store.
 *
 * Scans the table catalogs to discover all available table names. Uses
 * pagination with session management and supports cooperative scheduling
 * through yield/resume function pointers. Performs the scan asynchronously and
 * waits for completion.
 *
 * @param norm_name_vec Output vector to store the discovered table names.
 * @param yield_fptr Optional function pointer for yielding control during
 * pagination.
 * @param resume_fptr Optional function pointer for resuming after yielding.
 * @return true if the discovery operation completes successfully, false if any
 * error occurs.
 */
bool DataStoreServiceClient::DiscoverAllTableNames(
    std::vector<std::string> &norm_name_vec,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr)
{
    DiscoverAllTableNamesCallbackData *callback_data =
        discover_all_tables_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset(norm_name_vec, yield_fptr, resume_fptr);

    ScanNext(kv_table_catalogs_name,
             0,  // kv_partition_id
             "",
             "",
             callback_data->session_id_,
             false,
             false,
             true,
             10,
             nullptr,
             callback_data,
             &DiscoverAllTableNamesCallback);
    callback_data->Wait();

    return !callback_data->HasError();
}

/**
 * @brief Upserts database definition to the data store.
 *
 * Stores database definition information in the KV storage system. The storage
 * format uses the database name as the key and the database definition as the
 * value. Uses current timestamp for versioning and performs the operation
 * asynchronously with synchronous waiting for completion.
 *
 * @param db The database name to upsert.
 * @param definition The database definition to store.
 * @return true if the database is upserted successfully, false if any operation
 * fails.
 */
bool DataStoreServiceClient::UpsertDatabase(std::string_view db,
                                            std::string_view definition)
{
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();
    uint64_t now =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();

    keys.emplace_back(db);
    records.emplace_back(definition);
    records_ts.emplace_back(now);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);

    BatchWriteRecords(kv_database_catalogs_name,
                      0,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      callback_data,
                      &SyncCallback);
    callback_data->Wait();

    if (callback_data->Result().error_code() !=
        remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "UpsertDatabase failed, error:"
                   << callback_data->Result().error_msg();
        return false;
    }

    return true;
}

/**
 * @brief Drops a database from the data store.
 *
 * Removes a database definition from the KV storage system by performing a
 * DELETE operation on the database catalog. Uses current timestamp for
 * versioning and performs the operation asynchronously with synchronous waiting
 * for completion.
 *
 * @param db The database name to drop.
 * @return true if the database is dropped successfully, false if any operation
 * fails.
 */
bool DataStoreServiceClient::DropDatabase(std::string_view db)
{
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();
    uint64_t now =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();

    keys.emplace_back(db);
    records.emplace_back(std::string_view());
    records_ts.emplace_back(now);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::DELETE);

    BatchWriteRecords(kv_database_catalogs_name,
                      0,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      callback_data,
                      &SyncCallback);
    callback_data->Wait();

    if (callback_data->Result().error_code() !=
        remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DropDatabase failed, error:"
                   << callback_data->Result().error_msg();
        return false;
    }

    return true;
}

/**
 * @brief Fetches database definition from the data store.
 *
 * Retrieves database definition information from the KV storage system.
 * Supports cooperative scheduling through yield/resume function pointers
 * and performs the operation asynchronously with synchronous waiting.
 *
 * @param db The database name to fetch.
 * @param definition Output parameter for the database definition.
 * @param found Output parameter indicating if the database was found.
 * @param yield_fptr Optional function pointer for yielding control.
 * @param resume_fptr Optional function pointer for resuming after yielding.
 * @return true if the fetch operation completes successfully, false if any
 * error occurs.
 */
bool DataStoreServiceClient::FetchDatabase(
    std::string_view db,
    std::string &definition,
    bool &found,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr)
{
    FetchDatabaseCallbackData *callback_data =
        fetch_db_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset(definition, found, yield_fptr, resume_fptr);
    Read(kv_database_catalogs_name,
         0,
         db,
         callback_data,
         &FetchDatabaseCallback);
    callback_data->Wait();

    return !callback_data->HasError();
}

bool DataStoreServiceClient::FetchAllDatabase(
    std::vector<std::string> &dbnames,
    const std::function<void()> *yield_fptr,
    const std::function<void()> *resume_fptr)
{
    FetchAllDatabaseCallbackData *callback_data =
        fetch_all_dbs_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset(dbnames, yield_fptr, resume_fptr);

    ScanNext(kv_database_catalogs_name,
             0,
             callback_data->start_key_,
             callback_data->end_key_,
             callback_data->session_id_,
             false,
             false,
             true,
             100,
             nullptr,
             callback_data,
             &FetchAllDatabaseCallback);
    callback_data->Wait();

    return !callback_data->HasError();
}

bool DataStoreServiceClient::DropKvTable(const std::string &kv_table_name)
{
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();
    DropTable(std::string_view(kv_table_name.data(), kv_table_name.size()),
              callback_data,
              &SyncCallback);
    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "DataStoreHandler: Failed to do DropKvTable.";
        return false;
    }

    return true;
}

// NOTICE: this function is not atomic
void DataStoreServiceClient::DropKvTableAsync(const std::string &kv_table_name)
{
    // FIXME(lzx): this function may not be used now.
    assert(false);

    AsyncDropTableCallbackData *callback_data =
        new AsyncDropTableCallbackData();
    callback_data->kv_table_name_ = kv_table_name;
    DropTable(std::string_view(callback_data->kv_table_name_.data(),
                               callback_data->kv_table_name_.size()),
              callback_data,
              &AsyncDropTableCallback);
}

std::string DataStoreServiceClient::CreateKVCatalogInfo(
    const txservice::TableSchema *table_schema) const
{
    boost::uuids::random_generator generator;

    txservice::KVCatalogInfo kv_info;
    kv_info.kv_table_name_ =
        std::string("t").append(boost::lexical_cast<std::string>(generator()));

    std::vector<txservice::TableName> index_names = table_schema->IndexNames();
    for (auto idx_it = index_names.begin(); idx_it < index_names.end();
         ++idx_it)
    {
        if (idx_it->Type() == txservice::TableType::Secondary)
        {
            kv_info.kv_index_names_.emplace(
                *idx_it,
                std::string("i").append(
                    boost::lexical_cast<std::string>(generator())));
        }
        else
        {
            assert((idx_it->Type() == txservice::TableType::UniqueSecondary));
            kv_info.kv_index_names_.emplace(
                *idx_it,
                std::string("u").append(
                    boost::lexical_cast<std::string>(generator())));
        }
    }
    return kv_info.Serialize();
}

txservice::KVCatalogInfo::uptr DataStoreServiceClient::DeserializeKVCatalogInfo(
    const std::string &kv_info_str, size_t &offset) const
{
    txservice::KVCatalogInfo::uptr kv_info =
        std::make_unique<txservice::KVCatalogInfo>();
    kv_info->Deserialize(kv_info_str.data(), offset);
    return kv_info;
}

std::string DataStoreServiceClient::CreateNewKVCatalogInfo(
    const txservice::TableName &table_name,
    const txservice::TableSchema *current_table_schema,
    txservice::AlterTableInfo &alter_table_info)
{
    // Get current kv catalog info.
    const txservice::KVCatalogInfo *current_kv_catalog_info =
        static_cast<const txservice::KVCatalogInfo *>(
            current_table_schema->GetKVCatalogInfo());

    std::string new_kv_info, kv_table_name, new_kv_index_names;

    /* kv table name using current table name */
    kv_table_name = current_kv_catalog_info->kv_table_name_;
    uint32_t kv_val_len = kv_table_name.length();
    new_kv_info
        .append(reinterpret_cast<char *>(&kv_val_len), sizeof(kv_val_len))
        .append(kv_table_name.data(), kv_val_len);

    /* kv index names using new schema index names */
    // 1. remove dropped index kv name
    bool dropped = false;
    for (auto kv_index_it = current_kv_catalog_info->kv_index_names_.cbegin();
         kv_index_it != current_kv_catalog_info->kv_index_names_.cend();
         ++kv_index_it)
    {
        // Check if the index will be dropped.
        dropped = false;
        for (auto drop_index_it = alter_table_info.index_drop_names_.cbegin();
             alter_table_info.index_drop_count_ > 0 &&
             drop_index_it != alter_table_info.index_drop_names_.cend();
             drop_index_it++)
        {
            if (kv_index_it->first == drop_index_it->first)
            {
                dropped = true;
                // Remove dropped index
                alter_table_info.index_drop_names_[kv_index_it->first] =
                    kv_index_it->second;
                break;
            }
        }
        if (!dropped)
        {
            new_kv_index_names.append(kv_index_it->first.String())
                .append(" ")
                .append(kv_index_it->second)
                .append(" ")
                .append(1, static_cast<char>(kv_index_it->first.Engine()))
                .append(" ");
        }
    }
    assert(alter_table_info.index_drop_names_.size() ==
           alter_table_info.index_drop_count_);

    // 2. add new index
    boost::uuids::random_generator generator;
    for (auto add_index_it = alter_table_info.index_add_names_.cbegin();
         alter_table_info.index_add_count_ > 0 &&
         add_index_it != alter_table_info.index_add_names_.cend();
         add_index_it++)
    {
        // get index kv table name
        std::string add_index_kv_name;
        if (add_index_it->first.Type() == txservice::TableType::Secondary)
        {
            add_index_kv_name = std::string("i").append(
                boost::lexical_cast<std::string>(generator()));
        }
        else
        {
            assert(add_index_it->first.Type() ==
                   txservice::TableType::UniqueSecondary);
            add_index_kv_name = std::string("u").append(
                boost::lexical_cast<std::string>(generator()));
        }

        new_kv_index_names.append(add_index_it->first.String())
            .append(" ")
            .append(add_index_kv_name.data())
            .append(" ")
            .append(1, static_cast<char>(add_index_it->first.Engine()))
            .append(" ");

        // set index kv table name
        alter_table_info.index_add_names_[add_index_it->first] =
            add_index_kv_name;
    }
    assert(alter_table_info.index_add_names_.size() ==
           alter_table_info.index_add_count_);

    /* create final new kv info */
    kv_val_len = new_kv_index_names.size();
    new_kv_info
        .append(reinterpret_cast<char *>(&kv_val_len), sizeof(kv_val_len))
        .append(new_kv_index_names.data(), kv_val_len);

    return new_kv_info;
}

uint32_t DataStoreServiceClient::HashArchiveKey(
    const std::string &kv_table_name, const txservice::TxKey &tx_key)
{
    std::string_view tablename_sv =
        std::string_view(kv_table_name.data(), kv_table_name.size());
    size_t kv_table_name_hash = std::hash<std::string_view>()(tablename_sv);
    std::string_view key_sv = std::string_view(tx_key.Data(), tx_key.Size());
    size_t key_hash = std::hash<std::string_view>()(key_sv);
    uint32_t partition_id =
        (kv_table_name_hash ^ (key_hash << 1)) & 0x3FF;  // 1024 partitions
    return partition_id;
}

std::string DataStoreServiceClient::EncodeArchiveKey(
    std::string_view table_name, std::string_view key, uint64_t be_commit_ts)
{
    std::string archive_key;
    archive_key.reserve(table_name.size() + key.size() + KEY_SEPARATOR.size());
    archive_key.append(table_name);
    archive_key.append(KEY_SEPARATOR);
    archive_key.append(key);
    archive_key.append(KEY_SEPARATOR);
    archive_key.append(reinterpret_cast<const char *>(&be_commit_ts),
                       sizeof(uint64_t));
    return archive_key;
}

void DataStoreServiceClient::EncodeArchiveKey(
    std::string_view table_name,
    std::string_view key,
    uint64_t &be_commit_ts,
    std::vector<std::string_view> &keys,
    uint64_t &write_batch_size)
{
    keys.emplace_back(table_name);
    write_batch_size += table_name.size();

    keys.emplace_back(KEY_SEPARATOR);
    write_batch_size += KEY_SEPARATOR.size();

    keys.emplace_back(key);
    write_batch_size += key.size();

    keys.emplace_back(KEY_SEPARATOR);
    write_batch_size += KEY_SEPARATOR.size();

    keys.emplace_back(reinterpret_cast<const char *>(&be_commit_ts),
                      sizeof(uint64_t));
    write_batch_size += sizeof(uint64_t);
}

/**
 * @brief Encodes archive value data for storage.
 *
 * Serializes archive value information including deletion status, unpack info,
 * and encoded blob data into record parts for batch writing. Handles both
 * deleted and non-deleted records with appropriate data encoding.
 *
 * @param is_deleted Whether the record is marked as deleted.
 * @param value Pointer to the transaction record (nullptr for deleted records).
 * @param unpack_info_size Size of the unpack info data.
 * @param encoded_blob_size Size of the encoded blob data.
 * @param record_parts Vector to store the encoded record parts.
 * @param write_batch_size Running total of batch size (updated in place).
 */
void DataStoreServiceClient::EncodeArchiveValue(
    bool is_deleted,
    const txservice::TxRecord *value,
    size_t &unpack_info_size,
    size_t &encoded_blob_size,
    std::vector<std::string_view> &record_parts,
    size_t &write_batch_size)
{
    static const bool deleted = true;
    static const bool not_deleted = false;
    if (is_deleted)
    {
        record_parts.emplace_back(reinterpret_cast<const char *>(&deleted),
                                  sizeof(bool));
        write_batch_size += sizeof(bool);

        record_parts.emplace_back(std::string_view());  // unpack_info_size
        record_parts.emplace_back(std::string_view());  // unpack_info_data
        record_parts.emplace_back(std::string_view());  // encoded_blob_size
        record_parts.emplace_back(std::string_view());  // encoded_blob_data
    }
    else
    {
        record_parts.emplace_back(reinterpret_cast<const char *>(&not_deleted),
                                  sizeof(bool));
        write_batch_size += sizeof(bool);

        // Here copy the similar logic as EloqRecord Serialize function
        // for best of performance.
        record_parts.emplace_back(
            std::string_view(reinterpret_cast<const char *>(&unpack_info_size),
                             sizeof(uint64_t)));
        write_batch_size += sizeof(uint64_t);

        record_parts.emplace_back(value->UnpackInfoData(),
                                  value->UnpackInfoSize());
        write_batch_size += value->UnpackInfoSize();

        record_parts.emplace_back(
            std::string_view(reinterpret_cast<const char *>(&encoded_blob_size),
                             sizeof(uint64_t)));
        write_batch_size += sizeof(uint64_t);

        record_parts.emplace_back(value->EncodedBlobData(),
                                  value->EncodedBlobSize());
        write_batch_size += value->EncodedBlobSize();
    }
}

void DataStoreServiceClient::DecodeArchiveValue(
    const std::string &archive_value, bool &is_deleted, size_t &value_offset)
{
    size_t pos = 0;
    is_deleted = *reinterpret_cast<const bool *>(archive_value.data() + pos);
    pos += sizeof(bool);
    value_offset = pos;
}

/**
 * @brief Writes multiple MVCC archive records to the MVCC archive KV table
 * using sequential batch processing.
 *
 * Groups archive entries from the provided flush tasks by archive partition,
 * serializes keys and values into batch write requests, and dispatches those
 * requests sequentially within each partition. Uses SyncConcurrentRequest for
 * global concurrency control to limit the total number of in-flight requests
 * across all partitions.
 *
 * Key features:
 * - Sequential processing within each partition to maintain ordering
 * - Global concurrency control with max_flying_write_count limit (32)
 * - Automatic batching based on MAX_WRITE_BATCH_SIZE (64MB)
 * - Flow control to prevent overwhelming the system
 *
 * The method waits for all dispatched batches for each partition to complete
 * before returning.
 *
 * Side effects:
 * - Commits serialized archive records to kv_mvcc_archive_name with a default
 * TTL of 1 day.
 * - Converts per-record commit timestamps to big-endian form as part of key
 * encoding (the in-memory commit_ts field of those records is mutated during
 * processing).
 *
 * @param flush_task Map from KV table name to a vector of FlushTaskEntry
 * pointers whose archive vectors contain the FlushRecord entries to write. Only
 * entries with non-empty archive vectors are processed.
 * @return true if all batches for all partitions completed successfully; false
 * if any batch failed (an error will be logged).
 */
bool DataStoreServiceClient::PutArchivesAll(
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
        &flush_task)
{
    std::unordered_map<
        uint32_t,
        std::vector<std::pair<std::string_view, txservice::FlushRecord *>>>
        partitions_map;
    for (auto &[kv_table_name, flush_task_entry] : flush_task)
    {
        for (auto &entry : flush_task_entry)
        {
            auto &archive_vec = *entry->archive_vec_;

            if (archive_vec.empty())
            {
                continue;
            }

            for (size_t i = 0; i < archive_vec.size(); ++i)
            {
                txservice::TxKey tx_key = archive_vec[i].Key();
                uint32_t partition_id =
                    HashArchiveKey(kv_table_name.data(), tx_key);
                auto [it, inserted] = partitions_map.try_emplace(
                    KvPartitionIdOf(partition_id, true));
                if (inserted)
                {
                    it->second.reserve(archive_vec.size() / 1024 * 2 *
                                       flush_task_entry.size() *
                                       flush_task.size());
                }
                it->second.emplace_back(kv_table_name, &archive_vec[i]);
            }
        }
    }

    // Send the batch request
    for (auto &[partition_id, archive_ptrs] : partitions_map)
    {
        std::vector<std::string_view> keys;
        std::vector<std::string_view> records;
        std::vector<uint64_t> records_ts;
        std::vector<uint64_t> records_ttl;
        std::vector<WriteOpType> op_types;
        // temporary storage for the records in between batch
        // for keeping record upack info and encoded blob sizes
        std::vector<size_t> record_tmp_mem_area;
        record_tmp_mem_area.resize(archive_ptrs.size() *
                                   2);  // unpack_info_size + encoded_blob_size
        size_t write_batch_size = 0;
        uint64_t now = txservice::LocalCcShards::ClockTsInMillseconds();
        const uint64_t archive_ttl =
            now +
            1000 * 60 * 60 * 24;  // default ttl is 1 day for archive record

        uint16_t parts_cnt_per_key = 5;
        uint16_t parts_cnt_per_record = 5;

        // Send the batch request
        SyncConcurrentRequest *sync_concurrent =
            sync_concurrent_request_pool_.NextObject();
        PoolableGuard guard(sync_concurrent);
        sync_concurrent->Reset();

        size_t recs_cnt = archive_ptrs.size();
        keys.reserve(recs_cnt * parts_cnt_per_key);
        records.reserve(recs_cnt * parts_cnt_per_record);
        records_ts.reserve(recs_cnt);
        records_ttl.reserve(recs_cnt);
        op_types.reserve(recs_cnt);

        for (size_t i = 0; i < archive_ptrs.size(); ++i)
        {
            // Start a new batch if done with current partition.
            if (write_batch_size >= MAX_WRITE_BATCH_SIZE)
            {
                // Wait for in-flight requests to decrease if limit reached
                {
                    std::unique_lock<bthread::Mutex> lk(sync_concurrent->mux_);
                    while (sync_concurrent->unfinished_request_cnt_ >=
                           SyncConcurrentRequest::max_flying_write_count)
                    {
                        sync_concurrent->cv_.wait(lk);
                    }
                    sync_concurrent->unfinished_request_cnt_++;
                }
                BatchWriteRecords(kv_mvcc_archive_name,
                                  partition_id,
                                  std::move(keys),
                                  std::move(records),
                                  std::move(records_ts),
                                  std::move(records_ttl),
                                  std::move(op_types),
                                  true,
                                  sync_concurrent,
                                  SyncConcurrentRequestCallback,
                                  parts_cnt_per_key,
                                  parts_cnt_per_record);
                keys.clear();
                records.clear();
                records_ts.clear();
                records_ttl.clear();
                op_types.clear();

                keys.reserve(recs_cnt * parts_cnt_per_key);
                records.reserve(recs_cnt * parts_cnt_per_record);
                records_ts.reserve(recs_cnt);
                records_ttl.reserve(recs_cnt);
                op_types.reserve(recs_cnt);
                write_batch_size = 0;
            }

            txservice::FlushRecord &ckpt_rec = *archive_ptrs[i].second;
            std::string_view kv_table_name = archive_ptrs[i].first;
            txservice::TxKey tx_key = ckpt_rec.Key();

            assert(
                ckpt_rec.payload_status_ == txservice::RecordStatus::Normal ||
                ckpt_rec.payload_status_ == txservice::RecordStatus::Deleted);

            records_ts.push_back(ckpt_rec.commit_ts_);
            write_batch_size += sizeof(uint64_t);  // commit_ts

            records_ttl.push_back(archive_ttl);
            write_batch_size += sizeof(uint64_t);  // ttl

            op_types.push_back(WriteOpType::PUT);
            write_batch_size += sizeof(WriteOpType);

            // Encode key
            // convert commit_ts to big endian
            ckpt_rec.commit_ts_ =
                EloqShare::host_to_big_endian(ckpt_rec.commit_ts_);
            EncodeArchiveKey(kv_table_name,
                             std::string_view(tx_key.Data(), tx_key.Size()),
                             ckpt_rec.commit_ts_,
                             keys,
                             write_batch_size);

            // Encode value
            const txservice::TxRecord *rec = ckpt_rec.Payload();
            std::string record_str;
            size_t &unpack_info_size = record_tmp_mem_area[i * 2];
            size_t &encode_blob_size = record_tmp_mem_area[i * 2 + 1];
            if (rec != nullptr)
            {
                unpack_info_size = rec->UnpackInfoSize();
                encode_blob_size = rec->EncodedBlobSize();
            }

            EncodeArchiveValue(
                ckpt_rec.payload_status_ == txservice::RecordStatus::Deleted,
                rec,
                unpack_info_size,
                encode_blob_size,
                records,
                write_batch_size);
        }

        // Send out the last batch of this partition
        if (keys.size() > 0)
        {
            BatchWriteRecords(kv_mvcc_archive_name,
                              partition_id,
                              std::move(keys),
                              std::move(records),
                              std::move(records_ts),
                              std::move(records_ttl),
                              std::move(op_types),
                              true,
                              sync_concurrent,
                              SyncConcurrentRequestCallback,
                              parts_cnt_per_key,
                              parts_cnt_per_record);
            keys.clear();
            records.clear();
            records_ts.clear();
            records_ttl.clear();
            op_types.clear();

            keys.reserve(recs_cnt * parts_cnt_per_key);
            records.reserve(recs_cnt * parts_cnt_per_record);
            records_ts.reserve(recs_cnt);
            records_ttl.reserve(recs_cnt);
            op_types.reserve(recs_cnt);
            write_batch_size = 0;
            {
                std::unique_lock<bthread::Mutex> lk(sync_concurrent->mux_);
                sync_concurrent->unfinished_request_cnt_++;
            }
        }

        // Wait the result.
        {
            std::unique_lock<bthread::Mutex> lk(sync_concurrent->mux_);
            sync_concurrent->all_request_started_ = true;
            while (sync_concurrent->unfinished_request_cnt_ != 0)
            {
                sync_concurrent->cv_.wait(lk);
            }
        }

        if (sync_concurrent->result_.error_code() !=
            remote::DataStoreError::NO_ERROR)
        {
            LOG(ERROR) << "PutArchivesAll failed for error: "
                       << sync_concurrent->result_.error_msg();
            return false;
        }
    }

    return true;
}

/**
 * @brief Copies base table data to archive storage.
 *
 * Reads base table records and copies them to archive storage with concurrent
 * read operations. Manages in-flight read count to control concurrency and
 * handles both hash and range partitioned tables. Uses archive-specific
 * encoding and TTL settings for the copied data.
 *
 * @param flush_task Map of table names to flush task entries containing base
 * records to copy.
 * @return true if all records are successfully copied to archive, false if any
 * operation fails.
 */
bool DataStoreServiceClient::CopyBaseToArchive(
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
        &flush_task)
{
    // Prepare for the copied base table data to be flushed to the archive table
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
        archive_flush_task;
    constexpr uint32_t MAX_FLYING_READ_COUNT = 100;
    for (auto &[base_kv_table_name, flush_task_entry] : flush_task)
    {
        auto &table_name =
            flush_task_entry.front()->data_sync_task_->table_name_;
        auto &table_schema = flush_task_entry.front()->table_schema_;

        auto *catalog_factory = GetCatalogFactory(table_name.Engine());
        assert(catalog_factory != nullptr);

        for (auto &entry : flush_task_entry)
        {
            auto &base_vec = *entry->mv_base_vec_;
            if (base_vec.empty())
            {
                continue;
            }

            // Prepare the call back datas for a batch
            std::unique_ptr<std::vector<txservice::FlushRecord>> archive_vec =
                std::make_unique<std::vector<txservice::FlushRecord>>();
            archive_vec->reserve(base_vec.size());
            size_t batch_size = 0;
            bthread::Mutex mtx;
            bthread::ConditionVariable cv;
            size_t flying_cnt = 0;
            int error_code = 0;
            std::vector<ReadBaseForArchiveCallbackData> callback_datas;
            callback_datas.reserve(base_vec.size());
            for (size_t i = 0; i < base_vec.size(); ++i)
            {
                callback_datas.emplace_back(mtx, cv, flying_cnt, error_code);
            }

            for (size_t base_idx = 0; base_idx < base_vec.size(); ++base_idx)
            {
                txservice::TxKey &tx_key = base_vec[base_idx].first;
                assert(tx_key.Data() != nullptr && tx_key.Size() > 0);
                uint32_t partition_id = base_vec[base_idx].second;
                auto *callback_data = &callback_datas[base_idx];
                callback_data->ResetResult();
                size_t flying_cnt = callback_data->AddFlyingReadCount();
                Read(base_kv_table_name,
                     KvPartitionIdOf(partition_id, true),
                     std::string_view(tx_key.Data(), tx_key.Size()),
                     callback_data,
                     &SyncBatchReadForArchiveCallback);
                if (flying_cnt >= MAX_FLYING_READ_COUNT)
                {
                    callback_data->Wait();
                }
                if (callback_data->GetErrorCode() != 0)
                {
                    LOG(ERROR)
                        << "CopyBaseToArchive failed for read base table.";
                    return false;
                }
            }

            // Wait the result all return.
            {
                std::unique_lock<bthread::Mutex> lk(mtx);
                while (flying_cnt > 0)
                {
                    cv.wait(lk);
                }
            }
            // Process the results
            for (size_t i = 0; i < base_vec.size(); i++)
            {
                auto &callback_data = callback_datas[i];
                txservice::TxKey tx_key =
                    catalog_factory->CreateTxKey(callback_data.key_str_.data(),
                                                 callback_data.key_str_.size());
                batch_size += callback_data.key_str_.size();
                batch_size += callback_data.value_str_.size();
                std::string_view val = callback_data.value_str_;
                size_t offset = 0;
                bool is_deleted = false;
                std::unique_ptr<txservice::TxRecord> record =
                    catalog_factory->CreateTxRecord();
                if (table_name.Engine() == txservice::TableEngine::EloqKv)
                {
                    // mvcc is not used for EloqKV
                    assert(false);
                    txservice::TxObject *tx_object =
                        static_cast<txservice::TxObject *>(record.get());
                    record = tx_object->DeserializeObject(val.data(), offset);
                }
                else
                {
                    DeserializeTxRecordStr(val, is_deleted, offset);
                    if (!is_deleted)
                    {
                        record->Deserialize(val.data(), offset);
                    }
                }

                auto &ref = archive_vec->emplace_back();
                ref.SetKey(std::move(tx_key));
                ref.commit_ts_ = callback_data.ts_;
                ref.partition_id_ = callback_data.partition_id_;

                if (!is_deleted)
                {
                    if (table_name.Engine() == txservice::TableEngine::EloqKv)
                    {
                        // should not be here
                        assert(false);
                        ref.SetNonVersionedPayload(record.get());
                    }
                    else
                    {
                        assert(table_name ==
                                   txservice::Sequences::table_name_ ||
                               table_name.Engine() !=
                                   txservice::TableEngine::None);
                        ref.SetVersionedPayload(std::move(record));
                    }

                    ref.payload_status_ = txservice::RecordStatus::Normal;
                }
                else
                {
                    ref.payload_status_ = txservice::RecordStatus::Deleted;
                }
            }
            // Now all of the data that needs to be copied to the archive
            // table for this kv table name is in the archive_vec We need to
            // wrap it into a FlushTaskEntry and add it to the
            // archive_flush_task
            auto insert_it = archive_flush_task.try_emplace(
                base_kv_table_name,
                std::vector<std::unique_ptr<txservice::FlushTaskEntry>>());
            insert_it.first->second.emplace_back(
                std::make_unique<txservice::FlushTaskEntry>(
                    nullptr,
                    std::move(archive_vec),
                    nullptr,
                    nullptr,
                    flush_task_entry.front()->data_sync_task_,
                    table_schema,
                    batch_size));
        }
    }

    if (!archive_flush_task.empty())
    {
        // Put the archive records to the archive table.
        // This is a sync call
        bool ret = PutArchivesAll(archive_flush_task);
        if (!ret)
        {
            return false;
        }
    }

    return true;
}

/**
 * @brief Fetches archive records for a specific key from a given timestamp.
 *
 * Retrieves archived versions of a record from the MVCC archive storage.
 * Scans the archive table for records matching the specified key and timestamp
 * range. Currently asserts false as this functionality is not fully
 * implemented.
 *
 * @param table_name The table name to fetch archives for.
 * @param kv_info KV catalog information for the table.
 * @param key The key to fetch archive records for.
 * @param archives Output vector to store the fetched archive records.
 * @param from_ts Starting timestamp for the archive fetch.
 * @return Currently always returns false (not implemented).
 */
bool DataStoreServiceClient::FetchArchives(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    const txservice::TxKey &key,
    std::vector<txservice::VersionTxRecord> &archives,
    uint64_t from_ts)
{
    assert(false);

    LOG(INFO) << "FetchArchives: table_name: " << table_name.StringView();
    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);
    uint64_t be_from_ts = EloqShare::host_to_big_endian(from_ts);
    std::string lower_bound_key = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), be_from_ts);
    std::string upper_bound_key = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), UINT64_MAX);
    uint32_t partition_id = HashArchiveKey(kv_table_name, key);
    int32_t kv_partition_id = KvPartitionIdOf(partition_id, true);
    size_t batch_size = 100;
    FetchArchivesCallbackData callback_data(kv_mvcc_archive_name,
                                            kv_partition_id,
                                            lower_bound_key,
                                            upper_bound_key,
                                            batch_size,
                                            UINT64_MAX,
                                            true);

    ScanNext(kv_mvcc_archive_name,
             kv_partition_id,
             lower_bound_key,
             upper_bound_key,
             callback_data.session_id_,
             true,                         // include start key
             false,                        // include end key
             callback_data.scan_forward_,  // scan forward: true
             batch_size,
             nullptr,  // search_condition
             &callback_data,
             &FetchArchivesCallback);
    callback_data.Wait();

    if (callback_data.HasError())
    {
        LOG(ERROR) << "FetchVisibleArchive failed, error:"
                   << callback_data.Result().error_msg()
                   << " table_name: " << table_name.StringView()
                   << " key: " << std::string_view(key.Data(), key.Size());
        return false;
    }

    auto *catalog_factory = GetCatalogFactory(table_name.Engine());
    assert(catalog_factory != nullptr);

    for (size_t i = 0; i < callback_data.archive_values_.size(); ++i)
    {
        const std::string &archive_value_str = callback_data.archive_values_[i];

        bool is_deleted = false;
        std::string value_str;
        size_t value_offset = 0;
        DecodeArchiveValue(archive_value_str, is_deleted, value_offset);

        auto &ref = archives.emplace_back();
        ref.commit_ts_ = callback_data.archive_commit_ts_[i];
        ref.record_status_ = is_deleted ? txservice::RecordStatus::Deleted
                                        : txservice::RecordStatus::Normal;

        if (!is_deleted)
        {
            if (table_name.Engine() == txservice::TableEngine::EloqKv)
            {
                // should not be here
                assert(false);
            }
            else
            {
                std::unique_ptr<txservice::TxRecord> tmp_rec =
                    catalog_factory->CreateTxRecord();
                tmp_rec->Deserialize(archive_value_str.data(), value_offset);
                ref.record_ = std::move(tmp_rec);
            }
        }
    }

    return true;
}

/**
 * @brief Fetches the visible archive record for a key at a specific timestamp.
 *
 * Retrieves the most recent archive record for a given key that is visible
 * at the specified upper bound timestamp. Scans the archive table in reverse
 * order to find the latest visible version. Currently asserts false as this
 * functionality is not fully implemented.
 *
 * @param table_name The table name to fetch archive for.
 * @param kv_info KV catalog information for the table.
 * @param key The key to fetch archive record for.
 * @param upper_bound_ts The upper bound timestamp for visibility.
 * @param rec Output parameter for the fetched record.
 * @param rec_status Output parameter for the record status.
 * @param commit_ts Output parameter for the commit timestamp.
 * @return Currently always returns false (not implemented).
 */
bool DataStoreServiceClient::FetchVisibleArchive(
    const txservice::TableName &table_name,
    const txservice::KVCatalogInfo *kv_info,
    const txservice::TxKey &key,
    const uint64_t upper_bound_ts,
    txservice::TxRecord &rec,
    txservice::RecordStatus &rec_status,
    uint64_t &commit_ts)
{
    assert(false);

    const std::string &kv_table_name = kv_info->GetKvTableName(table_name);
    uint64_t be_upper_bound_ts = EloqShare::host_to_big_endian(upper_bound_ts);
    std::string lower_bound_key =
        EncodeArchiveKey(kv_table_name,
                         std::string_view(key.Data(), key.Size()),
                         be_upper_bound_ts);
    std::string upper_bound_key = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), 0);
    uint32_t partition_id = HashArchiveKey(kv_table_name, key);
    int32_t kv_partition_id = KvPartitionIdOf(partition_id, true);
    size_t batch_size = 1;
    FetchArchivesCallbackData callback_data(kv_mvcc_archive_name,
                                            kv_partition_id,
                                            lower_bound_key,
                                            upper_bound_key,
                                            batch_size,
                                            1,  // limit 1
                                            false);
    ScanNext(kv_mvcc_archive_name,
             kv_partition_id,
             lower_bound_key,
             upper_bound_key,
             callback_data.session_id_,
             true,                         // include start key
             false,                        // include end key
             callback_data.scan_forward_,  // scan forward: false
             batch_size,
             nullptr,  // search condition
             &callback_data,
             &FetchArchivesCallback);
    callback_data.Wait();

    if (callback_data.HasError())
    {
        LOG(ERROR) << "FetchVisibleArchive failed, error:"
                   << callback_data.Result().error_msg()
                   << " table_name: " << table_name.StringView()
                   << " key: " << std::string_view(key.Data(), key.Size());
        return false;
    }

    if (callback_data.archive_values_.empty())
    {
        rec_status = txservice::RecordStatus::Deleted;
        return true;
    }

    assert(callback_data.archive_values_.size() == 1);
    const std::string &archive_value_str = callback_data.archive_values_[0];

    bool is_deleted = false;
    size_t value_offset = 0;
    DecodeArchiveValue(archive_value_str, is_deleted, value_offset);
    commit_ts = callback_data.archive_commit_ts_[0];

    rec_status = is_deleted ? txservice::RecordStatus::Deleted
                            : txservice::RecordStatus::Normal;
    if (!is_deleted)
    {
        if (table_name.Engine() == txservice::TableEngine::EloqKv)
        {
            // should not be here
            assert(false);
        }
        else
        {
            rec.Deserialize(archive_value_str.data(), value_offset);
        }
    }

    return true;
}

/**
 * @brief Fetches archive records for a fetch record CC operation.
 *
 * Retrieves archive records for a specific key and snapshot read timestamp.
 * Encodes the appropriate key range for scanning the archive table and
 * initiates a scan operation to fetch all relevant archive versions.
 * Sets up the fetch CC object with the necessary scan parameters.
 *
 * @param fetch_cc Fetch record CC object containing key, timestamp, and result
 * storage.
 * @return DataStoreOpStatus indicating the operation status.
 */
txservice::store::DataStoreHandler::DataStoreOpStatus
DataStoreServiceClient::FetchArchives(txservice::FetchRecordCc *fetch_cc)
{
    // 1- fetch the visible version archive.
    // 2- fetch all archives that from the visible version to the latest
    // version.

    const std::string &kv_table_name = fetch_cc->kv_table_name_;
    const txservice::TxKey &key = fetch_cc->tx_key_;

    uint64_t be_read_ts =
        EloqShare::host_to_big_endian(fetch_cc->snapshot_read_ts_);
    fetch_cc->kv_start_key_ = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), be_read_ts);
    fetch_cc->kv_end_key_ = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), 0);
    uint32_t partition_id = HashArchiveKey(kv_table_name, key);
    // Also use the partion_id in fetch_cc to store kv partition
    fetch_cc->partition_id_ = KvPartitionIdOf(partition_id, true);
    fetch_cc->kv_session_id_.clear();

    ScanNext(kv_mvcc_archive_name,
             fetch_cc->partition_id_,
             fetch_cc->kv_start_key_,
             fetch_cc->kv_end_key_,
             fetch_cc->kv_session_id_,
             true,   // include start key
             false,  // include end key
             false,  // scan forward: false
             1,
             nullptr,  // search condition
             fetch_cc,
             &FetchRecordArchivesCallback);
    return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
}

txservice::store::DataStoreHandler::DataStoreOpStatus
DataStoreServiceClient::FetchVisibleArchive(
    txservice::FetchSnapshotCc *fetch_cc)
{
    // Only Fetch the visible version archive.

    const std::string &kv_table_name = fetch_cc->kv_table_name_;
    const txservice::TxKey &key = fetch_cc->tx_key_;

    uint64_t be_read_ts =
        EloqShare::host_to_big_endian(fetch_cc->snapshot_read_ts_);
    fetch_cc->kv_start_key_ = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), be_read_ts);
    fetch_cc->kv_end_key_ = EncodeArchiveKey(
        kv_table_name, std::string_view(key.Data(), key.Size()), 0);
    uint32_t partition_id = HashArchiveKey(kv_table_name, key);
    int32_t kv_partition_id = KvPartitionIdOf(partition_id, true);

    ScanNext(kv_mvcc_archive_name,
             kv_partition_id,
             fetch_cc->kv_start_key_,
             fetch_cc->kv_end_key_,
             "",
             true,   // include start key
             false,  // include end key
             false,  // scan forward: false
             1,
             nullptr,  // search condition
             fetch_cc,
             &FetchSnapshotArchiveCallback);
    return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
}

/**
 * @brief Creates a snapshot for backup operations.
 *
 * Initiates a snapshot creation process across all shards in the cluster.
 * Collects shard IDs from the cluster manager and coordinates snapshot creation
 * for both local and remote shards. Waits for completion and returns the
 * backup files generated during the process.
 *
 * @param backup_name The name for the backup snapshot.
 * @param backup_files Output vector to store the generated backup file paths.
 * @param backup_ts The timestamp for the backup.
 * @return true if the snapshot is created successfully, false if any operation
 * fails.
 */
bool DataStoreServiceClient::CreateSnapshotForBackup(
    const std::string &backup_name,
    std::vector<std::string> &backup_files,
    uint64_t backup_ts)
{
    CreateSnapshotForBackupClosure *closure =
        create_snapshot_for_backup_closure_pool_.NextObject();
    uint32_t shard_cnt = AllDataShardCount();
    std::vector<uint32_t> shard_ids;
    shard_ids.reserve(shard_cnt);
    for (uint32_t shard_id = 0; shard_id < shard_cnt; shard_id++)
    {
        shard_ids.push_back(shard_id);
    }

    CreateSnapshotForBackupCallbackData *callback_data =
        create_snapshot_for_backup_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);

    closure->Reset(*this,
                   std::move(shard_ids),
                   backup_name,
                   backup_ts,
                   &backup_files,
                   callback_data,
                   &CreateSnapshotForBackupCallback);
    CreateSnapshotForBackupInternal(closure);
    callback_data->Wait();

    return !callback_data->HasError();
}

/**
 * @brief Internal method for creating snapshots for backup operations.
 *
 * Processes snapshot creation for individual shards, handling both local and
 * remote shards differently. For local shards, prepares local requests; for
 * remote shards, prepares RPC requests. Manages the closure lifecycle and
 * coordinates completion when all shards are processed.
 *
 * @param closure The closure object managing the snapshot creation process.
 */
void DataStoreServiceClient::CreateSnapshotForBackupInternal(
    CreateSnapshotForBackupClosure *closure)
{
    if (closure->UnfinishedShards().empty())
    {
        // All shards have been processed, complete the operation
        closure->Run();
        return;
    }

    uint32_t shard_id = closure->UnfinishedShards().back();
    closure->UnfinishedShards().pop_back();

    if (IsLocalShard(shard_id))
    {
        // Handle local shard
        closure->PrepareRequest(true);
        data_store_service_->CreateSnapshotForBackup(
            shard_id,
            closure->GetBackupName(),
            closure->GetBackupTs(),
            closure->LocalBackupFilesPtr(),
            &closure->LocalResultRef(),
            closure);
    }
    else
    {
        // Handle remote shard
        closure->PrepareRequest(false);
        uint32_t node_index = GetOwnerNodeIndexOfShard(shard_id);
        closure->SetRemoteNodeIndex(node_index);
        auto *channel = dss_nodes_[node_index].Channel();

        EloqDS::remote::DataStoreRpcService_Stub stub(channel);
        brpc::Controller &cntl = *closure->Controller();
        cntl.set_timeout_ms(30000);  // Longer timeout for backup operations
        auto *req = closure->RemoteRequest();
        auto *resp = closure->RemoteResponse();
        stub.CreateSnapshotForBackup(&cntl, req, resp, closure);
    }
}

/**
 * @brief Determines if range copying is needed.
 *
 * Currently always returns true, indicating that range copying is always
 * required. This method is used to determine whether range data needs to be
 * copied during certain operations.
 *
 * @return Always returns true.
 */
bool DataStoreServiceClient::NeedCopyRange() const
{
    return true;
}

/**
 * @brief Restores transaction cache for a node group.
 *
 * Currently not implemented. This method is a placeholder for restoring
 * transaction cache state for a specific node group and term.
 * Will log an error and assert false if called.
 *
 * @param cc_ng_id The node group ID to restore cache for.
 * @param cc_ng_term The term for the node group.
 */
void DataStoreServiceClient::RestoreTxCache(txservice::NodeGroupId cc_ng_id,
                                            int64_t cc_ng_term)
{
    LOG(ERROR) << "RestoreTxCache not implemented";
    assert(false);
}

/**
 * @brief Handles leader start event.
 *
 * Currently always returns true. This method is called when the node becomes
 * a leader and can be used to perform leader-specific initialization.
 *
 * @param next_leader_node Pointer to store the next leader node ID (unused).
 * @return Always returns true.
 */
bool DataStoreServiceClient::OnLeaderStart(uint32_t *next_leader_node)
{
    return true;
}

/**
 * @brief Handles start following event.
 *
 * Currently empty implementation. This method is called when the node starts
 * following another leader and can be used to perform follower-specific
 * initialization.
 */
void DataStoreServiceClient::OnStartFollowing()
{
}

/**
 * @brief Handles shutdown event.
 *
 * Currently empty implementation. This method is called when the node is
 * shutting down and can be used to perform cleanup operations.
 */
void DataStoreServiceClient::OnShutdown()
{
}

/**
 * @brief Checks if a shard is local to this node.
 *
 * Determines whether the specified shard is owned by this node using the
 * cluster manager. This is used for scale-up scenarios where data needs to be
 * migrated from smaller to larger nodes.
 *
 * @param shard_id The shard ID to check.
 * @return true if the shard is local to this node, false otherwise.
 */
bool DataStoreServiceClient::IsLocalShard(uint32_t shard_id)
{
    if (data_store_service_ != nullptr)
    {
        return data_store_service_->IsOwnerOfShard(shard_id);
    }

    return false;
}

/**
 * @brief Checks if a partition is local to this node.
 *
 * Determines whether the specified partition is owned by this node using the
 * cluster manager. Used for determining whether operations should be performed
 * locally or remotely.
 *
 * @param partition_id The partition ID to check.
 * @return true if the partition is local to this node, false otherwise.
 */
bool DataStoreServiceClient::IsLocalPartition(int32_t partition_id)
{
    return IsLocalShard(GetShardIdByPartitionId(partition_id));
}

uint32_t DataStoreServiceClient::GetShardIdByPartitionId(
    int32_t partition_id) const
{
    // Now, only support one shard.
    return 0;
}

uint32_t DataStoreServiceClient::AllDataShardCount() const
{
    return dss_shards_.size();
}

uint32_t DataStoreServiceClient::GetOwnerNodeIndexOfShard(
    uint32_t shard_id) const
{
    assert(dss_shards_[shard_id].load(std::memory_order_acquire) != UINT32_MAX);
    return dss_shards_[shard_id].load(std::memory_order_acquire);
}

bool DataStoreServiceClient::UpdateOwnerNodeIndexOfShard(
    uint32_t shard_id, uint32_t old_node_index, uint32_t &new_node_index)
{
    new_node_index = dss_shards_[shard_id].load(std::memory_order_acquire);
    if (new_node_index != old_node_index)
    {
        return true;
    }

    uint64_t expect_val = 0;
    uint64_t current_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    if (dss_nodes_[old_node_index].expired_ts_.compare_exchange_strong(
            expect_val, current_ts))
    {
        // The old node channle is not updated by other, update it.
        uint32_t free_index = FindFreeNodeIndex();
        if (free_index == dss_nodes_.size())
        {
            LOG(ERROR) << "Find free node index failed";
            dss_nodes_[old_node_index].expired_ts_.store(
                expect_val, std::memory_order_release);
            return false;
        }
        auto &node = dss_nodes_[free_index];
        node.Reset(dss_nodes_[old_node_index].HostName(),
                   dss_nodes_[old_node_index].Port(),
                   dss_nodes_[old_node_index].ShardVersion());
        if (dss_shards_[shard_id].compare_exchange_strong(old_node_index,
                                                          free_index))
        {
            new_node_index = free_index;
            return true;
        }
        else
        {
            DLOG(INFO) << "Other thread updated the data shard, shard_id:"
                       << shard_id;
            node.expired_ts_.store(1, std::memory_order_release);
            new_node_index = old_node_index;
            return true;
        }
    }
    else
    {
        // Other thread is updating the shard. Waiting.
        return false;
    }
}

uint32_t DataStoreServiceClient::FindFreeNodeIndex()
{
    uint64_t current_ts =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    for (uint32_t i = 0; i < dss_nodes_.size(); i++)
    {
        uint64_t expired_ts =
            dss_nodes_[i].expired_ts_.load(std::memory_order_acquire);
        if (expired_ts > 0 && expired_ts < current_ts &&
            (current_ts - expired_ts) > NodeExpiredTime &&
            dss_nodes_[i].expired_ts_.compare_exchange_strong(expired_ts, 0))
        {
            return i;
        }
    }
    // not found
    return dss_nodes_.size();
}

void DataStoreServiceClient::HandleShardingError(
    const ::EloqDS::remote::CommonResult &result)
{
    assert(result.error_code() ==
           static_cast<uint32_t>(
               ::EloqDS::remote::DataStoreError::REQUESTED_NODE_NOT_OWNER));

    auto &new_key_sharding = result.new_key_sharding();
    auto error_type = new_key_sharding.type();
    if (error_type ==
        ::EloqDS::remote::KeyShardingErrorType::PrimaryNodeChanged)
    {
        uint32_t shard_id = new_key_sharding.shard_id();
        uint64_t shard_version = new_key_sharding.shard_version();
        auto &primary_node = new_key_sharding.new_primary_node();
        DSSNode new_primary_node;
        while (!UpgradeShardVersion(shard_id,
                                    shard_version,
                                    primary_node.host_name(),
                                    primary_node.port()))
        {
            DLOG(INFO) << "Upgrade shard version failed, shard_id: "
                       << shard_id;
            bthread_usleep(10000);
            continue;
        }
    }
    else
    {
        assert(false);
        // the whole node group has changed
        LOG(FATAL) << "The topology of data shards is changed";
        // TODO(lzx): handle the topology of cluster change.
    }
}

bool DataStoreServiceClient::UpgradeShardVersion(uint32_t shard_id,
                                                 uint64_t shard_version,
                                                 const std::string &host_name,
                                                 uint16_t port)
{
    if (shard_id >= dss_shards_.size())
    {
        assert(false);
        // Now only support one shard.
        LOG(FATAL) << "Shard id not found, shard_id: " << shard_id;
        return true;
    }

    uint32_t node_index = dss_shards_[shard_id].load(std::memory_order_acquire);
    auto &node_ref = dss_nodes_[node_index];
    if (node_ref.ShardVersion() < shard_version)
    {
        uint64_t expect_val = 0;
        uint64_t current_ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        if (!node_ref.expired_ts_.compare_exchange_strong(expect_val,
                                                          current_ts))
        {
            // Other thread is updating the shard, retry.
            DLOG(INFO) << "Other thread is updating the data shard, shard_id: "
                       << shard_id;
            return false;
        }

        uint32_t free_node_index = FindFreeNodeIndex();
        if (free_node_index == dss_nodes_.size())
        {
            DLOG(INFO) << "Find free node index failed";
            node_ref.expired_ts_.store(expect_val, std::memory_order_release);
            return false;
        }
        auto &free_node_ref = dss_nodes_[free_node_index];
        free_node_ref.Reset(host_name, port, shard_version);
        if (!dss_shards_[shard_id].compare_exchange_strong(node_index,
                                                           free_node_index))
        {
            assert(false);
            free_node_ref.expired_ts_.store(1, std::memory_order_release);
        }
    }
    return true;
}

txservice::store::DataStoreHandler::DataStoreOpStatus
DataStoreServiceClient::FetchRecord(
    txservice::FetchRecordCc *fetch_cc,
    txservice::FetchSnapshotCc *fetch_snapshot_cc)
{
    if (fetch_snapshot_cc != nullptr)
    {
        assert(fetch_cc == nullptr);
        return FetchSnapshot(fetch_snapshot_cc);
    }

    if (metrics::enable_kv_metrics)
    {
        fetch_cc->start_ = metrics::Clock::now();
    }

    if (!fetch_cc->tx_key_.IsOwner())
    {
        fetch_cc->tx_key_ = fetch_cc->tx_key_.Clone();
    }

    if (fetch_cc->only_fetch_archives_)
    {
        return FetchArchives(fetch_cc);
    }

    Read(fetch_cc->kv_table_name_,
         KvPartitionIdOf(fetch_cc->partition_id_,
                         !fetch_cc->table_name_.IsHashPartitioned()),
         std::string_view(fetch_cc->tx_key_.Data(), fetch_cc->tx_key_.Size()),
         fetch_cc,
         &FetchRecordCallback);

    return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
}

txservice::store::DataStoreHandler::DataStoreOpStatus
DataStoreServiceClient::FetchSnapshot(txservice::FetchSnapshotCc *fetch_cc)
{
    if (metrics::enable_kv_metrics)
    {
        fetch_cc->start_ = metrics::Clock::now();
    }

    if (!fetch_cc->tx_key_.IsOwner())
    {
        fetch_cc->tx_key_ = fetch_cc->tx_key_.Clone();
    }

    if (fetch_cc->only_fetch_archives_)
    {
        return FetchVisibleArchive(fetch_cc);
    }

    Read(fetch_cc->kv_table_name_,
         KvPartitionIdOf(fetch_cc->partition_id_,
                         !fetch_cc->table_name_.IsHashPartitioned()),
         std::string_view(fetch_cc->tx_key_.Data(), fetch_cc->tx_key_.Size()),
         fetch_cc,
         &FetchSnapshotCallback);

    return txservice::store::DataStoreHandler::DataStoreOpStatus::Success;
}

void DataStoreServiceClient::Read(const std::string_view kv_table_name,
                                  const uint32_t partition_id,
                                  const std::string_view key,
                                  void *callback_data,
                                  DataStoreCallback callback)
{
    ReadClosure *read_closure = read_closure_pool_.NextObject();
    read_closure->Reset(
        this, kv_table_name, partition_id, key, callback_data, callback);
    ReadInternal(read_closure);
}

void DataStoreServiceClient::ReadInternal(ReadClosure *read_closure)
{
    if (IsLocalPartition(read_closure->PartitionId()))
    {
        read_closure->PrepareRequest(true);
        data_store_service_->Read(read_closure->TableName(),
                                  read_closure->PartitionId(),
                                  read_closure->Key(),
                                  &read_closure->LocalValueRef(),
                                  &read_closure->LocalTsRef(),
                                  &read_closure->LocalTtlRef(),
                                  &read_closure->LocalResultRef(),
                                  read_closure);
    }
    else
    {
        read_closure->PrepareRequest(false);
        uint32_t node_index = GetOwnerNodeIndexOfShard(
            GetShardIdByPartitionId(read_closure->PartitionId()));
        read_closure->SetRemoteNodeIndex(node_index);
        auto *channel = dss_nodes_[node_index].Channel();

        EloqDS::remote::DataStoreRpcService_Stub stub(channel);
        brpc::Controller &cntl = *read_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = read_closure->ReadRequest();
        auto *resp = read_closure->ReadResponse();
        stub.Read(&cntl, req, resp, read_closure);
    }
}

void DataStoreServiceClient::DeleteRange(const std::string_view table_name,
                                         const int32_t partition_id,
                                         const std::string &start_key,
                                         const std::string &end_key,
                                         const bool skip_wal,
                                         void *callback_data,
                                         DataStoreCallback callback)
{
    DeleteRangeClosure *closure = delete_range_closure_pool_.NextObject();

    closure->Reset(*this,
                   table_name,
                   partition_id,
                   start_key,
                   end_key,
                   skip_wal,
                   callback_data,
                   callback);

    DeleteRangeInternal(closure);
}

void DataStoreServiceClient::DeleteRangeInternal(
    DeleteRangeClosure *delete_range_clouse)
{
    if (IsLocalPartition(delete_range_clouse->PartitionId()))
    {
        delete_range_clouse->PrepareRequest(true);
        data_store_service_->DeleteRange(delete_range_clouse->TableName(),
                                         delete_range_clouse->PartitionId(),
                                         delete_range_clouse->StartKey(),
                                         delete_range_clouse->EndKey(),
                                         delete_range_clouse->SkipWal(),
                                         delete_range_clouse->Result(),
                                         delete_range_clouse);
    }
    else
    {
        delete_range_clouse->PrepareRequest(false);
        uint32_t node_index = GetOwnerNodeIndexOfShard(
            GetShardIdByPartitionId(delete_range_clouse->PartitionId()));
        delete_range_clouse->SetRemoteNodeIndex(node_index);
        auto *channel = dss_nodes_[node_index].Channel();

        EloqDS::remote::DataStoreRpcService_Stub stub(channel);
        brpc::Controller &cntl = *delete_range_clouse->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = delete_range_clouse->DeleteRangeRequest();
        auto *resp = delete_range_clouse->DeleteRangeResponse();
        stub.DeleteRange(&cntl, req, resp, delete_range_clouse);
    }
}

void DataStoreServiceClient::FlushData(
    const std::vector<std::string> &kv_table_names,
    void *callback_data,
    DataStoreCallback callback)
{
    FlushDataClosure *closure = flush_data_closure_pool_.NextObject();
    uint32_t shard_cnt = AllDataShardCount();
    std::vector<uint32_t> shard_ids;
    shard_ids.reserve(shard_cnt);
    for (uint32_t shard_id = 0; shard_id < shard_cnt; shard_id++)
    {
        shard_ids.push_back(shard_id);
    }

    closure->Reset(
        *this, &kv_table_names, std::move(shard_ids), callback_data, callback);

    FlushDataInternal(closure);
}

void DataStoreServiceClient::FlushDataInternal(
    FlushDataClosure *flush_data_closure)
{
    assert(!flush_data_closure->UnfinishedShards().empty());
    uint32_t shard_id = flush_data_closure->UnfinishedShards().back();
    if (IsLocalShard(shard_id))
    {
        flush_data_closure->PrepareRequest(true);
        data_store_service_->FlushData(flush_data_closure->KvTableNames(),
                                       shard_id,
                                       flush_data_closure->Result(),
                                       flush_data_closure);
    }
    else
    {
        flush_data_closure->PrepareRequest(false);
        uint32_t node_index = GetOwnerNodeIndexOfShard(shard_id);
        flush_data_closure->SetRemoteNodeIndex(node_index);
        auto *channel = dss_nodes_[node_index].Channel();

        EloqDS::remote::DataStoreRpcService_Stub stub(channel);
        brpc::Controller &cntl = *flush_data_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = flush_data_closure->FlushDataRequest();
        auto *resp = flush_data_closure->FlushDataResponse();
        stub.FlushData(&cntl, req, resp, flush_data_closure);
    }
}

// NOTICE: the DropTable function is not atomic.
void DataStoreServiceClient::DropTable(std::string_view table_name,
                                       void *callback_data,
                                       DataStoreCallback callback)
{
    DLOG(INFO) << "DropTableWithRetry for table: " << table_name;

    DropTableClosure *closure = drop_table_closure_pool_.NextObject();
    uint32_t shard_cnt = AllDataShardCount();
    std::vector<uint32_t> shard_ids;
    shard_ids.reserve(shard_cnt);
    for (uint32_t shard_id = 0; shard_id < shard_cnt; shard_id++)
    {
        shard_ids.push_back(shard_id);
    }

    closure->Reset(
        *this, table_name, std::move(shard_ids), callback_data, callback);

    DropTableInternal(closure);
}

void DataStoreServiceClient::DropTableInternal(
    DropTableClosure *drop_table_closure)
{
    // TODO(lzx): drop table data on all data shards in parallel.
    uint32_t shard_id = drop_table_closure->UnfinishedShards().back();
    if (IsLocalShard(shard_id))
    {
        drop_table_closure->PrepareRequest(true);
        data_store_service_->DropTable(drop_table_closure->TableName(),
                                       shard_id,
                                       drop_table_closure->Result(),
                                       drop_table_closure);
    }
    else
    {
        drop_table_closure->PrepareRequest(false);
        uint32_t node_index = GetOwnerNodeIndexOfShard(shard_id);
        drop_table_closure->SetRemoteNodeIndex(node_index);
        auto *channel = dss_nodes_[node_index].Channel();

        EloqDS::remote::DataStoreRpcService_Stub stub(channel);
        brpc::Controller &cntl = *drop_table_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = drop_table_closure->DropTableRequest();
        auto *resp = drop_table_closure->DropTableResponse();
        stub.DropTable(&cntl, req, resp, drop_table_closure);
    }
}

void DataStoreServiceClient::ScanNext(
    const std::string_view table_name,
    uint32_t partition_id,
    const std::string_view start_key,
    const std::string_view end_key,
    const std::string_view session_id,
    bool inclusive_start,
    bool inclusive_end,
    bool scan_forward,
    uint32_t batch_size,
    const std::vector<remote::SearchCondition> *search_conditions,
    void *callback_data,
    DataStoreCallback callback)
{
    ScanNextClosure *closure = scan_next_closure_pool_.NextObject();
    closure->Reset(*this,
                   table_name,
                   partition_id,
                   start_key,
                   end_key,
                   inclusive_start,
                   inclusive_end,
                   scan_forward,
                   session_id,
                   batch_size,
                   search_conditions,
                   callback_data,
                   callback);
    ScanNextInternal(closure);
}

void DataStoreServiceClient::ScanNextInternal(
    ScanNextClosure *scan_next_closure)
{
    if (IsLocalPartition(scan_next_closure->PartitionId()))
    {
        scan_next_closure->PrepareRequest(true);
        data_store_service_->ScanNext(
            scan_next_closure->TableName(),
            scan_next_closure->PartitionId(),
            scan_next_closure->StartKey(),
            scan_next_closure->EndKey(),
            scan_next_closure->InclusiveStart(),
            scan_next_closure->InclusiveEnd(),
            scan_next_closure->ScanForward(),
            scan_next_closure->BatchSize(),
            scan_next_closure->LocalSearchConditionsPtr(),
            &scan_next_closure->LocalItemsRef(),
            &scan_next_closure->LocalSessionIdRef(),
            &scan_next_closure->Result(),
            scan_next_closure);
    }
    else
    {
        scan_next_closure->PrepareRequest(false);
        uint32_t node_index = GetOwnerNodeIndexOfShard(
            GetShardIdByPartitionId(scan_next_closure->PartitionId()));
        scan_next_closure->SetRemoteNodeIndex(node_index);
        auto *channel = dss_nodes_[node_index].Channel();

        EloqDS::remote::DataStoreRpcService_Stub stub(channel);
        brpc::Controller &cntl = *scan_next_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = scan_next_closure->ScanNextRequest();
        auto *resp = scan_next_closure->ScanNextResponse();
        stub.ScanNext(&cntl, req, resp, scan_next_closure);
    }
}

void DataStoreServiceClient::ScanClose(const std::string_view table_name,
                                       uint32_t partition_id,
                                       std::string &session_id,
                                       void *callback_data,
                                       DataStoreCallback callback)
{
    ScanNextClosure *closure = scan_next_closure_pool_.NextObject();
    closure->Reset(*this,
                   table_name,
                   partition_id,
                   "",     // start_key (empty for scan close)
                   "",     // end_key (empty for scan close)
                   false,  // inclusive_start
                   false,  // inclusive_end
                   true,   // scan_forward
                   session_id,
                   0,  // batch_size 0 for close
                   nullptr,
                   callback_data,
                   callback);
    ScanCloseInternal(closure);
}

void DataStoreServiceClient::ScanCloseInternal(
    ScanNextClosure *scan_next_closure)
{
    if (IsLocalPartition(scan_next_closure->PartitionId()))
    {
        scan_next_closure->PrepareRequest(true);
        data_store_service_->ScanClose(scan_next_closure->TableName(),
                                       scan_next_closure->PartitionId(),
                                       &scan_next_closure->LocalSessionIdRef(),
                                       &scan_next_closure->LocalResultRef(),
                                       scan_next_closure);
    }
    else
    {
        scan_next_closure->PrepareRequest(false);
        uint32_t node_index = GetOwnerNodeIndexOfShard(
            GetShardIdByPartitionId(scan_next_closure->PartitionId()));
        scan_next_closure->SetRemoteNodeIndex(node_index);
        auto *channel = dss_nodes_[node_index].Channel();

        EloqDS::remote::DataStoreRpcService_Stub stub(channel);
        brpc::Controller &cntl = *scan_next_closure->Controller();
        cntl.set_timeout_ms(5000);
        auto *req = scan_next_closure->ScanNextRequest();
        auto *resp = scan_next_closure->ScanNextResponse();
        stub.ScanClose(&cntl, req, resp, scan_next_closure);
    }
}

bool DataStoreServiceClient::InitTableRanges(
    const txservice::TableName &table_name, uint64_t version)
{
    // init_partition_id and kv_partition_id
    int32_t kv_partition_id = KvPartitionIdOf(table_name);
    int32_t init_range_id =
        txservice::Sequences::InitialRangePartitionIdOf(table_name);
    auto catalog_factory = GetCatalogFactory(table_name.Engine());
    assert(catalog_factory != nullptr);

    const txservice::TxKey *neg_inf_key =
        catalog_factory->PackedNegativeInfinity();

    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();

    std::string key_str =
        EncodeRangeKey(catalog_factory, table_name, *neg_inf_key);
    std::string rec_str = EncodeRangeValue(init_range_id, version, version, 0);

    keys.emplace_back(std::string_view(key_str.data(), key_str.size()));
    records.emplace_back(std::string_view(rec_str.data(), rec_str.size()));
    records_ts.emplace_back(version);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);
    BatchWriteRecords(kv_range_table_name,
                      kv_partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      callback_data,
                      &SyncCallback);
    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(WARNING) << "InitTableRanges: Failed to write range info.";
        return false;
    }

    return true;
}

bool DataStoreServiceClient::DeleteTableRanges(
    const txservice::TableName &table_name)
{
    int32_t kv_partition_id = KvPartitionIdOf(table_name);
    // delete all slices info from {kv_range_slices_table_name} table
    std::string start_key = table_name.String();
    std::string end_key = start_key;
    end_key.back()++;

    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();
    DeleteRange(kv_range_slices_table_name,
                kv_partition_id,
                start_key,
                end_key,
                false,
                callback_data,
                &SyncCallback);
    callback_data->Wait();

    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteTableRanges failed, error: "
                   << callback_data->Result().error_msg();
        return false;
    }

    // delete all range info from {kv_range_table_name} table
    callback_data->Reset();
    DeleteRange(kv_range_table_name,
                kv_partition_id,
                start_key,
                end_key,
                false,
                callback_data,
                &SyncCallback);
    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteTableRanges failed, error: "
                   << callback_data->Result().error_msg();
        return false;
    }

    return true;
}

bool DataStoreServiceClient::InitTableLastRangePartitionId(
    const txservice::TableName &table_name)
{
    int32_t init_range_id =
        txservice::Sequences::InitialRangePartitionIdOf(table_name);

    if (txservice::Sequences::Initialized())
    {
        bool res = txservice::Sequences::InitIdOfTableRangePartition(
            table_name, init_range_id);

        DLOG(INFO) << "UpdateLastRangePartition, table: "
                   << table_name.StringView() << ", res: " << (int) res;
        return res;
    }

    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();

    std::pair<txservice::TxKey, txservice::TxRecord::Uptr> seq_pair =
        txservice::Sequences::GetSequenceKeyAndInitRecord(
            table_name,
            txservice::SequenceType::RangePartitionId,
            init_range_id,
            1,
            1,
            init_range_id + 1);
    // See PutAll(): encode is_delete, encoded_blob_data and unpack_info
    std::string encoded_tx_record;
    if (table_name.IsHashPartitioned())
    {
        encoded_tx_record = std::string(seq_pair.second->EncodedBlobData(),
                                        seq_pair.second->EncodedBlobSize());
    }
    else
    {
        encoded_tx_record = SerializeTxRecord(false, seq_pair.second.get());
    }
    int32_t kv_partition_id =
        KvPartitionIdOf(txservice::Sequences::table_name_);

    for (int i = 0; i < 3; i++)
    {
        // Write directly into sequence table in kvstore.
        callback_data->Reset();
        keys.emplace_back(
            std::string_view(seq_pair.first.Data(), seq_pair.first.Size()));
        records.emplace_back(std::string_view(encoded_tx_record.data(),
                                              encoded_tx_record.size()));
        records_ts.push_back(100U);
        records_ttl.push_back(0U);
        op_types.push_back(WriteOpType::PUT);

        BatchWriteRecords(txservice::Sequences::kv_table_name_sv_,
                          kv_partition_id,
                          std::move(keys),
                          std::move(records),
                          std::move(records_ts),
                          std::move(records_ttl),
                          std::move(op_types),
                          false,
                          callback_data,
                          &SyncCallback);
        callback_data->Wait();
        if (callback_data->Result().error_code() ==
            EloqDS::remote::DataStoreError::NO_ERROR)
        {
            DLOG(INFO) << "DataStoreHandler:InitTableLastRangePartitionId "
                          "finished. Table: "
                       << table_name.StringView();
            return true;
        }
        else
        {
            LOG(WARNING) << "DataStoreHandler:InitTableLastRangePartitionId "
                            "failed, retrying. Table: "
                         << table_name.StringView()
                         << " Error: " << callback_data->Result().error_msg();
            bthread_usleep(500000U);
        }
    }
    return false;
}

bool DataStoreServiceClient::DeleteTableStatistics(
    const txservice::TableName &base_table_name)
{
    int32_t kv_partition_id = KvPartitionIdOf(base_table_name);

    // delete all sample keys from {kv_table_statistics_name} table
    std::string start_key = base_table_name.String();
    std::string end_key = start_key;
    end_key.back()++;

    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();
    DeleteRange(kv_table_statistics_name,
                kv_partition_id,
                start_key,
                end_key,
                false,
                callback_data,
                &SyncCallback);
    callback_data->Wait();

    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteTableStatistics failed, error: "
                   << callback_data->Result().error_msg();
        return false;
    }

    // delete table statistics version from
    // {kv_table_statistics_version_name}
    callback_data->Reset();
    DeleteRange(kv_table_statistics_version_name,
                kv_partition_id,
                start_key,
                end_key,
                false,
                callback_data,
                &SyncCallback);
    callback_data->Wait();

    if (callback_data->Result().error_code() !=
        EloqDS::remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteTableStatistics failed, error: "
                   << callback_data->Result().error_msg();
        return false;
    }

    return true;
}

void DataStoreServiceClient::BatchWriteRecords(
    std::string_view kv_table_name,
    int32_t partition_id,
    std::vector<std::string_view> &&key_parts,
    std::vector<std::string_view> &&record_parts,
    std::vector<uint64_t> &&records_ts,
    std::vector<uint64_t> &&records_ttl,
    std::vector<WriteOpType> &&op_types,
    bool skip_wal,
    void *callback_data,
    DataStoreCallback callback,
    uint16_t parts_cnt_per_key,
    uint16_t parts_cnt_per_record)
{
    assert(key_parts.size() % parts_cnt_per_key == 0);
    assert(record_parts.size() % parts_cnt_per_record == 0);
    BatchWriteRecordsClosure *closure = batch_write_closure_pool_.NextObject();

    closure->Reset(*this,
                   kv_table_name,
                   partition_id,
                   std::move(key_parts),
                   std::move(record_parts),
                   std::move(records_ts),
                   std::move(records_ttl),
                   std::move(op_types),
                   skip_wal,
                   callback_data,
                   callback,
                   parts_cnt_per_key,
                   parts_cnt_per_record);

    BatchWriteRecordsInternal(closure);
}

void DataStoreServiceClient::BatchWriteRecordsInternal(
    BatchWriteRecordsClosure *closure)
{
    assert(closure != nullptr);
    uint32_t req_shard_id = GetShardIdByPartitionId(closure->partition_id_);

    if (IsLocalShard(req_shard_id))
    {
        closure->PrepareRequest(true);
        data_store_service_->BatchWriteRecords(closure->kv_table_name_,
                                               closure->partition_id_,
                                               closure->key_parts_,
                                               closure->record_parts_,
                                               closure->record_ts_,
                                               closure->record_ttl_,
                                               closure->op_types_,
                                               closure->skip_wal_,
                                               closure->result_,
                                               closure,
                                               closure->PartsCountPerKey(),
                                               closure->PartsCountPerRecord());
    }
    else
    {
        // prepare request
        closure->PrepareRequest(false);
        uint32_t node_index = GetOwnerNodeIndexOfShard(req_shard_id);
        closure->SetRemoteNodeIndex(node_index);
        auto *channel = dss_nodes_[node_index].Channel();

        // send request
        remote::DataStoreRpcService_Stub stub(channel);
        stub.BatchWriteRecords(closure->Controller(),
                               closure->RemoteRequest(),
                               closure->RemoteResponse(),
                               closure);
    }
}

std::string DataStoreServiceClient::SerializeTxRecord(
    bool is_deleted, const txservice::TxRecord *rec)
{
    std::string record;
    record.append(reinterpret_cast<const char *>(&is_deleted), sizeof(bool));
    if (is_deleted)
    {
        return record;
    }
    rec->Serialize(record);
    return record;
}

void DataStoreServiceClient::SerializeTxRecord(
    bool is_deleted,
    const txservice::TxRecord *rec,
    std::vector<size_t> &record_tmp_mem_area,
    std::vector<std::string_view> &record_parts,
    size_t &write_batch_size)
{
    static const bool deleted = true;
    static const bool not_deleted = false;
    if (is_deleted)
    {
        record_parts.emplace_back(reinterpret_cast<const char *>(&deleted),
                                  sizeof(bool));
        write_batch_size += sizeof(bool);
        record_parts.emplace_back(std::string_view());  // unpack_info_size
        record_parts.emplace_back(std::string_view());  // unpack_info_data
        record_parts.emplace_back(std::string_view());  // encoded_blob_size
        record_parts.emplace_back(std::string_view());  // encoded_blob_data
    }
    else
    {
        record_parts.emplace_back(std::string_view(
            reinterpret_cast<const char *>(&not_deleted), sizeof(bool)));
        write_batch_size += sizeof(bool);
        SerializeTxRecord(
            rec, record_tmp_mem_area, record_parts, write_batch_size);
    }
}

void DataStoreServiceClient::SerializeTxRecord(
    const txservice::TxRecord *rec,
    std::vector<size_t> &record_tmp_mem_area,
    std::vector<std::string_view> &record_parts,
    size_t &write_batch_size)
{
    // Here copy the similar logic as EloqRecord Serialize function
    // for best of performance.
    record_tmp_mem_area.emplace_back(rec->UnpackInfoSize());
    size_t *unpack_info_size = &record_tmp_mem_area.back();
    record_parts.emplace_back(std::string_view(
        reinterpret_cast<const char *>(unpack_info_size), sizeof(size_t)));
    write_batch_size += sizeof(size_t);
    record_parts.emplace_back(rec->UnpackInfoData(), rec->UnpackInfoSize());
    write_batch_size += rec->UnpackInfoSize();
    record_tmp_mem_area.emplace_back(rec->EncodedBlobSize());
    uint64_t *encoded_blob_size = &record_tmp_mem_area.back();
    record_parts.emplace_back(std::string_view(
        reinterpret_cast<const char *>(encoded_blob_size), sizeof(size_t)));
    write_batch_size += sizeof(size_t);
    record_parts.emplace_back(rec->EncodedBlobData(), rec->EncodedBlobSize());
    write_batch_size += rec->EncodedBlobSize();
}

bool DataStoreServiceClient::DeserializeTxRecordStr(
    const std::string_view record, bool &is_deleted, size_t &offset)
{
    if (record.size() < (offset + sizeof(bool)))
    {
        return false;
    }

    is_deleted = *reinterpret_cast<const bool *>(record.data() + offset);
    offset += sizeof(bool);
    return true;
}

bool DataStoreServiceClient::InitPreBuiltTables()
{
    int32_t partition_id = 0;
    uint64_t table_version = 100U;
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;

    // Only need to store table catalog to catalog tables.
    for (const auto &[table_name, kv_table_name] : pre_built_table_names_)
    {
        auto tbl_sv = table_name.StringView();
        // check if the table is initialized
        txservice::TableName tablename(tbl_sv,
                                       txservice::TableType::Primary,
                                       txservice::TableEngine::EloqSql);
        std::string catalog_image;
        bool found = false;
        uint64_t version_ts = 0;
        if (!FetchTable(tablename, catalog_image, found, version_ts))
        {
            LOG(WARNING) << "InitPreBuiltTables failed on fetching table.";
            return false;
        }
        if (found)
        {
            assert(catalog_image.size() > 0);
            // update kv_table_name
            // eloqkv catalog image only store kv_table_name.
            pre_built_table_names_.at(table_name) = catalog_image;
            continue;
        }

        if (!table_name.IsHashPartitioned())
        {
            // init table last range partition id
            bool ok = InitTableRanges(tablename, table_version);
            ok &&InitTableLastRangePartitionId(tablename);
            if (!ok)
            {
                LOG(ERROR)
                    << "InitPreBuiltTables failed on initing table ranges.";
                return false;
            }
        }

        // write catalog to kvstore
        keys.emplace_back(tbl_sv);
        records.emplace_back(kv_table_name);
        records_ts.emplace_back(table_version);
        records_ttl.emplace_back(0);
        op_types.emplace_back(WriteOpType::PUT);
    }

    if (!keys.empty())
    {
        // write init catalog to kvstore
        SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
        PoolableGuard guard(callback_data);
        callback_data->Reset();
        BatchWriteRecords(kv_table_catalogs_name,
                          partition_id,
                          std::move(keys),
                          std::move(records),
                          std::move(records_ts),
                          std::move(records_ttl),
                          std::move(op_types),
                          false,
                          callback_data,
                          &SyncCallback);
        callback_data->Wait();

        if (callback_data->Result().error_code() !=
            remote::DataStoreError::NO_ERROR)
        {
            LOG(WARNING) << "InitPreBuiltTables failed" << std::endl;
            return false;
        }
    }

    return true;
}

void DataStoreServiceClient::UpsertTable(UpsertTableData *table_data)
{
    std::unique_ptr<UpsertTableData> data_guard(table_data);

    txservice::OperationType op_type = table_data->op_type_;
    auto *table_schema =
        op_type == txservice::OperationType::DropTable ||
                op_type == txservice::OperationType::TruncateTable
            ? table_data->old_table_schema_
            : table_data->new_table_schema_;

    const txservice::TableName &base_table_name =
        table_schema->GetBaseTableName();
    const txservice::KVCatalogInfo *kv_info = table_schema->GetKVCatalogInfo();
    auto *alter_table_info = table_data->alter_table_info_;

    bool ok = true;
    if (op_type == txservice::OperationType::CreateTable)
    {
        // 1- Create kv tables of base and indexes
        // (skip this step for all table data are stored in one cf.)

        // 2- Init table ranges
        if (!base_table_name.IsHashPartitioned())
        {
            // Only range partitioned base table needs to initialize range id.
            ok =
                ok && InitTableRanges(base_table_name, table_schema->Version());
        }
        // sk tables are always range partitioned.
        ok = ok &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this, table_schema](
                     const std::pair<txservice::TableName, std::string> &p)
                 { return InitTableRanges(p.first, table_schema->Version()); });

        // 3- Upsert table catalog

        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::Update)
    {
        // only update catalog info.
        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::AddIndex)
    {
        assert(alter_table_info);
        // 1- Create kv table of new index
        // (skip this step for all table data are stored in one cf.)

        // 2- Init table ranges
        // sk index tables are always range partitioned.
        ok = ok &&
             std::all_of(
                 alter_table_info->index_add_names_.begin(),
                 alter_table_info->index_add_names_.end(),
                 [this, table_schema](
                     const std::pair<txservice::TableName, std::string> &p)
                 { return InitTableRanges(p.first, table_schema->Version()); });
        // 3- Upsert table catalog
        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::DropIndex)
    {
        assert(alter_table_info);
        // 1- Drop kv table of indexes
        ok = ok &&
             std::all_of(
                 alter_table_info->index_drop_names_.begin(),
                 alter_table_info->index_drop_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DropKvTable(p.second); });

        // 2- Delete table ranges of the dropped index
        // sk index tables are always range partitioned.
        ok = ok &&
             std::all_of(
                 alter_table_info->index_drop_names_.begin(),
                 alter_table_info->index_drop_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableRanges(p.first); });

        // 3- Upsert table catalog
        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::DropTable)
    {
        // 1- Drop kv tables of base and index tables
        ok = ok && DropKvTable(kv_info->kv_table_name_) &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DropKvTable(p.second); });

        // 2- Delete table ranges of  base and index tables
        if (!base_table_name.IsHashPartitioned())
        {
            ok = ok && DeleteTableRanges(base_table_name);
        }
        ok = ok &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableRanges(p.first); });

        // 4- Delete table statistics
        ok = ok && DeleteTableStatistics(base_table_name);

        // 5- Delete table catalog
        ok = ok && DeleteCatalog(base_table_name, table_data->commit_ts_);
    }
    else if (op_type == txservice::OperationType::TruncateTable)
    {
        // 1- Drop kv tables of base table
        assert(kv_info->kv_index_names_.empty());
        ok = ok && DropKvTable(kv_info->kv_table_name_);

        // 2- Reset table ranges of  base and index tables
        if (!base_table_name.IsHashPartitioned())
        {
            ok = ok && DeleteTableRanges(base_table_name);
        }
        ok = ok &&
             std::all_of(
                 kv_info->kv_index_names_.begin(),
                 kv_info->kv_index_names_.end(),
                 [this](const std::pair<txservice::TableName, std::string> &p)
                 { return DeleteTableRanges(p.first); });

        if (alter_table_info)
        {
            auto *new_table_schema = table_data->new_table_schema_;
            ok = ok &&
                 std::all_of(
                     alter_table_info->index_add_names_.begin(),
                     alter_table_info->index_add_names_.end(),
                     [this, new_table_schema](
                         const std::pair<txservice::TableName, std::string> &p)
                     {
                         return InitTableRanges(p.first,
                                                new_table_schema->Version());
                     });
        }

        // 3- Delete table statistics
        ok = ok && DeleteTableStatistics(base_table_name);

        // 4- update table catalog
        ok = ok && UpsertCatalog(table_data->new_table_schema_,
                                 table_data->commit_ts_);
    }
    else
    {
        LOG(ERROR) << "UpsertTable: unknown operation type"
                   << " table name: " << base_table_name.StringView();
        assert(false);
    }

    if (ok)
    {
        table_data->SetFinished();
    }
    else
    {
        table_data->SetError(txservice::CcErrorCode::DATA_STORE_ERR);
    }
}

// The store format of table catalog in kvstore is as follows:
//
// key: base_table_name
// value: catalog_image
bool DataStoreServiceClient::UpsertCatalog(
    const txservice::TableSchema *table_schema, uint64_t write_time)
{
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();

    // Save table catalog image
    const txservice::TableName &base_table_name =
        table_schema->GetBaseTableName();
    const std::string &catalog_image = table_schema->SchemaImage();
    int32_t partition_id = 0;

    keys.emplace_back(base_table_name.StringView());
    records.emplace_back(
        std::string_view(catalog_image.data(), catalog_image.size()));
    records_ts.emplace_back(write_time);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::PUT);

    BatchWriteRecords(kv_table_catalogs_name,
                      partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      callback_data,
                      &SyncCallback);

    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "UpsertCatalog: failed to upsert table catalog, error:"
                   << callback_data->Result().error_msg();
        return false;
    }

    return true;
}

bool DataStoreServiceClient::DeleteCatalog(
    const txservice::TableName &base_table_name, uint64_t write_time)
{
    std::vector<std::string_view> keys;
    std::vector<std::string_view> records;
    std::vector<uint64_t> records_ts;
    std::vector<uint64_t> records_ttl;
    std::vector<WriteOpType> op_types;
    SyncCallbackData *callback_data = sync_callback_data_pool_.NextObject();
    PoolableGuard guard(callback_data);
    callback_data->Reset();

    // Delete table catalog image
    int32_t partition_id = 0;

    keys.emplace_back(base_table_name.StringView());
    records.emplace_back(std::string_view());
    records_ts.emplace_back(write_time);
    records_ttl.emplace_back(0);  // no ttl
    op_types.emplace_back(WriteOpType::DELETE);

    BatchWriteRecords(kv_table_catalogs_name,
                      partition_id,
                      std::move(keys),
                      std::move(records),
                      std::move(records_ts),
                      std::move(records_ttl),
                      std::move(op_types),
                      false,
                      callback_data,
                      &SyncCallback);

    callback_data->Wait();
    if (callback_data->Result().error_code() !=
        remote::DataStoreError::NO_ERROR)
    {
        LOG(ERROR) << "DeleteCatalog: failed to upsert table catalog";
        return false;
    }

    return true;
}

void DataStoreServiceClient::PreparePartitionBatches(
    EloqDS::PartitionFlushState &partition_state,
    const std::vector<std::pair<size_t, size_t>> &flush_recs,
    const std::vector<std::unique_ptr<txservice::FlushTaskEntry>> &entries,
    const txservice::TableName &table_name,
    uint16_t parts_cnt_per_key,
    uint16_t parts_cnt_per_record,
    uint64_t now)
{
    size_t write_batch_size = 0;
    PartitionBatchRequest batch_request;
    batch_request.Reset(
        parts_cnt_per_key, parts_cnt_per_record, flush_recs.size());
    auto PrepareObjectData = [&](txservice::FlushRecord &ckpt_rec,
                                 size_t &batch_size,
                                 PartitionBatchRequest &batch_request)
    {
        txservice::TxKey tx_key = ckpt_rec.Key();
        uint64_t ttl =
            ckpt_rec.payload_status_ == txservice::RecordStatus::Normal
                ? ckpt_rec.Payload()->GetTTL()
                : 0;
        if (ckpt_rec.payload_status_ == txservice::RecordStatus::Normal &&
            (!ckpt_rec.Payload()->HasTTL() || ttl > now))
        {
            batch_request.key_parts.emplace_back(
                std::string_view(tx_key.Data(), tx_key.Size()));
            batch_size += tx_key.Size();

            const txservice::TxRecord *rec = ckpt_rec.Payload();
            batch_request.record_parts.emplace_back(std::string_view(
                rec->EncodedBlobData(), rec->EncodedBlobSize()));
            batch_size += rec->EncodedBlobSize();

            batch_request.records_ts.push_back(ckpt_rec.commit_ts_);
            batch_size += sizeof(uint64_t);

            batch_request.records_ttl.push_back(ttl);
            batch_size += sizeof(uint64_t);

            batch_request.op_types.push_back(WriteOpType::PUT);
            batch_size += sizeof(WriteOpType);
        }
        else
        {
            batch_request.key_parts.emplace_back(
                std::string_view(tx_key.Data(), tx_key.Size()));
            batch_size += tx_key.Size();

            batch_request.record_parts.emplace_back(std::string_view());
            batch_size += 0;

            batch_request.records_ts.push_back(ckpt_rec.commit_ts_);
            batch_size += sizeof(uint64_t);

            batch_request.records_ttl.push_back(0);
            batch_size += sizeof(uint64_t);

            batch_request.op_types.push_back(WriteOpType::DELETE);
            batch_size += sizeof(WriteOpType);
        }
    };

    auto PrepareRecordData = [&](txservice::FlushRecord &ckpt_rec,
                                 size_t &batch_size,
                                 PartitionBatchRequest &batch_request)
    {
        uint64_t retired_ttl_for_deleted = now + 24 * 60 * 60 * 1000;
        txservice::TxKey tx_key = ckpt_rec.Key();
        bool is_deleted =
            !(ckpt_rec.payload_status_ == txservice::RecordStatus::Normal);
        batch_request.key_parts.emplace_back(
            std::string_view(tx_key.Data(), tx_key.Size()));
        batch_size += tx_key.Size();

        const txservice::TxRecord *rec = ckpt_rec.Payload();
        if (is_deleted)
        {
            batch_request.records_ttl.push_back(retired_ttl_for_deleted);
        }
        else
        {
            batch_request.records_ttl.push_back(0);
        }
        batch_size += sizeof(uint64_t);

        batch_request.op_types.push_back(WriteOpType::PUT);
        batch_size += sizeof(WriteOpType);

        SerializeTxRecord(is_deleted,
                          rec,
                          batch_request.record_tmp_mem_area,
                          batch_request.record_parts,
                          batch_size);

        batch_request.records_ts.push_back(ckpt_rec.commit_ts_);
        batch_size += sizeof(uint64_t);
    };

    // Process records and create batches
    for (auto idx : flush_recs)
    {
        txservice::FlushRecord &ckpt_rec =
            entries.at(idx.first)->data_sync_vec_->at(idx.second);

        // Start a new batch if size limit reached
        // or the record_tmp_mem_area is full. Since the record_parts is a
        // vector of string_view that references the record_tmp_mem_area, we
        // cannot allow the record_tmp_mem_area to be resized which will cause
        // the record_parts to be invalid.
        if (write_batch_size >= MAX_WRITE_BATCH_SIZE ||
            batch_request.record_tmp_mem_area.size() ==
                batch_request.record_tmp_mem_area.capacity())
        {
            partition_state.AddBatch(std::move(batch_request));

            batch_request.Reset(
                parts_cnt_per_key, parts_cnt_per_record, flush_recs.size());
            write_batch_size = 0;
        }

        assert(ckpt_rec.payload_status_ == txservice::RecordStatus::Normal ||
               ckpt_rec.payload_status_ == txservice::RecordStatus::Deleted);

        if (table_name.IsObjectTable())
        {
            PrepareObjectData(ckpt_rec, write_batch_size, batch_request);
        }
        else
        {
            PrepareRecordData(ckpt_rec, write_batch_size, batch_request);
        }
    }

    // Add the last batch if it has data
    if (batch_request.key_parts.size() > 0)
    {
        partition_state.AddBatch(std::move(batch_request));
    }
}

void DataStoreServiceClient::PrepareRangePartitionBatches(
    EloqDS::PartitionFlushState &partition_state,
    const std::vector<size_t> &flush_recs,
    const std::vector<std::unique_ptr<txservice::FlushTaskEntry>> &entries,
    const txservice::TableName &table_name,
    uint16_t parts_cnt_per_key,
    uint16_t parts_cnt_per_record,
    uint64_t now)
{
    size_t write_batch_size = 0;
    PartitionBatchRequest batch_request;

    bool enabled_mvcc =
        txservice::Sharder::Instance().GetLocalCcShards()->EnableMvcc();

    auto PrepareRecordData = [&](txservice::FlushRecord &ckpt_rec,
                                 size_t &batch_size,
                                 PartitionBatchRequest &batch_request)
    {
        uint64_t retired_ttl_for_deleted = now + 24 * 60 * 60 * 1000;
        txservice::TxKey tx_key = ckpt_rec.Key();
        bool is_deleted =
            !(ckpt_rec.payload_status_ == txservice::RecordStatus::Normal);
        batch_request.key_parts.emplace_back(
            std::string_view(tx_key.Data(), tx_key.Size()));
        batch_size += tx_key.Size();

        const txservice::TxRecord *rec = ckpt_rec.Payload();
        if (is_deleted)
        {
            batch_request.records_ttl.push_back(retired_ttl_for_deleted);
        }
        else
        {
            batch_request.records_ttl.push_back(0);
        }
        batch_size += sizeof(uint64_t);

        if (is_deleted && !enabled_mvcc)
        {
            batch_request.op_types.push_back(WriteOpType::DELETE);
        }
        else
        {
            batch_request.op_types.push_back(WriteOpType::PUT);
        }
        batch_size += sizeof(WriteOpType);

        SerializeTxRecord(is_deleted,
                          rec,
                          batch_request.record_tmp_mem_area,
                          batch_request.record_parts,
                          batch_size);

        batch_request.records_ts.push_back(ckpt_rec.commit_ts_);
        batch_size += sizeof(uint64_t);
    };

    size_t rec_cnt = 0;
    for (auto idx : flush_recs)
    {
        rec_cnt += entries.at(idx)->data_sync_vec_->size();
    }
    batch_request.Reset(parts_cnt_per_key, parts_cnt_per_record, rec_cnt);

    // Process records and create batches
    for (auto idx : flush_recs)
    {
        for (auto &ckpt_rec : *entries.at(idx)->data_sync_vec_)
        {
            // Start a new batch if size limit reached
            // or the record_tmp_mem_area is full. Since the record_parts is a
            // vector of string_view that references the record_tmp_mem_area, we
            // cannot allow the record_tmp_mem_area to be resized which will
            // cause the record_parts to be invalid.
            if (write_batch_size >= MAX_WRITE_BATCH_SIZE ||
                batch_request.record_tmp_mem_area.size() ==
                    batch_request.record_tmp_mem_area.capacity())
            {
                partition_state.AddBatch(std::move(batch_request));

                batch_request.Reset(
                    parts_cnt_per_key, parts_cnt_per_record, rec_cnt);
                write_batch_size = 0;
            }

            assert(
                ckpt_rec.payload_status_ == txservice::RecordStatus::Normal ||
                ckpt_rec.payload_status_ == txservice::RecordStatus::Deleted);

            // Currently there is no object table in range partitioned table
            PrepareRecordData(ckpt_rec, write_batch_size, batch_request);
        }
    }

    // Add the last batch if it has data
    if (batch_request.key_parts.size() > 0)
    {
        partition_state.AddBatch(std::move(batch_request));
    }
}

}  // namespace EloqDS