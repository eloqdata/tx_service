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

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "cc_handler_result.h"
#include "cc_req_base.h"
#include "cc_req_misc.h"  // FetchRangeSlicesReq
#include "cc_request.pb.h"
#include "error_messages.h"
#include "range_slice.h"  // SplitRangeInfo
#include "sharder.h"      // NodeConfig
#include "store/data_store_scanner.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service_metrics.h"
#include "type.h"

namespace txservice
{
class TxService;
struct FetchCatalogCc;
struct FetchTableStatisticsCc;
struct FetchTableRangesCc;
struct SliceDataItem;
class StoreSlice;
struct FetchRecordCc;
struct FetchSnapshotCc;
struct FlushRecord;
struct FlushTaskEntry;

namespace store
{
enum class DataStoreDataType
{
    Blob,
    Numeric,
    String,
    Bool
};

struct DataStoreSearchCond
{
    DataStoreSearchCond(std::string field_name,
                        std::string op,
                        std::string val_str,
                        DataStoreDataType data_type)
        : field_name_(field_name),
          op_(op),
          val_str_(val_str),
          data_type_(data_type)
    {
    }

    std::string field_name_;
    std::string op_;
    std::string val_str_;
    DataStoreDataType data_type_;
};

class DataStoreHandler
{
public:
    enum struct DataStoreOpStatus
    {
        Success = 0,
        Retry,
        Error
    };

    virtual ~DataStoreHandler() = default;

    virtual bool Connect() = 0;

    virtual void ScheduleTimerTasks() {};
    /**
     * @brief flush entries in \@param flush_task to base table or skindex table
     * in data store, stop and return false if the flush failed after max retry
     * times.
     * @param flush_task - the flush task to be flushed. Map key is the kv table
     * name, value is the vector of flush task entries. PutAll only flushes the
     * data_sync_vec_ in each flush task entry.
     * @return whether all entries are written to data store successfully
     */
    virtual bool PutAll(std::unordered_map<
                        std::string_view,
                        std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
                            &flush_task) = 0;

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param batch
     * to base table or skindex table in data store, stop and return false if
     * node_group is not longer leader.
     * @param table_name base table name or sk index name
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual bool PersistKV(const std::vector<std::string> &kv_table_names)
    {
        return true;
    }

    virtual bool NeedPersistKV()
    {
        return false;
    }

    /**
     * @param write_time is used to maintain idempotence. For eventual
     * consistency storage, records with larger write_time wins. Cassandra
     * supports microsecond precision write_time, BigTable supports milliseconds
     * write_time, while DynamoDB and RocksDB don't support write_time. For
     * commit, write_time is equal to commit_ts. For rollback, there is no
     * commit_ts, and write_time is equal max(last_valid_ts..., last_write_time)
     * + 1.
     */
    virtual void UpsertTable(
        const TableSchema *old_table_schema,
        const TableSchema *new_table_schema,
        OperationType op_type,
        uint64_t write_time,
        NodeGroupId ng_id,
        int64_t tx_term,
        CcHandlerResult<Void> *hd_res,
        const txservice::AlterTableInfo *alter_table_info = nullptr,
        CcRequestBase *cc_req = nullptr,
        CcShard *ccs = nullptr,
        CcErrorCode *err_code = nullptr) = 0;

    virtual void FetchTableCatalog(const TableName &ccm_table_name,
                                   FetchCatalogCc *fetch_cc) = 0;

    virtual void FetchTableRanges(FetchTableRangesCc *fetch_cc) = 0;

    virtual void FetchRangeSlices(FetchRangeSlicesReq *fetch_cc) = 0;

    /**
     * @brief Read a row from base table or skindex table in datastore with
     * specified key. Caller should pass in complete primary key or skindex key.
     */
    virtual bool Read(const txservice::TableName &table_name,
                      const txservice::TxKey &key,
                      txservice::TxRecord &rec,
                      bool &found,
                      uint64_t &version_ts,
                      const txservice::TableSchema *table_schema) = 0;

    // Fetch record from datastore asynchronously.
    virtual DataStoreOpStatus FetchRecord(
        FetchRecordCc *fetch_cc, FetchSnapshotCc *fetch_snapshot_cc = nullptr)
    {
        assert(false);
        return DataStoreOpStatus::Error;
    }

    virtual bool FetchTable(const TableName &table_name,
                            std::string &schema_image,
                            bool &found,
                            uint64_t &version_ts) = 0;

    bool FetchTable(const txservice::TableName &table_name,
                    std::string &schema_image,
                    bool &found)
    {
        uint64_t version_ts;
        return FetchTable(table_name, schema_image, found, version_ts);
    }

    virtual void FetchCurrentTableStatistics(
        const TableName &ccm_table_name, FetchTableStatisticsCc *fetch_cc) = 0;

    virtual void FetchTableStatistics(const TableName &ccm_table_name,
                                      FetchTableStatisticsCc *fetch_cc) = 0;

    virtual bool UpsertTableStatistics(
        const TableName &ccm_table_name,
        const std::unordered_map<TableName,
                                 std::pair<uint64_t, std::vector<TxKey>>>
            &sample_pool_map,
        uint64_t version) = 0;

    virtual DataStoreOpStatus LoadRangeSlice(const TableName &table_name,
                                             const KVCatalogInfo *kv_info,
                                             uint32_t partition_id,
                                             FillStoreSliceCc *load_slice_req)
    {
        return DataStoreOpStatus::Error;
    }

    virtual bool UpdateRangeSlices(
        const txservice::TableName &table_name,
        uint64_t version,
        txservice::TxKey range_start_key,
        std::vector<const txservice::StoreSlice *> slices,
        int32_t partition_id,
        uint64_t range_version)
    {
        return false;
    }

    /**
     * @brief Upsert list of ranges into range table. This will also update
     * range slice sizes.
     */
    virtual bool UpsertRanges(const TableName &table_name,
                              std::vector<SplitRangeInfo> range_info,
                              uint64_t version)
    {
        return false;
    }

    virtual bool DiscoverAllTableNames(
        std::vector<std::string> &norm_name_vec,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) = 0;

    //-- database
    virtual bool UpsertDatabase(std::string_view db,
                                std::string_view definition) = 0;
    virtual bool DropDatabase(std::string_view db) = 0;
    virtual bool FetchDatabase(
        std::string_view db,
        std::string &definition,
        bool &found,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) = 0;
    virtual bool FetchAllDatabase(
        std::vector<std::string> &dbnames,
        const std::function<void()> *yield_fptr = nullptr,
        const std::function<void()> *resume_fptr = nullptr) = 0;

    virtual bool DropKvTable(const std::string &kv_table_name) = 0;

    virtual void DropKvTableAsync(const std::string &kv_table_name) = 0;

    virtual std::unique_ptr<DataStoreScanner> ScanForward(
        const TableName &table_name,
        uint32_t ng_id,
        const TxKey &start_key,
        bool inclusive,
        uint8_t key_parts,
        const std::vector<DataStoreSearchCond> &search_cond,
        const KeySchema *key_schema,
        const RecordSchema *rec_schema,
        const KVCatalogInfo *kv_info,
        bool scan_foward) = 0;

    /**
     * @brief Write batch historical versions into DataStore.
     */
    virtual bool PutArchivesAll(
        std::unordered_map<
            std::string_view,
            std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
            &flush_task) = 0;
    /**
     * @brief Copy record from base/sk table to mvcc_archives.
     */
    virtual bool CopyBaseToArchive(
        std::unordered_map<
            std::string_view,
            std::vector<std::unique_ptr<txservice::FlushTaskEntry>>>
            &flush_task) = 0;

    /**
     * @brief  Get the latest visible(commit_ts <= upper_bound_ts) historical
     * version.
     */
    virtual bool FetchVisibleArchive(const TableName &table_name,
                                     const txservice::KVCatalogInfo *kv_info,
                                     const TxKey &key,
                                     const uint64_t upper_bound_ts,
                                     TxRecord &rec,
                                     RecordStatus &rec_status,
                                     uint64_t &commit_ts) = 0;

    /**
     * @brief  Fetch all archives whose commit_ts >= from_ts.
     */
    virtual bool FetchArchives(
        const txservice::TableName &table_name,
        const txservice::KVCatalogInfo *kv_info,
        const txservice::TxKey &key,
        std::vector<txservice::VersionTxRecord> &archives,
        uint64_t from_ts) = 0;

    void SetTxService(TxService *tx_service)
    {
        tx_service_ = tx_service;
    }

    virtual bool DeleteOutOfRangeData(const txservice::TableName &table_name,
                                      int32_t partition_id,
                                      const TxKey *start_key,
                                      const TableSchema *table_schema) = 0;

    virtual std::string CreateKVCatalogInfo(
        const TableSchema *table_schema) const = 0;

    virtual KVCatalogInfo::uptr DeserializeKVCatalogInfo(
        const std::string &kv_info_str, size_t &offset) const = 0;

    virtual std::string CreateNewKVCatalogInfo(
        const txservice::TableName &table_name,
        const txservice::TableSchema *current_table_schema,
        txservice::AlterTableInfo &alter_table_info) = 0;

    void RegisterKvMetrics(metrics::MetricsRegistry *metrics_registry,
                           metrics::CommonLabels common_labels = {})
    {
        assert(metrics_registry);
        if (metrics::enable_kv_metrics)
        {
            metrics::kv_meter = std::make_unique<metrics::Meter>(
                metrics_registry, common_labels);
            metrics::kv_meter->Register(metrics::NAME_KV_FLUSH_ROWS_TOTAL,
                                        metrics::Type::Counter,
                                        {{"type", {"base", "archive"}}});
            metrics::kv_meter->Register(metrics::NAME_KV_LOAD_SLICE_TOTAL,
                                        metrics::Type::Counter);
            metrics::kv_meter->Register(metrics::NAME_KV_LOAD_SLICE_DURATION,
                                        metrics::Type::Histogram);
            metrics::kv_meter->Register(metrics::NAME_KV_READ_TOTAL,
                                        metrics::Type::Counter);
            metrics::kv_meter->Register(metrics::NAME_KV_READ_DURATION,
                                        metrics::Type::Histogram);
        }
    };

    virtual bool NeedCopyRange() const = 0;

    virtual bool ByPassDataStore() const
    {
        return false;
    }

    virtual bool IsSharedStorage() const
    {
        return true;
    }

    virtual bool CreateSnapshotForStandby(
        std::vector<std::string> &snapshot_files)
    {
        assert(false);
        return true;
    }

    virtual bool CreateSnapshotForBackup(const std::string &backup_name,
                                         std::vector<std::string> &backup_files,
                                         uint64_t backup_ts = 0)
    {
        assert(false);
        return true;
    }

    virtual bool SendSnapshotToRemote(uint32_t ng_id,
                                      int64_t ng_term,
                                      std::vector<std::string> &snapshot_files,
                                      const std::string &remote_dest)
    {
        assert(false);
        return true;
    }

    virtual bool RemoveBackupSnapshot(const std::string &backup_name)
    {
        assert(false);
        return true;
    }

    virtual bool OnSnapshotReceived(
        const txservice::remote::OnSnapshotSyncedRequest *req)
    {
        return true;
    }

    virtual void OnStartFollowing()
    {
    }

    virtual bool OnLeaderStart(uint32_t *next_leader_node)
    {
        return true;
    }

    virtual std::string SnapshotSyncDestPath() const
    {
        return std::string("");
    }

    virtual void OnShutdown()
    {
    }

    // Restore Tx service cc maps from KV store using pre built tables
    virtual void RestoreTxCache(NodeGroupId cc_ng_id, int64_t cc_ng_term)
    {
        assert(false);
    }

    virtual bool AppendPreBuiltTable(const txservice::TableName &table_name)
    {
        return true;
    }

protected:
    TxService *tx_service_{nullptr};
};
}  // namespace store
}  // namespace txservice
