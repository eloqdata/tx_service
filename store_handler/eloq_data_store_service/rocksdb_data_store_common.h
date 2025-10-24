
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

#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <rocksdb/slice.h>

#include <cassert>
#include <chrono>
#include <string>

#include "data_store.h"
#include "data_store_service.h"
#include "rocksdb_config.h"

namespace EloqDS
{

// Key separator for building key in RocksDB
static constexpr char KEY_SEPARATOR[] = "/";

// Use most significant bit (MSB) of version_ts to indicate if the ttl is set
constexpr uint64_t MSB = 1ULL << 63;  // Mask for Bit 63
constexpr uint64_t MSB_MASK = ~MSB;   // Mask to clear Bit 63

class TTLCompactionFilter : public rocksdb::CompactionFilter
{
public:
    bool Filter(int level,
                const rocksdb::Slice &key,
                const rocksdb::Slice &existing_value,
                std::string *new_value,
                bool *value_changed) const override;

    const char *Name() const override;
};

// RocksDBEventListener is used to listen the flush event of RocksDB for
// recording unexpected write slow and stall when flushing
class RocksDBEventListener : public rocksdb::EventListener
{
public:
    void OnCompactionBegin(rocksdb::DB *db,
                           const rocksdb::CompactionJobInfo &ci) override;

    void OnCompactionCompleted(rocksdb::DB *db,
                               const rocksdb::CompactionJobInfo &ci) override;

    void OnFlushBegin(rocksdb::DB *db,
                      const rocksdb::FlushJobInfo &flush_job_info) override;

    void OnFlushCompleted(rocksdb::DB *db,
                          const rocksdb::FlushJobInfo &flush_job_info) override;

    std::string GetFlushReason(rocksdb::FlushReason flush_reason);
};

/**
 * @brief Wrapper class for cache iterator with TTL at the data store service to
 * avoid creating iterator for each scan request from client.
 */
class RocksDBIteratorTTLWrapper : public TTLWrapper
{
public:
    explicit RocksDBIteratorTTLWrapper(rocksdb::Iterator *iter) : iter_(iter)
    {
    }

    ~RocksDBIteratorTTLWrapper()
    {
        delete iter_;
    }

    rocksdb::Iterator *GetIter()
    {
        return iter_;
    }

private:
    rocksdb::Iterator *iter_;
};

class RocksDBDataStoreCommon : public DataStore
{
public:
    RocksDBDataStoreCommon(const EloqDS::RocksDBConfig &config,
                           bool create_if_missing,
                           bool tx_enable_cache_replacement,
                           uint32_t shard_id,
                           DataStoreService *data_store_service)
        : DataStore(shard_id, data_store_service),
          enable_stats_(config.enable_stats_),
          stats_dump_period_sec_(config.stats_dump_period_sec_),
          storage_path_(config.storage_path_ + "/ds_" +
                        std::to_string(shard_id)),
          max_write_buffer_number_(config.max_write_buffer_number_),
          max_background_jobs_(config.max_background_jobs_),
          max_background_flushes_(config.max_background_flush_),
          max_background_compactions_(config.max_background_compaction_),
          target_file_size_base_(config.target_file_size_base_bytes_),
          target_file_size_multiplier_(config.target_file_size_multiplier_),
          write_buff_size_(config.write_buffer_size_bytes_),
          use_direct_io_for_flush_and_compaction_(
              config.use_direct_io_for_flush_and_compaction_),
          use_direct_io_for_read_(config.use_direct_io_for_read_),
          level0_stop_writes_trigger_(config.level0_stop_writes_trigger_),
          level0_slowdown_writes_trigger_(
              config.level0_slowdown_writes_trigger_),
          level0_file_num_compaction_trigger_(
              config.level0_file_num_compaction_trigger_),
          max_bytes_for_level_base_(config.max_bytes_for_level_base_bytes_),
          max_bytes_for_level_multiplier_(
              config.max_bytes_for_level_multiplier_),
          compaction_style_(config.compaction_style_),
          soft_pending_compaction_bytes_limit_(
              config.soft_pending_compaction_bytes_limit_bytes_),
          hard_pending_compaction_bytes_limit_(
              config.hard_pending_compaction_bytes_limit_bytes_),
          max_subcompactions_(config.max_subcompactions_),
          write_rate_limit_(config.write_rate_limit_bytes_),
          batch_write_size_(config.batch_write_size_),
          periodic_compaction_seconds_(config.periodic_compaction_seconds_),
          dialy_offpeak_time_utc_(config.dialy_offpeak_time_utc_),
          db_path_(storage_path_ + "/db/"),
          ckpt_path_(storage_path_ + "/rocksdb_snapshot/"),
          backup_path_(storage_path_ + "/backups/"),
          received_snapshot_path_(storage_path_ + "/received_snapshot/"),
          create_db_if_missing_(create_if_missing),
          tx_enable_cache_replacement_(tx_enable_cache_replacement),
          ttl_compaction_filter_(nullptr)
    {
        info_log_level_ = StringToInfoLogLevel(config.info_log_level_);
        query_worker_pool_ =
            std::make_unique<ThreadWorkerPool>(config.query_worker_num_);
    }

    /**
     * @brief Initialize the data store.
     * @return True if connect successfully, otherwise false.
     */
    bool Initialize() override;

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param batch
     * to base table or skindex table in data store, stop and return false if
     * node_group is not longer leader.
     * @param flush_data_req The pointer of the request.
     */
    void FlushData(FlushDataRequest *flush_data_req) override;

    /**
     * @brief Write records to the data store.
     * @param batch_write_req The pointer of the request.
     */
    void DeleteRange(DeleteRangeRequest *delete_range_req) override;

    /**
     * @brief Write records to the data store.
     * @param batch_write_req The pointer of the request.
     */
    void Read(ReadRequest *req) override;

    /**
     * @brief Write records to the data store.
     * @param batch_write_req The pointer of the request.
     */
    void BatchWriteRecords(WriteRecordsRequest *batch_write_req) override;

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
     * @brief Scan records from the data store.
     */
    void ScanNext(ScanRequest *scan_req) override;

    /**
     * @brief Close scan operation.
     */
    void ScanClose(ScanRequest *scan_req) override;

    /**
     * @brief Swith this shard of data store to read only mode.
     */
    void SwitchToReadOnly() override;

    /**
     * @brief Swith this shard of data store to read write mode.
     */
    void SwitchToReadWrite() override;

    /**
     * @brief Get the shard status of this data store.
     * @return The shard status.
     */
    DSShardStatus FetchDSShardStatus() const;

protected:
#ifdef DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3
    virtual rocksdb::DBCloud *GetDBPtr() = 0;
#else
    virtual rocksdb::DB *GetDBPtr() = 0;
#endif

protected:
    const std::string BuildKey(const std::string_view table_name,
                               uint32_t partition_id,
                               const std::string_view key);
    const std::string BuildKeyForDebug(
        const std::unique_ptr<rocksdb::Slice[]> &key_slices, size_t slice_size);

    const std::string BuildKeyPrefix(const std::string_view table_name,
                                     uint32_t partition_id);
    void BuildKey(const std::string_view prefix,
                  const std::string_view key,
                  std::string &key_out);
    void BuildKeyPrefixSlices(
        const std::string_view table_name,
        const std::string_view partition_id,
        std::unique_ptr<rocksdb::Slice[]> &key_slices_out);

    void BuildKeySlices(std::unique_ptr<rocksdb::Slice[]> &key_slices_out,
                        const WriteRecordsRequest *batch_write_req,
                        const size_t &idx,
                        const uint16_t parts_cnt_per_key);
    bool DeserializeKey(const char *data,
                        const size_t size,
                        const std::string_view table_name,
                        const uint32_t partition_id,
                        std::string_view &key_out);
    void EncodeHasTTLIntoTs(uint64_t &ts, bool has_ttl);

    uint16_t TransformRecordToValueSlices(
        const WriteRecordsRequest *batch_write_req,
        const size_t &idx,
        const uint16_t parts_cnt_per_record,
        uint64_t &ts,
        const uint64_t &ttl,
        std::unique_ptr<rocksdb::Slice[]> &value_slices);
    void DecodeHasTTLFromTs(uint64_t &ts, bool &has_ttl);

    void DeserializeValueToRecord(const char *data,
                                  const size_t size,
                                  std::string &record,
                                  uint64_t &ts,
                                  uint64_t &ttl);

    rocksdb::InfoLogLevel StringToInfoLogLevel(
        const std::string &log_level_str);


protected:
    rocksdb::InfoLogLevel info_log_level_;
    const bool enable_stats_;
    const uint32_t stats_dump_period_sec_;
    const std::string storage_path_;
    const size_t max_write_buffer_number_;
    const size_t max_background_jobs_;
    const size_t max_background_flushes_;
    const size_t max_background_compactions_;
    const size_t target_file_size_base_;
    const size_t target_file_size_multiplier_;
    const size_t write_buff_size_;
    const bool use_direct_io_for_flush_and_compaction_;
    const bool use_direct_io_for_read_;
    const size_t level0_stop_writes_trigger_;
    const size_t level0_slowdown_writes_trigger_;
    const size_t level0_file_num_compaction_trigger_;
    const size_t max_bytes_for_level_base_;
    const size_t max_bytes_for_level_multiplier_;
    const std::string compaction_style_;
    const size_t soft_pending_compaction_bytes_limit_;
    const size_t hard_pending_compaction_bytes_limit_;
    const size_t max_subcompactions_;
    const size_t write_rate_limit_;
    const size_t batch_write_size_;
    const size_t periodic_compaction_seconds_;
    const std::string dialy_offpeak_time_utc_;
    const std::string db_path_;
    const std::string ckpt_path_;
    const std::string backup_path_;
    const std::string received_snapshot_path_;
    const bool create_db_if_missing_{false};
    bool tx_enable_cache_replacement_{true};
    std::unique_ptr<EloqDS::TTLCompactionFilter> ttl_compaction_filter_{
        nullptr};

    std::unique_ptr<ThreadWorkerPool> query_worker_pool_;
    std::shared_mutex db_mux_;
    std::mutex ddl_mux_;
};
}  // namespace EloqDS