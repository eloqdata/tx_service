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
#include <bthread/condition_variable.h>
#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store_service.h"
#include "ds_request.pb.h"
#include "internal_request.h"
#include "rocksdb_data_store.h"

namespace EloqDS
{

RocksDBDataStore::RocksDBDataStore(
    const EloqDS::RocksDBConfig &config,
    bool create_if_missing,
    bool tx_enable_cache_replacement,
    uint32_t shard_id,
    DataStoreService *data_store_service)
  : RocksDBDataStoreCommon(config,
                            create_if_missing,
                            tx_enable_cache_replacement,
                            shard_id,
                            data_store_service),
      db_(nullptr)
{
}

RocksDBDataStore::~RocksDBDataStore()
{
    if (query_worker_pool_ != nullptr || data_store_service_ != nullptr ||
        db_ != nullptr)
    {
        Shutdown();
    }
}

void RocksDBDataStore::Shutdown()
{
    std::unique_lock<std::shared_mutex> db_lk(db_mux_);
    DLOG(INFO) << "Shutting down RocksDBDataStore";

    // shutdown query worker pool
    query_worker_pool_->Shutdown();
    query_worker_pool_ = nullptr;

    data_store_service_->ForceEraseScanIters(shard_id_);
    data_store_service_ = nullptr;

    if (db_ != nullptr)
    {
        DLOG(INFO) << "Closing RocksDB at path: " << storage_path_;
        db_->Close();
        delete db_;
        db_ = nullptr;
        ttl_compaction_filter_ = nullptr;
    }
}

bool RocksDBDataStore::StartDB()
{
    if (db_)
    {
        // db is already started, no op
        DLOG(INFO) << "DB already started";
        return true;
    }

    rocksdb::Options options;
    options.create_if_missing = create_db_if_missing_;
    options.create_missing_column_families = true;
    // boost write performance by enabling unordered write
    options.unordered_write = true;

    // print db statistics every 60 seconds
    if (enable_stats_)
    {
        options.statistics = rocksdb::CreateDBStatistics();
        options.stats_dump_period_sec = stats_dump_period_sec_;
    }

    // Max background jobs number, rocksdb will auto turn max flush(1/4 of
    // max_background_jobs) and compaction jobs(3/4 of max_background_jobs)
    if (max_background_jobs_ > 0)
    {
        options.max_background_jobs = max_background_jobs_;
    }

    if (max_background_flushes_ > 0)
    {
        options.max_background_flushes = max_background_flushes_;
    }

    if (max_background_compactions_ > 0)
    {
        options.max_background_compactions = max_background_compactions_;
    }

    options.use_direct_io_for_flush_and_compaction =
        use_direct_io_for_flush_and_compaction_;
    options.use_direct_reads = use_direct_io_for_read_;

    // Set compation style
    if (compaction_style_ == "universal")
    {
        LOG(WARNING)
            << "Universal compaction has a size limitation. Please be careful "
               "when your DB (or column family) size is over 100GB";
        options.compaction_style = rocksdb::kCompactionStyleUniversal;
    }
    else if (compaction_style_ == "level")
    {
        options.compaction_style = rocksdb::kCompactionStyleLevel;
    }
    else if (compaction_style_ == "fifo")
    {
        LOG(ERROR) << "FIFO compaction style should not be used";
        std::abort();
    }
    else
    {
        LOG(ERROR) << "Invalid compaction style: " << compaction_style_;
        std::abort();
    }

    // set the max subcompactions
    if (max_subcompactions_ > 0)
    {
        options.max_subcompactions = max_subcompactions_;
    }

    // set the write rate limit
    if (write_rate_limit_ > 0)
    {
        options.rate_limiter.reset(
            rocksdb::NewGenericRateLimiter(write_rate_limit_));
    }

    options.info_log_level = info_log_level_;
    options.best_efforts_recovery = false;
    options.skip_checking_sst_file_sizes_on_db_open = true;
    options.skip_stats_update_on_db_open = true;
    // Important! keep atomic_flush true, since we disabled WAL
    options.atomic_flush = true;
    auto db_event_listener = std::make_shared<RocksDBEventListener>();
    options.listeners.emplace_back(db_event_listener);

    // The following two configuration items are setup for purpose of removing
    // expired kv data items according to their ttl Rocksdb will compact all sst
    // files which are older than periodic_compaction_seconds_ at
    // dialy_offpeak_time_utc_ Then all kv data items in the sst files will go
    // through the TTLCompactionFilter which is configurated for column family
    options.periodic_compaction_seconds = periodic_compaction_seconds_;
    options.daily_offpeak_time_utc = dialy_offpeak_time_utc_;

    if (target_file_size_base_ > 0)
    {
        options.target_file_size_base = target_file_size_base_;
    }

    if (target_file_size_multiplier_ > 0)
    {
        options.target_file_size_multiplier = target_file_size_multiplier_;
    }

    // mem table size
    if (write_buff_size_ > 0)
    {
        options.write_buffer_size = write_buff_size_;
    }
    // Max write buffer number
    if (max_write_buffer_number_ > 0)
    {
        options.max_write_buffer_number = max_write_buffer_number_;
    }

    if (level0_slowdown_writes_trigger_ > 0)
    {
        options.level0_slowdown_writes_trigger =
            level0_slowdown_writes_trigger_;
    }

    if (level0_stop_writes_trigger_ > 0)
    {
        options.level0_stop_writes_trigger = level0_stop_writes_trigger_;
    }

    if (level0_file_num_compaction_trigger_ > 0)
    {
        options.level0_file_num_compaction_trigger =
            level0_file_num_compaction_trigger_;
    }

    if (soft_pending_compaction_bytes_limit_ > 0)
    {
        options.soft_pending_compaction_bytes_limit =
            soft_pending_compaction_bytes_limit_;
    }

    if (hard_pending_compaction_bytes_limit_ > 0)
    {
        options.hard_pending_compaction_bytes_limit =
            hard_pending_compaction_bytes_limit_;
    }

    if (max_bytes_for_level_base_ > 0)
    {
        options.max_bytes_for_level_base = max_bytes_for_level_base_;
    }

    if (max_bytes_for_level_multiplier_ > 0)
    {
        options.max_bytes_for_level_multiplier =
            max_bytes_for_level_multiplier_;
    }

    // set ttl compaction filter
    assert(ttl_compaction_filter_ == nullptr);
    ttl_compaction_filter_ = std::make_unique<EloqDS::TTLCompactionFilter>();

    options.compaction_filter =
        static_cast<rocksdb::CompactionFilter *>(ttl_compaction_filter_.get());

    auto start = std::chrono::system_clock::now();
    std::unique_lock<std::shared_mutex> db_lk(db_mux_);
    auto status = rocksdb::DB::Open(options, db_path_, &db_);

    auto end = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    DLOG(INFO) << "DB Open took " << duration.count() << " ms";

    if (!status.ok())
    {
        ttl_compaction_filter_ = nullptr;

        LOG(ERROR) << "Unable to open db at path " << storage_path_
                   << " with error: " << status.ToString();

        // db does not exist. This node cannot escalate to be the ng leader.
        return false;
    }

    LOG(INFO) << "RocksDB started";
    return true;
}

rocksdb::DB *RocksDBDataStore::GetDBPtr()
{
    return db_;
}

}  // namespace EloqDS