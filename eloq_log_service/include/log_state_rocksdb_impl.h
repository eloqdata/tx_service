#pragma once

#include <cstdint>

#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_ALL)

#include <rocksdb/db.h>
#include <rocksdb/listener.h>

#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include <condition_variable>
#include <deque>

#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb_cloud_config.h"

using current_time_func = std::function<std::time_t(std::time_t *arg)>;
#endif

#include "log_state.h"

namespace txlog
{
inline void Serialize(std::array<char, 20> &res,
                      uint64_t timestamp,
                      uint32_t ng_id,
                      uint64_t tx_number)
{
    char *p = res.data();
    uint64_t ts_be = __builtin_bswap64(timestamp);
    std::memcpy(p, &ts_be, sizeof(uint64_t));

    p += sizeof(uint64_t);
    uint32_t ng_id_be = __builtin_bswap32(ng_id);
    std::memcpy(p, &ng_id_be, sizeof(uint32_t));

    p += sizeof(uint32_t);
    uint64_t tx_no_be = __builtin_bswap64(tx_number);
    std::memcpy(p, &tx_no_be, sizeof(uint64_t));
}

inline void Deserialize(rocksdb::Slice key,
                        uint64_t &timestamp,
                        uint32_t &ng_id,
                        uint64_t &tx_number)
{
    if (key.size() != 20)
    {
        LOG(ERROR) << "Deserialize: unexpected key size " << key.size()
                   << " (expected 20); skipping record";
        timestamp = 0;
        ng_id = 0;
        tx_number = 0;
        return;
    }
    const char *p = key.data();
    uint64_t ts_be, tx_no_be;
    std::memcpy(&ts_be, p, sizeof(uint64_t));
    timestamp = __builtin_bswap64(ts_be);

    p += sizeof(uint64_t);
    uint32_t ng_id_be;
    std::memcpy(&ng_id_be, p, sizeof(uint32_t));
    ng_id = __builtin_bswap32(ng_id_be);

    p += sizeof(uint32_t);
    std::memcpy(&tx_no_be, p, sizeof(uint64_t));
    tx_number = __builtin_bswap64(tx_no_be);
}

class ItemIteratorRocksDBImpl : public ItemIterator
{
public:
    explicit ItemIteratorRocksDBImpl(size_t worker_num,
                                     std::vector<Item::Pointer> &&ddl_list,
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
                                     rocksdb::DBCloud *db,
#else
                                     rocksdb::DB *db,
#endif
                                     uint64_t start_ts,
                                     uint32_t target_ng)
        : ItemIterator(std::move(ddl_list)),
          db_(db),
          start_key_(start_key_storage_.data(), start_key_storage_.size()),
          worker_num_(worker_num == 0 ? 1 : worker_num)
    {
        Serialize(start_key_storage_, start_ts, target_ng, 0);
        rocksdb::ReadOptions read_options;
        // set iterate_lower_bound for read_options for better performance
        read_options.iterate_lower_bound = &start_key_;
        //        read_options.readahead_size = 4 << 20;
        rocksdb_iterator_ =
            std::unique_ptr<rocksdb::Iterator>(db_->NewIterator(read_options));
        uint64_t last_ts = 0;
        uint32_t tmp_ng;
        uint64_t tmp_txn;
        rocksdb_iterator_->SeekToLast();
        if (!rocksdb_iterator_->Valid())
        {
            return;
        }
        keys_storage_.reserve(worker_num_ * 2);
        keys_.reserve(worker_num_ * 2);
        Deserialize(rocksdb_iterator_->key(), last_ts, tmp_ng, tmp_txn);
        // Use start_ts (the caller's requested lower bound) as the first
        // partition boundary so that per-worker iterators do not replay records
        // older than requested.
        std::vector<uint64_t> ts_list;
        ts_list.push_back(start_ts);
        auto gap = last_ts >= start_ts ? (last_ts - start_ts) / worker_num_ : 0;
        for (size_t i = 1; i < worker_num_; i++)
        {
            ts_list.push_back(start_ts + gap * i);
        }
        ts_list.push_back(last_ts);
        for (size_t i = 0; i < ts_list.size() - 1; i++)
        {
            rocksdb::ReadOptions read_options;
            std::array<char, 20> start_key{};
            // range start: current_ts, target_ng, txn number 0
            Serialize(start_key, ts_list[i], target_ng, 0);
            keys_storage_.push_back(start_key);
            keys_.push_back(rocksdb::Slice(keys_storage_.back().data(),
                                           keys_storage_.back().size()));
            read_options.iterate_lower_bound = &keys_.back();

            if (i != ts_list.size() - 2)
            {
                std::array<char, 20> end_key{};
                // range end: next_ts - 1, target_ng, txn number UINT64_MAX
                Serialize(end_key, ts_list[i + 1] - 1, target_ng, UINT64_MAX);
                keys_storage_.push_back(end_key);
                keys_.push_back(rocksdb::Slice(keys_storage_.back().data(),
                                               keys_storage_.back().size()));
                read_options.iterate_upper_bound = &keys_.back();
            }
            rocksdb_iterators_.push_back(std::unique_ptr<rocksdb::Iterator>(
                db_->NewIterator(read_options)));
        }
        items_.resize(worker_num_);
    }

    ~ItemIteratorRocksDBImpl() override = default;

    void SeekToFirst() override
    {
        ddl_idx_ = 0;
        rocksdb_iterator_->SeekToFirst();
    }

    bool Valid() override
    {
        if (ddl_idx_ < ddl_list_.size() || rocksdb_iterator_->Valid())
        {
            return true;
        }
        if (!rocksdb_iterator_->status().ok())
        {
            LOG(ERROR) << "RocksDB iterator failed: "
                       << rocksdb_iterator_->status().ToString();
        }
        LOG(INFO) << "total get item from rocksdb iterator: " << total;
        return false;
    };

    void Next() override
    {
        if (ddl_idx_ < ddl_list_.size())
        {
            ddl_idx_++;
        }
        else
        {
            rocksdb_iterator_->Next();
        }
    };

    const Item &GetItem() override
    {
        total++;
        if (ddl_idx_ < ddl_list_.size())
        {
            return *ddl_list_.at(ddl_idx_);
        }
        uint32_t ng;
        uint64_t timestamp, tx_number;
        Deserialize(rocksdb_iterator_->key(), timestamp, ng, tx_number);
        rocksdb::Slice value = rocksdb_iterator_->value();
        item_.tx_number_ = tx_number;
        item_.timestamp_ = timestamp;
        item_.log_message_ = {value.data(), value.size()};
        item_.item_type_ = LogItemType::DataLog;
        item_.cc_ng_ = ng;
        return item_;
    };

    size_t IteratorNum() override
    {
        return rocksdb_iterators_.size();
    }

    void SeekToFirst(size_t idx) override
    {
        rocksdb_iterators_[idx]->SeekToFirst();
    }

    bool Valid(size_t idx) override
    {
        if (rocksdb_iterators_[idx]->Valid())
        {
            return true;
        }
        if (!rocksdb_iterators_[idx]->status().ok())
        {
            LOG(ERROR) << "RocksDB iterator failed: "
                       << rocksdb_iterators_[idx]->status().ToString();
        }
        return false;
    }

    void Next(size_t idx) override
    {
        rocksdb_iterators_[idx]->Next();
    }

    const Item &GetItem(size_t idx) override
    {
        uint32_t ng;
        uint64_t timestamp, tx_number;
        Deserialize(rocksdb_iterators_[idx]->key(), timestamp, ng, tx_number);
        rocksdb::Slice value = rocksdb_iterators_[idx]->value();
        items_[idx].tx_number_ = tx_number;
        items_[idx].timestamp_ = timestamp;
        items_[idx].log_message_ = {value.data(), value.size()};
        items_[idx].item_type_ = LogItemType::DataLog;
        items_[idx].cc_ng_ = ng;
        return items_[idx];
    };

    void SeekToDDLFirst() override
    {
        ddl_idx_ = 0;
    }

    bool ValidDDL() override
    {
        return ddl_idx_ < ddl_list_.size();
    }

    void NextDDL() override
    {
        ddl_idx_++;
    }

    const Item &GetDDLItem() override
    {
        return *ddl_list_.at(ddl_idx_);
    }

private:
    int total{};
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    rocksdb::DBCloud *db_;
#else
    rocksdb::DB *db_;
#endif
    std::array<char, 20> start_key_storage_;
    rocksdb::Slice start_key_;
    std::unique_ptr<rocksdb::Iterator> rocksdb_iterator_;
    Item item_;
    std::vector<std::array<char, 20>> keys_storage_;
    std::vector<rocksdb::Slice> keys_;
    std::vector<std::unique_ptr<rocksdb::Iterator>> rocksdb_iterators_;
    std::vector<Item> items_;
    size_t worker_num_{1};
};

#if defined(LOG_STATE_TYPE_RKDB)

class LogStateRocksDBImpl : public LogState
{
public:
    explicit LogStateRocksDBImpl(const std::string &rocksdb_path,
                                 const size_t sst_files_size_limit,
                                 const size_t rocksdb_scan_threads);

    ~LogStateRocksDBImpl() override;

    void AddLogItem(uint32_t cc_ng_id,
                    uint64_t tx_number,
                    uint64_t timestamp,
                    const std::string &log_message) override;

    void AddLogItemBatch(uint32_t cc_ng_id,
                         uint64_t tx_number,
                         uint64_t timestamp,
                         const std::string &log_message) override;

    void FlushLogItemBatch() override;

    std::pair<bool, std::unique_ptr<ItemIterator>> GetLogReplayList(
        uint32_t ng_id, uint64_t start_timestamp) override;

    std::pair<bool, Item::Pointer> SearchTxDataLog(
        uint64_t tx_number,
        uint32_t ng_id,
        uint64_t lower_bound_ts = 0) override;

    void BeginSnapshot() override;

    /**
     * read and load snapshot files
     * @param snapshot_path
     * @param files
     */
    int ReadSnapshot(const std::string &snapshot_path,
                     const std::vector<std::string> &files) override;

    /**
     * write snapshot files to snapshot_path and return the relative filenames
     * @param snapshot_path
     * @return
     */
    std::vector<std::string> WriteSnapshot(
        const std::string &snapshot_path) override;

    /**
     * Start will be called twice if an instance crash and recover:
     * first in starting braft::node which calls
     * braft::StateMachine::on_snapshot_load, then in starting LogState. check
     * db_ to avoid openning RocksDB instance twice.
     */
    int Start() override;

    /**
     * DB purge thread function
     */
    void PurgingSstFiles();
    void NotifySstFileCreated(const rocksdb::FlushJobInfo &flush_job_info);
    uint64_t GetSstFilesSize();

    uint64_t GetApproximateReplayLogSize() override;

private:
    void StopRocksDB();

    static void PrintKey(rocksdb::Slice key);

    /**
     * close db_ and remove all files and dirs in rocksdb storage path but keep
     * the directory itself
     */
    void CloseAndClearRocksDB();

    /**
     * Stores log records for cc node groups.
     * Committed log records in the log state machine are organized by cc node
     * groups and within one group are ordered by commit timestamps.
     * This organization facilitates log truncation but is less efficient when
     * looking for the log record committed by a specific tx.
     */
    rocksdb::DB *db_;
    std::string rocksdb_storage_path_;
    rocksdb::WriteOptions write_option_;
    rocksdb::WriteBatch write_batch_;

    /**
     * Last truncate timestamp of every ng_id on this log group.
     */
    std::unordered_map<uint32_t, uint64_t> truncate_ts_;

    /**
     * The size of sst files in rocksdb storage path.
     */
    uint64_t sst_files_size_{0};
    /**
     * The max threshold of sst files size in rocksdb storage path.
     * If the size of sst files exceeds this threshold, the purge thread will
     * be triggered.
     */
    const size_t sst_files_size_limit_{0};

    const size_t rocksdb_scan_threads_{1};
    /**
     * The thread for dynamically calculating the size of sst files, and purge
     * sst files
     */
    std::thread purge_thread_;
    std::queue<rocksdb::FlushJobInfo> sst_created_queue_;
    bthread::Mutex sst_queue_mutex_;
    bthread::ConditionVariable sst_queue_cv_;
    uint64_t last_purging_sst_ckpt_ts_;
    std::atomic<bool> stop_purge_thread_{false};
};

class RocksDBEventListener : public rocksdb::EventListener
{
public:
    explicit RocksDBEventListener(LogStateRocksDBImpl *log_state)
        : log_state_(log_state)
    {
    }
    /**
     * Monitor the SST files creation
     */
    void OnFlushCompleted(rocksdb::DB *db,
                          const rocksdb::FlushJobInfo &flush_job_info) override;

private:
    std::string GetFlushReason(rocksdb::FlushReason flush_reason);
    LogStateRocksDBImpl *log_state_;
};

#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)

class CronJob
{
public:
    // default check/interval is 1 minute
    static constexpr uint32_t DEFAULT_CHECK_INTERVAL = 60;

    explicit CronJob(const std::string &job_name,
                     /*This function is used for unit test purpose*/
                     current_time_func current_time = std::time,
                     uint32_t interval_seconds = DEFAULT_CHECK_INTERVAL);

    void Start(uint32_t days_from_now,
               uint32_t starting_hour,
               uint32_t starting_minute,
               uint32_t starting_second,
               std::function<void(current_time_func)> job,
               bool repeatable = true);

    void Cancel();

private:
    /**
     * Calculate the next run time based on the current time and the starting
     * time.
     * - If all parameters (days_from_now, starting_hour, starting_minute,
     *   starting_second) are zero, uses interval_seconds_ for interval-based
     *   scheduling.
     * - Otherwise, uses the time-of-day parameters for daily scheduling.
     * @param days_from_now number of days from now to schedule the job
     * @param starting_hour hour of the day to schedule the job
     * @param starting_minute minute of the hour to schedule the job
     * @param starting_second second of the minute to schedule the job
     * @return the next run time in std::time_t format
     */
    std::time_t CalculateNextRunTime(uint32_t days_from_now,
                                     uint32_t starting_hour,
                                     uint32_t starting_minute,
                                     uint32_t starting_second);

    std::string job_name_;
    std::mutex wait_mutex_;
    std::condition_variable wait_cv_;
    std::atomic<bool> is_canceled_{false};
    std::atomic<bool> is_started_{false};
    current_time_func current_time_{std::time};
    uint32_t check_interval_seconds_;
    std::thread thd_;
};

// RocksDBEventListener is used to listen the flush event of RocksDB for
// recording unexpected write slow and stall when flushing
class RocksDBEventListener : public rocksdb::EventListener
{
public:
    void OnFlushCompleted(rocksdb::DB *db,
                          const rocksdb::FlushJobInfo &flush_job_info) override
    {
        if (flush_job_info.triggered_writes_slowdown ||
            flush_job_info.triggered_writes_stop)
        {
            LOG(INFO) << "Flush end, file: " << flush_job_info.file_path
                      << " ,job_id: " << flush_job_info.job_id
                      << " ,thread: " << flush_job_info.thread_id
                      << " ,file_number: " << flush_job_info.file_number
                      << " ,triggered_writes_slowdown: "
                      << flush_job_info.triggered_writes_slowdown
                      << " ,triggered_writes_stop: "
                      << flush_job_info.triggered_writes_stop
                      << " ,smallest_seqno: " << flush_job_info.smallest_seqno
                      << " ,largest_seqno: " << flush_job_info.largest_seqno
                      << " ,flush_reason: "
                      << GetFlushReason(flush_job_info.flush_reason);
        }
    }

    std::string GetFlushReason(rocksdb::FlushReason flush_reason)
    {
        switch (flush_reason)
        {
        case rocksdb::FlushReason::kOthers:
            return "kOthers";
        case rocksdb::FlushReason::kGetLiveFiles:
            return "kGetLiveFiles";
        case rocksdb::FlushReason::kShutDown:
            return "kShutDown";
        case rocksdb::FlushReason::kExternalFileIngestion:
            return "kExternalFileIngestion";
        case rocksdb::FlushReason::kManualCompaction:
            return "kManualCompaction";
        case rocksdb::FlushReason::kWriteBufferManager:
            return "kWriteBufferManager";
        case rocksdb::FlushReason::kWriteBufferFull:
            return "kWriteBufferFull";
        case rocksdb::FlushReason::kTest:
            return "kTest";
        case rocksdb::FlushReason::kDeleteFiles:
            return "kDeleteFiles";
        case rocksdb::FlushReason::kAutoCompaction:
            return "kAutoCompaction";
        case rocksdb::FlushReason::kManualFlush:
            return "kManualFlush";
        case rocksdb::FlushReason::kErrorRecovery:
            return "kErrorRecovery";
        case rocksdb::FlushReason::kErrorRecoveryRetryFlush:
            return "kErrorRecoveryRetryFlush";
        case rocksdb::FlushReason::kWalFull:
            return "kWalFull";
        default:
            return "unknown";
        }
    }
};

class DBCloudContainer
{
public:
    DBCloudContainer()
        : cloud_fs_(nullptr), cloud_env_(nullptr), db_(nullptr), is_open_(false)
    {
        LOG(INFO) << "DBCloudContainer constr "
                  << is_open_.load(std::memory_order_acquire);
    };

    ~DBCloudContainer();

    void Open(rocksdb::CloudFileSystem *cfs,
              const rocksdb::CloudFileSystemOptions &cfs_options,
              const std::string &rocksdb_storage_path,
              const int max_write_buffer_number = 16,
              const int max_background_jobs = 8,
              const uint64_t target_file_size_base = 64 * 1024 * 1024);

    bool IsClosed()
    {
        return !is_open_.load(std::memory_order_acquire);
    }

    bool IsOpened()
    {
        return is_open_.load(std::memory_order_acquire);
    }

    rocksdb::DBCloud *GetDBPtr() const
    {
        return db_;
    }

    // Get the cloud storage provider for archive operations
    std::shared_ptr<rocksdb::CloudStorageProvider> GetStorageProvider() const
    {
        if (!cloud_fs_)
        {
            return nullptr;
        }
        rocksdb::CloudFileSystem *cfs =
            dynamic_cast<rocksdb::CloudFileSystem *>(cloud_fs_.get());
        if (!cfs)
        {
            return nullptr;
        }
        return cfs->GetStorageProvider();
    }

    // Get the cloud file system for filename remapping
    rocksdb::CloudFileSystem *GetCloudFileSystem() const
    {
        if (!cloud_fs_)
        {
            return nullptr;
        }
        return dynamic_cast<rocksdb::CloudFileSystem *>(cloud_fs_.get());
    }

private:
    std::shared_ptr<rocksdb::FileSystem> cloud_fs_;
    std::unique_ptr<rocksdb::Env> cloud_env_;
    rocksdb::DBCloud *db_;
    std::atomic<bool> is_open_{false};
};

struct InMemoryLogStateToClear
{
    explicit InMemoryLogStateToClear(
        std::unique_ptr<std::deque<Item>> in_mem_data_log_queue)
        : in_mem_data_log_queue_(std::move(in_mem_data_log_queue))
    {
    }
    std::unique_ptr<std::deque<Item>> in_mem_data_log_queue_;
};

/**
 * The observer of LogStateRocksDBCloudImpl, which is used to notify the
 * amount of the log items in memory state on follower node is reached the high
 * water mark which need to be cleared.
 */
class LogStateRocksDBCloudImplObserver
{
public:
    virtual ~LogStateRocksDBCloudImplObserver() = default;
    /*
     * The callback function to notify the log items in memory state is exceed
     * certain amount.
     *
     */
    virtual void OnInMemStateFull(
        size_t log_count,
        size_t log_size,
        std::function<void(bool, uint64_t)> done) const = 0;
};

class LogStateRocksDBCloudImpl : public LogState
{
public:
    explicit LogStateRocksDBCloudImpl(
        std::string rocksdb_path,
        const RocksDBCloudConfig &cloud_config,
        const std::atomic<int64_t> &term_if_is_lg_leader,
        LogStateRocksDBCloudImplObserver *observer,
        const size_t in_mem_data_log_queue_size_high_watermark,
        const size_t rocksdb_max_write_buffer_number,
        const size_t rocksdb_max_background_jobs,
        const uint64_t rocksdb_target_file_size_base,
        const size_t rocksdb_scan_threads);

    ~LogStateRocksDBCloudImpl() override;

    void AddLogItem(uint32_t cc_ng_id,
                    uint64_t tx_number,
                    uint64_t timestamp,
                    const std::string &log_message) override;

    std::pair<bool, std::unique_ptr<ItemIterator>> GetLogReplayList(
        uint32_t ng_id, uint64_t start_timestamp) override;

    void PurgeLogItemsFromMemState(uint64_t last_applied_tx_number);
    static void *AsyncClearInMemoryLogState(void *arg);

    std::pair<bool, Item::Pointer> SearchTxDataLog(
        uint64_t tx_number,
        uint32_t ng_id,
        uint64_t lower_bound_ts = 0) override;

    void BeginSnapshot() override;
    void CleanSnapshotState() override;

    static void MoveFilesToArchive(
        const std::shared_ptr<DBCloudContainer> &dbc_move_log,
        uint64_t min_ckpt_ts,
        const RocksDBCloudConfig &cloud_config);

    static void PurgeArchiveFiles(
        const std::shared_ptr<DBCloudContainer> &dbc_purge_archive,
        uint32_t log_retention_days,
        const RocksDBCloudConfig &cloud_config);

    /**
     * read and load snapshot files
     * @param snapshot_path
     * @param files
     */
    int ReadSnapshot(const std::string &snapshot_path,
                     const std::vector<std::string> &files) override;

    /**
     * write snapshot files to snapshot_path and return the relative filenames
     * @param snapshot_path
     * @return
     */
    std::vector<std::string> WriteSnapshot(
        const std::string &snapshot_path) override;

    /**
     * Start will be called twice if an instance crash and recover:
     * first in starting braft::node which calls
     * braft::StateMachine::on_snapshot_load, then in starting LogState. check
     * db_ to avoid openning RocksDB instance twice.
     */
    int Start() override;

    uint64_t GetLastAppliedTx();
    uint64_t GetSnapshotLastAppliedTx();

    void StopRocksDB();

    void WaitForAsyncStartCloudDBFinishIfAny();

    void AsyncStartCloudDB(int64_t old_term, int64_t new_term);

    bool CheckOrWaitForMemDBInSync(const std::string &the_waiter,
                                   uint32_t timeout_us = 0);

    // Get the internal dbc_
    std::shared_ptr<DBCloudContainer> GetDBCloudContainer()
    {
        std::lock_guard<bthread::Mutex> lk(dbc_mutex_);
        return dbc_;
    }

    void SetMaxFileNumberAfterLatestFlush(uint64_t file_number)
    {
        uint64_t current_max_file_num =
            max_file_num_after_latest_flush_.load(std::memory_order_relaxed);
        while (file_number > current_max_file_num &&
               !max_file_num_after_latest_flush_.compare_exchange_weak(
                   current_max_file_num,
                   file_number,
                   std::memory_order_release,
                   std::memory_order_relaxed))
        {
            // Compare 'file_number' with 'current_max_file_num' and if
            // 'file_number' is greater, update 'current_max_file_num' with
            // 'file_number'. The compare_exchange_weak function returns false
            // if the comparison fails, which means 'current_max_file_num' is
            // not the actual value of 'current_max_file_num' anymore. In such
            // cases, the loop continues to retry the compare_exchange_weak
            // operation with the updated value of 'current_max_file_num', or
            // the updated 'current_max_file_num' is no longer small than
            // 'file_number'.
        }
    }

    uint64_t GetMaxFileNumberAfterLatestFlush()
    {
        return max_file_num_after_latest_flush_.load(std::memory_order_acquire);
    }

    /**
     * Refills the in-memory state from the cloud database.
     *
     * @param dbc The cloud database connection.
     * @param start_sst_number The start SST number to refill. It is captured
     * from the last snapshot.
     * @param end_sst_number The end SST number to refill. It is the maximum
     * file number after the latest flush during on_start_following.
     * @return void
     */
    bool RefillInMemStateFromCloudDB(std::shared_ptr<DBCloudContainer> dbc,
                                     uint64_t start_sst_number,
                                     uint64_t end_sst_number);

    uint64_t GetApproximateReplayLogSize() override;

private:
    /**
     * Stores log records for cc node groups.
     * Committed log records in the log state machine are organized by cc node
     * groups and within one group are ordered by commit timestamps.
     * This organization facilitates log truncation but is less efficient when
     * looking for the log record committed by a specific tx.
     */
    void AddLogItemToMemState(uint32_t cc_ng_id,
                              uint64_t tx_number,
                              uint64_t timestamp,
                              const std::string &log_message);

    void WriteSnapshotInMemState(std::ofstream &os);

    void LoadSnapshotInMemState(std::ifstream &is);

    static void PrintKey(rocksdb::Slice key);

    /**
     * traverse log records for node group: ng_id
     */
    void TraverseLogState(uint32_t ng_id);

    /**
     * Build the S3 client factory
     * @param endpoint The S3 endpoint
     * For minio support
     */
    rocksdb::S3ClientFactory BuildS3ClientFactory(const std::string &endpoint);

    std::thread cloud_db_init_thread_;

    // mutex for sync in memory state
    bthread::Mutex in_mem_state_mutex_;
    // Unlike the RocksDB state implementation, where each log node has its own
    // local RocksDB instance for storing the Raft log state, the RocksDB Cloud
    // state implementation stores the db files on S3 storage. This means that
    // all log group nodes can open the same RocksDB Cloud database when any of
    // them becomes the log group leader. However, in the event of leader
    // transfer, there is no guarantee that the RocksDB mem-table will have been
    // flushed to the SST table. Therefore, we leverage the in-memory queue in
    // the followers to cache the most recent state machine operations. This
    // allows the new leader to re-install the changes in the newly opened
    // RocksCloud instance.
    std::unique_ptr<std::deque<Item>> in_mem_data_log_queue_;
    // The high water mark size of the in-memory data log queue determines the
    // threshold at which a snapshot is triggered to purge the log queue. When
    // the size of the log queue reaches this threshold, the system initiates a
    // snapshot operation to remove older log entries and free up memory
    // resources
    const size_t in_mem_data_log_queue_size_high_watermark_{0};
    // indicate that the in memory data queue is purging
    bool purging_in_mem_data_log_queue_{false};
    // the count of log items before purging
    uint64_t log_count_before_purge_{0};
    // The count of log items after purging start.
    // Sine the in memory state can be refilled from cloud db, so the log item
    // order in the in memory state can be different from the original raft log
    // order, so the purge action could be wrong if it hit a log item refilled
    // from the cloud db. So we need to record the purge start index and make
    // sure the purge won't hit the log items before the purge_start_idx_
    size_t purge_start_idx_{0};
    // the rocksdb memtable number, which buffer the incoming log write for
    // reliving the write slow and stall
    const size_t rocksdb_max_write_buffer_number_;
    // the rocksdb background jobs number, which write the memtable to sst file
    // concurrently
    const size_t rocksdb_max_background_jobs_;
    // the rocksdb target file size base, which is the base of the target file
    const size_t rocksdb_target_file_size_base_;
    const size_t rocksdb_scan_threads_{1};
    static constexpr size_t ASYNC_PURGE_LOG_COUNT_THRESHOLD = 100 * 10000;

    // RocksDB Cloud configuration
    RocksDBCloudConfig cloud_config_;
    // managing the db cloud ptr by shared_ptr for purpose of breaking the
    // contention between the log operations and the AsyncStartDB
    std::shared_ptr<DBCloudContainer> dbc_;
    bthread::Mutex dbc_mutex_;

    // indicate that the in memory data queue is synced to RocksDB Cloud
    std::atomic<bool> mem_db_in_sync_{false};
    bthread::Mutex mem_db_in_sync_mutex_;
    bthread::ConditionVariable mem_db_in_sync_cv_;

    std::string rocksdb_storage_path_;
    rocksdb::WriteOptions write_option_;

    // last applied tx number
    uint64_t last_applied_data_tx_{0};

    // mark the idx of the last log item in the in memory data queue when
    // snapshot triggered
    uint64_t snapshot_in_mem_data_log_queue_size_{0};
    uint64_t snapshot_last_applied_data_tx_{0};

    /**
     * Last truncate timestamp of every ng_id on this log group.
     */
    std::unordered_map<uint32_t, uint64_t> truncate_ts_;

    /**
     * The reference to raft_log_instance term
     */
    const std::atomic<int64_t> &term_if_is_lg_leader_;

    /**
     * Cron job for purging obsolete log
     */
    CronJob log_purger_;
    CronJob archive_move_purger_;

    /**
     * State observer
     */
    LogStateRocksDBCloudImplObserver *observer_;

    /**
     * The sst file number of the last snapshot flush,
     * which is used to refill the in memory state from cloud db
     */
    std::atomic<uint64_t> max_file_num_after_latest_flush_{0};
};
#endif

}  // namespace txlog
#endif
