#include "log_state_rocksdb_impl.h"

#if defined(LOG_STATE_TYPE_RKDB)
#include <rocksdb/db.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/statistics.h>
#include <rocksdb/utilities/checkpoint.h>

#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <string>
#include <system_error>

#include "log_state_rocksdb_impl.h"
#include "log_utils.h"
#include "rocksdb/convenience.h"

namespace txlog
{
LogStateRocksDBImpl::LogStateRocksDBImpl(const std::string &rocksdb_path,
                                         const size_t sst_files_size_limit,
                                         const size_t rocksdb_scan_threads)
    : db_(nullptr),
      rocksdb_storage_path_(rocksdb_path),
      sst_files_size_limit_(sst_files_size_limit),
      rocksdb_scan_threads_(rocksdb_scan_threads),
      last_purging_sst_ckpt_ts_(0) {};

LogStateRocksDBImpl::~LogStateRocksDBImpl()
{
    StopRocksDB();
}

void LogStateRocksDBImpl::AddLogItem(uint32_t cc_ng_id,
                                     uint64_t tx_number,
                                     uint64_t timestamp,
                                     const std::string &log_message)
{
    std::array<char, 20> key{};
    Serialize(key, timestamp, cc_ng_id, tx_number);
    auto status = db_->Put(
        write_option_, rocksdb::Slice(key.data(), key.size()), log_message);
    if (!status.ok())
    {
        LOG(ERROR) << "add log item failed: " << status.ToString();
    }
    assert(status.ok());
}

void LogStateRocksDBImpl::AddLogItemBatch(uint32_t cc_ng_id,
                                          uint64_t tx_number,
                                          uint64_t timestamp,
                                          const std::string &log_message)
{
    std::array<char, 20> key{};
    Serialize(key, timestamp, cc_ng_id, tx_number);
    write_batch_.Put(rocksdb::Slice(key.data(), key.size()), log_message);
    // Flush if the batched data is more than 4MB.
    if (write_batch_.GetDataSize() > 4 * 1024 * 1024)
    {
        FlushLogItemBatch();
    }
}

void LogStateRocksDBImpl::FlushLogItemBatch()
{
    auto status = db_->Write(write_option_, &write_batch_);
    if (!status.ok())
    {
        LOG(ERROR) << "add log item failed: " << status.ToString();
    }
    assert(status.ok());
    write_batch_.Clear();
}

std::pair<bool, std::unique_ptr<ItemIterator>>
LogStateRocksDBImpl::GetLogReplayList(uint32_t ng_id, uint64_t start_timestamp)
{
    std::vector<Item::Pointer> ddl_list;
    GetClusterScaleOpList(ddl_list);
    size_t scale_size = ddl_list.size();
    LOG(INFO) << "cluster_scale_list size: " << scale_size;

    GetSchemaOpList(ddl_list);
    size_t schema_size = ddl_list.size() - scale_size;
    LOG(INFO) << "schema_log_list size: " << schema_size;

    GetSplitRangeOpList(ddl_list);
    size_t rs_size = ddl_list.size() - scale_size - schema_size;
    LOG(INFO) << "split_range_op_list size: " << rs_size;

    std::unique_ptr<ItemIterator> result =
        std::make_unique<ItemIteratorRocksDBImpl>(rocksdb_scan_threads_,
                                                  std::move(ddl_list),
                                                  db_,
                                                  start_timestamp,
                                                  ng_id);

    return std::make_pair(true, std::move(result));
}

std::pair<bool, Item::Pointer> LogStateRocksDBImpl::SearchTxDataLog(
    uint64_t tx_number, uint32_t ng_id, uint64_t lower_bound_ts)
{
    LOG(INFO) << "log state search tx: " << tx_number << ", ng_id: " << ng_id
              << " lower_bound_ts: " << lower_bound_ts;

    // search tx log from last_ckpt_ts + 1
    uint64_t start_ts = 0;
    if (lower_bound_ts != 0)
    {
        start_ts = lower_bound_ts - 1;
    }
    else
    {
        start_ts = LastCkptTimestamp(ng_id);
        if (start_ts != 0)  // 0 indicates no checkpoint happened
        {
            start_ts += 1;
        }
    }
    LOG(INFO) << "iterate log records, ng: " << ng_id
              << ", start ts: " << start_ts;

    // iterate log records in range [start, limit)
    std::array<char, 20> start_key{};
    Serialize(start_key, start_ts, ng_id, 0);
    rocksdb::Slice start(start_key.data(), start_key.size());
    rocksdb::ReadOptions read_option;
    // set iterate_upper_bound for read_option for better performance
    // use with prefix_extractor?
    read_option.iterate_lower_bound = &start;
    auto it = db_->NewIterator(read_option);
    std::array<char, 8> target_txn{};
    Item::Pointer ptr = nullptr;
    for (int idx = 0, shift = 56; shift >= 0; idx++, shift -= 8)
    {
        target_txn[idx] = (tx_number >> shift) & 0xff;
    }
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        std::string_view key_sv = it->key().ToStringView();
        // key_sv is in form of timestamp(8) + ng_id(4) + tx_no(8)
        if (key_sv.compare(12, 8, target_txn.data(), 8) == 0)
        {
            uint32_t ng;
            uint64_t ts;
            uint64_t tx_no;
            Deserialize(it->key(), ts, ng, tx_no);
            LOG(INFO) << "Found matching key: ng_id: " << ng
                      << ", tx_no: " << tx_no << ", timestamp: " << ts;
            ptr = std::make_shared<Item>(
                tx_number, ts, it->value().ToString(), LogItemType::DataLog);
            break;
        }
    }
    if (!it->status().ok())
    {
        LOG(ERROR) << "Iterate failed: " << it->status().ToString();
    }
    assert(it->status().ok());
    delete it;
    if (ptr == nullptr)
    {
        LOG(INFO) << "tx: " << tx_number << " not found in log state";
    }
    return std::make_pair(true, ptr);
}

void LogStateRocksDBImpl::BeginSnapshot()
{
    LogState::MakeCopyOfNgInfoAndCatalogOps();
}

/**
 * read and load snapshot files.
 * files are in form of relative paths to snapshot_path
 */
int LogStateRocksDBImpl::ReadSnapshot(const std::string &snapshot_path,
                                      const std::vector<std::string> &files)
{
    // close current running db and clear its storage
    LOG(INFO) << "close running db and clear storage";
    CloseAndClearRocksDB();

    // iterate and load files
    for (auto &file : files)
    {
        std::string path(snapshot_path);
        path.append("/").append(file);
        if (file == "data")  // ng terms file
        {
            std::ifstream is(path.c_str());
            LoadNgInfoAndCatalogOpsFrom(is);
            continue;
        }

        // rocksdb checkpoint file
        // create hard links to checkpoint file in rocksdb storage path
        std::string link(rocksdb_storage_path_);
        link.append("/").append(std::filesystem::path(file).filename());
        std::error_code error_code;
        std::filesystem::create_hard_link(path, link, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << error_code.value() << " " << error_code.message();
        }
    }
    LOG(INFO) << "after reading snapshot, rocksdb storage path: ";
    std::error_code error_code;
    std::filesystem::directory_iterator dir_ite(rocksdb_storage_path_,
                                                error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << error_code.value() << " " << error_code.message();
        return -1;
    }
    else
    {
        for (auto &entry : dir_ite)
        {
            LOG(INFO) << "file: " << entry.path();
        }
    }

    // Start rocksdb from rocksdb_storage_path we just set up above
    return Start();
}

/**
 * write snapshot files to snapshot_path and return the relative filenames
 */
std::vector<std::string> LogStateRocksDBImpl::WriteSnapshot(
    const std::string &snapshot_path)
{
    std::vector<std::string> res;
    {
        // write ng terms to file 'data'
        auto path = snapshot_path + "/data";
        {
            std::ofstream os(path.c_str());
            WriteSnapshotNgInfoAndCatalogOpsTo(os);
        }  // close file
        res.emplace_back("data");
    }

    // do rocksdb checkpoint, add checkpoint files to snapshot
    rocksdb::Checkpoint *checkpoint;
    auto status = rocksdb::Checkpoint::Create(db_, &checkpoint);
    if (!status.ok())
    {
        LOG(ERROR) << "Create checkpoint object failed: " << status.ToString();
    }
    assert(status.ok());

    // do compaction before checkpoint, so we get smaller checkpoint file?
    //    rocksdb::CompactRangeOptions options;
    //    options.change_level = true;
    //    options.target_level = -1;
    //    db_->CompactRange(options, nullptr, nullptr);

    LOG(INFO) << "log state rocksdbImpl doing checkpoint";
    std::string ckpt_path = snapshot_path + "/rocksdb_ckpt";
    // ckpt_path must not exist before CreateCheckpoint
    std::error_code error_code;
    std::filesystem::remove_all(ckpt_path, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << error_code.value() << " " << error_code.message();
    }
    status = checkpoint->CreateCheckpoint(ckpt_path);
    if (!status.ok())
    {
        LOG(ERROR) << "Create checkpoint failed: " << status.ToString();
    }
    assert(status.ok());
    std::filesystem::directory_iterator dir_ite(ckpt_path, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << error_code.value() << " " << error_code.message();
    }
    else
    {
        for (const auto &entry : std::filesystem::directory_iterator(ckpt_path))
        {
            // add relative path
            std::string path("rocksdb_ckpt/");
            res.emplace_back(path.append(entry.path().filename()));
        }
    }

    return res;
}

/**
 * Start log state.
 * Called from two places: when log instance starts and when loading snapshot.
 * Moreover, Start will be called twice if an instance crash and recover:
 * first in starting braft::node which calls
 * braft::StateMachine::on_snapshot_load, then in starting LogState.
 * check db_ to avoid executing rocksdb::DB::Open twice.
 */
int LogStateRocksDBImpl::Start()
{
    // before opening rocksdb, rocksdb_storage_path_ must exist, create it if
    // not exist
    std::error_code error_code;
    bool rocksdb_storage_path_exists =
        std::filesystem::exists(rocksdb_storage_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: "
                   << rocksdb_storage_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return -1;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(rocksdb_storage_path_, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: "
                       << rocksdb_storage_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return -1;
        }
    }

    // open local rocksdb if its not rocksdb cloud
    LOG(INFO) << "Starting log state local rocksdb";
    if (db_ != nullptr)
    {
        LOG(INFO) << "log state Rocksdb already started";
        return 0;
    }

    rocksdb::Status status;
    rocksdb::Options options;
    options.create_if_missing = true;

    // This option is important, this set disable_auto_compaction to
    // false will half the throughput (100MB/s -> 50MB/s)
    //
    // On low end machine, we observed write log latency vibration when
    // compaction happen, so we changed log key to be prefixed by timestamp
    // instead of ng_id, which in turn avoiding the necessarity of compaction
    options.disable_auto_compactions = true;

    // we don't need compaction style since we disabled compaction
    // Set compation style to universal can improve throughput by 2x
    // options.compaction_style = rocksdb::kCompactionStyleUniversal;

    // Since no compaction is necessary, we can have only 1 level of sst file
    options.num_levels = 1;
    options.info_log_level = rocksdb::INFO_LEVEL;
    // Important! keep atomic_flush true, since we disabled WAL
    options.atomic_flush = true;

    // Enable statistics to get memtabls size
    options.statistics = rocksdb::CreateDBStatistics();

    // set listener for flush events
    auto db_event_listener = std::make_shared<RocksDBEventListener>(this);
    options.listeners.emplace_back(db_event_listener);

    status = rocksdb::DB::Open(options, rocksdb_storage_path_, &db_);
    if (!status.ok())
    {
        LOG(ERROR) << status.ToString();
        return -1;
    }
    write_option_.disableWAL = true;
    LOG(INFO) << "local rocksdb started";
    stop_purge_thread_.store(false, std::memory_order_release);
    purge_thread_ = std::thread(&LogStateRocksDBImpl::PurgingSstFiles, this);

    return 0;
}

void LogStateRocksDBImpl::StopRocksDB()
{
    {
        std::lock_guard<bthread::Mutex> lk(sst_queue_mutex_);
        stop_purge_thread_.store(true, std::memory_order_release);
    }
    sst_queue_cv_.notify_all();

    if (purge_thread_.joinable())
    {
        purge_thread_.join();
    }

    if (db_ != nullptr)
    {
        db_->Close();
        delete db_;
        db_ = nullptr;
    }
}

/**
 * for debug use only
 */
void LogStateRocksDBImpl::PrintKey(rocksdb::Slice key)
{
    uint32_t ng;
    uint64_t timestamp, tx_no;
    Deserialize(key, timestamp, ng, tx_no);
    LOG(INFO) << "ng_id: " << ng << ",\ttimestamp: " << timestamp
              << ",\ttx_number: " << tx_no;
}

/**
 * Close db_ and remove all files and dirs in rocksdb storage path but keep
 * the directory itself.
 * Create rocksdb_storage_path directory if not exists (e.g. when an instance
 * first starts, starting braft::node will load snapshot but rocksdb hasn't
 * started yet) for later copying snapshot.
 */
void LogStateRocksDBImpl::CloseAndClearRocksDB()
{
    {
        std::lock_guard<bthread::Mutex> lk(sst_queue_mutex_);
        stop_purge_thread_.store(true, std::memory_order_release);
    }
    sst_queue_cv_.notify_all();

    if (purge_thread_.joinable())
    {
        purge_thread_.join();
    }

    // first close db_
    if (db_ != nullptr)
    {
        auto status = db_->Close();
        if (!status.ok())
        {
            LOG(ERROR) << "close rocksdb failed: " << status.ToString();
        }
        assert(status.ok());
        delete db_;
        db_ = nullptr;
        LOG(INFO) << "rocksdb instance closed";
    }

    // clear all files in rocksdb_storage_path_
    std::uintmax_t files_deleted = 0;
    std::error_code error_code;
    std::filesystem::directory_iterator dir_ite(rocksdb_storage_path_,
                                                error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << error_code.value() << " " << error_code.message();
    }
    else
    {
        for (const auto &entry : dir_ite)
        {
            files_deleted +=
                std::filesystem::remove_all(entry.path(), error_code);
            if (error_code.value() != 0)
            {
                LOG(ERROR) << error_code.value() << " " << error_code.message();
            }
        }
    }
    LOG(INFO) << "clear rocksdb storage path, " << files_deleted
              << " files or directories removed";
}

void LogStateRocksDBImpl::NotifySstFileCreated(
    const rocksdb::FlushJobInfo &flush_job_info)
{
    std::lock_guard<bthread::Mutex> lk(sst_queue_mutex_);
    sst_created_queue_.push(flush_job_info);
    sst_queue_cv_.notify_all();
}

uint64_t LogStateRocksDBImpl::GetSstFilesSize()
{
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    uint64_t size = 0;
    for (const auto &meta : metadata)
    {
        size += meta.size;
    }
    return size;
}

void LogStateRocksDBImpl::PurgingSstFiles()
{
    // calculate sst files size on disk at start
    sst_files_size_ = GetSstFilesSize();
    double files_size_at_sart =
        static_cast<double>(sst_files_size_) / 1024 / 1024;
    LOG(INFO) << "Sst files size on disk: " << std::fixed
              << std::setprecision(1) << files_size_at_sart << "MB";

    std::unique_lock<bthread::Mutex> lk(sst_queue_mutex_);
    while (!stop_purge_thread_.load(std::memory_order_acquire))
    {
        if (!lk.owns_lock())
        {
            lk.lock();
        }
        sst_queue_cv_.wait(lk);
        // accumulate sst files size
        while (!sst_created_queue_.empty())
        {
            // purge sst file
            auto flush_job_info = sst_created_queue_.front();
            sst_created_queue_.pop();
            lk.unlock();
            sst_files_size_ += flush_job_info.table_properties.data_size +
                               flush_job_info.table_properties.index_size +
                               flush_job_info.table_properties.filter_size;
            DLOG(INFO) << "sst files size on disk: " << std::fixed
                       << std::setprecision(1)
                       << static_cast<double>(sst_files_size_) / 1024 / 1024
                       << "MB";
            if (sst_files_size_ > sst_files_size_limit_)
            {
                break;
            }
            lk.lock();
        }

        // purge sst files if ssd files size exceeds limit
        if (sst_files_size_ > sst_files_size_limit_ &&
            !stop_purge_thread_.load(std::memory_order_acquire))
        {
            if (lk.owns_lock())
            {
                lk.unlock();
            }
            LOG(INFO) << "Sst files size on disk exceeds limit: " << std::fixed
                      << std::setprecision(1)
                      << static_cast<double>(sst_files_size_) / 1024 / 1024
                      << "MB, purge sst files";

            const std::unordered_map<uint32_t, CcNgInfo> ng_info_map =
                CopyCcNgInfo();
            // find the minimum last_ckpt_ts from all cc_ngs
            uint64_t min_last_ckpt_ts = UINT64_MAX;
            uint32_t max_cc_ng_id = 0;
            for (const auto &ng_info : ng_info_map)
            {
                min_last_ckpt_ts =
                    std::min(min_last_ckpt_ts, ng_info.second.last_ckpt_ts_);
                max_cc_ng_id = std::max(max_cc_ng_id, ng_info.first);
            }
            if (min_last_ckpt_ts == 0)
            {
                LOG(INFO) << "No checkpoint found, skip purge sst files";
                continue;
            }

            if (min_last_ckpt_ts <= last_purging_sst_ckpt_ts_)
            {
                // skip purging sst file if ckpt_ts is not updated
                LOG(INFO) << "The mininum checkpoint not been updated, skip "
                             "purge sst files, last_purging_sst_ckpt_ts_: "
                          << last_purging_sst_ckpt_ts_;
                continue;
            }
            DLOG(INFO) << "last_purging_sst_chpt_ts_: "
                       << last_purging_sst_ckpt_ts_
                       << " ,new min_last_chpt_ts: " << min_last_ckpt_ts;
            last_purging_sst_ckpt_ts_ = min_last_ckpt_ts;

            // prepare the range to delete
            // all log entries before the min_last_ckpt_ts belongs to all cc
            // node group could be deleted
            min_last_ckpt_ts -= 1;
            std::array<char, 20> start_key{};
            std::array<char, 20> end_key{};
            Serialize(start_key, 0, 0, 0);
            Serialize(end_key, min_last_ckpt_ts, max_cc_ng_id, 0);
            rocksdb::Slice start(start_key.data(), start_key.size());
            rocksdb::Slice end(end_key.data(), end_key.size());

            std::vector<std::string> delete_files;
            std::vector<rocksdb::LiveFileMetaData> metadata;
            db_->GetLiveFilesMetaData(&metadata);
            for (const auto &meta : metadata)
            {
                rocksdb::Slice smallestkey(meta.smallestkey);
                rocksdb::Slice largestkey(meta.largestkey);
#ifndef NDEBUG
                uint64_t sk_ts, sk_tx_no;
                uint32_t sk_ng_id;
                Deserialize(smallestkey, sk_ts, sk_ng_id, sk_tx_no);
                uint64_t lk_ts, lk_tx_no;
                uint32_t lk_ng_id;
                Deserialize(largestkey, lk_ts, lk_ng_id, lk_tx_no);
                DLOG(INFO) << "sst file: " << meta.name
                           << ", size: " << meta.size
                           << ", level: " << meta.level
                           << " smallest key: " << sk_ts << ", " << sk_ng_id
                           << ", " << sk_tx_no << " largest key: " << lk_ts
                           << ", " << lk_ng_id << ", " << lk_tx_no;
#endif

                // without compaction, sst files must in level 0
                if (meta.level == 0 && meta.size > 0 &&
                    largestkey.compare(end) < 0 &&
                    smallestkey.compare(start) > 0)
                {
                    delete_files.emplace_back(meta.name);
                }
            }

            std::sort(delete_files.begin(), delete_files.end());

            // delete sst files
            for (auto &file : delete_files)
            {
                DLOG(INFO) << "SST file: " << file << " purged.";
                auto status = db_->DeleteFile(file);
                if (!status.ok())
                {
                    LOG(ERROR)
                        << "Purge sst file failed: " << status.ToString();
                }
            }

            double files_size_before =
                static_cast<double>(sst_files_size_) / 1024 / 1024;
            sst_files_size_ = GetSstFilesSize();
            double files_size_after =
                static_cast<double>(sst_files_size_) / 1024 / 1024;
            LOG(INFO) << "Purge sst files, size before: " << std::fixed
                      << std::setprecision(1) << files_size_before
                      << "MB, size after: " << files_size_after << "MB, "
                      << (files_size_before - files_size_after) << "MB purged"
                      << " ,ckpt_ts: " << min_last_ckpt_ts;
            metadata.clear();
#ifndef NDEBUG
            // print live files after purge
            db_->GetLiveFilesMetaData(&metadata);
            for (const auto &meta : metadata)
            {
                rocksdb::Slice smallestkey(meta.smallestkey);
                rocksdb::Slice largestkey(meta.largestkey);
                uint64_t sk_ts, sk_tx_no;
                uint32_t sk_ng_id;
                Deserialize(smallestkey, sk_ts, sk_ng_id, sk_tx_no);
                uint64_t lk_ts, lk_tx_no;
                uint32_t lk_ng_id;
                Deserialize(largestkey, lk_ts, lk_ng_id, lk_tx_no);
                DLOG(INFO) << "sst file: " << meta.name
                           << ", size: " << meta.size
                           << ", level: " << meta.level
                           << " smallest key: " << sk_ts << ", " << sk_ng_id
                           << ", " << sk_tx_no << " largest key: " << lk_ts
                           << ", " << lk_ng_id << ", " << lk_tx_no;
            }
#endif
        }
    }
}

uint64_t LogStateRocksDBImpl::GetApproximateReplayLogSize()
{
    uint64_t total_memtable_size = 0;
    uint64_t total_sst_size = 0;

    /* Get total memtables size */
    std::string size_val;
    if (db_->GetProperty("rocksdb.cur-size-all-mem-tables", &size_val))
    {
        total_memtable_size = std::stoi(size_val);
    }
    else
    {
        LOG(ERROR) << "Failed to get memory table size";
    }

    /* Get total SStables size used in Tx recovery  */
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    for (const auto &meta : metadata)
    {
        rocksdb::Slice largestkey(meta.largestkey);
        uint64_t lk_ts, lk_tx_no;
        uint32_t lk_ng_id;
        Deserialize(largestkey, lk_ts, lk_ng_id, lk_tx_no);

        if (min_ckpt_ts_ <= lk_ts)
        {
            DLOG(INFO) << "SSTable " << meta.name
                       << " used in replay with size: "
                       << FormatSize(meta.size);
            total_sst_size += meta.size;
        }
    }

    DLOG(INFO) << "Total memtables size: " << FormatSize(total_memtable_size);
    DLOG(INFO) << "Total SSTables size used in replay: "
               << FormatSize(total_sst_size);

    uint64_t total_txlog_size = total_memtable_size + total_sst_size;
    DLOG(INFO) << "Total replay log size: " << FormatSize(total_txlog_size);
    return total_txlog_size;
}

void RocksDBEventListener::OnFlushCompleted(
    rocksdb::DB *db, const rocksdb::FlushJobInfo &flush_job_info)
{
    size_t file_size = flush_job_info.table_properties.data_size +
                       flush_job_info.table_properties.index_size +
                       flush_job_info.table_properties.filter_size;
    LOG(INFO) << "Flush sst end, file: " << flush_job_info.file_path
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
              << GetFlushReason(flush_job_info.flush_reason) << std::fixed
              << std::setprecision(1)
              << " ,file_size: " << static_cast<double>(file_size) / 1024 / 1024
              << "MB" << std::endl;

    log_state_->NotifySstFileCreated(flush_job_info);
}

std::string RocksDBEventListener::GetFlushReason(
    rocksdb::FlushReason flush_reason)
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
}  // namespace txlog
#endif
