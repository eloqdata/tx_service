#include "log_state_rocksdb_impl.h"

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)

#if defined(LOG_STATE_TYPE_RKDB_S3)
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3/S3Client.h>
#endif

#include <bthread/bthread.h>

#include <cstddef>
#include <ctime>
#include <filesystem>
#include <memory>
#include <system_error>
#include <unordered_map>

#include "fault_inject.h"
#include "log_state_rocksdb_impl.h"
#include "log_utils.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/convenience.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/statistics.h"
#include "rocksdb_cloud_config.h"

namespace ROCKSDB_NAMESPACE
{
extern std::string MakeTableFileName(uint64_t number);
}

namespace txlog
{
using std::time_t;

CronJob::CronJob(const std::string &job_name,
                 current_time_func current_time,
                 uint32_t interval_seconds)
    : job_name_(job_name),
      wait_mutex_(),
      wait_cv_(),
      is_canceled_(false),
      is_started_(false),
      current_time_(current_time),
      check_interval_seconds_(interval_seconds)
{
}

void CronJob::Start(uint32_t days_from_now,
                    uint32_t starting_hour,
                    uint32_t starting_minute,
                    uint32_t starting_second,
                    std::function<void(current_time_func)> job,
                    bool repeatable)
{
    if (is_started_.load(std::memory_order_acquire))
    {
        return;
    }

    is_started_.store(true, std::memory_order_release);

    thd_ = std::thread(
        [this,
         days_from_now,
         starting_hour,
         starting_minute,
         starting_second,
         job,
         repeatable]()
        {
            std::time_t current = current_time_(nullptr);
            std::time_t run_time = CalculateNextRunTime(
                days_from_now, starting_hour, starting_minute, starting_second);
            if (current >= run_time)
            {
                LOG(ERROR) << "The scheduled cron job time is early than "
                              "current time.";
                return;
            }

            while (!is_canceled_.load(std::memory_order_acquire))
            {
                DLOG(INFO) << "CronJob " << job_name_ << " scheduling check at "
                           << std::asctime(std::localtime(&current));
                // Get the current system time
                current = current_time_(nullptr);

                if (current >= run_time)
                {
                    job(current_time_);
                    // if the cron job is not repeatable
                    if (!repeatable)
                        break;
                    // Calculate next run time based on scheduling mode
                    // If all time params are zero, it's interval-based
                    // scheduling Otherwise, schedule the job on the next day
                    bool is_interval_based =
                        (days_from_now == 0 && starting_hour == 0 &&
                         starting_minute == 0 && starting_second == 0);
                    run_time = CalculateNextRunTime(
                        is_interval_based
                            ? 0
                            : (days_from_now == 0 ? 1 : days_from_now),
                        starting_hour,
                        starting_minute,
                        starting_second);
                }

                std::unique_lock<std::mutex> lk(wait_mutex_);
                bool is_canceled = wait_cv_.wait_for(
                    lk,
                    std::chrono::seconds(check_interval_seconds_),
                    [&]
                    { return is_canceled_.load(std::memory_order_acquire); });

                if (is_canceled)
                {
                    LOG(INFO) << "CronJob " << job_name_ << " canceled.";
                    break;
                }
                // otherwise, cv.wait_for time out, now doing the time check
            }
        });
}

void CronJob::Cancel()
{
    if (is_canceled_.load(std::memory_order_acquire))
    {
        return;
    }
    is_canceled_.store(true, std::memory_order_release);

    {
        std::unique_lock<std::mutex> lk(wait_mutex_);
        wait_cv_.notify_one();
    }

    if (thd_.joinable())
    {
        thd_.join();
    }
}

std::time_t CronJob::CalculateNextRunTime(uint32_t days_from_now,
                                          uint32_t starting_hour,
                                          uint32_t starting_minute,
                                          uint32_t starting_second)
{
    // Get the current system time
    std::time_t current = current_time_(nullptr);
    std::tm current_time;
    localtime_r(&current, &current_time);

    // Create next_run_time as a copy of current_time
    std::tm next_run_time = current_time;

    // If all time parameters are zero, use interval-based scheduling
    if (days_from_now == 0 && starting_hour == 0 && starting_minute == 0 &&
        starting_second == 0)
    {
        // Convert current time_t to seconds since epoch
        // Add interval_seconds_ directly to time_t
        // -1 to ensure the job runs at the end of the interval
        std::time_t next_time_t = current + check_interval_seconds_ - 1;

        // Convert back to tm for logging
        std::tm next_tm_for_log;
        localtime_r(&next_time_t, &next_tm_for_log);

        DLOG(INFO) << "CronJob " << job_name_ << " scheduled to run after "
                   << check_interval_seconds_ << " seconds."
                   << " Current time: " << std::asctime(&current_time)
                   << " Next run time: " << std::asctime(&next_tm_for_log);
        return next_time_t;
    }
    else
    {
        // Time-of-day scheduling: Add days_from_now to the current date
        next_run_time.tm_mday += days_from_now;

        // Set the desired time
        next_run_time.tm_hour = starting_hour;
        next_run_time.tm_min = starting_minute;
        next_run_time.tm_sec = starting_second;

        // Normalize the time (mktime will handle overflow)
        std::time_t next_time_t = std::mktime(&next_run_time);

        // Check if the scheduled time is in the past
        if (next_time_t < current)
        {
            // If so, add one more day
            next_run_time.tm_mday++;
            next_time_t = std::mktime(&next_run_time);
        }

        // Get normalized tm for logging
        std::tm next_tm_for_log;
        localtime_r(&next_time_t, &next_tm_for_log);

        DLOG(INFO) << "CronJob " << job_name_
                   << " Current time: " << std::asctime(&current_time)
                   << " Next run time: " << std::asctime(&next_tm_for_log);

        return next_time_t;
    }
}

void DBCloudContainer::Open(rocksdb::CloudFileSystem *cfs,
                            const rocksdb::CloudFileSystemOptions &cfs_options,
                            const std::string &rocksdb_storage_path,
                            const int max_write_buffer_number,
                            const int max_background_jobs,
                            const uint64_t target_file_size_base)
{
    DLOG(INFO) << "DBCloudContainer Open";
    cloud_fs_.reset(cfs);
    // Create options and use the AWS file system that we created
    // earlier
    cloud_env_ = rocksdb::NewCompositeEnv(cloud_fs_);
    rocksdb::Options options;
    options.env = cloud_env_.get();
    options.create_if_missing = true;

    // This option is important, this set disable_auto_compaction to
    // false will half the throughput (100MB/s -> 50MB/s)
    //
    // On low end machine, we observed write log latency vibration when
    // compaction happen, so we changed log key to be prefixed by timestamp
    // instead of ng_id, which in turn avoiding the necessarity of compaction
    // TODO(XiaoJi): setup remote compaction
    options.disable_auto_compactions = true;
    // keep the default sst file size, 64MB, larger sst file size can not
    // improve perf options.write_buffer_size = 64 * 1024 * 1024;
    options.max_background_jobs = max_background_jobs;
    options.max_write_buffer_number = max_write_buffer_number;
    options.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
    options.level0_stop_writes_trigger = std::numeric_limits<int>::max();
    // config base sst file size
    options.target_file_size_base = target_file_size_base;
    // skip Consistency check, which compares the actual file size with the size
    // recorded in the metadata, which can fail when
    // skip_cloud_files_in_getchildren is set to true
    options.paranoid_checks = false;

    // we don't need compaction style since we disabled compaction
    // Set compation style to universal can improve throughput by 2x
    // options.compaction_style = rocksdb::kCompactionStyleUniversal;

    // Since we disabled compaction, we only have one level util we setup remote
    // compaction
    options.num_levels = 1;

    options.info_log_level = rocksdb::INFO_LEVEL;
    options.best_efforts_recovery = false;
    options.skip_checking_sst_file_sizes_on_db_open = true;
    options.skip_stats_update_on_db_open = true;
    // Important! keep atomic_flush true, since we disabled WAL
    options.atomic_flush = true;

    // Enable statistics to get memtabls size
    options.statistics = rocksdb::CreateDBStatistics();

    auto db_event_listener = std::make_shared<RocksDBEventListener>();
    options.listeners.emplace_back(db_event_listener);

    // The max_open_files default value is -1, it cause DB open all files on
    // DB::Open() This behavior causes 2 effects,
    // 1. DB::Open() will be slow
    // 2. During DB::Open, some of the opened sst files keep in LRUCache will be
    // deleted due to LRU policy, which causes DB::Open failed
    // set max_open_files to 0 when db_option.paranoid_checks is false
    options.max_open_files = 0;

    auto status =
        rocksdb::DBCloud::Open(options, rocksdb_storage_path, "", 0, &db_);

    if (!status.ok())
    {
        LOG(ERROR) << "Unable to open db at path " << rocksdb_storage_path
                   << " with bucket " << cfs_options.src_bucket.GetBucketName()
                   << " with error: " << status.ToString();
        std::abort();
    }

    rocksdb::CloudFileSystem *cfs_ptr =
        dynamic_cast<rocksdb::CloudFileSystem *>(cloud_fs_.get());
    auto &cfs_options_ref = cfs_ptr->GetMutableCloudFileSystemOptions();
    // Restore skip_cloud_files_in_getchildren to false
    cfs_options_ref.skip_cloud_files_in_getchildren = false;

    status = db_->SetDBOptions(
        {{"max_open_files", "-1"}});  // restore max_open_files to default value

    if (!status.ok())
    {
        LOG(ERROR) << "Fail to set max_open_files to -1, error: "
                   << status.ToString();
        std::abort();
    }

    is_open_.store(true, std::memory_order_release);
    LOG(INFO) << "RocksDB Cloud started";
}

DBCloudContainer::~DBCloudContainer()
{
    DLOG(INFO) << "~DBCloudContainer()";
    is_open_.store(false, std::memory_order_release);
    // prevent below "if" reordered before above "store"
    std::atomic_thread_fence(std::memory_order_seq_cst);

    if (db_ != nullptr)
    {
        db_->Close();
        delete db_;
        db_ = nullptr;

        cloud_env_ = nullptr;
        cloud_fs_ = nullptr;
    }
}

bool LogStateRocksDBCloudImpl::CheckOrWaitForMemDBInSync(
    const std::string &the_waiter, uint32_t timeout_us)
{
    if (mem_db_in_sync_.load(std::memory_order_acquire) == false)
    {
        if (timeout_us == 0)
        {
            timeout_us = cloud_config_.db_ready_timeout_us_;
        }

        LOG(INFO) << the_waiter
                  << " only be called on leader, RocksDB Cloud "
                     "is rolling up, wait for it becomming ready, timeout: "
                  << timeout_us;

        if (!BthreadCondWaitFor(
                mem_db_in_sync_mutex_,
                mem_db_in_sync_cv_,
                timeout_us,
                [this]
                {
                    return term_if_is_lg_leader_.load(
                               std::memory_order_acquire) == -1 ||
                           mem_db_in_sync_.load(std::memory_order_acquire) ==
                               true;
                }))
        {
            // time out
            LOG(ERROR) << the_waiter
                       << " wait for RocksDB Cloud rolling up timeout";
            return false;
        }

        // not lg leader
        if (term_if_is_lg_leader_.load(std::memory_order_acquire) == -1)
        {
            LOG(ERROR) << the_waiter
                       << " wait for RocksDB Cloud rolling up, but lg node is "
                          "no longer leader";
            return false;
        }

        // mem db in sync
        LOG(INFO) << "RocksDB Cloud is ready, " << the_waiter
                  << " resuming proceed!";
    }

    return true;
}

// Stage 1: Move files from active DB to archive based on min_ckpt_ts
void LogStateRocksDBCloudImpl::MoveFilesToArchive(
    const std::shared_ptr<DBCloudContainer> &dbc_move_log,
    uint64_t min_ckpt_ts,
    const RocksDBCloudConfig &cloud_config)
{
    if (!dbc_move_log->IsOpened())
    {
        return;  // DB not opened, skip
    }

    // Validate archive config is set
    if (cloud_config.archive_object_path_.empty())
    {
        LOG(WARNING) << "Archive config not set (archive_object_path is "
                        "empty), skipping archive move";
        return;
    }

    rocksdb::DBCloud *db = dbc_move_log->GetDBPtr();
    auto storage_provider = dbc_move_log->GetStorageProvider();
    if (!storage_provider)
    {
        LOG(WARNING) << "Storage provider is not available, cannot move files "
                        "to archive";
        return;
    }

    // Use min_ckpt_ts as the purge threshold (files with keys before
    // min_ckpt_ts are safe to move)
    std::array<char, 20> start_key{};
    std::array<char, 20> end_key{};
    Serialize(start_key, 0, 0, 0);
    Serialize(end_key, min_ckpt_ts, 0, 0);
    rocksdb::Slice lower_bound =
        rocksdb::Slice(start_key.data(), start_key.size());
    rocksdb::Slice upper_bound = rocksdb::Slice(end_key.data(), end_key.size());

    std::vector<std::string> files_to_move;
    std::vector<rocksdb::LiveFileMetaData> file_meta;
    db->GetLiveFilesMetaData(&file_meta);

    for (auto &meta : file_meta)
    {
        rocksdb::Slice smallestkey(meta.smallestkey);
        rocksdb::Slice largestkey(meta.largestkey);

        // without compaction, sst files must be in level 0
        if (meta.level == 0 && meta.size > 0 &&
            largestkey.compare(upper_bound) < 0 &&
            smallestkey.compare(lower_bound) > 0)
        {
            files_to_move.emplace_back(meta.name);
        }
    }

    std::sort(files_to_move.begin(), files_to_move.end());

    // Build a map from file name to largest key timestamp for encoding in
    // filename
    std::unordered_map<std::string, uint64_t> file_to_timestamp;
    for (auto &meta : file_meta)
    {
        rocksdb::Slice largestkey(meta.largestkey);
        uint64_t lk_ts, lk_tx_no;
        uint32_t lk_ng_id;
        Deserialize(largestkey, lk_ts, lk_ng_id, lk_tx_no);
        file_to_timestamp[meta.name] = lk_ts;
    }

    // Get CloudFileSystem for filename remapping
    rocksdb::CloudFileSystem *cfs = dbc_move_log->GetCloudFileSystem();
    if (!cfs)
    {
        LOG(WARNING) << "Cloud file system is not available, cannot move files "
                        "to archive";
        return;
    }

    // Construct source bucket name (active bucket)
    std::string source_bucket_with_prefix =
        cloud_config.bucket_prefix_ + cloud_config.bucket_name_;

    // Construct archive bucket name with prefix (same bucket, different path)
    std::string archive_bucket_with_prefix =
        cloud_config.bucket_prefix_ + cloud_config.bucket_name_;

    // Move files to archive
    for (auto &file : files_to_move)
    {
        // Extract filename and remap it (RocksDB-Cloud may remap filenames)
        std::string filename = std::filesystem::path(file).filename().string();
        std::string remapped_filename = cfs->RemapFilename(filename);

        // Extract timestamp from file metadata and prefix to archived filename
        // Format: {timestamp}_{original_filename}
        // This allows files to be sorted by timestamp when sorted by filename
        uint64_t file_timestamp = file_to_timestamp[file];
        std::string archived_filename =
            std::to_string(file_timestamp) + "_" + remapped_filename;

        // Construct archive object key
        std::string archive_object_key = cloud_config.archive_object_path_;
        if (!archive_object_key.empty() && archive_object_key.back() != '/')
        {
            archive_object_key += "/";
        }
        archive_object_key += archived_filename;

        // Construct source object key (in active bucket)
        std::string source_object_key = cloud_config.object_path_;
        if (!source_object_key.empty() && source_object_key.back() != '/')
        {
            source_object_key += "/";
        }
        source_object_key += remapped_filename;

        // Always use CopyCloudObject for server-side copy (more efficient than
        // re-uploading from local cache). In RocksDB-Cloud, SST files always
        // exist in S3 - local storage is just a cache.
        //
        rocksdb::IOStatus copy_status =
            storage_provider->CopyCloudObject(source_bucket_with_prefix + "/",
                                              source_object_key,
                                              archive_bucket_with_prefix,
                                              archive_object_key);

        bool copy_success = false;
        if (copy_status.ok())
        {
            copy_success = true;
            LOG(INFO) << "Copied SST file " << file << " to archive as "
                      << archived_filename << " (cloud-to-cloud copy): "
                      << archive_bucket_with_prefix << "/"
                      << archive_object_key;
        }
        else
        {
            LOG(ERROR) << "Failed to copy SST file " << file
                       << " to archive via cloud-to-cloud copy: "
                       << copy_status.ToString()
                       << ", source: " << source_bucket_with_prefix << "/"
                       << source_object_key
                       << ", dest: " << archive_bucket_with_prefix << "/"
                       << archive_object_key;
        }

        if (!copy_success)
        {
            continue;  // Skip this file, don't delete from active
        }

        // Remove from active DB after successful copy
        auto status = db->DeleteFile(file);
        if (!status.ok())
        {
            LOG(ERROR) << "Failed to delete SST file " << file
                       << " from active DB: " << status.ToString();
        }
        else
        {
            LOG(INFO) << "Removed SST file " << file << " from active DB";
        }
    }
}

// Stage 2: Delete old files from archive based on log_retention_days
void LogStateRocksDBCloudImpl::PurgeArchiveFiles(
    const std::shared_ptr<DBCloudContainer> &dbc_purge_archive,
    uint32_t log_retention_days,
    const RocksDBCloudConfig &cloud_config)
{
    auto storage_provider = dbc_purge_archive->GetStorageProvider();
    if (!storage_provider)
    {
        LOG(WARNING)
            << "Storage provider is not available, cannot purge archive files";
        return;
    }

    // Calculate cutoff time: current time - log_retention_days
    std::chrono::system_clock::time_point current =
        std::chrono::system_clock::now();
    std::chrono::system_clock::time_point cutoff_time;

#ifdef WITH_FAULT_INJECT
    // Fault injection: override retention with seconds for unit testing
    CODE_FAULT_INJECTOR("override_log_retention_seconds", {
        std::shared_ptr<FaultEntry> entry =
            FaultInject::Entry("override_log_retention_seconds");
        DLOG(INFO) << "Fault inject: override_log_retention_seconds triggered"
                   << " ,map_para_ size: "
                   << (entry ? entry->map_para_.size() : 0)
                   << " ,retention_seconds: "
                   << (entry ? entry->map_para_["retention_seconds"] : "N/A");
        if (entry && entry->map_para_.find("retention_seconds") !=
                         entry->map_para_.end())
        {
            try
            {
                int retention_seconds =
                    std::stoi(entry->map_para_["retention_seconds"]);
                cutoff_time = current - std::chrono::seconds(retention_seconds);
                LOG(INFO)
                    << "Fault inject: overriding log retention with "
                    << retention_seconds << " seconds (cutoff_ts: "
                    << std::chrono::duration_cast<std::chrono::microseconds>(
                           cutoff_time.time_since_epoch())
                           .count()
                    << ")";
            }
            catch (const std::exception &e)
            {
                LOG(FATAL) << "Fault inject: failed to parse "
                              "retention_seconds: "
                           << entry->map_para_["retention_seconds"]
                           << ", error: " << e.what();
                std::abort();
            }
        }
        else
        {
            LOG(FATAL)
                << "Fault inject: override_log_retention_seconds "
                   "triggered but retention_seconds parameter is missing";
            std::abort();
        }
    });

    // Default behavior: use log_retention_days
    if (!FaultInject::Entry("override_log_retention_seconds"))
    {
        cutoff_time = current - std::chrono::hours(log_retention_days * 24);
    }
#else
    cutoff_time = current - std::chrono::hours(log_retention_days * 24);
#endif

    uint64_t cutoff_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                             cutoff_time.time_since_epoch())
                             .count();

    // List files in archive path (same bucket as active, different path)
    std::string bucket_name_with_prefix =
        cloud_config.bucket_prefix_ + cloud_config.bucket_name_;
    std::vector<std::string> archive_files;

    auto st = storage_provider->ListCloudObjectsWithPrefix(
        bucket_name_with_prefix,
        cloud_config.archive_object_path_,
        "",  // Empty prefix to list all files
        &archive_files);

    if (!st.ok())
    {
        LOG(ERROR) << "Failed to list archive files: " << st.ToString();
        return;
    }

    LOG(INFO) << "Archive purge: found " << archive_files.size()
              << " files, cutoff timestamp: " << cutoff_ts;

    // Process each file: parse timestamp from filename prefix
    // Filename format: {timestamp}_{original_filename}
    // Files sorted by filename are also sorted by timestamp, making purging
    // efficient
    for (const auto &file_name : archive_files)
    {
        // Skip non-SST files (only process .sst files)
        if (file_name.find(".sst") == std::string::npos)
        {
            continue;
        }

        // Parse timestamp from filename prefix (format:
        // {timestamp}_{original_filename})
        size_t underscore_pos = file_name.find('_');
        if (underscore_pos == std::string::npos)
        {
            // Old format file (no timestamp prefix) - skip for now or handle
            // separately
            LOG(WARNING) << "Archive file " << file_name
                         << " does not have timestamp prefix, skipping";
            continue;
        }

        std::string timestamp_str = file_name.substr(0, underscore_pos);
        uint64_t file_timestamp;
        try
        {
            file_timestamp = std::stoull(timestamp_str);
        }
        catch (const std::exception &e)
        {
            LOG(WARNING) << "Failed to parse timestamp from archive file "
                         << file_name << ": " << e.what();
            continue;
        }

        // Check if file is old enough to delete
        if (file_timestamp < cutoff_ts)
        {
            // Construct full archive object key
            std::string archive_object_key = cloud_config.archive_object_path_;
            if (!archive_object_key.empty() && archive_object_key.back() != '/')
            {
                archive_object_key += "/";
            }
            archive_object_key += file_name;

            // Delete from cloud storage
            // Note: The actual API method name may need to be verified
            // Based on RocksDB-Cloud API pattern, it should be
            // DeleteCloudObject
            auto delete_st = storage_provider->DeleteCloudObject(
                bucket_name_with_prefix, archive_object_key);
            if (!delete_st.ok())
            {
                LOG(INFO) << "Fail to deleting archive file " << file_name
                          << " bucket: " << bucket_name_with_prefix
                          << " (timestamp: " << file_timestamp
                          << ", cutoff: " << cutoff_ts << ")"
                          << " because: " << delete_st.ToString();
            }
            else
            {
                LOG(INFO) << "Deleting archive file " << file_name
                          << " (timestamp: " << file_timestamp
                          << ", cutoff: " << cutoff_ts << ")";
            }
        }
        // Note: Files are sorted by timestamp (due to filename format),
        // so we could stop processing once we find files newer than cutoff.
        // However, ListCloudObjectsWithPrefix may not guarantee sorted order,
        // so we process all files to be safe.
    }
}

LogStateRocksDBCloudImpl::LogStateRocksDBCloudImpl(
    std::string rocksdb_path,
    const RocksDBCloudConfig &cloud_config,
    const std::atomic<int64_t> &term_if_is_lg_leader,
    LogStateRocksDBCloudImplObserver *observer,
    const size_t in_mem_data_log_queue_size_high_watermark,
    const size_t rocksdb_max_write_buffer_number,
    const size_t rocksdb_max_background_jobs,
    const size_t rocksdb_target_file_size_base,
    const size_t rocksdb_scan_threads)
    : cloud_db_init_thread_(),
      in_mem_state_mutex_(),
      in_mem_data_log_queue_(std::make_unique<std::deque<Item>>()),
      in_mem_data_log_queue_size_high_watermark_(
          in_mem_data_log_queue_size_high_watermark),
      purging_in_mem_data_log_queue_(false),
      log_count_before_purge_(0),
      purge_start_idx_(0),
      rocksdb_max_write_buffer_number_(rocksdb_max_write_buffer_number),
      rocksdb_max_background_jobs_(rocksdb_max_background_jobs),
      rocksdb_target_file_size_base_(rocksdb_target_file_size_base),
      rocksdb_scan_threads_(rocksdb_scan_threads),
      cloud_config_(cloud_config),
      dbc_mutex_(),
      mem_db_in_sync_(false),
      rocksdb_storage_path_(std::move(rocksdb_path)),
      write_option_(),
      snapshot_in_mem_data_log_queue_size_(0),
      snapshot_last_applied_data_tx_(0),
      term_if_is_lg_leader_(term_if_is_lg_leader),
      log_purger_("LogPurger"),  // Uses default interval, but will use
                                 // time-of-day scheduling
      archive_move_purger_(
          "ArchiveMovePurger",
          std::time,
          cloud_config
              .archive_move_interval_seconds_),  // Use configured interval
      observer_(observer)
{
    // this dbc_ instance will serve the follower
    {
        std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
        dbc_ = std::make_shared<DBCloudContainer>();
    }
    write_option_.disableWAL = true;

    // schedule log purge - Stage 2: Purge archive files (time-of-day
    // scheduling)
    log_purger_.Start(
        0,  // days_from_now
        cloud_config_.log_purger_starting_hour_,
        cloud_config_.log_purger_starting_minute_,
        cloud_config_.log_purger_starting_second_,
        [&](current_time_func current_time)
        {
            std::shared_ptr<DBCloudContainer> dbc_purge_archive;
            {
                std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
                dbc_purge_archive = dbc_;
            }

            uint32_t log_retention_days = cloud_config_.log_retention_days_;

            LogStateRocksDBCloudImpl::PurgeArchiveFiles(
                dbc_purge_archive, log_retention_days, cloud_config_);
        });

    // Initialize Stage 1 purger (move to archive) - interval-based scheduling
    // All time params are 0, so it uses interval_seconds_ from constructor
    DLOG(INFO) << "Setting up archive move purger with interval: "
               << cloud_config_.archive_move_interval_seconds_ << " seconds";

    archive_move_purger_.Start(
        0,  // days_from_now = 0
        0,  // starting_hour = 0
        0,  // starting_minute = 0
        0,  // starting_second = 0 -> triggers interval-based scheduling
        [&](current_time_func current_time)
        {
            std::shared_ptr<DBCloudContainer> dbc_move_log;
            {
                std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
                dbc_move_log = dbc_;
            }

            uint64_t min_ckpt_ts = MinCkptTs();

            LogStateRocksDBCloudImpl::MoveFilesToArchive(
                dbc_move_log, min_ckpt_ts, cloud_config_);
        });
};

LogStateRocksDBCloudImpl::~LogStateRocksDBCloudImpl()
{
    WaitForAsyncStartCloudDBFinishIfAny();
    log_purger_.Cancel();
    archive_move_purger_.Cancel();
    StopRocksDB();
}

void LogStateRocksDBCloudImpl::AddLogItemToMemState(
    uint32_t cc_ng_id,
    uint64_t tx_number,
    uint64_t timestamp,
    const std::string &log_message)
{
    Item item =
        Item(tx_number, timestamp, log_message, LogItemType::DataLog, cc_ng_id);
    std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
    in_mem_data_log_queue_->emplace_back(std::move(item));
}

void *LogStateRocksDBCloudImpl::AsyncClearInMemoryLogState(void *arg)
{
    auto start = std::chrono::high_resolution_clock::now();
    InMemoryLogStateToClear *sc = static_cast<InMemoryLogStateToClear *>(arg);
    std::unique_ptr<InMemoryLogStateToClear> sc_ptr(sc);
    sc_ptr->in_mem_data_log_queue_->clear();
    auto end = std::chrono::high_resolution_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "Delete old in mem state in " << ms.count() << " ms";
    return nullptr;
}

void LogStateRocksDBCloudImpl::PurgeLogItemsFromMemState(
    uint64_t last_applied_tx_number)
{
    LOG(INFO) << "Purging in mem log items with tx number "
              << last_applied_tx_number;

    auto start = std::chrono::high_resolution_clock::now();
    std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
    size_t affected = 0;
    size_t in_mem_data_log_queue_size = in_mem_data_log_queue_->size();

    // find the pos of last_applied_tx_number, copy the rest to new queue
    size_t iter_idx = in_mem_data_log_queue_size - 1;
    bool found = false;

    // search for the tx_number in mem state, until purge_start_idx_
    auto it = in_mem_data_log_queue_->rbegin();
    for (; it != in_mem_data_log_queue_->rend(); it++)
    {
        if (it->tx_number_ == last_applied_tx_number)
        {
            found = true;
            break;
        }

        if (iter_idx == purge_start_idx_)
        {
            break;
        }

        iter_idx--;
    }

    auto end1 = std::chrono::high_resolution_clock::now();
    auto ms1 =
        std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start);
    LOG(INFO) << "Find last_applied_tx_number in mem state in " << ms1.count()
              << " ms";

    // copy the rest to new queue if found
    if (found)
    {
        // copy the rest to new queue
        auto new_in_mem_data_log_queue = std::make_unique<std::deque<Item>>();
        auto end2 = std::chrono::high_resolution_clock::now();
        for (auto pos = it.base(); pos != in_mem_data_log_queue_->end(); pos++)
        {
            new_in_mem_data_log_queue->emplace_back(std::move(*pos));
        }
        auto end3 = std::chrono::high_resolution_clock::now();
        auto ms3 =
            std::chrono::duration_cast<std::chrono::milliseconds>(end3 - end2);
        LOG(INFO) << "Copy rest of in mem state to new queue in " << ms3.count()
                  << " ms";
        affected =
            in_mem_data_log_queue_size - new_in_mem_data_log_queue->size();

        // clear old in mem state in background thread if affected is large,
        // cost handreds of ms
        if (affected >= ASYNC_PURGE_LOG_COUNT_THRESHOLD)
        {
            InMemoryLogStateToClear *sc =
                new InMemoryLogStateToClear(std::move(in_mem_data_log_queue_));
            bthread_t tid;
            bthread_start_background(
                &tid, NULL, AsyncClearInMemoryLogState, sc);
        }

        // replace in_mem_data_log_queue_ with a new one
        in_mem_data_log_queue_ = std::move(new_in_mem_data_log_queue);
        purge_start_idx_ = 0;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    LOG(INFO) << "Purged " << affected << " log items in mem state in "
              << ms.count() << " ms";
}

void LogStateRocksDBCloudImpl::AddLogItem(uint32_t cc_ng_id,
                                          uint64_t tx_number,
                                          uint64_t timestamp,
                                          const std::string &log_message)
{
    std::shared_ptr<DBCloudContainer> dbc_copy;
    {
        std::lock_guard<bthread::Mutex> lk(dbc_mutex_);
        dbc_copy = dbc_;
    }

    // Write to db cloud if db is opened
    if (dbc_copy && dbc_copy->IsOpened())
    {
        // Add both to mem state and cloud db state
        std::array<char, 20> key{};
        Serialize(key, timestamp, cc_ng_id, tx_number);
        auto status = dbc_copy->GetDBPtr()->Put(
            write_option_, rocksdb::Slice(key.data(), key.size()), log_message);
        if (!status.ok())
        {
            LOG(ERROR) << "add log item failed: " << status.ToString();
        }
        assert(status.ok());

        last_applied_data_tx_ = tx_number;
        std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
        log_count_before_purge_++;
        // purge in mem state if it's full by calling OnInMemStateFull
        if (log_count_before_purge_ >
                in_mem_data_log_queue_size_high_watermark_ &&
            !purging_in_mem_data_log_queue_)
        {
            purging_in_mem_data_log_queue_ = true;
            std::function<void(bool, uint64_t)> done =
                [this](bool succeed, uint64_t purged_log_count)
            {
                // reset purging_in_mem_data_log_queue_ to false after purge
                // finished
                DLOG(INFO) << "OnInMemStateFull callback";
                std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
                purging_in_mem_data_log_queue_ = false;
                // either purge succeed or not, reset
                // log_count_before_purge_, so unsucceeded snapshot will be
                // delayed for one more round
                log_count_before_purge_ =
                    log_count_before_purge_ - purged_log_count;
            };
            observer_->OnInMemStateFull(log_count_before_purge_,
                                        log_count_before_purge_ * sizeof(Item),
                                        std::move(done));
        }
    }
    else
    {
        // Add to mem state
        AddLogItemToMemState(cc_ng_id, tx_number, timestamp, log_message);
        last_applied_data_tx_ = tx_number;
    }
}

std::pair<bool, std::unique_ptr<ItemIterator>>
LogStateRocksDBCloudImpl::GetLogReplayList(uint32_t ng_id,
                                           uint64_t start_timestamp)
{
    std::unique_ptr<ItemIterator> result;

    // If db is rolling up, wait for it to be ready, otherwise return fail and
    // let log agent retry
    if (!CheckOrWaitForMemDBInSync("GetLogReplayList",
                                   cloud_config_.db_ready_timeout_us_))
    {
        LOG(INFO)
            << "GetLogReplayList fail when rocksdb cloud is still rolling up";
        return std::make_pair(false, std::move(result));
    }

    std::shared_ptr<DBCloudContainer> dbc_copy;
    {
        std::lock_guard<bthread::Mutex> lk(dbc_mutex_);
        dbc_copy = dbc_;
    }
    if (!dbc_copy || !dbc_copy->IsOpened())
    {
        LOG(ERROR) << "GetLogReplayList failed: RocksDB Cloud is not ready.";
        return std::make_pair(false, std::move(result));
    }

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

    result = std::make_unique<ItemIteratorRocksDBImpl>(rocksdb_scan_threads_,
                                                       std::move(ddl_list),
                                                       dbc_copy->GetDBPtr(),
                                                       start_timestamp,
                                                       ng_id);
    return std::make_pair(true, std::move(result));
}

std::pair<bool, Item::Pointer> LogStateRocksDBCloudImpl::SearchTxDataLog(
    uint64_t tx_number, uint32_t ng_id, uint64_t lower_bound_ts)
{
    if (!CheckOrWaitForMemDBInSync("SearchTxDataLog",
                                   cloud_config_.db_ready_timeout_us_))
    {
        return std::make_pair(false, nullptr);
    }

    std::shared_ptr<DBCloudContainer> dbc_copy;
    {
        std::lock_guard<bthread::Mutex> lk(dbc_mutex_);
        dbc_copy = dbc_;
    }
    if (!dbc_copy || !dbc_copy->IsOpened())
    {
        LOG(ERROR) << "SearchTxDataLog failed: RocksDB Cloud is not ready.";
        return std::make_pair(false, nullptr);
    }

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
    read_option.iterate_lower_bound = &start;
    auto it = dbc_copy->GetDBPtr()->NewIterator(read_option);
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

void LogStateRocksDBCloudImpl::BeginSnapshot()
{
    LogState::MakeCopyOfNgInfoAndCatalogOps();
    snapshot_in_mem_data_log_queue_size_ = in_mem_data_log_queue_->size();
    snapshot_last_applied_data_tx_ = GetLastAppliedTx();
}

void LogStateRocksDBCloudImpl::CleanSnapshotState()
{
    LogState::CleanSnapshotState();
    snapshot_in_mem_data_log_queue_size_ = 0;
    snapshot_last_applied_data_tx_ = 0;
}

uint64_t LogStateRocksDBCloudImpl::GetLastAppliedTx()
{
    return last_applied_data_tx_;
}

uint64_t LogStateRocksDBCloudImpl::GetSnapshotLastAppliedTx()
{
    return snapshot_last_applied_data_tx_;
}

/**
 * read and load snapshot files.
 * files are in form of relative paths to snapshot_path
 */
int LogStateRocksDBCloudImpl::ReadSnapshot(
    const std::string &snapshot_path, const std::vector<std::string> &files)
{
    LOG(INFO) << "RocksDB Cloud state ReadSnapshot";

    // load nginfo and catalog
    {
        auto file_path = snapshot_path + "/nginfo_and_catalog";
        std::ifstream is(file_path.c_str());
        LoadNgInfoAndCatalogOpsFrom(is);
    }

    // load in mem data log
    {
        auto file_path = snapshot_path + "/in_mem_data_log";
        std::ifstream is(file_path.c_str());
        // load in mem state only if file exist, since leader snapshot won't
        // include in mem state
        if (is.good())
        {
            LoadSnapshotInMemState(is);
        }
    }

    return Start();
}

void LogStateRocksDBCloudImpl::LoadSnapshotInMemState(std::ifstream &is)
{
    uint32_t in_mem_log_size = 0;
    is.read(reinterpret_cast<char *>(&in_mem_log_size), sizeof(uint32_t));
    LOG(INFO) << "read snapshot in mem log state size: " << in_mem_log_size;
    for (uint32_t i = 0; i < in_mem_log_size; i++)
    {
        uint64_t tx_number = 0;
        uint64_t timestamp = 0;
        std::string log_message;
        size_t log_message_size = 0;
        uint32_t cc_ng_id = UINT32_MAX;
        is.read(reinterpret_cast<char *>(&tx_number), sizeof(uint64_t));
        is.read(reinterpret_cast<char *>(&timestamp), sizeof(uint64_t));
        is.read(reinterpret_cast<char *>(&log_message_size), sizeof(size_t));
        log_message.resize(log_message_size);
        is.read(log_message.data(), log_message_size);
        is.read(reinterpret_cast<char *>(&cc_ng_id), sizeof(uint32_t));
        Item item = Item(
            tx_number, timestamp, log_message, LogItemType::DataLog, cc_ng_id);
        in_mem_data_log_queue_->emplace_back(std::move(item));
        LOG(INFO) << "read snapshot in mem log tx_number: " << tx_number
                  << " timestamp: " << timestamp;
    }
}

/**
 * write snapshot files to snapshot_path and return the relative filenames
 */
std::vector<std::string> LogStateRocksDBCloudImpl::WriteSnapshot(
    const std::string &snapshot_path)
{
    DLOG(INFO) << "snapshot_path: " << snapshot_path;
    std::vector<std::string> res;
    {
        // write ng terms to file 'nginfo_and_catalog'
        {
            auto path = snapshot_path + "/nginfo_and_catalog";
            std::ofstream os(path.c_str());
            WriteSnapshotNgInfoAndCatalogOpsTo(os);
        }  // close file
        res.emplace_back("nginfo_and_catalog");

        {
            auto path = snapshot_path + "/in_mem_data_log";
            std::ofstream os(path.c_str());
            WriteSnapshotInMemState(os);
        }
        res.emplace_back("in_mem_data_log");
    }

    return res;
}

void LogStateRocksDBCloudImpl::WriteSnapshotInMemState(std::ofstream &os)
{
    LOG(INFO) << "write snapshot in mem log size : "
              << snapshot_in_mem_data_log_queue_size_;
    os.write(
        reinterpret_cast<const char *>(&snapshot_in_mem_data_log_queue_size_),
        sizeof(uint32_t));

    if (snapshot_in_mem_data_log_queue_size_ == 0)
    {
        return;
    }

    auto start = std::chrono::high_resolution_clock::now();
    std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
    size_t cnt = 0;
    for (std::deque<Item>::iterator it = in_mem_data_log_queue_->begin();
         it != in_mem_data_log_queue_->end();
         ++it)
    {
        const uint64_t &tx_number = it->tx_number_;
        const uint64_t &timestamp = it->timestamp_;
        const std::string &log_message = it->log_message_;
        size_t log_message_size = log_message.size();
        uint32_t cc_ng_id = it->cc_ng_;
        os.write(reinterpret_cast<const char *>(&tx_number), sizeof(uint64_t));
        os.write(reinterpret_cast<const char *>(&timestamp), sizeof(uint64_t));
        os.write(reinterpret_cast<const char *>(&log_message_size),
                 sizeof(size_t));
        os.write(log_message.data(), log_message_size);
        os.write(reinterpret_cast<const char *>(&cc_ng_id), sizeof(uint32_t));
        cnt++;
        if (cnt == snapshot_in_mem_data_log_queue_size_)
        {
            break;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "write snapshot in mem log in " << ms.count() << " ms";
}

/**
 * Start log state.
 * Called from two places: when log instance starts and when loading snapshot.
 * Moreover, Start will be called twice if an instance crash and recover:
 * first in starting braft::node which calls
 * braft::StateMachine::on_snapshot_load, then in starting LogState.
 * check db_ to avoid executing rocksdb::DB::Open twice.
 */
int LogStateRocksDBCloudImpl::Start()
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

    return 0;
}

void LogStateRocksDBCloudImpl::StopRocksDB()
{
    // release DBCloudConatiner, then internal DBCloud will be closed if ref
    // count to zero
    {
        std::unique_lock<bthread::Mutex> lk(mem_db_in_sync_mutex_);
        mem_db_in_sync_.store(false, std::memory_order_release);
    }

    mem_db_in_sync_cv_.notify_all();
    {
        std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
        dbc_ = std::make_shared<DBCloudContainer>();
    }
    LOG(INFO) << "RocksDB Cloud stopped";
}

void LogStateRocksDBCloudImpl::WaitForAsyncStartCloudDBFinishIfAny()
{
    if (cloud_db_init_thread_.joinable())
    {
        cloud_db_init_thread_.join();
    }
}

std::string toLower(const std::string &str)
{
    std::string lowerStr = str;
    std::transform(lowerStr.begin(),
                   lowerStr.end(),
                   lowerStr.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lowerStr;
}

#if defined(LOG_STATE_TYPE_RKDB_S3)
rocksdb::S3ClientFactory LogStateRocksDBCloudImpl::BuildS3ClientFactory(
    const std::string &endpoint)
{
    return [endpoint](const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
                          &credentialsProvider,
                      const Aws::Client::ClientConfiguration &baseConfig)
               -> std::shared_ptr<Aws::S3::S3Client>
    {
        // Check endpoint url start with http or https
        if (endpoint.empty())
        {
            return nullptr;
        }

        std::string endpoint_url = toLower(endpoint);

        bool secured_url = false;
        if (endpoint_url.rfind("http://", 0) == 0)
        {
            secured_url = false;
        }
        else if (endpoint_url.rfind("https://", 0) == 0)
        {
            secured_url = true;
        }
        else
        {
            LOG(ERROR) << "Invalid S3 endpoint url";
            std::abort();
        }

        // Create a new configuration based on the base config
        Aws::Client::ClientConfiguration config = baseConfig;
        config.endpointOverride = endpoint_url;
        if (secured_url)
        {
            config.scheme = Aws::Http::Scheme::HTTPS;
            // Disable SSL verification for test env if necessary
            // config.verifySSL = false;
        }
        else
        {
            config.scheme = Aws::Http::Scheme::HTTP;
        }

        // Create and return the S3 client
        if (credentialsProvider)
        {
            return std::make_shared<Aws::S3::S3Client>(
                credentialsProvider,
                config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                true /* useVirtualAddressing */);
        }
        else
        {
            return std::make_shared<Aws::S3::S3Client>(config);
        }
    };
}
#endif

void LogStateRocksDBCloudImpl::AsyncStartCloudDB(int64_t old_term,
                                                 int64_t new_term)
{
    LOG(INFO) << "Start RocksDB Cloud Async";

    // this dbc instance will serve the leader
    {
        std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
        dbc_ = std::make_shared<DBCloudContainer>();
    }

    // wait prevous cloud db run to finish
    WaitForAsyncStartCloudDBFinishIfAny();

    cloud_db_init_thread_ = std::thread(
        [this, old_term, new_term, dbc_async_start(dbc_)]
        {
            LOG(INFO) << "RocksDB Cloud Init Thread start to work, old_term: "
                      << old_term << " ,new_term: " << new_term;

            CODE_FAULT_INJECTOR("new_leader_dont_start_rocksdb_cloud", {
                LOG(INFO) << "FaultInject triggered, "
                             "new_leader_dont_start_rocksdb_cloud";
                return;
            });

            LOG(INFO) << "Opening RocksDB Cloud";
            auto start = std::chrono::high_resolution_clock::now();
            CODE_FAULT_INJECTOR("new_leader_sleep_when_start_rocksdb_cloud", {
                LOG(INFO) << "FaultInject triggered, "
                             "new_leader_sleep_when_start_rocksdb_cloud";
                sleep(10);
            });

            // cloud fs config
            rocksdb::CloudFileSystemOptions cfs_options;
            rocksdb::Status status;

#if defined(LOG_STATE_TYPE_RKDB_S3)
            if (cloud_config_.aws_access_key_id_.length() == 0 ||
                cloud_config_.aws_secret_key_.length() == 0)
            {
                LOG(INFO) << "No AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                             "provided, use default credential provider";
                cfs_options.credentials.type =
                    rocksdb::AwsAccessType::kUndefined;
            }
            else
            {
                cfs_options.credentials.InitializeSimple(
                    cloud_config_.aws_access_key_id_,
                    cloud_config_.aws_secret_key_);
            }

            status = cfs_options.credentials.HasValid();
            if (!status.ok())
            {
                LOG(ERROR) << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                              "is required, error: "
                           << status.ToString();
                std::abort();
            }
#endif

            // Determine effective bucket configuration
            // OSS URL takes precedence over legacy configuration
            std::string effective_bucket_name = cloud_config_.bucket_name_;
            std::string effective_bucket_prefix = cloud_config_.bucket_prefix_;
            std::string effective_object_path = cloud_config_.object_path_;
            std::string effective_endpoint_url = cloud_config_.endpoint_url_;

            if (!cloud_config_.oss_url_.empty())
            {
                // Parse OSS URL and use it (overrides legacy config)
                S3UrlComponents url_components =
                    ParseS3Url(cloud_config_.oss_url_);
                if (!url_components.is_valid)
                {
                    LOG(ERROR)
                        << "Invalid oss_url: " << url_components.error_message
                        << ". URL format: s3://{bucket}/{path}, "
                           "gs://{bucket}/{path}, or "
                           "http(s)://{host}:{port}/{bucket}/{path}. "
                        << "Examples: s3://my-bucket/my-path, "
                        << "gs://my-bucket/my-path, "
                        << "http://localhost:9000/my-bucket/my-path";
                    std::abort();
                }

                effective_bucket_name = url_components.bucket_name;
                effective_bucket_prefix = "";  // No prefix in URL-based config
                effective_object_path = url_components.object_path;
                effective_endpoint_url = url_components.endpoint_url;

                LOG(INFO)
                    << "Using OSS URL configuration (overrides legacy config "
                       "if present): "
                    << cloud_config_.oss_url_
                    << " (bucket: " << effective_bucket_name
                    << ", object_path: " << effective_object_path
                    << ", endpoint: "
                    << (effective_endpoint_url.empty() ? "default"
                                                       : effective_endpoint_url)
                    << ")";
            }

            cloud_config_.bucket_name_ = effective_bucket_name;
            cloud_config_.bucket_prefix_ = effective_bucket_prefix;
            cloud_config_.object_path_ = effective_object_path;
            cloud_config_.endpoint_url_ = effective_endpoint_url;

            cfs_options.src_bucket.SetBucketName(cloud_config_.bucket_name_);
            cfs_options.src_bucket.SetBucketPrefix(
                cloud_config_.bucket_prefix_);
            cfs_options.src_bucket.SetRegion(cloud_config_.region_);
            cfs_options.src_bucket.SetObjectPath(cloud_config_.object_path_);
            cfs_options.dest_bucket.SetBucketName(cloud_config_.bucket_name_);
            cfs_options.dest_bucket.SetBucketPrefix(
                cloud_config_.bucket_prefix_);
            cfs_options.dest_bucket.SetRegion(cloud_config_.region_);
            cfs_options.dest_bucket.SetObjectPath(cloud_config_.object_path_);
            // Add sst_file_cache for accerlating random access on sst files
            cfs_options.sst_file_cache = rocksdb::NewLRUCache(
                cloud_config_.sst_file_cache_size_,
                cloud_config_.sst_file_cache_num_shard_bits_);
            // completelly disable cloud file purger
            cfs_options.run_purger = false;
            // set the cloud file deletion delay, set to zero if want to delete
            // immediately, e.g. when the log state generated by workload is
            // huge, you may want to set the retention time to second level, and
            // purge the old state files very quickly
            cfs_options.cloud_file_deletion_delay =
                std::chrono::seconds(cloud_config_.db_file_deletion_delay_);
            DLOG(INFO)
                << "Txlog RocksDBCloud cloud_file_deletion_delay set to "
                << (cfs_options.cloud_file_deletion_delay.has_value()
                        ? std::to_string(
                              cfs_options.cloud_file_deletion_delay->count())
                        : "null");

#if defined(LOG_STATE_TYPE_RKDB_S3)
            if (!effective_endpoint_url.empty())
            {
                cfs_options.s3_client_factory =
                    BuildS3ClientFactory(effective_endpoint_url);
                // Intermittent and unpredictable IOError happend from time to
                // time when using aws transfer manager with minio. Disable aws
                // transfer manager if endpoint is set (minio).
                cfs_options.use_aws_transfer_manager = false;
            }
            else
            {
                // use aws transfer manager to upload/download files
                // the transfer manager can leverage multipart upload and
                // download
                cfs_options.use_aws_transfer_manager = true;
            }
#endif

            cfs_options.delete_cloud_invisible_files_on_open = false;

            // Temp fix for very slow open db issue
            // TODO: implement customized sst file manager
            cfs_options.constant_sst_file_size_in_sst_file_manager =
                64 * 1024 * 1024L;
            // Skip listing cloud files in GetChildren when DumpDBSummary to
            // speed up open db
            cfs_options.skip_cloud_files_in_getchildren = true;

            // Get the cookie on open from the previous term
            rocksdb::CloudFileSystem *cfs;
            status = NewCloudFileSystem(cfs_options, &cfs);

            if (!status.ok())
            {
                LOG(ERROR)
                    << "Unable to create cloud storage filesystem, cloud type: "
#if defined(LOG_STATE_TYPE_RKDB_S3)
                    << "aws"
#elif defined(LOG_STATE_TYPE_RKDB_GCS)
                    << "gcp"
#endif
                    << ", at path rocksdb_cloud with bucket "
                    << effective_bucket_name << " prefix "
                    << effective_bucket_prefix
                    << ", with error: " << status.ToString();

                std::abort();
            }

            int64_t tmp_old_term = old_term;
            std::string cookie_on_open = "";
            std::string new_cookie_on_open = std::to_string(new_term);
            // find the latest cloud manifest file
            if (tmp_old_term >= 0)
            {
                auto storage_provider = cfs->GetStorageProvider();
                while (tmp_old_term >= 0)
                {
                    std::string cloud_manifest_file_name =
                        MakeCloudManifestFile(effective_object_path,
                                              std::to_string(tmp_old_term));
                    auto st = storage_provider->ExistsCloudObject(
                        effective_bucket_prefix + effective_bucket_name,
                        cloud_manifest_file_name);
                    if (st.ok())
                    {
                        LOG(INFO) << "Latest RocksDB Cloud Manifest file "
                                  << cloud_manifest_file_name << " found";
                        cookie_on_open = std::to_string(tmp_old_term);
                        break;
                    }
                    else
                    {
                        LOG(ERROR) << "RocksDB Cloud Manifest file "
                                   << cloud_manifest_file_name << " not found";
                        tmp_old_term--;
                    }
                }
            }

            auto stop = std::chrono::high_resolution_clock::now();
            uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              stop - start)
                              .count();
            LOG(INFO) << "Logstate Find last cloud manifest costs " << ms
                      << " milliseconds";

            auto &cfs_options_ref = cfs->GetMutableCloudFileSystemOptions();
            // new CLOUDMANIFEST suffixed by cookie and epochID suffixed
            // MANIFEST files are generated, which won't overwrite the old ones
            // opened by previous leader
            cfs_options.cookie_on_open = cookie_on_open;
            cfs_options_ref.cookie_on_open = cookie_on_open;
            cfs_options.new_cookie_on_open = new_cookie_on_open;
            cfs_options_ref.new_cookie_on_open = new_cookie_on_open;

            // sync cloudmanifest and manifest files when open db
            cfs_options.resync_on_open = true;
            cfs_options_ref.resync_on_open = true;

            // start db cloud in DBCLoudContainer and dump in memory log items
            dbc_async_start->Open(cfs,
                                  cfs_options,
                                  rocksdb_storage_path_,
                                  rocksdb_max_write_buffer_number_,
                                  rocksdb_max_background_jobs_,
                                  rocksdb_target_file_size_base_);

            stop = std::chrono::high_resolution_clock::now();
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(stop -
                                                                       start)
                     .count();
            LOG(INFO) << "Logstate Open RocksDB Cloud costs " << ms
                      << " milliseconds";

            uint64_t max_file_number =
                dbc_async_start->GetDBPtr()->GetNextFileNumber() - 1;
            LOG(INFO) << "The max_file_number after DBCloud open: "
                      << max_file_number;

            // set max file number after open the db
            SetMaxFileNumberAfterLatestFlush(max_file_number);

            // sync in memory state into db
            start = std::chrono::high_resolution_clock::now();

            // sync in memory state into db, the in_mem_data_log_queue_ won't be
            // changed after db is opened, so we can safely iterate it.
            // if leader changed again, we wait for this async start thread to
            // finish in on_stop_following, so that the in_mem_data_log_queue_
            // won't be changed after db is opened
            int64_t cnt = 0;

            CODE_FAULT_INJECTOR("disable_in_mem_state_to_db_sync", {
                LOG(INFO) << "FaultInject triggered, "
                             "disable_in_mem_state_to_db_sync";
                goto mem_db_in_sync;
            });

            {
                std::unique_lock<bthread::Mutex> in_mem_state_lk(
                    in_mem_state_mutex_);
                for (const auto &item : *in_mem_data_log_queue_)
                {
                    std::array<char, 20> key{};
                    uint32_t cc_ng_id = item.cc_ng_;
                    Serialize(key, item.timestamp_, cc_ng_id, item.tx_number_);
                    auto status = dbc_async_start->GetDBPtr()->Put(
                        write_option_,
                        rocksdb::Slice(key.data(), key.size()),
                        item.log_message_);
                    cnt++;
                    if (!status.ok())
                    {
                        LOG(ERROR)
                            << "add log item failed: " << status.ToString();
                        std::abort();
                    }
                }

                in_mem_data_log_queue_->clear();
                purge_start_idx_ = 0;
            }

        mem_db_in_sync:
        {
            std::unique_lock<bthread::Mutex> lk(mem_db_in_sync_mutex_);
            // update mem_db_in_sync_ only when term still matched, in case
            // of in middle step down during async start db
            if (new_term ==
                term_if_is_lg_leader_.load(std::memory_order_acquire))
            {
                mem_db_in_sync_.store(true, std::memory_order_release);
            }
            else
            {
                LOG(INFO) << "lg leader changed during async start "
                             "roccksdb cloud since term mismatch, term="
                          << new_term << ", current_term="
                          << term_if_is_lg_leader_.load(
                                 std::memory_order_acquire);
            }
        }
            mem_db_in_sync_cv_.notify_all();

            stop = std::chrono::high_resolution_clock::now();
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(stop -
                                                                       start)
                     .count();
            LOG(INFO) << "Dump " << cnt
                      << " in memory log items into RocksDB Cloud, costs " << ms
                      << " millseconds";
        });
}

bool LogStateRocksDBCloudImpl::RefillInMemStateFromCloudDB(
    std::shared_ptr<DBCloudContainer> dbc,
    uint64_t start_file_number,
    uint64_t end_file_number)
{
    if (start_file_number == end_file_number)
    {
        LOG(INFO) << "No need to refill in mem state from RocksDB Cloud, "
                     "start_sst_number == "
                  << start_file_number
                  << " ,end_sst_number == " << end_file_number;
        return true;
    }
    std::lock_guard<bthread::Mutex> lk(in_mem_state_mutex_);

    auto options = dbc->GetDBPtr()->GetOptions();
    auto fs = options.env->GetFileSystem();
    auto start = std::chrono::high_resolution_clock::now();
    for (auto file_number = start_file_number + 1;
         file_number <= end_file_number;
         file_number++)
    {
        std::string file_name =
            ROCKSDB_NAMESPACE::MakeTableFileName(file_number);
        // check if the file exists
        // the file number may be used by other file type instead of sst file,
        // so we need to check if the file exists, if not, skip this file and
        // continue
        rocksdb::SstFileReader sst_reader(options);
        rocksdb::Status status = sst_reader.Open(file_name);
        if (!status.ok())
        {
            LOG(WARNING) << "Can't open sst file: " << file_name
                         << ", error: " << status.ToString()
                         << ", the file number " << file_number
                         << " maybe used by other file type, skip this file "
                            "and continue";
            continue;
        }
        status = sst_reader.VerifyChecksum();
        if (!status.ok())
        {
            LOG(ERROR)
                << "Refill in mem state failed, due to verify checksum failed: "
                << status.ToString();
            return false;
        }
        rocksdb::ReadOptions read_options;
        std::unique_ptr<rocksdb::Iterator> it(
            sst_reader.NewIterator(read_options));
        if (!it->status().ok())
        {
            LOG(ERROR)
                << "Refill in mem state failed, due to new iterator failed: "
                << it->status().ToString();
            return false;
        }

        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            rocksdb::Slice key = it->key();
            rocksdb::Slice value = it->value();
            uint64_t timestamp, tx_number;
            uint32_t ng_id;
            Deserialize(key, timestamp, ng_id, tx_number);
            Item item;
            item.tx_number_ = tx_number;
            item.timestamp_ = timestamp;
            item.log_message_ = {value.data(), value.size()};
            item.cc_ng_ = ng_id;
            item.item_type_ = LogItemType::DataLog;
            in_mem_data_log_queue_->emplace_front(std::move(item));
        }
    }

    size_t log_cnt = in_mem_data_log_queue_->size();
    purge_start_idx_ = log_cnt == 0 ? 0 : log_cnt - 1;
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    LOG(INFO) << "Refill in memory state form RocksDB Cloud, log count: "
              << log_cnt << " ,duration: " << duration.count()
              << " millseconds";
    return true;
}

uint64_t LogStateRocksDBCloudImpl::GetApproximateReplayLogSize()
{
    std::shared_ptr<DBCloudContainer> dbc_copy;
    {
        std::lock_guard<bthread::Mutex> lk(dbc_mutex_);
        dbc_copy = dbc_;
    }
    if (!dbc_copy || !dbc_copy->IsOpened())
    {
        return 0;
    }

    uint64_t total_memtable_size = 0;
    uint64_t total_sst_size = 0;

    /* Get total memtables size */
    std::string size_val;
    if (dbc_copy->GetDBPtr()->GetProperty("rocksdb.cur-size-all-mem-tables",
                                          &size_val))
    {
        total_memtable_size = std::stoi(size_val);
    }
    else
    {
        LOG(ERROR) << "Failed to get memory table size";
    }

    /* Get total SStables size used in Tx recovery  */
    std::vector<rocksdb::LiveFileMetaData> metadata;
    dbc_copy->GetDBPtr()->GetLiveFilesMetaData(&metadata);
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

    LOG(INFO) << "Total memtables size: " << FormatSize(total_memtable_size);
    LOG(INFO) << "Total SSTables size used in replay: "
              << FormatSize(total_sst_size);

    uint64_t total_txlog_size = total_memtable_size + total_sst_size;
    LOG(INFO) << "Total replay log size: " << FormatSize(total_txlog_size);
    return total_txlog_size;
}

/**
 * for debug use only
 */
void LogStateRocksDBCloudImpl::PrintKey(rocksdb::Slice key)
{
    uint32_t ng;
    uint64_t timestamp, tx_no;
    Deserialize(key, timestamp, ng, tx_no);
    LOG(INFO) << "ng_id: " << ng << ",\ttimestamp: " << timestamp
              << ",\ttx_number: " << tx_no;
}
}  // namespace txlog
#endif
