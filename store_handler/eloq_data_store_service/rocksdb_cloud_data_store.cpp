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
#include "rocksdb_cloud_data_store.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <bthread/condition_variable.h>
#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>

#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_store_service.h"
#include "ds_request.pb.h"
#include "internal_request.h"
#include "purger_event_listener.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"

#define LONG_STR_SIZE 21

namespace EloqDS
{
bool RocksDBCloudDataStore::String2ll(const char *s,
                                      size_t slen,
                                      int64_t &value)
{
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    uint64_t v;

    /* A string of zero length or excessive length is not a valid number. */
    if (plen == slen || slen >= LONG_STR_SIZE)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0')
    {
        value = 0;
        return 1;
    }

    /* Handle negative numbers: just set a flag and continue like if it
     * was a positive number. Later convert into negative. */
    if (p[0] == '-')
    {
        negative = 1;
        p++;
        plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9')
    {
        v = p[0] - '0';
        p++;
        plen++;
    }
    else
    {
        return 0;
    }

    /* Parse all the other digits, checking for overflow at every step. */
    while (plen < slen && p[0] >= '0' && p[0] <= '9')
    {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
            return 0;
        v += p[0] - '0';

        p++;
        plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    /* Convert to negative if needed, and do the final overflow check when
     * converting from unsigned long long to long long. */
    if (negative)
    {
        if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1)) /* Overflow. */
            return 0;
        value = -v;
    }
    else
    {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;

        value = v;
    }
    return 1;
}

RocksDBCloudDataStore::RocksDBCloudDataStore(
    const EloqDS::RocksDBCloudConfig &cloud_config,
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
      cloud_config_(cloud_config),
      cloud_fs_(),
      cloud_env_(nullptr),
      db_(nullptr)
{
}

RocksDBCloudDataStore::~RocksDBCloudDataStore()
{
    if (query_worker_pool_ != nullptr || db_ != nullptr)
    {
        Shutdown();
    }
}

void RocksDBCloudDataStore::Shutdown()
{
    RocksDBDataStoreCommon::Shutdown();

    std::unique_lock<std::shared_mutex> db_lk(db_mux_);
    if (db_ != nullptr)
    {
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, db->Close()";
        db_->Close();
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, delete db_";
        delete db_;
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, db_ = nullptr";
        db_ = nullptr;
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, ttl_compaction_filter_ "
                      "= nullptr";
        ttl_compaction_filter_ = nullptr;
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, cloud_env_ = nullptr";
        cloud_env_ = nullptr;
        DLOG(INFO) << "RocksDBCloudDataStore Shutdown, cloud_fs_ = nullptr";
        cloud_fs_ = nullptr;
    }
}

static std::string toLower(const std::string &str)
{
    std::string lowerStr = str;
    std::transform(lowerStr.begin(),
                   lowerStr.end(),
                   lowerStr.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lowerStr;
}

#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
rocksdb::S3ClientFactory RocksDBCloudDataStore::BuildS3ClientFactory(
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
        }
        else
        {
            config.scheme = Aws::Http::Scheme::HTTP;
        }
        // Disable SSL verification for HTTPS
        config.verifySSL = false;

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

bool RocksDBCloudDataStore::StartDB()
{
    if (db_)
    {
        // db is already started, no op
        DLOG(INFO) << "DBCloud already started";
        return true;
    }
    rocksdb::Status status;
#ifdef DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3
    if (cloud_config_.aws_access_key_id_.length() == 0 ||
        cloud_config_.aws_secret_key_.length() == 0)
    {
        LOG(INFO) << "No AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                     "provided, use default credential provider";
        cfs_options_.credentials.type = rocksdb::AwsAccessType::kUndefined;
    }
    else
    {
        cfs_options_.credentials.InitializeSimple(
            cloud_config_.aws_access_key_id_, cloud_config_.aws_secret_key_);
    }

    status = cfs_options_.credentials.HasValid();
    if (!status.ok())
    {
        LOG(ERROR) << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                      "is required, error: "
                   << status.ToString();
        return false;
    }
#endif

    cfs_options_.src_bucket.SetBucketName(cloud_config_.bucket_name_,
                                          cloud_config_.bucket_prefix_);
    cfs_options_.src_bucket.SetRegion(cloud_config_.region_);
    cfs_options_.src_bucket.SetObjectPath(cloud_config_.object_path_);
    cfs_options_.dest_bucket.SetBucketName(cloud_config_.bucket_name_,
                                           cloud_config_.bucket_prefix_);
    cfs_options_.dest_bucket.SetRegion(cloud_config_.region_);
    cfs_options_.dest_bucket.SetObjectPath(cloud_config_.object_path_);
    // Add sst_file_cache for accerlating random access on sst files
    // use 2^5 = 32 shards for the cache, each shard has sst_file_cache_size_/32
    // bytes capacity
    cfs_options_.sst_file_cache =
        rocksdb::NewLRUCache(cloud_config_.sst_file_cache_size_,
                             cloud_config_.sst_file_cache_num_shard_bits_);
    // delay cloud file deletion forever since we delete obsolete files
    // using purger
    cfs_options_.cloud_file_deletion_delay = std::chrono::seconds(INT_MAX);

    // keep invisible files in cloud storage since they can be referenced
    // by other nodes with old valid cloud manifest files during leader transfer
    cfs_options_.delete_cloud_invisible_files_on_open = false;

    // sync cloudmanifest and manifest files when open db
    cfs_options_.resync_on_open = true;

    // run cloud file purger to delete obsolete files
    cfs_options_.run_purger = cloud_config_.run_purger_;
    cfs_options_.purger_periodicity_millis =
        cloud_config_.purger_periodicity_millis_;

    // Temp fix for very slow open db issue
    // TODO(monkeyzilla): implement customized sst file manager
    cfs_options_.constant_sst_file_size_in_sst_file_manager = 64 * 1024 * 1024L;
    // Skip listing cloud files in GetChildren when DumpDBSummary,
    // SanitizeOptions, Recover(CheckConsistency), WriteOptions to speed up open
    // db
    cfs_options_.skip_cloud_files_in_getchildren = true;

    DLOG(INFO) << "RocksDBCloudDataStore::StartDB, purger_periodicity_millis: "
               << cfs_options_.purger_periodicity_millis << " ms"
               << ", run_purger: " << cfs_options_.run_purger;

#ifdef DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3
    if (!cloud_config_.s3_endpoint_url_.empty())
    {
        cfs_options_.s3_client_factory =
            BuildS3ClientFactory(cloud_config_.s3_endpoint_url_);
        // Intermittent and unpredictable IOError happend from time to
        // time when using aws transfer manager with minio. Disable aws
        // transfer manager if endpoint is set (minio).
        cfs_options_.use_aws_transfer_manager = false;
    }
    else
    {
        // use aws transfer manager to upload/download files
        // the transfer manager can leverage multipart upload and download
        cfs_options_.use_aws_transfer_manager = true;
    }
#endif

    DLOG(INFO) << "DBCloud Open";
    auto start_time = std::chrono::steady_clock::now();
    rocksdb::CloudFileSystem *cfs;
    // Open the cloud file system
    status = EloqDS::NewCloudFileSystem(cfs_options_, &cfs);
    if (!status.ok())
    {
        LOG(ERROR) << "Unable to create cloud storage filesystem, cloud type: "
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
                   << "Aws"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
                   << "Gcp"
#endif
                   << ", at path rocksdb_cloud with bucket "
                   << cfs_options_.src_bucket.GetBucketName()
                   << ", with error: " << status.ToString();

        return false;
    }
    auto end_time = std::chrono::steady_clock::now();
    auto use_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time - start_time)
                        .count();
    LOG(INFO) << "DBCloud open, NewCloudFileSystem took " << use_time << " ms";

    std::string cookie_on_open = "";
    std::string new_cookie_on_open = "";

    // TODO(githubzilla): dss_shard_id is not used in the current
    // implementation, remove it later
    int64_t dss_shard_id = 0;
    std::string cloud_manifest_prefix;
    int64_t max_term = -1;
    auto storage_provider = cfs->GetStorageProvider();
    // find the max term cookie from cloud manifest files
    bool ret = FindMaxTermFromCloudManifestFiles(storage_provider,
                                                 cloud_config_.bucket_prefix_,
                                                 cloud_config_.bucket_name_,
                                                 cloud_config_.object_path_,
                                                 cloud_config_.branch_name_,
                                                 dss_shard_id,
                                                 cloud_manifest_prefix,
                                                 max_term);

    if (!ret)
    {
        LOG(ERROR) << "Failed to find max term from cloud manifest file for "
                      "branch: "
                   << cloud_config_.branch_name_;
        // this is a new db
        cookie_on_open = "";
        new_cookie_on_open = MakeCloudManifestCookie(
            cloud_config_.branch_name_, dss_shard_id, 0);
    }
    else if (max_term != -1)
    {
        cookie_on_open = MakeCloudManifestCookie(
            cloud_config_.branch_name_, dss_shard_id, max_term);
        new_cookie_on_open = MakeCloudManifestCookie(
            cloud_config_.branch_name_, dss_shard_id, max_term + 1);
    }
    else
    {
        // this is snapshot restore case, no valid cloud manifest file
        cookie_on_open = cloud_config_.branch_name_;
        new_cookie_on_open = MakeCloudManifestCookie(
            cloud_config_.branch_name_, dss_shard_id, 0);
    }

    // new CLOUDMANIFEST suffixed by cookie and epochID suffixed
    // MANIFEST files are generated, which won't overwrite the old ones
    // opened by previous leader
    auto &cfs_options_ref = cfs->GetMutableCloudFileSystemOptions();
    cfs_options_.cookie_on_open = cookie_on_open;
    cfs_options_ref.cookie_on_open = cookie_on_open;
    cfs_options_.new_cookie_on_open = new_cookie_on_open;
    cfs_options_ref.new_cookie_on_open = new_cookie_on_open;

    DLOG(INFO) << "StartDB cookie_on_open: " << cfs_options_.cookie_on_open
               << " new_cookie_on_open: " << cfs_options_.new_cookie_on_open;

    cloud_fs_.reset(cfs);
    // Create options and use the AWS file system that we created
    // earlier
    cloud_env_ = rocksdb::NewCompositeEnv(cloud_fs_);

    return OpenCloudDB(cfs_options_);
}

bool RocksDBCloudDataStore::OpenCloudDB(
    const rocksdb::CloudFileSystemOptions &cfs_options)
{
    rocksdb::Options options;
    options.env = cloud_env_.get();
    options.create_if_missing = create_db_if_missing_;
    options.create_missing_column_families = true;
    // boost write performance by enabling unordered write
    options.unordered_write = true;
    // skip Consistency check, which compares the actual file size with the size
    // recorded in the metadata, which can fail when
    // skip_cloud_files_in_getchildren is set to true
    options.paranoid_checks = false;

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

    // Add event listener for purger
    rocksdb::CloudFileSystemImpl *cfs_impl =
        dynamic_cast<rocksdb::CloudFileSystemImpl *>(cloud_fs_.get());
    if (cfs_impl == nullptr)
    {
        LOG(ERROR) << "Fail to get CloudFileSystemImpl from cloud_fs_";
        return false;
    }
    std::string bucket_name =
        cloud_config_.bucket_prefix_ + cloud_config_.bucket_name_;
    auto db_event_listener = std::make_shared<PurgerEventListener>(
        "", /*We still don't know the epoch now*/
        bucket_name,
        cloud_config_.object_path_,
        cfs_impl->GetStorageProvider());
    options.listeners.emplace_back(db_event_listener);

    // The max_open_files default value is -1, it cause DB open all files on
    // DB::Open() This behavior causes 2 effects,
    // 1. DB::Open() will be slow
    // 2. During DB::Open, some of the opened sst files keep in LRUCache will be
    // deleted due to LRU policy, which causes DB::Open failed
    // set max_open_files to 0 is safe when db_option.paranoid_checks is false
    options.max_open_files = 0;

    // set ttl compaction filter
    assert(ttl_compaction_filter_ == nullptr);
    ttl_compaction_filter_ = std::make_unique<EloqDS::TTLCompactionFilter>();

    options.compaction_filter =
        static_cast<rocksdb::CompactionFilter *>(ttl_compaction_filter_.get());

    // Disable auto compactions before blocking purger
    options.disable_auto_compactions = true;

    auto start = std::chrono::steady_clock::now();
    std::unique_lock<std::shared_mutex> db_lk(db_mux_);
    rocksdb::Status status;
    uint32_t retry_num = 0;
    // When restart in tests, the rocksdb::DBCloud::Open() operation may fail
    // due to (minio) s3 service and the status only print IOError. Then, we
    // retry serveral time if failed.
    while (retry_num < 10)
    {
        status = rocksdb::DBCloud::Open(options, db_path_, "", 0, &db_);
        if (status.ok())
        {
            break;
        }
        retry_num++;
        LOG(WARNING) << "Open rocksdb cloud error : " << status.ToString()
                     << ", retrying ...";
        bthread_usleep(retry_num * 200000);
    }

    auto end = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "DBCloud Open took " << duration.count() << " ms";

    if (!status.ok())
    {
        ttl_compaction_filter_ = nullptr;

        LOG(ERROR) << "Unable to open db at path " << storage_path_
                   << " with bucket " << cfs_options.src_bucket.GetBucketName()
                   << " with error: " << status.ToString();

        // db does not exist. This node cannot escalate to be the ng leader.
        return false;
    }

    // Restore skip_cloud_files_in_getchildren to false
    // after DB::Open
    rocksdb::CloudFileSystem *cfs =
        dynamic_cast<rocksdb::CloudFileSystem *>(cloud_fs_.get());
    auto &cfs_options_ref = cfs->GetMutableCloudFileSystemOptions();
    cfs_options_ref.skip_cloud_files_in_getchildren = false;

    // Stop background work - memtable flush and compaction
    // before blocking purger
    status = db_->PauseBackgroundWork();
    if (!status.ok())
    {
        LOG(ERROR) << "Fail to pause background work, error: "
                   << status.ToString();
        // Clean up the partially initialized database
        db_->Close();
        delete db_;
        db_ = nullptr;
        ttl_compaction_filter_ = nullptr;
        return false;
    }

    // set epoch for purger event listener
    std::string current_epoch;
    status = db_->GetCurrentEpoch(&current_epoch);
    if (!status.ok())
    {
        LOG(ERROR) << "Fail to get current epoch from db, error: "
                   << status.ToString();
        // Clean up the partially initialized database
        db_->Close();
        delete db_;
        db_ = nullptr;
        ttl_compaction_filter_ = nullptr;
        return false;
    }
    if (current_epoch.empty())
    {
        LOG(ERROR) << "Current epoch from db is empty";
        db_->ContinueBackgroundWork();
        return false;
    }
    db_event_listener->SetEpoch(current_epoch);
    db_event_listener->BlockPurger();

    // Resume background work
    db_->ContinueBackgroundWork();

    // Enable auto compactions after blocking purger
    status = db_->SetOptions({{"disable_auto_compactions", "false"}});

    if (!status.ok())
    {
        LOG(ERROR) << "Fail to enable auto compactions, error: "
                   << status.ToString();
        // Clean up the partially initialized database
        db_->Close();
        delete db_;
        db_ = nullptr;
        ttl_compaction_filter_ = nullptr;
        return false;
    }

    status = db_->SetDBOptions(
        {{"max_open_files", "-1"}});  // restore max_open_files to default value

    if (!status.ok())
    {
        LOG(ERROR) << "Fail to set max_open_files to -1, error: "
                   << status.ToString();
        // Clean up the partially initialized database
        db_->Close();
        delete db_;
        db_ = nullptr;
        ttl_compaction_filter_ = nullptr;
        return false;
    }

    if (cloud_config_.warm_up_thread_num_ != 0)
    {
        db_->WarmUp(cloud_config_.warm_up_thread_num_);
    }

    LOG(INFO) << "RocksDB Cloud started";
    return true;
}

void RocksDBCloudDataStore::CreateSnapshotForBackup(
    CreateSnapshotForBackupRequest *req)
{
    bool res = query_worker_pool_->SubmitWork(
        [this, req]()
        {
            // Create a guard to ensure the poolable object is released to pool
            std::unique_ptr<PoolableGuard> poolable_guard =
                std::make_unique<PoolableGuard>(req);

            std::unique_lock<std::shared_mutex> db_lk(db_mux_);

            if (db_ == nullptr)
            {
                req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN,
                               "DB not open");
                return;
            }

            // A successful checkpoint must be guaranteed before creating
            // snapshot. So, it is not necessary to flush memtable here.

            // Waiting and stop background work - memtable flush and compaction
            db_->PauseBackgroundWork();

            rocksdb::CloudFileSystem *cfs =
                dynamic_cast<rocksdb::CloudFileSystem *>(cloud_fs_.get());

            if (cfs == nullptr)
            {
                req->SetFinish(::EloqDS::remote::DataStoreError::DB_NOT_OPEN,
                               "cloud file system is not available");
            }
            else
            {
                std::string snapshot_cookie;
                snapshot_cookie.append(req->GetBackupName());
                snapshot_cookie.append("-");
                snapshot_cookie.append(std::to_string(shard_id_));
                snapshot_cookie.append("-");
                snapshot_cookie.append(std::to_string(req->GetBackupTs()));
                rocksdb::Status status =
                    cfs->RollNewBranch(db_path_, snapshot_cookie);
                if (!status.ok())
                {
                    req->SetFinish(
                        ::EloqDS::remote::DataStoreError::CREATE_SNAPSHOT_ERROR,
                        "Fail to create snapshot, error: " + status.ToString());
                }
                else
                {
                    req->AddBackupFile(snapshot_cookie);
                    req->SetFinish(::EloqDS::remote::DataStoreError::NO_ERROR,
                                   "");
                }
            }

            // Resume background work
            db_->ContinueBackgroundWork();
        });

    if (!res)
    {
        LOG(ERROR) << "Failed to submit switch to create snapshot work to "
                      "query worker pool";
        req->SetFinish(::EloqDS::remote::DataStoreError::CREATE_SNAPSHOT_ERROR,
                       "Fail to create snapshot, error: Fail to submit work to "
                       "query worker pool");
        req->Clear();
        req->Free();
    }
}

rocksdb::DBCloud *RocksDBCloudDataStore::GetDBPtr()
{
    return db_;
}

bool RocksDBCloudDataStore::CollectCachedSstFiles(
    std::vector<::EloqDS::remote::FileInfo> &file_infos)
{
    std::shared_lock<std::shared_mutex> db_lk(db_mux_);

    if (db_ == nullptr)
    {
        LOG(ERROR) << "DB not open, cannot collect file cache";
        return false;
    }

    // Get live file metadata from RocksDB
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);

    // Get list of files in local directory
    std::set<std::string> local_files;
    std::error_code ec;
    std::filesystem::directory_iterator dir_ite(db_path_, ec);
    if (ec.value() != 0)
    {
        LOG(ERROR) << "Failed to list local directory: " << ec.message();
        return false;
    }

    for (const auto &entry : dir_ite)
    {
        if (entry.is_regular_file())
        {
            std::string filename = entry.path().filename().string();
            // Only include .sst- files (format: {file_number}.sst-{epoch})
            if (filename.find(".sst-") != std::string::npos)
            {
                local_files.insert(filename);
            }
        }
    }

    // Build intersection: files that are both in metadata and local directory
    file_infos.clear();
    rocksdb::CloudFileSystem *cfs =
        dynamic_cast<rocksdb::CloudFileSystem *>(cloud_fs_.get());
    
    for (const auto &meta : metadata)
    {
        std::string filename =
            std::filesystem::path(meta.name).filename().string();
        std::string remapped_filename = cfs->RemapFilename(filename);
        // Only include files that exist locally
        if (local_files.find(remapped_filename) != local_files.end())
        {
            ::EloqDS::remote::FileInfo file_info;
            file_info.set_file_name(remapped_filename);
            file_info.set_file_size(meta.size);
            file_info.set_file_number(ExtractFileNumber(remapped_filename));

            file_infos.push_back(file_info);
        }
    }

    DLOG(INFO) << "Collected " << file_infos.size()
               << " cached SST files for shard " << shard_id_;
    return true;
}

uint64_t RocksDBCloudDataStore::ExtractFileNumber(const std::string &file_name)
{
    // SST file names are in format: {file_number}.sst-{epoch}
    // Example: "000011.sst-ef6b2d92d3687a84"
    // Extract numeric part before ".sst-"
    size_t sst_pos = file_name.find(".sst-");
    if (sst_pos == std::string::npos)
    {
        LOG(ERROR) << "Failed to extract file number from " << file_name;
        return 0;
    }

    std::string base = file_name.substr(0, sst_pos);
    // Remove leading zeros and convert to number
    size_t first_non_zero = base.find_first_not_of('0');
    if (first_non_zero == std::string::npos)
    {
        LOG(ERROR) << "All zeros in file number from " << file_name;
        return 0;  // All zeros
    }

    try
    {
        return std::stoull(base.substr(first_non_zero));
    }
    catch (const std::exception &e)
    {
        LOG(ERROR) << "Failed to extract file number from " << file_name;
        return 0;
    }
}

inline std::string RocksDBCloudDataStore::MakeCloudManifestCookie(
    const std::string &branch_name, int64_t dss_shard_id, int64_t term)
{
    if (branch_name.empty())
    {
        return std::to_string(dss_shard_id) + "-" + std::to_string(term);
    }

    return branch_name + "-" + std::to_string(dss_shard_id) + "-" +
           std::to_string(term);
}

inline std::string RocksDBCloudDataStore::MakeCloudManifestFile(
    const std::string &dbname,
    const std::string &branch_name,
    int64_t dss_shard_id,
    int64_t term)
{
    if (branch_name.empty() && (dss_shard_id < 0 || term < 0))
    {
        return dbname + "/CLOUDMANIFEST";
    }

    assert(dss_shard_id >= 0 && term >= 0);

    return dbname + "/CLOUDMANIFEST-" + branch_name + "-" +
           std::to_string(dss_shard_id) + "-" + std::to_string(term);
}

inline bool RocksDBCloudDataStore::IsCloudManifestFile(
    const std::string &filename)
{
    return filename.find("CLOUDMANIFEST") != std::string::npos;
}

// Helper function to split a string by a delimiter
inline std::vector<std::string> RocksDBCloudDataStore::SplitString(
    const std::string &str, char delimiter)
{
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter))
    {
        tokens.push_back(token);
    }
    return tokens;
}

inline bool RocksDBCloudDataStore::GetCookieFromCloudManifestFile(
    const std::string &filename,
    std::string &branch_name,
    int64_t &dss_shard_id,
    int64_t &term)
{
    const std::string prefix = "CLOUDMANIFEST";
    auto pos = filename.rfind('/');
    std::string manifest_part =
        (pos != std::string::npos) ? filename.substr(pos + 1) : filename;

    // Check if the filename starts with "CLOUDMANIFEST"
    if (manifest_part.find(prefix) != 0)
    {
        dss_shard_id = -1;
        term = -1;
        branch_name = "";
        return false;
    }

    // Remove the prefix "CLOUDMANIFEST" to parse the rest
    std::string suffix = manifest_part.substr(prefix.size());
    DLOG(INFO) << "GetCookieFromCloudManifestFile, filename: " << filename
               << ", suffix: " << suffix;

    // If there's no suffix
    if (suffix.empty())
    {
        dss_shard_id = -1;
        term = -1;
        branch_name = "";
        return true;
    }

    // Handle the case where suffix starts with a hyphen
    if (suffix[0] == '-')
    {
        suffix = suffix.substr(1);  // Remove the leading hyphen
        // If after removing the leading hyphen, the suffix is empty
        if (suffix.empty())
        {
            return false;
        }
    }
    else
    {
        // Invalid format if it doesn't start with a hyphen
        // This should not happen
        return false;
    }

    // Find the hyphens in the suffix (after removing the leading one)
    auto last_hyphen_pos = suffix.rfind('-');
    DLOG(INFO) << "GetCookieFromCloudManifestFile, last_hyphen_pos: "
               << last_hyphen_pos << ", suffix: " << suffix;

    if (last_hyphen_pos == std::string::npos)
    {
        // No hyphen found, this is just a branch name with no shard_id or term
        // Format: CLOUDMANIFEST-{branch_name}
        branch_name = suffix;
        dss_shard_id = -1;
        term = -1;
        return true;
    }

    // Now check if there's another hyphen before the last one
    auto second_last_hyphen_pos = suffix.rfind('-', last_hyphen_pos - 1);
    DLOG(INFO) << "GetCookieFromCloudManifestFile, second_last_hyphen_pos: "
               << second_last_hyphen_pos << ", suffix: " << suffix;

    if (second_last_hyphen_pos != std::string::npos)
    {
        // We found two hyphens, format is:
        // CLOUDMANIFEST-{branch_name}-{dss_shard_id}-{term}
        branch_name = suffix.substr(0, second_last_hyphen_pos);

        // Extract the dss_shard_id and term
        std::string dss_shard_id_str =
            suffix.substr(second_last_hyphen_pos + 1,
                          last_hyphen_pos - second_last_hyphen_pos - 1);
        std::string term_str = suffix.substr(last_hyphen_pos + 1);

        // Parse dss_shard_id and term
        bool res = String2ll(
            dss_shard_id_str.c_str(), dss_shard_id_str.size(), dss_shard_id);
        if (!res)
        {
            return false;
        }
        res = String2ll(term_str.c_str(), term_str.size(), term);
        return res;
    }
    else
    {
        // Only one hyphen found, format is: CLOUDMANIFEST-{dss_shard_id}-{term}
        // with empty branch name
        branch_name = "";

        // The part before the hyphen is dss_shard_id
        std::string dss_shard_id_str = suffix.substr(0, last_hyphen_pos);
        // The part after the hyphen is term
        std::string term_str = suffix.substr(last_hyphen_pos + 1);

        // Parse dss_shard_id and term
        bool res = String2ll(
            dss_shard_id_str.c_str(), dss_shard_id_str.size(), dss_shard_id);
        if (!res)
        {
            return false;
        }
        res = String2ll(term_str.c_str(), term_str.size(), term);
        return res;
    }
}

inline bool RocksDBCloudDataStore::FindMaxTermFromCloudManifestFiles(
    const std::shared_ptr<ROCKSDB_NAMESPACE::CloudStorageProvider>
        &storage_provider,
    const std::string &bucket_prefix,
    const std::string &bucket_name,
    const std::string &object_path,
    const std::string &branch_name,
    const int64_t dss_shard_id_in_cookie,
    std::string &cloud_manifest_prefix,
    int64_t &max_term)
{
    max_term = -1;
    cloud_manifest_prefix = "CLOUDMANIFEST-" + branch_name;

    int64_t shard_id = -1;
    int64_t term = -1;

    // find the max term cookie from cloud manifest files
    // read only db should be opened with the latest cookie
    auto start = std::chrono::system_clock::now();
    std::vector<std::string> cloud_objects;
    auto st = storage_provider->ListCloudObjectsWithPrefix(
        bucket_prefix + bucket_name,
        object_path,
        cloud_manifest_prefix,
        &cloud_objects);
    if (!st.ok())
    {
        LOG(ERROR) << "Failed to list cloud objects, error: " << st.ToString();
        return false;
    }

    if (cloud_objects.empty())
    {
        LOG(ERROR) << "No cloud manifest files found in bucket: "
                   << bucket_prefix + bucket_name
                   << ", object_path: " << object_path
                   << ", with prefix: " << cloud_manifest_prefix;
        return false;
    }

    std::string object_branch_name;
    for (const auto &object : cloud_objects)
    {
        shard_id = -1;
        term = -1;
        object_branch_name = "";
        DLOG(INFO) << "FindMaxTermFromCloudManifestFiles, object: " << object;
        if (IsCloudManifestFile(object))
        {
            bool res = GetCookieFromCloudManifestFile(
                object, object_branch_name, shard_id, term);
            if (res && branch_name == object_branch_name &&
                dss_shard_id_in_cookie == shard_id)
            {
                if (term > max_term)
                {
                    max_term = term;
                }
            }
        }
    }
    LOG(INFO) << "FindMaxTermFromCloudManifestFiles, branch_name: "
              << branch_name << " cc_ng_id: " << dss_shard_id_in_cookie
              << " max_term: " << max_term;
    auto end = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "FindMaxTermFromCloudManifestFiles tooks " << duration.count()
              << " ms";

    return true;
}
}  // namespace EloqDS