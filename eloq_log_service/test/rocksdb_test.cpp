#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_ALL)
#include <gflags/gflags.h>

#include <chrono>
#include <csignal>
#include <iostream>
#include <random>
#include <thread>

#if defined(LOG_STATE_TYPE_RKDB_S3)
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include "rocksdb/cloud/db_cloud.h"
#else
#include "rocksdb/db.h"
#endif

#include "test_utils.h"

DEFINE_string(database_path, "tiered_rocksdb_db", "Test database path");
DEFINE_string(log_message,
              "The quick brown fox jumps over the lazy dog",
              "log message");
DEFINE_uint32(test_duration, 20, "Test duration");
DEFINE_uint32(sleep_duration,
              30,
              "Sleep duration in middle of insert and iterate");

#if defined(LOG_STATE_TYPE_RKDB_S3)
DEFINE_string(aws_access_key_id, "", "AWS_ACCESS_KEY_ID");
DEFINE_string(aws_secret_access_key, "", "AWS_SECRET_ACCESS_KEY");
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
DEFINE_string(bucket_name, "", "S3 bucket name");
DEFINE_string(bucket_prefix, "", "S3 bucket prefix");
DEFINE_string(region, "", "Regin");
DEFINE_string(sst_file_cache_size, "1GB", "Local sst cache size");
#endif

DEFINE_bool(populate_data, true, "Populate data at first");
DEFINE_bool(scan_data, true, "Scan data");

// random seed
std::random_device rd;
std::default_random_engine generator(rd());
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);
// key for query
std::array<char, 20> random_query_key;

// cloud db
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
rocksdb::DBCloud *db;
#else
rocksdb::DB *db;
#endif

void Serialize(std::array<char, 20> &res,
               uint32_t ng_id,
               uint64_t timestamp,
               uint64_t tx_number)
{
    char *p = res.data();
    uint32_t ng_id_be = __builtin_bswap32(ng_id);
    std::memcpy(p, &ng_id_be, sizeof(uint32_t));
    // std::memcpy(p, &ng_id, sizeof(uint32_t));

    p += sizeof(uint32_t);
    uint64_t ts_be = __builtin_bswap64(timestamp);
    std::memcpy(p, &ts_be, sizeof(uint64_t));
    // std::memcpy(p, &timestamp, sizeof(uint64_t));

    p += sizeof(uint64_t);
    uint64_t tx_no_be = __builtin_bswap64(tx_number);
    std::memcpy(p, &tx_no_be, sizeof(uint64_t));
    // std::memcpy(p, &tx_number, sizeof(uint64_t));
}

void populate_data(std::atomic<bool> &interrupt)
{
    if (!FLAGS_populate_data)
    {
        std::cout << "Insert bypassed." << std::endl;
        return;
    }

    rocksdb::WriteOptions w_opt;
    w_opt.disableWAL = true;

    // insert db
    uint64_t cnt = 0;
    auto s = now();
    uint64_t populate_size = 0;
    while (!interrupt.load(std::memory_order_acquire))
    {
        std::array<char, 20> key{};
        uint64_t tx_number = distribution(generator);
        uint64_t timestamp = now().time_since_epoch().count();
        cnt++;
        Serialize(key, cnt % 3, timestamp, tx_number);
        if (tx_number % 100 == 1)
        {
            // update random_query_key by 1/10 chance
            random_query_key = key;
        }
        std::string log_message = FLAGS_log_message;
        log_message.append(key.data());
        auto status =
            db->Put(w_opt, rocksdb::Slice(key.data(), key.size()), log_message);
        if (!status.ok())
        {
            std::cerr << status.ToString() << std::endl;
        }
        populate_size += key.size();
        populate_size += log_message.size();

        // Keep recent sst kept in LRU cache(This seems not necessary, and this
        // iter will impact perf a lot) uint64_t interval =
        // std::chrono::duration_cast<std::chrono::seconds>(now() - s).count();
        // if (interval >= 5)
        //{
        // rocksdb::Iterator *it = db->NewIterator(rocksdb::ReadOptions());
        // it->SeekToFirst();
        // it->Next();
        // delete it;
        //}
        if (cnt % 1000000L == 0)
        {
            std::cout << "Insert 1 million records" << std::endl;
            rocksdb::FlushOptions flush_opt;
            flush_opt.allow_write_stall = true;
            flush_opt.wait = true;
            db->Flush(flush_opt);
        }
    }

    auto e = now();
    uint64_t t = duration(s, e);
    std::cout << "Insert " << cnt << " records in " << t << " milliseconds."
              << " qps: " << qps(cnt, t)
              << " throughput: " << throughput(populate_size, t) << "MB/s"
              << std::endl;
}

void query_by_key()
{
    std::string value;
    auto s = now();
    rocksdb::Status status = db->Get(
        rocksdb::ReadOptions(),
        rocksdb::Slice(random_query_key.data(), random_query_key.size()),
        &value);
    auto e = now();
    if (!status.ok())
    {
        std::cerr << "Read key failed, " << status.ToString() << std::endl;
        return;
    }
    auto t = duration_micro(s, e);
    std::cout << "Read key cost " << t << " microseconds." << std::endl;
}

void query_by_key_loop(std::atomic<bool> &interrupt)
{
    if (!FLAGS_populate_data)
    {
        std::cout << "Query by key bypassed." << std::endl;
        return;
    }

    while (!interrupt.load(std::memory_order_acquire))
    {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        query_by_key();
    }
}

void scan_data(uint32_t cc_ng)
{
    // iterate records
    uint64_t read_size = 0;
    uint64_t cnt = 0;
    auto s = now();

    std::array<char, 20> start_key{}, limit_key{};
    rocksdb::ReadOptions r_opt;
    // query data in between now and 10 seconds before
    auto n = now();
    auto before_n = n - std::chrono::seconds(60);
    uint64_t n_t = n.time_since_epoch().count();
    uint64_t before_n_t = before_n.time_since_epoch().count();
    Serialize(start_key, cc_ng, before_n_t, 0);
    Serialize(limit_key, cc_ng, n_t, 0);

    rocksdb::Slice start_key_slice =
        rocksdb::Slice(start_key.data(), start_key.size());
    rocksdb::Slice limit_key_slice =
        rocksdb::Slice(limit_key.data(), limit_key.size());

    r_opt.iterate_lower_bound = &start_key_slice;
    r_opt.iterate_upper_bound = &limit_key_slice;
    rocksdb::Iterator *it = db->NewIterator(r_opt);
    it->SeekToFirst();
    if (!it->status().ok())
    {
        std::cout << "Iterate error: " << it->status().ToString() << std::endl;
        return;
    }
    while (true)
    {
        if (!it->Valid())
        {
            if (!it->status().ok())
            {
                std::cout << "Iterate error: " << it->status().ToString()
                          << std::endl;
                return;
            }
            else
            {
                break;
            }
        }

        rocksdb::Slice key = it->key();
        rocksdb::Slice value = it->value();
        read_size += key.size();
        read_size += value.size();
        cnt++;
        if (cnt % 1000000L == 0)
        {
            std::cout << "Iterate over 1 million records for "
                      << std::to_string(cc_ng) << " cc node group" << std::endl;
        }
        it->Next();
    }

    auto e = now();
    auto t =
        std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count();
    std::cout << "Scan " << cnt << " key/values in " << t
              << " milliseconds, at speed of " << throughput(read_size, t)
              << "(MB/s), qps: " << qps(cnt, t) << std::endl;
}

void scan_data_loop(std::atomic<bool> &interrupt)
{
    if (!FLAGS_scan_data)
    {
        std::cout << "Scan data bypassed" << std::endl;
        return;
    }

    while (!interrupt.load(std::memory_order_acquire))
    {
        std::this_thread::sleep_for(std::chrono::seconds(60));
        uint64_t seed = distribution(generator);
        scan_data(seed % 3);
    }
}

int main(int argc, char *argv[])
{
    // https://github.com/aws/aws-sdk-cpp/issues/1534
    // signal(SIGPIPE, SIG_IGN);

    GFLAGS_NAMESPACE::SetUsageMessage(
        "Usage: rocksdb_tier_storage_test "
        "-database_path=/path_to_database "
        "-test_duration=120 -aws_access_key_id=ak -aws_secret_access_key_=sk "
        "-aws_s3_bucket_name=bk -aws_s3_bucket_prefix=bp -aws_region=region "
        "-log_message=\"messages for log\"");
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);

    // Wait for user input to start
    std::cout << "Press any key to continue...\n";
    std::cin.get();

    // format number by thousands
    auto thousands = std::make_unique<separate_thousands>();
    std::cout.imbue(std::locale(std::cout.getloc(), thousands.release()));

    uint32_t test_duration = FLAGS_test_duration;
    rocksdb::Status status;

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)

#if defined(LOG_STATE_TYPE_RKDB_S3)
    Aws::SDKOptions aws_options;
    aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    Aws::InitAPI(aws_options);
#endif

    // cloud fs config
    rocksdb::CloudFileSystemOptions cfs_options;
    // Store a reference to a cloud file system. A new cloud file system object
    // should be associated with every new cloud-db.
    std::shared_ptr<rocksdb::FileSystem> cloud_fs;
    // cloud fs
#if defined(LOG_STATE_TYPE_RKDB_S3)
    cfs_options.credentials.InitializeSimple(FLAGS_aws_access_key_id,
                                             FLAGS_aws_secret_access_key);
    if (!cfs_options.credentials.HasValid().ok())
    {
        std::cerr
            << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY is required."
            << std::endl;
        return -1;
    }
#endif
    cfs_options.src_bucket.SetBucketName(FLAGS_bucket_name,
                                         FLAGS_bucket_prefix);
    cfs_options.src_bucket.SetRegion(FLAGS_region);
    cfs_options.dest_bucket.SetObjectPath(FLAGS_database_path);
    cfs_options.dest_bucket.SetBucketName(FLAGS_bucket_name,
                                          FLAGS_bucket_prefix);
    cfs_options.dest_bucket.SetRegion(FLAGS_region);
    cfs_options.dest_bucket.SetObjectPath(FLAGS_database_path);

    // cfs_options.use_aws_transfer_manager = true;
    // cfs_options.use_direct_io_for_cloud_download = true;
    // cfs_options.skip_cloud_files_in_getchildren = true;

    // AWS s3 file system
    rocksdb::CloudFileSystem *cfs;
    status = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
        rocksdb::FileSystem::Default(), cfs_options, nullptr, &cfs);

    if (!status.ok())
    {
        std::cerr << "Unable to open db at path " << FLAGS_database_path
                  << " with bucket " << FLAGS_bucket_name << std::endl;
        return -1;
    }
    cloud_fs.reset(cfs);
    // Create options and use the AWS file system that we created earlier
    auto cloud_env = rocksdb::NewCompositeEnv(cloud_fs);
#endif

    rocksdb::Options options;
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    options.env = cloud_env.get();
#endif
    options.create_if_missing = true;
    // this option is important, this set disable_auto_compaction to false will
    // half the throughput (100MB/s -> 50MB/s)
    // options.disable_auto_compactions = true;
    // universal compaction style can improve throughput by 2x
    options.compaction_style = rocksdb::kCompactionStyleUniversal;
    options.num_levels = 2;
    options.info_log_level = rocksdb::INFO_LEVEL;
    options.max_open_files = 0;
    options.best_efforts_recovery = false;
    options.skip_checking_sst_file_sizes_on_db_open = true;
    options.skip_stats_update_on_db_open = true;
    options.atomic_flush = true;
    // options.disable_auto_flush = true;

    auto s = now();
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    status = rocksdb::DBCloud::Open(options, FLAGS_database_path, "", 0, &db);
#else
    status = rocksdb::DB::Open(options, FLAGS_database_path, &db);
#endif

    auto e = now();
    uint64_t t = duration(s, e);
    std::cout << "DB::Open() cost " << t << " millseconds" << std::endl;

    if (!status.ok())
    {
        std::cerr << status.ToString() << std::endl;
        return -1;
    }

    std::atomic<bool> interrupt{};
    interrupt.store(false, std::memory_order_release);
    std::thread duration_timer_thd =
        std::thread(test_run_timer, std::ref(interrupt), test_duration, false);

    std::thread populate_data_thd =
        std::thread(populate_data, std::ref(interrupt));

    std::thread query_by_key_thd =
        std::thread(query_by_key_loop, std::ref(interrupt));

    // std::thread scan_data_thd =
    // std::thread(scan_data_loop, std::ref(interrupt));

    duration_timer_thd.join();
    populate_data_thd.join();
    query_by_key_thd.join();
    // scan_data_thd.join();

    scan_data(0);

    // print the database size
    uint64_t sst_files_size = 0;
    db->GetIntProperty(rocksdb::DB::Properties::kTotalSstFilesSize,
                       &sst_files_size);
    std::cout << "Database size: " << sst_files_size / 1024 / 1024 << "MB"
              << std::endl;

    std::string level_stats;
    db->GetProperty(rocksdb::DB::Properties::kLevelStats, &level_stats);
    std::cout << level_stats << std::endl;

    // std::cout << "Test done! Wait 30s for RocksDB doing staff." << std::endl;
    // std::this_thread::sleep_for(30s);

    db->Close();
    delete db;

#if defined(LOG_STATE_TYPE_RKDB_S3)
    Aws::ShutdownAPI(aws_options);
#endif
}
#endif
