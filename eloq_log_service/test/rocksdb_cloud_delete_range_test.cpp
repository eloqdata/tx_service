#include <iostream>

#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_S3)  // S3
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <gflags/gflags.h>

#include <chrono>
#include <csignal>
#include <random>
#include <thread>

#include "log_utils.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "test_utils.h"

DEFINE_string(database_path, "rocksdb_cloud_db", "Test database path");
DEFINE_string(aws_access_key_id, "", "AWS_ACCESS_KEY_ID");
DEFINE_string(aws_secret_access_key, "", "AWS_SECRET_ACCESS_KEY");
DEFINE_string(aws_s3_bucket_name, "test-delete-range", "AWS S3 bucket name");
DEFINE_string(aws_s3_bucket_prefix, "rocksdb-", "AWS S3 bucket prefix");
DEFINE_string(aws_region, "", "AWS Regin");
DEFINE_string(sst_file_cache_size, "3GB", "Local sst cache size");
DEFINE_uint64(total_records, 5000000, "Total records to populate");

// random seed
std::random_device rd;
std::default_random_engine generator(rd());
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);

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

void PrintDBSize(rocksdb::DBCloud *db)
{
    // Get the live files metadata
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);

    // Calculate the total live file size
    uint64_t total_size = 0;
    for (const auto &file : metadata)
    {
        total_size += file.size;
    }

    LOG(INFO) << "Live File Size: " << total_size / 1024 / 1024 << " MB";
}

void count_rows_before_timestamp(rocksdb::DBCloud *db, uint64_t timestamp_bound)
{
    for (int ng_id = 0; ng_id < 3; ng_id++)
    {
        std::array<char, 20> start_key{};
        std::array<char, 20> end_key{};
        Serialize(start_key, ng_id, 0, 0);
        Serialize(end_key, ng_id, timestamp_bound, 0);
        rocksdb::ReadOptions read_options;
        rocksdb::Slice lower_bound =
            rocksdb::Slice(start_key.data(), start_key.size());
        rocksdb::Slice upper_bound =
            rocksdb::Slice(end_key.data(), end_key.size());
        read_options.iterate_upper_bound = &upper_bound;
        auto iter = db->NewIterator(read_options);
        uint64_t cnt = 0;
        for (iter->Seek(lower_bound); iter->Valid(); iter->Next())
        {
            cnt++;
        }
        if (!iter->status().ok())
        {
            std::cerr << "Error when iteration: " << iter->status().ToString();
        }
        delete iter;
        LOG(INFO) << "rows in ng_id " << ng_id << " ,purge_t "
                  << timestamp_bound << " is " << cnt;
    }
}

void DumpRocksDBLiveFiles(rocksdb::DBCloud *db)
{
    // Get the live files metadata
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);

    LOG(INFO) << "RocksDB live files: " << metadata.size();
    for (const auto &file : metadata)
    {
        LOG(INFO) << "file level: " << file.level
                  << ", file name: " << file.name
                  << ", size: " << file.size / 1024 / 1024 << "MB";
    }
}

void populate_data(rocksdb::DBCloud *db, const uint64_t total_records_count)
{
    rocksdb::WriteOptions w_opt;
    w_opt.disableWAL = true;
    std::string log_message = "The quick brown fox jumps over the lazy dog";

    // insert db
    uint32_t cnt = 0;
    auto s = now();
    uint64_t populate_size = 0;
    while (cnt < total_records_count)
    {
        std::array<char, 20> key{};
        uint64_t tx_number = distribution(generator);
        uint64_t timestamp = now().time_since_epoch().count();
        cnt++;
        Serialize(key, cnt % 3, timestamp, tx_number);
        auto status =
            db->Put(w_opt, rocksdb::Slice(key.data(), key.size()), log_message);
        if (!status.ok())
        {
            std::cerr << status.ToString() << std::endl;
        }
        populate_size += key.size();
        populate_size += log_message.size();

        if (cnt % 1000000L == 0)
        {
            LOG(INFO) << "Insert " << cnt / 1000000L << " million records";
            // rocksdb::FlushOptions flush_opt;
            // flush_opt.allow_write_stall = true;
            // flush_opt.wait = true;
            // db->Flush(flush_opt);
        }
    }

    auto e = now();
    uint64_t t = duration(s, e);
    LOG(INFO) << "Insert " << cnt << " records in " << t << " milliseconds."
              << " qps: " << qps(cnt, t)
              << " throughput: " << throughput(populate_size, t) << "MB/s";
}

int main(int argc, char *argv[])
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    LOG(INFO) << "Total records: " << FLAGS_total_records;
    LOG(INFO) << "Database path: " << FLAGS_database_path;
    LOG(INFO) << "Aws access key id set: " << !FLAGS_aws_access_key_id.empty();
    LOG(INFO) << "Aws secret access key set: "
              << !FLAGS_aws_secret_access_key.empty();
    LOG(INFO) << "Aws region: " << FLAGS_aws_region;
    LOG(INFO) << "Aws s3 bucket prefix: " << FLAGS_aws_s3_bucket_prefix;
    LOG(INFO) << "Aws s3 bucket name: " << FLAGS_aws_s3_bucket_name;
    LOG(INFO) << "Sst file cache size: " << FLAGS_sst_file_cache_size;

    // open rocksdb cloud
    Aws::SDKOptions aws_options;
    aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    aws_options.httpOptions.installSigPipeHandler = true;
    Aws::InitAPI(aws_options);

    // cloud fs config
    rocksdb::CloudFileSystemOptions cfs_options;
    // Store a reference to a cloud file system. A new cloud file system object
    // should be associated with every new cloud-db.
    std::shared_ptr<rocksdb::FileSystem> cloud_fs;
    // cloud fs
    if (FLAGS_aws_access_key_id.length() == 0 ||
        FLAGS_aws_secret_access_key.length() == 0)
    {
        cfs_options.credentials.type = rocksdb::AwsAccessType::kInstance;
    }
    else
    {
        cfs_options.credentials.InitializeSimple(FLAGS_aws_access_key_id,
                                                 FLAGS_aws_secret_access_key);
    }

    if (!cfs_options.credentials.HasValid().ok())
    {
        LOG(ERROR)
            << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY is required.";
        return -1;
    }

    cfs_options.src_bucket.SetBucketName(FLAGS_aws_s3_bucket_name,
                                         FLAGS_aws_s3_bucket_prefix);
    cfs_options.dest_bucket.SetBucketName(FLAGS_aws_s3_bucket_name,
                                          FLAGS_aws_s3_bucket_prefix);
    // cfs_options.use_aws_transfer_manager = true;
    // cfs_options.use_direct_io_for_cloud_download = true;
    // cfs_options.skip_cloud_files_in_getchildren = true;

    // AWS s3 file system
    rocksdb::CloudFileSystem *cfs;
    auto status = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
        rocksdb::FileSystem::Default(),
        FLAGS_aws_s3_bucket_name,
        FLAGS_database_path,
        FLAGS_aws_region,
        FLAGS_aws_s3_bucket_name,
        FLAGS_database_path,
        FLAGS_aws_region,
        cfs_options,
        nullptr,
        &cfs);
    if (!status.ok())
    {
        LOG(ERROR) << "Unable to open db at path " << FLAGS_database_path
                   << " with bucket " << FLAGS_aws_s3_bucket_name
                   << ", error: " << status.ToString();
        return -1;
    }
    cloud_fs.reset(cfs);
    // Create options and use the AWS file system that we created earlier
    auto cloud_env = rocksdb::NewCompositeEnv(cloud_fs);
    rocksdb::Options options;
    options.env = cloud_env.get();
    options.create_if_missing = true;
    options.compaction_style = rocksdb::kCompactionStyleUniversal;
    options.num_levels = 2;
    options.info_log_level = rocksdb::INFO_LEVEL;
    options.best_efforts_recovery = false;
    options.skip_checking_sst_file_sizes_on_db_open = true;
    options.skip_stats_update_on_db_open = true;
    options.atomic_flush = true;
    // The max_open_files default value is -1, it cause DB open all files on
    // DB::Open() This behavior causes 2 effects,
    // 1. DB::Open() will be slow
    // 2. During DB::Open, some of the opened sst files keep in LRUCache will be
    // deleted due to LRU policy, which causes DB::Open failed
    options.max_open_files = 0;

    rocksdb::DBCloud *db;
    status = rocksdb::DBCloud::Open(options, FLAGS_database_path, "", 0, &db);
    if (!status.ok())
    {
        LOG(ERROR) << "Open rocksdb cloud failed, error: " << status.ToString();
        return -1;
    }

    // Reset max_open_files to default value of -1 after DB::Open
    db->SetDBOptions({{"max_open_files", "-1"}});

    auto start_t = now().time_since_epoch();
    populate_data(db, FLAGS_total_records);
    auto end_t = now().time_since_epoch();
    uint64_t purge_t = start_t.count() + (end_t.count() - start_t.count()) / 2;

    LOG(INFO) << "--- Dump RocksDB live file list after populate data";
    DumpRocksDBLiveFiles(db);
    PrintDBSize(db);

    LOG(INFO) << "--- Count the rows near half" << std::endl;
    count_rows_before_timestamp(db, purge_t);

    LOG(INFO) << "--- Start compaction" << std::endl;
    start_t = now().time_since_epoch();
    rocksdb::CompactRangeOptions cpt_opt;
    db->CompactRange(cpt_opt, nullptr, nullptr);
    end_t = now().time_since_epoch();
    LOG(INFO) << "--- Stop compaction, takes "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_t -
                                                                       start_t)
                     .count();
    LOG(INFO) << "--- Dump RocksDB live file list after compaction"
              << std::endl;
    DumpRocksDBLiveFiles(db);
    PrintDBSize(db);

    LOG(INFO) << "--- Delete files in range";
    start_t = now().time_since_epoch();
    for (int ng_id = 0; ng_id < 3; ng_id++)
    {
        std::array<char, 20> start_key{};
        std::array<char, 20> end_key{};
        Serialize(start_key, ng_id, 0, 0);
        Serialize(end_key, ng_id, purge_t, 0);
        rocksdb::Slice lower_bound =
            rocksdb::Slice(start_key.data(), start_key.size());
        rocksdb::Slice upper_bound =
            rocksdb::Slice(end_key.data(), end_key.size());
        status = rocksdb::DeleteFilesInRange(
            db, db->DefaultColumnFamily(), &lower_bound, &upper_bound);
        if (!status.ok())
        {
            LOG(ERROR) << "DeleteFilesInRange failed for ng_id " << ng_id
                       << ", error: " << status.ToString();
            return -1;
        }
    }
    end_t = now().time_since_epoch();
    LOG(INFO) << "--- Delete files in range, takes "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_t -
                                                                       start_t)
                     .count();

    LOG(INFO)
        << "--- Dump RocksDB live file list after deleting files in range";
    DumpRocksDBLiveFiles(db);
    PrintDBSize(db);

    count_rows_before_timestamp(db, purge_t);

    LOG(INFO) << "Sleep 20 seconds before quit, wait for cloud file deletion ";
    sleep(20);

    db->Close();
    delete db;
    db = nullptr;

    cloud_env = nullptr;
    cloud_fs = nullptr;

    Aws::ShutdownAPI(aws_options);
}
#else
int main(int argc, char *argv[])
{
    std::cerr << "This test requires LOG_STATE_TYPE_RKDB_S3 defined."
              << std::endl;
    return -1;
}
#endif  // LOG_STATE_TYPE_RKDB_S3
