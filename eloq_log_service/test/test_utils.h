#pragma once

#include <assert.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <regex>
#include <string>
#include <thread>

#include "glog/logging.h"
#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_ALL)

#if defined(LOG_STATE_TYPE_RKDB_S3)
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#elif defined(LOG_STATE_TYPE_RKDB_GCS)
#include <google/cloud/storage/client.h>
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb_cloud_config.h"
#endif

#endif

using namespace std::chrono_literals;

inline void show_spinner(const std::string &prefix)
{
    static char bars[] = {'/', '-', '\\', '|'};
    static int nbars = sizeof(bars) / sizeof(char);
    static int pos = 0;

    std::cout << '\r' << prefix << bars[pos];
    std::cout.flush();
    pos = (pos + 1) % nbars;
};

inline auto now()
{
    return std::chrono::high_resolution_clock::now();
};

struct separate_thousands : std::numpunct<char>
{
    char_type do_thousands_sep() const override
    {
        return ',';
    }  // separate with commas
    string_type do_grouping() const override
    {
        return "\3";
    }  // groups of 3 digit
};

inline void test_run_timer(std::atomic<bool> &interrupt,
                           const uint32_t duration,
                           bool show_running_time = true)
{
    auto start = now();
    uint32_t time_last = 0;
    while (time_last < duration)
    {
        time_last =
            std::chrono::duration_cast<std::chrono::seconds>(now() - start)
                .count();
        if (show_running_time)
        {
            std::cout << "\r"
                      << "Test running time: " << time_last << " seconds";
        }
        std::cout.flush();
        std::this_thread::sleep_for(1000ms);
    }

    interrupt.store(true, std::memory_order_release);
}

using HighResolutionTimePoint =
    std::chrono::time_point<std::chrono::high_resolution_clock>;

inline uint64_t duration(const HighResolutionTimePoint start,
                         const HighResolutionTimePoint end)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
        .count();
}

inline uint64_t duration_micro(const HighResolutionTimePoint start,
                               const HighResolutionTimePoint end)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start)
        .count();
}

inline uint64_t qps(uint64_t count, uint64_t duration /*in milliseconds*/)
{
    if (duration == 0)
    {
        return 0;
    }
    return static_cast<uint64_t>(static_cast<double>(count) / duration * 1000);
}

inline uint64_t throughput(uint64_t size, uint64_t duration /*in milliseconds*/)
{
    if (duration == 0)
    {
        return 0;
    }
    return static_cast<uint64_t>(static_cast<double>(size) / 1024 / 1024 /
                                 duration * 1000);
}

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
inline bool DropBucket(std::string region,
                       const std::string &bucket_prefix,
                       const std::string &bucket_name)
{
    LOG(INFO) << "Drop bucket " << bucket_prefix << bucket_name;
    rocksdb::CloudFileSystemOptions cfs_options;
    cfs_options.src_bucket.SetBucketName(bucket_name, bucket_prefix);
    cfs_options.src_bucket.SetRegion(region);
    cfs_options.src_bucket.SetObjectPath("rocksdb_cloud");
    cfs_options.dest_bucket.SetBucketName(bucket_name, bucket_prefix);
    cfs_options.dest_bucket.SetRegion(region);
    cfs_options.dest_bucket.SetObjectPath("rocksdb_cloud");

    rocksdb::CloudFileSystem *cfs;
    auto status = txlog::NewCloudFileSystem(cfs_options, &cfs);

    if (!status.ok())
    {
        LOG(ERROR) << "Failed to create cloud file system: "
                   << status.ToString();
        return false;
    }

    auto storage_provider = cfs->GetStorageProvider();
    std::string bucket_name_with_prefix = bucket_prefix + bucket_name;
    rocksdb::IOStatus io_status =
        storage_provider->ExistsBucket(bucket_name_with_prefix);

    if (io_status.IsNotFound())
    {
        LOG(INFO) << "Bucket " << bucket_name_with_prefix << " not found";
        delete cfs;
        return true;
    }
    else if (!io_status.ok())
    {
        LOG(ERROR) << "Failed to check bucket " << bucket_name_with_prefix
                   << ": " << io_status.ToString();
        delete cfs;
        return false;
    }

    status = storage_provider->EmptyBucket(bucket_name_with_prefix, "");

    if (!status.ok())
    {
        LOG(ERROR) << "Failed to empty bucket " << bucket_name_with_prefix
                   << ": " << status.ToString();
        delete cfs;
        return false;
    }

    delete cfs;

    // DeleteBucket, storage_provider does not support DeleteBucket, using raw
    // api instead
#if defined(LOG_STATE_TYPE_RKDB_S3)
    Aws::Client::ClientConfiguration client_config;
    client_config.region = region;
    Aws::S3::S3Client s3_client(client_config);
    Aws::S3::Model::DeleteBucketRequest delete_bucket_request;
    delete_bucket_request.SetBucket(Aws::String(
        bucket_name_with_prefix.data(), bucket_name_with_prefix.size()));
    Aws::S3::Model::DeleteBucketOutcome delete_bucket_outcome =
        s3_client.DeleteBucket(delete_bucket_request);
    if (!delete_bucket_outcome.IsSuccess())
    {
        LOG(ERROR) << "Failed to delete bucket " << bucket_name_with_prefix
                   << ": " << delete_bucket_outcome.GetError().GetMessage();
        return false;
    }
#elif defined(LOG_STATE_TYPE_RKDB_GCS)

    google::cloud::storage::Client client;
    google::cloud::Status delete_status =
        client.DeleteBucket(bucket_name_with_prefix);
    if (!delete_status.ok())
    {
        LOG(ERROR) << "Failed to delete bucket " << bucket_name_with_prefix
                   << ": " << delete_status.message();
        return false;
    }
#endif

    LOG(INFO) << "Successfully drop bucket " << bucket_name_with_prefix;
    return true;
}

inline bool CreateBucket(std::string region,
                         std::string bucket_prefix,
                         std::string bucket_name)
{
    LOG(INFO) << "Create bucket " << bucket_prefix << bucket_name;
    rocksdb::CloudFileSystemOptions cfs_options;
    cfs_options.src_bucket.SetBucketName(bucket_name, bucket_prefix);
    cfs_options.src_bucket.SetRegion(region);
    cfs_options.src_bucket.SetObjectPath("rocksdb_cloud");
    cfs_options.dest_bucket.SetBucketName(bucket_name, bucket_prefix);
    cfs_options.dest_bucket.SetRegion(region);
    cfs_options.dest_bucket.SetObjectPath("rocksdb_cloud");

    rocksdb::CloudFileSystem *cfs;
    auto status = txlog::NewCloudFileSystem(cfs_options, &cfs);

    if (!status.ok())
    {
        LOG(ERROR) << "Failed to create cloud file system: "
                   << status.ToString();
        return false;
    }

    auto storage_provider = cfs->GetStorageProvider();
    std::string bucket_name_with_prefix = bucket_prefix + bucket_name;
    status = storage_provider->CreateBucket(bucket_name_with_prefix);

    if (!status.ok())
    {
        LOG(ERROR) << "Failed to create bucket " << bucket_name_with_prefix
                   << ": " << status.ToString();
        delete cfs;
        return false;
    }

    delete cfs;

    LOG(INFO) << "Successfully create bucket " << bucket_name_with_prefix;
    return true;
}
#endif