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

#include <string>

#include "INIReader.h"

#if (defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                      \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS))
#include "rocksdb/cloud/db_cloud.h"
#endif

namespace EloqDS
{
struct RocksDBConfig
{
    RocksDBConfig() = default;
    explicit RocksDBConfig(const INIReader &config_reader,
                           const std::string &eloq_data_path);
    RocksDBConfig(const RocksDBConfig &) = default;

    std::string info_log_level_;
    bool enable_stats_;
    size_t stats_dump_period_sec_;
    std::string storage_path_;
    size_t max_write_buffer_number_;
    size_t max_background_jobs_;
    size_t max_background_flush_;
    size_t max_background_compaction_;
    size_t target_file_size_base_bytes_;
    size_t target_file_size_multiplier_;
    size_t write_buffer_size_bytes_;
    bool use_direct_io_for_flush_and_compaction_;
    bool use_direct_io_for_read_;
    size_t level0_stop_writes_trigger_;
    size_t level0_slowdown_writes_trigger_;
    size_t level0_file_num_compaction_trigger_;
    size_t max_bytes_for_level_base_bytes_;
    size_t max_bytes_for_level_multiplier_;
    std::string compaction_style_;
    size_t soft_pending_compaction_bytes_limit_bytes_;
    size_t hard_pending_compaction_bytes_limit_bytes_;
    size_t max_subcompactions_;
    size_t write_rate_limit_bytes_;
    size_t query_worker_num_;
    size_t batch_write_size_;
    size_t periodic_compaction_seconds_;
    std::string dialy_offpeak_time_utc_;
};

#if (defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||                      \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS))

struct S3UrlComponents
{
    std::string protocol;       // "s3", "http", "https"
    std::string bucket_name;
    std::string object_path;
    std::string endpoint_url;   // derived from http/https URLs
    bool is_valid{false};
    std::string error_message;
};

// Parse S3 URL in format:
//   s3://{bucket_name}/{object_path}
//   http://{host}:{port}/{bucket_name}/{object_path}
//   https://{host}:{port}/{bucket_name}/{object_path}
// Examples:
//   s3://my-bucket/my-path
//   http://localhost:9000/my-bucket/my-path
//   https://s3.amazonaws.com/my-bucket/my-path
inline S3UrlComponents ParseS3Url(const std::string &s3_url)
{
    S3UrlComponents result;
    
    if (s3_url.empty())
    {
        result.error_message = "S3 URL is empty";
        return result;
    }

    // Find protocol separator
    size_t protocol_end = s3_url.find("://");
    if (protocol_end == std::string::npos)
    {
        result.error_message = "Invalid S3 URL format: missing '://' separator";
        return result;
    }

    result.protocol = s3_url.substr(0, protocol_end);
    
    // Validate protocol
    if (result.protocol != "s3" && result.protocol != "http" && 
        result.protocol != "https")
    {
        result.error_message = "Invalid protocol '" + result.protocol + 
                               "'. Must be one of: s3, http, https";
        return result;
    }

    // Extract the part after protocol
    size_t path_start = protocol_end + 3;  // Skip "://"
    if (path_start >= s3_url.length())
    {
        result.error_message = "Invalid S3 URL format: no content after protocol";
        return result;
    }

    std::string remaining = s3_url.substr(path_start);
    
    // For http/https, extract the endpoint (host:port) and then the bucket/path
    // Format: http(s)://{host}:{port}/{bucket_name}/{object_path}
    if (result.protocol == "http" || result.protocol == "https")
    {
        // Find the first slash after the host:port
        size_t first_slash = remaining.find('/');
        if (first_slash == std::string::npos)
        {
            result.error_message = 
                "Invalid S3 URL format: missing bucket and object path";
            return result;
        }
        
        // Store the full endpoint URL including protocol
        result.endpoint_url = result.protocol + "://" + 
                              remaining.substr(0, first_slash);
        remaining = remaining.substr(first_slash + 1);
    }
    
    // Now extract bucket_name and object_path from remaining
    // Format: {bucket_name}/{object_path}
    size_t first_slash = remaining.find('/');
    if (first_slash == std::string::npos)
    {
        result.error_message = 
            "Invalid S3 URL format: missing object path (format: {bucket_name}/{object_path})";
        return result;
    }
    
    result.bucket_name = remaining.substr(0, first_slash);
    result.object_path = remaining.substr(first_slash + 1);
    
    if (result.bucket_name.empty())
    {
        result.error_message = "Bucket name cannot be empty";
        return result;
    }
    
    if (result.object_path.empty())
    {
        result.error_message = "Object path cannot be empty";
        return result;
    }
    
    result.is_valid = true;
    return result;
}

struct RocksDBCloudConfig
{
    RocksDBCloudConfig() = default;
    explicit RocksDBCloudConfig(const INIReader &config);

    RocksDBCloudConfig(const RocksDBCloudConfig &) = default;

    std::string aws_access_key_id_;
    std::string aws_secret_key_;
    std::string bucket_name_;
    std::string bucket_prefix_;
    std::string object_path_;
    std::string region_;
    uint64_t sst_file_cache_size_;
    int sst_file_cache_num_shard_bits_;
    uint32_t db_ready_timeout_us_;
    uint32_t db_file_deletion_delay_;
    std::string s3_endpoint_url_;
    std::string s3_url_;  // New URL-based configuration
    size_t warm_up_thread_num_;
    bool run_purger_{true};
    size_t purger_periodicity_millis_{10 * 60 * 1000}; // 10 minutes
    std::string branch_name_;

    // Returns true if S3 URL configuration is being used
    bool IsS3UrlConfigured() const { return !s3_url_.empty(); }
};

inline rocksdb::Status NewCloudFileSystem(
    const rocksdb::CloudFileSystemOptions &cfs_options,
    rocksdb::CloudFileSystem **cfs)
{
    rocksdb::Status status;
    // Create a cloud file system
#if defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
    // AWS s3 file system
    status = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
        rocksdb::FileSystem::Default(), cfs_options, nullptr, cfs);
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
    // Google cloud storage file system
    status = rocksdb::CloudFileSystemEnv::NewGcpFileSystem(
        rocksdb::FileSystem::Default(), cfs_options, nullptr, cfs);
#endif
    return status;
};
#endif
}  // namespace EloqDS
