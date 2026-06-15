#pragma once

#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include <string>

#include "rocksdb/cloud/cloud_file_system.h"

namespace txlog
{
struct S3UrlComponents
{
    std::string protocol;  // "s3", "gs", "http", "https"
    std::string bucket_name;
    std::string object_path;
    std::string endpoint_url;  // derived from http/https URLs
    bool is_valid{false};
    std::string error_message;
};

inline std::string to_lower_copy(std::string s)
{
    std::transform(s.begin(),
                   s.end(),
                   s.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return s;
}

// Parse OSS URL in format:
//   s3://{bucket_name}/{object_path}
//   gs://{bucket_name}/{object_path}
//   http://{host}:{port}/{bucket_name}/{object_path}
//   https://{host}:{port}/{bucket_name}/{object_path}
// Examples:
//   s3://my-bucket/my-path
//   gs://my-bucket/my-path
//   http://localhost:9000/my-bucket/my-path
//   https://s3.amazonaws.com/my-bucket/my-path
inline S3UrlComponents ParseS3Url(const std::string &s3_url)
{
    S3UrlComponents result;

    if (s3_url.empty())
    {
        result.error_message = "OSS URL is empty";
        return result;
    }

    // Find protocol separator
    size_t protocol_end = s3_url.find("://");
    if (protocol_end == std::string::npos)
    {
        result.error_message =
            "Invalid OSS URL format: missing '://' separator";
        return result;
    }

    result.protocol = to_lower_copy(s3_url.substr(0, protocol_end));

    // Validate protocol
    if (result.protocol != "s3" && result.protocol != "gs" &&
        result.protocol != "http" && result.protocol != "https")
    {
        result.error_message = "Invalid protocol '" + result.protocol +
                               "'. Must be one of: s3, gs, http, https";
        return result;
    }

    // Extract the part after protocol
    size_t path_start = protocol_end + 3;  // Skip "://"
    if (path_start >= s3_url.length())
    {
        result.error_message =
            "Invalid OSS URL format: no content after protocol";
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
                "Invalid OSS URL format: missing bucket and object path";
            return result;
        }

        // Store the full endpoint URL including protocol
        result.endpoint_url =
            result.protocol + "://" + remaining.substr(0, first_slash);
        remaining = remaining.substr(first_slash + 1);
    }

    // Now extract bucket_name and object_path from remaining
    // Format: {bucket_name}/{object_path}
    size_t first_slash = remaining.find('/');
    if (first_slash == std::string::npos)
    {
        result.error_message =
            "Invalid OSS URL format: missing object path (format: "
            "{bucket_name}/{object_path})";
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

    std::string aws_access_key_id_{""};
    std::string aws_secret_key_{""};
    std::string bucket_name_{""};
    std::string bucket_prefix_{""};
    std::string object_path_{""};
    std::string region_{""};
    std::string endpoint_url_{""};
    std::string oss_url_{""};  // Object Storage Service URL-based configuration
                               // (supports s3://, gs://, http://, https://)
    uint64_t sst_file_cache_size_;
    int sst_file_cache_num_shard_bits_{5};
    uint32_t db_ready_timeout_us_;
    uint32_t db_file_deletion_delay_;
    uint32_t log_retention_days_;
    uint32_t log_purger_starting_hour_;
    uint32_t log_purger_starting_minute_;
    uint32_t log_purger_starting_second_;

    // Archive configuration - uses same bucket as active data, different object
    // path
    std::string archive_object_path_{
        ""};  // Defaults to object_path_ + "_archives"
    uint32_t archive_move_interval_seconds_{600};  // Default: 10 minutes

    // Returns true if OSS URL configuration is being used
    bool IsOssUrlConfigured() const
    {
        return !oss_url_.empty();
    }
};

inline rocksdb::Status NewCloudFileSystem(
    const rocksdb::CloudFileSystemOptions &cfs_options,
    rocksdb::CloudFileSystem **cfs)
{
#if defined(LOG_STATE_TYPE_RKDB_S3)
    // AWS s3 file system
    auto status = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
        rocksdb::FileSystem::Default(), cfs_options, nullptr, cfs);
#elif defined(LOG_STATE_TYPE_RKDB_GCS)
    // Google cloud storage file system
    auto status = rocksdb::CloudFileSystemEnv::NewGcpFileSystem(
        rocksdb::FileSystem::Default(), cfs_options, nullptr, cfs);
#else
    auto status = rocksdb::Status::InvalidArgument(
        "No cloud backend selected: define LOG_STATE_TYPE_RKDB_S3 or "
        "LOG_STATE_TYPE_RKDB_GCS");
#endif
    return status;
};

}  // namespace txlog
#endif
