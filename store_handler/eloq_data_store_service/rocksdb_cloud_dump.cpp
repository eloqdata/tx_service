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

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rocksdb/cloud/cloud_storage_provider.h>
#include <rocksdb/cloud/db_cloud.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/options_util.h>

#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// Define command line flags
DEFINE_string(aws_access_key_id, "", "AWS access key ID");
DEFINE_string(aws_secret_key, "", "AWS secret access key");
DEFINE_string(bucket_name, "", "S3 bucket name");
DEFINE_string(bucket_prefix, "", "S3 bucket prefix");
DEFINE_string(object_path,
              "rocksdb_cloud",
              "S3 object path for RocksDB Cloud storage");
DEFINE_string(region, "us-east-1", "AWS region");
DEFINE_string(s3_endpoint, "", "Custom S3 endpoint URL (optional)");
DEFINE_string(db_path, "./db", "Local DB path");
DEFINE_bool(list_cf, false, "List all column families");
DEFINE_bool(opendb, false, "Open the DB only");
DEFINE_string(dump_keys, "", "Dump all keys from specified column family");
DEFINE_string(get_value,
              "",
              "Get value for a specific key in column family (format: CF,KEY)");
DEFINE_string(cookie_on_open, "", "Cookie on open");

// MSB and MSB_MASK for TTL decoding
constexpr uint64_t MSB = 1ULL << 63;  // Mask for Bit 63
constexpr uint64_t MSB_MASK = ~MSB;   // Mask to clear Bit 63

// Utility functions
void print_usage(const char *prog_name)
{
    std::string usage_str =
        "Usage: " + std::string(prog_name) +
        " --aws_access_key_id=KEY_ID --aws_secret_key=SECRET "
        "--bucket_name=BUCKET --bucket_prefix=PREFIX [options]\n"
        "Options:\n"
        "  --aws_access_key_id=KEY      AWS access key ID\n"
        "  --aws_secret_key=SECRET      AWS secret access key\n"
        "  --bucket_name=BUCKET         S3 bucket name\n"
        "  --bucket_prefix=PREFIX       S3 bucket prefix\n"
        "  --object_path=PATH           S3 object path for RocksDB Cloud "
        "  --region=REGION              AWS region (default: us-east-1)\n"
        "  --s3_endpoint=URL            Custom S3 endpoint URL (optional)\n"
        "  --db_path=PATH               Local DB path (default: ./db)\n"
        "  --list_cf                    List all column families\n"
        "  --opendb                     Open the DB only\n"
        "  --dump_keys=CF               Dump all keys from specified column "
        "family\n"
        "  --get_value=CF,KEY           Get value for a specific key in "
        "column family\n"
        "  --cookie_on_open=COOKIE      Cookie on open\n";

    LOG(INFO) << usage_str;
}

// Structure to hold command line parameters
struct CmdLineParams
{
    std::string aws_access_key_id;
    std::string aws_secret_key;
    std::string bucket_name;
    std::string bucket_prefix;
    std::string object_path;
    std::string region;
    std::string s3_endpoint_url;
    std::string db_path;
    bool list_cf;
    bool opendb;
    std::string dump_keys_cf;
    std::pair<std::string, std::string> get_value;
    std::string cookie_on_open;
};

// Function to parse command line arguments
CmdLineParams parse_arguments()
{
    CmdLineParams params;

    params.aws_access_key_id = FLAGS_aws_access_key_id;
    params.aws_secret_key = FLAGS_aws_secret_key;
    params.bucket_name = FLAGS_bucket_name;
    params.bucket_prefix = FLAGS_bucket_prefix;
    params.object_path = FLAGS_object_path;
    params.region = FLAGS_region;
    params.s3_endpoint_url = FLAGS_s3_endpoint;
    params.db_path = FLAGS_db_path;
    params.list_cf = FLAGS_list_cf;
    params.opendb = FLAGS_opendb;
    params.dump_keys_cf = FLAGS_dump_keys;
    params.cookie_on_open = FLAGS_cookie_on_open;

    // Parse the get_value parameter (format: CF,KEY)
    if (!FLAGS_get_value.empty())
    {
        size_t comma_pos = FLAGS_get_value.find(',');
        if (comma_pos == std::string::npos)
        {
            throw std::runtime_error(
                "Invalid format for --get_value. Expected CF,KEY format.");
        }
        params.get_value.first = FLAGS_get_value.substr(0, comma_pos);
        params.get_value.second = FLAGS_get_value.substr(comma_pos + 1);
    }

    // Validate required parameters
    if (params.aws_access_key_id.empty())
    {
        throw std::runtime_error("AWS access key ID is required");
    }
    if (params.aws_secret_key.empty())
    {
        throw std::runtime_error("AWS secret access key is required");
    }
    if (params.bucket_name.empty())
    {
        throw std::runtime_error("Bucket name is required");
    }
    if (params.cookie_on_open.empty())
    {
        throw std::runtime_error("Cookie on open is required");
    }
    if (!params.list_cf && !params.opendb && params.dump_keys_cf.empty() &&
        params.get_value.first.empty())
    {
        throw std::runtime_error(
            "No action specified. Use --list_cf, --opendb, --dump_keys, or --get_value");
    }

    return params;
}

// Function to build an S3 client factory
rocksdb::S3ClientFactory buildS3ClientFactory(const std::string &endpoint)
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

        std::string endpoint_url = endpoint;
        std::transform(endpoint_url.begin(),
                       endpoint_url.end(),
                       endpoint_url.begin(),
                       [](unsigned char c) { return std::tolower(c); });

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
            LOG(ERROR) << "Invalid S3 endpoint URL";
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

// Function to decode key from RocksDB format
bool decodeKey(const rocksdb::Slice &full_key,
               std::string &table_name,
               uint32_t &partition_id,
               std::string &key)
{
    const std::string key_separator = "/";
    std::string full_key_str = full_key.ToString();

    size_t first_sep = full_key_str.find(key_separator);
    if (first_sep == std::string::npos)
    {
        return false;
    }

    table_name = full_key_str.substr(0, first_sep);

    size_t second_sep = full_key_str.find(key_separator, first_sep + 1);
    if (second_sep == std::string::npos)
    {
        return false;
    }

    std::string partition_id_str =
        full_key_str.substr(first_sep + 1, second_sep - first_sep - 1);
    try
    {
        partition_id = std::stoul(partition_id_str);
    }
    catch (const std::exception &e)
    {
        return false;
    }

    key = full_key_str.substr(second_sep + 1);
    return true;
}

// Function to decode has TTL from timestamp
void decodeHasTTLFromTs(uint64_t &ts, bool &has_ttl)
{
    // Check if the MSB is set
    if (ts & MSB)
    {
        has_ttl = true;
        ts &= MSB_MASK;  // Clear the MSB
    }
    else
    {
        has_ttl = false;
    }
}

// Function to deserialize value to record
void deserializeValueToRecord(const char *data,
                              const size_t size,
                              std::string &record,
                              uint64_t &ts,
                              uint64_t &ttl)
{
    if (size < sizeof(uint64_t))
    {
        record = "";
        ts = 0;
        ttl = 0;
        return;
    }

    size_t offset = 0;
    ts = *reinterpret_cast<const uint64_t *>(data);
    offset += sizeof(uint64_t);
    bool has_ttl = false;
    decodeHasTTLFromTs(ts, has_ttl);

    if (has_ttl)
    {
        if (size >= sizeof(uint64_t) * 2)
        {
            ttl = *(reinterpret_cast<const uint64_t *>(data + offset));
            offset += sizeof(uint64_t);
        }
        else
        {
            ttl = 0;
        }
    }
    else
    {
        ttl = 0;
    }

    record.assign(data + offset, size - offset);
}

// Main function
int main(int argc, char **argv)
{
    // Initialize Google's logging library
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;  // Log to stderr by default

    // Set up gflags
    gflags::SetUsageMessage("RocksDB Cloud Dump Utility");
    gflags::SetVersionString("1.0.0");

    // Parse command line flags
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    try
    {
        // Parse command line arguments
        CmdLineParams params = parse_arguments();

        // Create directory for DB path if it doesn't exist
        std::filesystem::create_directories(params.db_path);

        // Initialize AWS SDK
        Aws::SDKOptions aws_options;
        Aws::InitAPI(aws_options);

        // Set up cloud filesystem options
        rocksdb::CloudFileSystemOptions cfs_options;

        // Set credentials
        if (!params.aws_access_key_id.empty() && !params.aws_secret_key.empty())
        {
            cfs_options.credentials.InitializeSimple(params.aws_access_key_id,
                                                     params.aws_secret_key);
        }
        else
        {
            cfs_options.credentials.type = rocksdb::AwsAccessType::kUndefined;
        }

        // Validate credentials
        rocksdb::Status status = cfs_options.credentials.HasValid();
        if (!status.ok())
        {
            LOG(ERROR) << "Invalid AWS credentials: " << status.ToString();
            throw std::runtime_error("Invalid AWS credentials: " +
                                     status.ToString());
        }

        // Set bucket info
        cfs_options.src_bucket.SetBucketName(params.bucket_name,
                                             params.bucket_prefix);
        cfs_options.src_bucket.SetRegion(params.region);
        cfs_options.src_bucket.SetObjectPath(params.object_path);
        cfs_options.dest_bucket.SetBucketName(params.bucket_name,
                                              params.bucket_prefix);
        cfs_options.dest_bucket.SetRegion(params.region);
        cfs_options.dest_bucket.SetObjectPath(params.object_path);
        cfs_options.cookie_on_open = params.cookie_on_open;
        // Temp fix for very slow open db issue
        // TODO: implement customized sst file manager
        cfs_options.constant_sst_file_size_in_sst_file_manager =
            64 * 1024 * 1024L;
        // Skip listing cloud files in GetChildren when DumpDBSummary to speed
        // up open db
        cfs_options.skip_cloud_files_in_getchildren = true;

        // Add sst_file_cache for accelerating random access on sst files
        cfs_options.sst_file_cache =
            rocksdb::NewLRUCache(10 * 1024 * 1024 * 1024L, 5);  // 10GB cache

        cfs_options.resync_on_open =
            true;  // Sync cloud manifest and manifest files

        // Set S3 endpoint URL if provided
        if (!params.s3_endpoint_url.empty())
        {
            cfs_options.s3_client_factory =
                buildS3ClientFactory(params.s3_endpoint_url);
        }

        // Create cloud file system
        rocksdb::CloudFileSystem *cfs = nullptr;
        status = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
            rocksdb::FileSystem::Default(), cfs_options, nullptr, &cfs);

        if (!status.ok())
        {
            LOG(ERROR) << "Unable to create cloud storage filesystem: "
                       << status.ToString();
            throw std::runtime_error(
                "Unable to create cloud storage filesystem: " +
                status.ToString());
        }

        std::shared_ptr<rocksdb::FileSystem> cloud_fs(cfs);
        std::unique_ptr<rocksdb::Env> cloud_env =
            rocksdb::NewCompositeEnv(cloud_fs);

        // Create DB options
        rocksdb::Options options;
        options.env = cloud_env.get();
        options.create_if_missing = true;
        options.info_log_level = rocksdb::InfoLogLevel::INFO_LEVEL;
        options.max_open_files = -1;              // Allow unlimited open files
        options.disable_auto_compactions = true;  // Disable auto compactions
        options.best_efforts_recovery = false;
        options.skip_checking_sst_file_sizes_on_db_open = true;
        options.skip_stats_update_on_db_open = true;

        // Open the DB
        rocksdb::DBCloud *db = nullptr;
        std::vector<std::string> column_families;

        // First, list column families in the DB
        rocksdb::DBCloud::ListColumnFamilies(
            options, params.db_path, &column_families);
        if (!status.ok())
        {
            LOG(ERROR) << "Cannot list column families: " << status.ToString();
            throw std::runtime_error("Cannot list column families: " +
                                     status.ToString());
        }

        // If list_cf option is specified, just list column families and exit
        if (params.list_cf)
        {
            LOG(INFO) << "Column families in the database:";
            for (const auto &cf : column_families)
            {
                LOG(INFO) << "  " << cf;
            }
            cloud_env = nullptr;
            cloud_fs = nullptr;
            Aws::ShutdownAPI(aws_options);
            google::ShutdownGoogleLogging();
            return 0;
        }

        // Prepare column family descriptors
        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
        for (const auto &cf_name : column_families)
        {
            cf_descs.push_back(rocksdb::ColumnFamilyDescriptor(
                cf_name, rocksdb::ColumnFamilyOptions()));
        }

        LOG(INFO) << "Opening database with " << cf_descs.size()
                  << " column families...";
        // Open the database with all column families
        std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
        status = rocksdb::DBCloud::Open(
            options, params.db_path, cf_descs, "", 0, &cf_handles, &db);

        if (!status.ok())
        {
            LOG(ERROR) << "Unable to open RocksDB Cloud database: "
                       << status.ToString();
            throw std::runtime_error("Unable to open RocksDB Cloud database: " +
                                     status.ToString());
        }

        rocksdb::CloudFileSystem *cloud_fs_ptr =
            dynamic_cast<rocksdb::CloudFileSystem *>(cloud_fs.get());
        auto &cfs_options_ref =
            cloud_fs_ptr->GetMutableCloudFileSystemOptions();
        // Restore skip_cloud_files_in_getchildren to false
        cfs_options_ref.skip_cloud_files_in_getchildren = false;

        if (params.opendb)
        {
            LOG(INFO) << "Database opened successfully.";
            // Clean up resources
            for (auto cf_handle : cf_handles)
            {
                db->DestroyColumnFamilyHandle(cf_handle);
            }

            db->Close();
            DLOG(INFO) << "DB closed";
            delete db;
            DLOG(INFO) << "DB deleted";
            cloud_env = nullptr;
            DLOG(INFO) << "cloud_env reset";
            cloud_fs = nullptr;
            DLOG(INFO) << "cloud_fs reset";
            LOG(INFO) << "Database closed successfully.";
            Aws::ShutdownAPI(aws_options);
            google::ShutdownGoogleLogging();
            return 0;
        }

        // Build a map from column family name to handle
        std::unordered_map<std::string, rocksdb::ColumnFamilyHandle *> cf_map;
        for (size_t i = 0; i < column_families.size(); i++)
        {
            cf_map[column_families[i]] = cf_handles[i];
        }

        // Perform requested operations
        if (!params.dump_keys_cf.empty())
        {
            // Dump all keys from specified column family
            auto cf_it = cf_map.find(params.dump_keys_cf);
            if (cf_it == cf_map.end())
            {
                LOG(ERROR) << "Column family not found: "
                           << params.dump_keys_cf;
                throw std::runtime_error("Column family not found: " +
                                         params.dump_keys_cf);
            }

            rocksdb::ReadOptions read_options;
            std::unique_ptr<rocksdb::Iterator> it(
                db->NewIterator(read_options, cf_it->second));

            LOG(INFO) << "Keys in column family '" << params.dump_keys_cf
                      << "':";
            for (it->SeekToFirst(); it->Valid(); it->Next())
            {
                std::string table_name;
                uint32_t partition_id;
                std::string key;
                bool can_decode =
                    decodeKey(it->key(), table_name, partition_id, key);

                // Just print the raw key and whether it can be decoded
                std::string raw_key =
                    "Raw key: " + it->key().ToString() +
                    ", Decodable: " + (can_decode ? "Yes" : "No");
                LOG(INFO) << raw_key;
            }

            if (!it->status().ok())
            {
                LOG(ERROR) << "Error iterating keys: "
                           << it->status().ToString();
            }
        }

        if (!params.get_value.first.empty() && !params.get_value.second.empty())
        {
            // Get value for specific key in column family
            auto cf_it = cf_map.find(params.get_value.first);
            if (cf_it == cf_map.end())
            {
                LOG(ERROR) << "Column family not found: "
                           << params.get_value.first;
                throw std::runtime_error("Column family not found: " +
                                         params.get_value.first);
            }

            std::string value;
            rocksdb::ReadOptions read_options;
            status = db->Get(
                read_options, cf_it->second, params.get_value.second, &value);

            if (status.ok())
            {
                std::string record;
                uint64_t timestamp;
                uint64_t ttl;

                deserializeValueToRecord(
                    value.data(), value.size(), record, timestamp, ttl);

                LOG(INFO) << "Value for key '" << params.get_value.second
                          << "' in column family '" << params.get_value.first
                          << "':";
                LOG(INFO) << "  Timestamp: " << timestamp;
                LOG(INFO) << "  TTL: "
                          << (ttl > 0 ? std::to_string(ttl) : "None");
                LOG(INFO) << "  Record: " << record;
            }
            else if (status.IsNotFound())
            {
                std::string not_found_msg = "Key '" + params.get_value.second +
                                            "' not found in column family '" +
                                            params.get_value.first + "'";
                LOG(WARNING) << not_found_msg;
            }
            else
            {
                LOG(ERROR) << "Error getting value: " << status.ToString();
            }
        }

        // Clean up resources
        for (auto cf_handle : cf_handles)
        {
            db->DestroyColumnFamilyHandle(cf_handle);
        }

        db->Close();
        delete db;
        cloud_env = nullptr;
        cloud_fs = nullptr;
        Aws::ShutdownAPI(aws_options);

        return 0;
    }
    catch (const std::exception &e)
    {
        LOG(ERROR) << "Error: " << e.what();
        print_usage(argv[0]);
        google::ShutdownGoogleLogging();
        return 1;
    }

    // Clean up
    gflags::ShutDownCommandLineFlags();
    google::ShutdownGoogleLogging();

    return 0;
}