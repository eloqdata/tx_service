#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>

#include "log_server.h"
#include "log_utils.h"

#if BRPC_WITH_GLOG
#include "glog_error_logging.h"
#endif

#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#else
#ifndef GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = google;
#endif
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include "rocksdb_cloud_config.h"
#endif

#if defined(LOG_STATE_TYPE_RKDB_S3)
#include <aws/core/Aws.h>
#endif

DEFINE_string(config_path, "", "Configuration file path");

DEFINE_uint32(start_log_group_id, 0, "Start log group id");
DEFINE_string(conf, "", "Initial configuration of all group");
DEFINE_int32(node_id, 0, "node id in conf");
DEFINE_uint32(snapshot_interval, 600, "snapshot interval");
DEFINE_string(storage_path, "/tmp/log_service/raft_data", "raft storage path");
DEFINE_string(rocksdb_storage_path, "", "Storage path for rocksdb");
DEFINE_uint32(in_mem_data_log_queue_size_high_watermark,
              50 * 10000,
              "In memory data log queue max size");
DEFINE_uint32(log_group_replica_num, 3, "replica number in one log group");
DEFINE_uint32(rocksdb_max_write_buffer_number, 8, "Max write buffer number");
DEFINE_uint32(rocksdb_max_background_jobs, 12, "Max background jobs");
DEFINE_string(rocksdb_target_file_size_base,
              "64MB",
              "Target file size base for rocksdb");
DEFINE_string(rocksdb_sst_files_size_limit,
              "500MB",
              "The total RocksDB sst files size before purge");

DEFINE_bool(enable_request_checkpoint,
            false,
            "Enable sending checkpoint requests when the criteria are met.");

DEFINE_uint32(check_replay_log_size_interval_sec,
              10,
              "The interval for checking txlogs size used in tx recovery.");

DEFINE_string(notify_checkpointer_threshold_size,
              "1GB",
              "When the size of non-checkpoint txlogs reache this threshold, "
              "the log_service sends a checkpoint request to tx_service.");

DEFINE_bool(enable_brpc_builtin_services,
            true,
            "Enable showing brpc builtin services through http.");

DEFINE_uint32(rocksdb_scan_threads, 1, "The number of rocksdb scan threads");

#if defined(LOG_STATE_TYPE_RKDB_S3)
DEFINE_string(aws_access_key_id, "", "AWS_ACCESS_KEY_ID");
DEFINE_string(aws_secret_key, "", "AWS_SECRET_KEY");
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
DEFINE_string(oss_url,
              "",
              "OSS URL. Format: s3://{bucket}/{path}, gs://{bucket}/{path}, or "
              "http(s)://{host}:{port}/{bucket}/{path}. "
              "Takes precedence over legacy config if provided");
DEFINE_string(region, "", "Cloud service regin");
DEFINE_string(bucket_name,
              "",
              "Cloud storage bucket name (legacy, use oss_url instead)");
DEFINE_string(
    bucket_prefix,
    "",
    "Cloud storage bucket prefix (legacy, not supported with oss_url)");
DEFINE_string(object_path,
              "rocksdb_cloud",
              "Cloud storage object path, used to store the log files (legacy, "
              "use oss_url instead)");
DEFINE_string(sst_file_cache_size, "5GB", "Local sst cache size");
DEFINE_uint32(sst_file_cache_num_shard_bits,
              5,
              "Number of shards for sst file cache, default is 2^5 = 32");
DEFINE_uint32(
    rocksdb_cloud_ready_timeout,
    10,
    "Timeout before rocksdb cloud becoming ready on new log group leader");
DEFINE_uint32(rocksdb_cloud_file_deletion_delay,
              3600,
              "The file deletion delay for rocksdb cloud file");
DEFINE_uint32(log_retention_days,
              90,
              "The number of days for which logs should be retained");
DEFINE_string(log_purger_schedule,
              "00:00:01",
              "Time (in regular format: HH:MM:SS) to run log purger daily, "
              "deleting logs older than log_retention_days.");
DEFINE_string(archive_object_path,
              "",
              "Cloud storage object path for archive files. Defaults to "
              "object_path + '_archives' if not specified.");
DEFINE_uint32(archive_move_interval_seconds,
              600,
              "Interval in seconds between archive move operations. Default: "
              "600 (10 minutes).");
DEFINE_string(
    endpoint_url,
    "",
    "Cloud storage service endpoint url (legacy, use oss_url instead)");
#endif
#ifdef WITH_CLOUD_AZ_INFO
DEFINE_string(prefer_zone, "", "user preferred deployed availability zone");
DEFINE_string(current_zone,
              "",
              "the log service server node deployed on currently");
#endif

DEFINE_string(log_file_name_prefix,
              "log-service.log",
              "Sets the prefix for log files. Default is 'log-service.log'");

#if defined(LOG_STATE_TYPE_RKDB_S3)
Aws::SDKOptions aws_options;

static void aws_init()
{
    aws_options.httpOptions.installSigPipeHandler = true;
    aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    Aws::InitAPI(aws_options);
}

static void aws_deinit()
{
    Aws::ShutdownAPI(aws_options);
}
#endif

static bool CheckCommandLineFlagIsDefault(const char *name)
{
    gflags::CommandLineFlagInfo flag_info;

    bool flag_found = gflags::GetCommandLineFlagInfo(name, &flag_info);
    // Make sure the flag is declared.
    assert(flag_found);
    (void) flag_found;

    // Return `true` if the flag has the default value and has not been set
    // explicitly from the cmdline or via SetCommandLineOption
    return flag_info.is_default;
}

void launch(const std::string &tt_conf,
            uint32_t node_id,
            const std::string &storage_path
#if defined(LOG_STATE_TYPE_RKDB_ALL)
            ,
            const std::string &rocksdb_storage_path
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
            ,
            txlog::RocksDBCloudConfig rocksdb_cloud_config
#endif
#endif
#ifdef WITH_CLOUD_AZ_INFO
            ,
            const std::string &prefer_zone,
            const std::string &current_zone
#endif
)
{
    std::vector<std::string> ip_list;
    std::vector<uint16_t> port_list;
    std::vector<std::string> ip_port_list = txlog::split(tt_conf, ",");
    if (node_id >= ip_port_list.size())
    {
        LOG(ERROR) << "Invalid configuration: `node_id` must be less than node "
                      "size, node id = "
                   << node_id << ", node size = " << ip_port_list.size();
        if (!FLAGS_alsologtostderr)
        {
            std::cout
                << "Failed to start LogServer, invalid configuration of node_id"
                << std::endl;
        }
        return;
    }
    for (const auto &ip_port : ip_port_list)
    {
        auto p = ip_port.find(':');
        if (p == std::string::npos)
        {
            LOG(ERROR) << "Invalid configuration: expecting "
                          "ip:port,ip:port,ip:port... in conf";
            if (!FLAGS_alsologtostderr)
            {
                std::cout << "Failed to start LogServer, invalid configuration "
                             ": conf"
                          << std::endl;
            }
            return;
        }
        std::string ip = ip_port.substr(0, p), port = ip_port.substr(p + 1);
        ip_list.push_back(ip);
        port_list.emplace_back(std::stoi(port));
    }

    uint64_t notify_checkpointer_threshold_size =
        txlog::parse_size(FLAGS_notify_checkpointer_threshold_size);
#if defined(LOG_STATE_TYPE_RKDB_ALL)
    uint64_t rocksdb_target_file_size_base =
        txlog::parse_size(FLAGS_rocksdb_target_file_size_base);
#if !defined(LOG_STATE_TYPE_RKDB_CLOUD)
    size_t rocksdb_sst_files_size_limit =
        txlog::parse_size(FLAGS_rocksdb_sst_files_size_limit);
#endif
#endif
    txlog::LogServer server(node_id,
                            port_list[node_id],
                            ip_list,
                            port_list,
                            storage_path,
                            FLAGS_start_log_group_id,
                            FLAGS_log_group_replica_num,
#ifdef WITH_CLOUD_AZ_INFO
                            prefer_zone,
                            current_zone,
#endif

#if defined(LOG_STATE_TYPE_RKDB_ALL)
                            rocksdb_storage_path,
                            FLAGS_rocksdb_scan_threads,
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
                            rocksdb_cloud_config,
                            FLAGS_in_mem_data_log_queue_size_high_watermark,
#else
                            rocksdb_sst_files_size_limit,
#endif
                            FLAGS_rocksdb_max_write_buffer_number,
                            FLAGS_rocksdb_max_background_jobs,
                            rocksdb_target_file_size_base,
#endif
                            FLAGS_snapshot_interval,
                            FLAGS_enable_request_checkpoint,
                            FLAGS_check_replay_log_size_interval_sec,
                            notify_checkpointer_threshold_size);

    int start_status = server.Start(FLAGS_enable_brpc_builtin_services);
    if (start_status != 0)
    {
        LOG(ERROR) << "Failed to start log server, status: " << start_status;
        if (!FLAGS_alsologtostderr)
        {
            std::cout << "Failed to start LogServer, please check log file for "
                         "details."
                      << std::endl;
        }
        return;
    }
    if (!FLAGS_alsologtostderr)
    {
        std::cout << "LogServer Started, listenning on port "
                  << port_list[node_id] << "." << std::endl;
    }

    while (true)
    {
        sleep(100);
    }
    return;
}

void PrintHelloText()
{
    std::cout << "* Welcome to use LogServer." << std::endl;
    std::cout << "* Running logs will be written to the following path:"
              << std::endl;
    std::cout << FLAGS_log_dir << std::endl;
    std::cout << "* The above log path can be specified by arg --log_dir."
              << std::endl;
    std::cout << "* You can also run with [--help] for all available flags."
              << std::endl;
    std::cout << std::endl;
}

int main(int argc, char *argv[])
{
    // Increase max allowed rpc message size to 512mb.
    GFLAGS_NAMESPACE::SetCommandLineOption("max_body_size", "536870912");
    GFLAGS_NAMESPACE::SetCommandLineOption("graceful_quit_on_sigterm", "true");
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_GLOG
    InitGoogleLogging(argv);
#endif

    std::string config_file = FLAGS_config_path;
    INIReader config_reader(config_file);
    if (!config_file.empty() && config_reader.ParseError() != 0)
    {
        LOG(ERROR) << "Failed to parse config file, The first error line is "
                   << config_reader.ParseError();

        if (!FLAGS_alsologtostderr)
        {
            std::cout << "Failed to parse config file, The first error line is "
                      << config_reader.ParseError();
        }
        return -1;
    }

    FLAGS_log_file_name_prefix =
        !CheckCommandLineFlagIsDefault("log_file_name_prefix")
            ? FLAGS_log_file_name_prefix
            : config_reader.GetString(
                  "local", "log_file_name_prefix", FLAGS_log_file_name_prefix);

    FLAGS_start_log_group_id =
        !CheckCommandLineFlagIsDefault("start_log_group_id")
            ? FLAGS_start_log_group_id
            : config_reader.GetInteger(
                  "local", "start_log_group_id", FLAGS_start_log_group_id);
    FLAGS_conf = !CheckCommandLineFlagIsDefault("conf")
                     ? FLAGS_conf
                     : config_reader.GetString("local", "conf", FLAGS_conf);
    FLAGS_node_id =
        !CheckCommandLineFlagIsDefault("node_id")
            ? FLAGS_node_id
            : config_reader.GetInteger("local", "node_id", FLAGS_node_id);
    FLAGS_snapshot_interval =
        !CheckCommandLineFlagIsDefault("snapshot_interval")
            ? FLAGS_snapshot_interval
            : config_reader.GetInteger(
                  "local", "snapshot_interval", FLAGS_snapshot_interval);
    FLAGS_storage_path = !CheckCommandLineFlagIsDefault("storage_path")
                             ? FLAGS_storage_path
                             : config_reader.GetString(
                                   "local", "storage_path", FLAGS_storage_path);
#if defined(LOG_STATE_TYPE_RKDB_ALL)
    FLAGS_rocksdb_storage_path =
        !CheckCommandLineFlagIsDefault("rocksdb_storage_path")
            ? FLAGS_rocksdb_storage_path
            : config_reader.GetString(
                  "local", "rocksdb_storage_path", FLAGS_rocksdb_storage_path);
    // if rocksdb_storage_path is not set, use storage_path as default
    if (FLAGS_rocksdb_storage_path.empty())
    {
        FLAGS_rocksdb_storage_path = FLAGS_storage_path + "/rocksdb";
    }
#endif

    FLAGS_in_mem_data_log_queue_size_high_watermark =
        !CheckCommandLineFlagIsDefault(
            "in_mem_data_log_queue_size_high_watermark")
            ? FLAGS_in_mem_data_log_queue_size_high_watermark
            : config_reader.GetInteger(
                  "rocksdb",
                  "in_mem_data_log_queue_size_high_watermark",
                  FLAGS_in_mem_data_log_queue_size_high_watermark);
    FLAGS_log_group_replica_num =
        !CheckCommandLineFlagIsDefault("log_group_replica_num")
            ? FLAGS_log_group_replica_num
            : config_reader.GetInteger("local",
                                       "log_group_replica_num",
                                       FLAGS_log_group_replica_num);
    FLAGS_rocksdb_max_write_buffer_number =
        !CheckCommandLineFlagIsDefault("rocksdb_max_write_buffer_number")
            ? FLAGS_rocksdb_max_write_buffer_number
            : config_reader.GetInteger("rocksdb",
                                       "rocksdb_max_write_buffer_number",
                                       FLAGS_rocksdb_max_write_buffer_number);
    FLAGS_rocksdb_max_background_jobs =
        !CheckCommandLineFlagIsDefault("rocksdb_max_background_jobs")
            ? FLAGS_rocksdb_max_background_jobs
            : config_reader.GetInteger("rocksdb",
                                       "rocksdb_max_background_jobs",
                                       FLAGS_rocksdb_max_background_jobs);
    FLAGS_rocksdb_target_file_size_base =
        !CheckCommandLineFlagIsDefault("rocksdb_target_file_size_base")
            ? FLAGS_rocksdb_target_file_size_base
            : config_reader.GetString("rocksdb",
                                      "rocksdb_target_file_size_base",
                                      FLAGS_rocksdb_target_file_size_base);
    FLAGS_rocksdb_sst_files_size_limit =
        !CheckCommandLineFlagIsDefault("rocksdb_sst_files_size_limit")
            ? FLAGS_rocksdb_sst_files_size_limit
            : config_reader.GetString("rocksdb",
                                      "rocksdb_sst_files_size_limit",
                                      FLAGS_rocksdb_sst_files_size_limit);
    FLAGS_enable_request_checkpoint =
        !CheckCommandLineFlagIsDefault("enable_request_checkpoint")
            ? FLAGS_enable_request_checkpoint
            : config_reader.GetBoolean("local",
                                       "enable_request_checkpoint",
                                       FLAGS_enable_request_checkpoint);
    FLAGS_check_replay_log_size_interval_sec =
        !CheckCommandLineFlagIsDefault("check_replay_log_size_interval_sec")
            ? FLAGS_check_replay_log_size_interval_sec
            : config_reader.GetInteger(
                  "local",
                  "check_replay_log_size_interval_sec",
                  FLAGS_check_replay_log_size_interval_sec);
    FLAGS_notify_checkpointer_threshold_size =
        !CheckCommandLineFlagIsDefault("notify_checkpointer_threshold_size")
            ? FLAGS_notify_checkpointer_threshold_size
            : config_reader.GetString("local",
                                      "notify_checkpointer_threshold_size",
                                      FLAGS_notify_checkpointer_threshold_size);
    FLAGS_enable_brpc_builtin_services =
        !CheckCommandLineFlagIsDefault("enable_brpc_builtin_services")
            ? FLAGS_enable_brpc_builtin_services
            : config_reader.GetBoolean("local",
                                       "enable_brpc_builtin_services",
                                       FLAGS_enable_brpc_builtin_services);
    FLAGS_rocksdb_scan_threads =
        !CheckCommandLineFlagIsDefault("rocksdb_scan_threads")
            ? FLAGS_rocksdb_scan_threads
            : config_reader.GetInteger("rocksdb",
                                       "rocksdb_scan_threads",
                                       FLAGS_rocksdb_scan_threads);

#if defined(LOG_STATE_TYPE_RKDB_S3)
    FLAGS_aws_access_key_id =
        !CheckCommandLineFlagIsDefault("aws_access_key_id")
            ? FLAGS_aws_access_key_id
            : config_reader.GetString("rocksdb_cloud",
                                      "aws_access_key_id",
                                      FLAGS_aws_access_key_id);
    FLAGS_aws_secret_key =
        !CheckCommandLineFlagIsDefault("aws_secret_key")
            ? FLAGS_aws_secret_key
            : config_reader.GetString(
                  "rocksdb_cloud", "aws_secret_key", FLAGS_aws_secret_key);
#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    FLAGS_oss_url = !CheckCommandLineFlagIsDefault("oss_url")
                        ? FLAGS_oss_url
                        : config_reader.GetString(
                              "rocksdb_cloud", "oss_url", FLAGS_oss_url);
    FLAGS_region =
        !CheckCommandLineFlagIsDefault("region")
            ? FLAGS_region
            : config_reader.GetString("rocksdb_cloud", "region", FLAGS_region);
    FLAGS_bucket_name =
        !CheckCommandLineFlagIsDefault("bucket_name")
            ? FLAGS_bucket_name
            : config_reader.GetString(
                  "rocksdb_cloud", "bucket_name", FLAGS_bucket_name);
    FLAGS_bucket_prefix =
        !CheckCommandLineFlagIsDefault("bucket_prefix")
            ? FLAGS_bucket_prefix
            : config_reader.GetString(
                  "rocksdb_cloud", "bucket_prefix", FLAGS_bucket_prefix);
    FLAGS_object_path =
        !CheckCommandLineFlagIsDefault("object_path")
            ? FLAGS_object_path
            : config_reader.GetString(
                  "rocksdb_cloud", "object_path", FLAGS_object_path);
    FLAGS_sst_file_cache_size =
        !CheckCommandLineFlagIsDefault("sst_file_cache_size")
            ? FLAGS_sst_file_cache_size
            : config_reader.GetString("rocksdb_cloud",
                                      "sst_file_cache_size",
                                      FLAGS_sst_file_cache_size);
    FLAGS_sst_file_cache_num_shard_bits =
        !CheckCommandLineFlagIsDefault("sst_file_cache_num_shard_bits")
            ? FLAGS_sst_file_cache_num_shard_bits
            : config_reader.GetInteger("rocksdb_cloud",
                                       "sst_file_cache_num_shard_bits",
                                       FLAGS_sst_file_cache_num_shard_bits);
    FLAGS_endpoint_url =
        !CheckCommandLineFlagIsDefault("endpoint_url")
            ? FLAGS_endpoint_url
            : config_reader.GetString(
                  "rocksdb_cloud", "endpoint_url", FLAGS_endpoint_url);
    FLAGS_rocksdb_cloud_ready_timeout =
        !CheckCommandLineFlagIsDefault("rocksdb_cloud_ready_timeout")
            ? FLAGS_rocksdb_cloud_ready_timeout
            : config_reader.GetInteger("rocksdb_cloud",
                                       "rocksdb_cloud_ready_timeout",
                                       FLAGS_rocksdb_cloud_ready_timeout);
    FLAGS_rocksdb_cloud_file_deletion_delay =
        !CheckCommandLineFlagIsDefault("rocksdb_cloud_file_deletion_delay")
            ? FLAGS_rocksdb_cloud_file_deletion_delay
            : config_reader.GetInteger("rocksdb_cloud",
                                       "rocksdb_cloud_file_deletion_delay",
                                       FLAGS_rocksdb_cloud_file_deletion_delay);
    FLAGS_log_retention_days =
        !CheckCommandLineFlagIsDefault("log_retention_days")
            ? FLAGS_log_retention_days
            : config_reader.GetInteger("rocksdb_cloud",
                                       "log_retention_days",
                                       FLAGS_log_retention_days);
    FLAGS_log_purger_schedule =
        !CheckCommandLineFlagIsDefault("log_purger_schedule")
            ? FLAGS_log_purger_schedule
            : config_reader.GetString("rocksdb_cloud",
                                      "log_purger_schedule",
                                      FLAGS_log_purger_schedule);
    FLAGS_archive_object_path =
        !CheckCommandLineFlagIsDefault("archive_object_path")
            ? FLAGS_archive_object_path
            : config_reader.GetString("rocksdb_cloud",
                                      "archive_object_path",
                                      FLAGS_archive_object_path);
    FLAGS_archive_move_interval_seconds =
        !CheckCommandLineFlagIsDefault("archive_move_interval_seconds")
            ? FLAGS_archive_move_interval_seconds
            : config_reader.GetInteger("rocksdb_cloud",
                                       "archive_move_interval_seconds",
                                       FLAGS_archive_move_interval_seconds);
#endif

#ifdef WITH_CLOUD_AZ_INFO
    FLAGS_prefer_zone =
        !CheckCommandLineFlagIsDefault("prefer_zone")
            ? FLAGS_prefer_zone
            : config_reader.GetString(
                  "rocksdb_cloud", "prefer_zone", FLAGS_prefer_zone);
    FLAGS_current_zone =
        !CheckCommandLineFlagIsDefault("current_zone")
            ? FLAGS_current_zone
            : config_reader.GetString(
                  "rocksdb_cloud", "current_zone", FLAGS_current_zone);
#endif

    if (!FLAGS_alsologtostderr)
    {
        PrintHelloText();
        std::cout << "Starting log server with follow configs ..."
                  << "\n conf: " << FLAGS_conf << "; "
                  << "start log group id: " << FLAGS_start_log_group_id << "; "
                  << "node_id: " << FLAGS_node_id << "; "
                  << "raft storage_path: " << FLAGS_storage_path << "; "
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
                  << "log retention days: " << FLAGS_log_retention_days << "; "
#endif
                  << std::endl;
    }
    LOG(INFO) << "log server starting... conf: " << FLAGS_conf << "; "
              << "start log group id: " << FLAGS_start_log_group_id << "; "
              << "node_id: " << FLAGS_node_id << "; "
              << "raft storage_path: " << FLAGS_storage_path << "; "
#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
              << "log retention days: " << FLAGS_log_retention_days << "; "
#endif
              << std::endl;

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
    txlog::RocksDBCloudConfig rocksdb_cloud_config;

#if defined(LOG_STATE_TYPE_RKDB_S3)
    aws_init();
    rocksdb_cloud_config.aws_access_key_id_ = FLAGS_aws_access_key_id;
    rocksdb_cloud_config.aws_secret_key_ = FLAGS_aws_secret_key;
#endif

    rocksdb_cloud_config.oss_url_ = FLAGS_oss_url;
    rocksdb_cloud_config.region_ = FLAGS_region;
    rocksdb_cloud_config.bucket_name_ = FLAGS_bucket_name;
    rocksdb_cloud_config.bucket_prefix_ = FLAGS_bucket_prefix;
    rocksdb_cloud_config.object_path_ = FLAGS_object_path;
    rocksdb_cloud_config.endpoint_url_ = FLAGS_endpoint_url;
    rocksdb_cloud_config.sst_file_cache_size_ =
        txlog::parse_size(FLAGS_sst_file_cache_size);
    rocksdb_cloud_config.sst_file_cache_num_shard_bits_ =
        FLAGS_sst_file_cache_num_shard_bits;
    rocksdb_cloud_config.db_ready_timeout_us_ =
        FLAGS_rocksdb_cloud_ready_timeout * 1000 * 1000;
    rocksdb_cloud_config.db_file_deletion_delay_ =
        FLAGS_rocksdb_cloud_file_deletion_delay;
    rocksdb_cloud_config.log_retention_days_ = FLAGS_log_retention_days;

    // Archive configuration
    rocksdb_cloud_config.archive_object_path_ =
        FLAGS_archive_object_path.empty() ? FLAGS_object_path + "_archives"
                                          : FLAGS_archive_object_path;
    rocksdb_cloud_config.archive_move_interval_seconds_ =
        FLAGS_archive_move_interval_seconds;

    std::tm log_purger_tm{};
    std::istringstream iss(FLAGS_log_purger_schedule);
    iss >> std::get_time(&log_purger_tm, "%H:%M:%S");

    if (iss.fail())
    {
        LOG(ERROR)
            << "The argument `log_purger_schedule` has invalid time format. "
               "expected: HH:MM:SS";
        return 1;
    }
    else
    {
        rocksdb_cloud_config.log_purger_starting_hour_ = log_purger_tm.tm_hour;
        rocksdb_cloud_config.log_purger_starting_minute_ = log_purger_tm.tm_min;
        rocksdb_cloud_config.log_purger_starting_second_ = log_purger_tm.tm_sec;
        launch(FLAGS_conf,
               FLAGS_node_id,
               "local://" + FLAGS_storage_path,
               FLAGS_rocksdb_storage_path,
               rocksdb_cloud_config
#ifdef WITH_CLOUD_AZ_INFO
               ,
               FLAGS_prefer_zone,
               FLAGS_current_zone
#endif
        );
    }

#else
    launch(FLAGS_conf,
           FLAGS_node_id,
           "local://" + FLAGS_storage_path
#if defined(LOG_STATE_TYPE_RKDB)
           ,
           FLAGS_rocksdb_storage_path
#endif
#ifdef WITH_CLOUD_AZ_INFO
           ,
           FLAGS_prefer_zone,
           FLAGS_current_zone
#endif
    );
#endif

#if defined(LOG_STATE_TYPE_RKDB_S3)
    aws_deinit();
#endif

    if (!FLAGS_alsologtostderr)
    {
        std::cout << "LogServer Stopped." << std::endl;
    }

    return 0;
}
