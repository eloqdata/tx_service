#include "log_utils.h"

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)

#define CATCH_CONFIG_PREFIX_ALL
// enable fault injection for unit test
#define WITH_FAULT_INJECT 1

#if defined(LOG_STATE_TYPE_RKDB_S3)
#include <aws/core/Aws.h>
#endif

#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <sys/signal.h>
#include <unistd.h>

#include <catch2/catch_all.hpp>
#include <catch2/fakeit.hpp>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>

#include "fault_inject.h"
#include "log.pb.h"
#include "log_agent.h"
#include "log_server.h"
#include "rocksdb/convenience.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/sst_file_reader.h"
#include "test_utils.h"

namespace ROCKSDB_NAMESPACE
{
extern std::string MakeTableFileName(uint64_t number);
}

DEFINE_string(region, "", "Regin");
DEFINE_string(prefer_zone, "", "Prefer zone");
DEFINE_string(current_zone1, "", "Current zone 1");
DEFINE_string(current_zone2, "", "Current zone 2");
DEFINE_string(current_zone3, "", "Current zone 3");

#if defined(LOG_STATE_TYPE_RKDB_S3)
DEFINE_string(aws_access_key_id, "", "AWS_ACCESS_KEY_ID");
DEFINE_string(aws_secret_key, "", "AWS_SECRET_KEY");
#endif

void PrepareCloudZone()
{
#if defined(LOG_STATE_TYPE_RKDB_S3)
    if (FLAGS_prefer_zone.length() == 0)
    {
        FLAGS_prefer_zone = "ap-northeast-1a";
    }
    if (FLAGS_current_zone1.length() == 0)
    {
        FLAGS_current_zone1 = "ap-northeast-1a";
    }
    if (FLAGS_current_zone2.length() == 0)
    {
        FLAGS_current_zone2 = "ap-northeast-1c";
    }
    if (FLAGS_current_zone3.length() == 0)
    {
        FLAGS_current_zone3 = "ap-northeast-1d";
    }
#elif defined(LOG_STATE_TYPE_RKDB_GCS)
    if (FLAGS_prefer_zone.length() == 0)
    {
        FLAGS_prefer_zone = "us-central1-a";
    }
    if (FLAGS_current_zone1.length() == 0)
    {
        FLAGS_current_zone1 = "us-central1-a";
    }
    if (FLAGS_current_zone2.length() == 0)
    {
        FLAGS_current_zone2 = "us-central1-c";
    }
    if (FLAGS_current_zone3.length() == 0)
    {
        FLAGS_current_zone3 = "us-central1-b";
    }
#endif
}

// random generator
std::random_device rd;
std::default_random_engine generator(rd());
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);

#if defined(LOG_STATE_TYPE_RKDB_S3)
// Init AWS SDK
Aws::SDKOptions aws_options;

static void aws_init()
{
    LOG(INFO) << "aws_init()";
    aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
    aws_options.httpOptions.installSigPipeHandler = true;
    Aws::InitAPI(aws_options);
}

static void aws_deinit()
{
    LOG(INFO) << "aws_deinit()";
    Aws::ShutdownAPI(aws_options);
}
#endif

// A fake impl of replay service
class FakeReplayService : public brpc::StreamInputHandler,
                          public txlog::LogReplayService
{
public:
    FakeReplayService(
        txlog::LogAgent *log_agent,
        std::vector<std::unique_ptr<txlog::ReplayMessage>> *replay_messages)
        : log_agent_(log_agent), replay_messages_(replay_messages) {};
    ~FakeReplayService() = default;

    void Connect(::google::protobuf::RpcController *controller,
                 const ::txlog::LogReplayConnectRequest *request,
                 ::txlog::LogReplayConnectResponse *response,
                 ::google::protobuf::Closure *done) override
    {
        LOG(INFO) << "Replay service Connect RPC called";
        brpc::ClosureGuard done_guard(done);
        auto cntl = dynamic_cast<brpc::Controller *>(controller);
        brpc::StreamOptions stream_options;
        stream_options.handler = this;
        stream_options.idle_timeout_ms = 1000;
        if (brpc::StreamAccept(&stream_id_, *cntl, &stream_options) != 0)
        {
            cntl->SetFailed("Fail to accept stream");
            LOG(ERROR) << "Fail to accept stream";
            return;
        }
        DLOG(INFO) << "Replay service accept stream success";
        response->set_success(true);
    }

    int on_received_messages(brpc::StreamId streamId,
                             butil::IOBuf *const messages[],
                             size_t size) override
    {
        DLOG(INFO) << "Receive replay messages, size: " << size;
        for (size_t idx = 0; idx < size; idx++)
        {
            std::unique_ptr<txlog::ReplayMessage> msg =
                std::make_unique<txlog::ReplayMessage>();
            butil::IOBufAsZeroCopyInputStream wrapper(*messages[idx]);
            msg->ParseFromZeroCopyStream(&wrapper);
            DLOG(INFO) << "on_received_message: " << msg->DebugString();
            replay_messages_->push_back(std::move(msg));
        }
        return 0;
    }

    void on_idle_timeout(brpc::StreamId id) override
    {
    }

    void on_closed(brpc::StreamId id) override
    {
        LOG(INFO) << "Stream " << id << " is closed";
    }

    void UpdateLogGroupLeader(::google::protobuf::RpcController *controller,
                              const ::txlog::LogLeaderUpdateRequest *request,
                              ::txlog::LogLeaderUpdateResponse *response,
                              ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        uint32_t lg_id = request->lg_id();
        uint32_t node_id = request->node_id();
        response->set_error(false);
        log_agent_->UpdateLeaderCache(lg_id, node_id);
        LOG(INFO) << "Update log group:" << lg_id
                  << " leader to node_id:" << node_id;
    }
    void Shutdown()
    {
        brpc::StreamClose(stream_id_);
    }

private:
    txlog::LogAgent *log_agent_;
    brpc::StreamId stream_id_;
    std::vector<std::unique_ptr<txlog::ReplayMessage>> *replay_messages_;
};

class FakeReplayServer
{
public:
    FakeReplayServer(
        txlog::LogAgent *log_agent,
        std::vector<std::unique_ptr<txlog::ReplayMessage>> *replay_messages)
        : brpc_server_(), replay_service_(log_agent, replay_messages) {};
    ~FakeReplayServer() = default;

    void Start()
    {
        // add replay service into server
        if (brpc_server_.AddService(&replay_service_,
                                    brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(ERROR) << "Fail to add service";
            return;
        }
        // start server
        brpc::ServerOptions options;
        if (brpc_server_.Start(8888, &options) != 0)
        {
            LOG(ERROR) << "Fail to start ReplayServer";
            return;
        }
        LOG(INFO) << "Log replay server started";
    }

    void Stop()
    {
        replay_service_.Shutdown();
        brpc_server_.RemoveService(&replay_service_);
        brpc_server_.Stop(1);
        brpc_server_.Join();
        LOG(INFO) << "FakeReplayServer stopped";
    }

private:
    brpc::Server brpc_server_;
    FakeReplayService replay_service_;
};

void replay(txlog::LogAgent *log_agent, uint32_t cc_ng_id, int term)
{
    LOG(INFO) << "Replay request sending";
    auto start = now();
    std::atomic<bool> intrpt{};
    log_agent->ReplayLog(
        cc_ng_id, term, "127.0.0.1", 8888, -1, 0, intrpt, false);
    auto stop = now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    LOG(INFO) << "Replay request takes: " << duration.count()
              << " microseconds.";
}

// generate log request and return blob message containing log information
void generate_log_request(uint64_t ng_id, txlog::WriteLogRequest &request)
{
    // tx_number should not be larger than 1<<44, since (tx_no>>42)/3 is the
    // target lg_id, which must be 0 in our test
    uint64_t tx_number = distribution(generator);
    uint64_t timestamp = now().time_since_epoch().count();
    // set this tx's sending ng_id to 1
    tx_number |= (ng_id << 42);
    // add some random variation
    //    timestamp += distribution(generator);
    request.set_txn_number(tx_number);
    request.set_commit_timestamp(timestamp);
}

// launch in process log server
std::unique_ptr<txlog::LogServer> LaunchLogServer(
    const uint32_t node_id,
    const uint16_t port,
    const std::vector<std::string> &ip_list,
    const std::vector<uint16_t> &port_list,
    const std::string &storage_path,
    const txlog::RocksDBCloudConfig &rocksdb_cloud_config,
    const uint32_t snapshot_interval = 600)
{
    LOG(INFO) << "LaunchLogServer node_id: " << node_id << " ,port: " << port
              << " ,storage_path: " << storage_path;
    std::string rocksdb_storage_path;

    if (storage_path.substr(0, 8) == "local://")
    {
        rocksdb_storage_path = storage_path.substr(8) + "/rocksdb";
    }
    else
    {
        rocksdb_storage_path = storage_path + "/rocksdb";
    }

    std::unique_ptr<txlog::LogServer> log_server =
        std::make_unique<txlog::LogServer>(node_id,
                                           port,
                                           ip_list,
                                           port_list,
                                           storage_path,
                                           0,
                                           3,
#ifdef WITH_CLOUD_AZ_INFO
                                           "ap-northeast-1a",
                                           "ap-northeast-1a",
#endif
                                           rocksdb_storage_path,
                                           1,
                                           rocksdb_cloud_config,
                                           50 * 10000,
                                           16,
                                           8,
                                           64 * 1024 * 1024,
                                           snapshot_interval);

    int status = log_server->Start();
    if (status != 0)
    {
        std::cout << "Failed to start log server" << std::endl;
        log_server = nullptr;
        return nullptr;
    }
    return log_server;
}

// fork and launch log server
pid_t ForkLogServer(const uint32_t node_id,
                    const uint16_t port,
                    const std::vector<std::string> &ip_list,
                    const std::vector<uint16_t> &port_list,
                    const std::string &storage_path,
                    const txlog::RocksDBCloudConfig &rocksdb_cloud_config,
                    const uint32_t snapshot_interval = 600)
{
    pid_t log_server_pid = fork();
    if (log_server_pid == 0)
    {
        // this is child process
        std::unique_ptr<txlog::LogServer> log_server =
            LaunchLogServer(node_id,
                            port,
                            ip_list,
                            port_list,
                            storage_path,
                            rocksdb_cloud_config,
                            snapshot_interval);
        while (true)
        {
            LOG(INFO) << "Forked log_server node_id: " << node_id
                      << " port: " << port << " is running";
            sleep(5);
        }
    }
    else
    {
        LOG(INFO) << "Forked log_server node_id: " << node_id
                  << " port: " << port << " pid: " << log_server_pid;
        return log_server_pid;
    }
}

void PrepareLogStoragePaths(const std::vector<std::string> &log_storage_paths)
{
    for (auto &log_storage_path : log_storage_paths)
    {
        if (std::filesystem::exists(log_storage_path))
        {
            std::filesystem::remove_all(log_storage_path);
        }
        std::filesystem::create_directories(log_storage_path);
    }
}

void PrepareLogRequest(txlog::LogRequest &general_request,
                       uint64_t cc_ng_id,
                       int ng_term,
                       std::string &message)
{
    general_request.Clear();
    // prepare WriteLogRequest arguments
    txlog::WriteLogRequest *request =
        general_request.mutable_write_log_request();
    request->Clear();
    generate_log_request(cc_ng_id, *request);
    request->set_tx_term(ng_term);

    ::google::protobuf::Map<::google::protobuf::uint32,
                            ::google::protobuf::int64> *shard_terms =
        request->mutable_node_terms();
    shard_terms->clear();
    (*shard_terms)[cc_ng_id] = ng_term;
    // set log messages for each ng
    ::google::protobuf::Map<::google::protobuf::uint32, ::std::string>
        *txn_logs = request->mutable_log_content()
                        ->mutable_data_log()
                        ->mutable_node_txn_logs();

    txn_logs->clear();
    (*txn_logs)[cc_ng_id] = message;
    request->set_log_group_id(0);
}

void VerifyReplayMessages(
    std::vector<std::unique_ptr<txlog::ReplayMessage>> *replay_messages,
    const uint32_t verify_cc_ng_id,
    const int verify_ng_term,
    const uint64_t verify_commit_ts,
    const std::string &verify_message)
{
    for (size_t i = 0; i < replay_messages->size(); i++)
    {
        auto &replay_message = replay_messages->at(i);
        uint32_t replay_cc_ng_id = replay_message->cc_node_group_id();
        int64_t replay_cc_ng_term = replay_message->cc_node_group_term();
        CATCH_REQUIRE(replay_cc_ng_id == verify_cc_ng_id);
        CATCH_REQUIRE(replay_cc_ng_term == verify_ng_term);
        const std::string &log_records = replay_message->binary_log_records();
        if (log_records.size() > 0)
        {
            size_t offset = 0;
            while (offset < log_records.size())
            {
                uint64_t replay_commit_ts = *reinterpret_cast<const uint64_t *>(
                    log_records.data() + offset);
                CATCH_REQUIRE(replay_commit_ts == verify_commit_ts);
                offset += sizeof(uint64_t);
                uint32_t blob_length = *reinterpret_cast<const uint32_t *>(
                    log_records.data() + offset);
                offset += sizeof(uint32_t);
                std::string blob(log_records.data() + offset, blob_length);
                CATCH_REQUIRE(blob == verify_message);
                offset += blob_length;
            }
        }
    }
}

void populate_data(rocksdb::DBCloud *db,
                   const uint64_t total_records_count,
                   const int64_t timestamp_delta = 0)
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
        uint64_t timestamp =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();

        cnt++;
        txlog::Serialize(key, timestamp + timestamp_delta, cnt % 3, tx_number);
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

uint64_t count_rows(rocksdb::DBCloud *db)
{
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db->NewIterator(rocksdb::ReadOptions()));
    if (!it->status().ok())
    {
        LOG(ERROR) << "RocksDB iterator fail " << it->status().ToString();
    }
    // Iterate over the key-value pairs
    uint64_t cnt = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        cnt++;
    }

    if (!it->status().ok())
    {
        LOG(ERROR) << "RocksDB iterator fail " << it->status().ToString();
    }

    return cnt;
}

uint64_t count_rows_before_timestamp(rocksdb::DBCloud *db,
                                     uint64_t timestamp_bound)
{
    uint64_t total_counts = 0;

    for (int ng_id = 0; ng_id < 3; ng_id++)
    {
        std::array<char, 20> start_key{};
        std::array<char, 20> end_key{};
        txlog::Serialize(start_key, 0, 0, 0);
        txlog::Serialize(end_key, timestamp_bound, 0, 0);
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
        total_counts += cnt;
    }

    return total_counts;
}

uint64_t db_size(rocksdb::DBCloud *db)
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
    return total_size;
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

void DumpRocksDBLiveFiles(std::vector<std::string> &live_files)
{
    LOG(INFO) << "RocksDB live files: " << live_files.size();
    for (const auto &file : live_files)
    {
        LOG(INFO) << "file name: " << file;
    }
}
void PrepareBucket(const std::string &region,
                   const std::string &bucket_prefix,
                   const std::string &bucket_name)
{
    LOG(INFO) << "Prepare bucket " << bucket_prefix << bucket_name;
    DropBucket(region, bucket_prefix, bucket_name);
    CreateBucket(region, bucket_prefix, bucket_name);
}

struct CurrentTimeInterface
{
    virtual std::time_t current_time(std::time_t *arg) = 0;
};

void PrepareCloudConfig(txlog::RocksDBCloudConfig &rocksdb_cloud_config,
                        const std::string &bucket_prefix,
                        const std::string &bucket_name)
{
#if defined(LOG_STATE_TYPE_RKDB_S3)
    rocksdb_cloud_config.aws_access_key_id_ = FLAGS_aws_access_key_id;
    rocksdb_cloud_config.aws_secret_key_ = FLAGS_aws_secret_key;
#endif
    rocksdb_cloud_config.bucket_name_ = bucket_name;
    rocksdb_cloud_config.bucket_prefix_ = bucket_prefix;
    rocksdb_cloud_config.region_ = FLAGS_region;
    rocksdb_cloud_config.sst_file_cache_size_ = txlog::parse_size("20GB");
    rocksdb_cloud_config.db_ready_timeout_us_ = 10 * 1000 * 1000;
    rocksdb_cloud_config.db_file_deletion_delay_ = 3600;
}

void PrepareCFSOptions(rocksdb::CloudFileSystemOptions &cfs_options,
                       txlog::RocksDBCloudConfig &rocksdb_cloud_config)
{
#if defined(LOG_STATE_TYPE_RKDB_S3)
    if (rocksdb_cloud_config.aws_access_key_id_.length() == 0 ||
        rocksdb_cloud_config.aws_secret_key_.length() == 0)
    {
        cfs_options.credentials.type = rocksdb::AwsAccessType::kInstance;
    }
    else
    {
        cfs_options.credentials.InitializeSimple(
            rocksdb_cloud_config.aws_access_key_id_,
            rocksdb_cloud_config.aws_secret_key_);
    }

    if (!cfs_options.credentials.HasValid().ok())
    {
        LOG(ERROR) << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                      "is required.";
        std::abort();
    }
#endif
    cfs_options.src_bucket.SetBucketName(rocksdb_cloud_config.bucket_name_,
                                         rocksdb_cloud_config.bucket_prefix_);
    cfs_options.src_bucket.SetRegion(rocksdb_cloud_config.region_);
    cfs_options.src_bucket.SetObjectPath("rocksdb_cloud");
    cfs_options.dest_bucket.SetBucketName(rocksdb_cloud_config.bucket_name_,
                                          rocksdb_cloud_config.bucket_prefix_);
    cfs_options.dest_bucket.SetRegion(rocksdb_cloud_config.region_);
    cfs_options.dest_bucket.SetObjectPath("rocksdb_cloud");

    cfs_options.resync_on_open = true;
    // new CLOUDMANIFEST suffixed by cookie and epochID suffixed
    // MANIFEST files are generated, which won't overwrite the old ones
    // opened by previous leader
    cfs_options.cookie_on_open = "";
    cfs_options.new_cookie_on_open = "1";
    // delay cloud file deletion for 1 hour
    cfs_options.cloud_file_deletion_delay =
        std::chrono::seconds(rocksdb_cloud_config.db_file_deletion_delay_);
    cfs_options.cloud_request_callback = nullptr;
}

void PrepareCloudFileSystemSettings(
    txlog::RocksDBCloudConfig &rocksdb_cloud_config,
    rocksdb::CloudFileSystemOptions &cfs_options,
    const std::string &bucket_prefix,
    const std::string &bucket_name)
{
    PrepareCloudConfig(rocksdb_cloud_config, bucket_prefix, bucket_name);
    PrepareCFSOptions(cfs_options, rocksdb_cloud_config);
}

CATCH_TEST_CASE("test_rocksdb_cloud_sst_reader",
                "[log_server_rocksdb_cloud_tests]")
{
    LOG(INFO) << "TEST_CASE: test_rocksdb_cloud_sst_reader";
    std::string bucket_name =
        "rocksdb-cloud-tests" + std::to_string(time(NULL));
    std::string bucket_prefix = "log-server-test-";

    PrepareBucket(FLAGS_region, bucket_prefix, bucket_name);

    std::filesystem::path current_dir = std::filesystem::current_path();
    std::string storage_path_0 = current_dir / "log_server_rocksdb_tests/log0";

    // clear and create log storage paths
    PrepareLogStoragePaths({storage_path_0});

    txlog::RocksDBCloudConfig rocksdb_cloud_config;
    rocksdb::CloudFileSystemOptions cfs_options;
    PrepareCloudFileSystemSettings(
        rocksdb_cloud_config, cfs_options, bucket_prefix, bucket_name);

    rocksdb::CloudFileSystem *cfs;
    auto status = txlog::NewCloudFileSystem(cfs_options, &cfs);

    if (!status.ok())
    {
        LOG(ERROR) << "Unable to create cloud storage filesystem, cloud type: "
#if defined(LOG_STATE_TYPE_RKDB_S3)
                   << "aws"
#elif defined(LOG_STATE_TYPE_RKDB_GCS)
                   << "gcp"
#endif
                   << ", at path rocksdb_cloud with bucket "
                   << rocksdb_cloud_config.bucket_name_ << " prefix "
                   << rocksdb_cloud_config.bucket_prefix_
                   << ", with error: " << status.ToString();

        std::abort();
    }
    auto dbc = std::make_shared<txlog::DBCloudContainer>();
    dbc->Open(cfs, cfs_options, storage_path_0, 8, 8, 64 * 1024 * 1024);

    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    for (int i = 0; i < 1000 * 10000; i++)
    {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        dbc->GetDBPtr()->Put(write_option, key, value);
    }

    sleep(5);

    // Get the live files metadata
    size_t max_sst_file_number_before_flush =
        dbc->GetDBPtr()->GetNextFileNumber() - 1;
    LOG(INFO) << "max_sst_file_number_before_flush: "
              << max_sst_file_number_before_flush;
    std::vector<std::string> live_files_before_flush;
    size_t manifest_file_size_before_flush = 0;
    dbc->GetDBPtr()->GetLiveFiles(
        live_files_before_flush, &manifest_file_size_before_flush, false);
    DumpRocksDBLiveFiles(live_files_before_flush);

    sleep(5);

    // flush
    rocksdb::FlushOptions flush_opt;
    flush_opt.allow_write_stall = false;
    flush_opt.wait = true;

    dbc->GetDBPtr()->Flush(flush_opt);

    // Get the live files metadata
    size_t max_sst_file_number_after_flush =
        dbc->GetDBPtr()->GetNextFileNumber() - 1;
    LOG(INFO) << "max_sst_file_number_after_flush: "
              << max_sst_file_number_after_flush;
    std::vector<std::string> live_files_after_flush;
    size_t manifest_file_size_after_flush = 0;
    dbc->GetDBPtr()->GetLiveFiles(
        live_files_after_flush, &manifest_file_size_after_flush, false);
    DumpRocksDBLiveFiles(live_files_after_flush);

    CATCH_REQUIRE(live_files_after_flush.size() >
                  live_files_before_flush.size());
    CATCH_REQUIRE(manifest_file_size_after_flush >
                  manifest_file_size_before_flush);
    CATCH_REQUIRE(max_sst_file_number_after_flush >
                  max_sst_file_number_before_flush);

    std::sort(live_files_before_flush.begin(), live_files_before_flush.end());
    std::sort(live_files_after_flush.begin(), live_files_after_flush.end());
    std::vector<std::string> live_files_diff;
    std::set_difference(live_files_after_flush.begin(),
                        live_files_after_flush.end(),
                        live_files_before_flush.begin(),
                        live_files_before_flush.end(),
                        std::back_inserter(live_files_diff));
    DumpRocksDBLiveFiles(live_files_diff);
    CATCH_REQUIRE(live_files_diff.size() == live_files_after_flush.size() -
                                                live_files_before_flush.size());

    auto options = dbc->GetDBPtr()->GetOptions();
    auto fs = options.env->GetFileSystem();

    size_t log_cnt_from_live_files_diff = 0;
    for (const auto &file : live_files_diff)
    {
        LOG(INFO) << "intersect file: " << file;
        rocksdb::SstFileReader sst_reader(options);
        CATCH_REQUIRE(sst_reader.Open(file).ok());
        CATCH_REQUIRE(sst_reader.VerifyChecksum().ok());
        rocksdb::ReadOptions read_options;
        rocksdb::Iterator *it = sst_reader.NewIterator(read_options);
        CATCH_REQUIRE(it->status().ok());
        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            log_cnt_from_live_files_diff++;
        }
    }
    LOG(INFO) << "log_cnt_from_live_files_diff: "
              << log_cnt_from_live_files_diff;

    size_t log_cnt_from_file_number = 0;
    auto start = now();
    for (auto file_number = max_sst_file_number_before_flush + 1;
         file_number <= max_sst_file_number_after_flush;
         file_number++)
    {
        std::string file_name =
            ROCKSDB_NAMESPACE::MakeTableFileName(file_number);
        LOG(INFO) << "file_name: " << file_name;
        rocksdb::SstFileReader sst_reader(options);
        CATCH_REQUIRE(sst_reader.Open(file_name).ok());
        CATCH_REQUIRE(sst_reader.VerifyChecksum().ok());
        rocksdb::ReadOptions read_options;
        rocksdb::Iterator *it = sst_reader.NewIterator(read_options);
        CATCH_REQUIRE(it->status().ok());
        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            log_cnt_from_file_number++;
        }
    }
    auto stop = now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    LOG(INFO) << "log_cnt_from_file_number: " << log_cnt_from_file_number
              << " ,duration: " << duration.count() << " millseconds";

    CATCH_REQUIRE(log_cnt_from_live_files_diff == log_cnt_from_file_number);

    dbc->GetDBPtr()->Close();
    // drop s3 bucket
    DropBucket(FLAGS_region, bucket_prefix, bucket_name);
}

CATCH_TEST_CASE("test_rocksdb_cloud_compatiable",
                "[log_server_rocksdb_cloud_tests]")
{
    LOG(INFO) << "TEST_CASE: test_rocksdb_cloud_compatiable";
    std::string bucket_name =
        "rocksdb-cloud-tests" + std::to_string(time(NULL));
    std::string bucket_prefix = "log-server-test-";

    PrepareBucket(FLAGS_region, bucket_prefix, bucket_name);

    std::filesystem::path current_dir = std::filesystem::current_path();
    std::string storage_path_0 = current_dir / "log_server_rocksdb_tests/log0";

    // clear and create log storage paths
    PrepareLogStoragePaths({storage_path_0});

    std::string storage_path_1 = current_dir / "log_server_rocksdb_tests/log1";
    PrepareLogStoragePaths({storage_path_1});

    txlog::RocksDBCloudConfig rocksdb_cloud_config;
    rocksdb::CloudFileSystemOptions cfs_options;
    PrepareCloudFileSystemSettings(
        rocksdb_cloud_config, cfs_options, bucket_prefix, bucket_name);

    rocksdb::CloudFileSystem *cfs;
    auto status = txlog::NewCloudFileSystem(cfs_options, &cfs);

    if (!status.ok())
    {
        LOG(ERROR) << "Unable to create cloud storage filesystem, cloud type: "
#if defined(LOG_STATE_TYPE_RKDB_S3)
                   << "aws"
#elif defined(LOG_STATE_TYPE_RKDB_GCS)
                   << "gcp"
#endif
                   << ", at path rocksdb_cloud with bucket "
                   << rocksdb_cloud_config.bucket_name_ << " prefix "
                   << rocksdb_cloud_config.bucket_prefix_
                   << ", with error: " << status.ToString();

        std::abort();
    }
    auto dbc = std::make_shared<txlog::DBCloudContainer>();
    dbc->Open(cfs, cfs_options, storage_path_0, 8, 8, 64 * 1024 * 1024);

    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    dbc->GetDBPtr()->Put(write_option, "k1", "v1");
    dbc->GetDBPtr()->Put(write_option, "k11", "v11");
    // Create an iterator
    rocksdb::Iterator *it =
        dbc->GetDBPtr()->NewIterator(rocksdb::ReadOptions());

    // Iterate over the key-value pairs
    uint64_t cnt1 = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        cnt1++;
    }
    CATCH_REQUIRE(cnt1 == 2);

    // Check for errors or completion
    if (!it->status().ok())
    {
        std::cerr << "Error occurred during iteration: "
                  << it->status().ToString() << std::endl;
    }

    // Cleanup
    delete it;
    dbc->GetDBPtr()->Close();

    dbc = std::make_shared<txlog::DBCloudContainer>();
    // if the CLOUDMANIFEST with the new_cookie_open exists, the old one will be
    // overwritten
    cfs_options.cookie_on_open = "";
    cfs_options.new_cookie_on_open = "1";
    dbc->Open(cfs, cfs_options, storage_path_0, 8, 8, 64 * 1024 * 1024);
    dbc->GetDBPtr()->Put(write_option, "k2", "v2");
    it = dbc->GetDBPtr()->NewIterator(rocksdb::ReadOptions());
    // Iterate over the key-value pairs
    uint64_t cnt2 = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        cnt2++;
    }
    CATCH_REQUIRE(cnt2 == 1);

    // Cleanup
    delete it;
    dbc->GetDBPtr()->Close();

    dbc = std::make_shared<txlog::DBCloudContainer>();
    cfs_options.cookie_on_open = "1";
    cfs_options.new_cookie_on_open = "2";
    dbc->Open(cfs, cfs_options, storage_path_0, 8, 8, 64 * 1024 * 1024);
    dbc->GetDBPtr()->Put(write_option, "k3", "v3");
    it = dbc->GetDBPtr()->NewIterator(rocksdb::ReadOptions());
    // Iterate over the key-value pairs
    uint64_t cnt3 = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        cnt3++;
    }
    CATCH_REQUIRE(cnt3 == 2);

    // Cleanup
    delete it;
    dbc->GetDBPtr()->Close();
    // drop s3 bucket
    DropBucket(FLAGS_region, bucket_prefix, bucket_name);
}

CATCH_TEST_CASE("test_serialize_deserialize",
                "[log_server_rocksdb_cloud_tests]")
{
    LOG(INFO) << "TEST_CASE: test_serialize_deserialize";
    std::string bucket_name =
        "rocksdb-cloud-tests" + std::to_string(time(NULL));
    std::string bucket_prefix = "log-server-test-";

    PrepareBucket(FLAGS_region, bucket_prefix, bucket_name);

    std::vector<std::string> ip_list = {"127.0.0.1"};
    std::vector<uint16_t> port_list = {9000};

    // Get the current directory.
    std::filesystem::path current_dir = std::filesystem::current_path();
    std::string storage_path_0 = current_dir / "log_server_rocksdb_tests/log0";

    // clear and create log storage paths
    PrepareLogStoragePaths({storage_path_0});

    txlog::RocksDBCloudConfig rocksdb_cloud_config;
    rocksdb::CloudFileSystemOptions cfs_options;
    PrepareCloudFileSystemSettings(
        rocksdb_cloud_config, cfs_options, bucket_prefix, bucket_name);

    rocksdb::CloudFileSystem *cfs;
    auto status = txlog::NewCloudFileSystem(cfs_options, &cfs);

    if (!status.ok())
    {
        LOG(ERROR) << "Unable to create cloud storage filesystem, cloud type: "
#if defined(LOG_STATE_TYPE_RKDB_S3)
                   << "aws"
#elif defined(LOG_STATE_TYPE_RKDB_GCS)
                   << "gcp"
#endif
                   << ", at path rocksdb_cloud with bucket "
                   << rocksdb_cloud_config.bucket_name_ << " prefix "
                   << rocksdb_cloud_config.bucket_prefix_
                   << ", with error: " << status.ToString();

        std::abort();
    }
    auto dbc = std::make_shared<txlog::DBCloudContainer>();
    dbc->Open(cfs, cfs_options, storage_path_0, 8, 8, 64 * 1024 * 1024);

    rocksdb::WriteOptions write_option;
    write_option.disableWAL = true;
    std::array<char, 20> key{};
    uint32_t ng_id = 1;
    uint64_t timestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    uint64_t tx_number = 1509;
    txlog::Serialize(key, timestamp, ng_id, tx_number);
    std::string log_message = std::to_string(1);
    log_message.append(std::string(256, 'a'));
    status = dbc->GetDBPtr()->Put(
        write_option, rocksdb::Slice(key.data(), key.size()), log_message);
    CATCH_REQUIRE(status.ok());

    dbc->GetDBPtr()->Close();

    dbc = std::make_shared<txlog::DBCloudContainer>();
    cfs_options.cookie_on_open = "1";
    cfs_options.new_cookie_on_open = "2";
    dbc->Open(cfs, cfs_options, storage_path_0, 8, 8, 64 * 1024 * 1024);
    rocksdb::Iterator *it =
        dbc->GetDBPtr()->NewIterator(rocksdb::ReadOptions());
    it->SeekToFirst();
    rocksdb::Slice key_slice = it->key();
    CATCH_REQUIRE(it->status().ok());

    uint32_t ng_id_a = 0;
    uint64_t timestamp_a = 0;
    uint64_t tx_number_a = 0;
    txlog::Deserialize(key_slice, timestamp_a, ng_id_a, tx_number_a);
    CATCH_REQUIRE(ng_id_a == ng_id);
    CATCH_REQUIRE(timestamp_a == timestamp);
    CATCH_REQUIRE(tx_number_a == tx_number);

    delete it;
    dbc->GetDBPtr()->Close();
    // drop s3 bucket
    DropBucket(FLAGS_region, bucket_prefix, bucket_name);
}

// test replay can work with rocksdb cloud not started by new leader in a middle
// term
CATCH_TEST_CASE("test_A_B_A_replay_B_not_start_rocksdb_cloud",
                "[log_server_rocksdb_cloud_tests]")
{
    LOG(INFO) << "TEST_CASE: test_A_B_A_replay_B_not_start_rocksdb_cloud";
    std::string bucket_name =
        "rocksdb-cloud-tests" + std::to_string(time(NULL));
    std::string bucket_prefix = "log-server-test-";

    PrepareBucket(FLAGS_region, bucket_prefix, bucket_name);

    std::vector<std::string> ip_list = {"127.0.0.1", "127.0.0.1", "127.0.0.1"};
    std::vector<uint16_t> port_list = {9000, 9001, 9002};

    txlog::RocksDBCloudConfig rocksdb_cloud_config;
    PrepareCloudConfig(rocksdb_cloud_config, bucket_prefix, bucket_name);

    // Get the current directory.
    std::filesystem::path current_dir = std::filesystem::current_path();
    std::string storage_path_0 = current_dir / "log_server_rocksdb_tests/log0";
    std::string storage_path_1 = current_dir / "log_server_rocksdb_tests/log1";
    std::string storage_path_2 = current_dir / "log_server_rocksdb_tests/log2";

    // clear and create log storage paths
    PrepareLogStoragePaths({storage_path_0, storage_path_1, storage_path_2});

    pid_t log_server_0_pid = ForkLogServer(0,
                                           9000,
                                           ip_list,
                                           port_list,
                                           "local://" + storage_path_0,
                                           rocksdb_cloud_config,
                                           10);
    LOG(INFO) << "log_server_0 start";
    sleep(1);

    std::unique_ptr<txlog::LogServer> log_server_1 =
        LaunchLogServer(1,
                        9001,
                        ip_list,
                        port_list,
                        "local://" + storage_path_1,
                        rocksdb_cloud_config,
                        10);
    LOG(INFO) << "log_server_1 start";
    sleep(1);

    std::unique_ptr<txlog::LogServer> log_server_2 =
        LaunchLogServer(2,
                        9002,
                        ip_list,
                        port_list,
                        "local://" + storage_path_2,
                        rocksdb_cloud_config,
                        10);
    LOG(INFO) << "log_server_2 start";
    sleep(5);

    uint32_t cc_ng_id = 0;
    int ng_term = 1;

    // start log agent and log replay service
    std::unique_ptr<std::vector<std::unique_ptr<txlog::ReplayMessage>>>
        replay_messages = std::make_unique<
            std::vector<std::unique_ptr<txlog::ReplayMessage>>>();
    std::unique_ptr<txlog::LogAgent> log_agent =
        std::make_unique<txlog::LogAgent>();
    log_agent->Init(ip_list, port_list, 0, 3);

    FakeReplayServer replay_server(log_agent.get(), replay_messages.get());
    replay_server.Start();
    LOG(INFO) << "replay_server start";
    sleep(5);
    replay(log_agent.get(), cc_ng_id, ng_term);
    LOG(INFO) << "replay done";
    sleep(5);
    replay_messages->clear();

    // write data log
    txlog::LogResponse response;
    brpc::Controller cntl;

    txlog::LogRequest general_request;
    std::string message = std::to_string(1);
    message.append(std::string(256, 'a'));
    PrepareLogRequest(general_request, cc_ng_id, ng_term, message);
    uint64_t commit_ts = general_request.write_log_request().commit_timestamp();
    uint64_t tx_number = general_request.write_log_request().txn_number();
    log_agent->WriteLog(0, &cntl, &general_request, &response, nullptr);
    CATCH_REQUIRE(cntl.Failed() == false);
    LOG(INFO) << "Write log done";
    LOG(INFO) << "sleep 30 seconds, wait for 2 snapshot happen";
    sleep(30);

    // make sure new leader will not
    // start rocksdb cloud to simulate a absent CLOUDMANIFEST-{term}
    txlog::FaultInject::Instance().InjectFault(
        "new_leader_dont_start_rocksdb_cloud", "node_id=-1");

    // kil log server A
    uint32_t old_leader_id = log_agent->TEST_LogGroupLeaderNode(0);
    kill(log_server_0_pid, SIGKILL);
    LOG(INFO) << "log_server_0 killed";
    sleep(3);

    // rocksdb cloud is not quick enough to start
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        log_agent->RefreshLeader(0);
        uint32_t new_leader_id = log_agent->TEST_LogGroupLeaderNode(0);
        LOG(INFO) << "wait for leader change after kill node 0, old_leader_id: "
                  << old_leader_id << " ,new_leader_id: " << new_leader_id;
        if (new_leader_id != old_leader_id)
        {
            break;
        }
    }

    // remove fault injection
    txlog::FaultInject::Instance().InjectFault(
        "new_leader_dont_start_rocksdb_cloud", "remove");

    // restart A, seems this will fail
    // log_server_0_pid = ForkLogServer(0,
    // 9000,
    // ip_list,
    // port_list,
    //"local://" + storage_path_0,
    // rocksdb_cloud_config);

    std::unique_ptr<txlog::LogServer> log_server_0 =
        LaunchLogServer(0,
                        9000,
                        ip_list,
                        port_list,
                        "local://" + storage_path_0,
                        rocksdb_cloud_config);
    LOG(INFO) << "log_server_0 start";
    sleep(10);

    // wait for A become leader again
    // log_agent->TransferLeader(0, 0);
    // LOG(INFO) << "Transfer leader to node 0";
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        log_agent->RefreshLeader(0);
        uint32_t new_leader_id = log_agent->TEST_LogGroupLeaderNode(0);
        LOG(INFO) << "wait node 0 becomes leader again, new_leader_id: "
                  << new_leader_id;
        if (new_leader_id == 0)
        {
            break;
        }
    }

    // recover, the message should be replayed even if the rocksdb cloud is not
    // started for a middle term
    log_agent->RecoverTx(tx_number,
                         ng_term,
                         commit_ts - 1,
                         cc_ng_id,
                         ng_term,
                         "127.0.0.1",
                         8888,
                         0);
    sleep(5);
    LOG(INFO) << "Replay message size: " << replay_messages->size();
    CATCH_REQUIRE(replay_messages->size() == 1);
    // verify replay message
    VerifyReplayMessages(
        replay_messages.get(), cc_ng_id, ng_term, commit_ts, message);
    replay_messages->clear();

    // shutdown log servers
    log_agent = nullptr;
    replay_server.Stop();
    log_server_0 = nullptr;
    log_server_2 = nullptr;
    log_server_1 = nullptr;

    // drop s3 bucket
    DropBucket(FLAGS_region, bucket_prefix, bucket_name);
}

CATCH_TEST_CASE("test_snapshot_flushdb", "[log_server_rocksdb_cloud_tests]")
{
    LOG(INFO) << "TEST_CASE: test_snapshot_flushdb";
    std::string bucket_name =
        "rocksdb-cloud-tests" + std::to_string(time(NULL));
    std::string bucket_prefix = "log-server-test-";

    PrepareBucket(FLAGS_region, bucket_prefix, bucket_name);

    std::vector<std::string> ip_list = {"127.0.0.1"};
    std::vector<uint16_t> port_list = {9000};

    txlog::RocksDBCloudConfig rocksdb_cloud_config;
    PrepareCloudConfig(rocksdb_cloud_config, bucket_prefix, bucket_name);

    //
    // Get the current directory.
    std::filesystem::path current_dir = std::filesystem::current_path();
    std::string storage_path_0 = current_dir / "log_server_rocksdb_tests/log0";

    // clear and create log storage paths
    PrepareLogStoragePaths({storage_path_0});
    // make sure new leader sleep 10 seconds before start rocksdb cloud
    txlog::FaultInject::Instance().InjectFault(
        "new_leader_sleep_when_start_rocksdb_cloud", "node_id=-1");
    // start log server with very quick snapshot interval 1s
    std::unique_ptr<txlog::LogServer> log_server_0 =
        LaunchLogServer(0,
                        9000,
                        ip_list,
                        port_list,
                        "local://" + storage_path_0,
                        rocksdb_cloud_config,
                        1);

    sleep(2);

    uint32_t cc_ng_id = 0;
    int ng_term = 1;

    // start log agent and log replay service
    std::unique_ptr<std::vector<std::unique_ptr<txlog::ReplayMessage>>>
        replay_messages = std::make_unique<
            std::vector<std::unique_ptr<txlog::ReplayMessage>>>();
    std::unique_ptr<txlog::LogAgent> log_agent =
        std::make_unique<txlog::LogAgent>();
    log_agent->Init(ip_list, port_list, 0, 3);

    // replay and snapshot both will wait for RocksdDBCloud to start, expect
    // nothing bad happen
    FakeReplayServer replay_server(log_agent.get(), replay_messages.get());
    replay_server.Start();
    sleep(1);
    replay(log_agent.get(), cc_ng_id, ng_term);
    sleep(1);
    replay_messages->clear();
    // write data log
    txlog::LogResponse response;
    brpc::Controller cntl;
    txlog::LogRequest general_request;
    std::string message = std::to_string(1);
    message.append(std::string(256, 'a'));
    PrepareLogRequest(general_request, cc_ng_id, ng_term, message);
    uint64_t commit_ts = general_request.write_log_request().commit_timestamp();
    log_agent->WriteLog(0, &cntl, &general_request, &response, nullptr);
    CATCH_REQUIRE(cntl.Failed() == false);
    DLOG(INFO) << "Write log done";
    // wait for log server start, expect a snapshot happen before rocksdb
    // cloud rolling up
    // remove fault injection
    txlog::FaultInject::Instance().InjectFault(
        "new_leader_sleep_when_start_rocksdb_cloud", "remove");

    // wait for snapshot happen
    sleep(10);
    // shutdown log servers and clear log storage paths, make sure the replay
    // message is from rocksdb
    log_server_0 = nullptr;
    PrepareLogStoragePaths({storage_path_0});
    log_server_0 = LaunchLogServer(0,
                                   9000,
                                   ip_list,
                                   port_list,
                                   "local://" + storage_path_0,
                                   rocksdb_cloud_config);
    sleep(5);
    replay(log_agent.get(), cc_ng_id, ng_term);
    sleep(5);
    // verify replay message
    VerifyReplayMessages(
        replay_messages.get(), cc_ng_id, ng_term, commit_ts, message);
    replay_messages->clear();

    // shutdown log servers
    log_agent = nullptr;
    replay_server.Stop();
    log_server_0 = nullptr;

    // drop s3 bucket
    DropBucket(FLAGS_region, bucket_prefix, bucket_name);
}

CATCH_TEST_CASE("test_A_B_A_replay_A_refill_in_mem_state",
                "[log_server_rocksdb_cloud_tests]")
{
    LOG(INFO) << "TEST_CASE: test_A_B_A_replay_A_refill_in_mem_state";
    std::string bucket_name =
        "rocksdb-cloud-tests" + std::to_string(time(NULL));
    std::string bucket_prefix = "log-server-test-";

    PrepareBucket(FLAGS_region, bucket_prefix, bucket_name);

    std::vector<std::string> ip_list = {"127.0.0.1", "127.0.0.1", "127.0.0.1"};
    std::vector<uint16_t> port_list = {9000, 9001, 9002};

    txlog::RocksDBCloudConfig rocksdb_cloud_config;
    PrepareCloudConfig(rocksdb_cloud_config, bucket_prefix, bucket_name);

    // Get the current directory.
    std::filesystem::path current_dir = std::filesystem::current_path();
    std::string storage_path_0 = current_dir / "log_server_rocksdb_tests/log0";
    std::string storage_path_1 = current_dir / "log_server_rocksdb_tests/log1";
    std::string storage_path_2 = current_dir / "log_server_rocksdb_tests/log2";

    // clear and create log storage paths
    PrepareLogStoragePaths({storage_path_0, storage_path_1, storage_path_2});

    // start log server without snapshot during test
    std::unique_ptr<txlog::LogServer> log_server_0 =
        LaunchLogServer(0,
                        9000,
                        ip_list,
                        port_list,
                        "local://" + storage_path_0,
                        rocksdb_cloud_config,
                        24 * 60 * 60);
    sleep(5);

    std::unique_ptr<txlog::LogServer> log_server_1 =
        LaunchLogServer(1,
                        9001,
                        ip_list,
                        port_list,
                        "local://" + storage_path_1,
                        rocksdb_cloud_config,
                        24 * 60 * 60);

    std::unique_ptr<txlog::LogServer> log_server_2 =
        LaunchLogServer(2,
                        9002,
                        ip_list,
                        port_list,
                        "local://" + storage_path_2,
                        rocksdb_cloud_config,
                        24 * 60 * 60);

    sleep(5);

    uint32_t cc_ng_id = 0;
    int ng_term = 1;

    // start log agent and log replay service
    std::unique_ptr<std::vector<std::unique_ptr<txlog::ReplayMessage>>>
        replay_messages = std::make_unique<
            std::vector<std::unique_ptr<txlog::ReplayMessage>>>();
    std::unique_ptr<txlog::LogAgent> log_agent =
        std::make_unique<txlog::LogAgent>();
    log_agent->Init(ip_list, port_list, 0, 3);

    FakeReplayServer replay_server(log_agent.get(), replay_messages.get());
    replay_server.Start();
    sleep(5);
    replay(log_agent.get(), cc_ng_id, ng_term);
    sleep(5);
    replay_messages->clear();

    // stop prefer leader transfer
    txlog::FaultInject::Instance().InjectFault("disable_prefer_leader_transfer",
                                               "node_id=-1");

    // write data log
    txlog::LogResponse response;
    brpc::Controller cntl;

    txlog::LogRequest general_request;
    std::string message = std::to_string(1);
    message.append(std::string(256, 'a'));
    PrepareLogRequest(general_request, cc_ng_id, ng_term, message);
    uint64_t commit_ts = general_request.write_log_request().commit_timestamp();
    log_agent->WriteLog(0, &cntl, &general_request, &response, nullptr);

    CATCH_REQUIRE(cntl.Failed() == false);
    LOG(INFO) << "Write log done";

    // disable in memory state to db sync, prevent log server B save the data to
    // sst and read by log server A later
    txlog::FaultInject::Instance().InjectFault(
        "disable_in_mem_state_to_db_sync", "node_id=-1");

    // transfer leader to B
    LOG(INFO) << "Transfer leader from node A to node B, B will not sync in "
                 "mem state to db";
    log_agent->TransferLeader(0, 1);
    // wait leader changes
    sleep(5);

    log_agent->RefreshLeader(0);
    uint32_t old_leader_id = log_agent->TEST_LogGroupLeaderNode(0);

    // A can sync in mem state to db now
    txlog::FaultInject::Instance().InjectFault(
        "disable_in_mem_state_to_db_sync", "remove");
    // transfer leader back to A
    LOG(INFO) << "Transfer leader from node B to node A, A will refill in mem "
                 "state from db sst file";
    log_agent->TransferLeader(0, 0);

    // wait for A become leader
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        log_agent->RefreshLeader(0);
        uint32_t new_leader_id = log_agent->TEST_LogGroupLeaderNode(0);
        LOG(INFO) << "old_leader_id: " << old_leader_id
                  << " ,new_leader_id: " << new_leader_id;
        if (old_leader_id != new_leader_id)
        {
            break;
        }
    }
    // replay without wait
    replay(log_agent.get(), cc_ng_id, ng_term);
    sleep(5);
    LOG(INFO) << "Replay message size: " << replay_messages->size();
    // guaranteed to succeed only during single-threaded scanning
    CATCH_REQUIRE(replay_messages->size() == 3);
    // verify replay message
    VerifyReplayMessages(
        replay_messages.get(), cc_ng_id, ng_term, commit_ts, message);
    replay_messages->clear();

    txlog::FaultInject::Instance().InjectFault("disable_prefer_leader_transfer",
                                               "remove");

    // shutdown log servers
    log_agent = nullptr;
    replay_server.Stop();
    log_server_2 = nullptr;
    log_server_1 = nullptr;
    log_server_0 = nullptr;

    // drop s3 bucket
    DropBucket(FLAGS_region, bucket_prefix, bucket_name);
}

CATCH_TEST_CASE("test_A_B_A_replay", "[log_server_rocksdb_cloud_tests]")
{
    LOG(INFO) << "TEST_CASE: test_A_B_A_replay";
    std::string bucket_name =
        "rocksdb-cloud-tests" + std::to_string(time(NULL));
    std::string bucket_prefix = "log-server-test-";

    PrepareBucket(FLAGS_region, bucket_prefix, bucket_name);

    std::vector<std::string> ip_list = {"127.0.0.1", "127.0.0.1", "127.0.0.1"};
    std::vector<uint16_t> port_list = {9000, 9001, 9002};

    txlog::RocksDBCloudConfig rocksdb_cloud_config;
    PrepareCloudConfig(rocksdb_cloud_config, bucket_prefix, bucket_name);

    // Get the current directory.
    std::filesystem::path current_dir = std::filesystem::current_path();
    std::string storage_path_0 = current_dir / "log_server_rocksdb_tests/log0";
    std::string storage_path_1 = current_dir / "log_server_rocksdb_tests/log1";
    std::string storage_path_2 = current_dir / "log_server_rocksdb_tests/log2";

    // clear and create log storage paths
    PrepareLogStoragePaths({storage_path_0, storage_path_1, storage_path_2});

    pid_t log_server_0_pid = ForkLogServer(0,
                                           9000,
                                           ip_list,
                                           port_list,
                                           "local://" + storage_path_0,
                                           rocksdb_cloud_config);
    sleep(5);

    std::unique_ptr<txlog::LogServer> log_server_1 =
        LaunchLogServer(1,
                        9001,
                        ip_list,
                        port_list,
                        "local://" + storage_path_1,
                        rocksdb_cloud_config);

    std::unique_ptr<txlog::LogServer> log_server_2 =
        LaunchLogServer(2,
                        9002,
                        ip_list,
                        port_list,
                        "local://" + storage_path_2,
                        rocksdb_cloud_config);

    sleep(5);

    uint32_t cc_ng_id = 0;
    int ng_term = 1;

    // start log agent and log replay service
    std::unique_ptr<std::vector<std::unique_ptr<txlog::ReplayMessage>>>
        replay_messages = std::make_unique<
            std::vector<std::unique_ptr<txlog::ReplayMessage>>>();
    std::unique_ptr<txlog::LogAgent> log_agent =
        std::make_unique<txlog::LogAgent>();
    log_agent->Init(ip_list, port_list, 0, 3);

    FakeReplayServer replay_server(log_agent.get(), replay_messages.get());
    replay_server.Start();
    sleep(5);
    replay(log_agent.get(), cc_ng_id, ng_term);
    sleep(5);
    replay_messages->clear();

    // write data log
    txlog::LogResponse response;
    brpc::Controller cntl;

    txlog::LogRequest general_request;
    std::string message = std::to_string(1);
    message.append(std::string(256, 'a'));
    PrepareLogRequest(general_request, cc_ng_id, ng_term, message);
    uint64_t commit_ts = general_request.write_log_request().commit_timestamp();
    log_agent->WriteLog(0, &cntl, &general_request, &response, nullptr);

    CATCH_REQUIRE(cntl.Failed() == false);
    DLOG(INFO) << "Write log done";

    // kil log server A, and replay message after B is ready(rocksdb cloud
    // is started)
    kill(log_server_0_pid, SIGKILL);
    LOG(INFO) << "log_server_0 killed";
    // wait leader changes
    sleep(5);

    // restart log A
    log_agent->RefreshLeader(0);
    uint32_t old_leader_id = log_agent->TEST_LogGroupLeaderNode(0);
    std::unique_ptr<txlog::LogServer> log_server_0 =
        LaunchLogServer(0,
                        9000,
                        ip_list,
                        port_list,
                        "local://" + storage_path_0,
                        rocksdb_cloud_config);
    // wait for A become leader
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        log_agent->RefreshLeader(0);
        uint32_t new_leader_id = log_agent->TEST_LogGroupLeaderNode(0);
        LOG(INFO) << "old_leader_id: " << old_leader_id
                  << " ,new_leader_id: " << new_leader_id;
        if (old_leader_id != new_leader_id)
        {
            break;
        }
    }
    // replay without wait
    replay(log_agent.get(), cc_ng_id, ng_term);
    sleep(5);
    LOG(INFO) << "Replay message size: " << replay_messages->size();
    // guaranteed to succeed only during single-threaded scanning
    CATCH_REQUIRE(replay_messages->size() == 3);
    // verify replay message
    VerifyReplayMessages(
        replay_messages.get(), cc_ng_id, ng_term, commit_ts, message);
    replay_messages->clear();

    // shutdown log servers
    log_agent = nullptr;
    replay_server.Stop();
    log_server_2 = nullptr;
    log_server_1 = nullptr;
    log_server_0 = nullptr;

    // drop s3 bucket
    DropBucket(FLAGS_region, bucket_prefix, bucket_name);
}

CATCH_TEST_CASE("test_A_B_replay", "[log_server_rocksdb_cloud_tests]")
{
    LOG(INFO) << "TEST_CASE: test_A_B_replay";
    std::string bucket_name =
        "rocksdb-cloud-tests" + std::to_string(time(NULL));
    std::string bucket_prefix = "log-server-test-";

    PrepareBucket(FLAGS_region, bucket_prefix, bucket_name);

    std::vector<std::string> ip_list = {"127.0.0.1", "127.0.0.1", "127.0.0.1"};
    std::vector<uint16_t> port_list = {9000, 9001, 9002};

    txlog::RocksDBCloudConfig rocksdb_cloud_config;
    PrepareCloudConfig(rocksdb_cloud_config, bucket_prefix, bucket_name);

    // Get the current directory.
    std::filesystem::path current_dir = std::filesystem::current_path();
    std::string storage_path_0 = current_dir / "log_server_rocksdb_tests/log0";
    std::string storage_path_1 = current_dir / "log_server_rocksdb_tests/log1";
    std::string storage_path_2 = current_dir / "log_server_rocksdb_tests/log2";

    // clear and create log storage paths
    PrepareLogStoragePaths({storage_path_0, storage_path_1, storage_path_2});

    pid_t log_server_0_pid = ForkLogServer(0,
                                           9000,
                                           ip_list,
                                           port_list,
                                           "local://" + storage_path_0,
                                           rocksdb_cloud_config);
    sleep(5);

    std::unique_ptr<txlog::LogServer> log_server_1 =
        LaunchLogServer(1,
                        9001,
                        ip_list,
                        port_list,
                        "local://" + storage_path_1,
                        rocksdb_cloud_config);

    std::unique_ptr<txlog::LogServer> log_server_2 =
        LaunchLogServer(2,
                        9002,
                        ip_list,
                        port_list,
                        "local://" + storage_path_2,
                        rocksdb_cloud_config);

    sleep(5);

    uint32_t cc_ng_id = 0;
    int ng_term = 1;

    // start log agent and log replay service
    std::unique_ptr<std::vector<std::unique_ptr<txlog::ReplayMessage>>>
        replay_messages = std::make_unique<
            std::vector<std::unique_ptr<txlog::ReplayMessage>>>();
    std::unique_ptr<txlog::LogAgent> log_agent =
        std::make_unique<txlog::LogAgent>();
    log_agent->Init(ip_list, port_list, 0, 3);

    FakeReplayServer replay_server(log_agent.get(), replay_messages.get());
    replay_server.Start();
    sleep(5);
    replay(log_agent.get(), cc_ng_id, ng_term);
    sleep(5);
    replay_messages->clear();

    // write data log
    txlog::LogResponse response;
    brpc::Controller cntl;

    txlog::LogRequest general_request;
    std::string message = std::to_string(1);
    message.append(std::string(256, 'a'));
    PrepareLogRequest(general_request, cc_ng_id, ng_term, message);
    uint64_t commit_ts = general_request.write_log_request().commit_timestamp();
    log_agent->WriteLog(0, &cntl, &general_request, &response, nullptr);

    CATCH_REQUIRE(cntl.Failed() == false);
    DLOG(INFO) << "Write log done";

    // kil log server 0, and replay message
    kill(log_server_0_pid, SIGKILL);
    LOG(INFO) << "log_server_0 killed";
    replay(log_agent.get(), cc_ng_id, ng_term);
    sleep(5);
    LOG(INFO) << "Replay message size: " << replay_messages->size();

    // verify replay message
    VerifyReplayMessages(
        replay_messages.get(), cc_ng_id, ng_term, commit_ts, message);
    replay_messages->clear();

    // shutdown log servers
    log_agent = nullptr;
    replay_server.Stop();
    log_server_2 = nullptr;
    log_server_1 = nullptr;

    // drop s3 bucket
    DropBucket(FLAGS_region, bucket_prefix, bucket_name);
}

int main(int argc, char **argv)
{
#if defined(LOG_STATE_TYPE_RKDB_S3)
    aws_init();
#endif
    PrepareCloudZone();
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int ret = Catch::Session().run(argc, argv);

#if defined(LOG_STATE_TYPE_RKDB_S3)
    aws_deinit();
#endif
    return ret;
}
#else
#include <iostream>

int main(int argc, char **argv)
{
    std::cerr << "RocksDB Cloud is not enabled in this build." << std::endl;
    return 0;
}
#endif
