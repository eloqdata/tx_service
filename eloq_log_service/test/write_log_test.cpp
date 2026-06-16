#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <gflags/gflags.h>
#include <signal.h>

#include <atomic>
#include <chrono>
#include <climits>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "log.pb.h"
#include "log_agent.h"

#if BRPC_WITH_GLOG
#include "glog_error_logging.h"
#endif

DEFINE_string(lg_raft_conf,
              "127.0.0.1:8002:0",
              "Configuration of the target log replication group");
DEFINE_int32(thread_num, 50, "Number of threads sending requests");
DEFINE_int32(log_entry_size, 100, "Byte size of each log entry");
DEFINE_string(replay_service_ip, "127.0.0.1", "Replay service ip");
DEFINE_int32(replay_service_port, 8888, "Replay service port");
DEFINE_int32(test_duration, 20, "Test duration");
DEFINE_uint32(start_log_group_id, 0, "Start log group id");

// A fake impl of replay service
class FakeReplayService : public brpc::StreamInputHandler,
                          public txlog::LogReplayService
{
public:
    FakeReplayService() = default;
    ~FakeReplayService() = default;

    void Connect(::google::protobuf::RpcController *controller,
                 const ::txlog::LogReplayConnectRequest *request,
                 ::txlog::LogReplayConnectResponse *response,
                 ::google::protobuf::Closure *done) override
    {
        LOG(INFO) << "Replay service Connect RPC called";
        brpc::ClosureGuard done_guard(done);
        brpc::StreamId streamId;
        auto cntl = dynamic_cast<brpc::Controller *>(controller);
        brpc::StreamOptions stream_options;
        stream_options.handler = this;
        stream_options.idle_timeout_ms = 1000;
        if (brpc::StreamAccept(&streamId, *cntl, &stream_options) != 0)
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
        return 0;
    }

    void on_idle_timeout(brpc::StreamId id) override
    {
    }

    void on_closed(brpc::StreamId id) override
    {
        LOG(INFO) << "Stream " << id << " is closed";
    }
};

txlog::LogAgent log_agent;
std::random_device rd;
std::default_random_engine generator(rd());
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);

auto now()
{
    return std::chrono::high_resolution_clock::now();
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

/**
 * keep sending add log requests to lg0 in batch until interrupted
 */
void sender(uint32_t cc_ng_id,
            int ng_term,
            std::atomic<int64_t> &tt_cnt,
            std::atomic<int64_t> &tt_tm_us,
            std::atomic<bool> &interrupt)
{
    txlog::LogAgent &agent = log_agent;

    brpc::Controller cntl;
    txlog::LogRequest general_request;
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

    // keep sending WriteLogRequest
    int64_t cnt = 0;
    int64_t time_us = 0;

    while (!brpc::IsAskedToQuit() && !interrupt.load(std::memory_order_relaxed))
    {
        cntl.Reset();
        txlog::LogResponse response;

        uint64_t txn = request->txn_number();
        txn++;
        request->set_txn_number(txn);

        // set log messages for each ng
        ::google::protobuf::Map<::google::protobuf::uint32, ::std::string>
            *txn_logs = request->mutable_log_content()
                            ->mutable_data_log()
                            ->mutable_node_txn_logs();

        txn_logs->clear();
        std::string s = std::to_string(cnt);
        s.append(std::string(FLAGS_log_entry_size, 'a'));
        (*txn_logs)[1] = s;

        auto start = now();
        request->set_commit_timestamp(request->commit_timestamp() + 1);
        uint32_t log_group_count = agent.LogGroupCount();
        uint32_t log_group_id = agent.LogGroupId(txn % log_group_count);
        request->set_log_group_id(log_group_id);
        agent.WriteLog(
            log_group_id, &cntl, &general_request, &response, nullptr);
        auto stop = now();
        if (cntl.Failed() || response.response_status() !=
                                 txlog::LogResponse::ResponseStatus::
                                     LogResponse_ResponseStatus_Success)
        {
            LOG(INFO) << "Send request fail! log_group_id: " << log_group_id
                      << " error: " << cntl.ErrorCode()
                      << " s: " << response.response_status();
            sleep(1);
            agent.RefreshLeader(log_group_id);
        }
        else
        {
            cnt++;
            time_us += std::chrono::duration_cast<std::chrono::microseconds>(
                           stop - start)
                           .count();
        }
    }

    tt_cnt.fetch_add(cnt, std::memory_order_relaxed);
    tt_tm_us.fetch_add(time_us, std::memory_order_relaxed);
    LOG(INFO) << "thread " << std::this_thread::get_id() << " send " << cnt
              << " write log request, average response time: "
              << (time_us / cnt) << " microseconds";
}

void long_running_test(uint32_t cc_ng_id, int term)
{
    int send_time_s = FLAGS_test_duration;
    for (int i = 0; i < 1; i++)
    {
        auto start = now();
        LOG(INFO) << "send write log request for " << send_time_s
                  << " seconds...";
        std::vector<std::thread> send_threads(FLAGS_thread_num);
        std::atomic<int64_t> cnt{};      // total request cnt
        std::atomic<int64_t> time_us{};  // total request time cost
        std::atomic<bool> interrupt{};

        // start sender threads
        for (int j = 0; j < FLAGS_thread_num; j++)
        {
            send_threads[j] = std::thread(sender,
                                          cc_ng_id,
                                          term,
                                          std::ref(cnt),
                                          std::ref(time_us),
                                          std::ref(interrupt));
        }

        while (std::chrono::duration_cast<std::chrono::seconds>(now() - start)
                   .count() < send_time_s)
        {
            sleep(1);
        }

        // stop senders
        interrupt.store(true, std::memory_order_relaxed);
        for (int j = 0; j < FLAGS_thread_num; j++)
        {
            send_threads[j].join();
        }
        send_threads.clear();

        LOG(INFO) << "send " << cnt
                  << " write log request, qps: " << (cnt / send_time_s)
                  << ", average response time: " << (time_us / cnt)
                  << " microseconds";

        sleep(20);
    }
}

void my_handler(int s)
{
    exit(1);
}

void replay(uint32_t cc_ng_id, int term)
{
    auto start = now();
    std::atomic<bool> intrpt{};
    log_agent.ReplayLog(cc_ng_id,
                        term,
                        FLAGS_replay_service_ip,
                        FLAGS_replay_service_port,
                        -1,
                        0,
                        intrpt,
                        false);
    auto stop = now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    LOG(INFO) << "Replay request takes: " << duration.count()
              << "microseconds.";
}

int start_replay_server()
{
    brpc::Server server;
    FakeReplayService replay_service;

    // add replay service into server
    if (server.AddService(&replay_service, brpc::SERVER_DOESNT_OWN_SERVICE) !=
        0)
    {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // start server
    brpc::ServerOptions options;
    if (server.Start(FLAGS_replay_service_port, &options) != 0)
    {
        LOG(ERROR) << "Fail to start ReplayServer";
        return -1;
    }
    LOG(INFO) << "Log replay server started";
    server.RunUntilAskedToQuit();
    return 0;
}

int main(int argc, char *argv[])
{
    butil::AtExitManager exit_manager;
    signal(SIGINT, my_handler);

    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    LOG(INFO) << "lg_raft_conf: " << FLAGS_lg_raft_conf;
    LOG(INFO) << "thread_num: " << FLAGS_thread_num;
    LOG(INFO) << "log_entry_size: " << FLAGS_log_entry_size;
    LOG(INFO) << "replay_service_ip: " << FLAGS_replay_service_ip;
    LOG(INFO) << "replay_service_port: " << FLAGS_replay_service_port;
    LOG(INFO) << "test_duration" << FLAGS_test_duration;
    LOG(INFO) << "start_log_group_id" << FLAGS_start_log_group_id;

    std::vector<std::string> ips;
    std::vector<uint16_t> ports;

    std::string token;
    std::istringstream txlog_ip_list_stream(FLAGS_lg_raft_conf);
    while (std::getline(txlog_ip_list_stream, token, ','))
    {
        size_t c_idx = token.find_first_of(':');
        if (c_idx != std::string::npos)
        {
            ips.emplace_back(token.substr(0, c_idx));
            uint16_t pt = std::stoi(token.substr(c_idx + 1));
            ports.emplace_back(pt);
        }
        else
        {
            DLOG(ERROR) << "Port is missing in lg_raft_conf";
            return 1;
        }
    }
    log_agent.Init(ips, ports, FLAGS_start_log_group_id);

    int cc_ng_start_term = 1;
    std::thread replay_thread = std::thread(start_replay_server);
    replay(0, cc_ng_start_term);

    sleep(5);
    DLOG(INFO) << "Start send log";
    long_running_test(0, cc_ng_start_term);

    LOG(INFO) << "Log Client is going to quit";
    return 0;
}
