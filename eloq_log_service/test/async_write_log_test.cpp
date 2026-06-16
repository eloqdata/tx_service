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
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "log.pb.h"
#include "log_agent.h"

#if BRPC_WITH_GLOG
#include "glog_error_logging.h"
#endif

DEFINE_string(lg_raft_conf,
              "127.0.0.1:8002:0",
              "Configuration of the target log replication group");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(log_entry_size, 100, "Byte size of each log entry");
DEFINE_string(replay_service_ip, "127.0.0.1", "Replay service ip");
DEFINE_int32(replay_service_port, 8888, "Replay service port");
DEFINE_int32(test_duration, 20, "Test duration");
DEFINE_uint32(start_log_group_id, 0, "Start log group id");
DEFINE_uint32(log_replica_num, 3, "Replicate number of log");
DEFINE_int32(max_flying_req_num, 100, "Max flying request number");

class StatsData
{
public:
    StatsData() : count_(0), accumulated_time_(0)
    {
    }
    int64_t count_{0};
    int64_t accumulated_time_{0};
};

class CallbackThdStats
{
public:
    CallbackThdStats() : map_mutex_(), callback_thd_stats_()
    {
    }

    void AddTime(int64_t time)
    {
        std::thread::id current_thd_id = std::this_thread::get_id();
        std::lock_guard<std::mutex> lock(map_mutex_);
        if (callback_thd_stats_.count(current_thd_id) == 0)
        {
            callback_thd_stats_.emplace(current_thd_id, StatsData());
        }
        StatsData &stats = callback_thd_stats_[current_thd_id];
        stats.count_++;
        stats.accumulated_time_ += time;
    }

    StatsData GetStats()
    {
        std::lock_guard<std::mutex> lock(map_mutex_);
        StatsData all_stats;
        for (auto it = callback_thd_stats_.begin();
             it != callback_thd_stats_.end();
             ++it)
        {
            const auto &[thd_id, stats] = *it;
            all_stats.count_ += stats.count_;
            all_stats.accumulated_time_ += stats.accumulated_time_;
        }
        return all_stats;
    }

private:
    std::mutex map_mutex_;
    std::unordered_map<std::thread::id, StatsData> callback_thd_stats_;
};

txlog::LogAgent log_agent;
std::random_device rd;
std::default_random_engine generator(rd());
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);
CallbackThdStats callback_thd_stats;
std::atomic<int32_t> flying_reqs_count;

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
        brpc::StreamClose(id);
    }

    void on_closed(brpc::StreamId id) override
    {
        brpc::StreamClose(id);
        LOG(INFO) << "Stream " << id << " is closed";
    }
};

auto now()
{
    return std::chrono::high_resolution_clock::now();
}

bvar::LatencyRecorder write_log_latency("write_log");

class OnRPCDone : public google::protobuf::Closure
{
public:
    OnRPCDone()
    {
    }

    void Run()
    {
        // unique_ptr helps us to delete response/cntl automatically. unique_ptr
        // in gcc 3.4 is an emulated version.
        std::unique_ptr<OnRPCDone> self_guard(this);

        if (cntl_.Failed() || response_.response_status() !=
                                  txlog::LogResponse::ResponseStatus::
                                      LogResponse_ResponseStatus_Success)
        {
            log_agent.RefreshLeader(log_group_id_);
        }
        else
        {
            auto stop = now();
            int64_t time =
                std::chrono::duration_cast<std::chrono::microseconds>(stop -
                                                                      start_)
                    .count();
            callback_thd_stats.AddTime(time);
            write_log_latency << cntl_.latency_us();
        }

        flying_reqs_count.fetch_sub(1, std::memory_order_release);
    }

    txlog::LogResponse response_;
    brpc::Controller cntl_;
    uint32_t log_group_id_;
    std::chrono::time_point<std::chrono::high_resolution_clock> start_;
};

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
    uint64_t cnt = 0;
    uint64_t update_checkpoint_cnt = 2 * 10000;

    while (!brpc::IsAskedToQuit() && !interrupt.load(std::memory_order_relaxed))
    {
        OnRPCDone *done = new OnRPCDone();

        uint64_t txn = request->txn_number();
        txn++;
        request->set_txn_number(txn);

        // set log messages for each ng
        ::google::protobuf::Map<::google::protobuf::uint32, ::std::string>
            *txn_logs = request->mutable_log_content()
                            ->mutable_data_log()
                            ->mutable_node_txn_logs();

        txn_logs->clear();
        std::string s = std::to_string(cnt++);
        s.append(std::string(FLAGS_log_entry_size, 'a'));
        (*txn_logs)[1] = s;

        uint64_t timestamp = now().time_since_epoch().count();
        request->set_commit_timestamp(timestamp);
        uint32_t log_group_count = agent.LogGroupCount();
        uint32_t log_group_id = agent.LogGroupId(txn % log_group_count);
        request->set_log_group_id(log_group_id);
        done->log_group_id_ = log_group_id;
        done->start_ = now();
        agent.WriteLog(log_group_id,
                       &done->cntl_,
                       &general_request,
                       &done->response_,
                       done);

        // randomly update checkpoint ts
        if (cnt % update_checkpoint_cnt == 0)
        {
            LOG(INFO) << "update checkpoint ts " << request->commit_timestamp();
            log_agent.UpdateCheckpointTs(
                cc_ng_id, ng_term, request->commit_timestamp());
        }

        flying_reqs_count.fetch_add(1, std::memory_order_release);
        // sleep a while, otherwise bthread will hang on error "brpc _rq is
        // full, capacity=4096"
        // sleep before continue
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        int32_t _flying_reqs_count =
            flying_reqs_count.load(std::memory_order_acquire);
        while (_flying_reqs_count > FLAGS_max_flying_req_num)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
            _flying_reqs_count =
                flying_reqs_count.load(std::memory_order_acquire);
        }
    }
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
        std::atomic<bool> interrupt_dump{};
        std::vector<StatsData> stats_snapshots;

        // start sender threads
        for (int j = 0; j < FLAGS_thread_num; j++)
        {
            send_threads[j] = std::thread(sender,
                                          0,
                                          term,
                                          std::ref(cnt),
                                          std::ref(time_us),
                                          std::ref(interrupt));
        }

        std::thread dump_flying_reqs_num = std::thread(
            [&interrupt_dump, &start, &stats_snapshots]
            {
                std::cout << "\n";
                uint formatted_cnt_len =
                    std::to_string(FLAGS_max_flying_req_num).length();
                auto last_snapshot_time = now();
                while (!interrupt_dump.load(std::memory_order_relaxed))
                {
                    int32_t _flying_count =
                        flying_reqs_count.load(std::memory_order_acquire);
                    std::string cnt_str = std::to_string(_flying_count);
                    if (cnt_str.length() < formatted_cnt_len)
                    {
                        std::ostringstream padded_cnt;
                        for (uint i = 0;
                             i < formatted_cnt_len - cnt_str.length();
                             i++)
                        {
                            padded_cnt << '0';
                        }
                        padded_cnt << cnt_str;
                        cnt_str = padded_cnt.str();
                    }
                    else
                    {
                        formatted_cnt_len = cnt_str.length();
                    }
                    int test_last =
                        std::chrono::duration_cast<std::chrono::seconds>(now() -
                                                                         start)
                            .count();
                    LOG(INFO)
                        << "Test running time: " << test_last << " seconds "
                        << " Current flying requests count: " << cnt_str;
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(1000));
                    // dump stats every 10 seconds
                    if (std::chrono::duration_cast<std::chrono::seconds>(
                            now() - last_snapshot_time)
                            .count() > 10)
                    {
                        last_snapshot_time = now();
                        stats_snapshots.emplace_back(
                            callback_thd_stats.GetStats());
                    }
                }
                std::cout << "\n";
            });

        while (std::chrono::duration_cast<std::chrono::seconds>(now() - start)
                   .count() < send_time_s)
        {
            sleep(1);
            LOG(INFO) << "write log at qps=" << write_log_latency.qps(1)
                      << " latency=" << write_log_latency.latency(1) << "us";
        }

        // stop senders
        interrupt.store(true, std::memory_order_release);
        while (flying_reqs_count.load(std::memory_order_acquire) > 0)
        {
            sleep(1);
        }

        for (int j = 0; j < FLAGS_thread_num; j++)
        {
            if (send_threads[j].joinable())
            {
                send_threads[j].join();
            }
        }
        send_threads.clear();

        // stop dump
        interrupt_dump.store(true, std::memory_order_release);
        dump_flying_reqs_num.join();

        // wait for all callback finished;
        LOG(INFO) << "test done! wait for data to be collected.";
        StatsData stats = callback_thd_stats.GetStats();

        LOG(INFO) << "send " << stats.count_
                  << " write log request, qps: " << (stats.count_ / send_time_s)
                  << ", average response time: "
                  << (stats.accumulated_time_ / stats.count_)
                  << " microseconds";
        // dump snapshot stats, collect the data in the 10 seconds interval
        StatsData inital_stats_snapshot = StatsData();
        StatsData &prev_stats_snapshot = inital_stats_snapshot;
        for (size_t idx = 0; idx < stats_snapshots.size(); idx++)
        {
            StatsData &stats_snapshot = stats_snapshots[idx];
            int64_t count = stats_snapshot.count_ - prev_stats_snapshot.count_;
            int64_t time = stats_snapshot.accumulated_time_ -
                           prev_stats_snapshot.accumulated_time_;
            prev_stats_snapshot = stats_snapshot;

            LOG(INFO) << "snapshot " << idx << " send " << count
                      << " write log request, qps: " << (count / 10)
                      << ", average response time: " << (time / count)
                      << " microseconds";
        }
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
              << " microseconds.";
}

int start_replay_server(std::atomic<bool> &stop)
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
    while (!stop.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server.Stop(0);
    server.Join();
    LOG(INFO) << "Log replay server stopped";
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
    log_agent.Init(ips, ports, FLAGS_start_log_group_id, FLAGS_log_replica_num);

    std::atomic<bool> stop_replay{false};
    int cc_ng_start_term = 1;
    std::thread replay_thread =
        std::thread(start_replay_server, std::ref(stop_replay));
    replay(0, cc_ng_start_term);
    // for (int i = 0; i < 10; i++)
    //{
    // replay(i, cc_ng_start_term);
    //}

    sleep(2);
    DLOG(INFO) << "Start send log";
    long_running_test(0, cc_ng_start_term);

    stop_replay.store(true, std::memory_order_relaxed);
    if (replay_thread.joinable())
    {
        replay_thread.join();
    }

    LOG(INFO) << "Log Client is going to quit";
    return 0;
}
