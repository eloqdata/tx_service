#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <climits>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "log.pb.h"
#include "log_agent.h"
#include "replay_service_for_test.h"

#if BRPC_WITH_GLOG
#include "glog_error_logging.h"
#endif

using namespace std;
using namespace std::chrono;

DEFINE_int32(groupid, 0, "Id of the log group we will send request to");
DEFINE_string(lg_raft_conf,
              "127.0.0.1:8000:0,127.0.0.1:8001:0,127.0.0.1:8002:0",
              "Configuration of the target log replication group");
DEFINE_int32(thread_num, 48, "Number of threads sending requests");
DEFINE_int32(replay_port, 8888, "Port of the replay service");

bvar::LatencyRecorder g_latency_recorder("client");
txlog::LogAgent log_agent;
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);

// Thread-local engine avoids data races when multiple sender threads call
// distribution(generator) concurrently (std::default_random_engine is not
// thread-safe).
thread_local std::default_random_engine generator(std::random_device{}());

auto now()
{
    return std::chrono::high_resolution_clock::now();
}

void report_term(txlog::LogAgent &la, int ng_term)
{
    // send replay log request to update cc node group term
    std::atomic<bool> interrupt{false};
    la.ReplayLog(
        1, ng_term, "127.0.0.1", FLAGS_replay_port, -1, interrupt, true);
    // la.ReplayLog(2, ng_term, "some_ip", 54321, -1, interrupt, true);
}

// generate log request and return blob message containing log information
std::string generate_log_request(int ng_id, txlog::WriteLogRequest &request)
{
    // tx_number should not be larger than 1<<44, since (tx_no>>42)/3 is the
    // target lg_id, which must be 0 in our test
    uint64_t tx_number = distribution(generator);
    uint64_t timestamp = now().time_since_epoch().count();
    // set this tx's sending ng_id to 1
    tx_number |= (1L << 42);
    // add some random variation
    //    timestamp += distribution(generator);
    request.set_txn_number(tx_number);
    request.set_commit_timestamp(timestamp);
    return "ng_id: " + std::to_string(ng_id) +
           ", ts: " + std::to_string(timestamp) +
           ", tx_number: " + std::to_string(tx_number);
}

/**
 * keep sending add log requests to lg0 in batch until interrupted
 */
void sender(int ng_term,
            atomic<int64_t> &tt_cnt,
            atomic<int64_t> &tt_tm_us,
            atomic<bool> &interrupt,
            bool async)
{
    txlog::LogAgent &agent = log_agent;

    brpc::Controller cntl;
    txlog::LogRequest general_request;
    // prepare WriteLogRequest arguments
    txlog::WriteLogRequest *request =
        general_request.mutable_write_log_request();
    generate_log_request(1, *request);
    request->set_tx_term(ng_term);

    ::google::protobuf::Map<::google::protobuf::uint32,
                            ::google::protobuf::int64> *shard_terms =
        request->mutable_node_terms();
    (*shard_terms)[1] = ng_term;

    // keep sending WriteLogRequest
    int64_t cnt = 0;
    int64_t time_us = 0;

    while (!brpc::IsAskedToQuit() && !interrupt.load(std::memory_order_relaxed))
    {
        cntl.Reset();
        txlog::LogResponse response;
        request->set_txn_number(request->txn_number() + 1);

        // set log messages for each ng
        ::google::protobuf::Map<::google::protobuf::uint32, ::std::string>
            *txn_logs = request->mutable_log_content()
                            ->mutable_data_log()
                            ->mutable_node_txn_logs();

        string s = to_string(cnt);
        s.append(string(50, 'a'));
        (*txn_logs)[1] = s;

        auto start = now();
        if (async)
        {
        }
        else
        {
            request->set_commit_timestamp(request->commit_timestamp() + 1);
            agent.WriteLog(
                FLAGS_groupid, &cntl, &general_request, &response, nullptr);
        }
        auto stop = now();
        if (cntl.Failed() || response.response_status() !=
                                 txlog::LogResponse::ResponseStatus::
                                     LogResponse_ResponseStatus_Success)
        {
            sleep(1);
            LOG(INFO) << "Send request fail!";
            agent.RefreshLeader(FLAGS_groupid);
        }
        else
        {
            cnt++;
            time_us += duration_cast<microseconds>(stop - start).count();
        }
    }
    tt_cnt.fetch_add(cnt, std::memory_order_relaxed);
    tt_tm_us.fetch_add(time_us, memory_order_relaxed);
    LOG(INFO) << "thread " << std::this_thread::get_id() << " send " << cnt
              << " write log request, average response time: "
              << (cnt == 0 ? 0 : time_us / cnt) << " microseconds";
}

void replay(int term)
{
    auto start = now();
    std::atomic<bool> intrpt{};
    log_agent.ReplayLog(
        1, term, "127.0.0.1", FLAGS_replay_port, -1, intrpt, false);
    auto stop = now();
    auto duration = duration_cast<microseconds>(stop - start);
    LOG(INFO) << "Replay request takes: " << duration.count()
              << "microseconds.";
}

void long_running_replay_time_test(int term)
{
    using namespace std::chrono;
    int send_time_s = 180;
    //    auto last_truncate_ts = now().time_since_epoch().count();
    for (int i = 0; i < 1; i++)
    {
        auto start = now();
        LOG(INFO) << "send write log request for " << send_time_s
                  << " seconds...";
        vector<thread> send_threads(FLAGS_thread_num);
        atomic<int64_t> cnt{};      // total request cnt
        atomic<int64_t> time_us{};  // total request time cost
        atomic<bool> interrupt{};

        // start sender threads
        for (int j = 0; j < FLAGS_thread_num; j++)
        {
            send_threads[j] = thread(sender,
                                     term,
                                     std::ref(cnt),
                                     std::ref(time_us),
                                     std::ref(interrupt),
                                     false);
        }

        while (duration_cast<seconds>(now() - start).count() < send_time_s)
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
                  << ", average response time: "
                  << (cnt == 0 ? 0 : time_us / cnt) << " microseconds";

        // replay
        replay(term + 1);

        //        auto truncate_timestamp = stop.time_since_epoch().count();
        //        brpc::Controller cntl;
        //        txlog::LogRequest general_request;
        //        txlog::TruncateLogRequest *request =
        //            general_request.mutable_truncate_log_request();
        //        txlog::LogResponse response;
        //
        //        request->set_cc_node_group_id(1);
        //        request->set_ckpt_timestamp(truncate_timestamp);
        //        request->set_log_group_id(0);
        //        LOG(INFO) << "Send truncate log request, ckpt timestamp: "
        //                  << truncate_timestamp;
        //        log_agent.TruncateLog(&cntl, &general_request, &response,
        //        nullptr);
        //                last_truncate_ts = truncate_timestamp;
        sleep(20);
    }
}

int main(int argc, char *argv[])
{
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    // GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    std::vector<std::string> ips{"127.0.0.1", "127.0.0.1", "127.0.0.1"};
    std::vector<uint16_t> ports{8002, 8012, 8022};
    // std::vector<std::string> ips{"127.0.0.1"};
    // std::vector<uint16_t> ports{7998};
    log_agent.Init(ips, ports, 0);

    // Register configuration of target group to RouteTable
    std::string group_name("lg");
    group_name.append(std::to_string(FLAGS_groupid));
    braft::rtb::update_configuration(group_name, FLAGS_lg_raft_conf);
    DLOG(INFO) << "lg info: group_name: " << group_name
               << " ,raft_conf: " << FLAGS_lg_raft_conf;

    int baseterm = 1;
    report_term(log_agent, baseterm);
    DLOG(INFO) << "Report term done!";
    long_running_replay_time_test(baseterm);

    //    replay(baseterm);

    LOG(INFO) << "Log Client is going to quit";
    return 0;
}
