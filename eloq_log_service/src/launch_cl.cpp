#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <gflags/gflags.h>

#include <random>
#include <thread>
#include <vector>

#include "log.pb.h"
#include "log_agent.h"
#include "replay_service_for_test.h"

DEFINE_int32(groupid, 0, "Id of the log group we will send request to");
DEFINE_string(lg_raft_conf,
              "127.0.0.1:8000:0,127.0.0.1:8001:0,127.0.0.1:8002:0",
              "Configuration of the target log replication group");
// DEFINE_int32(lg_count, 1, "Number of log groups in configuration");
// DEFINE_int32(replica_num, 3, "Number of replicas in one log group");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(replay_port, 8888, "Port of the replay service");

bvar::LatencyRecorder g_latency_recorder("client");
txlog::LogAgent log_agent(1, 3);
std::random_device rd;
std::default_random_engine generator(rd());
std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);

void report_term(int ng_term)
{
    // send replay log request to update cc node group term
    std::atomic<bool> interrupt{};
    log_agent.ReplayLog(
        1, ng_term, "127.0.0.1", FLAGS_replay_port, interrupt, true);
    log_agent.ReplayLog(2, ng_term, "some_ip", 54321, interrupt, true);
}

// generate log request and return blob message containing log information
std::string generate_log_request(int ng_id,
                                 txlog::WriteLogRequest &request,
                                 std::vector<uint64_t> &res)
{
    // tx_number should not be larger than 1<<44, since (tx_no>>42)/3 is the
    // target lg_id, which must be 0 in our test
    uint64_t tx_number = distribution(generator);
    uint64_t timestamp = distribution(generator);
    // set this tx's sending ng_id to 1
    tx_number |= (1L << 42);

    res.push_back(tx_number);
    request.set_txn_number(tx_number);
    request.set_commit_timestamp(timestamp);
    return "ng_id: " + std::to_string(ng_id) +
           ", ts: " + std::to_string(timestamp) +
           ", tx_number: " + std::to_string(tx_number);
}

/**
 * sending add log requests to lg0
 * @param num how many requests to send
 * @param ng_term term of ngs
 */
std::vector<uint64_t> sender(int num, int ng_term)
{
    // Simulate a ccnode group leader.
    // On start, send replay log request to log service to register ccng term,
    // then we can send dummy write log request to log service.

    report_term(ng_term);

    std::vector<uint64_t> txs;
    // keep sending WriteLogRequest
    int seq = 0;
    while (!brpc::IsAskedToQuit() && seq < num)
    {
        brpc::Controller cntl;
        txlog::LogRequest general_request;
        txlog::LogResponse response;

        // prepare WriteLogRequest arguments
        txlog::WriteLogRequest *request =
            general_request.mutable_write_log_request();
        // set tx term
        request->set_tx_term(ng_term);
        std::string msg = generate_log_request(1, *request, txs);
        LOG(INFO) << "generating request: " << msg;
        // set log messages for each ng
        ::google::protobuf::Map<::google::protobuf::uint32, ::std::string>
            *txn_logs = request->mutable_node_txn_logs();
        (*txn_logs)[1] = msg;
        (*txn_logs)[2] = "hello world" + std::to_string(seq);
        ::google::protobuf::Map<::google::protobuf::uint32,
                                ::google::protobuf::int64> *shard_terms =
            request->mutable_node_terms();
        //        auto shard_term = request->mutable_node_terms();
        (*shard_terms)[1] = ng_term;
        (*shard_terms)[2] = ng_term;

        // send request through log_agent
        LOG(INFO) << "sending write log request to lg" << FLAGS_groupid;

        log_agent.WriteLog(
            FLAGS_groupid, &cntl, &general_request, &response, nullptr);

        if (!response.success())
        {
            LOG(WARNING) << "Request fail, redirecting information: "
                         << response.write_log_response().redirect();
            // update route table since we have redirect information
            log_agent.RefreshLeader(FLAGS_groupid);
        }
        else
        {
            LOG(INFO) << "Write log request success";
        }
        g_latency_recorder << 1;
        //        sleep(1);
        seq++;
    }
    return txs;
}

void write_log_test(int term)
{
    sender(10, term);
}

int start_replay_rpc_server()
{
    brpc::Server server;
    ReplayService replay_service_impl;
    // add replay service into server
    if (server.AddService(&replay_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // start server
    brpc::ServerOptions options;
    if (server.Start(FLAGS_replay_port, &options) != 0)
    {
        LOG(ERROR) << "Fail to start ReplayServer";
        return -1;
    }
    LOG(INFO) << "log replay server started";
    server.RunUntilAskedToQuit();
    return 0;
}

void replay_log_test(int term)
{
    // first send 10 write log requests to lg0 for later replay
    sender(10, term);
    std::thread thd(start_replay_rpc_server);
    sleep(1);  // waiting for replay rpc server
    std::atomic<bool> interrupt{};
    // replay log term must be bigger than sender term
    log_agent.ReplayLog(
        1, term + 1, "127.0.0.1", FLAGS_replay_port, interrupt, false);
    thd.join();
}

void recover_tx(int term, std::vector<uint64_t> &txs)
{
    for (auto tx_number : txs)
    {
        LOG(INFO) << "sending recover tx request: tx_number: " << tx_number;
        auto resp = log_agent.RecoverTx(tx_number, term, 1, term);
        LOG(INFO) << "tx " << tx_number
                  << " status: " << RecoverTxResponse_TxStatus_Name(resp);
    }
}

void dummy_recover(int term)
{
    std::vector<uint64_t> txs = {1, 2, 3, 4};
    for (auto tx_number : txs)
    {
        LOG(INFO) << "sending recover tx request: tx_number: " << tx_number;
        auto resp = log_agent.RecoverTx(tx_number, term, 1, term);
        LOG(INFO) << "tx " << tx_number
                  << " status: " << RecoverTxResponse_TxStatus_Name(resp);
    }
}

void recover_tx_test(int term)
{
    std::vector<uint64_t> txs = sender(10, term);
    std::thread thd(start_replay_rpc_server);
    sleep(1);
    // add three random invalid tx_number
    for (int i = 0; i < 3; i++)
    {
        txs.push_back(distribution(generator));
    }
    // send recover txs request
    recover_tx(term, txs);
    thd.join();
}

void truncate_log_test(int term)
{
    sender(10, term);
    // replay log
    std::thread thd(start_replay_rpc_server);
    sleep(1);  // waiting for replay rpc server
    std::atomic<bool> interrupt{};
    // replay log term must be bigger than sender term
    log_agent.ReplayLog(
        1, term + 1, "127.0.0.1", FLAGS_replay_port, interrupt, false);
    sleep(1);

    uint64_t truncate_timestamp = distribution(generator);

    // truncate all log records for ng1
    //    uint64_t truncate_timestamp = 0xFFFFFFFF;

    brpc::Controller cntl;
    txlog::LogRequest general_request;
    txlog::TruncateLogRequest *request =
        general_request.mutable_truncate_log_request();
    txlog::LogResponse response;

    request->set_cc_node_group_id(1);
    request->set_ckpt_timestamp(truncate_timestamp);
    request->set_log_group_id(0);
    LOG(INFO) << "Send truncate log request, ckpt timestamp: "
              << truncate_timestamp;
    log_agent.TruncateLog(&cntl, &general_request, &response, nullptr);
    if (!response.success())
    {
        LOG(WARNING) << "Request fail, redirecting information: "
                     << response.write_log_response().redirect();
        return;
    }
    else
    {
        LOG(INFO) << "Truncate log request success";
    }

    // send replay log request again to check if truncate succeeds
    // replay log term must be bigger than previous term
    log_agent.ReplayLog(
        1, term + 2, "127.0.0.1", FLAGS_replay_port, interrupt, false);
    thd.join();
}

// assuming snapshots loaded, check if recovered state is the same
void snapshot_test(int term)
{
    std::thread thd(start_replay_rpc_server);
    sleep(1);  // waiting for replay rpc server
    std::atomic<bool> interrupt{};
    // replay log term must be bigger than sender term
    log_agent.ReplayLog(
        1, term, "127.0.0.1", FLAGS_replay_port, interrupt, false);
    thd.join();
}

void new_leader_reship_test(int term)
{
    std::thread thd(start_replay_rpc_server);
    sleep(1);
    std::atomic<bool> interrupt{};
    sender(3, term);
    // replay log term must be bigger than sender term
    std::cout << "Send ReplayLog requests with term: " << term + 1 << std::endl;
    log_agent.ReplayLog(
        1, term + 1, "127.0.0.1", FLAGS_replay_port, interrupt, false);

    sleep(2);
    std::cout
        << "Plz trigger leader transfer manually, see if reship happens in "
           "client terminal (supposed to happen), you have 20 seconds"
        << std::endl;
    //
    // trigger leader transfer manually, see if reship happens in terminal
    // supposed to happen
    //

    sleep(20);
    std::cout << "Send requests with term: " << term + 2 << std::endl;

    sender(3, term + 2);
    sleep(2);
    std::cout
        << "Plz trigger leader transfer manually, see if reship happens in "
           "client terminal (supposed not to happen)"
        << std::endl;
    //
    // trigger leader transfer manually, see if reship happens in terminal
    // supposed not to happen
    //

    thd.join();
}

int main(int argc, char *argv[])
{
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    // GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    std::vector<std::unique_ptr<std::atomic_uint32_t>> lg_leader_idx_vct_;
    lg_leader_idx_vct_.push_back(std::make_unique<std::atomic_uint32_t>(0));
    log_agent.SetLogGroupLeaderIdxs(&lg_leader_idx_vct_);

    // Register configuration of target group to RouteTable
    std::string group_name("lg");
    group_name.append(std::to_string(FLAGS_groupid));
    braft::rtb::update_configuration(group_name, FLAGS_lg_raft_conf);

    std::vector<std::thread> thd_pool;
    int baseterm = 1;
    for (int i = 0; i < FLAGS_thread_num; i++)
    {
        //        thd_pool.emplace_back(write_log_test, baseterm);
        //        thd_pool.emplace_back(truncate_log_test, baseterm);
        thd_pool.emplace_back(replay_log_test, baseterm);
        //        thd_pool.emplace_back(recover_tx_test, baseterm);
        //        thd_pool.emplace_back(snapshot_test, baseterm);
        //        thd_pool.emplace_back(dummy_recover, baseterm);
        //        thd_pool.emplace_back(new_leader_reship_test, baseterm);
    }

    //    while (!brpc::IsAskedToQuit())
    //    {
    //        sleep(1);
    //        LOG(INFO) << "Sending Request to " << group_name << " ("
    //                  << FLAGS_lg_raft_conf << ")"
    //                  << " at qps=" << g_latency_recorder.qps(1);
    //    }
    for (int i = 0; i < FLAGS_thread_num; i++)
    {
        thd_pool[i].join();
    }
    LOG(INFO) << "Log Client is going to quit";
    return 0;
}
