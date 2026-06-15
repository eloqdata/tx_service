#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <gflags/gflags.h>

#include "log.pb.h"

DEFINE_bool(use_bthread, true, "Use bthread to send requests");
DEFINE_int32(add_percentage, 100, "Percentage of fetch_add");
DEFINE_int64(added_by, 1, "Num added to each peer");
DEFINE_int32(thread_num, 50, "Number of threads sending requests");
DEFINE_int32(timeout_ms, 1000, "Timeout for each request");
DEFINE_string(conf, "10.3.1.17:8100:0", "Configuration of the raft group");
DEFINE_string(group, "0", "Id of the replication group");

bvar::LatencyRecorder g_latency_recorder("client");

static void *sender(void *arg)
{
    while (!brpc::IsAskedToQuit())
    {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(FLAGS_group, &leader) != 0)
        {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st =
                braft::rtb::refresh_leader(FLAGS_group, FLAGS_timeout_ms);
            if (!st.ok())
            {
                // Not sure about the leader, sleep for a while and the ask
                // again.
                LOG(WARNING) << "Fail to refresh_leader : " << st;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
            }
            continue;
        }

        // Now we know who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (leader.type_ == braft::PeerId::Type::EndPoint)
        {
            if (channel.Init(leader.addr, NULL) != 0)
            {
                LOG(ERROR) << "Fail to init channel to " << leader;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
                continue;
            }
        }
        else
        {
            std::string naming_service_url;
            braft::HostNameAddr2NSUrl(leader.hostname_addr, naming_service_url);
            if (channel.Init(naming_service_url.c_str(),
                             braft::LOAD_BALANCER_NAME,
                             NULL) != 0)
            {
                LOG(ERROR) << "Fail to init channel to " << leader;
                bthread_usleep(FLAGS_timeout_ms * 1000L);
                continue;
            }
        }
        txlog::LogService_Stub stub(&channel);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_timeout_ms);
        // Randomly select which request we want to send;

        for (int i = 0; i < 1000000; i++)
        {
            cntl.Reset();
            txlog::LogResponse response;

            txlog::LogRequest general_request;
            txlog::WriteLogRequest *request =
                general_request.mutable_log_request();
            request->set_txn_number(1);
            request->set_timestamp(1);
            ::google::protobuf::Map< ::google::protobuf::uint32, ::std::string>
                *txn_logs = request->mutable_node_txn_logs();
            (*txn_logs)[1] = 'hello';
            (*txn_logs)[2] = 'world';
            ::google::protobuf::Map< ::google::protobuf::uint32,
                                     ::google::protobuf::uint32> *shard_terms =
                request->mutable_node_terms();
            (*shard_terms)[1] = 0;
            (*shard_terms)[2] = 0;
            stub.Write(&cntl, &general_request, &response, NULL);

            if (cntl.Failed())
            {
                LOG(WARNING) << "Fail to send request to " << leader << " : "
                             << cntl.ErrorText();
                // Clear leadership since this RPC failed.
                braft::rtb::update_leader(FLAGS_group, braft::PeerId());
                bthread_usleep(FLAGS_timeout_ms * 1000L);
                continue;
            }
            if (!response.log_response().success())
            {
                LOG(WARNING) << "Fail to send request to " << leader
                             << ", redirecting to "
                             << response.log_response().redirect();
                // Update route table since we have redirect information
                braft::rtb::update_leader(FLAGS_group,
                                          response.log_response().redirect());
                continue;
            }
            g_latency_recorder << 1;
        }
    }
    return NULL;
}

int main(int argc, char *argv[])
{
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Register configuration of target group to RouteTable
    if (braft::rtb::update_configuration(FLAGS_group, FLAGS_conf) != 0)
    {
        LOG(ERROR) << "Fail to register configuration " << FLAGS_conf
                   << " of group " << FLAGS_group;
        return -1;
    }

    std::vector<bthread_t> tids;
    tids.resize(FLAGS_thread_num);
    if (!FLAGS_use_bthread)
    {
        for (int i = 0; i < FLAGS_thread_num; ++i)
        {
            if (pthread_create(&tids[i], NULL, sender, NULL) != 0)
            {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    }
    else
    {
        for (int i = 0; i < FLAGS_thread_num; ++i)
        {
            if (bthread_start_background(&tids[i], NULL, sender, NULL) != 0)
            {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    while (!brpc::IsAskedToQuit())
    {
        sleep(1);
        LOG(INFO) << "Sending Request to " << FLAGS_group << " (" << FLAGS_conf
                  << ')' << " at qps=" << g_latency_recorder.qps(1);
    }

    LOG(INFO) << "Counter client is going to quit";
    for (int i = 0; i < FLAGS_thread_num; ++i)
    {
        if (!FLAGS_use_bthread)
        {
            pthread_join(tids[i], NULL);
        }
        else
        {
            bthread_join(tids[i], NULL);
        }
    }

    return 0;
}
