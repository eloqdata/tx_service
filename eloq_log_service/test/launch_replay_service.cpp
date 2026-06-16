#include <brpc/server.h>
#include <gflags/gflags.h>

#include <iostream>

#include "replay_service_for_test.h"

DEFINE_int32(replay_port, 8888, "Port of the replay service");

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

int main(int argc, char *argv[])
{
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    return start_replay_rpc_server();
}
