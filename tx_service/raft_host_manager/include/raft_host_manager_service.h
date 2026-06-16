#pragma once

#include <brpc/channel.h>

#include "cc_request.pb.h"
namespace host_manager
{
class RaftHostManager;
};
namespace txservice::remote
{
class RaftHostManagerService : public HostMangerService
{
public:
    RaftHostManagerService(host_manager::RaftHostManager &hm) : hm_(hm)
    {
    }

    void StartNode(::google::protobuf::RpcController *controller,
                   const StartNodeRequest *request,
                   StartNodeResponse *response,
                   ::google::protobuf::Closure *done) override;

    void CheckHealth(::google::protobuf::RpcController *controller,
                     const ::google::protobuf::Empty *request,
                     ::txservice::remote::HmHealthzHttpResponse *response,
                     ::google::protobuf::Closure *done) override;

    void DetachTxService(::google::protobuf::RpcController *controller,
                         const ::google::protobuf::Empty *request,
                         ::txservice::remote::DetachTxServiceResponse *response,
                         ::google::protobuf::Closure *done) override;

    void AttachTxService(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::AttachTxServiceRequest *request,
        ::txservice::remote::AttachTxServiceResponse *response,
        ::google::protobuf::Closure *done) override;

    void UpdateClusterConfig(::google::protobuf::RpcController *controller,
                             const UpdateClusterConfigRequest *request,
                             UpdateClusterConfigResponse *response,
                             ::google::protobuf::Closure *done) override;

    void Transfer(::google::protobuf::RpcController *controller,
                  const TransferRequest *request,
                  TransferResponse *response,
                  ::google::protobuf::Closure *done) override;

    void GetLeader(::google::protobuf::RpcController *controller,
                   const GetLeaderRequest *request,
                   GetLeaderResponse *response,
                   ::google::protobuf::Closure *done) override;

private:
    host_manager::RaftHostManager &hm_;
};
}  // namespace txservice::remote