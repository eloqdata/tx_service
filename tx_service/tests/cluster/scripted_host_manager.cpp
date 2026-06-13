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
#include "cluster/scripted_host_manager.h"

#include <brpc/closure_guard.h>
#include <glog/logging.h>

#include <mutex>
#include <string>

namespace txservice
{
namespace test
{
bool ScriptedHostManager::Start(const std::string &ip, uint16_t port)
{
    if (server_.AddService(this, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR) << "Failed to add the scripted host manager service.";
        return false;
    }

    brpc::ServerOptions options;
    // num_threads == 0 means use the default bthread worker count.
    options.num_threads = 0;

    // Bind to the requested interface so nodes can reach the HM at ip:port.
    std::string ip_port = ip + ":" + std::to_string(port);
    if (server_.Start(ip_port.c_str(), &options) != 0)
    {
        LOG(ERROR) << "Failed to start the scripted host manager server at "
                   << ip_port;
        return false;
    }
    return true;
}

void ScriptedHostManager::Stop()
{
    server_.Stop(0);
    server_.Join();
}

void ScriptedHostManager::SetLeader(uint32_t ng_id, uint32_t node_id)
{
    std::lock_guard<std::mutex> lk(mux_);
    ng_leader_[ng_id] = node_id;
}

void ScriptedHostManager::StartNode(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::StartNodeRequest *request,
    ::txservice::remote::StartNodeResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    // The driver already knows the topology, so we don't inspect the
    // registration payload. Accept all registrations.
    response->set_error(false);
}

void ScriptedHostManager::GetLeader(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::GetLeaderRequest *request,
    ::txservice::remote::GetLeaderResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lk(mux_);
    auto it = ng_leader_.find(request->ng_id());
    if (it == ng_leader_.end())
    {
        // Leadership not yet driven for this NG; report unresolved.
        response->set_error(true);
    }
    else
    {
        response->set_node_id(it->second);
        response->set_error(false);
    }
}
}  // namespace test
}  // namespace txservice
