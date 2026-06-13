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
#pragma once

#include <brpc/server.h>

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

#include "cc_request.pb.h"  // txservice::remote::HostMangerService (cc_generic_services)

namespace txservice
{
namespace test
{
// Minimal in-driver HostMangerService. Nodes register via StartNode and learn
// NG leaders via GetLeader. Leadership is assigned by the driver (SetLeader),
// not by raft. Runs on its own brpc server in the test-driver process.
//
// Ordering note: GetLeader only resolves an NG once the driver has called
// SetLeader for it. WaitClusterReady polls GetLeader until every NG resolves to
// its leader, so the driver MUST drive leadership (OnLeaderStart) and record it
// via SetLeader before / concurrently with the nodes' readiness wait.
class ScriptedHostManager : public txservice::remote::HostMangerService
{
public:
    // Starts the HM brpc server on `ip`, letting brpc atomically pick and bind
    // a free port from the ephemeral range (no reserve-then-bind TOCTOU).
    // Returns true on success; Port() then gives the bound port.
    bool Start(const std::string &ip);
    void Stop();

    // The port the server actually bound (valid after a successful Start()).
    uint16_t Port() const
    {
        return port_;
    }

    // Record that ng_id's leader is node_id, so GetLeader resolves it.
    void SetLeader(uint32_t ng_id, uint32_t node_id);

    // --- HostMangerService RPC overrides (cc_generic_services signatures) ---

    // Node registration during Sharder::Init. Accept all registrations.
    void StartNode(::google::protobuf::RpcController *controller,
                   const ::txservice::remote::StartNodeRequest *request,
                   ::txservice::remote::StartNodeResponse *response,
                   ::google::protobuf::Closure *done) override;

    // NG leader resolution (via Sharder::UpdateLeader) during WaitClusterReady.
    void GetLeader(::google::protobuf::RpcController *controller,
                   const ::txservice::remote::GetLeaderRequest *request,
                   ::txservice::remote::GetLeaderResponse *response,
                   ::google::protobuf::Closure *done) override;

private:
    std::mutex mux_;
    std::map<uint32_t, uint32_t> ng_leader_;  // ng_id -> node_id
    brpc::Server server_;
    uint16_t port_{0};  // bound port, set by Start()
};
}  // namespace test
}  // namespace txservice
