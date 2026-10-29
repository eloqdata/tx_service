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
// Let Catch provide main():
// clang-format off
// log_agent.h first: it includes braft and brpc in the order their logging
// setup needs, and include sorting would move it after Catch2.
#include "log_agent.h"

#include <atomic>
#include <catch2/catch_all.hpp>
#include <chrono>
#include <cstdint>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "log.pb.h"
// clang-format on

/**
 * @file
 * @brief The LogAgent side of change data capture: CdcScan on the cached
 * leader, which reports the node ID it sends to, and LogNodeByAddress, which
 * looks up the node ID of a NotLeader redirect.
 *
 * No log server runs. The agent is configured with one group of three nodes
 * on ports nothing listens on, so the leader cache is exercised as pure
 * bookkeeping and a scan can only fail; what the scan tests pin down is that
 * its closure runs exactly once, as an asynchronous caller relies on.
 */

namespace
{
/// Ports of the three configured nodes; nothing listens on them.
const std::vector<uint16_t> kPorts = {1, 2, 3};

/**
 * @brief Initializes a LogAgent with one log group, lg0, of three nodes on
 * 127.0.0.1 and kPorts; node ids are 0, 1 and 2, and the cache starts at 0.
 *
 * @param[in,out] agent The agent to initialize.
 */
void InitAgent(txlog::LogAgent &agent)
{
    std::vector<std::string> ips(kPorts.size(), "127.0.0.1");
    std::vector<uint16_t> ports = kPorts;
    agent.Init(ips, ports, 0, static_cast<uint32_t>(kPorts.size()));
}

/**
 * @brief A closure that counts its runs and signals the first.
 */
class CountingClosure : public google::protobuf::Closure
{
public:
    void Run() override
    {
        if (runs_.fetch_add(1) == 0)
        {
            ran_.set_value();
        }
    }

    /**
     * @brief Waits for the first run.
     *
     * @return true if the closure ran within the timeout.
     */
    bool WaitForRun()
    {
        return ran_future_.wait_for(std::chrono::seconds(10)) ==
               std::future_status::ready;
    }

    std::atomic<int> runs_{0};  ///< How many times Run() was called.

private:
    std::promise<void> ran_;
    std::future<void> ran_future_{ran_.get_future()};
};
}  // namespace

TEST_CASE(
    "LogNodeByAddress finds the group's node ID configured with that address",
    "[LogAgentCdc]")
{
    txlog::LogAgent agent;
    InitAgent(agent);

    REQUIRE(agent.LogNodeByAddress(0, "127.0.0.1:1") == 0u);
    REQUIRE(agent.LogNodeByAddress(0, "127.0.0.1:2") == 1u);
    REQUIRE(agent.LogNodeByAddress(0, "127.0.0.1:3") == 2u);
    // A lookup only: following the node is the caller's UpdateLeaderCache.
    REQUIRE(agent.TEST_LogGroupLeaderNode(0) == 0);
}

TEST_CASE("LogNodeByAddress finds nothing for an address it cannot match",
          "[LogAgentCdc]")
{
    txlog::LogAgent agent;
    InitAgent(agent);

    const std::vector<std::string> unmatched = {
        "",                 // no leader known
        "127.0.0.2:2",      // another host
        "127.0.0.1:4",      // a port no node has
        "127.0.0.1",        // no port
        ":2",               // no host
        "127.0.0.1:",       // empty port
        "127.0.0.1:2x",     // not a number
        "127.0.0.1:65538",  // past 16 bits; wraps to 2 if parsed naively
    };
    for (const std::string &address : unmatched)
    {
        INFO("address " << address);
        REQUIRE_FALSE(agent.LogNodeByAddress(0, address).has_value());
    }

    // A group the agent does not know.
    REQUIRE_FALSE(agent.LogNodeByAddress(7, "127.0.0.1:3").has_value());
}

TEST_CASE(
    "CdcScan on an unknown group fails and runs its closure once, "
    "before returning",
    "[LogAgentCdc]")
{
    txlog::LogAgent agent;
    InitAgent(agent);

    brpc::Controller cntl;
    txlog::CdcScanRequest request;
    request.set_log_group_id(7);
    txlog::CdcScanResponse response;
    std::optional<uint32_t> node_id;
    CountingClosure done;
    agent.CdcScan(7, &cntl, &request, &response, &node_id, &done);

    REQUIRE(done.runs_.load() == 1);
    REQUIRE(cntl.Failed());
    // Nothing was sent, so no node is reported.
    REQUIRE_FALSE(node_id.has_value());
}

TEST_CASE(
    "CdcScan reports the cached leader it sends to, and runs its closure "
    "once when the leader is unreachable",
    "[LogAgentCdc]")
{
    txlog::LogAgent agent;
    InitAgent(agent);
    agent.UpdateLeaderCache(0, 2);

    brpc::Controller cntl;
    cntl.set_timeout_ms(2000);
    cntl.set_max_retry(0);
    txlog::CdcScanRequest request;
    request.set_log_group_id(0);
    request.set_from_ts(1);
    txlog::CdcScanResponse response;
    std::optional<uint32_t> node_id;
    CountingClosure done;
    agent.CdcScan(0, &cntl, &request, &response, &node_id, &done);

    REQUIRE(done.WaitForRun());
    REQUIRE(node_id == 2u);
    REQUIRE(cntl.Failed());
    // A second run would be a double completion of the caller's state.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    REQUIRE(done.runs_.load() == 1);
}
