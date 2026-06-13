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

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// Shared port-reservation helpers used by both the Phase 1 single-node fixture
// (harness/test_node.cpp) and the Phase 2 cluster driver (cluster/
// test_cluster.cpp). The two reserve ephemeral ports the same way -- probing on
// INADDR_ANY (matching how brpc binds the DSS / cc-node / log-replay servers)
// -- so the logic lives here once instead of being copied per-caller.

namespace txservice
{
namespace test
{
// Header-only utilities; mark inline so multiple translation units can include
// this without violating the ODR.

// Binds an ephemeral TCP port on INADDR_ANY and returns (fd, port) without
// closing the socket, so the kernel will not hand the same port to a later
// call. Caller must close the fd once it has claimed the port. Throws on a hard
// socket-layer failure.
inline std::pair<int, uint16_t> BindEphemeralPort()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        throw std::runtime_error("BindEphemeralPort: socket");
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    // Probe on INADDR_ANY (0.0.0.0), matching how brpc binds the DSS/cc-node/
    // log-replay servers. Probing loopback-only would not accurately reserve
    // what the servers need (a port free on 127.0.0.1 can still be unbindable
    // on 0.0.0.0), causing spurious "Fail to listen" under port pressure.
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = 0;  // let the kernel choose
    if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0)
    {
        ::close(fd);
        throw std::runtime_error("BindEphemeralPort: bind");
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0)
    {
        ::close(fd);
        throw std::runtime_error("BindEphemeralPort: getsockname");
    }
    return {fd, ntohs(addr.sin_port)};
}

// Tries to bind a specific port on INADDR_ANY (matching the servers; see
// BindEphemeralPort). Returns the open fd on success or -1.
inline int TryBindPort(uint16_t port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0)
    {
        ::close(fd);
        return -1;
    }
    return fd;
}

// The TxService derives several ports from a single base: cc-stream = base,
// cc-node = base + 1, log-replay = base + 3 (see GET_CCNODE_RPC_PORT /
// GET_LOG_REPLAY_RPC_PORT in sharder.h). A free-port picker that only reserves
// the base is not enough -- base+1 / base+3 must be free too, or
// TxService::Start() fails to listen. This finds a base whose whole +0..+3
// window is simultaneously bindable, holding every socket open while probing so
// the kernel cannot reissue one of them mid-window.
inline uint16_t ReserveTxPortWindow()
{
    constexpr int kWindow = 4;  // base .. base+3
    constexpr int kMaxAttempts = 64;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt)
    {
        auto [base_fd, base] = BindEphemeralPort();
        // Avoid wrapping uint16 at the top of the range.
        if (base > 65535 - kWindow)
        {
            ::close(base_fd);
            continue;
        }
        std::vector<int> fds{base_fd};
        bool ok = true;
        for (int off = 1; off < kWindow; ++off)
        {
            int fd = TryBindPort(static_cast<uint16_t>(base + off));
            if (fd < 0)
            {
                ok = false;
                break;
            }
            fds.push_back(fd);
        }
        for (int fd : fds)
        {
            ::close(fd);
        }
        if (ok)
        {
            return base;
        }
    }
    // Extremely unlikely; let the caller's Start() surface the failure.
    auto [fd, port] = BindEphemeralPort();
    ::close(fd);
    return port;
}
}  // namespace test
}  // namespace txservice
