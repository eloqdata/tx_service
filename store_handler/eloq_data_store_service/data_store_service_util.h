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

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace EloqDS
{
// Node status in DSS
enum DSShardStatus : uint8_t
{
    // Node status is unknown, can be dead or alive, need to check
    // Unknown = 0,
    // Node is online with read only mode
    ReadOnly = 1,
    // Node is online with read write mode
    ReadWrite = 2,
    // Node is closed
    Closed = 3,
    // Node is opening data store.
    Starting = 4

};

// TODO(liunyl): define error code

struct DSSNode
{
public:
    std::string host_name_{""};
    uint16_t port_{0};

    bool operator<(const DSSNode &other) const
    {
        // Define ordering logic (e.g., by node_id, then host_name, then port)
        if (host_name_ != other.host_name_)
            return host_name_ < other.host_name_;
        return port_ < other.port_;
    }

    bool operator==(const DSSNode &other) const
    {
        return host_name_ == other.host_name_ && port_ == other.port_;
    }

    bool operator!=(const DSSNode &other) const
    {
        return !(*this == other);
    }

    DSSNode() = default;
    DSSNode(const std::string &host_name, uint16_t port)
        : host_name_(host_name), port_(port)
    {
    }

    DSSNode(const DSSNode &other)
        : host_name_(other.host_name_), port_(other.port_)
    {
    }

    DSSNode &operator=(const DSSNode &other)
    {
        if (this != &other)
        {
            host_name_ = other.host_name_;
            port_ = other.port_;
        }
        return *this;
    }
};

// For grouping nodes within a node group, we can store them in a vector
struct DSShard
{
    DSShard() = default;
    DSShard(const DSShard &other)
        : shard_id_(other.shard_id_),
          nodes_(other.nodes_),
          status_(other.status_),
          version_(other.version_)
    {
    }

    DSShard &operator=(const DSShard &other)
    {
        if (this != &other)
        {
            shard_id_ = other.shard_id_;
            nodes_ = other.nodes_;
            status_ = other.status_;
            version_ = other.version_;
        }
        return *this;
    }

    int32_t shard_id_{0};
    // host:port pairs
    std::vector<DSSNode> nodes_;
    DSShardStatus status_{DSShardStatus::Closed};
    uint64_t version_{1};
};

struct ScanTuple
{
    ScanTuple(std::string &&key, std::string &&value, uint64_t ts, uint64_t ttl)
        : key_(std::move(key)), value_(std::move(value)), ts_(ts), ttl_(ttl)
    {
    }

    ScanTuple(const ScanTuple &rhs) = delete;
    ScanTuple(ScanTuple &&rhs) = default;

    std::string key_;
    std::string value_;
    uint64_t ts_;
    uint64_t ttl_;
};

}  // namespace EloqDS
