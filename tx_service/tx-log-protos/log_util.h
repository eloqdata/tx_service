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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace txlog
{
class LogUtil
{
public:
    static const uint16_t replication_cardinality = 3;

    LogUtil()
    {
    }

    struct RaftGroupConfig
    {
        explicit RaftGroupConfig(std::string group_name,
                                 std::string group_conf,
                                 std::vector<uint32_t> group_nodes)
        {
            group_name_ = std::move(group_name);
            group_conf_ = std::move(group_conf);
            group_nodes_ = std::move(group_nodes);
        }

        std::string group_name_;
        std::string group_conf_;
        std::vector<uint32_t> group_nodes_;
    };

    /*
     * @brief Get log group configuration from the log service ip/port list
     * return a pair of a map of log group raft config and the log group
     * replicate number
     *
     * Each raft log service node could belong to multiple log groups. In
     * current design, one log service node could serves at most two log groups.
     * For example, log service nodes includes n1,n2,n3,n4 and replica num = 3.
     * Then there are two log groups (n1,n2,n3), (n4,n1,n2). We have n1 and n2
     * belong to two log groups.
     */
    static std::pair<std::unordered_map<uint32_t, RaftGroupConfig>, uint32_t>
    GenerateLogRaftGroupConfig(
        const std::vector<std::string> &ip_list,
        const std::vector<uint16_t> &port_list,
        const uint32_t start_log_group_id,
        uint32_t log_group_replica_num = replication_cardinality)
    {
        if (log_group_replica_num > ip_list.size())
        {
            log_group_replica_num = ip_list.size();
        }

        std::unordered_map<uint32_t, RaftGroupConfig> log_raft_group_config;
        // the format of log_group_conf is like:
        // `ip1:port1:0,ip2:port2:0,ip3:port3:0` given log group cardinality
        // is 3. raft PeerId's idx should always be 0 since one ip:port pair
        // belongs to only one log group.
        std::string log_group_conf;
        // id of log group. It's different from node_group_id, which is for cc
        // node raft group. Based on the value replication_cardinality, whose
        // default value is 3. The number of log group is equal to
        // #node_group/3.

        uint32_t log_group_id = start_log_group_id;
        std::vector<uint32_t> log_group_nodes;
        for (size_t pos = 0; pos < ip_list.size(); pos += log_group_replica_num)
        {
            log_group_conf.clear();
            log_group_nodes.clear();

            for (size_t idx = 0; idx < log_group_replica_num; ++idx)
            {
                size_t nid = pos + idx;
                if (nid >= ip_list.size())
                {
                    nid -= ip_list.size();
                }

                log_group_nodes.push_back(nid);

                if (idx != 0)
                {
                    log_group_conf.append(",");
                }

                log_group_conf.append(ip_list.at(nid));
                log_group_conf.append(":");
                log_group_conf.append(std::to_string(port_list.at(nid)));
                // raft PeerId's idx should always be 0 since one ip:port pair
                // belongs to only one log group.
                log_group_conf.append(":0");
            }

            // Registers all log groups in the routing table. The routing table
            // is singleton. Assumes for now that the routing table is
            // thread-safe and different threads may update the leader of a log
            // group concurrently.
            std::string log_group_name("lg");
            log_group_name.append(std::to_string(log_group_id));

            log_raft_group_config.try_emplace(
                log_group_id, log_group_name, log_group_conf, log_group_nodes);

            ++log_group_id;
        }

        return std::make_pair(std::move(log_raft_group_config),
                              log_group_replica_num);
    }

    static void DumpLogRaftGroupConfig(
        std::pair<std::unordered_map<uint32_t, RaftGroupConfig>, uint32_t>
            &config)
    {
        DLOG(INFO) << "Dump LogRaftGroupConfig: ";
        for (const auto &[log_group_id, raft_group_config] : config.first)
        {
            std::string group_nodes;
            for (const auto &node : raft_group_config.group_nodes_)
            {
                group_nodes.append(std::to_string(node));
                group_nodes.append(":");
            }
            DLOG(INFO) << "log_group_id: " << log_group_id
                       << " ,group_name:  " << raft_group_config.group_name_
                       << " ,group_conf: " << raft_group_config.group_conf_
                       << " ,group_nodes: " << group_nodes;
        }
    }
};
}  // namespace txlog
