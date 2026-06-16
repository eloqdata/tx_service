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
#include <braft/raft.h>
#include <braft/route_table.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>

#include <atomic>
#include <condition_variable>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "log.pb.h"
#include "log_util.h"
namespace txlog
{
/*
 * LogAgent is used to send request to tx log service.
 * Sending request contains three steps:
 * 1. find current raft group leader to send the request in cache group_stub_.
 * 2. if failed to find in cache, then call RefreshLeader to get the new leader
 * and fill the cache again.
 * 3. send the request to group leader asynchronously.
 * 4. TODO: redirect the request to the new leader if previous send request
 * fails.
 *
 * Notice: since cache exists, it not safe in multi-thread environment. As a
 * result, we clone LogAgent for each thread.
 */
class LogAgent
{
public:
    static const uint16_t replication_cardinality = 3;

    LogAgent(const LogAgent &rhs) = delete;

    LogAgent() : log_group_cnt_(0)
    {
        check_leader_thd_ = std::thread([this] { CheckLeaderRun(); });
    }

    ~LogAgent()
    {
        {
            std::unique_lock lk(check_leader_mutex_);
            check_leader_terminate_ = true;
            check_leader_cv_.notify_one();
        }
        check_leader_thd_.join();
    }

    /**
     * @brief initialize channel map and stub map.
     */
    void Init(std::vector<std::string> &ips,
              std::vector<uint16_t> &ports,
              const uint32_t start_log_group_id,
              const uint32_t log_group_replica_num = replication_cardinality);

    /*
     * Get the new leader from raft group given log_group_id.
     * Refresh the cache with the new leader information.
     */
    int RefreshLeader(uint32_t log_group_id, int timeout_ms = 1000);

    /*
     * Request the background check leader thread to refresh leader.
     */
    void RequestRefreshLeader(uint32_t log_group_id);

    /*
     * WriteLog request append new log record.
     */
    void WriteLog(uint32_t log_group_id,
                  brpc::Controller *controller,
                  const LogRequest *request,
                  LogResponse *response,
                  ::google::protobuf::Closure *done);

    void CheckMigrationIsFinished(
        uint32_t log_group_id,
        brpc::Controller *controller,
        const CheckMigrationIsFinishedRequest *request,
        CheckMigrationIsFinishedResponse *response,
        ::google::protobuf::Closure *done);

    CheckClusterScaleStatusResponse::Status CheckClusterScaleStatus(
        uint32_t log_group_id, const std::string &id);

    /*
     * UpdateCheckpointTs request informs all log groups of this node
     * group's checkpoint timestamp. Triggerred by checkpointer thread.
     */
    void UpdateCheckpointTs(uint32_t cc_node_group_id,
                            int64_t term,
                            uint64_t checkpoint_timestamp);

    /*
     * Pushes this cc node's term to all log groups. The method is invoked when
     * a cc node group is elected as the leader and is about to serve
     * concurrency control requests. The method and all its internal RPC calls
     * are synchronous, because the cc node cannot start serving until all log
     * groups are notified and re-install their un-checkpointed writes in the cc
     * node's cc maps which is typically the so-called replay process.
     * If log_group >= 0, send ReplayLogRequest to specified log_group, else
     * send to all log groups.
     */
    void ReplayLog(uint32_t cc_node_group_id,
                   int64_t term,
                   const std::string &source_ip,
                   uint16_t source_port,
                   int log_group,
                   uint64_t start_ts,
                   std::atomic<bool> &interrupt,
                   bool no_replay = false);

    /*
     * RemoveCcNodeGroup request informs all log groups of this node
     * group is removed in cluster.
     */
    void RemoveCcNodeGroup(uint32_t cc_node_group_id, int64_t term);

    /*
     * @brief Recovery transaction when failover happens, e.g. the orphan lock.
     */
    RecoverTxResponse_TxStatus RecoverTx(uint64_t tx_number,
                                         int64_t tx_term,
                                         uint64_t write_lock_ts,
                                         uint32_t cc_ng_id,
                                         int64_t cc_ng_term,
                                         const std::string &source_ip,
                                         uint16_t source_port,
                                         uint32_t log_group_id);
    /**
     * @brief Transfer the leader for a log group (id=log_group_id) to node
     * (id=leader_id)
     */
    void TransferLeader(uint32_t log_group_id, uint32_t leader_idx);

    /*
     * @brief The total number of log groups in the log service.
     * Typically the number of log nodes = LogGroupCount() *
     * LogGroupReplicaNum()
     */
    uint32_t LogGroupCount()
    {
        std::shared_lock<std::shared_mutex> lock(config_map_mutex_);
        return log_group_cnt_;
    }

    uint32_t LogGroupId(uint32_t log_group_idx)
    {
        std::shared_lock<std::shared_mutex> lock(config_map_mutex_);
        assert(log_group_idx < log_group_ids_.size());
        return log_group_ids_[log_group_idx];
    }

    /**
     * @brief The number of log group replica. Default value is 3, which is
     * defined in log_server.h.
     */
    uint32_t LogGroupReplicaNum()
    {
        std::shared_lock<std::shared_mutex> lock(config_map_mutex_);
        // TODO(liunyl): For now only accommodate the case for 1 log group.
        // We need to pass in the exact log group configuration in the future.
        return log_group_replica_num_;
    }

    /**
     * @brief Get the leader node id of the log group.(This is only for test
     * now)
     *
     * @param log_group_id
     * @return the leader node id of the log group.
     */
    uint32_t TEST_LogGroupLeaderNode(uint32_t log_group_id)
    {
        const std::atomic<uint32_t> &leader_node_id =
            lg_leader_cache_[log_group_id];
        return leader_node_id.load(std::memory_order_acquire);
    }

    std::shared_ptr<brpc::Channel> GetChannel(uint32_t log_group_id);

    void UpdateLeaderCache(uint32_t lg_id, uint32_t node_id);

    bool UpdateLogGroupConfig(std::vector<std::string> &ips,
                              std::vector<uint16_t> &ports,
                              uint32_t log_group_id);

private:
    void CheckLeaderRun();

    std::shared_mutex config_map_mutex_;
    std::unordered_map<uint32_t, std::pair<std::string, uint16_t>> log_nodes_;

    uint32_t log_group_cnt_;
    uint32_t log_group_replica_num_;
    std::vector<uint32_t> log_group_ids_;
    std::unordered_map<uint32_t, LogUtil::RaftGroupConfig>
        log_group_config_map_;

    std::unordered_map<uint32_t, std::shared_ptr<brpc::Channel>>
        log_channel_map_;

    // leader cache of log group: map key is log_group_id, map value is the
    // node_id.
    std::unordered_map<uint32_t, std::atomic<uint32_t>> lg_leader_cache_;

    // The background thread that periodically check the new log group leader.
    std::thread check_leader_thd_;
    int16_t request_check_group_leader_{-1};
    bool check_leader_terminate_{false};
    std::mutex check_leader_mutex_;
    std::condition_variable check_leader_cv_;
};
}  // namespace txlog
