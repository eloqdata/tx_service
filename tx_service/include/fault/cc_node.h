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

#include <brpc/channel.h>

#include <cstdint>
#include <string>
#include <unordered_set>

#include "cc_request.pb.h"
#include "log_replay_service.h"
#include "sharder.h"

namespace txservice::fault
{
namespace txservice
{
class LocalCcShards;
}

/**
 * @brief A cc node is a member of a Raft group in which the leader holds a part
 * of distributed cc maps and the followers are empty. When a cc node becomes
 * the leader, either because the Raft group starts or because of a failover,
 * the leader recovers the committed, unflushed writes from all log groups,
 * before starting serving cc requests. On failover, the cc entries in the old
 * leader are invalidated. By default, given the port number of the local node,
 * the Raft group is on port_number+1.
 *
 */
class CcNode
{
public:
    CcNode(const uint32_t ng_id,
           const uint32_t node_id,
           LocalCcShards &local_shards,
           uint32_t log_group_cnt);

    /**
     * @brief Stops and joins the deferred-promotion waiter.
     *
     * The waiter polls shard state through `this`, so it must not outlive the
     * object. It checks the stop flag every iteration, so the join is bounded
     * by one poll interval rather than by the promotion timeout.
     */
    ~CcNode()
    {
        stop_deferred_promotion_.store(true, std::memory_order_release);
        if (deferred_promotion_thd_.joinable())
        {
            deferred_promotion_thd_.join();
        }
    }

    bool CheckLogGroupReplayFinished(uint32_t log_group_id, int64_t ng_term);

    void FinishLogGroupReplay(uint32_t log_group_id,
                              int64_t ng_term,
                              uint32_t latest_committed_txn_no,
                              uint64_t last_ckpt_ts);

    /**
     * Pin data of this node group if this ccnode is group leader.
     * Must be called in pair with UnpinData().
     *
     * @return leader term of this ccnode
     */
    int64_t PinData();

    int64_t StandbyPinData();

    /**
     * Unpin data of this node group so that ccmaps and catalogs can be cleared
     * if ccnode is no longer leader.
     * Must be called in pair with PinData().
     * waitToFinish: true - Wait until the UnpinData to finish
     */
    void UnpinData();

    bool UpdateCkptTs(uint64_t new_ckpt_ts);

    uint64_t GetCkptTs()
    {
        return last_ckpt_ts_.load(std::memory_order_relaxed);
    }

    bool OnLeaderStart(int64_t term,
                       uint64_t &replay_start_ts,
                       bool &retry,
                       uint32_t *next_leader_node = nullptr);
    bool OnLeaderStop(int64_t term);
    bool Failover(const std::string &target_host,
                  const uint16_t target_port,
                  std::string &error_message);

    void RecoverLeaderTerm(int64_t leader_term);
    // Only called when a standby node starts following
    void OnStartFollowing(uint32_t node_id, int64_t term, bool resubscribe);
    bool OnSnapshotReceived(const remote::OnSnapshotSyncedRequest *req);
    bool PromoteStandbyTermIfCandidate(int64_t standby_term);

private:
    /**
     * @brief The three kinds of outstanding replay work, counted separately.
     *
     * They are reported apart rather than summed because they fail for
     * different reasons, and a stuck recovery is only diagnosable if you know
     * which one is not moving: a parked drain means a page fetch is not
     * completing, buffered commands with no parked drain means nothing is
     * re-driving them (a version hole, say), and in-flight record fetches mean
     * the store has not answered yet.
     */
    struct ReplayWorkCounts
    {
        size_t parked_drains_{0};
        size_t buffered_cmds_{0};
        size_t record_fetches_{0};

        size_t Total() const
        {
            return parked_drains_ + buffered_cmds_ + record_fetches_;
        }
    };

    /**
     * @brief How many of this node group's entries have a buffered-command
     * drain waiting on a page fetch, summed across all shards.
     *
     * @return The count, zero when every replayed tail has been applied.
     * Read on each shard's own core via WaitableCc rather than racing the
     * shard-owned bookkeeping.
     */
    ReplayWorkCounts DrainBlockedCount() const;

    /**
     * @brief Names the keys whose replayed tail is still unapplied.
     *
     * @return A comma-separated list for logging, empty when nothing is
     * blocked. A count tells you recovery is stuck; this tells you on what.
     */
    std::string DescribeDrainBlocked() const;

    /**
     * @brief Describes every entry still holding buffered replayed/standby
     * commands, with each buffered head's version tuple (see
     * CcMap::DescribeBufferedCommands). Failure-path evidence for the
     * replay-stall FATAL: DescribeDrainBlocked covers only parked drains,
     * which are legitimately absent when the stuck work is buffered commands.
     *
     * @return A per-shard "core N: ..." list for logging, empty when nothing
     * is buffered anywhere.
     */
    std::string DescribeBufferedCommands() const;

    /** @brief Turns the candidate term into the real term, opening this node
     * group for service. */
    void PromoteAfterReplay(int64_t candidate_term);

    /** @brief Spawns the bthread that waits for replayed tails, then promotes.
     */
    void StartDeferredPromotion(int64_t candidate_term, size_t pending);

    /**
     * @brief Waits for every replayed tail to apply and then promotes.
     *
     * Polls with bthread_usleep backoff, abandons the promotion if the term
     * moves, and on deadline leaves the node a candidate rather than serving a
     * key that is missing acknowledged writes.
     */
    void AwaitDrainsAndPromote(int64_t candidate_term);

    void NotifyNewLeaderStart(uint32_t leader_ng_id, uint32_t leader_node_id);
    void SubscribePrimaryNode(uint32_t node_id, int64_t term, bool resubscribe);
    void ClearCcNodeGroupData();
    bool WaitForShardSyncWithRetry(const size_t shard_idx,
                                   const uint32_t target_node_id,
                                   std::string &error_message);

    //  CcNode belongs to node group: ng_id_.
    const uint32_t ng_id_;
    // CcNode is located on node: node_id_.
    const uint32_t node_id_;

    std::atomic<bool> is_processing_{false};
    std::atomic<int64_t> requested_subscribe_primary_term_{-1};

    std::atomic<uint64_t> last_ckpt_ts_;

    // number of threads currently accessing data of this node group, this node
    // group's data cannot be cleared unless pinning_threads_ is 0
    int pinning_threads_;
    std::mutex pinning_threads_mux_;
    std::condition_variable pinning_threads_cv_;

    LocalCcShards &local_cc_shards_;

    // recovered_log_groups_ records the log groups which have finished the
    // recovery.
    std::unordered_set<uint32_t> recovered_log_groups_;
    // recovery_mux_ is used to protect recovered_log_groups_,
    // since it will be updated by replay thread and raft service thread
    // concurrently.
    std::mutex recovery_mux_;

    // The deferred-promotion waiter (docs/08 §10) and its stop flag. Owned
    // rather than detached so teardown cannot leave a thread dereferencing a
    // destroyed CcNode.
    std::thread deferred_promotion_thd_;
    std::atomic<bool> stop_deferred_promotion_{false};

    uint32_t log_group_cnt_;
};

}  // namespace txservice::fault
