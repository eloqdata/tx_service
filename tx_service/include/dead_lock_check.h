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

#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "proto/cc_request.pb.h"

namespace txservice
{
const uint64_t MICRO_SECOND = 1000000;

class LocalCcShards;
struct CheckDeadLockResult;
struct CheckDeadLockCc;

struct LockNode
{
    LockNode(uint32_t nodeid, uint32_t coreid, uint64_t addr)
        : is_ccentry_addr(true),
          node_id(nodeid),
          core_id(coreid),
          ety_addr(addr)
    {
    }

    explicit LockNode(uint64_t txid)
        : is_ccentry_addr(false), node_id(0), tx_id(txid)
    {
    }

    bool operator==(const LockNode &other) const
    {
        assert(is_ccentry_addr == other.is_ccentry_addr);
        if (is_ccentry_addr)
        {
            return node_id == other.node_id && ety_addr == other.ety_addr;
        }
        else
        {
            return tx_id == other.tx_id;
        }
    }

    // false: txid; true: entry_address
    bool is_ccentry_addr;
    // Only valid when is_ccentry_addr=true, to save which node from for entry
    uint32_t node_id;
    // Only valid when is_ccentry_addr=true, to save which core from for entry
    uint32_t core_id;
    // To save txid or entry addr according is_ccentry_addr;
    union
    {
        uint64_t tx_id;
        uint64_t ety_addr;
    };
};

struct NeHash
{
    std::size_t operator()(const LockNode &ne) const
    {
        if (ne.is_ccentry_addr)
        {
            return std::hash<uint64_t>{}(ne.node_id ^ ne.ety_addr);
        }
        else
        {
            return std::hash<uint64_t>{}(ne.tx_id);
        }
    }
};

struct NeEqual
{
    bool operator()(const LockNode &lhs, const LockNode &rhs) const
    {
        return (lhs == rhs);
    }
};

struct LockNodeSet
{
    std::unordered_set<LockNode, NeHash, NeEqual> lock_node_set;
};

/**An edge is from a transaction that is waiting a ccentry to another
 * transaction that has locked the same ccentry*/
struct TxEdge
{
    TxEdge(uint64_t tx_wait, uint64_t tx_lock)
        : tx_wait_(tx_wait), tx_lock_(tx_lock)
    {
    }
    bool operator==(const TxEdge &other) const
    {
        return (tx_wait_ == other.tx_wait_ && tx_lock_ == other.tx_lock_);
    }

    uint64_t tx_wait_;
    uint64_t tx_lock_;
};

struct EdgeHash
{
    std::size_t operator()(const TxEdge &edge) const
    {
        return std::hash<uint64_t>{}(edge.tx_wait_ ^ edge.tx_lock_);
    }
};

struct EdgeEqual
{
    bool operator()(const TxEdge &lhs, const TxEdge &rhs) const
    {
        return (lhs == rhs);
    }
};

struct EdgeLess
{
    bool operator()(const TxEdge &lhs, const TxEdge &rhs) const
    {
        return (lhs.tx_wait_ == rhs.tx_wait_ ? lhs.tx_lock_ < rhs.tx_lock_
                                             : lhs.tx_wait_ < rhs.tx_wait_);
    }
};

namespace tr = txservice::remote;
class DeadLockCheck
{
public:
    explicit DeadLockCheck(LocalCcShards &local_shards);
    ~DeadLockCheck();
    static void MergeRemoteWaitingLockInfo(const tr::DeadLockResponse *rsp);
    static void MergeLocalWaitingLockInfo(const CheckDeadLockResult &dlres);
    static void SetStop()
    {
        if (inst_)
        {
            {
                std::unique_lock<std::mutex> lk(inst_->mutex_);
                inst_->stop_ = true;
                inst_->con_var_.notify_one();
            }
            if (inst_->thd_.joinable())
            {
                inst_->thd_.join();
            }
        }
    }
    static void Free()
    {
        delete inst_;
        inst_ = nullptr;
    }
    static void Init(LocalCcShards &local_shards)
    {
        inst_ = new DeadLockCheck(local_shards);
    }
    static void SetTimeInterval(uint64_t itval)
    {
        time_interval_ = itval * MICRO_SECOND;
    }
    static void UpdateCheckNodeId(uint32_t node_id);

    static void RequestCheck()
    {
        if (inst_)
        {
            std::unique_lock<std::mutex> lk(inst_->mutex_);
            inst_->requested_check_ = true;
            inst_->con_var_.notify_one();
        }
    }

    // Request check with throttling to prevent excessive deadlock checks
    // Global throttling: once per second for all transactions
    // Returns true if check was requested, false if throttled
    static bool RequestCheckWithThrottle(uint64_t tx_id);

protected:
    void Run();
    void GatherLockDependancy();
    std::vector<std::vector<TxEdge>> DetectDeadLock(
        std::map<TxEdge, int32_t, EdgeLess> &map_edge);
    void RemoveDeadTransaction(std::vector<std::vector<TxEdge>> &vct_dead);
    std::map<TxEdge, int32_t, EdgeLess> GenerateTxWaitGraph();

protected:
    static DeadLockCheck *inst_;
    // Time interval for dead lock check, microseconds
    static uint64_t time_interval_;
    // map for cc entry and its locked txids
    std::unordered_map<LockNode, LockNodeSet, NeHash, NeEqual>
        entry_locked_txid_map_;
    // the map for txid and the entrys it wants to acquire lock on
    std::unordered_map<LockNode, LockNodeSet, NeHash, NeEqual>
        txid_waited_entry_map_;
    // The map for txids and the number that locked entrys
    std::unordered_map<uint64_t, uint32_t> txid_ety_count_map_;
    // The related nodes have send back the result or not.
    std::unordered_map<uint32_t, bool> reply_map_;
    // The count of nodes without relay
    int32_t node_unfinished_ = 0;
    // The deadlock check round.
    uint64_t check_round_ = 0;

    std::thread thd_;
    // If process has been closed and this thread need to stop;
    bool stop_;

    // mutex_ and con_var_ are used to wait local node and remote nodes to
    // finish dead lock check and return the related cc entrys and tx ids.
    std::mutex mutex_;
    std::condition_variable con_var_;

    LocalCcShards &local_shards_;
    std::unique_ptr<CheckDeadLockCc> dead_lock_cc_;
    // The last time to check dead lock.
    uint64_t last_check_time_;
    // The node to rise dead lock check.
    uint32_t check_node_id_;
    // If the dead lock check is requested by this node.
    bool requested_check_{false};
    // Last throttled check request time (microseconds)
    // Used to throttle deadlock check requests to once per second globally for all transactions
    uint64_t last_throttled_check_time_{0};
    static constexpr uint64_t CHECK_THROTTLE_INTERVAL_ = 1 * MICRO_SECOND;  // 1 second
};
}  // namespace txservice
