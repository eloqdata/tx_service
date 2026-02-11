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
#include "dead_lock_check.h"

#include <stack>

#include "cc_request.h"
#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "sharder.h"

using namespace std::chrono_literals;

namespace txservice
{
DeadLockCheck *DeadLockCheck::inst_ = nullptr;
uint64_t DeadLockCheck::time_interval_ = 5 * 1000000;  // 5 seconds
CcRequestPool<AbortTransactionCc> abort_tran_pool;

DeadLockCheck::DeadLockCheck(LocalCcShards &local_shards)
    : reply_map_(),
      stop_(false),
      local_shards_(local_shards),
      dead_lock_cc_(new CheckDeadLockCc),
      check_node_id_(0)
{
    last_check_time_ = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    thd_ = std::thread([this] { Run(); });
    pthread_setname_np(thd_.native_handle(), "dead_lock_check");
}

DeadLockCheck::~DeadLockCheck()
{
}

void DeadLockCheck::MergeRemoteWaitingLockInfo(const tr::DeadLockResponse *rsp)
{
    uint32_t node_id = rsp->node_id();
    std::unique_lock<std::mutex> lock(inst_->mutex_);
    // check round match
    if (rsp->check_round() != inst_->check_round_)
    {
        return;
    }

    for (int i = 0; i < rsp->block_entry_size(); i++)
    {
        const tr::BlockEntry &be = rsp->block_entry(i);

        LockNode le(node_id, be.core_id(), be.entry());

        auto setid = inst_->entry_locked_txid_map_.try_emplace(le);
        for (int j = 0; j < be.locked_tids_size(); j++)
        {
            LockNode tx(be.locked_tids(j));
            setid.first->second.lock_node_set.insert(tx);
        }

        for (int j = 0; j < be.waited_tids_size(); j++)
        {
            LockNode tx(be.waited_tids(j));
            auto it = inst_->txid_waited_entry_map_.try_emplace(tx);
            it.first->second.lock_node_set.insert(le);
        }
    }

    for (int i = 0; i < rsp->tx_etys_size(); i++)
    {
        const tr::TxEntrys &te = rsp->tx_etys(i);
        auto it = inst_->txid_ety_count_map_.try_emplace(te.txid(), 0);
        it.first->second += te.ety_count();
    }

    inst_->reply_map_[node_id] = true;
    inst_->node_unfinished_--;

    if (inst_->node_unfinished_ == 0)
    {
        inst_->con_var_.notify_one();
    }
}

void DeadLockCheck::MergeLocalWaitingLockInfo(const CheckDeadLockResult &dlres)
{
    std::unique_lock<std::mutex> lock(inst_->mutex_);

    for (size_t i = 0; i < dlres.entry_lock_info_vec_.size(); i++)
    {
        auto &vcteti = dlres.entry_lock_info_vec_[i];
        for (auto &iter : vcteti)
        {
            LockNode le(
                inst_->local_shards_.NodeId(), (uint32_t) i, iter.first);
            auto setid = inst_->entry_locked_txid_map_.try_emplace(le);
            for (uint64_t txid : iter.second.lock_txids)
            {
                LockNode tx(txid);
                setid.first->second.lock_node_set.insert(tx);
            }
            for (uint64_t txid : iter.second.wait_txids)
            {
                LockNode tx(txid);
                auto it = inst_->txid_waited_entry_map_.try_emplace(tx);
                it.first->second.lock_node_set.insert(le);
            }
        }
    }

    for (auto &vcttx : dlres.txid_ety_lock_count_)
    {
        for (auto &iter : vcttx)
        {
            auto it = inst_->txid_ety_count_map_.try_emplace(iter.first, 0);
            it.first->second += iter.second;
        }
    }

    inst_->reply_map_[inst_->local_shards_.NodeId()] = true;
    inst_->node_unfinished_--;

    if (inst_->node_unfinished_ == 0)
    {
        inst_->con_var_.notify_one();
    }
}

void DeadLockCheck::UpdateCheckNodeId(uint32_t node_id)
{
    assert(inst_ != nullptr);
    std::unique_lock<std::mutex> lk(inst_->mutex_);
    DLOG(INFO) << "[Global dead lock detector]: update check node id to "
               << node_id;
    inst_->check_node_id_ = node_id;
    inst_->last_check_time_ = LocalCcShards::ClockTs();
}

void DeadLockCheck::GatherLockDependancy()
{
    std::unique_lock<std::mutex> lk(mutex_);
    DLOG(INFO) << "[Global dead lock detector]: gather lock "
                  "dependancy information";
    inst_->check_node_id_ = Sharder::Instance().NodeId();
    inst_->last_check_time_ = LocalCcShards::ClockTs();

    reply_map_.clear();
    auto all_node_groups = Sharder::Instance().AllNodeGroups();

    node_unfinished_ = 0;
    entry_locked_txid_map_.clear();
    txid_waited_entry_map_.clear();
    txid_ety_count_map_.clear();
    // pass round to remote node and match the round when processing response
    check_round_++;
    CheckDeadLockResult &local_result = dead_lock_cc_->GetDeadLockResult();

    // Send dead lock request to local and remote nodes
    for (uint32_t ng_id : *all_node_groups)
    {
        uint32_t node_id = Sharder::Instance().LeaderNodeId(ng_id);
        // If this node is leader of multiple ngs and we've already asked it,
        // no need to visit this node again.
        if (reply_map_.find(node_id) != reply_map_.end())
        {
            continue;
        }

        if (node_id == Sharder::Instance().NodeId())
        {
            reply_map_.try_emplace(node_id, false);
            local_result.Reset();
            for (size_t i = 0; i < local_shards_.Count(); i++)
            {
                local_shards_.EnqueueCcRequest(i, dead_lock_cc_.get());
            }
        }
        else
        {
            tr::CcMessage send_msg;
            send_msg.set_type(tr::CcMessage::MessageType::
                                  CcMessage_MessageType_DeadLockRequest);
            send_msg.set_tx_number(0);
            send_msg.set_handler_addr(0);
            send_msg.set_tx_term(0);
            send_msg.set_command_id(0);

            tr::DeadLockRequest *dl = send_msg.mutable_dead_lock_request();
            dl->set_src_node_id(Sharder::Instance().NodeId());
            dl->set_check_round(check_round_);
            auto send_res =
                Sharder::Instance().GetCcStreamSender()->SendMessageToNode(
                    node_id, send_msg);
            if (send_res.sent || send_res.queued_for_retry)
            {
                reply_map_.try_emplace(node_id, false);
            }
        }
    }
    node_unfinished_ = reply_map_.size();

    // Wait all nodes to finish dead check and return data. If exceed the half
    // of interval time, this time for dead lock will be neglect
    do
    {
        con_var_.wait_for(lk,
                          std::chrono::microseconds(time_interval_ / 2),
                          [&]()
                          {
                              return local_result.unfinish_count_.load(
                                         std::memory_order_relaxed) == 0 &&
                                         node_unfinished_ == 0 ||
                                     stop_;
                          });
    } while (local_result.unfinish_count_.load(std::memory_order_relaxed) !=
                 0 &&
             !stop_);

    if (stop_)
    {
        return;
    }

    if (node_unfinished_ > 0)
    {
        LOG(INFO) << "[Global dead lock detector]: fails to receive lock "
                     "waiting information from node. Failed nodes count: "
                  << node_unfinished_;
        return;
    }

    std::map<TxEdge, int32_t, EdgeLess> map_edge = GenerateTxWaitGraph();
    std::vector<std::vector<TxEdge>> vct_dead = DetectDeadLock(map_edge);

    RemoveDeadTransaction(vct_dead);
}

// This method will preprocess the data that collected from all nodes. The
// origin data shows the releations between txid and ccentry. One relation are
// ccentry and the txids that have locked this ccentry. The other relation are
// txids and its waited ccentry. This method will convert the relations between
// txids and ccentrys to edges that txids that have locked the ccentrys and the
// other txid that is waiting the ccentrys.
// Every edge is a relation from a transaction that is waiting a ccentry to
// other transaction that has locked the same ccentry.
// For the reurn map, the first paramter is the edge itself, the second
// parameter is to show if this edge has been visited in traverse.
std::map<TxEdge, int32_t, EdgeLess> DeadLockCheck::GenerateTxWaitGraph()
{
    std::map<TxEdge, int32_t, EdgeLess> map_edge;
    for (auto it_wait = txid_waited_entry_map_.begin();
         it_wait != txid_waited_entry_map_.end();
         it_wait++)
    {
        const LockNode &tx_waiting_lock = it_wait->first;
        for (auto &it_ety : it_wait->second.lock_node_set)
        {
            auto it_lock_set = entry_locked_txid_map_.find(it_ety);
            if (it_lock_set == entry_locked_txid_map_.end())
            {
                continue;
            }

            for (auto &tx_holding_lock : it_lock_set->second.lock_node_set)
            {
                // Some time a transaction need to upgrade lock from read to
                // write intend or from write intent to write. If it is blocked
                // by other lock, it will be added into block queue of the
                // ccentry. If not except this case, it will generate a circle
                // from this transaction to this transaction.
                if (tx_waiting_lock == tx_holding_lock)
                {
                    continue;
                }

                map_edge.emplace(
                    TxEdge(tx_waiting_lock.tx_id, tx_holding_lock.tx_id), -1);
            }
        }
    }

    return map_edge;
}

// Traverse the wait-for graph to detect deadlock cycles using depth-first
// search.
//
// In the transaction semantics:
// - A "TxEdge" represents a waiting relationship where:
//   tx_wait_ = ID of transaction that is waiting to acquire a lock on an entry
//   tx_lock_ = ID of transaction that currently holds the lock on that entry
// - The wait-for graph maps these relationships where an edge from tx_wait_ to
//   tx_lock_ means "transaction tx_wait_ is waiting for a lock held by
//   transaction tx_lock_"
// - A cycle in this graph represents a deadlock situation
std::vector<std::vector<TxEdge>> DeadLockCheck::DetectDeadLock(
    std::map<TxEdge, int32_t, EdgeLess> &graph)
{
    // Iterator type for edges
    using EdgeIt = std::map<TxEdge, int32_t, EdgeLess>::iterator;

    int visit_round = 1;
    std::vector<std::vector<TxEdge>> cycles;

    // Try each edge in the wait-for graph as a DFS root
    for (EdgeIt root_it = graph.begin(); root_it != graph.end(); ++root_it)
    {
        // Unpack the edge and its visit marker
        const TxEdge &root_edge = root_it->first;
        int32_t &root_visit_mark = root_it->second;

        // Skip if already visited in a previous round
        if (root_visit_mark > 0)
        {
            continue;
        }

        // Mark this edge in the current DFS round
        root_visit_mark = visit_round;

        // This tracks the chain of lock dependencies we're exploring
        std::unordered_set<TxEdge, EdgeHash, EdgeEqual> visited_path;
        visited_path.insert(root_edge);
        std::vector<EdgeIt> dfs_stack{root_it};

        // Start exploring children whose tx_wait_ matches this tx_lock_
        uint64_t cur_node = root_edge.tx_lock_;
        EdgeIt child_it = graph.lower_bound(TxEdge(cur_node, 0));

        // Perform explicit DFS
        while (!dfs_stack.empty())
        {
            // If no valid child edge, backtrack
            if (child_it == graph.end() || child_it->first.tx_wait_ != cur_node)
            {
                // Pop last edge from DFS stack
                EdgeIt popped_it = dfs_stack.back();
                dfs_stack.pop_back();
                visited_path.erase(popped_it->first);

                // If stack empty, this DFS session ends
                if (dfs_stack.empty())
                {
                    break;
                }

                // Move up to parent and resume
                EdgeIt parent_it = dfs_stack.back();
                cur_node = parent_it->first.tx_wait_;
                child_it = parent_it;
                ++child_it;
                continue;
            }

            // Process current child edge
            const TxEdge &child_edge = child_it->first;
            int32_t &child_visit_mark = child_it->second;

            if (child_visit_mark == visit_round)
            {
                // Back-edge within this round -> potential cycle
                if (visited_path.count(child_edge))
                {
                    // Build the detected cycle
                    std::vector<TxEdge> cycle{child_edge};
                    for (auto path_it = dfs_stack.rbegin();
                         path_it != dfs_stack.rend();
                         ++path_it)
                    {
                        TxEdge path_edge = (*path_it)->first;
                        if (path_edge == child_edge)
                        {
                            break;
                        }
                        cycle.push_back(path_edge);
                    }
                    cycles.push_back(cycle);
                }
                ++child_it;
            }
            else if (child_visit_mark > 0)
            {
                // Already handled in a previous round, skip
                ++child_it;
            }
            else
            {
                // First visit in this round: descend deeper
                child_visit_mark = visit_round;
                visited_path.insert(child_edge);
                dfs_stack.push_back(child_it);
                cur_node = child_edge.tx_lock_;
                child_it = graph.lower_bound(TxEdge(cur_node, 0));
            }
        }

        // Move to the next DFS round
        ++visit_round;
    }

    return cycles;
}

void DeadLockCheck::RemoveDeadTransaction(
    std::vector<std::vector<TxEdge>> &vct_dead)
{
    for (std::vector<TxEdge> &deadlock_edges : vct_dead)
    {
        uint64_t tx_id = 0;
        uint32_t min_ety = UINT32_MAX;

        for (TxEdge &edge : deadlock_edges)
        {
            auto it = txid_ety_count_map_.find(edge.tx_wait_);
            if (it == txid_ety_count_map_.end())
            {
                LOG(ERROR) << "txid_ety_count_map_ not found tx_id:"
                           << edge.tx_wait_;
                assert(false);
                continue;
            }

            uint32_t cnt = it->second;
            if (cnt < min_ety)
            {
                min_ety = cnt;
                tx_id = edge.tx_wait_;
            }
            // This brance to ensure the small tx id to be abort and test case
            // will not fail due to uncertainty
            else if (cnt == min_ety && tx_id > edge.tx_wait_)
            {
                tx_id = edge.tx_wait_;
            }
        }

        LockNode le(tx_id);
        LockNodeSet &lety = txid_waited_entry_map_.find(le)->second;
        for (const LockNode &lent : lety.lock_node_set)
        {
            LOG(INFO) << "Deadlock detected between tx_id_lock:"
                      << entry_locked_txid_map_.find(lent)
                             ->second.lock_node_set.begin()
                             ->tx_id
                      << " and tx_id_wait:" << tx_id
                      << ", going to abort transaction: " << tx_id;

            if (lent.node_id == Sharder::Instance().NodeId())
            {
                AbortTransactionCc *atcc = abort_tran_pool.NextRequest();
                atcc->Reset(lent.ety_addr,
                            entry_locked_txid_map_.find(lent)
                                ->second.lock_node_set.begin()
                                ->tx_id,
                            tx_id,
                            lent.node_id);
                local_shards_.EnqueueCcRequest(lent.core_id, atcc);
            }
            else
            {
                tr::CcMessage send_msg;

                send_msg.set_type(
                    tr::CcMessage::MessageType::
                        CcMessage_MessageType_AbortTransactionRequest);
                send_msg.set_tx_number(0);
                send_msg.set_handler_addr(0);
                send_msg.set_tx_term(0);
                send_msg.set_command_id(0);

                tr::AbortTransactionRequest *atreq =
                    send_msg.mutable_abort_tran_req();
                atreq->set_src_node_id(Sharder::Instance().NodeId());
                atreq->set_node_id(lent.node_id);
                atreq->set_core_id(lent.core_id);
                atreq->set_entry(lent.ety_addr);
                atreq->set_wait_txid(tx_id);
                atreq->set_lock_txid(entry_locked_txid_map_.find(lent)
                                         ->second.lock_node_set.begin()
                                         ->tx_id);

                Sharder::Instance().GetCcStreamSender()->SendMessageToNode(
                    lent.node_id, send_msg);
            }
        }
    }
}

void DeadLockCheck::Run()
{
    // Wait until LocalCcShards thread has started. Or it will maybe make crash.
    while (LocalCcShards::ClockTs() < last_check_time_)
    {
        std::unique_lock<std::mutex> lk(mutex_);
        con_var_.wait_for(lk, 10s, [this]() { return stop_; });
    }

    while (!stop_)
    {
        CODE_FAULT_INJECTOR("trigger_dead_lock_detection", {
            FaultInject::Instance().InjectFault("trigger_dead_lock_detection",
                                                "remove");
            GatherLockDependancy();
        });

        std::unique_lock<std::mutex> lk(mutex_);
        con_var_.wait_for(
            lk,
            1s,
            [this]()
            {
                if (stop_)
                {
                    return true;
                }

                if (requested_check_.load(std::memory_order_acquire))
                {
                    uint64_t ival = LocalCcShards::ClockTs() - last_check_time_;
                    if (ival >= time_interval_)
                    {
                        return true;
                    }
                }

                return false;
            });

        if (stop_)
        {
            continue;
        }

        if (Sharder::Instance().LeaderTerm(
                Sharder::Instance().NativeNodeGroup()) < 0)
        {
            continue;
        }

        if (!requested_check_.load(std::memory_order_acquire))
        {
            continue;
        }
        else if (LocalCcShards::ClockTs() - last_check_time_ < time_interval_)
        {
            continue;
        }

        // Reset the check flag before gather lock dependancy
        requested_check_.store(false, std::memory_order_release);
        lk.unlock();
        GatherLockDependancy();
    }
}
}  // namespace txservice
