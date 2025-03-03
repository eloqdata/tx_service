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
uint64_t DeadLockCheck::time_interval_ = 5 * 1000000;
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
}

DeadLockCheck::~DeadLockCheck()
{
}

void DeadLockCheck::MergeRemoteWaitingLockInfo(const tr::DeadLockResponse *rsp)
{
    uint32_t node_id = rsp->node_id();
    std::unique_lock<std::mutex> lock(inst_->mutex_);

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
    if (inst_->check_node_id_ > node_id ||
        LocalCcShards::ClockTs() - inst_->last_check_time_ > time_interval_ * 5)
    {
        inst_->check_node_id_ = node_id;
    }

    if (inst_->check_node_id_ == node_id)
    {
        inst_->last_check_time_ = LocalCcShards::ClockTs();
    }
}

void DeadLockCheck::GatherLockDependancy()
{
    std::unique_lock<std::mutex> lk(mutex_);
    UpdateCheckNodeId(Sharder::Instance().NodeId());
    reply_map_.clear();
    auto all_node_groups = Sharder::Instance().AllNodeGroups();

    node_unfinished_ = 0;
    entry_locked_txid_map_.clear();
    txid_waited_entry_map_.clear();
    txid_ety_count_map_.clear();

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
            dead_lock_cc_->GetDeadLockResult().Reset();
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
            bool hr =
                Sharder::Instance().GetCcStreamSender()->SendMessageToNode(
                    node_id, send_msg);
            if (hr)
            {
                reply_map_.try_emplace(node_id, false);
            }
        }
    }
    node_unfinished_ = reply_map_.size();

    // Wait all nodes to finish dead check and return data. If exceed the half
    // of interval time, this time for dead lock will be neglect
    con_var_.wait_for(lk,
                      std::chrono::microseconds(time_interval_ / 2),
                      [this]() { return node_unfinished_ == 0 || stop_; });

    if (stop_)
    {
        return;
    }

    if (node_unfinished_ > 0)
    {
        LOG(INFO) << "[Global dead lock detector]: fails to receive lock "
                     "waiting information from node. Failed nodes: "
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

// This method will traverse all edge and find if there has circle from one edge
// to other edge and reback to the visited edge.
// The input map is the edges and the sign that show if the edge has been
// visited.
// The return value is the multi groups of edges that make the dead lock circle.
std::vector<std::vector<TxEdge>> DeadLockCheck::DetectDeadLock(
    std::map<TxEdge, int32_t, EdgeLess> &map_edge)
{
    // iround used to show it is which time to traverse map_edge from the first
    // edge to last edge. this value will save it into the second parameter in
    // map_edge. Its usage is to show if the edge has been visited at his time.
    // If not, it can not be a dead lock cycle. If same, it will judge it has
    // multi paths or dead lock cycle.
    int32_t iround = 1;
    std::vector<std::vector<TxEdge>> vct_v_edge;
    for (auto it_edge = map_edge.begin(); it_edge != map_edge.end(); it_edge++)
    {
        // If this edge has been visited, continue.
        if (it_edge->second > 0)
        {
            continue;
        }

        // To Judge if here has circle or only it has multi paths from an edge
        // to other edges.
        std::unordered_set<TxEdge, EdgeHash, EdgeEqual> set_dup;
        // Here the traverse is depth-first, this vector will be used as stack
        // and save the iterators of every layer. It will got to the depth layer
        // until no edges or visited edges. Then reback upper layer and move to
        // next edge until all related edges have been visited.
        std::vector<std::map<TxEdge, int32_t, EdgeLess>::iterator> vct_iter;
        it_edge->second = iround;
        // Push the first layer
        vct_iter.push_back(it_edge);
        set_dup.insert(it_edge->first);
        // The current layer's locked transaction will be as the waiting
        // transaction of next layer.
        TxNumber tx_id = it_edge->first.tx_lock_;
        // Get the first edge's iterator of its waiting txid = tx_id
        auto iter = map_edge.lower_bound(TxEdge(tx_id, 0));

        while (true)
        {
            // If the iterator has go to the map's end or edge's waiting txid
            // has not equal tx_id, it mean it has visited all related edges
            // with tx_id and need to go to upper layer.
            if (iter == map_edge.end() || iter->first.tx_wait_ != tx_id)
            {
                vct_iter.pop_back();
                if (vct_iter.size() <= 1)
                {
                    break;
                }

                iter = *vct_iter.rbegin();
                set_dup.erase(iter->first);
                tx_id = iter->first.tx_wait_;
                iter++;
                continue;
            }

            // iter->second saved which time to visit this edge, if it is not
            // equal iround, it means that it has been visited previous. If
            // iter->second < 0, it means it is a new edge and not visit.
            if (iter->second == iround)
            {
                // It maybe has multi paths from one edge to other edge, so if
                // it meet visited edge with same sign, it does not mean here
                // has dead lock circle. This judgement will dicide is is only
                // multi paths or dead lock cycle.
                if (set_dup.find(iter->first) == set_dup.end())
                {
                    // Due to it is not circle, it will move next edge and judge
                    // again, do not need go to next layer.
                    iter++;
                }
                else
                {
                    // If find current edge from set_dup, it will make sure here
                    // has dead lock cycle. Then here will copy the edges into a
                    // v_e from end of vct_iter, until meet the current edge.
                    // All edges that make up the cycle will be saved into v_e.
                    std::vector<TxEdge> v_e;
                    v_e.push_back(iter->first);
                    for (auto it = vct_iter.rbegin(); it != vct_iter.rend();
                         it++)
                    {
                        if ((*it)->first == iter->first)
                        {
                            break;
                        }

                        v_e.push_back((*it)->first);
                    }

                    vct_v_edge.push_back(v_e);
                    // Save the cycle and move next edge.
                    iter++;
                    continue;
                }
            }
            else if (iter->second > 0)
            {
                // This edge has been visited previous time, so it does not need
                // to go to deep layer.
                iter++;
            }
            else
            {
                // Save current layer's information into set_dup for dead lock
                // cycle check and vct_iter and move into
                // next layer.
                iter->second = iround;
                set_dup.insert(iter->first);
                vct_iter.push_back(iter);
                tx_id = iter->first.tx_lock_;
                iter = map_edge.lower_bound(TxEdge(tx_id, 0));
            }
        }

        // Next traverse, counter++
        iround++;
    }

    return vct_v_edge;
}

void DeadLockCheck::RemoveDeadTransaction(
    std::vector<std::vector<TxEdge>> &vct_dead)
{
    for (std::vector<TxEdge> &dead : vct_dead)
    {
        uint64_t tx_id = 0;
        uint32_t min_ety = UINT32_MAX;

        for (TxEdge &edge : dead)
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
            lk, 1s, [this]() { return stop_ || requested_check_; });

        // Proceeds on ival >= interval OR requested_check_
        uint64_t ival = LocalCcShards::ClockTs() - last_check_time_;
        if (stop_ || (ival < time_interval_ && !requested_check_))
        {
            continue;
        }

#ifdef ON_KEY_OBJECT
        if (Sharder::Instance().PrimaryNodeTerm() > 0)
        {
            continue;
        }
#endif

        // If the last check riser is this node, it will call dead lock check
        // again. Or if the time spend more than two times than interval time
        // due to last check riser crashed, this node will rise the check. To
        // avoid multi nodes rise the check at the same time, here add
        // local_shards_.NodeId() * MICRO_SECOND to make more waitting seconds
        // according the node id.
        if (ival < time_interval_ * 2 + local_shards_.NodeId() * MICRO_SECOND &&
            check_node_id_ != local_shards_.NodeId())
        {
            continue;
        }

        requested_check_ = false;
        lk.unlock();
        GatherLockDependancy();
    }
}
}  // namespace txservice
