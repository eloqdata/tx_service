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
#include "fault/cc_node.h"

#include <brpc/controller.h>
#include <brpc/errno.pb.h>
#include <bthread/bthread.h>
#include <bthread/mutex.h>

#include <atomic>
#include <cstdio>
#include <iostream>
#include <mutex>

#include "cc_map.h"
#include "cc_node_service.h"
#include "cc_req_misc.h"
#include "cc_request.pb.h"
#include "local_cc_shards.h"
#include "metrics.h"
#include "sharder.h"
#include "tx_service.h"
#include "tx_service_common.h"
#include "util.h"

namespace txservice::fault
{

CcNode::CcNode(const uint32_t ng_id,
               const uint32_t node_id,
               LocalCcShards &local_shards,
               uint32_t log_group_cnt)
    : ng_id_(ng_id),
      node_id_(node_id),
      last_ckpt_ts_(0),
      pinning_threads_(0),
      local_cc_shards_(local_shards),
      log_group_cnt_(log_group_cnt)
{
}

bool CcNode::CheckLogGroupReplayFinished(uint32_t log_group_id, int64_t ng_term)
{
    std::lock_guard<std::mutex> lk(recovery_mux_);
    int64_t term = Sharder::Instance().LeaderTerm(ng_id_);
    if (term >= ng_term)
    {
        return true;
    }
    int64_t candidate_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
    if (candidate_term > ng_term)
    {
        return true;
    }
    if (recovered_log_groups_.find(log_group_id) == recovered_log_groups_.end())
    {
        return false;
    }
    return true;
}

void CcNode::FinishLogGroupReplay(uint32_t log_group_id,
                                  int64_t ng_term,
                                  uint32_t latest_committed_txn_no,
                                  uint64_t last_ckpt_ts)
{
    // recovery_mux_ is used to protect recovered_log_groups_, since raft
    // service thread will also modify it concurrently.
    std::lock_guard<std::mutex> lk(recovery_mux_);

    // ignore the FinishReplayMsg whose ng_term is smaller than the current
    // candidate_leader_term_ or if this cc node is not recovering.
    int64_t candidate_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
    if (candidate_term < 0 || candidate_term > ng_term)
    {
        LOG(ERROR) << "Term changed since log replay start, old term: "
                   << ng_term << " ,candidate term: " << candidate_term;
        return;
    }

    // set CcNode's last_ckpt_ts to the greatest last_ckpt_ts received from all
    // log groups
    UpdateCkptTs(last_ckpt_ts);

    // native cc node finishes log replay from its bound log group, set
    // starting txn numbers of local cc shards. Since only native cc_node can
    // start transaction.
    // each ccshard reads tx_ident_ in NewTx(), which is concurrent with this
    // write, native cc node's leader_term_ synchronizes them.
    // Since cc_node is not bound to specific log node group, finally the
    // cc_shard will be setup with the MAX last_committed_txn_no from all log
    // groups
    if (ng_id_ == node_id_)
    {
        local_cc_shards_.SetTxIdent(latest_committed_txn_no);
    }

    auto lg_it = recovered_log_groups_.emplace(log_group_id);
    if (lg_it.second && recovered_log_groups_.size() == log_group_cnt_)
    {
        // reset the recovered_log_groups_ since we have finished the log replay
        // work for the current term.
        recovered_log_groups_.clear();

        candidate_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
        Sharder::Instance().SetLeaderTerm(ng_id_, candidate_term);
        LOG(INFO) << "The leader of cc node group ng#" << ng_id_
                  << " with the term " << candidate_term
                  << " has been recovered.";
        Sharder::Instance().SetCandidateTerm(ng_id_, -1);

        Sharder::Instance().NodeGroupFinishRecovery(ng_id_);
    }
}

int64_t CcNode::PinData()
{
    std::unique_lock lk(pinning_threads_mux_);
    int64_t leader_term = Sharder::Instance().LeaderTerm(ng_id_);
    if (leader_term > 0)
    {
        pinning_threads_++;
    }
    else
    {
        leader_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
        if (leader_term > 0)
        {
            pinning_threads_++;
        }
    }
    return leader_term;
}

int64_t CcNode::StandbyPinData()
{
    std::unique_lock lk(pinning_threads_mux_);
    int64_t standby_term = Sharder::Instance().StandbyNodeTerm();
    if (standby_term > 0)
    {
        pinning_threads_++;
    }
    else
    {
        standby_term = Sharder::Instance().CandidateStandbyNodeTerm();
        if (standby_term > 0)
        {
            pinning_threads_++;
        }
    }
    return standby_term;
}

void CcNode::UnpinData()
{
    std::unique_lock lk(pinning_threads_mux_);
    if (pinning_threads_ > 0)
    {
        pinning_threads_--;
        // wake up braft thread in case it is waiting in on_leader_stop()
        pinning_threads_cv_.notify_one();
    }
}

bool CcNode::UpdateCkptTs(uint64_t new_ckpt_ts)
{
    uint64_t expected_old_value = last_ckpt_ts_.load(std::memory_order_relaxed);
    // update last_ckpt_ts_ only if new_ckpt_ts is bigger
    bool success = false;
    while (new_ckpt_ts > expected_old_value && !success)
    {
        // if CAS fails, expected_old_value will be set to the actual value of
        // last_ckpt_ts_
        success = last_ckpt_ts_.compare_exchange_weak(
            expected_old_value, new_ckpt_ts, std::memory_order_relaxed);
    }
    return success;
}

/**
 * @brief Notify all the nodes that the node_id of the new leader in node
 * group leader_ng_id, and request these nodes to update their leader cache.
 *
 * @param leader_ng_id
 * @param leader_node_id
 */
void CcNode::NotifyNewLeaderStart(uint32_t leader_ng_id,
                                  uint32_t leader_node_id)
{
    auto all_nodes = Sharder::Instance().GetAllNodesConfigs();

    for (auto &[node_id, _] : *all_nodes)
    {
        if (node_id == leader_node_id)
        {
            Sharder::Instance().UpdateLeader(leader_ng_id, leader_node_id);
            continue;
        }

        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(node_id);
        if (channel == nullptr)
        {
            // Fails to establish the channel to the tx node. Silently
            // returns. The tx will be recovered again by next
            // conflicting tx.
            LOG(ERROR) << "Fail to init the channel to the leader of ng#"
                       << leader_ng_id << " to notify leader start.";
            continue;
        }

        remote::CcRpcService_Stub stub(channel.get());
        remote::NotifyNewLeaderStartRequest req;
        req.set_ng_id(leader_ng_id);
        req.set_node_id(leader_node_id);
        remote::NotifyNewLeaderStartResponse res;
        res.set_error(false);

        brpc::Controller cntl;
        cntl.set_timeout_ms(100);
        stub.NotifyNewLeaderStart(&cntl, &req, &res, nullptr);

        // Retry is not needed at here, the remote nodes will also
        // refresh their leader caches passively.
        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail the NotifyNewLeaderStart RPC of ng#"
                       << leader_ng_id << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
        }
        else if (res.error())
        {
            LOG(ERROR) << "Fail to notify the new leader of ng#" << leader_ng_id
                       << " to remote node id:" << node_id;
        }
    }
}

bool CcNode::OnLeaderStart(int64_t term,
                           uint64_t &replay_start_ts,
                           bool &retry,
                           uint32_t *next_leader_node)
{
    bool expected = false;
    if (!is_processing_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel))
    {
        // Keep retring
        retry = true;
        return false;
    }

    std::shared_ptr<void> defer_release(
        nullptr,
        [this](void *)
        { is_processing_.store(false, std::memory_order_release); });

    if (Sharder::Instance().CandidateLeaderTerm(ng_id_) >= term ||
        Sharder::Instance().LeaderTerm(ng_id_) >= term)
    {
        // This ccnode has become a candidate leader or leader
        return true;
    }

    int64_t prev_standby_term = Sharder::Instance().StandbyNodeTerm();
    int64_t prev_candidate_standby_term =
        Sharder::Instance().CandidateStandbyNodeTerm();
    int64_t prev_subsribe_term = PrimaryTermFromStandbyTerm(prev_standby_term);

    if (prev_standby_term > 0)
    {
        size_t core_cnt = local_cc_shards_.Count();
        WaitableCc sub_cc(
            [](CcShard &ccs)
            {
                auto &grps = ccs.GetStandbysequenceGrps();
                for (auto &[grp_id, grp_info] : grps)
                {
                    if (grp_info.subscribed_)
                    {
                        // Unsubscribe to reject incoming data.
                        grp_info.Unsubscribe();
                    }
                }

                return true;
            },
            core_cnt);

        for (uint32_t core_id = 0; core_id < core_cnt; core_id++)
        {
            local_cc_shards_.EnqueueCcRequest(core_id, &sub_cc);
        }
        sub_cc.Wait();

        uint64_t inflight_stanbdy_req_cnt =
            Sharder::Instance().InflightStandbyReqCount();
        while (inflight_stanbdy_req_cnt > 0)
        {
            bthread_usleep(100);
            inflight_stanbdy_req_cnt =
                Sharder::Instance().InflightStandbyReqCount();
        }

        // Standby pins the node group when processing DDL to avoid leaving
        // inconsistent state caused by term change during DDL. Wait until all
        // pins are cleared on this ng before clearing the standby term.
        {
            std::unique_lock lk(pinning_threads_mux_);
            pinning_threads_cv_.wait(lk,
                                     [this] { return pinning_threads_ == 0; });
        }

        // no longer subscribed to previous term
        Sharder::Instance().SetStandbyNodeTerm(-1);
        Sharder::Instance().SetCandidateStandbyNodeTerm(-1);
    }
    else if (prev_candidate_standby_term > 0)
    {
        // no longer subscribed to previous term
        assert(prev_standby_term < 0);
        Sharder::Instance().SetCandidateStandbyNodeTerm(-1);
        LOG(INFO) << "Candidate standby cannot escalate to leader";
        // A new leader has been elected. The cache needs to be cleared since
        // it is no longer valid.
        ClearCcNodeGroupData();

        // transfer leader to next node
        retry = false;
        return false;
    }

    if (!txservice_skip_kv && !local_cc_shards_.store_hd_->IsSharedStorage())
    {
        if (!local_cc_shards_.store_hd_->OnLeaderStart(next_leader_node))
        {
            retry = false;
            return false;
        }
    }

    {
        // replay thread and leader election thread may update
        // candidate_leader_term_, leader_term_ and recovered_log_groups_
        // concurrently.
        std::lock_guard<std::mutex> lk(recovery_mux_);
        Sharder::Instance().SetCandidateTerm(ng_id_, term);
        // new leader will send ReplayLog request to logservice to replay logs.
        // It should reset the recovered_log_groups_ ahead.
        recovered_log_groups_.clear();
    }

    if (metrics::enable_metrics)
    {
        Sharder::Instance().GetLocalCcShards()->GetNodeMeter()->Collect(
            metrics::NAME_LEADER_CHANGES, 1, std::to_string(ng_id_));
        Sharder::Instance().GetLocalCcShards()->GetNodeMeter()->Collect(
            metrics::NAME_IS_LEADER, 1, std::to_string(ng_id_));
    }

    LOG(INFO) << "CC node " << node_id_ << " becomes the leader of ng#"
              << ng_id_ << ". Term: " << term;

    bool cache_survivied = false;
    if (ng_id_ == Sharder::Instance().NativeNodeGroup() &&
        prev_subsribe_term > 0)
    {
        if (!txservice_skip_wal)
        {
            // If node was a follower, we can keep
            // the cache and replay from the consistent ts of in memory
            // cache.
            bthread::Mutex mux;
            uint64_t last_consistent_ts = UINT64_MAX;
            WaitableCc get_consistent_ts_cc(
                [&mux, &last_consistent_ts](CcShard &ccs)
                {
                    uint64_t shard_ts = ccs.MinLastStandbyConsistentTs();
                    std::unique_lock<bthread::Mutex> lk(mux);
                    last_consistent_ts = std::min(last_consistent_ts, shard_ts);
                    return true;
                },
                local_cc_shards_.Count());
            // Get the last consistent ts of the in memory cache and replay
            // from then.
            for (uint16_t core_id = 0; core_id < local_cc_shards_.Count();
                 core_id++)
            {
                local_cc_shards_.EnqueueToCcShard(core_id,
                                                  &get_consistent_ts_cc);
            }
            get_consistent_ts_cc.Wait();

            if (last_consistent_ts != UINT64_MAX && last_consistent_ts != 0)
            {
                cache_survivied = true;
                replay_start_ts = last_consistent_ts + 1;
            }
        }
        else
        {
            cache_survivied = true;
        }

        if (!cache_survivied)
        {
            //  if cache does not survive to the next term, clear ccm.
            ClearCcNodeGroupData();
            replay_start_ts = 0;
        }
        else if (!txservice_skip_kv &&
                 local_cc_shards_.store_hd_->IsSharedStorage())
        {
            // Update ckpt ts of cache. since for non-shared kv, each standby
            // does its own ckpt and already has ckpt ts set.
            uint16_t core_cnt = local_cc_shards_.Count();
            cache_survivied = true;
            EscalateStandbyCcmCc escalate_cc(core_cnt, last_ckpt_ts_);
            for (uint16_t core_id = 0; core_id < core_cnt; ++core_id)
            {
                local_cc_shards_.EnqueueCcRequest(core_id, &escalate_cc);
            }
            escalate_cc.Wait();
            // Notify checkpointer to flush the updated cache
            local_cc_shards_.NotifyCheckPointer();
        }
    }

    if (!local_cc_shards_.IsRangeBucketsInitialized(ng_id_))
    {
        // TODO: HARDCORE SEED
        // If kv is not enabled, just copy bucket info from preferred ng.
        auto ng_ids = Sharder::Instance().AllNodeGroups();
        local_cc_shards_.InitRangeBuckets(
            ng_id_, *ng_ids, Sharder::Instance().ClusterConfigVersion());
    }

    local_cc_shards_.InitPrebuiltTables(ng_id_, term);

    // when kv is enabled, and cache replacement is disabled, then load all
    // datas from kv
    if (!txservice_skip_kv && !txservice_enable_cache_replacement &&
        !cache_survivied)
    {
        local_cc_shards_.store_hd_->RestoreTxCache(ng_id_, term);
    }

    return true;
}

bool CcNode::OnLeaderStop(int64_t term)
{
    bool expected = false;
    if (!is_processing_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel))
    {
        // Keep retrying
        return false;
    }

    std::shared_ptr<void> defer_release(
        nullptr,
        [this](void *)
        { is_processing_.store(false, std::memory_order_release); });

    {
        // replay thread and leader election thread may update
        // candidate_leader_term_, leader_term_ and recovered_log_groups_
        // concurrently.
        std::lock_guard<std::mutex> lk(recovery_mux_);
        Sharder::Instance().SetLeaderTerm(ng_id_, -1);
        Sharder::Instance().SetCandidateTerm(ng_id_, -1);
    }

    if (metrics::enable_metrics)
    {
        Sharder::Instance().GetLocalCcShards()->GetNodeMeter()->Collect(
            metrics::NAME_IS_LEADER, 0, std::to_string(ng_id_));
    }

    LOG(INFO) << "CC node " << node_id_ << " steps down as the leader of ng#"
              << ng_id_ << ".";

    // Wait for data unpin then clear all node_group data
    {
        std::unique_lock lk(pinning_threads_mux_);
        pinning_threads_cv_.wait(lk, [this] { return pinning_threads_ == 0; });
    }
    ClearCcNodeGroupData();
    return true;
}

bool CcNode::Failover(const std::string &target_host,
                      const uint16_t target_port,
                      std::string &error_message)
{
    // Concurrent control on term changing.
    bool expected = false;
    while (!is_processing_.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel))
    {
        bthread_usleep(100);
        expected = false;
    }

    int64_t leader_term = Sharder::Instance().LeaderTerm(ng_id_);
    // CHECK #1: Verify the current node is a leader
    if (leader_term > 0)
    {
        // Reject all requests until failover command finishes.
        Sharder::Instance().SetLeaderTerm(ng_id_, -1);
    }
    else
    {
        error_message =
            "ERR failover failed because this node is no longer leader";
        return false;
    }
    is_processing_.store(false, std::memory_order_release);

    // Get all node configs for this node group
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> ng_configs =
        Sharder::Instance().GetNodeGroupConfigs();

    // Look for both nodes only in the current node group
    auto current_group_it = ng_configs.find(ng_id_);
    assert(current_group_it != ng_configs.end());

    // Check if target node and local node are both in this node group
    uint32_t target_node_id = UINT32_MAX;
    std::string local_host;
    uint16_t local_port = 0;

    for (const auto &node_config : current_group_it->second)
    {
        // Check for target node
        if (target_node_id == UINT32_MAX &&
            node_config.host_name_ == target_host &&
            node_config.port_ == target_port + 10000)
        {
            // CHECK #2: Target node must be a candidate node
            if (!node_config.is_candidate_)
            {
                error_message = "ERR target node is not a candidate";
                RecoverLeaderTerm(leader_term);
                return false;
            }
            target_node_id = node_config.node_id_;
        }

        // Check for local node
        if (node_config.node_id_ == node_id_)
        {
            local_port = node_config.port_;
            local_host = node_config.host_name_;
        }

        // Stop if we found both nodes
        if (target_node_id != UINT32_MAX && local_port != 0)
            break;
    }

    // Validate both nodes were found
    assert(local_port != 0 && !local_host.empty());

    if (target_node_id == UINT32_MAX)
    {
        error_message = "ERR target node not found in current node group";
        RecoverLeaderTerm(leader_term);
        return false;
    }

    // CHECK #3: Cannot failover to self
    if (target_node_id == node_id_)
    {
        error_message = "ERR cannot failover to self";
        RecoverLeaderTerm(leader_term);
        return false;
    }

    // CHECK #4: Target candidate should have caught up with leader
    auto local_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_count = local_shards->Count();
    for (size_t core_idx = 0; core_idx < core_count; ++core_idx)
    {
        if (!WaitForShardSyncWithRetry(core_idx, target_node_id, error_message))
        {
            RecoverLeaderTerm(leader_term);
            return false;
        }
    }

    DLOG(INFO) << "All shards are fully in sync.";

    // Create RPC channel to the host manager
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 3;

    // Calculate host manager port and initialize connection
    uint16_t local_hm_port = local_port + 4;
    if (channel.Init(local_host.c_str(), local_hm_port, &options) != 0)
    {
        error_message = "ERR failed to connect to local host manager";
        RecoverLeaderTerm(leader_term);
        return false;
    }

    // Send leadership transfer request
    remote::TransferRequest request;
    request.set_ng_id(ng_id_);
    request.set_node_id(target_node_id);
    request.set_term(leader_term);

    remote::TransferResponse response;
    brpc::Controller cntl;

    remote::HostMangerService_Stub stub(&channel);
    stub.Transfer(&cntl, &request, &response, NULL);

    // Handle RPC errors
    if (cntl.Failed())
    {
        error_message =
            "ERR failed to transfer leadership: " + cntl.ErrorText();
        RecoverLeaderTerm(leader_term);
        return false;
    }

    // Check for application-level errors
    int32_t error_code = response.error_code();
    if (error_code != 0)
    {
        error_message = "ERR leader transfer failed with error code: " +
                        std::to_string(error_code);
        RecoverLeaderTerm(leader_term);
        return false;
    }

    // Success - respond with confirmation message
    std::string status_message =
        "OK Leader transfer initiated to " + target_host + ":" +
        std::to_string(target_port) +
        " (node ID: " + std::to_string(target_node_id) + ")";
    return true;
}

void CcNode::RecoverLeaderTerm(int64_t leader_term)
{
    bool expected = false;
    while (!is_processing_.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel))
    {
        bthread_usleep(100);
        expected = false;
    }
    Sharder::Instance().SetLeaderTerm(ng_id_, leader_term);
    is_processing_.store(false, std::memory_order_release);
}

void CcNode::OnStartFollowing(uint32_t node_id, int64_t term, bool resubscribe)
{
#ifndef ON_KEY_OBJECT
    assert(false);
    LOG(ERROR) << "OnStartFollowing should not be called if standby feature is "
                  "disabled.";
    return;
#else
    int64_t expected = -1;
    while (!requested_subscribe_primary_term_.compare_exchange_strong(
        expected, term, std::memory_order_acq_rel))
    {
        if (expected >= term)
        {
            // No need to send request since there's another in progress.
            return;
        }
    }
    if (resubscribe)
    {
        // resubscribe is called on tx processor and on start following needs
        // to be handled on a worker thread.
        Sharder::Instance().GetTxWorkerPool()->SubmitWork(
            [this, node_id, term]
            { SubscribePrimaryNode(node_id, term, true); });
    }
    else
    {
        SubscribePrimaryNode(node_id, term, false);
    }
#endif
}

bool CcNode::OnSnapshotReceived(const remote::OnSnapshotSyncedRequest *req)
{
    bool expected = false;
    while (!is_processing_.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel))
    {
        bthread_usleep(100);
        expected = false;
    }

    if (Sharder::Instance().StandbyNodeTerm() > 0 ||
        Sharder::Instance().CandidateStandbyNodeTerm() !=
            req->standby_node_term())
    {
        is_processing_.store(false, std::memory_order_release);
        return false;
    }

    bool succ = local_cc_shards_.store_hd_->OnSnapshotReceived(req);
    if (succ)
    {
        int64_t standby_term = req->standby_node_term();
        Sharder::Instance().SetStandbyNodeTerm(standby_term);
        Sharder::Instance().SetCandidateStandbyNodeTerm(-1);
        LOG(INFO) << "node #" << node_id_
                  << " is caught up with primary node in ng#" << ng_id_;
        // when kv is enabled, and cache replacement is disabled, then load all
        // datas from kv
        if (!txservice_skip_kv && !txservice_enable_cache_replacement)
        {
            local_cc_shards_.store_hd_->RestoreTxCache(ng_id_, standby_term);
        }
    }

    is_processing_.store(false, std::memory_order_release);

    return succ;
}

void CcNode::ClearCcNodeGroupData()
{
    uint16_t core_cnt = local_cc_shards_.Count();
    ClearCcNodeGroup clear_ccm_req(ng_id_, core_cnt);
    for (uint16_t core_id = 0; core_id < core_cnt; ++core_id)
    {
        local_cc_shards_.EnqueueCcRequest(core_id, &clear_ccm_req);
    }
    clear_ccm_req.Wait();
}

void CcNode::SubscribePrimaryNode(uint32_t leader_node_id,
                                  int64_t primary_term,
                                  bool resubscribe)
{
#ifndef ON_KEY_OBJECT
    assert(false);
    LOG(ERROR)
        << "SubscribePrimaryNode should not be called if standby feature "
           "is disabled.";
    return;
#endif

    assert(ng_id_ == Sharder::Instance().NativeNodeGroup());
    bool expected = false;
    while (!is_processing_.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel))
    {
        bthread_usleep(100);
        expected = false;
    }
    std::shared_ptr<void> defer_release(
        nullptr,
        [this, &primary_term](void *)
        {
            requested_subscribe_primary_term_.compare_exchange_strong(
                primary_term, -1, std::memory_order_acq_rel);
        });
    if (Sharder::Instance().CandidateLeaderTerm(ng_id_) > primary_term ||
        Sharder::Instance().LeaderTerm(ng_id_) > primary_term)
    {
        // Already a leader of a newer term
        is_processing_.store(false, std::memory_order_release);
        return;
    }
    LOG(INFO) << "Subscribing to primary node " << leader_node_id << " at term "
              << primary_term;

    int64_t prev_primary_term = Sharder::Instance().PrimaryNodeTerm();

    bool delay_clear_ccm = false;
    if (prev_primary_term > 0)
    {
        if ((prev_primary_term > primary_term && resubscribe) ||
            (!resubscribe && prev_primary_term >= primary_term))
        {
            // already subscribed to a newer term, no op
            is_processing_.store(false, std::memory_order_release);
            return;
        }
        else
        {
            // Wait for data unpin then clear all node_group data
            {
                std::unique_lock lk(pinning_threads_mux_);
                pinning_threads_cv_.wait(
                    lk, [this] { return pinning_threads_ == 0; });
            }

            // clean old term ccm cache since this node was following on an
            // older term
            Sharder::Instance().SetStandbyNodeTerm(-1);
            Sharder::Instance().SetCandidateStandbyNodeTerm(-1);

            if (resubscribe)
            {
                // If resubscribe, we need to clear ccmap immediately since we
                // have reset standby term and candidate standby term. If on
                // leader start is called before the ccmap is cleared, we will
                // have a outdated cache. We do not need delay the clear ccmap
                // since the ccmap is already outdated and needs to be cleared
                // anyway.
                ClearCcNodeGroupData();
            }
            else
            {
                // If not resubscribe, we need to delay the clear ccmap until we
                // make sure that the new leader is qualified to become the
                // leader of the ng. Otherwise we might loose the valid cache on
                // this node if we cleared too early.
                delay_clear_ccm = true;
            }
        }
    }

    if (!resubscribe &&
        Sharder::Instance().LeaderNodeId(ng_id_) != leader_node_id)
    {
        Sharder::Instance().UpdateLeader(ng_id_, leader_node_id);
    }
    auto *store_hd = Sharder::Instance().GetLocalCcShards()->store_hd_;
    //  term is already updated. Release processing latch to allow other rpc
    //  to proceed.
    is_processing_.store(false, std::memory_order_release);

    // Notify primary node to start forwarding data change to this node.
    auto channel = Sharder::Instance().GetCcNodeServiceChannel(leader_node_id);
    while (!channel)
    {
        int64_t pending_sub_term =
            requested_subscribe_primary_term_.load(std::memory_order_acquire);
        if (pending_sub_term != primary_term)
        {
            // Someone is trying to follow a newer term. Discard current
            // request.
            LOG(INFO) << "Failed to subscribe to primary node due to newer "
                         "primary leader term "
                      << pending_sub_term << ", requested term "
                      << primary_term;
            return;
        }
        bthread_usleep(1000);
        channel = Sharder::Instance().GetCcNodeServiceChannel(leader_node_id);
    }

    std::unique_ptr<remote::CcRpcService_Stub> stub =
        std::make_unique<remote::CcRpcService_Stub>(channel.get());
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);

    //  Ask primary to start forwarding msgs and get
    // starting seq id.
    remote::StandbyStartFollowingRequest start_follow_req;
    remote::StandbyStartFollowingResponse start_follow_resp;
    start_follow_req.set_node_group_id(ng_id_);
    start_follow_req.set_node_id(node_id_);
    start_follow_req.set_ng_term(primary_term);

    stub->StandbyStartFollowing(
        &cntl, &start_follow_req, &start_follow_resp, nullptr);

    while (cntl.Failed() || start_follow_resp.error())
    {
        if (Sharder::Instance().LeaderTerm(ng_id_) > 0 ||
            Sharder::Instance().CandidateLeaderTerm(ng_id_) > 0 ||
            Sharder::Instance().StandbyNodeTerm() > 0 ||
            Sharder::Instance().CandidateStandbyNodeTerm() > 0)
        {
            return;
        }
        int64_t pending_sub_term =
            requested_subscribe_primary_term_.load(std::memory_order_acquire);
        if (pending_sub_term != primary_term)
        {
            LOG(INFO) << "Failed to subscribe to primary node due to newer "
                         "primary leader term "
                      << pending_sub_term << ", requested term "
                      << primary_term;
            return;
        }

        if (cntl.Failed() && cntl.ErrorCode() != EAGAIN &&
            cntl.ErrorCode() != brpc::EOVERCROWDED &&
            cntl.ErrorCode() != brpc::ERPCTIMEDOUT)
        {
            // Refresh channel
            do
            {
                channel = Sharder::Instance().UpdateCcNodeServiceChannel(
                    leader_node_id, channel);
                if (channel == nullptr)
                {
                    bthread_usleep(1000);
                }
            } while (channel == nullptr);

            stub = std::make_unique<remote::CcRpcService_Stub>(channel.get());
        }
        cntl.Reset();
        cntl.set_timeout_ms(5000);
        start_follow_resp.Clear();
        bthread_usleep(1000);
        stub->StandbyStartFollowing(
            &cntl, &start_follow_req, &start_follow_resp, nullptr);
    }

    expected = false;
    while (!is_processing_.compare_exchange_strong(
        expected, true, std::memory_order_acq_rel))
    {
        bthread_usleep(100);
        expected = false;
    }
    int64_t standby_term =
        (primary_term << 32) | start_follow_resp.subscribe_id();
    // verify we're not on a newer term.
    if (Sharder::Instance().LeaderTerm(ng_id_) > 0 ||
        Sharder::Instance().CandidateLeaderTerm(ng_id_) > 0 ||
        Sharder::Instance().StandbyNodeTerm() > standby_term ||
        Sharder::Instance().CandidateStandbyNodeTerm() > standby_term)
    {
        LOG(INFO) << "rejected subscribe req due to primary "
                     "term mismatch";
        is_processing_.store(false, std::memory_order_release);
        return;
    }

    // Do not clear ccm until we're sure that the new elected leader is
    // qualified to become the leader of the ng.
    if (delay_clear_ccm)
    {
        ClearCcNodeGroupData();
    }

    if (!txservice_skip_kv)
    {
        store_hd->OnStartFollowing();
    }
    uint32_t seq_grp_cnt = start_follow_resp.start_sequence_id_size();
    std::vector<uint64_t> init_seq_ids;
    init_seq_ids.reserve(seq_grp_cnt);
    for (uint32_t grp_id = 0; grp_id < seq_grp_cnt; grp_id++)
    {
        init_seq_ids.push_back(start_follow_resp.start_sequence_id(grp_id));
    }
    uint16_t core_cnt = local_cc_shards_.Count();

    // reset start seq id
    WaitableCc sub_cc(
        [&init_seq_ids, core_cnt, seq_grp_cnt](CcShard &ccs)
        {
            for (uint32_t grp_id = 0; grp_id < seq_grp_cnt; grp_id++)
            {
                if (grp_id % core_cnt == ccs.core_id_)
                {
                    ccs.SubsribeToPrimaryNode(grp_id, init_seq_ids.at(grp_id));
                }
            }

            return true;
        },
        core_cnt);
    for (uint32_t core_id = 0; core_id < core_cnt; core_id++)
    {
        local_cc_shards_.EnqueueCcRequest(core_id, &sub_cc);
    }
    sub_cc.Wait();
    // Initialize bucket info.

    // TODO(lzx): fetch ng_configs from LeaderNode if storage is not shared
    // and cluster scaling is enabled for eloqkv standby nodes feature.
    if (txservice_skip_kv || !local_cc_shards_.store_hd_->IsSharedStorage())
    {
        // Cluster scale is not enabled in this case and the bucket slots are
        // static.
        auto ng_ids = Sharder::Instance().AllNodeGroups();

        local_cc_shards_.InitRangeBuckets(
            ng_id_, *ng_ids, Sharder::Instance().ClusterConfigVersion());
    }
    else
    {
        std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs =
            Sharder::Instance().GetNodeGroupConfigs();
        uint64_t version = Sharder::Instance().ClusterConfigVersion();
        std::set<NodeGroupId> ng_ids;
        for (auto &[ng_id, _] : ng_configs)
        {
            ng_ids.emplace(ng_id);
        }
        local_cc_shards_.InitRangeBuckets(ng_id_, ng_ids, version);
        if (Sharder::Instance().ClusterConfigVersion() < version)
        {
            // Use a dummy cc request that returns once it's put into cc
            // queue.
            WaitableCc cc;
            Sharder::Instance().UpdateClusterConfig(
                ng_configs, version, &cc, local_cc_shards_.GetCcShard(0));
            cc.Wait();
        }
    }

    if (!txservice_skip_kv)
    {
        // If we need to sync kv snapshot, remain in candidate standby until
        // snapshot is received.
        Sharder::Instance().SetCandidateStandbyNodeTerm(standby_term);
    }
    else
    {
        Sharder::Instance().SetStandbyNodeTerm(standby_term);
        LOG(INFO) << "node #" << node_id_
                  << " is caught up with primary node in ng#" << ng_id_;
    }

    LOG(INFO) << "subscribed to primary node at term " << primary_term;

    is_processing_.store(false, std::memory_order_release);
    // Ask primary to resend msg from the given seq id since some of the
    // messages sent before starting seq id is set on standby node might have
    // been dropped.
    remote::ResetStandbySequenceIdRequest reset_req;
    remote::ResetStandbySequenceIdResponse reset_resp;
    reset_req.set_ng_id(ng_id_);
    reset_req.set_standby_node_term(standby_term);
    reset_req.set_node_id(node_id_);
    reset_req.mutable_seq_id()->CopyFrom(start_follow_resp.start_sequence_id());
    for (auto i = 0; i < reset_req.seq_id_size(); i++)
    {
        reset_req.add_seq_grp(i);
    }

    cntl.Reset();
    stub->ResetStandbySequenceId(&cntl, &reset_req, &reset_resp, nullptr);

    // Check rpc result
    while (cntl.Failed())
    {
        // We only need to retry if the message is not delivered.
        if (Sharder::Instance().LeaderTerm(ng_id_) > 0 ||
            Sharder::Instance().CandidateLeaderTerm(ng_id_) > 0 ||
            (Sharder::Instance().StandbyNodeTerm() != standby_term &&
             Sharder::Instance().CandidateStandbyNodeTerm() != standby_term))
        {
            // Stop retrying if the node has become a leader or the standby term
            // has changed.
            return;
        }
        if (cntl.Failed() && cntl.ErrorCode() != EAGAIN &&
            cntl.ErrorCode() != brpc::EOVERCROWDED &&
            cntl.ErrorCode() != brpc::ERPCTIMEDOUT)
        {
            // Refresh channel
            do
            {
                channel = Sharder::Instance().UpdateCcNodeServiceChannel(
                    leader_node_id, channel);
                if (channel == nullptr)
                {
                    bthread_usleep(1000);
                }
            } while (channel == nullptr);

            stub = std::make_unique<remote::CcRpcService_Stub>(channel.get());
        }
        cntl.Reset();
        cntl.set_timeout_ms(5000);
        reset_resp.Clear();
        bthread_usleep(1000);
        stub->ResetStandbySequenceId(&cntl, &reset_req, &reset_resp, nullptr);
    }

    // If the data store is not shared between standby and primary, ask primary
    // to send a snapshot of previous data
    if (!txservice_skip_kv)
    {
        remote::StorageSnapshotSyncRequest snapshot_req;
        remote::StorageSnapshotSyncResponse snapshot_resp;

        snapshot_req.set_ng_id(ng_id_);
        snapshot_req.set_standby_node_term(standby_term);
        snapshot_req.set_standby_node_id(node_id_);

        std::array<char, 128> buffer;
        std::string username;
        FILE *output_stream = popen("echo $USER", "r");
        while (fgets(buffer.data(), 128, output_stream) != nullptr)
        {
            username.append(buffer.data());
        }
        if (!username.empty())
        {
            // remove the trailing \n of output.
            assert(username.back() == '\n');
            username.pop_back();
        }
        pclose(output_stream);

        // Checkpointer will retry if snapshot sync failed. Do not retry here
        // too frequently since it might overwhelm the primary node.
        snapshot_req.set_dest_path(store_hd->SnapshotSyncDestPath());
        snapshot_req.set_user(username);
        cntl.Reset();
        cntl.set_timeout_ms(10000);
        stub->RequestStorageSnapshotSync(
            &cntl, &snapshot_req, &snapshot_resp, nullptr);
        if (snapshot_resp.error())
        {
            LOG(ERROR) << "snapshot sync failed";
        }
    }
}

bool CcNode::WaitForShardSyncWithRetry(const size_t shard_idx,
                                       const uint32_t target_node_id,
                                       std::string &error_message)
{
    const int MAX_RETRY_COUNT = 10;
    const int RETRY_INTERVAL_MS = 500;  // 500ms between retries
    bool is_shard_in_sync = false;

    auto CheckShardSync = [&](CcShard &shard)
    {
        auto standby_nodes = shard.GetSubscribedStandbys();

        // Check if the target node is in the subscribed standbys
        bool target_found = false;
        for (auto &node_id : standby_nodes)
        {
            if (node_id == target_node_id)
            {
                target_found = true;
                break;
            }
        }

        if (!target_found)
        {
            error_message = "ERR target node is not subscribed to shard " +
                            std::to_string(shard_idx);
            is_shard_in_sync = false;
            return true;
        }

        uint64_t standby_seq_id = shard.GetStandbyLag(target_node_id);
        uint64_t leader_latest_seq = shard.GetNextForwardSequnceId() - 1;

        // Check if standby is lagging
        is_shard_in_sync = (standby_seq_id == leader_latest_seq ||
                            standby_seq_id == UINT64_MAX);
        return true;
    };

    // Initial check
    WaitableCc check_sync_cc(CheckShardSync);
    auto local_shards = Sharder::Instance().GetLocalCcShards();
    local_shards->EnqueueCcRequest(shard_idx, &check_sync_cc);
    check_sync_cc.Wait();

    // If not in sync, retry with backoff
    if (!is_shard_in_sync)
    {
        for (int retry = 0; retry < MAX_RETRY_COUNT; retry++)
        {
            // Yield current bthread to allow other tasks to run
            bthread_usleep(RETRY_INTERVAL_MS * 1000);

            DLOG(INFO) << "Waiting for standby node " << target_node_id
                       << " to sync with leader on shard " << shard_idx
                       << ", retry " << (retry + 1) << "/" << MAX_RETRY_COUNT;

            check_sync_cc.Reset(CheckShardSync, 1);
            local_shards->EnqueueCcRequest(shard_idx, &check_sync_cc);
            check_sync_cc.Wait();

            if (is_shard_in_sync)
            {
                break;
            }
        }

        if (!is_shard_in_sync)
        {
            error_message =
                "ERR target node has not caught up with the leader's data "
                "on shard " +
                std::to_string(shard_idx) + " after " +
                std::to_string(MAX_RETRY_COUNT) + " retries";
            return false;
        }
    }

    return true;
}
}  // namespace txservice::fault
