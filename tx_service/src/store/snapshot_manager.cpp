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
#include "store/snapshot_manager.h"

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <gflags/gflags.h>

#include <chrono>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "cc/local_cc_shards.h"

namespace txservice
{

namespace store
{
namespace
{
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
constexpr auto kStandbySnapshotTtl = std::chrono::hours(1);

struct RequestSyncSnapshotAggregate
{
    bthread::Mutex mux;
    bthread::ConditionVariable cv;
    size_t pending{0};
    size_t success{0};
    uint64_t min_ack_ckpt_ts{std::numeric_limits<uint64_t>::max()};
};

struct RequestSyncSnapshotRpcCtx
{
    brpc::Controller cntl;
    remote::RequestSyncSnapshotRequest req;
    remote::RequestSyncSnapshotResponse resp;
    RequestSyncSnapshotAggregate *aggregate{nullptr};
    uint32_t standby_node_id{0};
    int64_t standby_term{0};
    uint32_t ng_id{0};
};

class RequestSyncSnapshotDone : public google::protobuf::Closure
{
public:
    explicit RequestSyncSnapshotDone(
        std::shared_ptr<RequestSyncSnapshotRpcCtx> ctx)
        : ctx_(std::move(ctx))
    {
    }

    void Run() override
    {
        std::unique_ptr<RequestSyncSnapshotDone> self_guard(this);
        auto *agg = ctx_->aggregate;
        const bool succ = !ctx_->cntl.Failed() && !ctx_->resp.error();
        if (agg == nullptr)
        {
            if (!succ)
            {
                LOG(WARNING)
                    << "RequestSyncSnapshot async dispatch failed, ng_id="
                    << ctx_->ng_id
                    << ", standby_node_id=" << ctx_->standby_node_id
                    << ", standby_term=" << ctx_->standby_term
                    << ", error=" << ctx_->cntl.ErrorText()
                    << ", failed=" << ctx_->cntl.Failed()
                    << ", resp_error=" << ctx_->resp.error();
            }
            return;
        }
        {
            std::lock_guard<bthread::Mutex> lk(agg->mux);
            if (succ)
            {
                agg->success++;
                agg->min_ack_ckpt_ts = std::min(agg->min_ack_ckpt_ts,
                                                ctx_->resp.current_ckpt_ts());
            }
            agg->pending--;
        }
        agg->cv.notify_all();
    }

private:
    std::shared_ptr<RequestSyncSnapshotRpcCtx> ctx_;
};

bool DispatchRequestSyncSnapshotAsync(uint32_t ng_id,
                                      uint32_t standby_node_id,
                                      int64_t standby_term,
                                      uint64_t snapshot_ts,
                                      RequestSyncSnapshotAggregate *aggregate)
{
    auto channel = Sharder::Instance().GetCcNodeServiceChannel(standby_node_id);
    if (!channel)
    {
        LOG(WARNING) << "RequestSyncSnapshot channel is nullptr for standby "
                     << "node #" << standby_node_id << " at standby term "
                     << standby_term << ", ng_id=" << ng_id
                     << ", snapshot_ts=" << snapshot_ts;
        return false;
    }

    auto rpc_ctx = std::make_shared<RequestSyncSnapshotRpcCtx>();
    rpc_ctx->cntl.set_timeout_ms(1000);
    rpc_ctx->req.set_standby_node_term(standby_term);
    rpc_ctx->req.set_ng_id(ng_id);
    rpc_ctx->req.set_snapshot_ts(snapshot_ts);
    rpc_ctx->aggregate = aggregate;
    rpc_ctx->standby_node_id = standby_node_id;
    rpc_ctx->standby_term = standby_term;
    rpc_ctx->ng_id = ng_id;

    if (aggregate != nullptr)
    {
        std::lock_guard<bthread::Mutex> lk(aggregate->mux);
        aggregate->pending++;
    }

    remote::CcRpcService_Stub stub(channel.get());
    DLOG(INFO) << "RequestSyncSnapshot async dispatch, ng_id=" << ng_id
               << ", standby_node_id=" << standby_node_id
               << ", standby_term=" << standby_term
               << ", snapshot_ts=" << snapshot_ts
               << ", aggregate=" << (aggregate != nullptr);
    stub.RequestSyncSnapshot(&rpc_ctx->cntl,
                             &rpc_ctx->req,
                             &rpc_ctx->resp,
                             new RequestSyncSnapshotDone(rpc_ctx));
    return true;
}
#endif
}  // namespace

void SnapshotManager::Start()
{
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    if (store_hd_ != nullptr)
    {
        auto local_ngs = Sharder::Instance().LocalNodeGroups();
        for (uint32_t ng_id : local_ngs)
        {
            store_hd_->DeleteStandbySnapshotsBefore(
                ng_id, std::numeric_limits<uint64_t>::max());
        }
    }
#endif
    standby_sync_worker_ = std::thread([this] { StandbySyncWorker(); });
    pthread_setname_np(standby_sync_worker_.native_handle(), "ss_standby_sync");
}

void SnapshotManager::Shutdown()
{
    if (standby_sync_worker_.joinable())
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        terminated_ = true;
        standby_sync_cv_.notify_all();
        lk.unlock();
        standby_sync_worker_.join();
    }
    backup_worker_.Shutdown();
}

void SnapshotManager::StandbySyncWorker()
{
    constexpr auto kBlockedTaskRetryInterval = std::chrono::milliseconds(200);
#ifndef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    while (true)
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        standby_sync_cv_.wait(
            lk, [this] { return !pending_req_.empty() || terminated_; });
        if (terminated_)
        {
            return;
        }
        assert(!pending_req_.empty());
        lk.unlock();
        SyncWithStandby();
        lk.lock();

        if (terminated_)
        {
            return;
        }

        if (!pending_req_.empty())
        {
            // Pending requests are still blocked by subscribe/barrier checks.
            // Back off to avoid tight checkpoint retry loops.
            standby_sync_cv_.wait_for(lk, kBlockedTaskRetryInterval);
        }
    }
#else
    while (true)
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        std::vector<std::pair<uint32_t, uint64_t>> snapshots_to_delete;
        CollectExpiredSnapshotsLocked(std::chrono::system_clock::now(),
                                      &snapshots_to_delete);
        if (terminated_)
        {
            return;
        }
        if (!snapshots_to_delete.empty())
        {
            lk.unlock();
            for (const auto &[ng_id, snapshot_ts] : snapshots_to_delete)
            {
                store_hd_->DeleteStandbySnapshot(ng_id, snapshot_ts);
            }
            continue;
        }

        if (pending_req_.empty())
        {
            const auto deadline = NextSnapshotCleanupDeadlineLocked();
            if (deadline == std::chrono::system_clock::time_point::max())
            {
                standby_sync_cv_.wait(
                    lk,
                    [this]
                    {
                        return !pending_req_.empty() ||
                               !snapshot_cleanup_queue_.empty() || terminated_;
                    });
            }
            else
            {
                standby_sync_cv_.wait_until(
                    lk,
                    deadline,
                    [this, deadline]
                    {
                        return !pending_req_.empty() || terminated_ ||
                               NextSnapshotCleanupDeadlineLocked() < deadline;
                    });
            }
        }

        if (terminated_)
        {
            return;
        }

        CollectExpiredSnapshotsLocked(std::chrono::system_clock::now(),
                                      &snapshots_to_delete);
        if (!snapshots_to_delete.empty())
        {
            lk.unlock();
            for (const auto &[ng_id, snapshot_ts] : snapshots_to_delete)
            {
                store_hd_->DeleteStandbySnapshot(ng_id, snapshot_ts);
            }
            continue;
        }

        if (pending_req_.empty())
        {
            continue;
        }
        assert(!pending_req_.empty());
        lk.unlock();
        SyncWithStandby();
        lk.lock();

        if (terminated_)
        {
            return;
        }

        if (!pending_req_.empty())
        {
            // Pending requests are still blocked by subscribe/barrier checks.
            // Back off to avoid tight checkpoint retry loops.
            standby_sync_cv_.wait_for(lk, kBlockedTaskRetryInterval);
        }
    }
#endif
}

bool SnapshotManager::OnSnapshotSyncRequested(
    const txservice::remote::StorageSnapshotSyncRequest *req)
{
    DLOG(INFO) << "Received snapshot sync request from standby node #"
               << req->standby_node_id()
               << " for standby term: " << req->standby_node_term()
               << ", ng_id=" << req->ng_id()
               << ", dest_path=" << req->dest_path();
    assert(store_hd_ != nullptr);
    if (store_hd_ == nullptr)
    {
        LOG(ERROR) << "Store handler is nullptr but standby feature enabled.";
        return false;
    }

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    bool should_resend_completed_snapshot = false;
    uint64_t completed_snapshot_ts = 0;
    uint32_t completed_ng_id = 0;
    uint32_t completed_standby_node_id = 0;
    int64_t completed_standby_term = 0;
#endif
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);

        if (terminated_)
        {
            return false;
        }

        auto node_it = subscription_barrier_.find(req->standby_node_id());
        if (node_it == subscription_barrier_.end())
        {
            if (IsSnapshotSyncCompletedLocked(req->standby_node_id(),
                                              req->standby_node_term()))
            {
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
                if (!GetCompletedSnapshotTsLocked(req->standby_node_id(),
                                                  req->standby_node_term(),
                                                  &completed_snapshot_ts))
                {
                    return true;
                }
                if (completed_snapshot_ts == 0)
                {
                    return true;
                }
                DLOG(INFO) << "Received duplicate snapshot sync request for "
                              "completed standby node #"
                           << req->standby_node_id()
                           << ", standby term: " << req->standby_node_term()
                           << ", snapshot_ts=" << completed_snapshot_ts;
                should_resend_completed_snapshot = true;
                completed_ng_id = req->ng_id();
                completed_standby_node_id = req->standby_node_id();
                completed_standby_term = req->standby_node_term();
#else
                return true;
#endif
            }
            else
            {
                LOG(WARNING) << "No subscription barrier found for standby "
                                "node #"
                             << req->standby_node_id()
                             << ", standby term: " << req->standby_node_term();
                return false;
            }
        }
        else
        {
            auto barrier_it = node_it->second.find(req->standby_node_term());
            if (barrier_it == node_it->second.end())
            {
                if (IsSnapshotSyncCompletedLocked(req->standby_node_id(),
                                                  req->standby_node_term()))
                {
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
                    if (!GetCompletedSnapshotTsLocked(req->standby_node_id(),
                                                      req->standby_node_term(),
                                                      &completed_snapshot_ts))
                    {
                        return true;
                    }
                    if (completed_snapshot_ts == 0)
                    {
                        return true;
                    }
                    DLOG(INFO)
                        << "Received duplicate snapshot sync request for "
                           "completed standby node #"
                        << req->standby_node_id()
                        << ", standby term: " << req->standby_node_term()
                        << ", snapshot_ts=" << completed_snapshot_ts;
                    should_resend_completed_snapshot = true;
                    completed_ng_id = req->ng_id();
                    completed_standby_node_id = req->standby_node_id();
                    completed_standby_term = req->standby_node_term();
#else
                    return true;
#endif
                }
                else
                {
                    LOG(WARNING)
                        << "No barrier found for standby node #"
                        << req->standby_node_id()
                        << " at standby term: " << req->standby_node_term();
                    return false;
                }
            }
            else
            {
                uint64_t active_tx_max_ts = barrier_it->second;

                auto ins_pair =
                    pending_req_.try_emplace(req->standby_node_id());
                if (!ins_pair.second)
                {
                    // check if the queued task is newer than the new received
                    // req. If so, discard the new req, otherwise, update the
                    // task.
                    auto &cur_task = ins_pair.first->second;
                    int64_t cur_task_standby_node_term =
                        cur_task.req.standby_node_term();
                    int64_t req_standby_node_term = req->standby_node_term();

                    if (cur_task_standby_node_term >= req_standby_node_term)
                    {
                        // discard the task.
                        return true;
                    }
                }

                ins_pair.first->second.req.CopyFrom(*req);
                ins_pair.first->second.subscription_active_tx_max_ts =
                    active_tx_max_ts;
                standby_sync_cv_.notify_all();
                return true;
            }
        }
    }

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    if (should_resend_completed_snapshot)
    {
        return DispatchRequestSyncSnapshotAsync(completed_ng_id,
                                                completed_standby_node_id,
                                                completed_standby_term,
                                                completed_snapshot_ts,
                                                nullptr);
    }
#endif

    return true;
}

void SnapshotManager::RegisterSubscriptionBarrier(uint32_t standby_node_id,
                                                  int64_t standby_node_term,
                                                  uint64_t active_tx_max_ts)
{
    std::unique_lock<std::mutex> lk(standby_sync_mux_);
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    auto completed_it = completed_snapshot_term_and_ts_.find(standby_node_id);
    if (completed_it != completed_snapshot_term_and_ts_.end())
    {
        for (const auto &entry : completed_it->second)
        {
            int64_t completed_term = entry.first;
            if (completed_term > standby_node_term)
            {
                DLOG(INFO) << "Ignore stale subscription barrier registration "
                              "for standby node #"
                           << standby_node_id << ", term " << standby_node_term
                           << " because completed term " << completed_term
                           << " is newer";
                return;
            }
        }

        if (completed_it->second.find(standby_node_term) !=
            completed_it->second.end())
        {
            DLOG(INFO) << "Skip barrier registration for already completed "
                          "standby node #"
                       << standby_node_id << ", term " << standby_node_term;
            return;
        }
    }
#else
    auto completed_it = completed_snapshot_terms_.find(standby_node_id);
    if (completed_it != completed_snapshot_terms_.end())
    {
        for (int64_t completed_term : completed_it->second)
        {
            if (completed_term > standby_node_term)
            {
                DLOG(INFO) << "Ignore stale subscription barrier registration "
                              "for standby node #"
                           << standby_node_id << ", term " << standby_node_term
                           << " because completed term " << completed_term
                           << " is newer";
                return;
            }
        }

        if (completed_it->second.find(standby_node_term) !=
            completed_it->second.end())
        {
            DLOG(INFO) << "Skip barrier registration for already completed "
                          "standby node #"
                       << standby_node_id << ", term " << standby_node_term;
            return;
        }
    }
#endif

    // Ignore out-of-order old barrier registrations when a newer standby term
    // is already known for this standby node.
    auto node_it = subscription_barrier_.find(standby_node_id);
    if (node_it != subscription_barrier_.end())
    {
        for (const auto &entry : node_it->second)
        {
            int64_t existing_term = entry.first;
            if (existing_term > standby_node_term)
            {
                DLOG(INFO)
                    << "Ignore stale subscription barrier registration for "
                       "standby node #"
                    << standby_node_id << ", term " << standby_node_term
                    << " because newer term " << existing_term
                    << " already exists";
                return;
            }
        }
    }

    // Drop queued work from older standby terms. They are superseded by this
    // new subscription barrier and should not be synced anymore.
    auto pending_it = pending_req_.find(standby_node_id);
    if (pending_it != pending_req_.end() &&
        pending_it->second.req.standby_node_term() > standby_node_term)
    {
        DLOG(INFO) << "Ignore stale barrier registration for standby node #"
                   << standby_node_id << ", term " << standby_node_term
                   << " because queued pending task term "
                   << pending_it->second.req.standby_node_term() << " is newer";
        return;
    }

    if (pending_it != pending_req_.end() &&
        pending_it->second.req.standby_node_term() < standby_node_term)
    {
        pending_req_.erase(pending_it);
    }

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    if (completed_it != completed_snapshot_term_and_ts_.end())
    {
        auto term_it = completed_it->second.begin();
        while (term_it != completed_it->second.end())
        {
            if (term_it->first < standby_node_term)
            {
                term_it = completed_it->second.erase(term_it);
            }
            else
            {
                ++term_it;
            }
        }
        if (completed_it->second.empty())
        {
            completed_snapshot_term_and_ts_.erase(completed_it);
        }
    }
#else
    if (completed_it != completed_snapshot_terms_.end())
    {
        auto term_it = completed_it->second.begin();
        while (term_it != completed_it->second.end())
        {
            if (*term_it < standby_node_term)
            {
                term_it = completed_it->second.erase(term_it);
            }
            else
            {
                ++term_it;
            }
        }
        if (completed_it->second.empty())
        {
            completed_snapshot_terms_.erase(completed_it);
        }
    }
#endif

    auto &node_barriers = subscription_barrier_[standby_node_id];

    // Keep only current and newer terms for this node.
    auto it = node_barriers.begin();
    while (it != node_barriers.end())
    {
        if (it->first < standby_node_term)
        {
            it = node_barriers.erase(it);
        }
        else
        {
            ++it;
        }
    }

    // Keep the first registered barrier for the same standby term to make
    // duplicate reset/subscribe retries idempotent.
    if (node_barriers.find(standby_node_term) == node_barriers.end())
    {
        node_barriers[standby_node_term] = active_tx_max_ts;
    }
}

bool SnapshotManager::GetSubscriptionBarrier(uint32_t standby_node_id,
                                             int64_t standby_node_term,
                                             uint64_t *active_tx_max_ts)
{
    if (active_tx_max_ts == nullptr)
    {
        return false;
    }

    std::unique_lock<std::mutex> lk(standby_sync_mux_);
    auto node_it = subscription_barrier_.find(standby_node_id);
    if (node_it == subscription_barrier_.end())
    {
        return false;
    }

    auto barrier_it = node_it->second.find(standby_node_term);
    if (barrier_it == node_it->second.end())
    {
        return false;
    }

    *active_tx_max_ts = barrier_it->second;
    return true;
}

void SnapshotManager::EraseSubscriptionBarrier(uint32_t standby_node_id,
                                               int64_t standby_node_term)
{
    std::unique_lock<std::mutex> lk(standby_sync_mux_);
    EraseSubscriptionBarrierLocked(standby_node_id, standby_node_term);
}

void SnapshotManager::EraseSubscriptionBarriersByNode(uint32_t standby_node_id)
{
    std::unique_lock<std::mutex> lk(standby_sync_mux_);
    pending_req_.erase(standby_node_id);
    subscription_barrier_.erase(standby_node_id);
    EraseSnapshotSyncCompletedByNodeLocked(standby_node_id);
}

void SnapshotManager::EraseSubscriptionBarrierLocked(uint32_t standby_node_id,
                                                     int64_t standby_node_term)
{
    auto node_it = subscription_barrier_.find(standby_node_id);
    if (node_it == subscription_barrier_.end())
    {
        return;
    }

    node_it->second.erase(standby_node_term);
    if (node_it->second.empty())
    {
        subscription_barrier_.erase(node_it);
    }
}

bool SnapshotManager::IsSnapshotSyncCompletedLocked(
    uint32_t standby_node_id, int64_t standby_node_term) const
{
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    auto node_it = completed_snapshot_term_and_ts_.find(standby_node_id);
    if (node_it == completed_snapshot_term_and_ts_.end())
    {
        return false;
    }
#else
    auto node_it = completed_snapshot_terms_.find(standby_node_id);
    if (node_it == completed_snapshot_terms_.end())
    {
        return false;
    }
#endif
    return node_it->second.find(standby_node_term) != node_it->second.end();
}

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
void SnapshotManager::TrackSnapshotLocked(uint32_t ng_id, uint64_t snapshot_ts)
{
    snapshot_cleanup_queue_.push_back(
        {ng_id,
         snapshot_ts,
         std::chrono::system_clock::now() + kStandbySnapshotTtl});
}

void SnapshotManager::CollectExpiredSnapshotsLocked(
    std::chrono::system_clock::time_point now,
    std::vector<std::pair<uint32_t, uint64_t>> *snapshots_to_delete)
{
    if (snapshots_to_delete == nullptr)
    {
        return;
    }

    while (!snapshot_cleanup_queue_.empty())
    {
        const auto &entry = snapshot_cleanup_queue_.front();
        if (entry.expire_at > now)
        {
            break;
        }
        snapshots_to_delete->emplace_back(entry.ng_id, entry.snapshot_ts);
        EraseSnapshotSyncCompletedBySnapshotTsLocked(entry.snapshot_ts);
        snapshot_cleanup_queue_.pop_front();
    }
}

std::chrono::system_clock::time_point
SnapshotManager::NextSnapshotCleanupDeadlineLocked() const
{
    if (snapshot_cleanup_queue_.empty())
    {
        return std::chrono::system_clock::time_point::max();
    }
    return snapshot_cleanup_queue_.front().expire_at;
}

bool SnapshotManager::GetCompletedSnapshotTsLocked(
    uint32_t standby_node_id,
    int64_t standby_node_term,
    uint64_t *standby_snapshot_ts) const
{
    if (standby_snapshot_ts == nullptr)
    {
        return false;
    }

    auto node_it = completed_snapshot_term_and_ts_.find(standby_node_id);
    if (node_it == completed_snapshot_term_and_ts_.end())
    {
        return false;
    }

    auto term_it = node_it->second.find(standby_node_term);
    if (term_it == node_it->second.end())
    {
        return false;
    }

    *standby_snapshot_ts = term_it->second;
    return true;
}
#endif

void SnapshotManager::MarkSnapshotSyncCompletedLocked(
    uint32_t standby_node_id,
    int64_t standby_node_term,
    uint64_t standby_snapshot_ts)
{
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    auto &snapshot_term_and_ts =
        completed_snapshot_term_and_ts_[standby_node_id];
    snapshot_term_and_ts[standby_node_term] = standby_snapshot_ts;
    auto it = snapshot_term_and_ts.begin();
    while (it != snapshot_term_and_ts.end())
    {
        if (it->first < standby_node_term)
        {
            it = snapshot_term_and_ts.erase(it);
        }
        else
        {
            ++it;
        }
    }
#else
    auto &completed_terms = completed_snapshot_terms_[standby_node_id];
    completed_terms.insert(standby_node_term);

    auto it = completed_terms.begin();
    while (it != completed_terms.end())
    {
        if (*it < standby_node_term)
        {
            it = completed_terms.erase(it);
        }
        else
        {
            ++it;
        }
    }
    (void) standby_snapshot_ts;
#endif
}

void SnapshotManager::EraseSnapshotSyncCompletedByNodeLocked(
    uint32_t standby_node_id)
{
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    completed_snapshot_term_and_ts_.erase(standby_node_id);
#else
    completed_snapshot_terms_.erase(standby_node_id);
#endif
}

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
void SnapshotManager::EraseSnapshotSyncCompletedBySnapshotTsLocked(
    uint64_t snapshot_ts)
{
    for (auto node_it = completed_snapshot_term_and_ts_.begin();
         node_it != completed_snapshot_term_and_ts_.end();)
    {
        auto &term_and_ts = node_it->second;
        for (auto term_it = term_and_ts.begin(); term_it != term_and_ts.end();)
        {
            if (term_it->second == snapshot_ts)
            {
                term_it = term_and_ts.erase(term_it);
            }
            else
            {
                ++term_it;
            }
        }

        if (term_and_ts.empty())
        {
            node_it = completed_snapshot_term_and_ts_.erase(node_it);
        }
        else
        {
            ++node_it;
        }
    }
}
#endif

// If kvstore is enabled, we must flush data in-memory to kvstore firstly.
// For non-shared kvstore, also we create and send the snapshot to standby
// nodes.
// Then, notify standby nodes that data committed before subscribe timepoint
// has been flushed to kvstore. (standby nodes begin fetch record from
// kvstore on cache miss).
void SnapshotManager::SyncWithStandby()
{
    DLOG(INFO) << "SyncWithStandby";
    assert(store_hd_ != nullptr);
    if (store_hd_ == nullptr)
    {
        LOG(ERROR) << "Store handler is nullptr but standby feature enabled.";
        return;
    }

    using namespace txservice;
    NodeGroupId node_group = Sharder::Instance().NativeNodeGroup();
    int64_t leader_term = Sharder::Instance().LeaderTerm(node_group);
    if (leader_term < 0)
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        // clear all requests
        pending_req_.clear();
        // clear barriers as well, all queued sync states are stale when this
        // leader term is no longer valid.
        subscription_barrier_.clear();
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
        completed_snapshot_term_and_ts_.clear();
        snapshot_cleanup_queue_.clear();
#else
        completed_snapshot_terms_.clear();
#endif
        return;
    }

    uint32_t current_subscribe_id = Sharder::Instance().GetCurrentSubscribeId();

    uint64_t cur_ckpt_ts = GetCurrentCheckpointTs(node_group);

    std::vector<PendingSnapshotSyncTask> tasks;

    // Dequeue all pending tasks that can be covered by this snapshot.
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        auto it = pending_req_.begin();
        while (it != pending_req_.end())
        {
            uint32_t pending_node_id = it->first;
            auto &pending_task = it->second;
            int64_t pending_task_standby_node_term =
                pending_task.req.standby_node_term();
            int64_t pending_task_primary_term =
                PrimaryTermFromStandbyTerm(pending_task_standby_node_term);

            if (pending_task_primary_term < leader_term)
            {
                // discard the task.
                EraseSubscriptionBarrierLocked(pending_node_id,
                                               pending_task_standby_node_term);
                it = pending_req_.erase(it);
                continue;
            }
            else if (pending_task_primary_term > leader_term)
            {
                // A new term is requested. Our current snapshot has expired.
                assert(!Sharder::Instance().CheckLeaderTerm(node_group,
                                                            leader_term));
                return;
            }

            bool covered = true;
            uint32_t pending_task_subscribe_id =
                SubscribeIdFromStandbyTerm(pending_task_standby_node_term);

            if (pending_task_subscribe_id >= current_subscribe_id)
            {
                LOG(WARNING)
                    << "The snapshot generated at subscribe counter: "
                    << current_subscribe_id
                    << ", does not cover the task subscribe id: "
                    << pending_task_subscribe_id << ". Wait for next round";
                covered = false;
            }
            else if (cur_ckpt_ts <= pending_task.subscription_active_tx_max_ts)
            {
                LOG(INFO) << "Current checkpoint ts " << cur_ckpt_ts
                          << " does not pass subscription barrier ts "
                          << pending_task.subscription_active_tx_max_ts
                          << " for standby node #"
                          << pending_task.req.standby_node_id()
                          << ", standby term "
                          << pending_task.req.standby_node_term()
                          << ". Wait for next round";
                covered = false;
            }

            if (!covered)
            {
                // requested version is newer than cur snapshot. Wait till next
                // round.
                it++;
                continue;
            }
            // Keep a copy for current round execution. Do not move out of
            // pending_req_, because completion logic still needs the original
            // standby term to decide whether the queued task is stale.
            tasks.emplace_back(pending_task);

            // Keep the request entry so completion logic can check whether it
            // needs to stay queued, but make sure to advance the iterator so we
            // don't loop on the
            // same element indefinitely.
            it++;
        }
    }

    if (tasks.empty())
    {
        return;
    }

    bool ckpt_res = this->RunOneRoundCheckpoint(node_group, leader_term);
    if (!ckpt_res)
    {
        // Data flush failed. Retry on next run.
        LOG(ERROR) << "Failed to do checkpoint on SyncWithStandby";
        return;
    }

    std::vector<std::string> snapshot_files;
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    // Create a fresh standby snapshot after the checkpoint completed.
    const uint64_t standby_snapshot_ts = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    bool res = store_hd_->CreateSnapshotForStandby(
        node_group, snapshot_files, standby_snapshot_ts);
    if (!res)
    {
        LOG(ERROR) << "Failed to create snpashot for sync with standby";
        return;
    }
    {
        std::unique_lock<std::mutex> lk(standby_sync_mux_);
        TrackSnapshotLocked(node_group, standby_snapshot_ts);
    }
    DLOG(INFO) << "SyncWithStandby created standby snapshot, ng_id="
               << node_group << ", term=" << leader_term
               << ", snapshot_ts=" << standby_snapshot_ts;
    size_t sync_snapshot_rpc_count = 0;
    size_t sync_snapshot_success = 0;
    RequestSyncSnapshotAggregate sync_snapshot_agg;
#endif

    for (auto &task : tasks)
    {
        auto &req = task.req;
        uint32_t node_id = req.standby_node_id();
        int64_t req_standby_node_term = req.standby_node_term();

        // Skip stale copied tasks that have already been superseded/removed
        // after this round snapshot task list was built.
        {
            std::unique_lock<std::mutex> lk(standby_sync_mux_);
            auto pending_it = pending_req_.find(node_id);
            if (pending_it == pending_req_.end() ||
                pending_it->second.req.standby_node_term() !=
                    req_standby_node_term)
            {
                continue;
            }
        }

        std::string ip;
        uint16_t port;
        Sharder::Instance().GetNodeAddress(node_id, ip, port);
#ifndef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
        std::string remote_dest = req.user() + "@" + ip + ":" + req.dest_path();
#endif
        int64_t req_primary_term =
            PrimaryTermFromStandbyTerm(req_standby_node_term);

        if (!Sharder::Instance().CheckLeaderTerm(req.ng_id(), req_primary_term))
        {
            // Abort immediately if term no longer match.
            return;
        }

        bool succ = true;
#ifndef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
        if (!snapshot_files.empty())
        {
            succ = store_hd_->SendSnapshotToRemote(
                req.ng_id(), req_primary_term, snapshot_files, remote_dest);
        }
#else
        assert(snapshot_files.empty());
#endif

        if (succ)
        {
            bool notify_succ = false;
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
            notify_succ =
                DispatchRequestSyncSnapshotAsync(req.ng_id(),
                                                 req.standby_node_id(),
                                                 req.standby_node_term(),
                                                 standby_snapshot_ts,
                                                 &sync_snapshot_agg);
            if (notify_succ)
            {
                sync_snapshot_rpc_count++;
            }
#else
            auto channel = Sharder::Instance().GetCcNodeServiceChannel(
                req.standby_node_id());
            DLOG(INFO) << "Notifying standby node #" << req.standby_node_id()
                       << " for snapshot synced at term "
                       << req.standby_node_term()
                       << ", channel: " << (channel ? "valid" : "null");
            if (channel)
            {
                // needs retry if failed
                // since the standby node may be still spinning up.
                remote::CcRpcService_Stub stub(channel.get());
                int retry_times = 5;
                while (retry_times-- > 0)
                {
                    brpc::Controller cntl;
                    cntl.set_timeout_ms(1000);
                    remote::OnSnapshotSyncedRequest on_synced_req;
                    remote::OnSnapshotSyncedResponse on_sync_resp;
                    on_synced_req.set_snapshot_path(req.dest_path());
                    on_synced_req.set_standby_node_term(
                        req.standby_node_term());
                    on_synced_req.set_ng_id(req.ng_id());
                    stub.OnSnapshotSynced(
                        &cntl, &on_synced_req, &on_sync_resp, nullptr);
                    if (cntl.Failed())
                    {
                        LOG(WARNING) << "OnSnapshotSynced to standby node #"
                                     << req.standby_node_id() << " failed, "
                                     << " error: " << cntl.ErrorText()
                                     << " error code: " << cntl.ErrorCode();
                        // sleep 1 second and retry
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        continue;
                    }
                    if (on_sync_resp.error())
                    {
                        LOG(WARNING)
                            << "OnSnapshotSynced to standby node #"
                            << req.standby_node_id()
                            << " returned error response at standby term "
                            << req.standby_node_term();
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        continue;
                    }

                    notify_succ = true;
                    break;
                }
            }
            else
            {
                LOG(WARNING) << "OnSnapshotSynced channel is nullptr for "
                             << "standby node #" << req.standby_node_id()
                             << " at standby term " << req.standby_node_term();
            }
#endif

            if (!notify_succ)
            {
                // Keep pending/barrier so standby can retry with the same
                // standby term.
                continue;
            }

            {
                std::unique_lock<std::mutex> lk(standby_sync_mux_);
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
                const uint64_t completed_snapshot_ts = standby_snapshot_ts;
#else
                const uint64_t completed_snapshot_ts = 0;
#endif
                MarkSnapshotSyncCompletedLocked(req.standby_node_id(),
                                                req.standby_node_term(),
                                                completed_snapshot_ts);
                auto pending_req_iter =
                    pending_req_.find(req.standby_node_id());
                if (pending_req_iter != pending_req_.end())
                {
                    // Check again to see if the request has been updated.
                    auto &next_pending_task = pending_req_iter->second;
                    int64_t next_pending_task_standby_term =
                        next_pending_task.req.standby_node_term();
                    int64_t next_pending_task_primary_term =
                        PrimaryTermFromStandbyTerm(
                            next_pending_task_standby_term);

                    assert(PrimaryTermFromStandbyTerm(
                               req.standby_node_term()) == leader_term);

                    if (next_pending_task_primary_term < leader_term)
                    {
                        EraseSubscriptionBarrierLocked(
                            req.standby_node_id(),
                            next_pending_task_standby_term);
                        pending_req_.erase(pending_req_iter);
                    }
                    else if (next_pending_task_primary_term == leader_term)
                    {
                        uint32_t next_pending_task_subscribe_id =
                            SubscribeIdFromStandbyTerm(
                                next_pending_task_standby_term);
                        uint32_t cur_task_subscribe_id =
                            SubscribeIdFromStandbyTerm(req.standby_node_term());
                        if (next_pending_task_subscribe_id <=
                            cur_task_subscribe_id)
                        {
                            EraseSubscriptionBarrierLocked(
                                req.standby_node_id(),
                                next_pending_task_standby_term);
                            pending_req_.erase(pending_req_iter);
                        }
                    }
                }
            }

            EraseSubscriptionBarrier(req.standby_node_id(),
                                     req.standby_node_term());
        }
    }
#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    {
        std::unique_lock<bthread::Mutex> lk(sync_snapshot_agg.mux);
        while (sync_snapshot_agg.pending != 0)
        {
            sync_snapshot_agg.cv.wait(lk);
        }
        sync_snapshot_success = sync_snapshot_agg.success;
    }
#endif

#ifdef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
    DLOG(INFO) << "SyncWithStandby dispatched bootstrap snapshot, ng_id="
               << node_group << ", term=" << leader_term
               << ", current_snapshot_ts=" << standby_snapshot_ts
               << ", sync_snapshot_rpc_count=" << sync_snapshot_rpc_count
               << ", sync_snapshot_success=" << sync_snapshot_success;
#endif
}

uint64_t SnapshotManager::GetCurrentCheckpointTs(uint32_t node_group)
{
    auto local_shards = Sharder::Instance().GetLocalCcShards();
    assert(local_shards != nullptr);
    if (local_shards == nullptr)
    {
        return 0;
    }

    CkptTsCc ckpt_req(local_shards->Count(), node_group);
    for (size_t i = 0; i < local_shards->Count(); i++)
    {
        local_shards->EnqueueCcRequest(i, &ckpt_req);
    }
    ckpt_req.Wait();
    return ckpt_req.GetCkptTs();
}

bool SnapshotManager::RunOneRoundCheckpoint(uint32_t node_group,
                                            int64_t ng_leader_term)
{
    using namespace txservice;
    auto &local_shards = *Sharder::Instance().GetLocalCcShards();

    // Get table names in this node group, checkpointer should be TableName
    // string owner.
    // Use max number for ckpt ts to flush all in memory data to kv.
    std::unordered_map<TableName, bool> tables =
        local_shards.GetCatalogTableNameSnapshot(node_group, UINT64_MAX);

    std::shared_ptr<DataSyncStatus> data_sync_status =
        std::make_shared<DataSyncStatus>(node_group, ng_leader_term, true);
    data_sync_status->SetNoTruncateLog();

    bool can_be_skipped = false;
    uint64_t last_ckpt_ts = Sharder::Instance().GetNodeGroupCkptTs(node_group);

    // Iterate all the tables and execute CkptScanCc requests on this node
    // group's ccmaps on each ccshard. The result of CkptScanCc is stored in
    // ckpt_vec.
    for (auto it = tables.begin(); it != tables.end(); ++it)
    {
        const TableName &table_name = it->first;
        bool is_dirty = it->second;
        // This should correspond to CcShard::ActiveTxMinTs.
        if (!table_name.IsMeta())
        {
            // Skip the table if it's not updated since last sync ts.
            GetTableLastCommitTsCc get_commit_ts_cc(
                table_name, node_group, local_shards.Count());
            for (size_t core = 0; core < local_shards.Count(); core++)
            {
                local_shards.EnqueueCcRequest(core, &get_commit_ts_cc);
            }
            get_commit_ts_cc.Wait();

            if (get_commit_ts_cc.LastCommitTs() < last_ckpt_ts)
            {
                continue;
            }

            uint64_t table_last_synced_ts = UINT64_MAX;
            local_shards.EnqueueDataSyncTaskForTable(table_name,
                                                     node_group,
                                                     ng_leader_term,
                                                     UINT64_MAX,
                                                     table_last_synced_ts,
                                                     false,
                                                     is_dirty,
                                                     can_be_skipped,
                                                     data_sync_status);
        }
    }

    std::unique_lock<std::mutex> task_sender_lk(data_sync_status->mux_);
    data_sync_status->all_task_started_ = true;

    // Waiting for the flush task to be completed.
    data_sync_status->cv_.wait(
        task_sender_lk,
        [&data_sync_status]
        { return data_sync_status->unfinished_tasks_ == 0; });

    DLOG_IF(WARNING, data_sync_status->err_code_ != CcErrorCode::NO_ERROR)
        << "SyncWithStandby for node_group: " << node_group
        << " term: " << ng_leader_term
        << " DataSync error: " << int(data_sync_status->err_code_);
    DLOG_IF(WARNING, data_sync_status->has_skipped_entries_)
        << "SyncWithStandby for node_group: " << node_group
        << " term: " << ng_leader_term
        << " DataSyncScan has skipped some entries, return checkpoint failure";

    return (data_sync_status->err_code_ == CcErrorCode::NO_ERROR &&
            !data_sync_status->has_skipped_entries_);
}

void SnapshotManager::UpdateBackupTaskStatus(
    txservice::remote::CreateBackupRequest *task_ptr,
    txservice::remote::BackupTaskStatus st,
    const std::vector<std::string> &snapshot_files,
    const uint64_t checkpoint_ts)
{
    std::unique_lock<std::mutex> lk(backup_tasks_mux_);
    task_ptr->set_status(st);
    for (const auto &f : snapshot_files)
    {
        task_ptr->add_backup_files(f);
    }
    task_ptr->set_backup_ts(checkpoint_ts);
}

void SnapshotManager::HandleBackupTask(
    txservice::remote::CreateBackupRequest *task_ptr)
{
    using namespace txservice;
    uint32_t node_group = task_ptr->ng_id();
    int64_t leader_term = task_ptr->ng_term();
    const std::string backup_name = task_ptr->backup_name();
    assert(leader_term > 0);
    assert(store_hd_ != nullptr);

    if (!txservice::Sharder::Instance().CheckLeaderTerm(node_group,
                                                        leader_term))
    {
        std::unique_lock<std::mutex> lk(backup_tasks_mux_);
        task_ptr->set_status(txservice::remote::BackupTaskStatus::Failed);
        LOG(INFO) << "NodeGroup term changed, terminate to create "
                     "backup, name: "
                  << backup_name;
        return;
    }
    LOG(INFO) << "Begin to create backup, name: " << backup_name;

    {
        std::unique_lock<std::mutex> lk(backup_tasks_mux_);
        if (task_ptr->status() !=
            txservice::remote::BackupTaskStatus::Terminating)
        {
            task_ptr->set_status(txservice::remote::BackupTaskStatus::Running);
        }
        else
        {
            task_ptr->set_status(txservice::remote::BackupTaskStatus::Failed);
            LOG(INFO) << "Terminated backup, name: " << backup_name;
            return;
        }
    }

    bool ckpt_res = this->RunOneRoundCheckpoint(node_group, leader_term);
    if (!ckpt_res)
    {
        task_ptr->set_status(txservice::remote::BackupTaskStatus::Failed);
        LOG(INFO) << "Failed to do backup for checkpoint failed, name: "
                  << backup_name;

        return;
    }

    // Fetch last checkpoint ts of the recent checkpoint.
    uint64_t last_ckpt_ts = Sharder::Instance().GetNodeGroupCkptTs(node_group);

    {
        std::unique_lock<std::mutex> lk(backup_tasks_mux_);
        if (task_ptr->status() ==
            txservice::remote::BackupTaskStatus::Terminating)
        {
            task_ptr->set_status(txservice::remote::BackupTaskStatus::Failed);
            LOG(INFO) << "Terminated backup, name: " << backup_name;
            return;
        }
    }

    // Now take a snapshot for non-shared storage, and then send to dest path.
    // For shared storage, need user to call storage to create snapshot.
    std::vector<std::string> snapshot_files;

    if (store_hd_->IsSharedStorage())
    {
#if (defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||  \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS) || \
     defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE))
        // For shared storage with cloud filesystem enabled, create snapshot
        bool res = store_hd_->CreateSnapshotForBackup(
            backup_name, snapshot_files, last_ckpt_ts);
        if (!res)
        {
            LOG(ERROR) << "Failed to create snapshot for backup in shared "
                          "storage mode";
            this->UpdateBackupTaskStatus(
                task_ptr, txservice::remote::BackupTaskStatus::Failed);
            store_hd_->RemoveBackupSnapshot(backup_name);
            return;
        }
        LOG(INFO) << "Backup finished with snapshot creation, name:"
                  << backup_name;
#else
        LOG(INFO) << "Backup finished, name:" << backup_name;
#endif

        this->UpdateBackupTaskStatus(
            task_ptr,
            txservice::remote::BackupTaskStatus::Finished,
            snapshot_files,
            last_ckpt_ts);
        return;
    }
    else
    {
        bool res = store_hd_->CreateSnapshotForBackup(
            backup_name, snapshot_files, last_ckpt_ts);
        if (!res)
        {
            LOG(ERROR) << "Failed to create snpashot for backup";
            this->UpdateBackupTaskStatus(
                task_ptr, txservice::remote::BackupTaskStatus::Failed);
            store_hd_->RemoveBackupSnapshot(backup_name);
            return;
        }
    }

    if (task_ptr->dest_path().empty())
    {
        this->UpdateBackupTaskStatus(
            task_ptr,
            txservice::remote::BackupTaskStatus::Finished,
            snapshot_files,
            last_ckpt_ts);
        LOG(INFO) << "Backup task is finished, backup name: " << backup_name
                  << ", dest_path is not set.";
        return;
    }

    assert(!snapshot_files.empty());
    if (snapshot_files.empty())
    {
        DLOG(INFO) << "snapshot_files is empty";
        this->UpdateBackupTaskStatus(
            task_ptr,
            txservice::remote::BackupTaskStatus::Finished,
            snapshot_files,
            last_ckpt_ts);
        return;
    }
    else
    {
        std::atomic<bool> succ{true};
        if (!Sharder::Instance().CheckLeaderTerm(node_group, leader_term))
        {
            // Abort immediately if term no longer match.
            succ.store(false, std::memory_order_relaxed);
            LOG(WARNING) << "Terminate backup task for leader term changed.";

            this->UpdateBackupTaskStatus(
                task_ptr, txservice::remote::BackupTaskStatus::Failed);
            store_hd_->RemoveBackupSnapshot(backup_name);

            return;
        }

        // Send the first snapshot file to remote node in order to create
        // non-existent directory. Otherwise, multiple rsync processes
        // creating a non-existent directory at the same time will result in
        // an error.

        std::string remote_dest;
        if (!task_ptr->dest_user().empty() && !task_ptr->dest_host().empty())
        {
            remote_dest = task_ptr->dest_user() + "@" + task_ptr->dest_host() +
                          ":" + task_ptr->dest_path();
        }
        else
        {
            remote_dest = task_ptr->dest_path();
        }
        if (remote_dest.back() != '/')
        {
            remote_dest.append("/");
        }
        remote_dest.append(std::to_string(node_group));
        remote_dest.append("/");

        bool send_result = store_hd_->SendSnapshotToRemote(
            node_group, leader_term, snapshot_files, remote_dest);
        if (send_result)
        {
            LOG(INFO) << "Backup task is finished, backup name: " << backup_name
                      << ", dest_path:" << remote_dest;
            this->UpdateBackupTaskStatus(
                task_ptr,
                txservice::remote::BackupTaskStatus::Finished,
                snapshot_files,
                last_ckpt_ts);
        }
        else
        {
            LOG(ERROR)
                << "Failed to send backup files to dest path, backup name: "
                << backup_name << ", dest_path:" << remote_dest;
            this->UpdateBackupTaskStatus(
                task_ptr, txservice::remote::BackupTaskStatus::Failed);
        }

        // remove local temporary store directory of backup files.
        bool remove_bucket_result =
            store_hd_->RemoveBackupSnapshot(backup_name);
        if (remove_bucket_result)
        {
            LOG(INFO) << "Removed local temporary store directory of "
                         "backup files , backup name:"
                      << backup_name;
        }
        else
        {
            LOG(ERROR) << "Failed to remove local temporary store directory of "
                          "backup files , backup name:"
                       << backup_name;
        }
    }
}

txservice::remote::BackupTaskStatus SnapshotManager::CreateBackup(
    const txservice::remote::CreateBackupRequest *req)
{
    if (store_hd_ == nullptr)
    {
        return txservice::remote::BackupTaskStatus::Failed;
    }

    txservice::remote::CreateBackupRequest *task_ptr;
    {
        std::unique_lock<std::mutex> lk(backup_tasks_mux_);

        if (backup_task_queue_.size() >= MaxBackupTaskCount)
        {
            auto tmp_status = backup_task_queue_.front()->status();
            if (tmp_status != txservice::remote::BackupTaskStatus::Failed &&
                tmp_status != txservice::remote::BackupTaskStatus::Finished)
            {
                return txservice::remote::BackupTaskStatus::Failed;
            }
            txservice::NodeGroupId ng_id = backup_task_queue_.front()->ng_id();
            const std::string &backup_name =
                backup_task_queue_.front()->backup_name();
            if (!store_hd_->IsSharedStorage())
            {
                store_hd_->RemoveBackupSnapshot(backup_name);
            }
            ng_backup_tasks_.at(ng_id).erase(backup_name);
            backup_task_queue_.pop_front();
        }

        txservice::NodeGroupId ng_id = req->ng_id();
        const std::string &backup_name = req->backup_name();
        auto ng_it = ng_backup_tasks_.find(ng_id);
        if (ng_it != ng_backup_tasks_.end())
        {
            auto backup_it = ng_it->second.find(backup_name);
            if (backup_it != ng_it->second.end())
            {
                // Idempotency: if the backup already exists and is finished,
                // return success. This allows retrying CreateBackup after a
                // previous successful completion.
                if (backup_it->second.status() ==
                    txservice::remote::BackupTaskStatus::Finished)
                {
                    return txservice::remote::BackupTaskStatus::Finished;
                }
                // If backup exists but is not finished, it's an error
                return txservice::remote::BackupTaskStatus::Failed;
            }
        }
        else
        {
            ng_backup_tasks_.try_emplace(ng_id);
        }

        auto ins_pair = ng_backup_tasks_.at(ng_id).try_emplace(backup_name);
        assert(ins_pair.second);
        auto &task = ins_pair.first->second;
        backup_task_queue_.push_back(&task);
        task.CopyFrom(*req);
        task.set_status(txservice::remote::BackupTaskStatus::Inited);
        task_ptr = &task;

        int64_t ng_term =
            txservice::Sharder::Instance().LeaderTerm(task.ng_id());
        task.set_ng_term(ng_term);
    }

    backup_worker_.SubmitWork([this, task_ptr](size_t)
                              { HandleBackupTask(task_ptr); });
    return txservice::remote::BackupTaskStatus::Inited;
}

txservice::remote::BackupTaskStatus SnapshotManager::GetBackupStatus(
    txservice::NodeGroupId ng_id,
    const std::string &backup_name,
    ::txservice::remote::FetchBackupResponse *response)
{
    if (store_hd_ == nullptr)
    {
        return txservice::remote::BackupTaskStatus::Failed;
    }

    std::unique_lock<std::mutex> lk(backup_tasks_mux_);
    auto ng_it = ng_backup_tasks_.find(ng_id);
    if (ng_it != ng_backup_tasks_.end())
    {
        auto backup_it = ng_it->second.find(backup_name);
        if (backup_it != ng_it->second.end())
        {
            auto backup_status = backup_it->second.status();
            if (backup_status == txservice::remote::BackupTaskStatus::Finished)
            {
                for (const auto &f : backup_it->second.backup_files())
                {
                    response->add_backup_files(f);
                }
                response->set_backup_ts(backup_it->second.backup_ts());
            }

            return backup_status;
        }
    }

    return txservice::remote::BackupTaskStatus::Unknown;
}

void SnapshotManager::TerminateBackup(txservice::NodeGroupId ng_id,
                                      const std::string &backup_name)
{
    if (store_hd_ == nullptr)
    {
        return;
    }
    using namespace txservice::remote;
    std::unique_lock<std::mutex> lk(backup_tasks_mux_);
    auto ng_it = ng_backup_tasks_.find(ng_id);
    if (ng_it != ng_backup_tasks_.end())
    {
        auto backup_it = ng_it->second.find(backup_name);
        if (backup_it != ng_it->second.end() &&
            backup_it->second.status() != BackupTaskStatus::Failed &&
            backup_it->second.status() != BackupTaskStatus::Finished)
        {
            backup_it->second.set_status(BackupTaskStatus::Terminating);
        }
    }
}

}  // namespace store
}  // namespace txservice
