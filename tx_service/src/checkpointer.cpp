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
#include "checkpointer.h"

#include <brpc/controller.h>

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "catalog_key_record.h"
#include "cc_request.h"
#include "error_messages.h"
#include "metrics.h"
#include "range_slice.h"
#include "sharder.h"
#include "standby.h"
#include "statistics.h"
#include "tx_service_metrics.h"
#include "tx_start_ts_collector.h"

namespace txservice
{
extern bool txservice_skip_wal;
#if defined(DISABLE_CKPT_REPORT) && !defined(DEBUG)
DEFINE_bool(report_ckpt, false, "Print log on do checkpoint.");
#else
DEFINE_bool(report_ckpt, true, "Print log on do checkpoint.");
#endif
DEFINE_int32(
    ckpt_stall_warn_rounds,
    3,
    "Log a warning when a node group's checkpoint timestamp fails to advance "
    "for this many consecutive rounds, naming the pinning transaction. 0 "
    "disables the warning.");

bool PassValidate(const char *, bool)
{
    return true;
}
bool PassValidateInt32(const char *, int32_t)
{
    return true;
}
BRPC_VALIDATE_GFLAG(report_ckpt, PassValidate);
BRPC_VALIDATE_GFLAG(ckpt_stall_warn_rounds, PassValidateInt32);

Checkpointer::Checkpointer(LocalCcShards &shards,
                           store::DataStoreHandler *write_hd,
                           const uint32_t &checkpoint_interval,
                           TxLog *log_agent,
                           uint32_t ckpt_delay_seconds,
                           uint32_t min_ckpt_request_interval)
    : local_shards_(shards),
      ckpt_mux_(),
      ckpt_cv_(),
      store_hd_(write_hd),
      ckpt_thd_status_(Status::Active),
      checkpoint_interval_(checkpoint_interval),
      min_ckpt_request_interval_(min_ckpt_request_interval),
      last_checkpoint_ts_(std::chrono::system_clock::now()),
      // Initialize last_checkpoint_request_ts_ to a time point that is
      // sufficiently in the past, so that the first checkpoint request can be
      // processed immediately
      last_checkpoint_request_ts_(
          std::chrono::system_clock::now() -
          std::chrono::seconds(2 * min_ckpt_request_interval)),
      ckpt_delay_time_(ckpt_delay_seconds * 1000000),
      ongoing_data_sync_cnt_(0),
      tx_service_(nullptr),
      log_agent_(log_agent)
{
    tx_service_ = shards.tx_service_;
    for (std::unique_ptr<CcShard> &ccs : shards.cc_shards_)
    {
        ccs->ckpter_ = this;
    }

    if (store_hd_)
    {
        thd_ = std::thread([this] { Run(); });
        pthread_setname_np(thd_.native_handle(), "checkpointer");
    }

    DLOG(INFO) << "checkpointer init, checkpoint_interval_: "
               << checkpoint_interval_
               << " ,min_ckpt_request_interval_: " << min_ckpt_request_interval_
               << " ,ckpt_delay_seconds: " << ckpt_delay_seconds;
}

std::pair<uint64_t, uint64_t> Checkpointer::GetNewCheckpointTs(
    uint32_t node_group_id,
    bool is_last_ckpt,
    CkptTsCc::PinningTxInfo &pinning_tx)
{
    size_t core_cnt = local_shards_.Count();
    CkptTsCc ckpt_req(core_cnt, node_group_id);

    // Find minimum ckpt_ts from all the ccshards in parallel. ckpt_ts is
    // the minimum timestamp minus 1 among all the active transactions, thus
    // it's safe to flush all the entries smaller than or equal to ckpt_ts.
    for (auto &ccs : local_shards_.cc_shards_)
    {
        ccs->Enqueue(&ckpt_req);
    }
    ckpt_req.Wait();
    if (FLAGS_report_ckpt)
    {
        ckpt_req.ShardMemoryUsageReport();
    }
    local_shards_.TableRangeHeapUsageReport();

    ckpt_req.UpdateStandbyConsistentTs();

    pinning_tx = ckpt_req.GetPinningTx();

    uint64_t ckpt_ts = UINT64_MAX;
    ckpt_ts = ckpt_req.GetCkptTs();

    if (local_shards_.EnableMvcc() && !is_last_ckpt)
    {
        uint64_t min_si_tx_ts =
            TxStartTsCollector::Instance().GlobalMinSiTxStartTs();
        uint64_t delayed_ckpt_ts = ckpt_req.GetCkptTs() - ckpt_delay_time_;
        if (min_si_tx_ts < delayed_ckpt_ts)
        {
            ckpt_ts = delayed_ckpt_ts;
        }
        else if (min_si_tx_ts < ckpt_req.GetCkptTs())
        {
            ckpt_ts = min_si_tx_ts;
        }
    }

    return {ckpt_ts, ckpt_req.GetMemUsage()};
}

void Checkpointer::CollectCkptStallMetric(uint32_t node_group, uint32_t rounds)
{
    if (!metrics::enable_metrics)
    {
        return;
    }

    metrics::Meter *meter = local_shards_.GetNodeMeter();
    if (meter != nullptr)
    {
        meter->Collect(metrics::NAME_CHECKPOINT_STALL_ROUNDS,
                       static_cast<double>(rounds),
                       std::to_string(node_group));
    }
}

void Checkpointer::WarnIfCkptStalled(uint32_t node_group,
                                     uint64_t ckpt_ts,
                                     uint64_t last_ckpt_ts,
                                     const CkptTsCc::PinningTxInfo &pinning_tx)
{
    // Counted (and exported) before the flag is consulted: ckpt_stall_warn_
    // rounds gates the log line only. Gating the count on it would make
    // setting the flag to 0 freeze the gauge at 0 for the whole stall.
    auto stall_it = ckpt_stall_states_.try_emplace(node_group).first;
    CkptStallState &stall = stall_it->second;
    uint32_t rounds = ++stall.stall_rounds_;
    CollectCkptStallMetric(node_group, rounds);

    if (FLAGS_ckpt_stall_warn_rounds <= 0)
    {
        return;
    }

    if (rounds < static_cast<uint32_t>(FLAGS_ckpt_stall_warn_rounds))
    {
        return;
    }

    // Rate-limit to one warning per node group per minute: under memory
    // pressure checkpoint rounds run every few seconds.
    auto now = std::chrono::steady_clock::now();
    if (now - stall.last_warn_time_ < std::chrono::seconds(60))
    {
        return;
    }
    stall.last_warn_time_ = now;

    if (pinning_tx.txn_ != 0)
    {
        // wlock_ts_ comes from the shard ts base, which tracks the system
        // clock; the derived age is approximate but adequate for diagnosis.
        uint64_t now_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        uint64_t age_seconds = now_us > pinning_tx.wlock_ts_
                                   ? (now_us - pinning_tx.wlock_ts_) / 1000000
                                   : 0;
        LOG(WARNING) << "Checkpoint of node group #" << node_group
                     << " has not advanced for " << rounds
                     << " consecutive rounds: ckpt_ts=" << ckpt_ts
                     << " <= last_ckpt_ts=" << last_ckpt_ts
                     << ". Pinned by txn " << pinning_tx.txn_ << " on core #"
                     << pinning_tx.core_id_
                     << ", which acquired its first write lock " << age_seconds
                     << "s ago (wlock_ts=" << pinning_tx.wlock_ts_
                     << "). If this transaction never finishes, dirty data "
                        "cannot be flushed and memory cannot be reclaimed.";
    }
    else
    {
        LOG(WARNING) << "Checkpoint of node group #" << node_group
                     << " has not advanced for " << rounds
                     << " consecutive rounds: ckpt_ts=" << ckpt_ts
                     << " <= last_ckpt_ts=" << last_ckpt_ts
                     << ". No write-lock-holding transaction found; the "
                        "timestamp may be capped by the MVCC minimum start ts "
                        "or standby state.";
    }
}

void Checkpointer::Ckpt(bool is_last_ckpt)
{
    if (local_shards_.Count() == 0 || store_hd_ == nullptr)
    {
        return;
    }
    int64_t candidate_standby_node_term =
        Sharder::Instance().CandidateStandbyNodeTerm();
    if (candidate_standby_node_term > 0)
    {
        // request snapshot from primary if standby is not synced on every
        // checkpoint attempt. This request can be called multiple times and
        // will be deduped on the primary node based on requested term.

        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(
                Sharder::Instance().LeaderNodeId(
                    Sharder::Instance().NativeNodeGroup()));
        if (channel != nullptr)
        {
            remote::CcRpcService_Stub stub(channel.get());
            remote::StorageSnapshotSyncRequest snapshot_req;
            remote::StorageSnapshotSyncResponse snapshot_resp;

            snapshot_req.set_ng_id(Sharder::Instance().NativeNodeGroup());
            // standby node term
            snapshot_req.set_standby_node_term(candidate_standby_node_term);
            snapshot_req.set_standby_node_id(Sharder::Instance().NodeId());

#ifndef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
            std::array<char, 200> buffer;
            std::string username;
            FILE *output_stream = popen("echo $USER", "r");
            while (fgets(buffer.data(), 200, output_stream) != nullptr)
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

            snapshot_req.set_user(username);
            const std::string dest_path = store_hd_->SnapshotSyncDestPath();
            snapshot_req.set_dest_path(dest_path);
#endif
            DLOG(INFO) << "Checkpointer send RequestStorageSnapshotSync, ng_id="
                       << snapshot_req.ng_id()
                       << ", standby_node_id=" << snapshot_req.standby_node_id()
                       << ", standby_node_term="
                       << snapshot_req.standby_node_term();
            brpc::Controller cntl;
            stub.RequestStorageSnapshotSync(
                &cntl, &snapshot_req, &snapshot_resp, nullptr);
        }

        return;
    }

    int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
    bool is_standby_node = standby_node_term > 0;
    if (is_standby_node
#ifndef DATA_STORE_TYPE_ELOQDSS_ELOQSTORE
        && Sharder::Instance().GetDataStoreHandler()->IsSharedStorage()
#endif
    )
    {
        // Standby only needs to do checkpoint if its using local disk storage.
        return;
    }

    std::vector<uint32_t> node_groups;
    if (is_standby_node)
    {
        node_groups.push_back(Sharder::Instance().NativeNodeGroup());
        assert(!Sharder::Instance().GetDataStoreHandler()->IsSharedStorage());
    }
    else
    {
        node_groups = Sharder::Instance().LocalNodeGroups();
    }

    for (uint32_t node_group : node_groups)
    {
        int64_t leader_term = -1;
        if (!is_standby_node)
        {
            int64_t ng_candidate_leader_term =
                Sharder::Instance().CandidateLeaderTerm(node_group);
            int64_t ng_leader_term = Sharder::Instance().LeaderTerm(node_group);
            leader_term = std::max(ng_candidate_leader_term, ng_leader_term);
        }

        if (!is_standby_node && leader_term < 0)
        {
            continue;
        }

        CkptTsCc::PinningTxInfo pinning_tx;
        auto [ckpt_ts, mem_usage] =
            GetNewCheckpointTs(node_group, is_last_ckpt, pinning_tx);
        uint64_t last_ckpt_ts =
            Sharder::Instance().GetNodeGroupCkptTs(node_group);

        if (ckpt_ts <= last_ckpt_ts)
        {
            WarnIfCkptStalled(node_group, ckpt_ts, last_ckpt_ts, pinning_tx);
            continue;
        }
        CollectCkptStallMetric(node_group, 0);
        auto stall_it = ckpt_stall_states_.find(node_group);
        if (stall_it != ckpt_stall_states_.end())
        {
            stall_it->second.stall_rounds_ = 0;
        }

        LOG_IF(INFO, FLAGS_report_ckpt)
            << "Begin checkpoint of node group #" << node_group
            << " with timestamp: " << ckpt_ts
            << ". The memory usage of node is: " << mem_usage << " KB.";

        // Get table names in this node group, checkpointer should be TableName
        // string owner.
        std::unordered_map<TableName, bool> tables =
            local_shards_.GetCatalogTableNameSnapshot(node_group, ckpt_ts);

        std::shared_ptr<DataSyncStatus> status =
            std::make_shared<DataSyncStatus>(
                node_group,
                is_standby_node ? standby_node_term : leader_term,
                true);

        uint64_t last_succ_ckpt_ts = UINT64_MAX;
        bool can_be_skipped = !is_last_ckpt;

        // Iterate all the tables and execute CkptScanCc requests on this node
        // group's ccmaps on each ccshard. The result of CkptScanCc is stored in
        // ckpt_vec.
        for (auto it = tables.begin(); it != tables.end(); ++it)
        {
            // Check leader term for leader node
            if (!is_standby_node &&
                Sharder::Instance().LeaderTerm(node_group) != leader_term)
            {
                break;
            }

            const TableName &table_name = it->first;
            bool is_dirty = it->second;
            // This should correspond to CcShard::ActiveTxMinTs.
            if (!table_name.IsMeta())
            {
                if (!is_dirty)
                {
                    // Skip the table if it's not updated since last sync ts.
                    GetTableLastCommitTsCc get_commit_ts_cc(
                        table_name, node_group, local_shards_.Count());
                    for (size_t core = 0; core < local_shards_.Count(); core++)
                    {
                        local_shards_.EnqueueCcRequest(core, &get_commit_ts_cc);
                    }
                    get_commit_ts_cc.Wait();

                    if (get_commit_ts_cc.LastCommitTs() < last_ckpt_ts)
                    {
                        continue;
                    }
                }

                uint64_t table_last_synced_ts = UINT64_MAX;
                local_shards_.EnqueueDataSyncTaskForTable(
                    table_name,
                    node_group,
                    is_standby_node ? standby_node_term : leader_term,
                    ckpt_ts,
                    table_last_synced_ts,
                    is_standby_node,
                    is_dirty,
                    can_be_skipped,
                    status);

                // Maybe we couldn't truncate log in this round of checkpoint.
                // Since some of the data sync tasks might be skipped due to
                // another task in queue. So we have no way of knowing if
                // the table or range was successfully flushed into storage in
                // this round of checkpoint. Check the smallest valid synced ts
                // of all tables and use it to truncate log.
                if (table_last_synced_ts != UINT64_MAX)
                {
                    last_succ_ckpt_ts =
                        std::min(last_succ_ckpt_ts, table_last_synced_ts);
                }
            }
        }

        // Check leadter term for leader node
        if (!is_standby_node &&
            Sharder::Instance().LeaderTerm(node_group) != leader_term)
        {
            continue;
        }

        if (last_succ_ckpt_ts != UINT64_MAX && last_succ_ckpt_ts > last_ckpt_ts)
        {
            assert(last_succ_ckpt_ts != 0);
            LOG_IF(INFO, FLAGS_report_ckpt)
                << "Checkpoint of node group #" << node_group
                << " succeeded with timestamp: " << last_succ_ckpt_ts;

            Sharder::Instance().UpdateNodeGroupCkptTs(node_group,
                                                      last_succ_ckpt_ts);

            if (!is_standby_node)
            {
                assert(standby_node_term < 0 && leader_term >= 0);
                NotifyLogOfCkptTs(node_group, leader_term, last_succ_ckpt_ts);

                BrocastPrimaryCkptTs(node_group,
                                     leader_term,
                                     last_succ_ckpt_ts,
                                     status->HasDataStoreWrite());
            }
        }

        {
            std::unique_lock<std::mutex> task_sender_lk(status->mux_);
            status->all_task_started_ = true;
            if (status->unfinished_scan_tasks_ == 0 &&
                status->unfinished_tasks_ != 0)
            {
                local_shards_.FlushCurrentFlushBuffer();
            }
            if (is_last_ckpt)
            {
                // Wait for all tasks to be done if this is last checkpoint
                // before graceful shutdown.
                status->cv_.wait(task_sender_lk,
                                 [&status]
                                 { return status->unfinished_tasks_ == 0; });
            }

            if (status->unfinished_tasks_ == 0)
            {
                if (status->need_truncate_log_ &&
                    status->err_code_ == CcErrorCode::NO_ERROR)
                {
                    if (status->truncate_log_ts_ == 0)
                    {
                        // Since no table has been modified since the last sync
                        // timestamp, update status->truncate_log_ts_ to mark
                        // the completion of this checkpoint.
                        //
                        // This update is crucial during a graceful shutdown in
                        // a cluster with a standby node using `eloqctl stop`.
                        // The updated node group checkpoint ts is used by
                        // eloqctl to verify that the final round of
                        // checkpointing has been completed.
                        status->truncate_log_ts_ = ckpt_ts;
                    }

                    // Truncate redo log
                    LOG_IF(INFO, FLAGS_report_ckpt)
                        << "Checkpoint of node group #" << node_group
                        << " succeeded with timestamp: "
                        << status->truncate_log_ts_;

                    // Note: `status->truncate_log_ts_ may be larger than
                    // `ckpt_ts`. So we use `status->truncate_log_ts_` to
                    // truncate log.
                    if (status->truncate_log_ts_ != UINT64_MAX &&
                        status->truncate_log_ts_ > last_ckpt_ts)
                    {
                        assert(status->truncate_log_ts_ >= ckpt_ts);
                        Sharder::Instance().UpdateNodeGroupCkptTs(
                            node_group, status->truncate_log_ts_);

                        if (!is_standby_node)
                        {
                            assert(standby_node_term < 0 && leader_term >= 0);
                            NotifyLogOfCkptTs(node_group,
                                              leader_term,
                                              status->truncate_log_ts_);

                            BrocastPrimaryCkptTs(node_group,
                                                 leader_term,
                                                 status->truncate_log_ts_,
                                                 status->HasDataStoreWrite());
                        }
                    }
                }

                CollectCkptMetric(status->err_code_ == CcErrorCode::NO_ERROR);
            }
        }
    }
}

void Checkpointer::Run()
{
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    last_checkpoint_ts_ = std::chrono::system_clock::now();
    while (ckpt_thd_status_ == Status::Active)
    {
        while (!ckpt_cv_.wait_for(
            lk,
            std::chrono::seconds(checkpoint_interval_),
            [this]
            {
                if (ckpt_thd_status_ != Status::Active)
                {
                    return true;
                }

                // Either have requested a checkpoint, or
                // we've sleeped for at least checkpoint_interval_ seconds.
                // Only enqueue new checkpoint task if there's idle worker.
                return (request_ckpt_.load(std::memory_order_acquire) ||
                        std::chrono::system_clock::now() >=
                            last_checkpoint_ts_ +
                                std::chrono::seconds(checkpoint_interval_));
            }))
        {
            // go back to sleep if there's no idle worker.
        }

        CODE_FAULT_INJECTOR("checkpointer_skip_ckpt", {
            request_ckpt_.store(false, std::memory_order_release);
            last_checkpoint_ts_ = std::chrono::system_clock::now();
            continue;
        });

        if (ckpt_thd_status_ != Status::Active)
        {
            break;
        }

        last_checkpoint_ts_ = std::chrono::system_clock::now();
        lk.unlock();
        Ckpt(false);
        lk.lock();
        // notify all waiting that one round checkpoint is done.
        ckpt_cv_.notify_all();

        request_ckpt_.store(false, std::memory_order_release);
    }

    // ensure normal shutdown execute checkpoint since we could receive
    // terminating request during the last checkpoint.
    lk.unlock();
    Ckpt(true);
    lk.lock();
    // notify all waiting that one round checkpoint is done.
    ckpt_cv_.notify_all();
    ckpt_thd_status_ = Status::Terminated;
}

/**
 * @brief Called by TxProcessor thread to notify checkpointer thread
 * to do checkpoint if there is no freeable entries to be kicked out
 * from ccmap.
 */
void Checkpointer::Notify(bool request_ckpt)
{
    if (request_ckpt)
    {
        auto now = std::chrono::system_clock::now();
        if (now < last_checkpoint_request_ts_.load(std::memory_order_relaxed) +
                      std::chrono::seconds(min_ckpt_request_interval_))
        {
            return;
        }

        bool expected = false;
        if (!request_ckpt_.compare_exchange_strong(expected, true))
        {
            return;
        }

        last_checkpoint_request_ts_.store(now);
    }
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    ckpt_cv_.notify_one();
}

bool Checkpointer::IsTerminated()
{
    std::scoped_lock<std::mutex> lk(ckpt_mux_);
    return ckpt_thd_status_ == Status::Terminated;
}

void Checkpointer::Terminate()
{
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    if (ckpt_thd_status_ == Status::Terminated)
    {
        // The cluster is in standby mode. The final round of checkpointing was
        // performed as part of the cluster stop command issued via eloqctl.
    }
    else
    {
        assert(ckpt_thd_status_ == Status::Active);
        ckpt_thd_status_ = Status::Terminating;
        ckpt_cv_.notify_one();
    }
}

void Checkpointer::Join()
{
    // The checkpoint worker is terminated, when the tx service is
    // going to be shut down. The checkpoint worker flushes one more
    // time unflushed records to the data store, before exiting. The
    // caller of this method, i.e., the destructor of the tx
    // service, is blocked until last flushing finishes.
    if (thd_.joinable())
    {
        thd_.join();
    }
}

void Checkpointer::NotifyLogOfCkptTs(uint32_t node_group,
                                     int64_t term,
                                     uint64_t ckpt_ts)
{
    if (!txservice_skip_wal)
    {
        assert(log_agent_ != nullptr);
        log_agent_->UpdateCheckpointTs(node_group, term, ckpt_ts);
    }
}

bool Checkpointer::CkptEntryForTest(
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        &flush_task_entries)
{
    return store_hd_->PutAll(flush_task_entries, nullptr, nullptr);
}

bool Checkpointer::FlushArchiveForTest(
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        &flush_task_entries)
{
    return store_hd_->PutArchivesAll(flush_task_entries, nullptr, nullptr);
}
}  // namespace txservice
