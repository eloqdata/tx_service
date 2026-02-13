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

#include "catalog_key_record.h"
#include "cc_request.h"
#include "error_messages.h"
#include "metrics.h"
#include "range_slice.h"
#include "sharder.h"
#include "standby.h"
#include "statistics.h"
#include "tx_start_ts_collector.h"

namespace txservice
{
extern bool txservice_skip_wal;
#if defined(DISABLE_CKPT_REPORT) && !defined(DEBUG)
DEFINE_bool(report_ckpt, false, "Print log on do checkpoint.");
#else
DEFINE_bool(report_ckpt, true, "Print log on do checkpoint.");
#endif
bool PassValidate(const char *, bool)
{
    return true;
}
BRPC_VALIDATE_GFLAG(report_ckpt, PassValidate);

Checkpointer::Checkpointer(LocalCcShards &shards,
                           store::DataStoreHandler *write_hd,
                           const uint32_t &checkpoint_interval,
                           TxLog *log_agent,
                           uint32_t ckpt_delay_seconds,
                           uint32_t min_checkpoint_interval)
    : local_shards_(shards),
      ckpt_mux_(),
      ckpt_cv_(),
      store_hd_(write_hd),
      ckpt_thd_status_(Status::Active),
      checkpoint_interval_(checkpoint_interval),
      min_checkpoint_interval_(min_checkpoint_interval),
      ckpt_delay_time_(ckpt_delay_seconds * 1000000),
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
               << " ,min_checkpoint_interval_: " << min_checkpoint_interval_
               << " ,ckpt_delay_seconds: " << ckpt_delay_seconds;
}

std::pair<uint64_t, uint64_t> Checkpointer::GetNewCheckpointTs(
    uint32_t node_group_id, bool is_last_ckpt)
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
            snapshot_req.set_dest_path(store_hd_->SnapshotSyncDestPath());
            brpc::Controller cntl;
            stub.RequestStorageSnapshotSync(
                &cntl, &snapshot_req, &snapshot_resp, nullptr);
        }

        return;
    }

    int64_t standby_node_term = Sharder::Instance().StandbyNodeTerm();
    bool is_standby_node = standby_node_term > 0;
    if (is_standby_node &&
        Sharder::Instance().GetDataStoreHandler()->IsSharedStorage())
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

        auto [ckpt_ts, mem_usage] =
            GetNewCheckpointTs(node_group, is_last_ckpt);
        uint64_t last_ckpt_ts =
            Sharder::Instance().GetNodeGroupCkptTs(node_group);

        if (ckpt_ts <= last_ckpt_ts)
        {
            continue;
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

                BrocastPrimaryCkptTs(
                    node_group, leader_term, last_succ_ckpt_ts);
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
                                                 status->truncate_log_ts_);
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
    last_checkpoint_ts_ = std::chrono::high_resolution_clock::now();
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
                        std::chrono::high_resolution_clock::now() >=
                            last_checkpoint_ts_ +
                                std::chrono::seconds(checkpoint_interval_));
            }))
        {
            // go back to sleep if there's no idle worker.
        }

        CODE_FAULT_INJECTOR("checkpointer_skip_ckpt", {
            request_ckpt_.store(false, std::memory_order_release);
            last_checkpoint_ts_ = std::chrono::high_resolution_clock::now();
            continue;
        });

        if (ckpt_thd_status_ != Status::Active)
        {
            break;
        }

        last_checkpoint_ts_ = std::chrono::high_resolution_clock::now();
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
        if (std::chrono::high_resolution_clock::now() <
            last_checkpoint_ts_ +
                std::chrono::seconds(min_checkpoint_interval_))
        {
            return;
        }

        bool expected = false;
        if (!request_ckpt_.compare_exchange_strong(
                expected, true, std::memory_order_release))
        {
            return;
        }
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
    return store_hd_->PutAll(flush_task_entries);
}

bool Checkpointer::FlushArchiveForTest(
    std::unordered_map<std::string_view,
                       std::vector<std::unique_ptr<FlushTaskEntry>>>
        &flush_task_entries)
{
    return store_hd_->PutArchivesAll(flush_task_entries);
}
}  // namespace txservice
