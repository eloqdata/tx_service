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

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "checkpoint_metrics_state.h"
#include "metrics.h"
#include "sharder.h"
#include "txlog.h"
#include "util.h"

using namespace std::chrono;

namespace txservice
{

class Checkpointer
{
public:
    static constexpr size_t continuous_ckpt_fail_threshold = 3;

    Checkpointer(LocalCcShards &shards,
                 store::DataStoreHandler *write_hd,
                 const uint32_t &checkpoint_interval,
                 TxLog *log_agent,
                 uint32_t ckpt_delay_seconds,
                 uint32_t min_ckpt_request_interval);

    ~Checkpointer() = default;

    void Ckpt(bool is_last_ckpt);

    /**
     * @brief Computes the node group's new checkpoint ts and memory usage.
     * @param pinning_tx Out: the write-lock-holding tx that determined the
     * ts (txn_ == 0 when the ts came from the clock). Consumed by
     * WarnIfCkptStalled when the ts fails to advance.
     */
    std::pair<uint64_t, uint64_t> GetNewCheckpointTs(
        uint32_t node_group_id,
        bool is_last_ckpt,
        CkptTsCc::PinningTxInfo &pinning_tx);

    /**
     * @brief Checkpoint one Entry to KvStore synchronously.
     * Now, only used for test.
     */
    bool CkptEntryForTest(
        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            &flush_task_entries);

    bool FlushArchiveForTest(
        std::unordered_map<std::string_view,
                           std::vector<std::unique_ptr<FlushTaskEntry>>>
            &flush_task_entries);

    void Run();

    /**
     * @brief Called by TxProcessor thread to notify checkpointer thread
     * to do checkpoint if there is no freeable entries to be kicked out
     * from ccmap. This will also be called by data sync worker thread when
     * it runs out of task.
     * @param  request_ckpt  If true, will request checkpoint immediately.
     */
    void Notify(bool request_ckpt = true);

    bool IsTerminated();

    /**
     * @brief When TxService is stopping, this function will be called and
     * triggers checkpoint to flush data to KvStore.
     *
     */
    void Terminate();

    void Join();

    /** Records one eligible checkpoint attempt for interval telemetry. */
    void RecordCheckpointAttempt(NodeGroupId node_group_id, int64_t term);

    /** Records a successful local durable checkpoint-ts advance. */
    void RecordCheckpointAdvance(NodeGroupId node_group_id, int64_t term);

    /** Applies one terminal checkpoint outcome to per-NG failure state. */
    void ReportCheckpointOutcome(
        NodeGroupId node_group_id,
        int64_t term,
        DataSyncStatus::CheckpointOutcome outcome,
        DataSyncStatus::CheckpointFailureReason failure_reason);

    /**
     * Drops leadership-tenure metrics state after this node loses an NG.
     * Cumulative failure counters are intentionally retained.
     */
    void ClearCheckpointMetricsForNodeGroup(NodeGroupId node_group_id);

    void IncrementOngoingDataSyncCnt()
    {
        ongoing_data_sync_cnt_.fetch_add(1, std::memory_order_relaxed);
    }

    void DecrementOngoingDataSyncCnt()
    {
        ongoing_data_sync_cnt_.fetch_sub(1, std::memory_order_relaxed);
    }

    bool IsOngoingDataSync() const
    {
        return ongoing_data_sync_cnt_.load(std::memory_order_relaxed) > 0;
    }

private:
    enum struct Status
    {
        Active,
        Terminating,
        Terminated
    };

    LocalCcShards &local_shards_;
    // protects status_
    std::mutex ckpt_mux_;
    std::condition_variable ckpt_cv_;
    std::atomic<bool> request_ckpt_{false};
    store::DataStoreHandler *store_hd_;
    std::thread thd_;
    Status ckpt_thd_status_;
    const uint32_t checkpoint_interval_;
    const uint32_t min_ckpt_request_interval_;
    std::chrono::system_clock::time_point last_checkpoint_ts_;
    std::atomic<std::chrono::system_clock::time_point>
        last_checkpoint_request_ts_;
    uint32_t ckpt_delay_time_;  // unit: Microsecond
    std::atomic<uint64_t> ongoing_data_sync_cnt_{0};
    TxService *tx_service_;
    TxLog *log_agent_;

    // Checkpoint callbacks and raft leadership callbacks run on different
    // threads. The term is revalidated while holding this mutex so a callback
    // from a failed-over term cannot recreate erased NG state.
    std::mutex checkpoint_metrics_mux_;
    CheckpointMetricsState checkpoint_metrics_state_{
        continuous_ckpt_fail_threshold};

    // Per-node-group checkpoint-stall tracking: how many consecutive Ckpt()
    // rounds the checkpoint ts failed to advance (reset to 0 whenever it
    // advances), and when a stall warning was last logged (rate-limits
    // WarnIfCkptStalled to one warning per node group per minute). The
    // default last_warn_time_ (epoch) never suppresses the first warning.
    struct CkptStallState
    {
        uint32_t stall_rounds_{0};
        std::chrono::steady_clock::time_point last_warn_time_;
    };
    // Node group id -> stall state. Only accessed from the checkpointer
    // thread.
    std::unordered_map<uint32_t, CkptStallState> ckpt_stall_states_;

    void NotifyLogOfCkptTs(uint32_t node_group, int64_t term, uint64_t ckpt_ts);

    /**
     * @brief Tracks consecutive rounds in which @p node_group's checkpoint ts
     * failed to advance and, past a threshold, logs a rate-limited warning
     * naming the pinning transaction. Called from the checkpointer thread.
     */
    void WarnIfCkptStalled(uint32_t node_group,
                           uint64_t ckpt_ts,
                           uint64_t last_ckpt_ts,
                           const CkptTsCc::PinningTxInfo &pinning_tx);

    bool IsCurrentCheckpointTerm(NodeGroupId node_group_id, int64_t term) const;
    void ApplyCheckpointMetricsUpdateLocked(
        const CheckpointMetricsState::Update &update,
        const metrics::Name *interval_metric = nullptr);
    static const char *CheckpointFailureReasonLabel(
        DataSyncStatus::CheckpointFailureReason reason);
};
}  // namespace txservice
