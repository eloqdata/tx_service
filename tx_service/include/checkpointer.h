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
#include <vector>

#include "cc/local_cc_shards.h"
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

    std::pair<uint64_t, uint64_t> GetNewCheckpointTs(uint32_t node_group_id,
                                                     bool is_last_ckpt);

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

    void CollectCkptMetric(bool success)
    {
        if (metrics::enable_metrics)
        {
            if (success)
            {
                if (consecutive_fail_cnt_ > 0)
                {
                    Sharder::Instance()
                        .GetLocalCcShards()
                        ->GetNodeMeter()
                        ->Collect(
                            metrics::NAME_IS_CONTINUOUS_CHECKPOINT_FAILURES, 0);
                }
                // reset value
                consecutive_fail_cnt_.store(0, std::memory_order_relaxed);
            }
            else
            {
                size_t fail_cnt = consecutive_fail_cnt_.fetch_add(
                    1, std::memory_order_relaxed);
                if (fail_cnt + 1 >= continuous_ckpt_fail_threshold)
                {
                    Sharder::Instance()
                        .GetLocalCcShards()
                        ->GetNodeMeter()
                        ->Collect(
                            metrics::NAME_IS_CONTINUOUS_CHECKPOINT_FAILURES, 1);
                }
            }
        }
    }

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

    std::atomic<size_t> consecutive_fail_cnt_{0};

    void NotifyLogOfCkptTs(uint32_t node_group, int64_t term, uint64_t ckpt_ts);
};
}  // namespace txservice
