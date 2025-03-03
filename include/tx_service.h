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

#include <bthread/bthread.h>
#include <bthread/task_group.h>
#include <butil/macros.h>
#include <cuckoofilter/cuckoofilter.h>
#include <mimalloc-2.1/mimalloc.h>
#include <pthread.h>

#include <algorithm>  // std::min
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "checkpointer.h"
#include "circular_queue.h"
#include "concurrent_queue_wsize.h"
#include "dead_lock_check.h"
#include "local_cc_handler.h"
#include "local_cc_shards.h"
#include "spinlock.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service_common.h"
#include "tx_service_metrics.h"
#include "tx_start_ts_collector.h"
#include "txlog.h"

#ifdef ON_KEY_OBJECT
#include "store/snapshot_manager.h"  // SnapshotManager
#endif

using namespace std::chrono_literals;
namespace bthread
{
extern BAIDU_THREAD_LOCAL TaskGroup *tls_task_group;
};
namespace txservice
{

// the OFFSET_TABLE contains only prime numbers
inline const size_t OFFSET_TABLE[] = {
#include "offset_inl.list"

};

/**
 * @brief TxProcessor is a worker processing concurrency control (cc) requests
 * on one cc shard (identified by the thread/core ID), advances tx state
 * machines allocated for this shard and dispatches cc requests from the shard's
 * tx's to other cc shards, either in the same node or remote nodes .
 *
 */
class TxProcessor
{
public:
    static const int64_t t1sec = 1000000L;
    static const int64_t t2sec = 4000000L;

    TxProcessor(size_t thd_id,
                LocalCcShards &shards,
                TxLog *txlog_hd,
                metrics::MetricsRegistry *metrics_registry = nullptr,
                metrics::CommonLabels common_labels = {},
                std::vector<std::tuple<metrics::Name,
                                       metrics::Type,
                                       std::vector<metrics::LabelGroup>>>
                    external_metrics = {})
        : thd_id_(thd_id),
          terminated_(false),
          tx_proc_status_(TxProcessorStatus::Busy),
          local_cc_shards_(shards),
          active_tx_cnt_(0),
          new_tx_cnt_(0),
          new_txs_(),
          new_tx_token_(new_txs_),
          free_txs_(),
          txlog_hd_(txlog_hd)
    {
        if (metrics::enable_busy_round_metrics)
        {
            auto meter = GetMeter();
            meter->Register(metrics::NAME_BUSY_ROUND_DURATION,
                            metrics::Type::Histogram);
            meter->Register(metrics::NAME_BUSY_ROUND_ACTIVE_TX_COUNT,
                            metrics::Type::Gauge);
            meter->Register(metrics::NAME_BUSY_ROUND_PROCESSED_CC_REQUEST_COUNT,
                            metrics::Type::Gauge);
            meter->Register(metrics::NAME_EMPTY_ROUND_RATIO,
                            metrics::Type::Gauge);
        }

        if (metrics::enable_tx_metrics)
        {
            auto meter = GetMeter();
            meter->Register(metrics::NAME_TX_DURATION,
                            metrics::Type::Histogram);
            meter->Register(metrics::NAME_TX_PROCESSED_TOTAL,
                            metrics::Type::Counter);
            meter->Register(metrics::NAME_REMOTE_REQUEST_DURATION,
                            metrics::Type::Histogram,
                            {{"type",
                              {"read",
                               "acquire_write",
                               "validate",
                               "post_process",
                               "scan_next",
                               "write_log"}}});
            meter->Register(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                            metrics::Type::Gauge,
                            {{"type",
                              {"read",
                               "acquire_write",
                               "validate",
                               "post_process",
                               "scan_next",
                               "write_log"}}});
        }

        if (metrics::enable_metrics && !external_metrics.empty())
        {
            auto meter = GetMeter();
            for (auto tuple : external_metrics)
            {
                auto metric_name = std::get<0>(tuple);
                auto metric_type = std::get<1>(tuple);
                auto metric_labels = std::get<2>(tuple);
                meter->Register(
                    metric_name, metric_type, std::move(metric_labels));
            }
        }

        coordi_ = std::make_shared<TxProcCoordinator>(thd_id, this);
    }

    ~TxProcessor()
    {
        coordi_->tx_processor_.store(nullptr);
    }

    metrics::Meter *GetMeter()
    {
        return local_cc_shards_.GetCcShard(thd_id_)->GetMeter();
    };

    TransactionExecution *NewTx()
    {
        TransactionExecution::uptr tx = nullptr;
        bool success = free_txs_.try_dequeue(tx);
        if (success)
        {
            assert(tx != nullptr);
            tx->Restart(cc_hd_.get(), txlog_hd_, this);
        }
        else
        {
            tx = std::make_unique<TransactionExecution>(
                cc_hd_.get(), txlog_hd_, this);
        }

        TransactionExecution *tx_ptr = tx.get();

        // The memory order of new_txs_.enqueue() ensures that the update of
        // active_tx_cnt_ happens before the new tx appearing in the queue.
        uint32_t prev_tx_cnt =
            active_tx_cnt_.fetch_add(1, std::memory_order_relaxed);

        new_tx_cnt_.fetch_add(1, std::memory_order_relaxed);
        // Add the new transaction into the new tx set.
        new_txs_.enqueue(std::move(tx));

        // Wakes up the tx processor thread if it is asleep.
        if (prev_tx_cnt == 0)
        {
            TxProcessorStatus native_proc_status =
                tx_proc_status_.load(std::memory_order_relaxed);
#ifdef EXT_TX_PROC_ENABLED
            int16_t ext_processor_cnt =
                coordi_->ext_processor_cnt_.load(std::memory_order_relaxed);
#ifdef ON_KEY_OBJECT
            if (ext_processor_cnt == 0)
            {
                // New txm task. Notify the external processor directly.
                coordi_->NotifyExternalProcessor();
            }
#endif
            if (native_proc_status == TxProcessorStatus::Sleep ||
                (ext_processor_cnt == 0 &&
                 native_proc_status == TxProcessorStatus::Standby))
            {
                Notify(coordi_->sleep_mux_, coordi_->sleep_cv_);
            }
#else
            if (native_proc_status == TxProcessorStatus::Sleep)
            {
                Notify(coordi_->sleep_mux_, coordi_->sleep_cv_);
            }
#endif
        }

        return tx_ptr;
    }

#ifdef EXT_TX_PROC_ENABLED
    TransactionExecution *NewExternalTx()
    {
        TransactionExecution::uptr tx = nullptr;
        bool success = free_txs_.try_dequeue(tx);
        if (success)
        {
            assert(tx != nullptr);
            tx->Restart(cc_hd_.get(), txlog_hd_, this, true);
        }
        else
        {
            tx = std::make_unique<TransactionExecution>(
                cc_hd_.get(), txlog_hd_, this, true);
        }

#ifdef ON_KEY_OBJECT
        // Increment external txm count of this TxProcessor.
        coordi_->external_txm_cnt_.fetch_add(1, std::memory_order_relaxed);
#endif

        TransactionExecution *tx_ptr = tx.get();
        active_tx_lock_.Lock();
        active_tx_map_.try_emplace(tx.get(), std::move(tx));
        active_tx_lock_.Unlock();
        return tx_ptr;
    }
#endif

    void RunOneRound(size_t &active_cnt,
                     size_t &req_cnt,
                     bool &yield
#ifdef EXT_TX_PROC_ENABLED
                     ,
                     std::atomic<TxShardStatus> &shard_status,
                     bool is_ext_proc
#endif
    )
    {
#ifdef EXT_TX_PROC_ENABLED
        TxShardStatus expected = TxShardStatus::Free;
        bool success = shard_status.compare_exchange_strong(
            expected, TxShardStatus::Occupied, std::memory_order_acq_rel);
        if (!success)
        {
            active_cnt = 0;
            req_cnt = 0;
            yield = true;
            return;
        }

        CcShard *shard = local_cc_shards_.GetCcShard(thd_id_);
        CcShardHeap *shard_heap = shard->GetShardHeap();
        if (shard_heap == nullptr)
        {
            assert(is_ext_proc);
            shard_status.store(TxShardStatus::Free, std::memory_order_release);
            return;
        }
        mi_heap_t *prev_heap = shard_heap->SetAsDefaultHeap();
        if (is_ext_proc)
        {
            // Override thread id as well if current thread id is not heap owner
            // thread id.
            shard->OverrideHeapThread();
            coordi_->ext_tx_proc_heap_ = prev_heap;
        }
        one_round_cnt_.fetch_add(1, std::memory_order_relaxed);
#endif

        yield = false;
        active_cnt = 0;
        req_cnt = 0;

        size_t new_tx_cnt = new_tx_cnt_.load(std::memory_order_relaxed);
        if (new_tx_cnt > 0)
        {
            std::array<TransactionExecution::uptr, 100> txs;
            size_t dq_cnt = std::min(txs.size(), new_tx_cnt);

            new_tx_cnt = new_txs_.try_dequeue_bulk(
                new_tx_token_, std::make_move_iterator(txs.begin()), dq_cnt);

            for (size_t idx = 0; idx < new_tx_cnt; ++idx)
            {
                idle_txs_.Enqueue(std::move(txs[idx]));
            }

            new_tx_cnt_.fetch_sub(new_tx_cnt, std::memory_order_relaxed);
        }

        size_t idle_size = idle_txs_.Size();
        for (size_t idx = 0; idx < idle_size; ++idx)
        {
            TransactionExecution::uptr tx = std::move(idle_txs_.Peek());
            idle_txs_.Dequeue();

            TxmStatus txm_status = tx->Forward();

            switch (txm_status)
            {
            case TxmStatus::Finished:
                free_txs_.enqueue(std::move(tx));
                active_tx_cnt_.fetch_sub(1, std::memory_order_relaxed);
                break;
            case TxmStatus::Idle:
                idle_txs_.Enqueue(std::move(tx));
                break;
            case TxmStatus::Busy:
                on_fly_txs_.Enqueue(std::move(tx));
                break;
            default:
                break;
            }
        }

        if (is_busy_round_ && metrics::enable_busy_round_metrics)
        {
            auto meter = GetMeter();
            meter->CollectDuration(metrics::NAME_BUSY_ROUND_DURATION,
                                   busy_round_start_);
            meter->Collect(metrics::NAME_BUSY_ROUND_ACTIVE_TX_COUNT,
                           busy_round_active_tx_count_);
            meter->Collect(metrics::NAME_BUSY_ROUND_PROCESSED_CC_REQUEST_COUNT,
                           busy_round_processed_cc_req_count_);
            is_busy_round_ = false;
        }

#ifdef EXT_TX_PROC_ENABLED
        size_t loop_cnt = 3;
        if (is_ext_proc)
        {
            CheckWaitingTxs();
        }
#else
        size_t loop_cnt = 5;
#endif

#ifdef ON_KEY_OBJECT
        loop_cnt = 1;
#endif

        for (size_t loop = 0; loop < loop_cnt; ++loop)
        {
#ifdef EXT_TX_PROC_ENABLED
            if (is_ext_proc)
            {
                CheckResumeTx();
            }
#endif

            size_t fly_size = on_fly_txs_.Size();
            for (size_t idx = 0; idx < fly_size; ++idx)
            {
                TransactionExecution::uptr tx = std::move(on_fly_txs_.Peek());
                on_fly_txs_.Dequeue();

                TxmStatus txm_status = tx->Forward();

                switch (txm_status)
                {
                case TxmStatus::Finished:
                    free_txs_.enqueue(std::move(tx));
                    active_tx_cnt_.fetch_sub(1, std::memory_order_relaxed);
                    break;
                case TxmStatus::Idle:
                    idle_txs_.Enqueue(std::move(tx));
                    break;
                case TxmStatus::Busy:
                    on_fly_txs_.Enqueue(std::move(tx));
                    break;
                default:
                    break;
                }
            }

            if (loop == 0 && metrics::enable_busy_round_metrics &&
                local_cc_shards_.QueueSize(thd_id_) >=
                    metrics::busy_round_threshold)
            {
                is_busy_round_ = true;
                busy_round_start_ = metrics::Clock::now();
            }

            // Process CcRequests.
            req_cnt += local_cc_shards_.ProcessRequests(thd_id_);
        }

        active_cnt =
            on_fly_txs_.Size() + new_tx_cnt_.load(std::memory_order_relaxed);

#ifdef EXT_TX_PROC_ENABLED

        mi_heap_set_default(prev_heap);
        if (is_ext_proc)
        {
            assert(coordi_->ext_tx_proc_heap_ != nullptr);
            mi_restore_default_thread_id();
            coordi_->ext_tx_proc_heap_ = nullptr;
        }
        shard_status.store(TxShardStatus::Free, std::memory_order_release);
#endif

        if (metrics::enable_busy_round_metrics)
        {
            empty_round_count_ += req_cnt == 0 ? 1 : 0;
            if (++total_round_count_ == empty_round_threshold_)
            {
                auto meter = GetMeter();
                meter->Collect(metrics::NAME_EMPTY_ROUND_RATIO,
                               static_cast<double>(empty_round_count_) /
                                   total_round_count_);
                empty_round_count_ = 0;
                total_round_count_ = 0;
            }

            if (is_busy_round_)
            {
                busy_round_active_tx_count_ = active_cnt;
                busy_round_processed_cc_req_count_ = req_cnt;
            }
        }
    }

    void Run()
    {
        using namespace std::chrono_literals;

        auto tstart = std::chrono::steady_clock::now();

        size_t idle_rnd = 0;
        local_cc_shards_.GetCcShard(thd_id_)->InitializeShardHeap();
        local_cc_shards_.SetTxProcNotifier(
            thd_id_, &tx_proc_status_, coordi_.get());

#ifdef EXT_TX_PROC_ENABLED
        size_t local_round_cnt = one_round_cnt_.load(std::memory_order_relaxed);
#endif

        while (!terminated_.load(std::memory_order_relaxed))
        {
            size_t tx_cnt = 0, req_cnt = 0;
            bool yield = false;

#ifdef EXT_TX_PROC_ENABLED
            RunOneRound(tx_cnt, req_cnt, yield, coordi_->shard_status_, false);
            ++local_round_cnt;

            size_t round_cnt = one_round_cnt_.load(std::memory_order_relaxed);
            bool has_ext_proc =
                coordi_->ext_processor_cnt_.load(std::memory_order_relaxed) > 0;
            bool is_ext_proc_active = local_round_cnt != round_cnt;

            if (yield ||
                (has_ext_proc && (is_ext_proc_active ||
                                  (req_cnt + tx_cnt == 0 && idle_rnd > 1000))))
            {
                tx_cnt = 0;
                req_cnt = 0;
                idle_rnd = 0;

                tx_proc_status_.store(TxProcessorStatus::Standby,
                                      std::memory_order_relaxed);

                std::unique_lock<std::mutex> lk(coordi_->sleep_mux_);
                do
                {
                    local_round_cnt = round_cnt;
                    bool no_ext_proc = coordi_->sleep_cv_.wait_for(
                        lk,
                        2s,
                        [this]()
                        {
                            return coordi_->ext_processor_cnt_.load(
                                       std::memory_order_relaxed) == 0 ||
                                   terminated_.load(std::memory_order_relaxed);
                        });

                    round_cnt = one_round_cnt_.load(std::memory_order_relaxed);

                    // If the round counter is not incremented since last sleep,
                    // it means that there is no external processor, or the
                    // external processor has not visited the shard for a while.
                    // Steps out of the standby mode to forward tx's and process
                    // cc requests.
                    if (no_ext_proc || round_cnt == local_round_cnt)
                    {
                        local_round_cnt = round_cnt;
                        break;
                    }
                } while (!terminated_.load(std::memory_order_relaxed));

                tx_proc_status_.store(TxProcessorStatus::Busy,
                                      std::memory_order_relaxed);
            }
#else
            RunOneRound(tx_cnt, req_cnt, yield);
#endif

            if (tx_cnt > 0 || req_cnt > 0)
            {
                idle_rnd = 0;
                continue;
            }

            if (idle_rnd == 0)
            {
                // Records the time when busy wait starts.
                tstart = std::chrono::steady_clock::now();
            }

            ++idle_rnd;

            if ((idle_rnd & 0x3F) == 0)
            {
                // For every 64 busy wait cycles, checks if the busy wait
                // window exceeds 1ms.
                auto tnow = std::chrono::steady_clock::now();
                if (tnow - tstart >= 1ms && IsIdle())
                {
                    idle_rnd = 0;

                    tx_proc_status_.store(TxProcessorStatus::Sleep,
                                          std::memory_order_relaxed);

                    std::unique_lock<std::mutex> lk(coordi_->sleep_mux_);
                    coordi_->sleep_cv_.wait(lk, [this]() { return !IsIdle(); });

#ifdef EXT_TX_PROC_ENABLED
                    local_round_cnt =
                        one_round_cnt_.load(std::memory_order_relaxed);
#endif
                    tx_proc_status_.store(TxProcessorStatus::Busy,
                                          std::memory_order_relaxed);
                }
            }
        }
    }

    void InitializeLocalHandler()
    {
        if (cc_hd_ == nullptr)
        {
            cc_hd_ =
                std::make_unique<LocalCcHandler>(thd_id_, local_cc_shards_);
        }
    }

    void Terminate()
    {
        TxShardStatus expected = TxShardStatus::Free;
        while (!coordi_->shard_status_.compare_exchange_weak(
            expected, TxShardStatus::Deconstructed, std::memory_order_acq_rel))
        {
            expected = TxShardStatus::Free;
        }

        // decrease use_count of share pointer to TableSchema
        cc_hd_ = nullptr;

        {
            std::unique_lock<std::mutex> lk(coordi_->sleep_mux_);
            terminated_.store(true, std::memory_order_relaxed);
            coordi_->sleep_cv_.notify_one();
        }
    }

#ifdef EXT_TX_PROC_ENABLED
    std::function<void()> TxProcessorFunctor()
    {
        return [this, coordi = coordi_]()
        {
            size_t active_cnt = 0, req_cnt = 0;
            bool yield = false;
            RunOneRound(
                active_cnt, req_cnt, yield, coordi->shard_status_, true);
        };
    }

    std::function<void(int16_t)> UpdateExtProcFunctor()
    {
        return [this, coordi = coordi_](int16_t thd_delta) -> void
        {
            int16_t ext_thd_cnt = coordi->ext_processor_cnt_.fetch_add(
                thd_delta, std::memory_order_relaxed);

            ext_thd_cnt += thd_delta;
            assert(ext_thd_cnt >= 0);
#ifdef ON_KEY_OBJECT
            assert(ext_thd_cnt <= 1);
#else

            // There is no external thread anymore. Wakes up the native tx
            // processor.
            if (ext_thd_cnt == 0)
            {
                Notify(coordi->sleep_mux_, coordi->sleep_cv_);
            }
#endif
        };
    }

    std::function<bool(bool)> OverrideShardHeapFunctor()
    {
        return [this, coordi = coordi_](bool yield) -> bool
        {
            if (yield)
            {
                // Since only brpc worker thread will read and modify
                // coordi_->ext_tx_proc_heap_, it is safe to directly
                // access without lock.
                if (coordi->ext_tx_proc_heap_)
                {
                    // tx proc is occupied by ext tx processor.
                    mi_heap_set_default(coordi->ext_tx_proc_heap_);
                    mi_restore_default_thread_id();
                    coordi->ext_tx_proc_heap_ = nullptr;
                    return true;
                }
            }
            else
            {
                CcShard *shard = local_cc_shards_.GetCcShard(thd_id_);
                CcShardHeap *shard_heap = shard->GetShardHeap();
                assert(shard_heap);
                shard->OverrideHeapThread();
                coordi->ext_tx_proc_heap_ = shard_heap->SetAsDefaultHeap();
            }
            return false;
        };
    }

#ifdef ON_KEY_OBJECT
    std::function<bool()> HasWork()
    {
        return [this, coordi = coordi_]() -> bool
        {
            if (coordi->external_txm_cnt_.load(std::memory_order_relaxed) > 0)
            {
                return true;
            }
            TxProcessor *txp =
                coordi->tx_processor_.load(std::memory_order_relaxed);
            return txp != nullptr && !txp->IsIdle();
        };
    }
#endif

    void EnlistTx(TransactionExecution *txm)
    {
        resume_tx_queue_.Enqueue(txm);
    }

    /**
     * @brief Lets the external processor to forward the tx state machine.
     * Forwarding needs to hold the latch of the tx shard, because it accesses
     * thread-unsafe resources (such as cc handler) belonging to the tx shard.
     *
     * @param txm
     * @return true, if the external processor acquires the latch successfully
     * and forwards the tx state machine.
     * @return false, if someone else is holding the latch. The tx will be
     * enlisted into the resume queue for later execution.
     */
    bool ForwardTx(TransactionExecution *txm)
    {
#ifdef ON_KEY_OBJECT
        assert(bthread::tls_task_group->group_id_ >= 0);
        if (bthread::tls_task_group->group_id_ != (int32_t) thd_id_)
        {
            // For redis a tx life cycle can spread across multiple cmds, which
            // might be put into different bthread task group. If the task group
            // id does not match the tx processor id, it is not safe to forward
            // txm.
            return false;
        }
#endif
        TxShardStatus expected = TxShardStatus::Free;
        bool success = coordi_->shard_status_.compare_exchange_strong(
            expected, TxShardStatus::Occupied, std::memory_order_acquire);
        if (!success)
        {
            return false;
        }
        // Override default heap since we're accessing txm in cc shard.
        CcShard *shard = local_cc_shards_.GetCcShard(thd_id_);
        CcShardHeap *shard_heap = shard->GetShardHeap();
        shard->OverrideHeapThread();
        coordi_->ext_tx_proc_heap_ = shard_heap->SetAsDefaultHeap();

        TxmStatus txm_status = txm->Forward();
        if (txm_status == TxmStatus::Finished)
        {
            active_tx_lock_.Lock();
            auto it = active_tx_map_.find(txm);
            if (it != active_tx_map_.end())
            {
                TransactionExecution::uptr tx_uptr = std::move(it->second);
                active_tx_map_.erase(it);
                active_tx_lock_.Unlock();
                tx_progress_.erase(tx_uptr.get());

                free_txs_.enqueue(std::move(tx_uptr));
            }
            else
            {
                active_tx_lock_.Unlock();
            }
        }
        mi_heap_set_default(coordi_->ext_tx_proc_heap_);
        mi_restore_default_thread_id();
        coordi_->ext_tx_proc_heap_ = nullptr;
        assert(coordi_->shard_status_.load(std::memory_order_relaxed) ==
               TxShardStatus::Occupied);
        coordi_->shard_status_.store(TxShardStatus::Free,
                                     std::memory_order_release);
        return true;
    }

    void CheckWaitingTxs()
    {
        static const uint64_t check_progress_period = 2000000;
        uint64_t now_ts = LocalCcShards::ClockTs();
#ifdef ON_KEY_OBJECT

        static const uint64_t check_progress_block_period = 10000;
        if (now_ts - progress_check_ts_block_ <= check_progress_block_period)
        {
            return;
        }

        for (auto &[tx, progress] : tx_progress_block_)
        {
            if (tx->state_stack_.empty())
            {
                continue;
            }
            // If the tx has been stuck on the same command for a while, enlists
            // the tx for execution.
            uint16_t cmd_id = tx->CommandId();
            if (cmd_id == progress.cmd_id_)
            {
                EnlistTx(tx);
            }
            else if (cmd_id > progress.cmd_id_)
            {
                progress.cmd_id_ = cmd_id;
            }
        }

        progress_check_ts_block_ = now_ts;
#endif
        if (now_ts - progress_check_ts_ <= check_progress_period)
        {
            return;
        }
        for (auto &[tx, progress] : tx_progress_)
        {
            if (tx->state_stack_.empty())
            {
                continue;
            }
            // If the tx has been stuck on the same command for a while, enlists
            // the tx for execution.
            uint16_t cmd_id = tx->CommandId();
            if (cmd_id == progress.cmd_id_)
            {
                EnlistTx(tx);
            }
            else if (cmd_id > progress.cmd_id_)
            {
                progress.cmd_id_ = cmd_id;
            }
        }

        progress_check_ts_ = now_ts;
    }

    void CheckResumeTx()
    {
        size_t resume_cnt = resume_tx_queue_.SizeApprox();
        while (resume_cnt > 0)
        {
            std::array<TransactionExecution *, 100> tx_bulk{};
            size_t deque_cap = std::min(resume_cnt, tx_bulk.size());
            size_t deque_size =
                resume_tx_queue_.TryDequeueBulk(tx_bulk.begin(), deque_cap);

            for (size_t idx = 0; idx < deque_size; ++idx)
            {
                TransactionExecution *tx_ptr = tx_bulk[idx];
                if (tx_ptr->TxStatus() == TxnStatus::Finished)
                {
                    continue;
                }

                TxmStatus txm_status = tx_ptr->Forward();
                if (txm_status == TxmStatus::Finished)
                {
                    active_tx_lock_.Lock();
                    auto it = active_tx_map_.find(tx_ptr);
                    if (it == active_tx_map_.end())
                    {
                        active_tx_lock_.Unlock();
                        continue;
                    }

                    TransactionExecution::uptr tx_uptr = std::move(it->second);
                    active_tx_map_.erase(it);
                    active_tx_lock_.Unlock();
                    tx_progress_.erase(tx_uptr.get());

                    free_txs_.enqueue(std::move(tx_uptr));
                }
            }

            resume_cnt = resume_tx_queue_.SizeApprox();
        }
    }

    void EnlistWaitingTx(TransactionExecution *txm)
    {
        uint16_t cmd_id = txm->CommandId();
        uint64_t clock_ts = LocalCcShards::ClockTs();

#ifdef ON_KEY_OBJECT
        auto op =
            txm->state_stack_.empty() ? nullptr : txm->state_stack_.back();
        if (op != nullptr && op->IsBlockCommand())
        {
            auto tx_it = tx_progress_block_.try_emplace(txm, cmd_id, clock_ts);
            if (!tx_it.second)
            {
                tx_it.first->second.cmd_id_ = cmd_id;
                tx_it.first->second.wait_clock_ts_ = clock_ts;
            }
        }
        else
#endif
        {
            auto tx_it = tx_progress_.try_emplace(txm, cmd_id, clock_ts);
            if (!tx_it.second)
            {
                tx_it.first->second.cmd_id_ = cmd_id;
                tx_it.first->second.wait_clock_ts_ = clock_ts;
            }
        }
    }
#endif

    bool IsIdle()
    {
        return active_tx_cnt_.load(std::memory_order_relaxed) == 0 &&
               local_cc_shards_.IsIdle(thd_id_) &&
               !terminated_.load(std::memory_order_relaxed);
    }

    bool AllTxFinished()
    {
#ifdef EXT_TX_PROC_ENABLED
        active_tx_lock_.Lock();
        int external_ongoing_tx_cnt = active_tx_map_.size();
        active_tx_lock_.Unlock();

        return external_ongoing_tx_cnt == 0 &&
               active_tx_cnt_.load(std::memory_order_relaxed) == 0;
#else
        return active_tx_cnt_.load(std::memory_order_relaxed) == 0;
#endif
    }

private:
    /**
     * @brief Notifies the tx processor that a tx or a cc request waits to be
     * processed and wakes up the processor if it is asleep and there is no
     * exteranl thread to process. Even though in general std::mutex is not need
     * for cv.notify(), the method intentionally places std::mutex before
     * notify(). This is because tx's or cc requests are managed via lock-free
     * data structures. The std::mutex in this method creates a barrier, forcing
     * the lock-free mutations to precede cv.notify(), so that whoever woken up
     * will see the effects of the mutations. Moreover, it creates a critical
     * section such that the to-sleep tx processor either precedes cv.notify(),
     * thereby being woken up by the notify signal, or succeeds cv.notify(),
     * thereby detecting the tx or cc request mutations and thus not entering
     * the sleep mode.
     *
     */
    void Notify(std::mutex &sleep_mux, std::condition_variable &sleep_cv)
    {
        std::unique_lock<std::mutex> lk(sleep_mux);
        sleep_cv.notify_one();
    }

    /**
     * @brief This method is only utilized for sampling the tx_duration metric.
     */
    inline size_t CheckAndUpdateTxCurrentRound()
    {
        return (tx_current_round_++ % metrics::collect_tx_duration_round) == 0;
    };

    size_t thd_id_;
    std::atomic<bool> terminated_;
    std::atomic<TxProcessorStatus> tx_proc_status_{TxProcessorStatus::Busy};

    LocalCcShards &local_cc_shards_;
    std::unique_ptr<LocalCcHandler> cc_hd_;

    std::atomic<uint16_t> active_tx_cnt_;
    std::atomic<uint16_t> new_tx_cnt_;
    moodycamel::ConcurrentQueue<TransactionExecution::uptr> new_txs_;
    moodycamel::ConsumerToken new_tx_token_;

    CircularQueue<TransactionExecution::uptr> idle_txs_{100};
    CircularQueue<TransactionExecution::uptr> on_fly_txs_{100};

    moodycamel::ConcurrentQueue<TransactionExecution::uptr> free_txs_;

    TxLog *txlog_hd_;

    std::shared_ptr<TxProcCoordinator> coordi_;

#ifdef EXT_TX_PROC_ENABLED
    /**
     * @brief The number of rounds this shard has been processed. We use the
     * number to track if external threads have visited the shard for a certain
     * amount of time, and if not (because of external threads being occupied),
     * wake up the native tx processor to process the shard's binding active
     * tx's and cc requests.
     *
     */
    std::atomic<size_t> one_round_cnt_{0};

    std::unordered_map<TransactionExecution *, TransactionExecution::uptr>
        active_tx_map_;
    SimpleSpinlock active_tx_lock_;
    ConcurrentQueueWSize<TransactionExecution *> resume_tx_queue_;

    struct TxProgress
    {
        TxProgress() = delete;
        TxProgress(uint16_t cmd_id, uint64_t clock_ts)
            : cmd_id_(cmd_id), wait_clock_ts_(clock_ts)
        {
        }

        uint16_t cmd_id_;
        uint64_t wait_clock_ts_;
    };

#ifdef ON_KEY_OBJECT
    // The map of transaction with blocked operation and its TxProcess
    std::unordered_map<TransactionExecution *, TxProgress> tx_progress_block_;
    uint64_t progress_check_ts_block_{0};
#endif
    std::unordered_map<TransactionExecution *, TxProgress> tx_progress_;
    uint64_t progress_check_ts_{0};
#endif

    metrics::TimePoint busy_round_start_;
    bool is_busy_round_{false};
    size_t busy_round_processed_cc_req_count_{0};
    size_t busy_round_active_tx_count_{0};
    size_t empty_round_count_{0};
    size_t total_round_count_{0};
    size_t empty_round_threshold_{1000};

    // tx_current_round_ is only utilized for sampling the tx_duration and
    // remote request metric.
    size_t tx_current_round_{1};

public:
    friend class TxService;
    friend struct txservice::SplitFlushRangeOp;
    friend class TransactionExecution;
    friend struct TxProcCoordinator;
};

class TxService
{
public:
    TxService(
        CatalogFactory *catalog_factory,
        SystemHandler *system_handler,
        const std::map<std::string, uint32_t> &conf,
        uint32_t node_id,  // = 0,
        uint32_t ng_id,    // = 0,
        std::unordered_map<uint32_t, std::vector<NodeConfig>>
            *ng_configs,                    // = nullptr,
        uint64_t cluster_config_version,    // = 0,
        store::DataStoreHandler *store_hd,  // = nullptr,
        TxLog *log_hd,                      // = nullptr,
        bool enable_mvcc = true,
        bool skip_wal = false,
        bool skip_kv = false,  // only used in eloqkv
        bool enable_cache_replacement = true,
        metrics::MetricsRegistry *metrics_registry = nullptr,
        metrics::CommonLabels common_labels = {},
        std::unordered_map<TableName, std::string> *prebuilt_tables = nullptr,
        std::function<void(std::string_view, std::string_view)> publish_func =
            nullptr,
        std::vector<std::tuple<metrics::Name,
                               metrics::Type,
                               std::vector<metrics::LabelGroup>>>
            external_metrics = {})
        : local_cc_shards_(node_id,
                           ng_id,
                           conf,
                           catalog_factory,
                           system_handler,
                           ng_configs,  // here only need ng_configs.size()
                           cluster_config_version,
                           store_hd,
                           this,
                           enable_mvcc,
                           metrics_registry,
                           common_labels,
                           prebuilt_tables,
                           publish_func),
          ckpt_(local_cc_shards_,
                store_hd,
                conf.at("checkpointer_interval"),
                log_hd,
                conf.at("checkpointer_delay_seconds"))
    {
        assert(store_hd != nullptr || skip_kv);
        uint32_t core_cnt = conf.at("core_num");
        pool_.reserve(core_cnt);
        thd_pool_.reserve(core_cnt);

        for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
        {
            if (metrics::enable_metrics)
            {
                common_labels["core_id"] = std::to_string(thd_idx);
                pool_.emplace_back(
                    std::make_unique<TxProcessor>(thd_idx,
                                                  local_cc_shards_,
                                                  log_hd,
                                                  metrics_registry,
                                                  common_labels,
                                                  external_metrics));
            }
            else
            {
                pool_.emplace_back(std::make_unique<TxProcessor>(
                    thd_idx, local_cc_shards_, log_hd));
            }
        }

        txservice_skip_wal = skip_wal;
        txservice_skip_kv = skip_kv;
        txservice_enable_cache_replacement = enable_cache_replacement;

        if (conf.find("enable_key_cache") != conf.end())
        {
            if (enable_mvcc && conf.at("enable_key_cache"))
            {
                LOG(WARNING) << "Txservice key cache is disabled due to "
                                "incompatibility with MVCC.";
            }
            // Key cache is only available in non-mvcc mode.
            txservice_enable_key_cache =
                conf.at("enable_key_cache") && !enable_mvcc;
        }

        if (txservice_skip_kv)
        {
            if (txservice_enable_cache_replacement)
            {
                LOG(WARNING) << "Txservice cache replacement is disabled since "
                                "no kv is attached.";
                txservice_enable_cache_replacement = false;
            }
        }
    }

    int Start(uint32_t node_id,
              uint32_t ng_id,
              const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
                  *ng_configs,
              uint64_t cluster_config_version,
              const std::vector<std::string> *txlog_ips,
              const std::vector<uint16_t> *txlog_ports,
              const std::string *hm_ip,
              const uint16_t *hm_port,
              const std::string *hm_bin_path,
              const std::map<std::string, uint32_t> &conf,
              std::unique_ptr<TxLog> log_agent,
              const std::string &local_path,
              bool enable_brpc_builtin_services = true,
              bool fork_host_manager = true)
    {
        if (!txservice_enable_cache_replacement && !txservice_skip_kv)
        {
            if (local_cc_shards_.store_hd_->IsSharedStorage())
            {
                LOG(ERROR) << "Share storage is not supported when cache "
                              "replacement is disabled";
                return -1;
            }
        }

#ifdef ON_KEY_OBJECT
        // must start before host_manager
        store::SnapshotManager::Instance().Init(local_cc_shards_.store_hd_);
        store::SnapshotManager::Instance().Start();
#endif
        uint16_t ng_rep_cnt = (uint16_t) conf.at("rep_group_cnt");
        if (Sharder::Instance().Init(node_id,
                                     ng_id,
                                     ng_configs,
                                     cluster_config_version,
                                     txlog_ips,
                                     txlog_ports,
                                     hm_ip,
                                     hm_port,
                                     hm_bin_path,
                                     &local_cc_shards_,
                                     std::move(log_agent),
                                     local_path,
                                     ng_rep_cnt,
                                     enable_brpc_builtin_services,
                                     fork_host_manager) < 0)

        {
            return -1;
        }
        TxStartTsCollector::Instance().Init(
            &local_cc_shards_,
            conf.at("collect_active_tx_ts_interval_seconds"));
        DeadLockCheck::Init(local_cc_shards_);
        for (size_t thd_idx = 0; thd_idx < pool_.size(); ++thd_idx)
        {
            TxProcessor *tp = pool_[thd_idx].get();

            tp->InitializeLocalHandler();
            thd_pool_.emplace_back(std::thread([tp] { tp->Run(); }));
        }
#if defined(EXT_TX_PROC_ENABLED) && defined(ON_KEY_OBJECT)
        // set ext_tx_prc_func to brpc
        bthread_set_ext_tx_prc_func(GetTxProcFunctors());
#endif

        // Start cc stream receiver server.
        Sharder::Instance().StartCcStreamReceiver(enable_brpc_builtin_services);

        // Connect cc stream sender to remote nodes
        Sharder::Instance().ConnectCcStreamSender();

        if (local_cc_shards_.EnableMvcc())
        {
            TxStartTsCollector::Instance().Start();
        }
        local_cc_shards_.StartBackgroudWorkers();
        return 0;
    }

    void WaitClusterReady()
    {
        Sharder::Instance().WaitClusterReady();
    }

    void Shutdown()
    {
#ifdef ON_KEY_OBJECT
        store::SnapshotManager::Instance().Shutdown();
#endif
        DeadLockCheck::SetStop();
        ckpt_.Terminate();
        ckpt_.Join();
        // Terminate the DataSync thds.
        local_cc_shards_.Terminate();
        if (local_cc_shards_.EnableMvcc())
        {
            TxStartTsCollector::Instance().Shutdown();
        }

        Sharder::Instance().Shutdown();

        for (size_t thd_idx = 0; thd_idx < thd_pool_.size(); ++thd_idx)
        {
            pool_[thd_idx]->Terminate();
        }
        for (auto &thd_idx : thd_pool_)
        {
            thd_idx.join();
        }

        // Maybe there has remote request in cache, so here close stream sender
        // after TxProcessor terminated.
        Sharder::Instance().CloseStreamSender();
        DeadLockCheck::Free();
    }

    TransactionExecution *NewTx()
    {
        // The rand seed will be initialized automatically.
        static thread_local uint32_t tx_runs = butil::fast_rand();
        // Based on the OFFSET_TABLE, each thread has its own tx_run pattern,
        // and the workloads are balanced between TxProcessors.
        static thread_local uint32_t tx_run_offset =
            OFFSET_TABLE[tx_runs % ARRAY_SIZE(OFFSET_TABLE)];
        uint32_t run_cnt = tx_runs;
        tx_runs += tx_run_offset;
        size_t sid = run_cnt % pool_.size();
        return pool_[sid]->NewTx();
    }

    bool AllTxFinished()
    {
        return std::all_of(pool_.begin(),
                           pool_.end(),
                           [](const std::unique_ptr<TxProcessor> &tp)
                           { return tp->AllTxFinished(); });
    }

#ifdef EXT_TX_PROC_ENABLED
    TransactionExecution *NewTx(size_t shard_id)
    {
        size_t sid =
            shard_id < pool_.size() ? shard_id : (shard_id % pool_.size());
        return pool_[sid]->NewExternalTx();
    }

#ifdef ON_KEY_OBJECT
    std::function<std::tuple<std::function<void()>,
                             std::function<void(int16_t)>,
                             std::function<bool(bool)>,
                             std::function<bool()>>(int16_t)>
    GetTxProcFunctors()
    {
        return [this](int16_t group_id)
        {
            assert(group_id >= 0);
            int16_t sid = group_id % pool_.size();
            return std::make_tuple(pool_[sid]->TxProcessorFunctor(),
                                   pool_[sid]->UpdateExtProcFunctor(),
                                   pool_[sid]->OverrideShardHeapFunctor(),
                                   pool_[sid]->HasWork());
        };
    }
#else
    std::function<
        std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    GetTxProcFunctors()
    {
        return [this](int16_t group_id)
        {
            assert(group_id >= 0);
            int16_t sid = group_id % pool_.size();
            return std::make_pair(pool_[sid]->TxProcessorFunctor(),
                                  pool_[sid]->UpdateExtProcFunctor());
        };
    }
#endif
#endif

    LocalCcShards &CcShards()
    {
        return local_cc_shards_;
    }

    LocalCcShards local_cc_shards_;
    std::vector<std::unique_ptr<TxProcessor>> pool_;
    std::vector<std::thread> thd_pool_;
    Checkpointer ckpt_;

    friend class txservice::fault::RecoveryService;
};

}  // namespace txservice
