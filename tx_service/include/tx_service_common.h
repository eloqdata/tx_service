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
#include <mimalloc-2.1/mimalloc.h>

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace txservice
{

class TxProcessor;

// whether skip write redo log to log_service.
inline bool txservice_skip_wal = false;
// whether skip accessing KV when cc map cache misses.
inline bool txservice_skip_kv = false;
// max latency of each sequence group between primary and standby nodes.
inline uint64_t txservice_max_standby_lag = 400000;
// If checkpointed data can be evicted from memory if memory is full. If this is
// off, all data will be cached in memory.
inline bool txservice_enable_cache_replacement = true;

// Whether to automatically redirect redis command to the leader node when the
// data is not on the local node.
inline bool txservice_auto_redirect_redis_cmd = true;

enum struct TxShardStatus
{
    Uninitialized = 0,
    Free,
    Occupied,
    Deconstructed
};

struct TxProcCoordinator
{
    explicit TxProcCoordinator(int32_t core_id,
                               TxProcessor *processor = nullptr)
        : core_id_(core_id), tx_processor_(processor)
    {
    }

#ifdef ELOQ_MODULE_ENABLED
    void NotifyExternalProcessor() const;
#endif

    void UpdateExtTxProcessorCnt(int16_t delta);

    int32_t core_id_{-1};
    std::mutex sleep_mux_;
    std::condition_variable sleep_cv_;
    std::atomic<TxShardStatus> shard_status_{TxShardStatus::Uninitialized};
    std::atomic<TxProcessor *> tx_processor_{nullptr};
#ifdef EXT_TX_PROC_ENABLED
    // The external txm count. If it's not zero, the external processor
    // shouldn't sleep.
    std::atomic<int16_t> external_txm_cnt_{0};
    std::atomic<int16_t> ext_processor_cnt_{0};
    // Original thread default heap for external tx processor.
    // This is only set when external tx processor occupies the shard,
    // and only should be access after occupying shard_status_.
    mi_heap_t *ext_tx_proc_heap_{nullptr};

#if defined(WITH_JEMALLOC)
    // unsigned ext_tx_arena_id_{0};
#endif
#endif
};
}  // namespace txservice
