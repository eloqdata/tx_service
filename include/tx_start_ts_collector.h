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
#include <string>
#include <thread>
#include <unordered_map>

#include "fault/fault_inject.h"  // CODE_FAULT_INJECTOR

namespace txservice
{
class LocalCcShards;

class TxStartTsCollector
{
public:
    static TxStartTsCollector &Instance()
    {
        static TxStartTsCollector instance_;
        return instance_;
    }

    void Init(LocalCcShards *shards, uint32_t delay_seconds = 60);

    void Start();
    void Shutdown();

    uint64_t GlobalMinSiTxStartTs()
    {
        CODE_FAULT_INJECTOR("stop_safely_clean_archives", { return 1U; });
        return min_start_ts_.load(std::memory_order_relaxed);
    }

    void SetDelaySeconds(uint32_t secs)
    {
        delay_seconds_ = secs;
    }
    uint32_t GetDelaySeconds()
    {
        return delay_seconds_;
    }

private:
    TxStartTsCollector() = default;
    ~TxStartTsCollector() = default;

    void Run();
    uint64_t CollectMinTxStartTs();

    bool active_;
    std::mutex active_mux_;
    std::condition_variable active_cv_;

    // {ng_id -> min_tx_start_ts}
    std::unordered_map<uint32_t, uint64_t> min_start_ts_map_;
    std::atomic<uint64_t> min_start_ts_;
    // Run time period of scheduled recycling task
    uint32_t delay_seconds_;

    LocalCcShards *local_shards_;

    std::thread thd_;
};

}  // namespace txservice
