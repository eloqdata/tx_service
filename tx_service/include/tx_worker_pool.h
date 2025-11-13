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
#include <condition_variable>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

static constexpr size_t MAX_WORKERS_NUM = 3;

namespace txservice
{
class TxWorkerPool
{
public:
    TxWorkerPool(size_t max_workers_num = MAX_WORKERS_NUM);
    ~TxWorkerPool() = default;

    void SubmitWork(std::function<void(size_t)> work);

    void BulkSubmitWork(std::vector<std::function<void(size_t)>> work);

    size_t WorkQueueSize();
    void Shutdown();
    size_t WorkerPoolSize()
    {
        return max_workers_num_;
    }

private:
    size_t max_workers_num_;
    std::vector<std::thread> workers_;
    std::deque<std::function<void(size_t)>> work_queue_;
    std::mutex work_queue_mutex_;
    std::condition_variable work_queue_cv_;
    std::atomic<bool> shutdown_indicator_{false};
};
}  // namespace txservice
