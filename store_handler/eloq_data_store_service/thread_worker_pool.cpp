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
#include "thread_worker_pool.h"

#include <cassert>

namespace EloqDS
{
ThreadWorkerPool::ThreadWorkerPool(size_t max_workers_num)
    : max_workers_num_(max_workers_num)
{
    Initialize();
}

void ThreadWorkerPool::Initialize()
{
    while (shutdown_indicator_.load(std::memory_order_acquire))
    {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    // start all worker threads.
    if (workers_.empty())
    {
        for (size_t i = 0; i < max_workers_num_; i++)
        {
            std::thread worker = std::thread(
                [this]
                {
                    while (true)
                    {
                        // Acquire work queue mutex
                        std::unique_lock<std::mutex> lk(work_queue_mutex_);
                        // Wait for new work come in or shutdown happen
                        work_queue_cv_.wait(
                            lk,
                            [this]
                            {
                                return !work_queue_.empty() ||
                                       shutdown_indicator_.load(
                                           std::memory_order_acquire);
                            });
                        // Take work if work queue is not empty
                        if (!work_queue_.empty())
                        {
                            std::function<void()> work =
                                std::move(work_queue_.front());
                            work_queue_.pop_front();
                            lk.unlock();
                            // Do work
                            work();
                        }
                        else
                        {
                            // Quit loop if shutdown
                            assert(shutdown_indicator_.load(
                                std::memory_order_acquire));
                            lk.unlock();
                            break;
                        }
                    }
                });
            workers_.push_back(std::move(worker));
        }
    }
}

size_t ThreadWorkerPool::WorkQueueSize()
{
    std::unique_lock<std::mutex> lk(work_queue_mutex_);
    return work_queue_.size();
}

bool ThreadWorkerPool::SubmitWork(std::function<void()> work)
{
    std::unique_lock<std::mutex> lk(work_queue_mutex_);
    if (shutdown_indicator_.load(std::memory_order_acquire))
    {
        return false;
    }
    work_queue_.push_back(std::move(work));
    work_queue_cv_.notify_one();
    return true;
}

void ThreadWorkerPool::Shutdown()
{
    {
        std::unique_lock<std::mutex> lk(work_queue_mutex_);
        shutdown_indicator_.store(true, std::memory_order_release);
        work_queue_cv_.notify_all();
    }

    for (std::thread &worker : workers_)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }

    workers_.clear();
    shutdown_indicator_.store(false, std::memory_order_release);
}
}  // namespace EloqDS
