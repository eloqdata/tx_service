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
#include <bthread/moodycamelqueue.h>

namespace txservice
{
template <typename T,
          typename Traits = moodycamel::ConcurrentQueueDefaultTraits>
class ConcurrentQueueWSize
{
public:
    void Enqueue(const T &item)
    {
        size_.fetch_add(1, std::memory_order_relaxed);
        queue_.enqueue(item);
    }

    void Enqueue(T &&item)
    {
        size_.fetch_add(1, std::memory_order_relaxed);
        queue_.enqueue(item);
    }

    template <typename It>
    bool EnqueueBulk(It itemFirst, size_t count)
    {
        size_.fetch_add(count, std::memory_order_relaxed);
        return queue_.enqueue_bulk(itemFirst, count);
    }

    template <typename It>
    size_t TryDequeueBulk(It itemFirst, size_t max)
    {
        if (IsEmpty())
        {
            return 0;
        }

        size_t cnt = queue_.try_dequeue_bulk(itemFirst, max);
        size_.fetch_sub(cnt, std::memory_order_relaxed);
        return cnt;
    }

    template <typename U>
    bool TryDequeue(U &item)
    {
        if (IsEmpty())
        {
            return false;
        }

        bool success = queue_.try_dequeue(item);
        if (success)
        {
            size_.fetch_sub(1, std::memory_order_relaxed);
        }
        return success;
    }

    size_t SizeApprox() const
    {
        return size_.load(std::memory_order_relaxed);
    }

    bool IsEmpty() const
    {
        return size_.load(std::memory_order_relaxed) == 0;
    }

private:
    moodycamel::ConcurrentQueue<T, Traits> queue_;
    std::atomic<uint32_t> size_{0};
};
}  // namespace txservice
