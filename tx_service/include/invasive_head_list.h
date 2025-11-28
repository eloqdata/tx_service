#pragma once

#include <atomic>
#include <cassert>

namespace txservice
{
/**
 * @brief A concurrent list that only allows to add and remove from the head.
 * The list is invasive in that it assumes the element class has a member
 * variable next_ pointing to the next element in the list. Adding and removing
 * from the head concurrently is usually a bad idea as it causes contention on a
 * single point between producers (adders) and consumers (removers). However,
 * the design serves well in two scenarios. First, producers and consumers are
 * in most cases from the same thread. This is the case when we use it as a
 * resource pool in a shard, which in normal execution has a single worker
 * thread. Second, the list has a single consumer who always tries to pop all
 * elements at once. This is the case when the worker thread of the shard in
 * every loop gets all inbound requests and resumed transactions to process.
 *
 * @tparam T
 */
template <typename T>
class InvasiveHeadList
{
public:
    void Add(T *element)
    {
        T *head = head_.load(std::memory_order_relaxed);
        do
        {
            element->next_ = head;
            if (head_.compare_exchange_weak(
                    head, element, std::memory_order_acq_rel))
            {
                return;
            }
        } while (true);
    }

    T *Pop()
    {
        T *head = head_.load(std::memory_order_relaxed);
        do
        {
            // The head is nullptr. There is nothing to pop from the list.
            if (head == nullptr)
            {
                return nullptr;
            }

            T *next = head->next_;
            if (head_.compare_exchange_weak(
                    head, next, std::memory_order_acq_rel))
            {
                head->next_ = nullptr;
                return head;
            }
        } while (true);
    }

    T *PopAll()
    {
        T *head = head_.exchange(nullptr, std::memory_order_release);
        return head;
    }

private:
    std::atomic<T *> head_{nullptr};
};
}  // namespace txservice
