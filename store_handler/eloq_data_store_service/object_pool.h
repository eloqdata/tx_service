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

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "glog/logging.h"

// #include "circular_queue.h"

namespace EloqDS
{

class Poolable
{
public:
    virtual ~Poolable()
    {
    }

    bool InUse() const
    {
        return in_use_.load(std::memory_order_acquire);
    }

    void Use()
    {
        in_use_.store(true, std::memory_order_release);
    }

    void Free()
    {
        in_use_.store(false, std::memory_order_release);
    }

    virtual void Clear() = 0;

private:
    std::atomic<bool> in_use_{false};
};

class PoolableGuard
{
public:
    explicit PoolableGuard(Poolable *ptr) : ptr_(ptr)
    {
    }

    ~PoolableGuard()
    {
        if (ptr_)
        {
            ptr_->Clear();
            ptr_->Free();
        }
    }

    Poolable *Release()
    {
        Poolable *ptr = ptr_;
        ptr_ = nullptr;
        return ptr;
    }

private:
    Poolable *ptr_;
};

template <typename T>
class ObjectPool
{
public:
    explicit ObjectPool(size_t max_size = UINT64_MAX)
        : head_(0), max_size_(max_size)
    {
        pool_.reserve(8);
        for (size_t idx = 0; idx < 8; ++idx)
        {
            pool_.emplace_back(std::make_unique<T>());
        }
    }

    ~ObjectPool()
    {
        for (size_t i = 0; i < pool_.size(); i++)
        {
            T *req_ptr = pool_[i].get();
            while (req_ptr->InUse())
            {
                bthread_usleep(100);
            }
        }
    }

    T *NextObject()
    {
        size_t count = 0;

        T *obj_ptr = nullptr;
        while (count < pool_.size())
        {
            obj_ptr = pool_[head_].get();

            if (!obj_ptr->InUse())
            {
                break;
            }

            ++head_;
            head_ = head_ == pool_.size() ? 0 : head_;
            ++count;
        }

        if (count == pool_.size())
        {
            if (count == max_size_)
            {
                return nullptr;
            }

            const size_t old_size = pool_.size();
            const size_t desired = old_size + (old_size >> 1);
            const size_t new_size =
                std::min(max_size_, std::max(old_size + 1, desired));
            if (new_size <= old_size)
            {
                return nullptr;  // cannot grow further
            }
            pool_.resize(new_size);
            for (size_t idx = old_size; idx < pool_.size(); ++idx)
            {
                pool_[idx] = std::make_unique<T>();
            }
            obj_ptr = pool_[old_size].get();
            obj_ptr->Use();
            head_ = old_size + 1;
            return pool_[old_size].get();
        }
        else
        {
            obj_ptr->Use();
            T *ptr = pool_[head_].get();
            ++head_;
            head_ = head_ == pool_.size() ? 0 : head_;
            return ptr;
        }
    }

    bool IsAllFree()
    {
        for (size_t i = 0; i < pool_.size(); i++)
        {
            T *req_ptr = pool_[i].get();
            if (req_ptr->InUse())
            {
                return false;
            }
        }

        return true;
    }

private:
    std::vector<std::unique_ptr<T>> pool_;
    size_t head_;
    size_t max_size_;
};
}  // namespace EloqDS
