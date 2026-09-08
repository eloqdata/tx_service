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

#include <algorithm>
#include <memory>
#include <vector>

#include "cc_req_base.h"
#include "circular_queue.h"

namespace txservice
{
template <typename T>
class CcRequestPool
{
public:
    /**
     * Retains at most max_size request objects; zero disables the pool.
     * NextRequest returns nullptr when every retained request is in use and
     * the limit has been reached. Acquisition runs on the owning shard.
     */
    explicit CcRequestPool(size_t max_size = UINT64_MAX)
        : head_(0), max_size_(max_size)
    {
        const size_t initial_size = std::min<size_t>(8, max_size_);
        pool_.reserve(initial_size);
        for (size_t idx = 0; idx < initial_size; ++idx)
        {
            pool_.emplace_back(std::make_unique<T>());
        }
    }

    T *NextRequest()
    {
        size_t count = 0;

        CcRequestBase *req_ptr = nullptr;
        while (count < pool_.size())
        {
            req_ptr = static_cast<CcRequestBase *>(pool_[head_].get());

            if (!req_ptr->InUse())
            {
                break;
            }

            ++head_;
            head_ = head_ == pool_.size() ? 0 : head_;
            ++count;
        }

        if (count == pool_.size())
        {
            if (count >= max_size_)
            {
                return nullptr;
            }

            const size_t old_size = pool_.size();
            // Clamp the increment before adding it to avoid exceeding the
            // limit or overflowing the unlimited default's size arithmetic.
            const size_t growth = std::min(max_size_ - old_size,
                                           std::max<size_t>(1, old_size / 2));
            const size_t new_size = old_size + growth;
            pool_.reserve(new_size);
            for (size_t idx = old_size; idx < new_size; ++idx)
            {
                pool_.emplace_back(std::make_unique<T>());
            }
            req_ptr = static_cast<CcRequestBase *>(pool_[old_size].get());
            req_ptr->Use();
            head_ = old_size + 1;
            head_ = head_ == pool_.size() ? 0 : head_;
            return pool_[old_size].get();
        }
        else
        {
            req_ptr->Use();
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
            CcRequestBase *req_ptr =
                static_cast<CcRequestBase *>(pool_[i].get());
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
}  // namespace txservice
