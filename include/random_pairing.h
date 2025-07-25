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

// Implement random pairing algorithm(reservoir sample)
// *A Dip in the Reservoir: Maintaining Sample Synopses of Evolving Datasets*
// https://www.vldb.org/conf/2006/p595-gemulla.pdf

#include <algorithm>
#include <cassert>
#include <random>
#include <vector>

#include "type.h"  // Default Copy function.

namespace txservice
{
template <typename T, typename CopyT = Copy<T>>
class RandomPairing
{
public:
    explicit RandomPairing(uint32_t capacity) : capacity_(capacity)
    {
    }

    RandomPairing(const std::vector<T> &keys, uint32_t capacity)
        : capacity_(capacity)
    {
        sample_pool_.reserve(capacity_);

        for (size_t i = 0; i < keys.size(); ++i)
        {
            const T &key = keys[i];

            // keys.size() maybe larger than capacity_, in case like shrink node
            // groups. By calling Insert, re-sample from the original sample
            // pool, and shrink sample pool size to capacity_.
            Insert(key, i + 1);
        }
    }

    void Insert(const T &key, uint64_t dataset)
    {
        if (c1_ + c2_ <= 0)
        {
            if (static_cast<uint32_t>(sample_pool_.size()) < capacity_)
            {
                Insert(key);
                assert(
                    std::is_sorted(sample_pool_.begin(), sample_pool_.end()));
            }
            else
            {
                std::uniform_int_distribution<uint64_t> random_dis(0,
                                                                   dataset - 1);
                uint64_t random = random_dis(random_dev_);
                if (random < capacity_)
                {
                    Replace(random, key);
                    assert(std::is_sorted(sample_pool_.begin(),
                                          sample_pool_.end()));
                }
            }
        }
        else
        {
            std::uniform_int_distribution<uint64_t> random_dis(0,
                                                               c1_ + c2_ - 1);
            uint64_t random = random_dis(random_dev_);

            if (random < c1_)
            {
                assert(sample_pool_.size() < capacity_);
                assert(c1_ > 0);
                c1_ -= 1;

                Insert(key);
                assert(
                    std::is_sorted(sample_pool_.begin(), sample_pool_.end()));
            }
            else
            {
                assert(c2_ > 0);
                c2_ -= 1;
            }
        }
    }

    void Delete(const T &key, uint64_t dataset)
    {
        auto iter =
            std::lower_bound(sample_pool_.begin(), sample_pool_.end(), key);
        if (iter != sample_pool_.end() && *iter == key)
        {
            c1_ += 1;
            sample_pool_.erase(iter);

            assert(std::is_sorted(sample_pool_.begin(), sample_pool_.end()));
            assert(sample_pool_.size() < capacity_);
        }
        else
        {
            if (!sample_pool_.empty())
            {
                if (dataset >= sample_pool_.size())
                {
                    c2_ += 1;
                }
                else
                {
                    std::uniform_int_distribution<uint64_t> random_dis(0,
                                                                       dataset);

                    uint64_t random = random_dis(random_dev_);
                    sample_pool_.erase(sample_pool_.begin() + random);
                }
            }
        }
    }

    const std::vector<T> &SampleKeys() const
    {
        return sample_pool_;
    }

    uint64_t Size() const
    {
        return sample_pool_.size();
    }

    uint32_t Capacity() const
    {
        return capacity_;
    }

    void Clear()
    {
        ClearCounter();
        sample_pool_.clear();
    }

    void ClearCounter()
    {
        c1_ = 0;
        c2_ = 0;
    }

private:
    void Insert(const T &key)
    {
        assert(sample_pool_.size() < capacity_);

        auto iter =
            std::lower_bound(sample_pool_.begin(), sample_pool_.end(), key);

        if (iter != sample_pool_.end())
        {
            if (key < *iter)
            {
                T dup_key;
                CopyT()(dup_key, key);
                sample_pool_.insert(iter, std::move(dup_key));
            }
        }
        else
        {
            sample_pool_.push_back(key);
        }

        assert(sample_pool_.size() <= capacity_);
    }

    void Replace(uint64_t random, const T &key)
    {
        auto target_iter =
            std::lower_bound(sample_pool_.begin(), sample_pool_.end(), key);
        if (target_iter != sample_pool_.end() && *target_iter == key)
        {
            return;
        }

        auto random_iter = sample_pool_.begin() + random;
        CopyT()(*random_iter, key);

        if (target_iter < random_iter)
        {
            std::rotate(target_iter, random_iter, random_iter + 1);
        }
        else if (target_iter > random_iter + 1)
        {
            std::rotate(random_iter, random_iter + 1, target_iter);
        }
        else
        {
            // key already in correct position.
        }
    }

private:
    // no. of deletions which have been in the sample
    uint32_t c1_{0};

    // no. of deletions which have not been in the sample
    uint32_t c2_{0};

    // order array
    std::vector<T> sample_pool_;

    std::mt19937_64 random_dev_;

    uint32_t capacity_{0};
};
}  // namespace txservice
