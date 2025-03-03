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

#include <assert.h>

#include <algorithm>
#include <random>
#include <vector>

#include "tx_key.h"
#include "type.h"

namespace txservice
{
template <typename KeyT, typename CopyKey>
class RandomPairing
{
public:
    explicit RandomPairing(uint32_t capacity) : capacity_(capacity)
    {
    }

    RandomPairing(const std::vector<KeyT> &keys, uint32_t capacity)
        : capacity_(capacity)
    {
        sample_pool_.reserve(capacity_);

        for (size_t i = 0; i < keys.size(); ++i)
        {
            const KeyT &key = keys[i];

            // keys.size() maybe larger than capacity_, in case like shrink node
            // groups. By calling Insert, re-sample from the original sample
            // pool, and shrink sample pool size to capacity_.
            Insert(key, i + 1);
        }
    }

    void Insert(const KeyT &key, uint64_t dataset)
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

    void Delete(const KeyT &key)
    {
        auto iter =
            std::lower_bound(sample_pool_.begin(), sample_pool_.end(), key);
        if (iter != sample_pool_.end() && *iter == key)
        {
            c1_ += 1;

            while (iter != sample_pool_.end() - 1)
            {
                *iter = std::move(*(iter + 1));
                iter++;
            }

            sample_pool_.resize(sample_pool_.size() - 1);

            assert(std::is_sorted(sample_pool_.begin(), sample_pool_.end()));
            assert(sample_pool_.size() < capacity_);
        }
        else
        {
            c2_ += 1;
        }
    }

    const std::vector<KeyT> &SampleKeys() const
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
    void Insert(const KeyT &key)
    {
        assert(sample_pool_.size() < capacity_);

        auto iter =
            std::lower_bound(sample_pool_.begin(), sample_pool_.end(), key);

        if (iter != sample_pool_.end())
        {
            if (key < *iter)
            {
                uint64_t i = std::distance(sample_pool_.begin(), iter);
                sample_pool_.resize(sample_pool_.size() + 1);

                for (uint64_t j = sample_pool_.size() - 1; j > i; --j)
                {
                    sample_pool_[j] = std::move(sample_pool_[j - 1]);
                }

                CopyKey()(sample_pool_[i], key);
            }
        }
        else
        {
            sample_pool_.push_back(key);
        }

        assert(sample_pool_.size() <= capacity_);
    }

    void Replace(uint64_t random, const KeyT &key)
    {
        auto iter =
            std::lower_bound(sample_pool_.begin(), sample_pool_.end(), key);
        if (iter != sample_pool_.end() && *iter == key)
        {
            return;
        }

        CopyKey()(sample_pool_[random], key);

        for (uint64_t i = random; i < sample_pool_.size() - 1 &&
                                  sample_pool_[i + 1] < sample_pool_[i];
             ++i)
        {
            std::swap(sample_pool_[i], sample_pool_[i + 1]);
        }

        for (uint64_t i = random;
             i > 0 && sample_pool_[i] < sample_pool_[i - 1];
             --i)
        {
            std::swap(sample_pool_[i], sample_pool_[i - 1]);
        }
    }

private:
    // no. of deletions which have been in the sample
    uint32_t c1_{0};

    // no. of deletions which have not been in the sample
    uint32_t c2_{0};

    // order array
    std::vector<KeyT> sample_pool_;

    std::mt19937_64 random_dev_;

    uint32_t capacity_{0};
};
}  // namespace txservice
