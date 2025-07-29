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

// Implement equal-depth histogram
// *ACCURATE ESTIMATION OF THE NUMBER OF TUPLES SATISFYING A CONDITION*
// https://dl.acm.org/doi/pdf/10.1145/971697.602294

#include <absl/container/btree_set.h>

#include <algorithm>
#include <cassert>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "schema.h"
#include "tx_key.h"

namespace txservice
{

/**
 *_________________________________________________________
 *_________________________________________________        |
 *_________________________________________        |       |
 *_________________________________        |       |       |
 *_________________________        |       |       |       |
 *_________________        |       |       |       |       |
 *                 |       |       |       |       |       |
 *---------------------------------------------------------------------------->
 *-00            S[0]    S[1]    S[2]    S[3]    S[4]    S[5]              +00
 *
 * Example: DistributionSteps with step_values_.size() == 6,
 *          (-00, +00) is divided into (step_values_.size() + 1) equal buckets.
 * */
template <typename KeyT>
class DistributionSteps
{
public:
    DistributionSteps() = default;

    explicit DistributionSteps(
        const absl::btree_set<const KeyT *, PtrLessThan<KeyT>> &keys)
    {
        size_t sz = keys.size();
        if (sz > most_steps_)
        {
            uint32_t step_length = sz / most_steps_;

            auto iter = keys.begin();
            for (uint32_t i = 0; i < most_steps_; ++i)
            {
                const KeyT *key_ptr = *iter;
                step_values_.push_back(*key_ptr);
                std::advance(iter, step_length);
            }
        }
        else
        {
            for (const KeyT *key_ptr : keys)
            {
                step_values_.push_back(*key_ptr);
            }
        }

        assert(std::is_sorted(step_values_.begin(), step_values_.end()));
    }

    bool Available() const
    {
        return !step_values_.empty();
    }

    // Return percentage of [min_key, max_key)
    double Selectivity(const KeySchema *key_schema,
                       const KeyT &min_key,
                       const KeyT &max_key) const
    {
        double sel = 0;

        if (max_key <= min_key)
        {
            return sel;
        }

        double sel_less_max_key = Selectivity(key_schema, max_key);
        double sel_less_min_key = Selectivity(key_schema, min_key);

        sel = sel_less_max_key - sel_less_min_key;

        assert(sel >= 0 && sel <= 1);

        return sel;
    }

private:
    // Percentage of Selectivity(<key)
    double Selectivity(const KeySchema *key_schema, const KeyT &key) const
    {
        assert(Available());

        typename std::vector<KeyT>::const_iterator it;

        it = std::lower_bound(step_values_.begin(), step_values_.end(), key);

        if (it == step_values_.end())
        {
            return (step_values_.size() +
                    (key.PosInInterval(
                        key_schema,
                        step_values_.back(),
                        KeyT::PackedPositiveInfinity(key_schema)))) /
                   (step_values_.size() + 1.0);
        }
        else if (it == step_values_.begin())
        {
            return key.PosInInterval(key_schema,
                                     *KeyT::PackedNegativeInfinity(),
                                     step_values_.front()) /
                   (step_values_.size() + 1.0);
        }
        else
        {
            size_t steps = std::distance(step_values_.begin(), it);
            double pos = key.PosInInterval(key_schema, *(std::prev(it)), *it);

            return (steps + pos) / (step_values_.size() + 1.0);
        }
    }

    std::string ToString() const
    {
        std::stringstream ss;
        size_t sz = step_values_.size();
        if (sz > 0)
        {
            for (size_t i = 0; i < sz - 1; ++i)
            {
                ss << step_values_[i].ToString() << ", ";
            }
            ss << step_values_.back().ToString();
        }
        return ss.str();
    }

private:
    static constexpr uint32_t most_steps_{128};
    std::vector<KeyT> step_values_;
};
}  // namespace txservice
