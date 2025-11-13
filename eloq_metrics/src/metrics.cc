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
#include "metrics.h"

#include <algorithm>

namespace metrics
{

MetricKey MetricHash::operator()(const Metric &metric) const
{
    auto name_hash_value = std::hash<std::string>{}(metric.name_);

    size_t metric_type_val =
        static_cast<std::underlying_type<Type>::type>(metric.type_);

    size_t seed = 0;

    for (auto &label : metric.labels_)
    {
        seed ^= std::hash<std::string>{}(label.first + label.second) +
                0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed + name_hash_value + metric_type_val;
}

bool Metric::operator<(const Metric &metric) const
{
    if (metric.labels_.size() != labels_.size())
    {
        return metric.name_ > name_ && metric.type_ > type_;
    }
    return metric.name_ > name_ && metric.type_ > type_ &&
           std::lexicographical_compare(labels_.begin(),
                                        labels_.end(),
                                        metric.labels_.begin(),
                                        metric.labels_.end());
}

bool Metric::operator==(const Metric &metric) const
{
    return metric.type_ == type_ && metric.name_ == name_ &&
           metric.labels_.size() == labels_.size() &&
           std::equal(metric.labels_.begin(),
                      metric.labels_.end(),
                      labels_.begin(),
                      labels_.end());
}
}  // namespace metrics
