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

#if ELOQ_METRICS_WITH_ABSEIL
#include <absl/container/flat_hash_map.h>
#else
#include <unordered_map>
#endif

#include <chrono>
#include <string>
#include <vector>

namespace metrics
{
inline bool enable_metrics = false;

class Name
{
public:
    Name(std::string name) : name_(std::move(name)) {};

    const std::string &GetName() const
    {
        return name_;
    };

private:
    const std::string name_;
};

using Clock = std::chrono::steady_clock;
using Labels = std::vector<std::pair<std::string, std::string>>;
using TimePoint = decltype(Clock::now());
using MetricKey = size_t;

#if ELOQ_METRICS_WITH_ABSEIL
template <typename K, typename V>
using Map = absl::flat_hash_map<K, V>;
#else
template <typename K, typename V>
using Map = std::unordered_map<K, V>;
#endif

using CommonLabels = Map<std::string, std::string>;

enum class Type
{
    Gauge,
    Counter,
    Histogram
};

struct Value
{
    enum class IncDecValue
    {
        Increment,
        Decrement,
        None,
    };
    IncDecValue inc_value_;
    double value_;

    Value() = delete;

    explicit Value(const IncDecValue &inc_value)
        : inc_value_{inc_value}, value_{0}
    {
    }

    Value(const double &value) : inc_value_{IncDecValue::None}, value_{value}
    {
    }

    bool HasValue() const
    {
        return inc_value_ == IncDecValue::None;
    }
};

struct Metric
{
    std::string name_;
    Type type_;
    Labels labels_;

    Metric() = delete;

    Metric(const std::string &name, metrics::Type type, const Labels &labels)
        : name_(name), type_(type), labels_(labels)
    {
    }

    Metric(const std::string &name, metrics::Type type) : Metric(name, type, {})
    {
    }

    bool operator<(const Metric &metric) const;

    bool operator==(const Metric &metric) const;
};

struct MetricHash
{
    MetricKey operator()(const Metric &metric) const;
};

enum class MetricsErrors
{
    Success = 1000,
    OpenErr = -1001,
};

/**
 * @brief The entry class for metrics collection is MetricsRegistry.
 *
 * The purpose of this class is to help initialize the context of the metrics
 * collector and to provide different implementations of the metrics collector.
 * Generally only need one MetricsRegistry instance per application. The metrics
 * collection is related to the runtime and deployment environment. We currently
 * offer a Prometheus-based implementation.
 */
class MetricsRegistry
{
public:
    virtual MetricsErrors Open() = 0;
    virtual MetricKey Register(const Name &, metrics::Type, const Labels &) = 0;
    virtual void Collect(MetricKey, const Value &) = 0;
    virtual ~MetricsRegistry() = default;
};
}  // namespace metrics
