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

#include "memory"
#include "metrics.h"
#include "prometheus/client_metric.h"

namespace metrics
{

/// for now only support prometheus.
enum class CollectorProvider
{
    Prometheus __attribute__((unused))
};

class MetricsCollector
{
public:
    virtual ~MetricsCollector() = default;

    explicit MetricsCollector(std::string address = "0.0.0.0",
                              const uint32_t port = 18081)
    {
        MetricsCollector::address_ = std::move(address);
        MetricsCollector::port_ = std::to_string(port);
    }

    virtual bool Open() = 0;

    // Updated: Return MetricHandle with embedded data
    virtual MetricHandle SetMetric(std::unique_ptr<Metric> &) = 0;

    // Updated: Accept MetricHandle instead of separate key and type
    virtual bool Collect(const MetricHandle &, const Value &) = 0;

    // Updated: Accept MetricHandle instead of separate key and type
    virtual prometheus::ClientMetric CollectClientMetrics(
        const MetricHandle &) = 0;

protected:
    std::string address_;
    std::string port_;
};
}  // namespace metrics
