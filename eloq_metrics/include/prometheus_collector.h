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
#ifdef WITH_GLOG
#include <glog/logging.h>
#endif

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <memory>
#include <string>
#include <vector>

#include "metrics.h"
#include "metrics_collector.h"

namespace metrics
{

// Prometheus-specific metric data storage
class PrometheusMetricData : public MetricCollectorData
{
public:
    // Constructor for Histogram
    explicit PrometheusMetricData(prometheus::Histogram &histogram)
        : histogram_(&histogram), gauge_(nullptr), counter_(nullptr)
    {
    }

    // Constructor for Gauge
    explicit PrometheusMetricData(prometheus::Gauge &gauge)
        : histogram_(nullptr), gauge_(&gauge), counter_(nullptr)
    {
    }

    // Constructor for Counter
    explicit PrometheusMetricData(prometheus::Counter &counter)
        : histogram_(nullptr), gauge_(nullptr), counter_(&counter)
    {
    }

    bool Collect(const Value &metric_value, const Type &metric_type) override;

    // Accessors for Prometheus-specific types (used by PrometheusCollector)
    prometheus::Histogram *GetHistogram() const
    {
        return histogram_;
    }
    prometheus::Gauge *GetGauge() const
    {
        return gauge_;
    }
    prometheus::Counter *GetCounter() const
    {
        return counter_;
    }

private:
    prometheus::Histogram *histogram_;
    prometheus::Gauge *gauge_;
    prometheus::Counter *counter_;
};

static const std::vector<double> PROMETHEUS_HISTOGRAM_DEF_BUCKETS = {
    1e+1, 2e+1, 4e+1, 6e+1, 8e+1,  // <  100 us
    1e+2, 2e+2, 4e+2, 6e+2, 8e+2,  // <    1 ms
    1e+3, 2e+3, 4e+3, 6e+3, 8e+3,  // <   10 ms
    1e+4, 2e+4, 4e+4, 6e+4, 8e+4,  // <  100 ms
    1e+5, 2e+5, 4e+5, 6e+5, 8e+5,  // <    1  s
    1e+6, 2e+6, 4e+6, 6e+6, 8e+6,  // <   10  s
    1e+7                           // >=  10+ s
};

class PrometheusCollector : public MetricsCollector
{
public:
    const std::string DEFAULT_METRICS_URL = "/eloq_metrics";

public:
    explicit PrometheusCollector(std::string host, uint32_t port);

    PrometheusCollector(const PrometheusCollector &) = delete;
    PrometheusCollector(PrometheusCollector &&) = delete;

    bool Open() override;

    // Updated SetMetric to return MetricHandle with embedded data
    MetricHandle SetMetric(std::unique_ptr<Metric> &metric_ptr) override;

    // Updated Collect to use data from handle
    bool Collect(const MetricHandle &handle,
                 const Value &metric_value) override;

    prometheus::ClientMetric CollectClientMetrics(
        const MetricHandle &handle) override;

    ~PrometheusCollector() override;

private:
    [[nodiscard]] static prometheus::Labels Convert2Labels(
        const Labels &labels);

private:
    std::shared_ptr<prometheus::Registry> registry_ =
        std::make_shared<prometheus::Registry>();
    std::unique_ptr<prometheus::Exposer> exposer_;
    // Removed: histograms_, gauges_, counters_ maps - data is now in
    // MetricHandle
};
}  // namespace metrics
