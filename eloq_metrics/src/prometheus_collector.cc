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

#include "prometheus_collector.h"

#include <prometheus/CivetServer.h>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>

#include "metrics.h"

namespace metrics
{

// PrometheusMetricData implementation
bool PrometheusMetricData::Collect(const Value &metric_value,
                                   const Type &metric_type)
{
    switch (metric_type)
    {
    case Type::Histogram:
        if (histogram_)
        {
            histogram_->Observe(metric_value.value_);
            return true;
        }
        break;
    case Type::Gauge:
        if (gauge_)
        {
            switch (metric_value.inc_value_)
            {
            case Value::IncDecValue::Increment:
                gauge_->Increment();
                break;
            case Value::IncDecValue::Decrement:
                gauge_->Decrement();
                break;
            default:
                gauge_->Set(metric_value.value_);
                break;
            }
            return true;
        }
        break;
    case Type::Counter:
        if (counter_)
        {
            if (metric_value.HasValue())
            {
                counter_->Increment(metric_value.value_);
            }
            else
            {
                counter_->Increment();
            }
            return true;
        }
        break;
    default:
        break;
    }
    return false;
}

PrometheusCollector::PrometheusCollector(std::string host, uint32_t port)
    : MetricsCollector(std::move(host), port), exposer_(nullptr)
{
}

bool PrometheusCollector::Open()
{
    try
    {
        std::unique_ptr<prometheus::Exposer> exporter_ptr =
            std::make_unique<prometheus::Exposer>(address_ + ":" + port_);
        this->exposer_ = std::move(exporter_ptr);
        this->exposer_->RegisterCollectable(this->registry_,
                                            DEFAULT_METRICS_URL);
#ifdef WITH_GLOG
        LOG(INFO) << "Prometheus Collector Init Push URL is "
                  << DEFAULT_METRICS_URL;
#endif
    }
    catch (const CivetException &civet_exception)
    {
#ifdef WITH_GLOG
        LOG(ERROR) << "Prometheus Collector Init Error. bind_port=" << port_
                   << ", Error Message" << civet_exception.what();
#else
        std::cerr << "Prometheus Collector Init Error. bind_port=" << port_
                  << ", Error Message" << civet_exception.what() << std::endl;
#endif
        return false;
    }

    return true;
}

PrometheusCollector::~PrometheusCollector()
{
    if (this->exposer_)
    {
        this->exposer_->RemoveCollectable(this->registry_, this->address_);
    }
}

MetricHandle PrometheusCollector::SetMetric(std::unique_ptr<Metric> &metric_ptr)
{
    auto option = metric_ptr.get();
    MetricHash metric_hash;
    auto metric_key = metric_hash(*option);

    auto prometheus_labels = Convert2Labels(metric_ptr->labels_);
    Type metric_type = metric_ptr->type_;

    std::shared_ptr<PrometheusMetricData> data;

    switch (metric_type)
    {
    case Type::Histogram:
    {
        auto &histogram_family = prometheus::BuildHistogram()
                                     .Name(metric_ptr->name_)
                                     .Register(*registry_);
        auto &histogram = histogram_family.Add(
            prometheus_labels, PROMETHEUS_HISTOGRAM_DEF_BUCKETS);
        data = std::make_shared<PrometheusMetricData>(histogram);
        break;
    }
    case Type::Gauge:
    {
        auto &gauge_family = prometheus::BuildGauge()
                                 .Name(metric_ptr->name_)
                                 .Register(*registry_);
        auto &gauge = gauge_family.Add(prometheus_labels);
        data = std::make_shared<PrometheusMetricData>(gauge);
        break;
    }
    case Type::Counter:
    {
        auto &counter_family = prometheus::BuildCounter()
                                   .Name(metric_ptr->name_)
                                   .Register(*registry_);
        auto &counter = counter_family.Add(prometheus_labels);
        data = std::make_shared<PrometheusMetricData>(counter);
        break;
    }
    default:
        // Return handle without data for unsupported types
        return MetricHandle(metric_key, metric_type);
    }

    return MetricHandle(metric_key, metric_type, data);
}

bool PrometheusCollector::Collect(const MetricHandle &handle,
                                  const Value &metric_value)
{
    if (!this->exposer_)
    {
#ifdef WITH_GLOG
        LOG(WARNING) << " Prometheus Collector found exposer_ is null";
#endif
        return false;
    }

    if (!handle.collector_data)
    {
        return false;
    }

    return handle.collector_data->Collect(metric_value, handle.type);
}

prometheus::ClientMetric PrometheusCollector::CollectClientMetrics(
    const MetricHandle &handle)
{
    if (!handle.collector_data)
    {
        return prometheus::ClientMetric{};
    }

    // Cast to PrometheusMetricData to access Prometheus-specific types
    auto *prom_data =
        dynamic_cast<PrometheusMetricData *>(handle.collector_data.get());
    if (!prom_data)
    {
        return prometheus::ClientMetric{};
    }

    // Use type-specific accessors to collect metrics
    switch (handle.type)
    {
    case Type::Histogram:
        if (auto *hist = prom_data->GetHistogram())
        {
            return hist->Collect();
        }
        break;
    case Type::Gauge:
        if (auto *gauge = prom_data->GetGauge())
        {
            return gauge->Collect();
        }
        break;
    case Type::Counter:
        if (auto *counter = prom_data->GetCounter())
        {
            return counter->Collect();
        }
        break;
    default:
        break;
    }
    return prometheus::ClientMetric{};
}

prometheus::Labels PrometheusCollector::Convert2Labels(const Labels &labels)
{
    std::map<std::string, std::string> prometheus_labels;
    std::for_each(
        labels.begin(),
        labels.end(),
        [&prometheus_labels](
            const std::pair<std::string, std::string> &inner_label)
        { prometheus_labels.insert({inner_label.first, inner_label.second}); });
    return prometheus_labels;
}

// PutMetricsIfAbsent removed - metric creation is now done directly in
// SetMetric()

}  // namespace metrics
