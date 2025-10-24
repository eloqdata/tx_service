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

#include "metrics.h"

namespace metrics
{

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

MetricKey PrometheusCollector::SetMetric(std::unique_ptr<Metric> &metric_ptr)
{
    auto option = metric_ptr.get();
    MetricHash metric_hash;
    auto metric_key = metric_hash(*option);
    PutMetricsIfAbsent(metric_ptr, metric_key);
    return metric_key;
}

bool PrometheusCollector::Collect(MetricKey metric_key,
                                  const Value &metric_value,
                                  const Type &metric_type)
{
    if (!this->exposer_)
    {
#ifdef WITH_GLOG
        LOG(WARNING) << " Prometheus Collector found exposer_ is null";
#endif
        // Warning when exposer_ is null, but still collect the metrics for the
        // TxThreadScaleService
        return false;
    }

    switch (metric_type)
    {
    case Type::Histogram:
    {
        assert(histograms_.find(metric_key) != histograms_.end());
        histograms_.find(metric_key)->second.get().Observe(metric_value.value_);
        break;
    }
    case Type::Gauge:
    {
        assert(gauges_.find(metric_key) != gauges_.end());
        switch (metric_value.inc_value_)
        {
        case Value::IncDecValue::Increment:
            gauges_.find(metric_key)->second.get().Increment();
            break;
        case Value::IncDecValue::Decrement:
            gauges_.find(metric_key)->second.get().Decrement();
            break;
        default:
            gauges_.find(metric_key)->second.get().Set(metric_value.value_);
            break;
        }
        break;
    }
    case Type::Counter:
    {
        assert(counters_.find(metric_key) != counters_.end());
        auto &counter = counters_.find(metric_key)->second.get();
        if (metric_value.HasValue())
        {
            counter.Increment(metric_value.value_);
        }
        else
        {
            counter.Increment();
        }
        break;
    }
    default:
        break;
    }
    return true;
}

prometheus::ClientMetric PrometheusCollector::CollectClientMetrics(
    MetricKey metric_key, Type metric_type)
{
    switch (metric_type)
    {
    case Type::Histogram:
    {
        assert(histograms_.find(metric_key) != histograms_.end());
        auto &histogram = histograms_.find(metric_key)->second.get();
        const prometheus::ClientMetric histogram_metric = histogram.Collect();
        return histogram_metric;
    }
    case Type::Gauge:
    {
        assert(gauges_.find(metric_key) != gauges_.end());
        auto &gauge = gauges_.find(metric_key)->second.get();
        const prometheus::ClientMetric gauge_metric = gauge.Collect();
        return gauge_metric;
    }
    case Type::Counter:
    {
        assert(counters_.find(metric_key) != counters_.end());
        auto &counter = counters_.find(metric_key)->second.get();
        const prometheus::ClientMetric counter_metric = counter.Collect();
        return counter_metric;
    }
    default:
        assert(false);
        return prometheus::ClientMetric{};
    }
}

prometheus::Labels PrometheusCollector::Convert2Labels(const Labels &labels)
{
    std::map<std::string, std::string> prometheus_labels;
    std::for_each(
        labels.begin(),
        labels.end(),
        [&prometheus_labels](
            const std::pair<std::string, std::string> &inner_label) {
            prometheus_labels.insert({inner_label.first, inner_label.second});
        });
    return prometheus_labels;
}

void PrometheusCollector::PutMetricsIfAbsent(
    std::unique_ptr<Metric> &metric_ptr, std::size_t metric_key)
{
    auto prometheus_labels = Convert2Labels(metric_ptr->labels_);
    Type metric_type = metric_ptr->type_;
    switch (metric_type)
    {
    case Type::Histogram:
    {
        if (histograms_.find(metric_key) != histograms_.end())
        {
            break;
        }
        auto &histogram_family = prometheus::BuildHistogram()
                                     .Name(metric_ptr->name_)
                                     .Register(*registry_);

        auto &histogram = histogram_family.Add(
            prometheus_labels, PROMETHEUS_HISTOGRAM_DEF_BUCKETS);

        histograms_.emplace(metric_key, histogram);
        break;
    }
    case Type::Gauge:
    {
        if (gauges_.find(metric_key) != gauges_.end())
        {
            break;
        }
        auto &gauge_family = prometheus::BuildGauge()
                                 .Name(metric_ptr->name_)
                                 .Register(*registry_);

        auto &gauge = gauge_family.Add(prometheus_labels);
        gauges_.emplace(metric_key, gauge);
        break;
    }
    case Type::Counter:
    {
        if (counters_.find(metric_key) != counters_.end())
        {
            break;
        }
        auto &counter_family = prometheus::BuildCounter()
                                   .Name(metric_ptr->name_)
                                   .Register(*registry_);

        auto &counter = counter_family.Add(prometheus_labels);
        counters_.emplace(metric_key, counter);
        break;
    }
    default:
#ifdef WITH_GLOG
        LOG(ERROR) << "ERR: For now not support metrics_type " << &metric_type;
#endif
        ;
    }
}

}  // namespace metrics
