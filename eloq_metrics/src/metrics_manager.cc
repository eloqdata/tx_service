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
#include "metrics_manager.h"

#include "metrics.h"

namespace metrics
{
bool MetricsMgr::operator==(const metrics::MetricsMgr &other)
{
    if (other.mgr_init_err_ != this->mgr_init_err_)
    {
        return false;
    }

    if (other.metrics_collector_ != this->metrics_collector_)
    {
        return false;
    }
    return true;
}
MetricHandle MetricsMgr::MetricsRegistry(std::unique_ptr<Metric> metric)
{
    auto metric_ref = metric.get();
    auto metric_type = metric_ref->type_;
    auto metric_key = metrics_collector_->SetMetric(metric);
    return MetricHandle(metric_key, metric_type);
}

MetricsMgr::MetricsMgrResult MetricsMgr::GetMetricMgrInstance()
{
    static MetricsMgr metric_mgr;
    std::string err_msg;
    switch (metric_mgr.mgr_init_err_)
    {
    case MetricsMgrErrors::UnSupportMetricCollector:
        err_msg =
            "Not support metric collector type. for now only support "
            "prometheus.";
        break;
    case MetricsMgrErrors::CollectorOpenErr:
        err_msg = "Metrics Collector Open failure. Possible port conflict";
        break;
    case MetricsMgrErrors::Success:
        err_msg = "";
        break;
    default:
        break;
    }
    if (err_msg.empty())
    {
        return MetricsMgr::MetricsMgrResult{&metric_mgr, nullptr};
    }
    else
    {
        char *msg_char_ptr = new char[err_msg.size()];
        std::copy(err_msg.begin(), err_msg.end(), msg_char_ptr);
        return MetricsMgr::MetricsMgrResult{nullptr, msg_char_ptr};
    }
}

MetricsMgr::~MetricsMgr() = default;
}  // namespace metrics
