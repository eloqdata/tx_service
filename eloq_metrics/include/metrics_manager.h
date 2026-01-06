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

#include <algorithm>

#include "metrics.h"
#include "metrics_collector.h"
#include "prometheus_collector.h"

namespace metrics
{
const std::string BIND_ADDR_ENV = "MOMO_METRICS_PUSH_ADDR";
const std::string BIND_PORT_ENV = "ELOQ_METRICS_PORT";
const std::string COLLECTOR_PROVIDER = "ELOQ_METRICS_COLLECTOR_PROVIDER";

enum class MetricsMgrErrors
{
    Success,
    UnSupportMetricCollector,
    CollectorOpenErr,
};

class MetricsMgr
{
public:
    const std::string default_bind_addr_ = "0.0.0.0";
    const std::string default_port_ = "18081";

    struct MetricsMgrResult;
    MetricHandle MetricsRegistry(std::unique_ptr<Metric> metric);

    std::shared_ptr<MetricsCollector> GetCollector() const
    {
        return metrics_collector_;
    }

    static metrics::MetricsMgr::MetricsMgrResult GetMetricMgrInstance();

    ~MetricsMgr();

    MetricsMgr(MetricsMgr &&) = delete;
    MetricsMgr(const MetricsMgr &) = delete;
    void operator=(const MetricsMgr &) = delete;
    void operator=(MetricsMgr &&) = delete;
    bool operator==(const MetricsMgr &);

private:
    MetricsMgr() : metrics_collector_{nullptr}
    {
        auto bind_port_var = std::getenv(BIND_PORT_ENV.c_str());
        auto bind_addr_var = std::getenv(BIND_ADDR_ENV.c_str());
        auto collector_provider = std::getenv(COLLECTOR_PROVIDER.c_str());

        std::string collector_provider_normalize;
        if (collector_provider == nullptr)
        {
#ifdef WITH_GLOG
            LOG(INFO) << "ENV=" << COLLECTOR_PROVIDER << " not found "
                      << ",set ELOQ_METRICS_COLLECTOR_PROVIDER=prometheus";
#endif
            collector_provider_normalize = "prometheus";
        }
        else
        {
            auto collector_provider_str = std::string(collector_provider);
            std::transform(collector_provider_str.begin(),
                           collector_provider_str.end(),
                           collector_provider_normalize.begin(),
                           ::tolower);
        }
        if (collector_provider_normalize != "prometheus")
        {
#ifdef WITH_GLOG
            LOG(INFO) << "only support prometheus as metrics collector";
            mgr_init_err_ = MetricsMgrErrors::UnSupportMetricCollector;
#endif
        }
        auto bind_addr =
            bind_addr_var == nullptr ? default_bind_addr_ : bind_addr_var;
        auto bind_port_str =
            bind_port_var == nullptr ? default_port_ : bind_port_var;

        auto bind_port = std::stoi(bind_port_str);

#ifdef WITH_GLOG
        LOG(INFO) << "MetricsCollector BIND_PORT=" << bind_port
                  << ",BIND_ADDR=" << bind_addr
                  << ",COLLECTOR_PROVIDER=" << collector_provider_normalize;
#endif
        auto registry = std::make_shared<prometheus::Registry>();

        metrics_collector_ =
            std::make_shared<PrometheusCollector>(bind_addr, bind_port);
        if (!metrics_collector_->Open())
        {
            mgr_init_err_ = MetricsMgrErrors::CollectorOpenErr;
        }
    }

private:
    std::shared_ptr<MetricsCollector> metrics_collector_;
    MetricsMgrErrors mgr_init_err_ = MetricsMgrErrors::Success;
};

struct MetricsMgr::MetricsMgrResult
{
    metrics::MetricsMgr *mgr_;
    const char *not_ok_;

    MetricsMgrResult() = delete;

    explicit MetricsMgrResult(MetricsMgr *mgr, const char *error_msg)
        : mgr_{mgr}, not_ok_{error_msg}
    {
    }
};
}  // namespace metrics
