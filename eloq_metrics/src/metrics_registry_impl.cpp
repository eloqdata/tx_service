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
#include "metrics_registry_impl.h"

#include <cassert>

namespace eloq_metrics_app
{
MetricsRegistryImpl::MetricsRegistryResult MetricsRegistryImpl::GetRegistry()
{
    struct make_registry_shared : public MetricsRegistryImpl
    {
    };
    // Use shared_ptr for singleton pattern to ensure lifetime
    static std::shared_ptr<MetricsRegistryImpl> registry_impl =
        std::make_shared<make_registry_shared>();

    if (registry_impl->metrics_mgr_result_.not_ok_ == nullptr)
    {
        // Return shared_ptr to ensure the singleton instance remains alive
        return MetricsRegistryImpl::MetricsRegistryResult{registry_impl,
                                                          nullptr};
    }
    else
    {
        return MetricsRegistryImpl::MetricsRegistryResult{
            nullptr,
            registry_impl->metrics_mgr_result_.not_ok_,
        };
    }
}

//  This method is the one that needs to be extended, the open method does not
//  do anything for the current implementation.
metrics::MetricsErrors MetricsRegistryImpl::Open()
{
    return metrics::MetricsErrors::Success;
}

metrics::MetricHandle MetricsRegistryImpl::Register(
    const metrics::Name &name,
    metrics::Type type,
    const metrics::Labels &labels)
{
    auto metric = metrics::Metric(name.GetName(), type, labels);

    return metrics_mgr_result_.mgr_->MetricsRegistry(
        std::make_unique<metrics::Metric>(metric));
}

void MetricsRegistryImpl::Collect(const metrics::MetricHandle &handle,
                                  const metrics::Value &val)
{
    auto collector = metrics_mgr_result_.mgr_->GetCollector();
    if (collector != nullptr)
    {
        collector->Collect(handle.key, val, handle.type);
    }
}
}  // namespace eloq_metrics_app
