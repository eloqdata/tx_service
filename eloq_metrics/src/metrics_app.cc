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
#include <vector>

#include "dummy_executor.h"
#include "metrics.h"
#include "metrics_registry_impl.h"
using namespace eloq_metrics_app;

int main()
{
#ifdef WITH_GLOG
    LOG(INFO) << "Mono Metrics Demo App Running";
#endif

    MetricsRegistryImpl::MetricsRegistryResult metrics_registry_result =
        MetricsRegistryImpl::GetRegistry();
    std::unique_ptr<metrics::MetricsRegistry> metrics_registry =
        std::move(metrics_registry_result.metrics_registry_);

    cpu_set_t cpu_set;
    std::string task_name = "DummyExecutor";
    std::vector<eloq_metrics_app::Executor *> exec_vec;
    for (int i = 0; i < 3; ++i)
    {
        std::string task_name = "Task_" + std::to_string(i);
        exec_vec.push_back(new Executor(task_name,
                                        i,
                                        cpu_set,
                                        metrics_registry.get(),
                                        {{"task", task_name}}));
    }

    for (auto &exec : exec_vec)
    {
        exec->Join();
    }

#ifdef WITH_GLOG
    LOG(INFO) << "Mono Metrics Demo Complete";
#endif
    return EXIT_SUCCESS;
}
