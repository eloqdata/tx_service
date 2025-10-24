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
#include "dummy_executor.h"

#include <boost/random.hpp>

namespace eloq_metrics_app
{
const std::string TASK_TOTAL_RUNNING_TIME = "TASK_TOTAL_RUNNING_TIME";
const metrics::Name dummy_executor_hist_metric{"dummy_executor_hist_metric"};

Executor::Executor(eloq_metrics_app::Executor &&executor) noexcept
    : inner_thread_(std::move(executor.inner_thread_))
{
}

Executor &Executor::operator=(Executor &&executor_) noexcept
{
    this->inner_thread_ = std::move(executor_.inner_thread_);
    this->task_name_ = executor_.task_name_;
    return *this;
}

Executor::Executor(std::string task_name,
                   std::size_t cpu_id,
                   cpu_set_t cpu_set,
                   metrics::MetricsRegistry *metrics_registry,
                   metrics::CommonLabels common_labels)
    : Executor(task_name, metrics_registry, common_labels)
{
    auto native_thread = this->inner_thread_->native_handle();
    CPU_ZERO(&cpu_set);
    CPU_SET(cpu_id, &cpu_set);
    pthread_setaffinity_np(native_thread, sizeof(cpu_set), &cpu_set);
}

Executor::Executor(const std::string task_name,
                   metrics::MetricsRegistry *metrics_registry,
                   metrics::CommonLabels common_labels)
    : task_name_(task_name), running_state_(true)
{
    meter_ = std::make_unique<metrics::Meter>(metrics_registry, common_labels);
    meter_->Register(dummy_executor_hist_metric, metrics::Type::Histogram);
    this->inner_thread_ =
        std::make_unique<boost::thread>(boost::thread([this] { _ExecTask(); }));
}

void Executor::_ExecTask()
{
    auto total_running_time = std::getenv(TASK_TOTAL_RUNNING_TIME.c_str());
    auto total_running_time_num =
        total_running_time == nullptr ? 60 : std::stoi(total_running_time);

    auto start = boost::posix_time::second_clock::local_time();
    boost::random::mt19937 rng;
    boost::random::uniform_int_distribution<> max_range(10, 100);

    while (true)
    {
        metrics::Value val = metrics::Value(max_range(rng));
        meter_->Collect(dummy_executor_hist_metric, val);
        auto tick = boost::posix_time::second_clock::local_time();
        boost::posix_time::time_duration diff = tick - start;
        if (diff.total_seconds() > 0 && diff.total_seconds() % 5 == 0)
        {
#ifdef WITH_GLOG
            LOG(INFO) << "Task " << this->task_name_ << " On CPU_"
                      << sched_getcpu()
                      << " elapsed time = " << diff.total_seconds();
#endif
        }
        if (diff.total_seconds() >= total_running_time_num)
        {
#ifdef WITH_GLOG
            LOG(INFO) << "Task " << this->task_name_
                      << " Complete elapsed time >= " << total_running_time_num;
#endif
            break;
        }
    }
}

void Executor::Join()
{
    this->inner_thread_->join();
}
Executor::~Executor()
{
}

}  // namespace eloq_metrics_app
