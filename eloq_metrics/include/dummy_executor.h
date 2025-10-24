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
#include <boost/thread.hpp>
#include <cstddef>
#include <string>

#ifdef WITH_GLOG
#include "glog/logging.h"
#endif

#include "meter.h"
#include "metrics.h"

namespace eloq_metrics_app
{
class Executor
{
public:
    Executor() = delete;

    Executor(const Executor &) = delete;

    Executor(const Executor &&) = delete;

    Executor &operator=(const Executor &) = delete;

    Executor &operator=(Executor &&executor_) noexcept;

    Executor(Executor &&executor) noexcept;

    explicit Executor(std::string task_name,
                      std::size_t cpu_id,
                      cpu_set_t cpu_set,
                      metrics::MetricsRegistry *metrics_registry,
                      metrics::CommonLabels common_labels = {});
    explicit Executor(std::string task_name,
                      metrics::MetricsRegistry *metrics_registry,
                      metrics::CommonLabels common_labels = {});
    virtual ~Executor();
    void Join();

private:
    std::string task_name_;
    boost::atomic<bool> running_state_;
    std::unique_ptr<boost::thread> inner_thread_;
    std::unique_ptr<metrics::Meter> meter_;

private:
    void _ExecTask();
};

}  // namespace eloq_metrics_app
