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
// clang-format off
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include <catch2/catch_all.hpp>
// clang-format on

#include "meter.h"
#include "prometheus_collector.h"

namespace
{
class RecordingRegistry : public metrics::MetricsRegistry
{
public:
    metrics::MetricsErrors Open() override
    {
        return metrics::MetricsErrors::Success;
    }

    metrics::MetricHandle Register(
        const metrics::Name &,
        metrics::Type type,
        const metrics::Labels &,
        const metrics::HistogramBuckets &histogram_buckets) override
    {
        histogram_buckets_.push_back(histogram_buckets);
        return metrics::MetricHandle(histogram_buckets_.size(), type);
    }

    void Collect(const metrics::MetricHandle &, const metrics::Value &) override
    {
    }

    std::vector<metrics::HistogramBuckets> histogram_buckets_;
};
}  // namespace

SCENARIO("Metrics Collector no open", "[MCNoOpen]")
{
    INFO("Run unit test MCNoOpen");
    GIVEN("Init PrometheusCollector")
    {
        metrics::PrometheusCollector collector{"0.0.0.0", 18080};
        WHEN("The open method is not called")
        {
            metrics::Metric metric{
                "tx_counter", metrics::Type::Counter, {{"core_id", "core-1"}}};
            auto metrics_opt_unique = std::make_unique<metrics::Metric>(metric);
            metrics::MetricHash hash_func;
            auto hash_value = hash_func(metric);
            // Create handle without collector data (since Open() wasn't called)
            metrics::MetricHandle handle(hash_value, metrics::Type::Counter);
            THEN("call collector will return false")
            {
                auto coll_res = collector.Collect(handle, metrics::Value(100));
                INFO("call collect result " << coll_res);
                REQUIRE(coll_res == false);
            }
        }
    }
}

SCENARIO("Metrics Option Hash Unique", "[MetricsOptionHash]")
{
    INFO("Run unit test MetricsOptionHash");
    GIVEN("Init MetricsOptionHash")
    {
        metrics::MetricHash hash_func;
        std::unordered_set<std::size_t> hash_value_set;
        std::size_t counter{10000};
        WHEN("Generate metric option 10000 elements")
        {
            for (int i = 0; i < counter; i++)
            {
                auto name = "tx_counter_" + std::to_string(i);
                metrics::Labels labels{
                    {"core_id", std::to_string(i)},
                    {"thread_id", "t_id" + std::to_string(i)}};

                metrics::Metric metric{name, metrics::Type::Counter, labels};

                auto hash_value = hash_func(metric);
                if (hash_value_set.find(hash_value) == hash_value_set.end())
                {
                    hash_value_set.insert(hash_value);
                }
            }
            THEN("metrics option hash_value not duplicated.")
            {
                REQUIRE(hash_value_set.size() == counter);
            }
        }
    }
}

SCENARIO("MetricsCollector collect", "[MCCollectSuccess]")
{
    INFO("Run unit test MCCollectSuccess");
    GIVEN("Init PrometheusCollector and metrics option")
    {
        metrics::PrometheusCollector collector{"0.0.0.0", 18081};
        metrics::MetricHash hash_func;

        metrics::Metric metric{
            "tx_counter", metrics::Type::Counter, {{"core_id", "1"}}};

        auto metrics_opt_unique = std::make_unique<metrics::Metric>(metric);
        auto hash_value = hash_func(metric);
        auto open_success = collector.Open();
        REQUIRE(open_success == true);
        WHEN("set metrics and collect")
        {
            auto handle = collector.SetMetric(metrics_opt_unique);
            THEN("collect will return success")
            {
                auto res_val = collector.Collect(handle, metrics::Value{200});
                REQUIRE(res_val == true);
            }
        }
    }
}

SCENARIO("Metrics collector call Open several times",
         "[MCCallOpenSeveralTimes]")
{
    INFO("Run unit test MCCallOpenSeveralTimes");
    WHEN("init prometheus collector")
    {
        metrics::PrometheusCollector collector{"0.0.0.0", 18082};
        WHEN("The open method is called")
        {
            auto open_one = collector.Open();
            THEN("open one will return success")
            {
                REQUIRE(open_one == true);
            }
        }

        WHEN("The open method is call twice")
        {
            collector.Open();
            auto open_two = collector.Open();
            THEN("open two will return false")
            {
                REQUIRE(open_two == false);
            }
        }
    }
}

SCENARIO("Histograms can override default buckets", "[HistogramBuckets]")
{
    RecordingRegistry registry;
    metrics::Meter meter(&registry, {});
    meter.Register(metrics::Name{"custom_bucket_histogram"},
                   metrics::Type::Histogram,
                   {},
                   {1.0, 5.0, 3600.0});
    meter.Register(metrics::Name{"default_bucket_histogram"},
                   metrics::Type::Histogram);
    REQUIRE(registry.histogram_buckets_.size() == 2);
    REQUIRE(registry.histogram_buckets_[0] ==
            (metrics::HistogramBuckets{1.0, 5.0, 3600.0}));
    REQUIRE(registry.histogram_buckets_[1].empty());

    metrics::PrometheusCollector collector{"0.0.0.0", 18083};
    REQUIRE(collector.Open());

    metrics::Metric custom_metric{"custom_bucket_histogram",
                                  metrics::Type::Histogram,
                                  {},
                                  {1.0, 5.0, 3600.0}};
    auto custom_metric_ptr = std::make_unique<metrics::Metric>(custom_metric);
    auto custom_handle = collector.SetMetric(custom_metric_ptr);
    REQUIRE(collector.Collect(custom_handle, metrics::Value{6.0}));

    auto custom_sample = collector.CollectClientMetrics(custom_handle);
    // prometheus-cpp appends its implicit +Inf bucket.
    REQUIRE(custom_sample.histogram.bucket.size() == 4);
    REQUIRE(custom_sample.histogram.bucket[0].upper_bound == 1.0);
    REQUIRE(custom_sample.histogram.bucket[1].upper_bound == 5.0);
    REQUIRE(custom_sample.histogram.bucket[2].upper_bound == 3600.0);

    metrics::Metric default_metric{
        "default_bucket_histogram", metrics::Type::Histogram, {}};
    auto default_metric_ptr = std::make_unique<metrics::Metric>(default_metric);
    auto default_handle = collector.SetMetric(default_metric_ptr);
    auto default_sample = collector.CollectClientMetrics(default_handle);
    REQUIRE(default_sample.histogram.bucket.size() ==
            metrics::PROMETHEUS_HISTOGRAM_DEF_BUCKETS.size() + 1);
}

SCENARIO("Invalid histogram buckets are rejected before registration",
         "[HistogramBuckets]")
{
    metrics::PrometheusCollector collector{"0.0.0.0", 18084};
    REQUIRE(collector.Open());

    metrics::Metric descending_metric{"recoverable_bucket_histogram",
                                      metrics::Type::Histogram,
                                      {},
                                      {5.0, 1.0}};
    auto descending_metric_ptr =
        std::make_unique<metrics::Metric>(descending_metric);
    REQUIRE_THROWS_AS(collector.SetMetric(descending_metric_ptr),
                      std::invalid_argument);

    // Reusing the name proves validation happened before family registration.
    metrics::Metric recovered_metric{"recoverable_bucket_histogram",
                                     metrics::Type::Histogram,
                                     {},
                                     {1.0, 5.0}};
    auto recovered_metric_ptr =
        std::make_unique<metrics::Metric>(recovered_metric);
    auto recovered_handle = collector.SetMetric(recovered_metric_ptr);
    REQUIRE(recovered_handle.collector_data != nullptr);

    metrics::Metric duplicate_metric{
        "duplicate_bucket_histogram", metrics::Type::Histogram, {}, {1.0, 1.0}};
    auto duplicate_metric_ptr =
        std::make_unique<metrics::Metric>(duplicate_metric);
    REQUIRE_THROWS_AS(collector.SetMetric(duplicate_metric_ptr),
                      std::invalid_argument);
}
