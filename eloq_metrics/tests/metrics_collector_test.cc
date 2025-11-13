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
#include <catch2/catch_all.hpp>
#include <unordered_set>

#include "prometheus_collector.h"

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
            metrics::MetricHash hash_func;
            auto hash_value = hash_func(metric);
            THEN("call collector will return false")
            {
                auto coll_res =
                    collector.Collect(hash_value, 100, metrics::Type::Counter);
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
            collector.SetMetric(metrics_opt_unique);
            THEN("collect will return success")
            {
                auto res_val = collector.Collect(
                    hash_value, metrics::Value{200}, metrics::Type::Counter);
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
