#include <benchmark/benchmark.h>
#include <prometheus/counter.h>

#include "metrics.h"
#include "metrics_manager.h"
#include "prometheus/registry.h"

/* Counter Metrics */
static auto benchmark_counter_metric = metrics::Metric(
    "benchmark_counter_metric",
    metrics::Type::Counter,
    {{"core_id", "0"}, {"instance", "localhost:8000"}, {"type", "benchmark"}});

static std::unique_ptr<metrics::Metric> benchmark_counter_metric_ptr =
    std::make_unique<metrics::Metric>(benchmark_counter_metric);

static metrics::MetricHandle benchmark_counter_metric_handle =
    (*(metrics::MetricsMgr::GetMetricMgrInstance().mgr_))
        .MetricsRegistry(std::move(benchmark_counter_metric_ptr));

static std::shared_ptr<metrics::MetricsCollector> benchmark_counter_collector =
    (*(metrics::MetricsMgr::GetMetricMgrInstance().mgr_)).GetCollector();

static void BM_Counter_Prometheus_Collect(benchmark::State &state)
{
    prometheus::Registry registry;
    auto &counter_family = prometheus::BuildCounter()
                               .Name("benchmark_counter_metric")
                               .Register(registry);
    auto &counter = counter_family.Add({{"core_id", "0"},
                                        {"instance", "localhost:8000"},
                                        {"type", "benchmark"}});
    for (auto _ : state)
    {
        counter.Increment(1);
    }
}

static void BM_Counter_Get_Prometheus_And_Collect(benchmark::State &state)
{
    metrics::MetricHash metric_opt_hash;
    auto hash_key = metric_opt_hash(benchmark_counter_metric);

    prometheus::Registry registry;
    auto &counter_family = prometheus::BuildCounter()
                               .Name("benchmark_counter_metric")
                               .Register(registry);
    auto &counter = counter_family.Add({{"core_id", "0"},
                                        {"instance", "localhost:8000"},
                                        {"type", "benchmark"}});

    metrics::Map<std::size_t, std::reference_wrapper<prometheus::Counter>>
        metrics_map = {{hash_key, counter}};

    for (auto _ : state)
    {
        auto &his_ref = metrics_map.find(hash_key)->second.get();
        his_ref.Increment(1);
    }
}

static void BM_Counter_MonoWrapper_Collect(benchmark::State &state)
{
    for (auto _ : state)
    {
        benchmark_counter_collector->Collect(
            benchmark_counter_metric_handle.key,
            metrics::Value(1),
            benchmark_counter_metric_handle.type);
    }
}

BENCHMARK(BM_Counter_Prometheus_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_Counter_Get_Prometheus_And_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_Counter_MonoWrapper_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();
