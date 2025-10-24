#include <benchmark/benchmark.h>
#include <prometheus/gauge.h>

#include "metrics.h"
#include "metrics_manager.h"
#include "prometheus/registry.h"

/* Gauge Metrics */
static auto benchmark_gauge_metric = metrics::Metric(
    "benchmark_gauge_metric",
    metrics::Type::Gauge,
    {{"core_id", "0"}, {"instance", "localhost:8000"}, {"type", "benchmark"}});

static std::unique_ptr<metrics::Metric> benchmark_gauge_metric_ptr =
    std::make_unique<metrics::Metric>(benchmark_gauge_metric);

static const std::shared_ptr<metrics::CollectorWrapper>
    benchmark_gauge_metric_collector =
        (*(metrics::MetricsMgr::GetMetricMgrInstance().mgr_))
            .MetricsRegistry(std::move(benchmark_gauge_metric_ptr));

static void BM_Gauge_Prometheus_Collect(benchmark::State &state)
{
    prometheus::Registry registry;
    auto &gauge_family = prometheus::BuildGauge()
                             .Name("benchmark_gauge_metric")
                             .Register(registry);
    auto &gauge = gauge_family.Add({{"core_id", "0"},
                                    {"instance", "localhost:8000"},
                                    {"type", "benchmark"}});
    for (auto _ : state)
    {
        gauge.Set(1);
    }
}

static void BM_Gauge_Get_Prometheus_And_Collect(benchmark::State &state)
{
    metrics::MetricHash metric_opt_hash;
    auto hash_key = metric_opt_hash(benchmark_gauge_metric);

    prometheus::Registry registry;
    auto &gauge_family = prometheus::BuildGauge()
                             .Name("benchmark_gauge_metric")
                             .Register(registry);
    auto &gauge = gauge_family.Add({{"core_id", "0"},
                                    {"instance", "localhost:8000"},
                                    {"type", "benchmark"}});

    metrics::Map<std::size_t, std::reference_wrapper<prometheus::Gauge>>
        metrics_map = {{hash_key, gauge}};

    for (auto _ : state)
    {
        auto &his_ref = metrics_map.find(hash_key)->second.get();
        his_ref.Set(1);
    }
}

static void BM_Gauge_MonoWrapper_Collect(benchmark::State &state)
{
    for (auto _ : state)
    {
        benchmark_gauge_metric_collector->Collect(1);
    }
}

BENCHMARK(BM_Gauge_Prometheus_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_Gauge_Get_Prometheus_And_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_Gauge_MonoWrapper_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();
