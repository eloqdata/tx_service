#include <benchmark/benchmark.h>
#include <prometheus/histogram.h>

#include "metrics.h"
#include "metrics_manager.h"
#include "prometheus/registry.h"

/* Histogram Metrics */
static auto benchmark_histogram_metric = metrics::Metric(
    "benchmark_histogram_metric",
    metrics::Type::Histogram,
    {{"core_id", "0"}, {"instance", "localhost:8000"}, {"type", "benchmark"}});

static std::unique_ptr<metrics::Metric> benchmark_histogram_metric_ptr =
    std::make_unique<metrics::Metric>(benchmark_histogram_metric);

static const std::shared_ptr<metrics::CollectorWrapper>
    benchmark_histogram_metric_collector =
        (*(metrics::MetricsMgr::GetMetricMgrInstance().mgr_))
            .MetricsRegistry(std::move(benchmark_histogram_metric_ptr));

static void BM_Histogram_Prometheus_Collect(benchmark::State &state)
{
    prometheus::Registry registry;
    auto &histogram_family = prometheus::BuildHistogram()
                                 .Name("benchmark_histogram_metric")
                                 .Register(registry);
    auto &histogram =
        histogram_family.Add({{"core_id", "0"},
                              {"instance", "localhost:8000"},
                              {"type", "benchmark"}},
                             metrics::PROMETHEUS_HISTOGRAM_DEF_BUCKETS);
    for (auto _ : state)
    {
        histogram.Observe(1);
    }
}

static void BM_Histogram_Get_Prometheus_And_Collect(benchmark::State &state)
{
    metrics::MetricHash metric_opt_hash;
    auto hash_key = metric_opt_hash(benchmark_histogram_metric);

    prometheus::Registry registry;
    auto &histogram_family = prometheus::BuildHistogram()
                                 .Name("benchmark_histogram_metric")
                                 .Register(registry);
    auto &histogram =
        histogram_family.Add({{"core_id", "0"},
                              {"instance", "localhost:8000"},
                              {"type", "benchmark"}},
                             metrics::PROMETHEUS_HISTOGRAM_DEF_BUCKETS);

    metrics::Map<std::size_t, std::reference_wrapper<prometheus::Histogram>>
        metrics_map = {{hash_key, histogram}};

    for (auto _ : state)
    {
        auto &his_ref = metrics_map.find(hash_key)->second.get();
        his_ref.Observe(1);
    }
}

static void BM_Histogram_MonoWrapper_Collect(benchmark::State &state)
{
    for (auto _ : state)
    {
        benchmark_histogram_metric_collector->Collect(1);
    }
}

BENCHMARK(BM_Histogram_Prometheus_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_Histogram_Get_Prometheus_And_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_Histogram_MonoWrapper_Collect)
    ->Iterations(10000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();
