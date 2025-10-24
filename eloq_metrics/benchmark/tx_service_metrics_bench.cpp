#include <benchmark/benchmark.h>

#include <memory>

#include "meter.h"
#include "metrics.h"
#include "metrics_registry_impl.h"

using namespace eloq_metrics_app;

static MetricsRegistryImpl::MetricsRegistryResult metrics_registry_result =
    MetricsRegistryImpl::GetRegistry();
static std::unique_ptr<metrics::MetricsRegistry> metrics_registry =
    std::move(metrics_registry_result.metrics_registry_);

static metrics::CommonLabels common_labels{
    {"core_id", "0"}, {"instance", "localhost:8000"}, {"type", "benchmark"}};

static void BM_get_time_point(benchmark::State &state)
{
    while (state.KeepRunning())
    {
        auto time_start = metrics::Clock::now();
    }
}

static void BM_collect_gauge(benchmark::State &state)
{
    auto meter =
        std::make_unique<metrics::Meter>(metrics_registry.get(), common_labels);
    metrics::Name name{"guage"};
    meter->Register(name, metrics::Type::Gauge);
    while (state.KeepRunning())
    {
        meter->Collect(name, 1);
    }
}

static void BM_collect_counter(benchmark::State &state)
{
    auto meter =
        std::make_unique<metrics::Meter>(metrics_registry.get(), common_labels);
    metrics::Name name{"counter"};
    meter->Register(name, metrics::Type::Counter);
    while (state.KeepRunning())
    {
        meter->Collect(name, 1);
    }
}

static void BM_collect_histogram(benchmark::State &state)
{
    auto meter =
        std::make_unique<metrics::Meter>(metrics_registry.get(), common_labels);
    metrics::Name name{"histogram"};
    meter->Register(name, metrics::Type::Histogram);
    while (state.KeepRunning())
    {
        meter->Collect(name, 1);
    }
}

static void BM_collect_histogram_with_one_label(benchmark::State &state)
{
    auto meter =
        std::make_unique<metrics::Meter>(metrics_registry.get(), common_labels);
    metrics::Name name{"histogram"};
    meter->Register(
        name,
        metrics::Type::Histogram,
        {{"request_type",
          {"read", "write", "acquire_write", "scan_next", "post_process"}}});
    while (state.KeepRunning())
    {
        meter->Collect(name, 1, "read");
    }
}

BENCHMARK(BM_get_time_point)
    ->Iterations(1000000)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_collect_gauge)
    ->Iterations(1000000)
    ->Repetitions(100)
    ->UseRealTime()
    ->ReportAggregatesOnly();
BENCHMARK(BM_collect_counter)
    ->Iterations(1000000)
    ->Repetitions(100)
    ->UseRealTime()
    ->ReportAggregatesOnly();
BENCHMARK(BM_collect_histogram)
    ->Iterations(1000000)
    ->Repetitions(100)
    ->UseRealTime()
    ->ReportAggregatesOnly();

BENCHMARK(BM_collect_histogram_with_one_label)
    ->Iterations(1000000)
    ->Repetitions(100)
    ->UseRealTime()
    ->ReportAggregatesOnly();
