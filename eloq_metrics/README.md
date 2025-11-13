# eloq-metrics

Eloq metrics library

## How to build

`eloq-metrics` encapsulates `prometheus-cpp` and the project includes three library.

- `eloq-metrics-shared` —— Dependency on [prometheus-cpp](https://github.com/jupp0r/prometheus-cpp)
  and [glog](https://github.com/google/glog) (optional)

- `eloq-metrics-app` —— An example of multi-thread program on how to use eloq metrics lib, depending on boost thread and
  boost random.

- `eloq-metrics-bench` —— Dependency on google [benchmark](https://github.com/google/benchmark)

``` sh
# build eloq-metrics
git clone git@github.com:eloqdata/eloq-metrics.git
cd eloq-metrics
cmake -E make_directory build && \

cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="${CXXFLAGS} -fPIC" \

cd build && cmake --build .  -j4
```

#### With VCPKG (Optional)

It is recommended to use [vcpkg](https://github.com/microsoft/vcpkg) to install the above dependencies.

**NOTE**: Due to the limitation of CMake support for the latest version of boost, please install the 2022.05.10 tag of vcpkg

``` sh
# install vcpkg (optional)
git clone https://github.com/microsoft/vcpkg.git
git checkout tags/2022.05.10 -b 2022.05.10-tags
./bootstrap-vcpkg.sh
echo "export VCPKG_ROOT=/home/xxx/vcpkg" >> ~/.bashrc
echo "export PATH=$VCPKG_ROOT:$PATH" >> ~/.bashrc
source ~/.bashrc
```


``` sh
# Use vcpkg to install all dependencies
vcpkg install prometheus-cpp
vcpkg install boost-random
vcpkg install boost-thread
vcpkg install benchmark
```

If you use vcpkg then you need to add the following parameter to the build: `-DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake`

``` sh
# build eloq-metrics
git clone git@github.com:eloqdata/eloq-metrics.git
cd eloq-metrics
cmake -E make_directory build && \
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="${CXXFLAGS} -fPIC" \
  -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake

cd build && cmake --build .  -j4
```

If you want to build `eloq-metrics-app`, set the `-DENABLE_ELOQ_METRICS_APP=ON`.
If you want to build `eloq-metrics-test`, set the `-DENABLE_TESTING=ON`.
``` sh
cd eloq-metrics
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="${CXXFLAGS} -fPIC" \
  -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
  -DENABLE_ELOQ_METRICS_APP=ON \
  -DENABLE_TESTING=ON

cd build && cmake --build .  -j4
```

#### Integrate `abseil-cpp` (Optional)

When the cost of collecting metrics itself is comparable to that of the process being measured, collecting metrics will have considerable costs. To reduce the impact of this, we provide `absl::flat_hash_map` instead of `std::unordered_map`.

- `-DABSEIL_CPP_PATH=path/to/abseil-cpp` (Default to `eloq-metrics/abseil-cpp`)
- `-DWITH_ABSEIL=ON`

**NOTE**: If `WITH_ABSEIL=ON` but `abseil-cpp` is not found at `ABSEIL_CPP_PATH`, we assume `abseil-cpp` is installed as external library.

``` sh
cd eloq-metrics
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="${CXXFLAGS} -fPIC" \
  -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
  -DENABLE_BENCHMARK=ON \
  -DWITH_ABSEIL=ON

cd build && cmake --build .  -j4
```

## Usage
The usage of `eloq-metrics` follows 3 steps:

1. Initializing the MetricsRegistry.

2. Initializing the Meters.

3. Registering Metrics and Gathering the values.

For details on how to use it, see also `dummy_executor.cpp` and `meter.h`.

``` c++
#include "metrics_collector.h"
#include "metrics_manager.h"

int main()
{
  // 1. Initializing the MetricsRegistry.
  MetricsRegistryImpl::MetricsRegistryResult metrics_registry_result =
      MetricsRegistryImpl::GetRegistry();
  std::unique_ptr<metrics::MetricsRegistry> metrics_registry =
      std::move(metrics_registry_result.metrics_registry_);

  // 2. Initializing the Meters.
  metrics::CommonLabels common_labels{
      {"core_id", "0"}, {"instance", "localhost:8000"}};
  auto meter =
      std::make_unique<metrics::Meter>(metrics_registry.get(), common_labels);

  // 3. Registering Metrics and Gathering the values
  metrics::Name name{"histogram"};
  meter->Register(name, metrics::Type::Histogram);
  meter->Collect(name, 1)
}
```

## Benchmark

``` sh
# build release for benchmark
cd eloq-metrics
 -DCMAKE_BUILD_TYPE=Release \
 -DCMAKE_CXX_FLAGS="${CXXFLAGS} -fPIC" \
 -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
 -DENABLE_BENCHMARK=ON
cd build && cmake --build .  -j4
./bin/eloq-metrics-bench  --benchmark_color=true
```

``` text
2024-03-18T09:00:40+00:00
Running ./build/bin/eloq-metrics-bench
Run on (8 X 3000 MHz CPU s)
CPU Caches:
 L1 Data 32 KiB (x4)
 L1 Instruction 32 KiB (x4)
 L2 Unified 1024 KiB (x4)
 L3 Unified 36608 KiB (x1)
Load Average: 0.00, 0.00, 0.00
----------------------------------------------------------------------------------------------------------------
Benchmark                                                                      Time             CPU   Iterations
----------------------------------------------------------------------------------------------------------------
BM_Gauge_Prometheus_Collect/iterations:10000000/real_time                   22.6 ns         22.6 ns     10000000
BM_Gauge_Get_Prometheus_And_Collect/iterations:10000000/real_time           28.8 ns         28.8 ns     10000000
BM_Gauge_MonoWrapper_Collect/iterations:10000000/real_time                  33.2 ns         33.2 ns     10000000
BM_Counter_Prometheus_Collect/iterations:10000000/real_time                 34.6 ns         34.6 ns     10000000
BM_Counter_Get_Prometheus_And_Collect/iterations:10000000/real_time         40.2 ns         40.2 ns     10000000
BM_Counter_MonoWrapper_Collect/iterations:10000000/real_time                48.0 ns         48.0 ns     10000000
BM_Histogram_Prometheus_Collect/iterations:10000000/real_time                221 ns          221 ns     10000000
BM_Histogram_Get_Prometheus_And_Collect/iterations:10000000/real_time        225 ns          225 ns     10000000
BM_Histogram_MonoWrapper_Collect/iterations:10000000/real_time               233 ns          233 ns     10000000
```
