find_package(benchmark CONFIG REQUIRED)

set(BENCH_DIR ${PROJECT_SOURCE_DIR}/benchmark)

add_executable(eloq-metrics-bench
        ${BENCH_DIR}/eloq_metrics_gauge_bench.cpp
        ${BENCH_DIR}/eloq_metrics_counter_bench.cpp
        ${BENCH_DIR}/eloq_metrics_histogram_bench.cpp
)

add_executable(tx-service-metrics-bench
        ${BENCH_DIR}/tx_service_metrics_bench.cpp
)

add_executable(redis-metrics-bench
        ${BENCH_DIR}/redis_metrics_bench.cpp
)

set(BENCH_LINK_LIBS
        ${LIB_NAME}-shared
        benchmark::benchmark
        benchmark::benchmark_main
)

target_link_libraries(eloq-metrics-bench PRIVATE ${BENCH_LINK_LIBS})
target_link_libraries(tx-service-metrics-bench PRIVATE ${BENCH_LINK_LIBS})
target_link_libraries(redis-metrics-bench PRIVATE ${BENCH_LINK_LIBS})

add_test(
        NAME eloq-metrics-bench
        COMMAND mono-bench
)

set_property(
        TEST eloq-metrics-bench
        APPEND PROPERTY LABELS Benchmark
)
