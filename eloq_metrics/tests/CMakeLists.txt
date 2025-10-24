find_package(Catch2 CONFIG REQUIRED)
find_package(CURL REQUIRED)

set(LIB_NAME "eloq-metrics")

if(CURL_FOUND)
    message(STATUS "Found CURL ${CURL_VERSION} pkg_name ${CURL_FIND_NAME}")
    set(METRICS_HTTP_SERVICE_TEST_SOURCE ${METRICS_ROOT_DIR}/tests/metrics_http_service_test.cc)
    set(CURL_LINK_LIB CURL::libcurl)
    message(STATUS "Metrics Collector http service test ${METRICS_HTTP_SERVICE_TEST_SOURCE}")
endif()

add_executable(eloq-metrics-test
        ${METRICS_ROOT_DIR}/tests/metrics_collector_test.cc
        ${METRICS_ROOT_DIR}/tests/metrics_manager_test.cc
        ${METRICS_HTTP_SERVICE_TEST_SOURCE}
        )

target_link_libraries(eloq-metrics-test
        PRIVATE
        ${LIB_NAME}
        Catch2::Catch2 Catch2::Catch2WithMain
        ${CURL_LINK_LIB}
        )

add_test(NAME eloq-metrics-test COMMAND eloq-metrics-test)
