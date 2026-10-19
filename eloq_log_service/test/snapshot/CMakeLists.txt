# Shared by the parent WITH_TESTS build and the standalone TEST_LOG_SNAPSHOT
# build so CI runs the same suite as a focused local configure.
find_package(Catch2 CONFIG REQUIRED)
if(WITH_LOG_STATE STREQUAL "ROCKSDB")
    add_executable(log_snapshot_tests ../log_snapshot_test.cpp)
    set_property(TARGET log_snapshot_tests APPEND_STRING PROPERTY LINK_FLAGS
        " -Wl,--wrap=pthread_create -Wl,--wrap=fsync")
else()
    add_executable(log_snapshot_tests ../log_snapshot_stream_test.cpp)
endif()
target_link_libraries(log_snapshot_tests PRIVATE logservice Catch2::Catch2WithMain)
add_test(NAME log_snapshot_tests COMMAND log_snapshot_tests)
set_tests_properties(log_snapshot_tests PROPERTIES TIMEOUT 60)
