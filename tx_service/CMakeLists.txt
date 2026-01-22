cmake_minimum_required(VERSION 3.12)
project(eloq)

find_package(Threads)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-parentheses -Wno-error -W -fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -DFAULT_INJECTOR")

if(CMAKE_COMPILER_IS_GNUCC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-reorder -Wno-unused-parameter -Wno-parentheses")
endif()

if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W3")
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

option(INI_USE_HEAP "Whether parse ini on heap" ON)

if (INI_USE_HEAP)
    add_definitions(-DINI_USE_STACK=0)
    add_definitions(-DINI_ALLOW_REALLOC=1)
endif()

set(WITH_LOG_STATE "ROCKSDB" CACHE STRING "The log state implementation")
set_property(CACHE WITH_LOG_STATE PROPERTY STRINGS "MEMORY" "ROCKSDB" "ROCKSDB_CLOUD_S3" "ROCKSDB_CLOUD_GCS")
message(NOTICE "WITH_LOG_STATE: ${WITH_LOG_STATE}")

# Add compile flags for LOG STATE TYPE
if(WITH_LOG_STATE STREQUAL "MEMORY")
  add_compile_definitions(LOG_STATE_TYPE_MEM)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB")
  add_compile_definitions(LOG_STATE_TYPE_RKDB)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_S3")
  add_compile_definitions(LOG_STATE_TYPE_RKDB_S3)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_GCS")
  add_compile_definitions(LOG_STATE_TYPE_RKDB_GCS)
else()
  message(FATAL_ERROR "Unknown WITH_LOG_STATE: ${WITH_LOG_STATE}")
endif()

option(BRPC_WITH_GLOG "With glog" ON)
option(RUN_TX_SERVICE_TESTS "Run unit-tests" ON)
option(FORK_HM_PROCESS "Whether fork host manager process" ON)
message(NOTICE "FORK_HM_PROCESS : ${FORK_HM_PROCESS}")

if(FORK_HM_PROCESS)
    add_definitions(-DFORK_HM_PROCESS)
endif()

option(RUNNING_TXSERVICE_CTEST "running tx_service alone to run test" ON)
message(STATUS "RUNNING_TXSERVICE_CTEST : ${RUNNING_TXSERVICE_CTEST}")

if(RUNNING_TXSERVICE_CTEST)
    add_definitions(-DRUNNING_TXSERVICE_CTEST)
endif()

add_subdirectory(abseil-cpp)

set(ABSEIL
    absl::btree
    absl::flat_hash_map
    absl::span
)

find_package(Protobuf REQUIRED)
find_package(GFLAGS REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(MIMALLOC REQUIRED)

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)

if((NOT GFLAGS_INCLUDE_PATH) OR(NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()

message(STATUS "OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")

message(STATUS "protobuf dir: ${PROTOBUF_LIBRARY}")
message(STATUS "protobuf include dir: ${Protobuf_INCLUDE_DIR}")
message(STATUS "gflags libs dir: ${GFLAGS_LIBRARY}")
message(STATUS "gflags include dir: ${GFLAGS_INCLUDE_DIR}")

if(CMAKE_COMPILER_IS_GNUCC)
    find_path(BRPC_INCLUDE_PATH NAMES brpc/stream.h)
    find_library(BRPC_LIB NAMES brpc)

    if((NOT BRPC_INCLUDE_PATH) OR(NOT BRPC_LIB))
        message(FATAL_ERROR "Fail to find brpc")
    endif()

    find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
    find_library(BRAFT_LIB NAMES braft)

    if((NOT BRAFT_INCLUDE_PATH) OR(NOT BRAFT_LIB))
        message(FATAL_ERROR "Fail to find braft")
    endif()

    if(BRPC_WITH_GLOG)
        find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
        find_library(GLOG_LIB NAMES glog VERSION ">=0.6.0" REQUIRED)

        if((NOT GLOG_INCLUDE_PATH) OR(NOT GLOG_LIB))
            message(FATAL_ERROR "Fail to find glog")
        endif()

        include_directories(${GLOG_INCLUDE_PATH})
        set(LINK_LIB ${LINK_LIB} ${GLOG_LIB})
        message(STATUS "glog lib dir: ${GLOG_LIB}")
    endif()

    find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
    find_library(LEVELDB_LIB NAMES leveldb)

    if((NOT LEVELDB_INCLUDE_PATH) OR(NOT LEVELDB_LIB))
        message(FATAL_ERROR "Fail to find leveldb")
    endif()

    # find_path(OPENSSL_INCLUDE_PATH NAMES openssl/ssl.h)
    # find_library(OPENSSL_LIB NAMES ssl)
    # if ((NOT OPENSSL_INCLUDE_PATH) OR (NOT OPENSSL_LIB))
    # message(FATAL_ERROR "Fail to find openssl")
    # endif()
endif()

# Compile all protobuf files under proto directory.
set(PROTO_SRC ${CMAKE_CURRENT_SOURCE_DIR}/include/proto)
set(PROTO_NAME cc_request)
execute_process(
    COMMAND protoc ./${PROTO_NAME}.proto --cpp_out=./ --proto_path=./
    WORKING_DIRECTORY ${PROTO_SRC}
)

set(LOG_PROTO_SRC ${CMAKE_CURRENT_SOURCE_DIR}/tx-log-protos)
set(LOG_PROTO_NAME log)
execute_process(
    COMMAND protoc ./${LOG_PROTO_NAME}.proto --cpp_out=./ --proto_path=./
    WORKING_DIRECTORY ${LOG_PROTO_SRC}
)

set(TX_SERVICE_SOURCE_DIR ${CMAKE_SOURCE_DIR})
SET(METRICS_SERVICE_SOURCE_DIR ${CMAKE_SOURCE_DIR}/../eloq_metrics)

set(INCLUDE_DIR
    ${TX_SERVICE_SOURCE_DIR}/include
    ${TX_SERVICE_SOURCE_DIR}/abseil-cpp
    ${TX_SERVICE_SOURCE_DIR}/include/cc
    ${TX_SERVICE_SOURCE_DIR}/include/remote
    ${TX_SERVICE_SOURCE_DIR}/include/fault
    ${TX_SERVICE_SOURCE_DIR}/tx-log-protos
    ${METRICS_SERVICE_SOURCE_DIR}/include
    ${Protobuf_INCLUDE_DIR})

if(CMAKE_COMPILER_IS_GNUCC)
    set(INCLUDE_DIR ${INCLUDE_DIR}
        ${MIMALLOC_INCLUDE_PATH}
        ${BRPC_INCLUDE_PATH}
        ${BRAFT_INCLUDE_PATH}
        ${GLOG_INCLUDE_PATH}
        ${GFLAGS_INCLUDE_PATH})
endif()

set(LINK_LIB ${LINK_LIB} ${PROTOBUF_LIBRARY})

if(CMAKE_COMPILER_IS_GNUCC)
    set(LINK_LIB ${LINK_LIB}
        ${MIMALLOC_LIB}
        ${GFLAGS_LIBRARY}
        ${LEVELDB_LIB}
        ${BRAFT_LIB}
        ${BRPC_LIB}
        ${OPENSSL_LIB}
    )
endif()

SET(TxService_SOURCES
    ${TX_SERVICE_SOURCE_DIR}/src/tx_key.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_execution.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_operation.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/checkpointer.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_trace.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_start_ts_collector.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/sharder.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/catalog_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/range_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/range_bucket_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_entry.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_map.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_shard.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_handler_result.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/local_cc_handler.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/local_cc_shards.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/non_blocking_lock.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_req_misc.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/range_slice.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/reader_writer_cntl.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_request.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/eloq_string_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/eloq_basic_catalog_factory.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/remote_cc_handler.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/remote_cc_request.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_node_service.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_stream_receiver.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_stream_sender.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/log_replay_service.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/cc_node.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/fault_inject.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_worker_pool.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/dead_lock_check.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_index_operation.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/sk_generator.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/data_sync_task.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/standby.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/store/snapshot_manager.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/sequences/sequences.cpp
    ${METRICS_SERVICE_SOURCE_DIR}/src/metrics.cc
)

set(INCLUDE_DIR ${INCLUDE_DIR} ${PROTO_SRC} ${LOG_PROTO_SRC} ${OPENSSL_INCLUDE_DIR})
set(TxService_SOURCES ${TxService_SOURCES} ${PROTO_SRC}/${PROTO_NAME}.pb.cc)
set(TxService_SOURCES ${TxService_SOURCES} ${LOG_PROTO_SRC}/log_agent.cpp)
set(TxService_SOURCES ${TxService_SOURCES} ${LOG_PROTO_SRC}/${LOG_PROTO_NAME}.pb.cc)

add_library(txservice STATIC ${TxService_SOURCES})

target_include_directories(txservice PUBLIC ${INCLUDE_DIR})

target_link_libraries(txservice PUBLIC mimalloc ${LINK_LIB} ${PROTOBUF_LIBRARIES} ${ABSEIL})

include(FetchContent)
    # Import yaml-cpp library used by host manager
FetchContent_Declare(
    yaml-cpp
    GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
    GIT_TAG yaml-cpp-0.7.0 # Can be a tag (yaml-cpp-x.x.x), a commit hash, or a branch name (master)
)
FetchContent_MakeAvailable(yaml-cpp)

# add_executable(hello ${CMAKE_SOURCE_DIR}/hello.cpp)
# target_link_libraries(hello PRIVATE txservice ${CMAKE_THREAD_LIBS_INIT})
SET(HOST_MANAGER_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/raft_host_manager)
set(HOST_MANAGER_INCLUDE_DIR
    ${HOST_MANAGER_SOURCE_DIR}/include
    ${TX_SERVICE_SOURCE_DIR}/tx-log-protos
    ${OPENSSL_INCLUDE_DIR}
    ${PROTO_SRC}
    ${LOG_PROTO_SRC})

if(CMAKE_COMPILER_IS_GNUCC)
    set(HOST_MANAGER_INCLUDE_DIR ${HOST_MANAGER_INCLUDE_DIR}
        ${BRPC_INCLUDE_PATH}
        ${BRAFT_INCLUDE_PATH}
        ${GLOG_INCLUDE_PATH}
        ${GFLAGS_INCLUDE_PATH})
endif()

set(HOST_MANAGER_LINK_LIB ${HOST_MANAGER_LINK_LIB} ${PROTOBUF_LIBRARIES})

if(CMAKE_COMPILER_IS_GNUCC)
    find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
    find_library(BRAFT_LIB NAMES braft)

    if((NOT BRAFT_INCLUDE_PATH) OR(NOT BRAFT_LIB))
        message(FATAL_ERROR "Fail to find braft")
    endif()

    set(HOST_MANAGER_LINK_LIB ${HOST_MANAGER_LINK_LIB}
        ${GFLAGS_LIBRARY}
        ${LEVELDB_LIB}
        ${BRAFT_LIB}
        ${BRPC_LIB}
        ${OPENSSL_LIB})
    find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
    find_library(GLOG_LIB NAMES glog VERSION ">=0.6.0" REQUIRED)

    if((NOT GLOG_INCLUDE_PATH) OR(NOT GLOG_LIB))
        message(FATAL_ERROR "Fail to find glog")
    endif()

    include_directories(${GLOG_INCLUDE_PATH})
    set(HOST_MANAGER_LINK_LIB ${HOST_MANAGER_LINK_LIB} ${GLOG_LIB})
endif()

SET(RaftHM_SOURCES
    ${HOST_MANAGER_SOURCE_DIR}/src/main.cpp
    ${HOST_MANAGER_SOURCE_DIR}/src/raft_host_manager_service.cpp
    ${HOST_MANAGER_SOURCE_DIR}/src/raft_host_manager.cpp
    ${HOST_MANAGER_SOURCE_DIR}/src/ini.c
    ${HOST_MANAGER_SOURCE_DIR}/src/INIReader.cpp
    ${PROTO_SRC}/${PROTO_NAME}.pb.cc
    ${LOG_PROTO_SRC}/log_agent.cpp
    ${LOG_PROTO_SRC}/${LOG_PROTO_NAME}.pb.cc
)

include_directories(${HOST_MANAGER_INCLUDE_DIR})
add_executable(host_manager ${RaftHM_SOURCES})
target_link_libraries(host_manager ${HOST_MANAGER_LINK_LIB} yaml-cpp::yaml-cpp)

install(TARGETS host_manager
    RUNTIME DESTINATION bin)

message(NOTICE "with tests: ${RUN_TX_SERVICE_TESTS}")

if(RUN_TX_SERVICE_TESTS)
    # Ensure tests are enabled
    enable_testing()

    find_package(Catch2 CONFIG REQUIRED)

    # CcEntry-Test
    add_executable(CcEntry-Test ${TX_SERVICE_SOURCE_DIR}/tests/CcEntry-Test.cpp)
    target_include_directories(CcEntry-Test PUBLIC ${TX_SERVICE_SOURCE_DIR}/tests/include ${INCLUDE_DIR})
    target_link_libraries(CcEntry-Test PUBLIC txservice Catch2::Catch2)

    # CcPage-Test
    add_executable(CcPage-Test ${TX_SERVICE_SOURCE_DIR}/tests/CcPage-Test.cpp)
    target_include_directories(CcPage-Test PUBLIC ${TX_SERVICE_SOURCE_DIR}/tests/include ${INCLUDE_DIR})
    target_link_libraries(CcPage-Test PUBLIC txservice Catch2::Catch2)

    # StartTsCollector-Test
    add_executable(StartTsCollector-Test ${TX_SERVICE_SOURCE_DIR}/tests/StartTsCollector-Test.cpp)
    target_include_directories(StartTsCollector-Test PUBLIC ${TX_SERVICE_SOURCE_DIR}/tests/include ${INCLUDE_DIR})
    target_link_libraries(StartTsCollector-Test PUBLIC txservice Catch2::Catch2)

    # Register tests with CTest
    add_test(NAME CcEntry-Test COMMAND CcEntry-Test)
    add_test(NAME CcPage-Test COMMAND CcPage-Test)
    add_test(NAME StartTsCollector-Test COMMAND StartTsCollector-Test)
endif()
