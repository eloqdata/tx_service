# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1335 USA
cmake_minimum_required(VERSION 3.8)
project(eloqkv C CXX)

# Install dependencies to compile redis_cli:
# With apt:
# sudo apt-get install libreadline-dev
# sudo apt-get install ncurses-dev
# With yum:
# sudo yum install readline-devel
# sudo yum install ncurses-devel

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)

# set(METRICS_LIB "eloq-metrics" CACHE STRING "metrics library name.")

option(INI_USE_HEAP "Whether parse ini on heap" ON)

if (INI_USE_HEAP)
    add_definitions(-DINI_USE_STACK=0)
    add_definitions(-DINI_ALLOW_REALLOC=1)
endif()

IF ( (CMAKE_BUILD_TYPE STREQUAL "") OR (CMAKE_BUILD_TYPE MATCHES "Debug"))
  message(NOTICE "WITH_FAULT_INJECT: ON")
  add_definitions(-DWITH_FAULT_INJECT)
ELSE()
  message(NOTICE "WITH_FAULT_INJECT: OFF")
  remove_definitions(-DWITH_FAULT_INJECT)
ENDIF()

option(LINK_SO "Whether examples are linked dynamically" ON)

option(BRPC_WITH_GLOG "With glog" ON)

option(ABSL_PROPAGATE_CXX_STD "ABSL PROPAGATE CXX STD" ON)

option(WITH_ASAN "Enable memory sanitize" OFF)
message(NOTICE "WITH ASAN: ${WITH_ASAN}")

# KV data store backend for redis server, Cassandra, DynamoDB, RocksDB Cloud
set(WITH_DATA_STORE "ELOQDSS_ROCKSDB_CLOUD_S3" CACHE STRING "The KV data store to compile with")
set_property(CACHE WITH_DATA_STORE PROPERTY STRINGS "ELOQDSS_ROCKSDB_CLOUD_S3" "ELOQDSS_ROCKSDB_CLOUD_GCS" "ELOQDSS_ROCKSDB" "ELOQDSS_ELOQSTORE")
message(NOTICE "With DATA_STORE: ${WITH_DATA_STORE}")


# Add compile flags for KV stores
if (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
   add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
elseif (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS")
   add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
  set(KV_STORAGE_VAL 3 CACHE STRING "eloq_ds" FORCE)
  add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
elseif (WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
   add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
endif ()


option(DISABLE_CODE_LINE_IN_LOG "Enable DISABLE_CODE_LINE_IN_LOG" OFF)
message(STATUS "DISABLE_CODE_LINE_IN_LOG : ${DISABLE_CODE_LINE_IN_LOG}")
if (DISABLE_CODE_LINE_IN_LOG)
    add_definitions(-DDISABLE_CODE_LINE_IN_LOG)
endif()

option(ENABLE_CPU_PROFILING "Enable CPU profiling" OFF)
message("ENABLE_CPU_PROFILING: ${ENABLE_CPU_PROFILING}")

if (ENABLE_CPU_PROFILING)
    find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
    find_library(GPERFTOOLS_LIBRARIES NAMES profiler)
    include_directories(${GPERFTOOLS_INCLUDE_DIR})
    add_compile_definitions(BRPC_ENABLE_CPU_PROFILER)
endif()

add_definitions("-Wall -g")

include(FindThreads)
include(FindProtobuf)

# Search for libthrift* by best effort. If it is not found and brpc is
# compiled with thrift protocol enabled, a link error would be reported.
find_library(THRIFT_LIB NAMES thrift)

if (NOT THRIFT_LIB)
    set(THRIFT_LIB "")
endif ()

find_library(THRIFTNB_LIB NAMES thriftnb)

if (NOT THRIFTNB_LIB)
    set(THRIFTNB_LIB "")
endif ()

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)

if (LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
else ()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
endif ()

if ((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif ()

include_directories(${BRPC_INCLUDE_PATH})

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)

if ((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif ()

include_directories(${GFLAGS_INCLUDE_PATH})

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    include(CheckFunctionExists)
    CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)

    if (NOT HAVE_CLOCK_GETTIME)
        set(DEFINE_CLOCK_GETTIME "-DNO_CLOCK_GETTIME_IN_MAC")
    endif ()
endif ()

set(CMAKE_CXX_FLAGS "${DEFINE_CLOCK_GETTIME} -DGFLAGS_NS=${GFLAGS_NS} -fno-omit-frame-pointer -g")
#set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -DNDEBUG -O2 -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")

if (WITH_ASAN)
    # Add compile flags
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    # Add link flags for executables and shared libraries
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    add_compile_definitions(BOOST_USE_ASAN)
    add_compile_definitions(BOOST_USE_UCONTEXT)
endif()

# find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
# find_library(LEVELDB_LIB NAMES leveldb)

# if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
#     message(FATAL_ERROR "Fail to find leveldb")
# endif ()

# include_directories(${LEVELDB_INCLUDE_PATH})

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OPENSSL_ROOT_DIR
            "/usr/local/opt/openssl" # Homebrew installed OpenSSL
            )
endif ()

find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

if (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
  find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
  if (NOT ROCKSDB_INCLUDE_PATH)
    message(FATAL_ERROR "Fail to find RocksDB include path")
  endif()
  message(STATUS "ROCKSDB_INCLUDE_PATH: ${ROCKSDB_INCLUDE_PATH}")
  include_directories(${ROCKSDB_INCLUDE_PATH})

  find_library(ROCKSDB_LIB NAMES rocksdb)
  if (NOT ROCKSDB_LIB)
    message(FATAL_ERROR "Fail to find RocksDB lib path")
  endif()
  message(STATUS "ROCKSDB_LIB: ${ROCKSDB_LIB}")
  set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${ROCKSDB_LIB})
endif()

if ((WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS"))
    if (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
        find_path(AWS_CORE_INCLUDE_PATH aws/core/Aws.h)
        if((NOT AWS_CORE_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find aws/core include path")
        endif()
        message(STATUS "aws/core include path: ${AWS_CORE_INCLUDE_PATH}")

        find_library(AWS_CORE_LIB aws-cpp-sdk-core)
        if((NOT AWS_CORE_LIB ))
          message(FATAL_ERROR "Fail to find aws-cpp-sdk-core lib")
        endif()
        message(STATUS "aws-cpp-sdk-core library: ${AWS_CORE_LIB}")

        find_path(AWS_KINESIS_INCLUDE_PATH aws/kinesis/KinesisClient.h)
        if((NOT AWS_KINESIS_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find aws/kinesis include path")
        endif()
        message(STATUS "aws/kinesis include path: ${AWS_KINESIS_INCLUDE_PATH}")

        find_library(AWS_KINESIS_LIB aws-cpp-sdk-kinesis)
        if((NOT AWS_KINESIS_LIB))
          message(FATAL_ERROR "Fail to find aws-cpp-sdk-kinesis lib")
        endif()
        message(STATUS "aws-cpp-sdk-kinesis library: ${AWS_KINESIS_LIB}")

        find_path(AWS_KINESIS_INCLUDE_PATH aws/kinesis/KinesisClient.h)
        if((NOT AWS_KINESIS_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find aws/kinesis include path")
        endif()
        message(STATUS "aws/kinesis include path: ${AWS_KINESIS_INCLUDE_PATH}")

        find_library(AWS_KINESIS_LIB aws-cpp-sdk-kinesis)
        if((NOT AWS_KINESIS_LIB))
          message(FATAL_ERROR "Fail to find aws-cpp-sdk-kinesis lib")
        endif()
        message(STATUS "aws-cpp-sdk-kinesis library: ${AWS_KINESIS_LIB}")

        find_path(AWS_S3_INCLUDE_PATH aws/s3/S3Client.h)
        if((NOT AWS_S3_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find aws/s3 include path")
        endif()
        message(STATUS "aws/s3 include path: ${AWS_S3_INCLUDE_PATH}")

        find_library(AWS_S3_LIB aws-cpp-sdk-s3)
        if((NOT AWS_S3_LIB ))
          message(FATAL_ERROR "Fail to find aws-cpp-sdk-s3 lib")
        endif()
        message(STATUS "aws-cpp-sdk-s3 library: ${AWS_S3_LIB}")

        set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${AWS_CORE_INCLUDE_PATH})
        set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${AWS_KINESIS_INCLUDE_PATH})
        set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${AWS_S3_INCLUDE_PATH})

        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${AWS_CORE_LIB})
        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${AWS_KINESIS_LIB})
        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${AWS_S3_LIB})

        find_library(ROCKSDB_CLOUD_LIB NAMES rocksdb-cloud-aws)

    elseif (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS")
        find_path(GCP_CS_INCLUDE_PATH google/cloud/storage/client.h)
        if((NOT GCP_CS_INCLUDE_PATH))
          message(FATAL_ERROR "Fail to find google/cloud/storage include path")
        endif()
        message(STATUS "google/cloud/storage include path: ${GCP_CS_INCLUDE_PATH}")

        find_library(GCP_COMMON_LIB google_cloud_cpp_common)
        if((NOT GCP_COMMON_LIB))
          message(FATAL_ERROR "Fail to find google_cloud_cpp_common lib")
        endif()
        message(STATUS "google_cloud_cpp_common library: ${GCP_COMMON_LIB}")

        find_library(GCP_CS_LIB google_cloud_cpp_storage)
        if((NOT GCP_CS_LIB))
          message(FATAL_ERROR "Fail to find google_cloud_cpp_storage lib")
        endif()
        message(STATUS "google_cloud_cpp_storage library: ${GCP_CS_LIB}")

        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${GCP_COMMON_LIB})
        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${GCP_CS_LIB})

        find_library(ROCKSDB_CLOUD_LIB NAMES rocksdb-cloud-gcp)
    endif ()

    # Common RocksDB Cloud setup (outside the S3/GCS specific blocks)
    find_path(ROCKSDB_CLOUD_INCLUDE_PATH NAMES rocksdb/db.h PATH_SUFFIXES "rocksdb_cloud_header")
    if (NOT ROCKSDB_CLOUD_INCLUDE_PATH)
      message(FATAL_ERROR "Fail to find RocksDB Cloud include path")
    endif ()
    message(STATUS "ROCKSDB_CLOUD_INCLUDE_PATH: ${ROCKSDB_CLOUD_INCLUDE_PATH}")
    set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${ROCKSDB_CLOUD_INCLUDE_PATH})
    include_directories(${ROCKSDB_INCLUDE_PATH})

    if (NOT ROCKSDB_CLOUD_LIB)
      message(FATAL_ERROR "Fail to find RocksDB Cloud lib path")
    endif ()
    message(STATUS "ROCKSDB_CLOUD_LIB: ${ROCKSDB_CLOUD_LIB}")
    set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${ROCKSDB_CLOUD_LIB})
elseif (WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
    set(ELOQSTORE_PARENT_DIR ${CMAKE_CURRENT_SOURCE_DIR} CACHE PATH "EloqStore parent directory")
    include(${PROJECT_SOURCE_DIR}/build_eloq_store.cmake)
endif()


if ((WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB"))
    SET(RESELOQ_LIBRARY ${RESELOQ_LIBRARY} ${ROCKSDB_LIBRARIES})
elseif (WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
    SET(RESELOQ_LIBRARY ${RESELOQ_LIBRARY} eloqstore)
endif ()


set(DYNAMIC_LIB
        ${CMAKE_THREAD_LIBS_INIT}
        ${GFLAGS_LIBRARY}
        ${PROTOBUF_LIBRARIES}
        ${GPERFTOOLS_LIBRARIES}
        # ${LEVELDB_LIB}
        ${BRAFT_LIB}
        ${BRPC_LIB}
        ${OPENSSL_CRYPTO_LIBRARY}
        ${OPENSSL_SSL_LIBRARY}
        ${THRIFT_LIB}
        ${THRIFTNB_LIB}
        dl
        )

set(DS_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR})
message(NOTICE "data store service proto dir: ${DS_PROTO_DIR}")
set(PROTO_SRC ${DS_PROTO_DIR})
set(PROTO_NAME ds_request)
execute_process(
	COMMAND protoc ./${PROTO_NAME}.proto --cpp_out=./
	WORKING_DIRECTORY ${PROTO_SRC}
)

if (BRPC_WITH_GLOG)
    find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
    find_library(GLOG_LIB NAMES glog VERSION ">=0.6.0" REQUIRED)

    if ((NOT GLOG_INCLUDE_PATH) OR(NOT GLOG_LIB))
        message(FATAL_ERROR "Fail to find glog")
    endif ()

    include_directories(${GLOG_INCLUDE_PATH})
    set(DYNAMIC_LIB ${DYNAMIC_LIB} ${GLOG_LIB})
endif ()


# #######
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
            pthread
            "-framework CoreFoundation"
            "-framework CoreGraphics"
            "-framework CoreData"
            "-framework CoreText"
            "-framework Security"
            "-framework Foundation"
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
            "-Wl,-U,_ProfilerStart"
            "-Wl,-U,_ProfilerStop")
endif ()

if(WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
    set(DYNAMIC_LIB ${DYNAMIC_LIB} ${URING_LIB})
    set(DYNAMIC_LIB ${DYNAMIC_LIB} ${BOOST_CONTEXT_TARGET})
endif ()

# crcspeed
# SET(CRCSPEED_SOURCES crcspeed/crcspeed.c crcspeed/crc16speed.c crcspeed/crc64speed.c)
# SET(RESELOQ_SOURCES ${RESELOQ_SOURCES} ${CRCSPEED_SOURCES})

SET(RESELOQ_SOURCES ${RESELOQ_SOURCES}
        thread_worker_pool.cpp
        data_store_service.cpp
        data_store_fault_inject.cpp
        data_store_service_config.cpp
        ds_request.pb.cc
    )

# The INIReader definition in the eloqstore library was renamed to avoid
# clashing with the other INIReader implementations (log service / core).
SET(RESELOQ_SOURCES ${RESELOQ_SOURCES}
    INIReader.cpp
    ini.c
)

if ((WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3") OR
        (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS"))
    SET(RESELOQ_SOURCES ${RESELOQ_SOURCES}
	  rocksdb_data_store_common.cpp
          rocksdb_cloud_data_store.cpp
          rocksdb_config.cpp
          purger_event_listener.cpp
          purger_sliding_window.cpp
    )
elseif (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
    SET(RESELOQ_SOURCES ${RESELOQ_SOURCES}
	  rocksdb_data_store_common.cpp
          rocksdb_data_store.cpp
          rocksdb_config.cpp
    )
elseif (WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
    SET(RESELOQ_SOURCES ${RESELOQ_SOURCES}
        eloq_store_config.cpp
        eloq_store_data_store.cpp
    )
endif ()

if (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
    SET(RESELOQ_SOURCES ${RESELOQ_SOURCES}
        s3_file_downloader.cpp
    )
endif()


include_directories(
        ${PROJECT_SOURCE_DIR}/
if(WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
        ${PROJECT_SOURCE_DIR}/eloqstore/
endif ()
)

# set(ABSEIL
#         absl::btree
#         absl::flat_hash_map
#         absl::span
#         )

# add_subdirectory(lua)
# add_subdirectory(fpconv)

# add_executable(redis_press redis_press.cpp)
add_executable(dss_server main.cpp ${RESELOQ_SOURCES})
target_link_libraries(dss_server ${RESELOQ_LIBRARY} ${DYNAMIC_LIB} )
# target_link_libraries(dss_server  ${ABSEIL} lua fpconv)

# Add rocksdb_cloud_dump utility
if ((WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3") OR
    (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS"))
    add_executable(rocksdb_cloud_dump rocksdb_cloud_dump.cpp)
    target_link_libraries(rocksdb_cloud_dump ${DYNAMIC_LIB} ${ROCKSDB_LIBRARIES} ${GFLAGS_LIBRARY})
    set_target_properties(rocksdb_cloud_dump PROPERTIES
        BUILD_RPATH "$ORIGIN/../lib"
        INSTALL_RPATH "$ORIGIN/../lib"
        INSTALL_RPATH_USE_LINK_PATH TRUE)
    install(TARGETS rocksdb_cloud_dump
            RUNTIME DESTINATION bin)
endif()

set_target_properties(dss_server PROPERTIES
	BUILD_RPATH "$ORIGIN/../lib"
	INSTALL_RPATH "$ORIGIN/../lib"
        INSTALL_RPATH_USE_LINK_PATH TRUE)

install(TARGETS dss_server
        RUNTIME DESTINATION bin)
