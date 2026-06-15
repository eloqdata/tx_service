cmake_minimum_required(VERSION 3.8)
project (host_manager)

find_package(Threads)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wno-parentheses -Wno-error -W -fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DFAULT_INJECTOR")

if ( CMAKE_COMPILER_IS_GNUCC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-reorder -Wno-unused-parameter -Wno-parentheses")
endif()
if ( MSVC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} /W3")
endif()

option(BRPC_WITH_GLOG "With glog" ON)

find_package(Protobuf REQUIRED)
find_package(GFLAGS REQUIRED)
find_package(OpenSSL REQUIRED)

message(STATUS "OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")

message(STATUS "protobuf dir: ${PROTOBUF_LIBRARY}")
message(STATUS "protobuf include dir: ${Protobuf_INCLUDE_DIR}")
message(STATUS "gflags libs dir: ${GFLAGS_LIBRARY}")
message(STATUS "gflags include dir: ${GFLAGS_INCLUDE_DIR}")
message(STATUS "cmake install dir: ${CMAKE_INSTALL_PREFIX}")

if ( CMAKE_COMPILER_IS_GNUCC )
    find_path(BRPC_INCLUDE_PATH NAMES brpc/stream.h)
    find_library(BRPC_LIB NAMES brpc)
    if ((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
        message(FATAL_ERROR "Fail to find brpc")
    endif()

    find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
    find_library(BRAFT_LIB NAMES braft)
    if ((NOT BRAFT_INCLUDE_PATH) OR (NOT BRAFT_LIB))
        message (FATAL_ERROR "Fail to find braft")
    endif()

    if(BRPC_WITH_GLOG)
        find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
        find_library(GLOG_LIB NAMES glog)
        if((NOT GLOG_INCLUDE_PATH) OR (NOT GLOG_LIB))
            message(FATAL_ERROR "Fail to find glog")
        endif()
        include_directories(${GLOG_INCLUDE_PATH})
        set(LINK_LIB ${LINK_LIB} ${GLOG_LIB})
        message(STATUS "glog lib dir: ${GLOG_LIB}")
    endif()

    find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
    find_library(LEVELDB_LIB NAMES leveldb)
    if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
        message(FATAL_ERROR "Fail to find leveldb")
    endif()
endif()


include(FetchContent)

# Import yaml-cpp library used by host manager
FetchContent_Declare(
    yaml-cpp
    GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
    GIT_TAG yaml-cpp-0.7.0 # Can be a tag (yaml-cpp-x.x.x), a commit hash, or a branch name (master)
)
FetchContent_MakeAvailable(yaml-cpp)

# Compile all protobuf files under proto directory.
SET(PROTO_SRC ${CMAKE_SOURCE_DIR}/include/proto)
FILE(GLOB PROTO_FILES RELATIVE ${PROTO_SRC} ${PROTO_SRC}/*.proto)
FOREACH(PROTO_FILE ${PROTO_FILES})
	STRING(REGEX REPLACE "[^/]proto" "" PROTO_NAME ${PROTO_FILE})
	LIST(APPEND PROTO_CC_FILES ${PROTO_SRC}/${PROTO_NAME}.pb.cc)
	ADD_CUSTOM_COMMAND(
		OUTPUT "${PROTO_SRC}/${PROTO_NAME}.pb.cc" "${PROTO_SRC}/${PROTO_NAME}.pb.h"
		COMMAND protoc ${PROTO_FILE} --proto_path=./ --cpp_out=./
		WORKING_DIRECTORY ${PROTO_SRC}
	)
ENDFOREACH(PROTO_FILE)

set(INCLUDE_DIR
   ${CMAKE_SOURCE_DIR}/include
   ${CMAKE_CURRENT_LIST_DIR}/../../third_party/ini
   ${Protobuf_INCLUDE_DIR})

if ( CMAKE_COMPILER_IS_GNUCC )
    set(INCLUDE_DIR ${INCLUDE_DIR}
        ${BRPC_INCLUDE_PATH}
        ${BRAFT_INCLUDE_PATH}
        ${GLOG_INCLUDE_PATH}
        ${GFLAGS_INCLUDE_PATH})
endif()

set(LINK_LIB ${LINK_LIB} ${PROTOBUF_LIBRARY})

if ( CMAKE_COMPILER_IS_GNUCC )
   set(LINK_LIB ${LINK_LIB}
       ${GFLAGS_LIBRARY}
       ${LEVELDB_LIB}
       ${BRAFT_LIB}
       ${BRPC_LIB}
       ${OPENSSL_LIBRARIES}
    )
endif()

SET(RaftHM_SOURCES
    ${CMAKE_SOURCE_DIR}/src/main.cpp
    ${CMAKE_SOURCE_DIR}/src/raft_host_manager_service.cpp
    ${CMAKE_SOURCE_DIR}/src/raft_host_manager.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../../third_party/ini/ini.c
    ${CMAKE_CURRENT_LIST_DIR}/../../third_party/ini/INIReader.cpp
    )

set(INCLUDE_DIR ${INCLUDE_DIR} ${PROTO_SRC} ${OPENSSL_INCLUDE_DIR})
set(RaftHM_SOURCES ${RaftHM_SOURCES} ${PROTO_CC_FILES})
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_INSTALL_PREFIX}/bin)

include_directories(${INCLUDE_DIR})
add_executable(host_manager ${RaftHM_SOURCES})
target_link_libraries(host_manager ${LINK_LIB})
target_link_libraries(host_manager PRIVATE yaml-cpp::yaml-cpp)
