cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

SET(ELOQ_STORE_SOURCE_DIR ${ELOQSTORE_PARENT_DIR}/eloqstore)

option(ELOQ_MODULE_ENABLED "Enable EloqModule" OFF)
message("ELOQ_MODULE_ENABLED: " ${ELOQ_MODULE_ENABLED})
if (ELOQ_MODULE_ENABLED)
    add_compile_definitions(ELOQ_MODULE_ENABLED)
endif()

find_package(Threads REQUIRED)
find_package(glog REQUIRED)

if(WITH_ASAN)
    message("build eloqstore with ASAN: ${WITH_ASAN}")
    # https://www.boost.org/doc/libs/master/libs/context/doc/html/context/stack/sanitizers.html
    add_compile_definitions(BOOST_USE_ASAN)
    add_compile_definitions(BOOST_USE_UCONTEXT)

    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)

    find_library(Boost_CONTEXT_ASAN_LIBRARY
            NAMES boost_context-asan
    )

    if (NOT Boost_CONTEXT_ASAN_LIBRARY)
        message(FATAL_ERROR
                "libboost_context-asan not found in system library paths")
    endif ()

    set(BOOST_CONTEXT_TARGET ${Boost_CONTEXT_ASAN_LIBRARY})
else ()
    find_library(Boost_CONTEXT_LIBRARY NAMES boost_context)

    if (NOT Boost_CONTEXT_LIBRARY)
        message(FATAL_ERROR
                "libboost_context not found in system library paths")
    endif ()
    set(BOOST_CONTEXT_TARGET ${Boost_CONTEXT_LIBRARY})
endif()
message("BOOST_CONTEXT_TARGET: ${BOOST_CONTEXT_TARGET}")

find_package(jsoncpp REQUIRED)
find_package(CURL REQUIRED)
find_library(ZSTD_LIBRARY zstd)
find_package(AWSSDK REQUIRED COMPONENTS s3)

find_path(URING_INCLUDE_PATH NAMES liburing.h)
find_library(URING_LIB NAMES uring)
if ((NOT URING_INCLUDE_PATH) OR (NOT URING_LIB))
    message(FATAL_ERROR "Fail to find liburing")
endif()

find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${ELOQ_STORE_SOURCE_DIR}/.git")
    option(GIT_SUBMODULE "Check submodules during build" ON)
    if(GIT_SUBMODULE)
        # Update submodules as needed
        message(STATUS "Submodule update")
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                        WORKING_DIRECTORY ${ELOQ_STORE_SOURCE_DIR}
                        RESULT_VARIABLE GIT_SUBMOD_RESULT)
        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif()
    endif()
endif()

if(NOT EXISTS "${ELOQ_STORE_SOURCE_DIR}/external/concurrentqueue/CMakeLists.txt")
    message(FATAL_ERROR "The submodules were not downloaded! GIT_SUBMODULE was turned off or failed. Please update submodules and try again.")
endif()
add_subdirectory(${ELOQ_STORE_SOURCE_DIR}/external/concurrentqueue)

if (NOT WITH_TXSERVICE)
    add_subdirectory(${ELOQ_STORE_SOURCE_DIR}/external/abseil)
endif()

SET(ELOQ_STORE_INCLUDE
    ${URING_INCLUDE_PATH}
    ${Boost_INCLUDE_DIRS}
    ${ELOQ_STORE_SOURCE_DIR}/external
    ${ELOQ_STORE_SOURCE_DIR}/include
    ${ELOQ_STORE_SOURCE_DIR}
    )

set(ELOQ_STORE_SOURCES
    ${ELOQ_STORE_SOURCE_DIR}/external/random.cc
    ${ELOQ_STORE_SOURCE_DIR}/external/xxhash.c
    ${ELOQ_STORE_SOURCE_DIR}/src/async_io_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/cloud_storage_service.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/coding.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/comparator.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/compression.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/eloq_store.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/eloqstore_module.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/file_gc.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/kill_point.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/replayer.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/data_page.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/data_page_builder.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/index_page_builder.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/index_page_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/mem_index_page.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/object_store.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/page.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/page_mapper.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/root_meta.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/storage/shard.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/write_buffer_aggregator.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/archive_crond.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/background_write.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/batch_write_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/prewarm_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/read_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/scan_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/task_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/tasks/write_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/src/types.cpp)

add_library(eloqstore STATIC ${ELOQ_STORE_SOURCES})

target_include_directories(eloqstore PUBLIC ${ELOQ_STORE_INCLUDE})
target_link_libraries(eloqstore PRIVATE ${URING_LIB} ${BOOST_CONTEXT_TARGET} glog::glog jsoncpp_lib ${CURL_LIBRARIES} ${ZSTD_LIBRARY} ${AWSSDK_LINK_LIBRARIES}
    PUBLIC absl::flat_hash_map
)

# Conditional linking for metrics when compiled with txservice
# Note: eloq-metrics target is created by build_eloq_metrics.cmake which is included
# after this file, so we can't check TARGET eloq-metrics here. CMake will validate
# the link dependency after all targets are created.
if(WITH_TXSERVICE)
    # Link eloq_metrics library (created by build_eloq_metrics.cmake)
    target_link_libraries(eloqstore PRIVATE eloq-metrics)
    # Add include directory for metrics headers
    # From build_eloq_store.cmake location, eloq_metrics is at ../../eloq_metrics
    # CMAKE_CURRENT_LIST_DIR is the directory of this file
    target_include_directories(eloqstore PRIVATE 
        ${CMAKE_CURRENT_LIST_DIR}/../../eloq_metrics/include)
    # ELOQSTORE_WITH_TXSERVICE is already defined via add_compile_definitions in parent CMakeLists.txt
    # but we ensure it's also set for this target explicitly
    target_compile_definitions(eloqstore PRIVATE ELOQSTORE_WITH_TXSERVICE)
    message(STATUS "EloqStore metrics enabled: will link eloq-metrics")
endif()
