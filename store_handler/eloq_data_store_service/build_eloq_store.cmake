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
    set(BOOST_CONTEXT_ASAN_PATH "$ENV{HOME}/boost_ucontext_asan")
    # https://www.boost.org/doc/libs/master/libs/context/doc/html/context/stack/sanitizers.html
    add_compile_definitions(BOOST_USE_ASAN)
    add_compile_definitions(BOOST_USE_UCONTEXT)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    find_library(Boost_CONTEXT_LIBRARY
            NAMES boost_context
            PATHS ${BOOST_CONTEXT_ASAN_PATH}/lib
            NO_DEFAULT_PATH)
    set(BOOST_CONTEXT_TARGET ${Boost_CONTEXT_LIBRARY})
else ()
    find_package(Boost REQUIRED COMPONENTS context)
    set(BOOST_CONTEXT_TARGET Boost::context)
endif()

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

if(NOT EXISTS "${ELOQ_STORE_SOURCE_DIR}/concurrentqueue/CMakeLists.txt")
    message(FATAL_ERROR "The submodules were not downloaded! GIT_SUBMODULE was turned off or failed. Please update submodules and try again.")
endif()
add_subdirectory(${ELOQ_STORE_SOURCE_DIR}/concurrentqueue)

if (NOT WITH_TXSERVICE)
    add_subdirectory(${ELOQ_STORE_SOURCE_DIR}/abseil)
endif()

set(INI_SOURCES ${ELOQ_STORE_SOURCE_DIR}/inih/ini.c ${ELOQ_STORE_SOURCE_DIR}/inih/cpp/INIReader.cpp)

SET(ELOQ_STORE_INCLUDE
    ${ELOQ_STORE_SOURCE_DIR}
    ${URING_INCLUDE_PATH}
    ${Boost_INCLUDE_DIRS}
    )

set(ELOQ_STORE_SOURCES
    ${ELOQ_STORE_SOURCE_DIR}/coding.cpp
    ${ELOQ_STORE_SOURCE_DIR}/data_page_builder.cpp
    ${ELOQ_STORE_SOURCE_DIR}/comparator.cpp
    ${ELOQ_STORE_SOURCE_DIR}/index_page_builder.cpp
    ${ELOQ_STORE_SOURCE_DIR}/mem_index_page.cpp
    ${ELOQ_STORE_SOURCE_DIR}/index_page_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/write_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/read_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/scan_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/prewarm_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/batch_write_task.cpp
    ${ELOQ_STORE_SOURCE_DIR}/background_write.cpp
    ${ELOQ_STORE_SOURCE_DIR}/async_io_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/data_page.cpp
    ${ELOQ_STORE_SOURCE_DIR}/page.cpp
    ${ELOQ_STORE_SOURCE_DIR}/page_mapper.cpp
    ${ELOQ_STORE_SOURCE_DIR}/task_manager.cpp
    ${ELOQ_STORE_SOURCE_DIR}/eloq_store.cpp
    ${ELOQ_STORE_SOURCE_DIR}/shard.cpp
    ${ELOQ_STORE_SOURCE_DIR}/root_meta.cpp
    ${ELOQ_STORE_SOURCE_DIR}/replayer.cpp
    ${ELOQ_STORE_SOURCE_DIR}/external/random.cc
    ${ELOQ_STORE_SOURCE_DIR}/external/xxhash.c
    ${ELOQ_STORE_SOURCE_DIR}/kill_point.cpp
    ${ELOQ_STORE_SOURCE_DIR}/file_gc.cpp
    ${ELOQ_STORE_SOURCE_DIR}/archive_crond.cpp
    ${ELOQ_STORE_SOURCE_DIR}/object_store.cpp
    ${ELOQ_STORE_SOURCE_DIR}/types.cpp
    ${ELOQ_STORE_SOURCE_DIR}/kv_options.cpp
    ${ELOQ_STORE_SOURCE_DIR}/compression.cpp
    ${ELOQ_STORE_SOURCE_DIR}/eloqstore_module.cpp)

add_library(eloqstore STATIC ${ELOQ_STORE_SOURCES} ${INI_SOURCES})

# Rename the inih C++ wrapper symbol when building eloqstore to avoid
# clashing with the other INIReader implementations (log service / core).
target_compile_definitions(eloqstore PRIVATE INIReader=EloqStorePrivateINIReader)

target_include_directories(eloqstore PUBLIC ${ELOQ_STORE_INCLUDE})
target_link_libraries(eloqstore PRIVATE ${URING_LIB} ${BOOST_CONTEXT_TARGET} glog::glog absl::flat_hash_map jsoncpp_lib ${CURL_LIBRARIES} ${ZSTD_LIBRARY} ${AWSSDK_LINK_LIBRARIES})
