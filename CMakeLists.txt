cmake_minimum_required(VERSION 3.8)
project(data_substrate)

# Set library name
set(DATA_SUBSTRATE_LIB "data_substrate" CACHE STRING "Data substrate library name")
set(METRICS_LIB "eloq-metrics" CACHE STRING "Metrics library name")

# Options from original eloq CMakeLists
set(WITH_DATA_STORE "ELOQDSS_ELOQSTORE" CACHE STRING "Which key-value storage to use")
set_property(CACHE WITH_DATA_STORE PROPERTY STRINGS "DYNAMODB" "BIGTABLE" "ROCKSDB" "ELOQDSS_ROCKSDB" "ELOQDSS_ROCKSDB_CLOUD_S3" "ELOQDSS_ROCKSDB_CLOUD_GCS" "ELOQDSS_ELOQSTORE")

option(WITH_LOG_SERVICE "Compile with built-in log service." ON)
option(OPEN_LOG_SERVICE "Compile with open log service." ON)
option(SMALL_RANGE "Whether enable small range" OFF)
option(STATISTICS "Whether enable table statistics" ON)
option(EXT_TX_PROC_ENABLED "Allows external threads to move forward the tx service." ON)
option(ELOQ_MODULE_ENABLED "Register the tx service as an ELOQ module." ON)
option(BUILD_SHARED_LIBS "Build shared libraries instead of static libraries" OFF)

message(NOTICE "WITH_DATA_STORE: ${WITH_DATA_STORE}")
message(NOTICE "WITH_LOG_SERVICE: ${WITH_LOG_SERVICE}")
message(NOTICE "OPEN_LOG_SERVICE: ${OPEN_LOG_SERVICE}")
message(NOTICE "BUILD_SHARED_LIBS: ${BUILD_SHARED_LIBS}")
message(NOTICE "WITH_JEMALLOC: ${WITH_JEMALLOC}")

add_library(jemalloc_cfg INTERFACE)
option(WITH_JEMALLOC "Use jemalloc" OFF)
if(WITH_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC REQUIRED jemalloc)
    target_compile_definitions(jemalloc_cfg INTERFACE WITH_JEMALLOC)
    target_include_directories(jemalloc_cfg INTERFACE ${JEMALLOC_INCLUDE_DIRS})
    target_link_libraries(jemalloc_cfg INTERFACE ${JEMALLOC_LINK_LIBRARIES})
endif()


# Synchronize WITH_DATA_STORE and WITH_LOG_STATE
# Determine RocksDB type from WITH_DATA_STORE
set(DATA_STORE_USES_LOCAL_ROCKSDB FALSE)
set(DATA_STORE_USES_CLOUD_S3_ROCKSDB FALSE)
set(DATA_STORE_USES_CLOUD_GCS_ROCKSDB FALSE)
set(DATA_STORE_USES_ROCKSDB FALSE)

option(INI_USE_HEAP "Whether parse ini on heap" ON)

if (INI_USE_HEAP)
    add_definitions(-DINI_USE_STACK=0)
    add_definitions(-DINI_ALLOW_REALLOC=1)
endif()

if(WITH_DATA_STORE STREQUAL "ROCKSDB" OR WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
    set(DATA_STORE_USES_LOCAL_ROCKSDB TRUE)
    set(DATA_STORE_USES_ROCKSDB TRUE)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
    set(DATA_STORE_USES_CLOUD_S3_ROCKSDB TRUE)
    set(DATA_STORE_USES_ROCKSDB TRUE)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS")
    set(DATA_STORE_USES_CLOUD_GCS_ROCKSDB TRUE)
    set(DATA_STORE_USES_ROCKSDB TRUE)
endif()

# Check if WITH_LOG_STATE is already set in cache
get_property(LOG_STATE_CACHED CACHE WITH_LOG_STATE PROPERTY VALUE SET)

# If DATA_STORE uses RocksDB, synchronize WITH_LOG_STATE
if(DATA_STORE_USES_ROCKSDB)
    if(NOT LOG_STATE_CACHED)
        # Auto-set WITH_LOG_STATE based on WITH_DATA_STORE
        if(DATA_STORE_USES_LOCAL_ROCKSDB)
            set(WITH_LOG_STATE "ROCKSDB" CACHE STRING "The log state implementation" FORCE)
            message(WARNING "WITH_LOG_STATE was not set. Auto-set to 'ROCKSDB' to match WITH_DATA_STORE='${WITH_DATA_STORE}'. Please set WITH_LOG_STATE explicitly if you need a different value.")
        elseif(DATA_STORE_USES_CLOUD_S3_ROCKSDB)
            set(WITH_LOG_STATE "ROCKSDB_CLOUD_S3" CACHE STRING "The log state implementation" FORCE)
            message(WARNING "WITH_LOG_STATE was not set. Auto-set to 'ROCKSDB_CLOUD_S3' to match WITH_DATA_STORE='${WITH_DATA_STORE}'. Please set WITH_LOG_STATE explicitly if you need a different value.")
        elseif(DATA_STORE_USES_CLOUD_GCS_ROCKSDB)
            set(WITH_LOG_STATE "ROCKSDB_CLOUD_GCS" CACHE STRING "The log state implementation" FORCE)
            message(WARNING "WITH_LOG_STATE was not set. Auto-set to 'ROCKSDB_CLOUD_GCS' to match WITH_DATA_STORE='${WITH_DATA_STORE}'. Please set WITH_LOG_STATE explicitly if you need a different value.")
        endif()
        set_property(CACHE WITH_LOG_STATE PROPERTY STRINGS "MEMORY" "ROCKSDB" "ROCKSDB_CLOUD_S3" "ROCKSDB_CLOUD_GCS")
    else()
        # Validate compatibility
        if(DATA_STORE_USES_LOCAL_ROCKSDB AND NOT WITH_LOG_STATE STREQUAL "ROCKSDB")
            message(FATAL_ERROR "WITH_DATA_STORE='${WITH_DATA_STORE}' uses local RocksDB, but WITH_LOG_STATE='${WITH_LOG_STATE}' is incompatible. WITH_LOG_STATE must be 'ROCKSDB' when using local RocksDB.")
        elseif(DATA_STORE_USES_CLOUD_S3_ROCKSDB AND NOT WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_S3")
            message(FATAL_ERROR "WITH_DATA_STORE='${WITH_DATA_STORE}' uses cloud RocksDB (S3), but WITH_LOG_STATE='${WITH_LOG_STATE}' is incompatible. WITH_LOG_STATE must be 'ROCKSDB_CLOUD_S3' when using cloud RocksDB (S3).")
        elseif(DATA_STORE_USES_CLOUD_GCS_ROCKSDB AND NOT WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_GCS")
            message(FATAL_ERROR "WITH_DATA_STORE='${WITH_DATA_STORE}' uses cloud RocksDB (GCS), but WITH_LOG_STATE='${WITH_LOG_STATE}' is incompatible. WITH_LOG_STATE must be 'ROCKSDB_CLOUD_GCS' when using cloud RocksDB (GCS).")
        endif()
    endif()
else()
    # DATA_STORE doesn't use RocksDB, so WITH_LOG_STATE can be anything
    # No synchronization needed in this case
endif()

message(NOTICE "WITH_DATA_STORE: ${WITH_DATA_STORE}")
message(NOTICE "WITH_LOG_STATE: ${WITH_LOG_STATE}")

# Enable Position Independent Code for shared libraries
if(BUILD_SHARED_LIBS)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    message(NOTICE "Building SHARED libraries")
else()
    message(NOTICE "Building STATIC libraries")
endif()

# Add compile definitions based on options
if(WITH_DATA_STORE STREQUAL "DYNAMODB")
    set(KV_STORAGE_VAL 1 CACHE STRING "dynamodb" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_DYNAMODB)
    find_package(AWSSDK REQUIRED COMPONENTS dynamodb)
elseif(WITH_DATA_STORE STREQUAL "ROCKSDB")
    add_compile_definitions(DATA_STORE_TYPE_ROCKSDB)
elseif(WITH_DATA_STORE STREQUAL "BIGTABLE")
    set(KV_STORAGE_VAL 2 CACHE STRING "big table" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_BIGTABLE)
    find_package(google_cloud_cpp_bigtable REQUIRED)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
    set(KV_STORAGE_VAL 3 CACHE STRING "eloq_ds" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS")
    set(KV_STORAGE_VAL 3 CACHE STRING "eloq_ds" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
    set(KV_STORAGE_VAL 3 CACHE STRING "eloq_ds" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
    set(KV_STORAGE_VAL 3 CACHE STRING "eloq_ds" FORCE)
    add_compile_definitions(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
    option(WITH_TXSERVICE "Whether compile eloqstore with txservice" ON)
    set(ELOQSTORE_PARENT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/store_handler/eloq_data_store_service CACHE PATH "EloqStore parent directory")
    INCLUDE(${CMAKE_CURRENT_SOURCE_DIR}/store_handler/eloq_data_store_service/build_eloq_store.cmake)
else()
    message(FATAL_ERROR "Unset WITH_DATA_STORE")
endif()

# Find RocksDB if needed
if(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB" OR WITH_DATA_STORE STREQUAL "ROCKSDB")
    find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
    if(NOT ROCKSDB_INCLUDE_PATH)
        message(FATAL_ERROR "Fail to find RocksDB include path")
    endif()
    message(STATUS "ROCKSDB_INCLUDE_PATH: ${ROCKSDB_INCLUDE_PATH}")
    include_directories(${ROCKSDB_INCLUDE_PATH})

    find_library(ROCKSDB_LIB NAMES rocksdb)
    if(NOT ROCKSDB_LIB)
        message(FATAL_ERROR "Fail to find RocksDB lib path")
    endif()
    message(STATUS "ROCKSDB_LIB: ${ROCKSDB_LIB}")
    set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${ROCKSDB_LIB})
endif()

# Handle RocksDB Cloud S3/GCS configurations
if((WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_GCS") 
          OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS"))
    if((WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3"))
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
        set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${AWS_S3_INCLUDE_PATH})

        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${AWS_CORE_LIB})
        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${AWS_S3_LIB})

        find_library(ROCKSDB_CLOUD_LIB NAMES rocksdb-cloud-aws)
        if(NOT ROCKSDB_CLOUD_LIB)
            message(FATAL_ERROR "Fail to find RocksDB Cloud lib path")
        endif()
        message(STATUS "ROCKSDB_CLOUD_LIB: ${ROCKSDB_CLOUD_LIB}")
        set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARIES} ${ROCKSDB_CLOUD_LIB})
    endif()

    # Common RocksDB Cloud setup
    find_path(ROCKSDB_CLOUD_INCLUDE_PATH NAMES rocksdb/db.h PATH_SUFFIXES "rocksdb_cloud_header")
    if(NOT ROCKSDB_CLOUD_INCLUDE_PATH)
        message(FATAL_ERROR "Fail to find RocksDB Cloud include path")
    endif()
    message(STATUS "ROCKSDB_CLOUD_INCLUDE_PATH: ${ROCKSDB_CLOUD_INCLUDE_PATH}")
    set(ROCKSDB_INCLUDE_PATH ${ROCKSDB_INCLUDE_PATH} ${ROCKSDB_CLOUD_INCLUDE_PATH})
    include_directories(${ROCKSDB_INCLUDE_PATH})

endif()

# Compile proto file for data store service
if((WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB") OR (WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE"))
    set(DS_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/store_handler/eloq_data_store_service)
    message(NOTICE "data store service proto dir: ${DS_PROTO_DIR}")
    set(PROTO_SRC ${DS_PROTO_DIR})
    set(PROTO_NAME ds_request)
    execute_process(
        COMMAND protoc ./${PROTO_NAME}.proto --cpp_out=./
        WORKING_DIRECTORY ${PROTO_SRC}
    )
endif()

# Add compile definitions based on options
if(WITH_LOG_SERVICE)
    add_compile_definitions(WITH_LOG_SERVICE)
endif()

if(OPEN_LOG_SERVICE)
    add_compile_definitions(OPEN_LOG_SERVICE=1)
endif()

if(EXT_TX_PROC_ENABLED)
    add_compile_definitions(EXT_TX_PROC_ENABLED)
endif()

if(ELOQ_MODULE_ENABLED)
    add_compile_definitions(ELOQ_MODULE_ENABLED)
endif()

if(SMALL_RANGE)
    add_compile_definitions(SMALL_RANGE)
endif()

if(STATISTICS)
    add_compile_definitions(STATISTICS)
endif()


# Include build files for subcomponents
INCLUDE(build_tx_service.cmake)
INCLUDE(build_eloq_metrics.cmake)

if(WITH_LOG_SERVICE)
    if(OPEN_LOG_SERVICE)
        INCLUDE(build_log_service.cmake)
    else()
        INCLUDE(build_eloq_log_service.cmake)
    endif()
endif()

# Find mimalloc
find_package(MIMALLOC REQUIRED)
include_directories(${MIMALLOC_INCLUDE_DIR})

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include
    ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/cc
    ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/remote
    ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/fault
    ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/store
    ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/tx-log-protos
    ${CMAKE_CURRENT_SOURCE_DIR}/eloq_metrics/include
    ${CMAKE_CURRENT_SOURCE_DIR}/store_handler
)

if(WITH_LOG_SERVICE)
    if(OPEN_LOG_SERVICE)
        include_directories(${CMAKE_CURRENT_SOURCE_DIR}/log_service/include)
    else()
        include_directories(${CMAKE_CURRENT_SOURCE_DIR}/eloq_log_service/include)
    endif()
endif()

# Collect all source files for the library
SET(DATA_SUBSTRATE_SOURCES
    # Core data substrate files
    core/src/data_substrate.cpp
    core/src/log_init.cpp
    core/src/metrics_init.cpp
    core/src/storage_init.cpp
    core/src/tx_service_init.cpp
)

# Add store handler sources based on data store type
if(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB" OR WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3" OR WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS")
    SET(DATA_SUBSTRATE_SOURCES ${DATA_SUBSTRATE_SOURCES}
        store_handler/data_store_service_client.cpp
        store_handler/data_store_service_client_closure.cpp
        store_handler/data_store_service_scanner.cpp
        store_handler/store_util.cpp
        store_handler/eloq_data_store_service/ini.c
        store_handler/eloq_data_store_service/INIReader.cpp
        store_handler/eloq_data_store_service/thread_worker_pool.cpp
        store_handler/eloq_data_store_service/data_store_service.cpp
        store_handler/eloq_data_store_service/data_store_fault_inject.cpp
        store_handler/eloq_data_store_service/data_store_service_config.cpp
        store_handler/eloq_data_store_service/ds_request.pb.cc
        store_handler/eloq_data_store_service/rocksdb_config.cpp
        store_handler/eloq_data_store_service/rocksdb_data_store_common.cpp
    )
    
    if(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB")
        SET(DATA_SUBSTRATE_SOURCES ${DATA_SUBSTRATE_SOURCES}
            store_handler/eloq_data_store_service/rocksdb_data_store.cpp
        )
    elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
        SET(DATA_SUBSTRATE_SOURCES ${DATA_SUBSTRATE_SOURCES}
            store_handler/eloq_data_store_service/rocksdb_cloud_data_store.cpp
            store_handler/eloq_data_store_service/purger_event_listener.cpp
            store_handler/eloq_data_store_service/purger_sliding_window.cpp
	    store_handler/eloq_data_store_service/s3_file_downloader.cpp
        )
    elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS")
        SET(DATA_SUBSTRATE_SOURCES ${DATA_SUBSTRATE_SOURCES}
            store_handler/eloq_data_store_service/rocksdb_cloud_data_store.cpp
            store_handler/eloq_data_store_service/purger_event_listener.cpp
            store_handler/eloq_data_store_service/purger_sliding_window.cpp
        )
    endif()
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
    SET(DATA_SUBSTRATE_SOURCES ${DATA_SUBSTRATE_SOURCES}
        store_handler/data_store_service_client.cpp
        store_handler/data_store_service_client_closure.cpp
        store_handler/data_store_service_scanner.cpp
        store_handler/store_util.cpp
        store_handler/eloq_data_store_service/thread_worker_pool.cpp
        store_handler/eloq_data_store_service/data_store_service.cpp
        store_handler/eloq_data_store_service/data_store_fault_inject.cpp
        store_handler/eloq_data_store_service/data_store_service_config.cpp
        store_handler/eloq_data_store_service/eloq_store_data_store.cpp
        store_handler/eloq_data_store_service/ds_request.pb.cc
        store_handler/eloq_data_store_service/eloq_store_config.cpp
    )
elseif((WITH_DATA_STORE STREQUAL "ROCKSDB") OR (WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_S3") OR(WITH_DATA_STORE STREQUAL "ROCKSDB_CLOUD_GCS"))
    SET(DATA_SUBSTRATE_SOURCES ${DATA_SUBSTRATE_SOURCES}
        store_handler/rocksdb_config.cpp
        store_handler/rocksdb_handler.cpp
        store_handler/store_util.cpp
    )
endif()

# Build the library (static or shared based on BUILD_SHARED_LIBS option)
ADD_LIBRARY(${DATA_SUBSTRATE_LIB} ${DATA_SUBSTRATE_SOURCES})

# Link with dependencies
SET(DATA_SUBSTRATE_LINK_LIBS
    txservice
    ${METRICS_LIB}
)

if(WITH_LOG_SERVICE)
    SET(DATA_SUBSTRATE_LINK_LIBS ${DATA_SUBSTRATE_LINK_LIBS} logservice)
endif()

if(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB" OR WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3" OR WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS")
    SET(DATA_SUBSTRATE_LINK_LIBS ${DATA_SUBSTRATE_LINK_LIBS} ${ROCKSDB_LIBRARIES})
elseif(WITH_DATA_STORE STREQUAL "DYNAMODB")
    SET(DATA_SUBSTRATE_LINK_LIBS ${DATA_SUBSTRATE_LINK_LIBS} ${AWSSDK_LINK_LIBRARIES})
elseif(WITH_DATA_STORE STREQUAL "BIGTABLE")
    SET(DATA_SUBSTRATE_LINK_LIBS ${DATA_SUBSTRATE_LINK_LIBS} google-cloud-cpp::bigtable)
elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ELOQSTORE")
    SET(DATA_SUBSTRATE_LINK_LIBS ${DATA_SUBSTRATE_LINK_LIBS} eloqstore)
endif()

target_link_libraries(${DATA_SUBSTRATE_LIB} PUBLIC ${DATA_SUBSTRATE_LINK_LIBS})

if(WITH_JEMALLOC)
    target_link_libraries(${DATA_SUBSTRATE_LIB} PUBLIC jemalloc_cfg)
endif()

# Export include directories for other targets
TARGET_INCLUDE_DIRECTORIES(${DATA_SUBSTRATE_LIB} PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include
    ${CMAKE_CURRENT_SOURCE_DIR}/eloq_metrics/include
    ${CMAKE_CURRENT_SOURCE_DIR}/store_handler
)

# --- Installation Rules ---
# install tx_service lib and includes files
install(TARGETS txservice
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(TARGETS logservice
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(TARGETS data_substrate
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(TARGETS eloq-metrics
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

if(FORK_HM_PROCESS) # Only install host_manager if it's built
    install(TARGETS host_manager
            RUNTIME DESTINATION bin)
endif()