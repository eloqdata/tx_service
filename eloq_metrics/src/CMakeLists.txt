find_package(prometheus-cpp CONFIG REQUIRED)

option(WITH_GLOG "Whether to use glog" OFF)
option(ENABLE_ELOQ_METRICS_APP "Whether to enable mono metrics app" OFF)

message(STATUS "WITH_GLOG ${WITH_GLOG}")
message(STATUS "ENABLE_ELOQ_METRICS_APP ${ENABLE_ELOQ_METRICS_APP}")

if(WITH_GLOG)
    find_package(glog REQUIRED)
    add_compile_definitions(WITH_GLOG)
    set(GLOG_LINK_LIB glog::glog)
endif()

message(STATUS "Current Compiler is ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if(ENABLE_ELOQ_METRICS_APP)
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
    find_package(Boost REQUIRED COMPONENTS random thread)
endif()

if(ELOQ_METRICS_INCLUDE_DIR)
    include_directories(${ELOQ_METRICS_INCLUDE_DIR})
    set(INCLUDE_DIR "${ELOQ_METRICS_INCLUDE_DIR}")
else()
    include_directories(${PROJECT_SOURCE_DIR}/include)
    set(INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include)
endif()

if(ELOQ_METRICS_INCLUDE_DIR)
    set(SRC_DIR "${ELOQ_METRICS_SRC_DIR}")
else()
    set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
endif()

message(STATUS "ELOQ_METRICS_INCLUDE_DIR : ${INCLUDE_DIR}")
message(STATUS "ELOQ_METRICS_SRC_DIR : ${SRC_DIR}")

set(TARGET_SOURCE_LIST
        ${INCLUDE_DIR}/metrics.h
        ${INCLUDE_DIR}/meter.h
        ${INCLUDE_DIR}/metrics_collector.h
        ${INCLUDE_DIR}/prometheus_collector.h
        ${INCLUDE_DIR}/metrics_manager.h
        ${SRC_DIR}/metrics.cc
        ${SRC_DIR}/prometheus_collector.cc
        ${SRC_DIR}/metrics_manager.cc
        )

if(ENABLE_ELOQ_METRICS_APP OR ENABLE_BENCHMARK)
    set(TARGET_SOURCE_LIST ${TARGET_SOURCE_LIST}
        ${INCLUDE_DIR}/metrics_registry_impl.h
        ${SRC_DIR}/metrics_registry_impl.cpp
    )
endif()

add_library(${LIB_NAME}-shared SHARED "${TARGET_SOURCE_LIST}")
add_library(${LIB_NAME} STATIC "${TARGET_SOURCE_LIST}")

target_include_directories(${LIB_NAME}-shared
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        )

target_include_directories(${LIB_NAME} PUBLIC ${INCLUDE_DIR})

if(WITH_ABSEIL)
    set(ABSEIL absl::flat_hash_map)
endif()

target_link_libraries(${LIB_NAME}-shared PUBLIC prometheus-cpp::pull ${GLOG_LINK_LIB} PRIVATE ${ABSEIL})
target_link_libraries(${LIB_NAME} PUBLIC prometheus-cpp::pull ${GLOG_LINK_LIB} PRIVATE ${ABSEIL})

set_target_properties(${LIB_NAME}-shared
        PROPERTIES
        OUTPUT_NAME ${LIB_NAME}-shared
        VERSION "${PROJECT_VERSION}"
        POSITION_INDEPENDENT_CODE 1
        SOVERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}"
        DEBUG_POSTFIX "_d"
        MACOSX_RPATH ON
        )

file(RELATIVE_PATH _rel ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR} ${CMAKE_INSTALL_PREFIX})

if(APPLE)
    set(_rpath "@loader_path/${_rel}")
else()
    set(_rpath "\$ORIGIN/${_rel}")
endif()

file(TO_NATIVE_PATH "${_rpath}/${CMAKE_INSTALL_LIBDIR}" use_message_RPATH)

if(ENABLE_ELOQ_METRICS_APP)
    add_executable(${LIB_NAME}-app
            ${INCLUDE_DIR}/dummy_executor.h
            ${SRC_DIR}/metrics_app.cc
            ${SRC_DIR}/dummy_executor.cc
            )

    target_link_libraries(${LIB_NAME}-app PRIVATE Boost::random Boost::thread ${LIB_NAME}-shared ${GLOG_LINK_LIB} ${ABSEIL})
endif()


# install
generate_export_header(${LIB_NAME}-shared
        BASE_NAME ${PROJECT_NAME}
        EXPORT_FILE_NAME ${CMAKE_BINARY_DIR}/${INSTALL_INCLUDEDIR}/eloq-metrics.h
        )

install(TARGETS ${LIB_NAME}-shared
        EXPORT ${PROJECT_NAME}-targets
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}/${PROJECT_NAME}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}/${PROJECT_NAME}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}/${PROJECT_NAME}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}
        )

install(
        DIRECTORY ${PROJECT_SOURCE_DIR}/include/ ${CMAKE_BINARY_DIR}/include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}
)

if(GENERATE_PKGCONFIG)
    set(PKGCONFIG_LIBS)
    set(PKGCONFIG_REQUIRES)

    configure_file(
            ${PROJECT_SOURCE_DIR}/cmake/eloq-metrics-pc.in
            ${CMAKE_CURRENT_BINARY_DIR}/eloq-metrics.pc
            @ONLY
    )

    install(
            FILES ${CMAKE_CURRENT_BINARY_DIR}/eloq-metrics.pc
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
    )

    unset(PKGCONFIG_LIBS)
    unset(PKGCONFIG_REQUIRES)
endif()
