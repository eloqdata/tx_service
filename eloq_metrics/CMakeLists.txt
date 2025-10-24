cmake_minimum_required(VERSION 3.8 FATAL_ERROR)

project(eloq_metrics LANGUAGES CXX
        VERSION 0.0.1
        )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(METRICS_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(METRICS_SRC_DIR "${ELOQ_METRICS_ROOT_DIR}/src")
set(METRICS_INCLUDE_DIR "${ELOQ_METRICS_ROOT_DIR}/include")

set(LIB_NAME "eloq-metrics" CACHE STRING "library name.")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

message(STATUS "eloq_metrics will be installed to ${CMAKE_INSTALL_PREFIX}")
message(STATUS "Build type set to ${CMAKE_BUILD_TYPE}")

include(GenerateExportHeader)
include(GNUInstallDirs)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(GENERATE_PKGCONFIG "Generate and install pkg-config files" ${UNIX})

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_BINDIR})

set(INSTALL_LIBDIR ${CMAKE_INSTALL_LIBDIR} CACHE PATH "Installation directory for libraries")
set(INSTALL_BINDIR ${CMAKE_INSTALL_BINDIR} CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDEDIR ${CMAKE_INSTALL_INCLUDEDIR} CACHE PATH "Installation directory for header files")
set(INSTALL_CMAKEDIR ${DEF_INSTALL_CMAKEDIR} CACHE PATH "Installation directory for CMake files")

foreach(p LIB BIN include CMAKE)
    file(TO_NATIVE_PATH ${CMAKE_INSTALL_PREFIX}/${INSTALL_${p}DIR} _path)
    message(STATUS "Installing ${p} components to ${_path}")
    unset(_path)
endforeach()

option(WITH_ABSEIL "Enable integration with Abseil" OFF)
message(STATUS "WITH_ABSEIL: ${WITH_ABSEIL}")
if(WITH_ABSEIL)
    option(ABSL_PROPAGATE_CXX_STD "ABSL PROPAGATE CXX STD" ON)
    set(ABSEIL_CPP_PATH "${METRICS_ROOT_DIR}/abseil-cpp" CACHE PATH "Path to the Abseil library")
    message(STATUS "ABSEIL_CPP_PATH: ${ABSEIL_CPP_PATH}")
    if(EXISTS ${ABSEIL_CPP_PATH})
        message(STATUS "Using Abseil from: ${ABSEIL_CPP_PATH}")
        add_subdirectory(${ABSEIL_CPP_PATH})
        include_directories(${ABSEIL_CPP_PATH})
    else()
        message(STATUS "Cannot find ${ABSEIL_CPP_PATH}, searching with find_package...")
        find_package(absl CONFIG REQUIRED)
        message(STATUS "Found absl: ${absl_DIR}")
    endif()
    add_compile_definitions(ELOQ_METRICS_WITH_ABSEIL=1)
endif()

option(ENABLE_TESTING "Build tests" OFF)
option(ENABLE_BENCHMARK "Whether enable google benchmark" OFF)

message(STATUS "ENABLE_TESTING : ${ENABLE_TESTING}")
message(STATUS "ENABLE_BENCHMARK : ${ENABLE_BENCHMARK}")

add_subdirectory(src)

if(ENABLE_BENCHMARK)
    add_subdirectory(benchmark)
endif()

if(ENABLE_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

include(CMakePackageConfigHelpers)

write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config-version.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
)

install(
        EXPORT ${PROJECT_NAME}-targets
        NAMESPACE "${PROJECT_NAME}::"
        FILE ${PROJECT_NAME}-targets.cmake
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
)

configure_package_config_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/eloq-metrics.cmake.in"
        ${PROJECT_NAME}-config.cmake
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
        NO_CHECK_REQUIRED_COMPONENTS_MACRO
        PATH_VARS CMAKE_INSTALL_INCLUDEDIR
)

install(
        FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
)
