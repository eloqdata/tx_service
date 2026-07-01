get_filename_component(ELOQ_DATA_SUBSTRATE_ROOT
    "${CMAKE_CURRENT_LIST_DIR}/.."
    ABSOLUTE)

set(ELOQ_THIRD_PARTY_ROOT
    "${ELOQ_DATA_SUBSTRATE_ROOT}/third_party"
    CACHE PATH "Data Substrate third-party workspace")

if(DEFINED ENV{ELOQ_THIRD_PARTY_PREFIX})
    set(_ELOQ_THIRD_PARTY_DEFAULT_PREFIX "$ENV{ELOQ_THIRD_PARTY_PREFIX}")
elseif(EXISTS "${ELOQ_THIRD_PARTY_ROOT}/install")
    set(_ELOQ_THIRD_PARTY_DEFAULT_PREFIX "${ELOQ_THIRD_PARTY_ROOT}/install")
elseif(EXISTS "/opt/eloq/third_party")
    set(_ELOQ_THIRD_PARTY_DEFAULT_PREFIX "/opt/eloq/third_party")
else()
    set(_ELOQ_THIRD_PARTY_DEFAULT_PREFIX "${ELOQ_THIRD_PARTY_ROOT}/install")
endif()

set(ELOQ_THIRD_PARTY_PREFIX
    "${_ELOQ_THIRD_PARTY_DEFAULT_PREFIX}"
    CACHE PATH "Prebuilt third-party dependency prefix")

option(ELOQ_THIRD_PARTY_REQUIRED
    "Fail when source-built third-party libraries are not resolved from ELOQ_THIRD_PARTY_PREFIX"
    OFF)

foreach(_ELOQ_PREFIX_PATH_VAR
        CMAKE_PREFIX_PATH
        CMAKE_INCLUDE_PATH
        CMAKE_LIBRARY_PATH
        CMAKE_PROGRAM_PATH)
    if(_ELOQ_PREFIX_PATH_VAR STREQUAL "CMAKE_INCLUDE_PATH")
        list(PREPEND ${_ELOQ_PREFIX_PATH_VAR} "${ELOQ_THIRD_PARTY_PREFIX}/include")
    elseif(_ELOQ_PREFIX_PATH_VAR STREQUAL "CMAKE_LIBRARY_PATH")
        list(PREPEND ${_ELOQ_PREFIX_PATH_VAR}
            "${ELOQ_THIRD_PARTY_PREFIX}/lib"
            "${ELOQ_THIRD_PARTY_PREFIX}/lib64")
    elseif(_ELOQ_PREFIX_PATH_VAR STREQUAL "CMAKE_PROGRAM_PATH")
        list(PREPEND ${_ELOQ_PREFIX_PATH_VAR} "${ELOQ_THIRD_PARTY_PREFIX}/bin")
    else()
        list(PREPEND ${_ELOQ_PREFIX_PATH_VAR} "${ELOQ_THIRD_PARTY_PREFIX}")
    endif()
endforeach()

set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE CACHE BOOL
    "Prefer package config files from the third-party prefix"
    FORCE)

# Preferring CONFIG resolution makes find_package(Protobuf) pick protobuf's own
# package config, which exports the protobuf::libprotobuf imported target but --
# unlike CMake's FindProtobuf module -- leaves the legacy ${PROTOBUF_LIBRARIES}
# variable empty. This tree links protobuf through that legacy variable (e.g.
# the host_manager executable), so a config-mode resolution would drop libprotobuf
# from the link line ("DSO missing from command line"). Seed the legacy variable
# from the imported target once, so every consumer keeps working under either
# resolution mode.
find_package(Protobuf QUIET)
if(TARGET protobuf::libprotobuf AND NOT PROTOBUF_LIBRARIES)
    set(PROTOBUF_LIBRARIES protobuf::libprotobuf CACHE STRING
        "Protobuf libraries (seeded from the imported target under PREFER_CONFIG)"
        FORCE)
endif()

include_directories(BEFORE SYSTEM "${ELOQ_THIRD_PARTY_PREFIX}/include")
link_directories(
    "${ELOQ_THIRD_PARTY_PREFIX}/lib"
    "${ELOQ_THIRD_PARTY_PREFIX}/lib64")

function(eloq_assert_under_prefix path_var description)
    if(NOT ELOQ_THIRD_PARTY_REQUIRED)
        return()
    endif()

    if(NOT DEFINED ${path_var} OR "${${path_var}}" STREQUAL "")
        message(FATAL_ERROR
            "${description} was not found; expected it under ${ELOQ_THIRD_PARTY_PREFIX}")
    endif()

    list(GET ${path_var} 0 _ELOQ_ASSERT_PATH)
    if(NOT IS_ABSOLUTE "${_ELOQ_ASSERT_PATH}")
        return()
    endif()

    get_filename_component(_ELOQ_ASSERT_REAL_PATH "${_ELOQ_ASSERT_PATH}" REALPATH)
    get_filename_component(_ELOQ_ASSERT_REAL_PREFIX "${ELOQ_THIRD_PARTY_PREFIX}" REALPATH)
    string(FIND "${_ELOQ_ASSERT_REAL_PATH}" "${_ELOQ_ASSERT_REAL_PREFIX}/" _ELOQ_PREFIX_POS)

    if(NOT _ELOQ_PREFIX_POS EQUAL 0 AND
       NOT _ELOQ_ASSERT_REAL_PATH STREQUAL _ELOQ_ASSERT_REAL_PREFIX)
        message(FATAL_ERROR
            "${description} resolved to ${_ELOQ_ASSERT_REAL_PATH}, "
            "outside ELOQ_THIRD_PARTY_PREFIX=${_ELOQ_ASSERT_REAL_PREFIX}")
    endif()
endfunction()

function(eloq_assert_target_under_prefix target description)
    if(NOT ELOQ_THIRD_PARTY_REQUIRED)
        return()
    endif()

    if(NOT TARGET "${target}")
        message(FATAL_ERROR "${description} target '${target}' was not found")
    endif()

    get_filename_component(_ELOQ_ASSERT_REAL_PREFIX "${ELOQ_THIRD_PARTY_PREFIX}" REALPATH)
    set(_ELOQ_TARGET_LOCATIONS "")

    foreach(_ELOQ_TARGET_PROP
            IMPORTED_LOCATION
            IMPORTED_LOCATION_RELEASE
            IMPORTED_IMPLIB
            IMPORTED_IMPLIB_RELEASE)
        get_target_property(_ELOQ_TARGET_PROP_VALUE "${target}" "${_ELOQ_TARGET_PROP}")
        if(_ELOQ_TARGET_PROP_VALUE)
            list(APPEND _ELOQ_TARGET_LOCATIONS ${_ELOQ_TARGET_PROP_VALUE})
        endif()
    endforeach()

    get_target_property(_ELOQ_TARGET_INCLUDES "${target}" INTERFACE_INCLUDE_DIRECTORIES)
    if(_ELOQ_TARGET_INCLUDES)
        list(APPEND _ELOQ_TARGET_LOCATIONS ${_ELOQ_TARGET_INCLUDES})
    endif()

    foreach(_ELOQ_TARGET_LOCATION ${_ELOQ_TARGET_LOCATIONS})
        if(NOT IS_ABSOLUTE "${_ELOQ_TARGET_LOCATION}")
            continue()
        endif()

        get_filename_component(_ELOQ_TARGET_REAL_LOCATION "${_ELOQ_TARGET_LOCATION}" REALPATH)
        string(FIND "${_ELOQ_TARGET_REAL_LOCATION}" "${_ELOQ_ASSERT_REAL_PREFIX}/" _ELOQ_TARGET_PREFIX_POS)
        if(NOT _ELOQ_TARGET_PREFIX_POS EQUAL 0 AND
           NOT _ELOQ_TARGET_REAL_LOCATION STREQUAL _ELOQ_ASSERT_REAL_PREFIX)
            message(FATAL_ERROR
                "${description} target '${target}' uses ${_ELOQ_TARGET_REAL_LOCATION}, "
                "outside ELOQ_THIRD_PARTY_PREFIX=${_ELOQ_ASSERT_REAL_PREFIX}")
        endif()
    endforeach()
endfunction()

message(STATUS "ELOQ_THIRD_PARTY_PREFIX: ${ELOQ_THIRD_PARTY_PREFIX}")
