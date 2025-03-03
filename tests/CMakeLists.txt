
cmake_minimum_required( VERSION 3.0 )

project( CatchExamples CXX )

find_package(Catch2 REQUIRED)

option(USE_ROCKSDB_LOG_STATE "Whether use rocksdb log state or in-memory log state" ON)
message(NOTICE "USE_ROCKSDB_LOG_STATE : ${USE_ROCKSDB_LOG_STATE}")
option(WITH_ROCKSDB_CLOUD "RocksDB Cloud storage backend, S3 or GCS")
set_property(CACHE WITH_ROCKSDB_CLOUD PROPERTY STRINGS "S3" "GCS")
message(NOTICE "With RocksDB Cloud: ${WITH_ROCKSDB_CLOUD}")

if (USE_ROCKSDB_LOG_STATE)
  if (WITH_ROCKSDB_CLOUD MATCHES "S3|GCS")
    if (WITH_ROCKSDB_CLOUD STREQUAL "S3")
        add_compile_definitions(WITH_ROCKSDB_CLOUD=1)
    elseif (WITH_ROCKSDB_CLOUD STREQUAL "GCS")
        add_compile_definitions(WITH_ROCKSDB_CLOUD=2)
    endif()
  endif()
  add_compile_definitions(USE_ROCKSDB_LOG_STATE)
endif()
	   
# define folders used:
set( TESTS_DIR ${CMAKE_SOURCE_DIR}/tests )
set( HEADER_DIR   ${CMAKE_SOURCE_DIR}/tests/include) 

# single-file sources:

set( SOURCES_SINGLE_FILE
    CcEntry-Test.cpp
    CcPage-Test.cpp
    StartTsCollector-Test.cpp
)

set( SOURCES_ALL
    ${SOURCES_SINGLE_FILE}
)

foreach( name ${SOURCES_ALL} )
    list( APPEND SOURCES_ALL_PATH ${TESTS_DIR}/${name} )
endforeach()

string( REPLACE ".cpp" "" BASENAMES_SINGLE_FILE  "${SOURCES_SINGLE_FILE}" )

set( TARGETS_SINGLE_FILE   ${BASENAMES_SINGLE_FILE} )

set( TARGETS_ALL
    ${TARGETS_SINGLE_FILE}
)

enable_testing()
include(CTest)
include(Catch)

foreach( name ${TARGETS_SINGLE_FILE} )
    add_executable( ${name} ${TESTS_DIR}/${name}.cpp)
    target_link_libraries(${name} PUBLIC txservice logservice Catch2::Catch2)
    catch_discover_tests(${name})
endforeach()

foreach( name ${TARGETS_ALL} )
    target_include_directories( ${name} PRIVATE ${HEADER_DIR} ${CMAKE_SOURCE_DIR} ${INCLUDE_DIR} )

    set_property(TARGET ${name} PROPERTY CXX_STANDARD 17)
    set_property(TARGET ${name} PROPERTY CXX_EXTENSIONS OFF)

    # Add desired warnings
    if ( CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang|GNU" )
        target_compile_options( ${name}  PRIVATE -Wall -Wextra -Wunreachable-code )
    endif()
    # Clang specific warning go here
    if ( CMAKE_CXX_COMPILER_ID MATCHES "Clang" )
        # Actually keep these
        target_compile_options( ${name}  PRIVATE -Wweak-vtables -Wexit-time-destructors -Wglobal-constructors -Wmissing-noreturn )
    endif()
    if ( CMAKE_CXX_COMPILER_ID MATCHES "MSVC" )
        target_compile_options( ${name}  PRIVATE /W4 /w44265 /WX )
    endif()
endforeach()

