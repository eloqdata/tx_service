# Unit tests for tx_service. Built only when the parent configures -DWITH_TESTS=ON.
# Assumes parent context: targets `txservice`, `logservice`, `data_substrate`
# and the LOG_STATE_TYPE_* compile definitions are already defined.

find_package(Catch2 REQUIRED)
include(Catch)

set(TESTS_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(HARNESS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/harness)

# --- Shared test harness library (MemDataStore, TestNode, helpers) ---
# Sources are added by later tasks; the library may start near-empty.
add_library(test_harness STATIC
    ${HARNESS_DIR}/mem_data_store.cpp
    ${HARNESS_DIR}/test_node.cpp
)
target_include_directories(test_harness PUBLIC
    ${TESTS_DIR}
    ${HARNESS_DIR}
)
target_link_libraries(test_harness PUBLIC data_substrate)
set_property(TARGET test_harness PROPERTY CXX_STANDARD 20)

# --- Test executables ---
# Tests that rely on Catch2 to provide main() (the "Let Catch provide main()"
# files). They link Catch2::Catch2WithMain.
set(CATCH_MAIN_TESTS
    CcEntry-Test
    CcPage-Test
    LargeObjLRU-Test
)

# Tests that define their own int main() (they need gflags::ParseCommandLineFlags
# before brpc is used). They link Catch2::Catch2 (no bundled main).
set(OWN_MAIN_TESTS
    StartTsCollector-Test
    MemDataStore-Test
    TxConsistency-Test
    TestNodeSmoke-Test
)

function(add_substrate_test name catch_lib)
    add_executable(${name} ${TESTS_DIR}/${name}.cpp)
    target_include_directories(${name} PRIVATE ${TESTS_DIR} ${HARNESS_DIR})
    target_link_libraries(${name} PRIVATE test_harness ${catch_lib})
    set_property(TARGET ${name} PROPERTY CXX_STANDARD 20)
    set_property(TARGET ${name} PROPERTY CXX_EXTENSIONS OFF)
    catch_discover_tests(${name} PROPERTIES LABELS "${name}")
endfunction()

foreach(name ${CATCH_MAIN_TESTS})
    add_substrate_test(${name} Catch2::Catch2WithMain)
endforeach()

foreach(name ${OWN_MAIN_TESTS})
    add_substrate_test(${name} Catch2::Catch2)
endforeach()
