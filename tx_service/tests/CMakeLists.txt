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

# --- Phase 2 cluster harness: workload proto + driver lib ---
set(WORKLOAD_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/cluster/txnode_workload.proto)
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/txnode_workload.pb.cc
           ${CMAKE_CURRENT_BINARY_DIR}/txnode_workload.pb.h
    COMMAND protoc --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
            -I ${CMAKE_CURRENT_SOURCE_DIR}/cluster ${WORKLOAD_PROTO}
    DEPENDS ${WORKLOAD_PROTO}
    COMMENT "Generating txnode_workload.pb.{h,cc}")
add_library(cluster_harness STATIC
    ${CMAKE_CURRENT_BINARY_DIR}/txnode_workload.pb.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster/scripted_host_manager.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster/test_cluster.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster/txnode_bringup.cpp)
target_include_directories(cluster_harness PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${HARNESS_DIR})
target_link_libraries(cluster_harness PUBLIC data_substrate)
set_property(TARGET cluster_harness PROPERTY CXX_STANDARD 20)

# --- Standalone tx-service node binary (driven by the cluster test driver) ---
# Not a Catch test: it has its own main() and parses gflags. cluster_harness
# provides TxNode + the generated WorkloadService; test_harness provides the
# MemDataStore object whose vtable txnode_bringup.cpp references. The two
# harness libs do not share sources, so linking both is duplicate-symbol-free.
# Key/Rec are duplicated (as MakeKey/MakeRec) in txnode_main.cpp, so it does not
# rely on test_harness's test_node.cpp symbols.
add_executable(txnode ${CMAKE_CURRENT_SOURCE_DIR}/cluster/txnode_main.cpp)
target_include_directories(txnode PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${HARNESS_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster)
target_link_libraries(txnode PRIVATE cluster_harness test_harness data_substrate)
set_property(TARGET txnode PROPERTY CXX_STANDARD 20)

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

# --- Phase 2 cross-NG cluster test (own main; links cluster_harness) ---
# Drives a real 2-node out-of-process cluster, so it links cluster_harness (the
# TestCluster driver + generated WorkloadService stub) and must be built after
# the `txnode` binary it spawns (TestCluster locates txnode next to the test
# binary, in the same tests/ dir).
add_executable(ClusterCrossNg-Test ${TESTS_DIR}/ClusterCrossNg-Test.cpp)
target_include_directories(ClusterCrossNg-Test PRIVATE
    ${TESTS_DIR} ${HARNESS_DIR} ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster)
target_link_libraries(ClusterCrossNg-Test PRIVATE
    cluster_harness test_harness data_substrate Catch2::Catch2)
set_property(TARGET ClusterCrossNg-Test PROPERTY CXX_STANDARD 20)
set_property(TARGET ClusterCrossNg-Test PROPERTY CXX_EXTENSIONS OFF)
add_dependencies(ClusterCrossNg-Test txnode)
catch_discover_tests(ClusterCrossNg-Test PROPERTIES LABELS "ClusterCrossNg-Test")
