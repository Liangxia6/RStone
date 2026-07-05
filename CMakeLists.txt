cmake_minimum_required(VERSION 3.20)

project(RStone VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(RSTONE_BUILD_TESTS "Build RStone tests" ON)

add_library(rstone_core
  src/common/config.cpp
  src/common/serialization.cpp
  src/dashboard/dashboard_server.cpp
  src/dashboard/status_json.cpp
  src/storage/file_kv_engine.cpp
  src/storage/kv_command.cpp
  src/pd/metadata_store.cpp
  src/pd/pd_server.cpp
  src/pd/pd_service.cpp
  src/pd/scheduler.cpp
  src/region/region.cpp
  src/raft/raft_node.cpp
  src/raft/raft_storage.cpp
  src/raft/snapshot.cpp
  src/rpc/in_process_rpc.cpp
  src/rpc/rpc_codec.cpp
  src/rpc/tcp_rpc.cpp
  src/gateway/route_cache.cpp
  src/gateway/gateway_server.cpp
  src/gateway/rpc_gateway_client.cpp
  src/gateway/gateway_service.cpp
  src/client/rstone_client.cpp
  src/store/single_region_cluster.cpp
  src/store/multi_region_cluster.cpp
  src/store/distributed_region_node.cpp
  src/store/store_service.cpp
)

target_include_directories(rstone_core
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(rstone_core
  PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)

add_executable(rstone-server src/main.cpp)
target_link_libraries(rstone-server PRIVATE rstone_core)

add_executable(rstone-cli tools/rstone_cli.cpp)
target_link_libraries(rstone-cli PRIVATE rstone_core)

if(RSTONE_BUILD_TESTS)
  enable_testing()

  add_executable(rstone_unit_tests
    tests/unit/test_main.cpp
    tests/unit/status_test.cpp
    tests/unit/config_test.cpp
    tests/unit/serialization_test.cpp
    tests/unit/status_json_test.cpp
    tests/unit/file_kv_engine_test.cpp
    tests/unit/pd_metadata_store_test.cpp
    tests/unit/pd_server_test.cpp
    tests/unit/pd_service_test.cpp
    tests/unit/scheduler_test.cpp
    tests/unit/raft_node_test.cpp
    tests/unit/raft_storage_test.cpp
    tests/unit/snapshot_test.cpp
    tests/unit/rpc_test.cpp
    tests/unit/rpc_codec_test.cpp
    tests/unit/tcp_rpc_test.cpp
    tests/unit/gateway_server_test.cpp
    tests/unit/multi_region_gateway_test.cpp
    tests/unit/rpc_gateway_client_test.cpp
    tests/unit/gateway_service_test.cpp
    tests/unit/tcp_gateway_e2e_test.cpp
    tests/unit/client_test.cpp
    tests/unit/single_region_cluster_test.cpp
    tests/unit/multi_region_cluster_test.cpp
    tests/unit/store_service_test.cpp
  )
  target_link_libraries(rstone_unit_tests PRIVATE rstone_core)
  add_test(NAME rstone_unit_tests COMMAND rstone_unit_tests)
endif()
