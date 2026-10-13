cmake_minimum_required(VERSION 3.20)

project(rpc_tutorial VERSION 0.1.0 LANGUAGES CXX)

if(PROJECT_SOURCE_DIR STREQUAL PROJECT_BINARY_DIR)
    message(FATAL_ERROR "Use an out-of-source build: cmake -S . -B build")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include(CTest)
option(RPC_ENABLE_LIVE_TESTS "Run real TCP and ZooKeeper integration tests" OFF)

include(cmake/RpcDependencies.cmake)
include(cmake/RpcTargets.cmake)

# 显式列出生产源码，新增测试或程序入口不会意外进入公共库。
add_library(rpc_common STATIC)
add_library(cookrpc::rpc_common ALIAS rpc_common)
target_sources(rpc_common PRIVATE
    rpc_src/compress_data/zstd_compress.cc
    rpc_src/conn_balancer/load_balancer.cc
    rpc_src/conn_balancer/zk_conn_handler.cc
    rpc_src/core/rpc_client.cc
    rpc_src/core/rpc_options.cc
    rpc_src/core/request_controls.cc
    rpc_src/encrypt/aes_encrypt.cc
    rpc_src/load_config/registry_config.cc
    rpc_src/load_config/rpc_client_config.cc
    rpc_src/load_config/rpc_server_config.cc
    rpc_src/load_config/thread_pool_config.cc
    rpc_src/network/connection.cc
    rpc_src/network/connection_manager.cc
    rpc_src/network/create_socket.cc
    rpc_src/network/message_cycle.cc
    rpc_src/protocol/rpc_protocol.cc
    rpc_src/protocol/transport_codec.cc
    rpc_src/registry/service_registry.cc
    rpc_src/thread_pool/thread_pool.cc
    rpc_src/thread_pool/thread_pool_singleton.cc
    utils/utils_fun.cc
    protos/message.proto
)

target_compile_features(rpc_common PUBLIC cxx_std_17)
target_compile_definitions(rpc_common PUBLIC
    RPC_TUTORIAL_VERSION="${PROJECT_VERSION}"
    THREADED
)
rpc_configure_target(rpc_common)

# 保留当前不带模块目录的头文件引用方式，搜索路径由目标传播。
set(RPC_GENERATED_DIR "${PROJECT_BINARY_DIR}/generated")
target_include_directories(rpc_common PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/compress_data>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/conn_balancer>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/core>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/encrypt>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/load_config>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/network>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/protocol>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/registry>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/serializer>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/service>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/rpc_src/thread_pool>"
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/utils>"
    "$<BUILD_INTERFACE:${RPC_GENERATED_DIR}>"
)

file(MAKE_DIRECTORY "${RPC_GENERATED_DIR}")
protobuf_generate(
    TARGET rpc_common
    LANGUAGE cpp
    PROTOC_OUT_DIR "${RPC_GENERATED_DIR}"
    APPEND_PATH
)

target_link_libraries(rpc_common
    PUBLIC
        Threads::Threads
        protobuf::libprotobuf
        fmt::fmt
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        ${RPC_ZSTD_TARGET}
        ${RPC_ZOOKEEPER_TARGET}
    PRIVATE
        OpenSSL::Crypto
)

rpc_add_executable(rpc_server rpc_src/core/servers_main.cc)
rpc_add_executable(rpc_client rpc_src/core/client_main.cc)
rpc_add_executable(stress_test_client rpc_src/tools/stress_test_client.cc)

if(BUILD_TESTING)
    add_subdirectory(rpc_src/test)
endif()
