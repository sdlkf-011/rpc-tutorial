cmake_minimum_required(VERSION 3.20)

project(rpc_tutorial VERSION 0.1.0 LANGUAGES CXX)

# 使用 C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 查找依赖库
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

# 优先使用包管理器提供的配置文件，同时兼容系统安装的 CMake 查找模块
find_package(Protobuf CONFIG QUIET)
if(NOT TARGET protobuf::libprotobuf)
  find_package(Protobuf MODULE REQUIRED)
endif()

find_package(fmt CONFIG QUIET)
if(NOT TARGET fmt::fmt)
  find_package(fmt REQUIRED)
endif()

find_package(spdlog CONFIG QUIET)
if(NOT TARGET spdlog::spdlog)
  find_package(spdlog REQUIRED)
endif()

find_package(nlohmann_json CONFIG QUIET)
if(NOT TARGET nlohmann_json::nlohmann_json)
  find_package(nlohmann_json REQUIRED)
endif()

find_package(zstd CONFIG QUIET)
if(TARGET zstd::libzstd)
  set(RPC_ZSTD_TARGET zstd::libzstd)
elseif(TARGET zstd::libzstd_shared)
  set(RPC_ZSTD_TARGET zstd::libzstd_shared)
elseif(TARGET zstd::libzstd_static)
  set(RPC_ZSTD_TARGET zstd::libzstd_static)
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd)
  set(RPC_ZSTD_TARGET PkgConfig::ZSTD)
endif()

find_package(unofficial-zookeeper CONFIG QUIET)
if(TARGET unofficial::zookeeper::zookeeper)
  set(RPC_ZOOKEEPER_TARGET unofficial::zookeeper::zookeeper)
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(ZOOKEEPER QUIET IMPORTED_TARGET zookeeper-mt)
  if(NOT TARGET PkgConfig::ZOOKEEPER)
    pkg_check_modules(ZOOKEEPER QUIET IMPORTED_TARGET zookeeper_mt)
  endif()
  if(TARGET PkgConfig::ZOOKEEPER)
    set(RPC_ZOOKEEPER_TARGET PkgConfig::ZOOKEEPER)
  else()
    find_path(ZOOKEEPER_INCLUDE_DIR NAMES zookeeper/zookeeper.h)
    find_library(ZOOKEEPER_LIBRARY NAMES zookeeper_mt zookeeper-mt zookeeper)
    if(NOT ZOOKEEPER_INCLUDE_DIR OR NOT ZOOKEEPER_LIBRARY)
      message(FATAL_ERROR
        "ZooKeeper C client was not found; install zookeeper[sync] or the system development package")
    endif()
    add_library(rpc_zookeeper INTERFACE)
    target_include_directories(rpc_zookeeper INTERFACE "${ZOOKEEPER_INCLUDE_DIR}")
    target_link_libraries(rpc_zookeeper INTERFACE "${ZOOKEEPER_LIBRARY}")
    set(RPC_ZOOKEEPER_TARGET rpc_zookeeper)
  endif()
endif()

# ServiceRegistry 使用 ZooKeeper 同步 API；通过 vcpkg 安装时需要启用
# zookeeper[sync] 特性
include(CheckCXXSourceCompiles)
set(CMAKE_REQUIRED_DEFINITIONS -DTHREADED)
set(CMAKE_REQUIRED_LIBRARIES ${RPC_ZOOKEEPER_TARGET})
if(DEFINED ZOOKEEPER_INCLUDE_DIRS)
  set(CMAKE_REQUIRED_INCLUDES ${ZOOKEEPER_INCLUDE_DIRS})
elseif(DEFINED ZOOKEEPER_INCLUDE_DIR)
  set(CMAKE_REQUIRED_INCLUDES ${ZOOKEEPER_INCLUDE_DIR})
endif()
check_cxx_source_compiles([[
  #include <zookeeper/zookeeper.h>
  int main() {
    return zoo_exists(nullptr, "/", 0, nullptr);
  }
]] ZOOKEEPER_HAS_SYNC_API)
unset(CMAKE_REQUIRED_DEFINITIONS)
unset(CMAKE_REQUIRED_LIBRARIES)
unset(CMAKE_REQUIRED_INCLUDES)
if(NOT ZOOKEEPER_HAS_SYNC_API)
  message(FATAL_ERROR
    "ZooKeeper synchronous API is required; install zookeeper[sync]")
endif()

# 收集公共源码
file(GLOB_RECURSE COMMON_SOURCES CONFIGURE_DEPENDS
  "${PROJECT_SOURCE_DIR}/rpc_src/*.cpp"
  "${PROJECT_SOURCE_DIR}/rpc_src/*.cc"
  "${PROJECT_SOURCE_DIR}/utils/*.cpp"
  "${PROJECT_SOURCE_DIR}/utils/*.cc"
)

# 排除程序入口和测试源码
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/servers_main\\.(cc|cpp)$")
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/client_main\\.(cc|cpp)$")
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/stress_test_client\\.(cc|cpp)$")
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/rpc_src/test/.*\\.(cc|cpp)$")
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/load_balancer_test\\.(cc|cpp)$")
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/zstd_compress_test\\.(cc|cpp)$")
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/aes_encrypt_test\\.(cc|cpp)$")

# 将协议定义文件加入构建输入；生成文件保留在构建目录，避免不同环境在源码目录留下过期文件。
list(APPEND COMMON_SOURCES "${PROJECT_SOURCE_DIR}/protos/message.proto")

# 创建公共静态库
add_library(rpc_common STATIC ${COMMON_SOURCES})

# 配置头文件搜索路径
target_include_directories(rpc_common
  PUBLIC
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/rpc_src
    ${PROJECT_SOURCE_DIR}/rpc_src/compress_data
    ${PROJECT_SOURCE_DIR}/rpc_src/conn_balancer
    ${PROJECT_SOURCE_DIR}/rpc_src/core
    ${PROJECT_SOURCE_DIR}/rpc_src/encrypt
    ${PROJECT_SOURCE_DIR}/rpc_src/load_config
    ${PROJECT_SOURCE_DIR}/rpc_src/network
    ${PROJECT_SOURCE_DIR}/rpc_src/protocol
    ${PROJECT_SOURCE_DIR}/rpc_src/registry
    ${PROJECT_SOURCE_DIR}/rpc_src/serializer
    ${PROJECT_SOURCE_DIR}/rpc_src/service
    ${PROJECT_SOURCE_DIR}/rpc_src/thread_pool
    ${CMAKE_CURRENT_BINARY_DIR}/generated
    ${PROJECT_SOURCE_DIR}/utils
)

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/generated")
protobuf_generate(
  TARGET rpc_common
  LANGUAGE cpp
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
  APPEND_PATH
)

# 链接公共依赖
target_link_libraries(rpc_common
  PUBLIC
    Threads::Threads
    OpenSSL::Crypto
    protobuf::libprotobuf
    fmt::fmt
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    ${RPC_ZSTD_TARGET}
    ${RPC_ZOOKEEPER_TARGET}
)

# 定义工程版本宏
target_compile_definitions(rpc_common
  PUBLIC
    RPC_TUTORIAL_VERSION="${PROJECT_VERSION}"
    THREADED
)

# 开启编译警告
function(enable_project_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4 /permissive-)
  else()
    target_compile_options(${target} PRIVATE
      -Wall
      -Wextra
      -Wpedantic
    )
  endif()
endfunction()

enable_project_warnings(rpc_common)

# 服务端程序
add_executable(rpc_server
  rpc_src/core/servers_main.cc
)

target_link_libraries(rpc_server
  PRIVATE
    rpc_common
)

enable_project_warnings(rpc_server)

# 客户端程序
add_executable(rpc_client
  rpc_src/core/client_main.cc
)

target_link_libraries(rpc_client
  PRIVATE
    rpc_common
)

enable_project_warnings(rpc_client)

# 压测程序
add_executable(stress_test_client
  rpc_src/tools/stress_test_client.cc
)

target_link_libraries(stress_test_client
  PRIVATE
    rpc_common
)

enable_project_warnings(stress_test_client)

# GoogleTest 测试仅在 BUILD_TESTING 开启时参与构建。
include(CTest)

if(BUILD_TESTING)
  find_package(GTest REQUIRED)

  add_executable(load_balancer_test
    rpc_src/test/load_balancer_test.cc
  )

  add_executable(serializer_test
    rpc_src/test/serializer_test.cc
  )

  add_executable(rpc_protocol_test
    rpc_src/test/rpc_protocol_test.cc
  )

  add_executable(service_test
    rpc_src/test/service_test.cc
  )

  add_executable(thread_pool_test
    rpc_src/test/thread_pool_test.cc
  )

  target_link_libraries(load_balancer_test
    PRIVATE
      rpc_common
      GTest::gtest_main
  )

  target_link_libraries(serializer_test
    PRIVATE
      rpc_common
      GTest::gtest_main
  )

  target_link_libraries(rpc_protocol_test
    PRIVATE
      rpc_common
      GTest::gtest_main
  )

  target_link_libraries(service_test
    PRIVATE
      rpc_common
      GTest::gtest_main
  )

  target_link_libraries(thread_pool_test
    PRIVATE
      rpc_common
      GTest::gtest_main
  )

  enable_project_warnings(load_balancer_test)
  enable_project_warnings(service_test)
  enable_project_warnings(thread_pool_test)

  add_executable(aes_encrypt_test
    rpc_src/test/aes_encrypt_test.cc
  )

  target_link_libraries(aes_encrypt_test
    PRIVATE
      rpc_common
      GTest::gtest_main
  )

  enable_project_warnings(aes_encrypt_test)

  include(GoogleTest)
  gtest_discover_tests(load_balancer_test)
  gtest_discover_tests(rpc_protocol_test)
  gtest_discover_tests(serializer_test)

  add_executable(rpc_client_config_test rpc_src/test/rpc_client_config_test.cc)
  target_link_libraries(rpc_client_config_test PRIVATE rpc_common GTest::gtest_main)
  enable_project_warnings(rpc_client_config_test)
  gtest_discover_tests(rpc_client_config_test)

  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  add_test(NAME scripts_regression
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/rpc_src/test/scripts_test.py")
  option(RPC_ENABLE_LIVE_TESTS "Run TCP/ZooKeeper expiration and restart integration tests" OFF)
  if(RPC_ENABLE_LIVE_TESTS AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(live_integration_test rpc_src/test/live_integration_test.cc)
    target_link_libraries(live_integration_test PRIVATE rpc_common)
    enable_project_warnings(live_integration_test)
    add_test(NAME live_tcp_zookeeper
      COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/rpc_src/test/live_test.py"
        --driver "$<TARGET_FILE:live_integration_test>")
    set_tests_properties(live_tcp_zookeeper PROPERTIES TIMEOUT 120 SKIP_RETURN_CODE 77)
  endif()

  # Linux 链接器替换 ZooKeeper C API，确定性验证真实代码的会话交错，
  # 无需运行外部集群。其他平台仍构建常规测试
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(network_test rpc_src/test/network_test.cc)
    target_link_libraries(network_test PRIVATE rpc_common GTest::gtest_main)
    target_link_options(network_test PRIVATE
      "-Wl,--wrap=zookeeper_init" "-Wl,--wrap=zookeeper_close"
      "-Wl,--wrap=zoo_get_children" "-Wl,--wrap=zoo_get"
      "-Wl,--wrap=deallocate_String_vector")
    enable_project_warnings(network_test)
    gtest_discover_tests(network_test PROPERTIES TIMEOUT 30)

    add_executable(service_registry_test rpc_src/test/service_registry_test.cc)
    target_link_libraries(service_registry_test PRIVATE rpc_common GTest::gtest_main)
    target_link_options(service_registry_test PRIVATE
      "-Wl,--wrap=zookeeper_init" "-Wl,--wrap=zookeeper_close"
      "-Wl,--wrap=zoo_exists" "-Wl,--wrap=zoo_wexists"
      "-Wl,--wrap=zoo_get" "-Wl,--wrap=zoo_create" "-Wl,--wrap=zoo_client_id")
    enable_project_warnings(service_registry_test)
    gtest_discover_tests(service_registry_test PROPERTIES TIMEOUT 30)

    add_executable(zk_conn_handler_test rpc_src/test/zk_conn_handler_test.cc)
    target_link_libraries(zk_conn_handler_test PRIVATE rpc_common GTest::gtest_main)
    target_link_options(zk_conn_handler_test PRIVATE
      "-Wl,--wrap=zookeeper_init"
      "-Wl,--wrap=zookeeper_close"
      "-Wl,--wrap=zoo_get_children"
      "-Wl,--wrap=zoo_get"
      "-Wl,--wrap=deallocate_String_vector"
      "-Wl,--wrap=zoo_exists"
      "-Wl,--wrap=zoo_create"
    )
    enable_project_warnings(zk_conn_handler_test)
    gtest_discover_tests(zk_conn_handler_test PROPERTIES TIMEOUT 30)
  endif()

  add_executable(zstd_compress_test
    rpc_src/test/zstd_compress_test.cc
  )

  target_link_libraries(zstd_compress_test
    PRIVATE
      rpc_common
      GTest::gtest_main
  )

  enable_project_warnings(zstd_compress_test)

  gtest_discover_tests(zstd_compress_test)
  gtest_discover_tests(aes_encrypt_test)
  gtest_discover_tests(service_test)
  gtest_discover_tests(thread_pool_test PROPERTIES TIMEOUT 30)
endif()
