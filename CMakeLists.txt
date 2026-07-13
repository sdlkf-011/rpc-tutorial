cmake_minimum_required(VERSION 3.20)

project(rpc_tutorial VERSION 0.1.0 LANGUAGES CXX)

# 使用 C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 查找依赖库
find_package(Threads REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)

# 收集公共源码
file(GLOB_RECURSE COMMON_SOURCES CONFIGURE_DEPENDS
  "${PROJECT_SOURCE_DIR}/rpc_src/*.cpp"
  "${PROJECT_SOURCE_DIR}/rpc_src/*.cc"
  "${PROJECT_SOURCE_DIR}/utils/*.cpp"
  "${PROJECT_SOURCE_DIR}/utils/*.cc"
)

# 排除三个程序入口
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/server_main\\.(cc|cpp)$")
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/client_main\\.(cc|cpp)$")
list(FILTER COMMON_SOURCES EXCLUDE REGEX ".*/stress_test_client\\.(cc|cpp)$")

# 添加 protobuf 生成代码
list(APPEND COMMON_SOURCES
  "${PROJECT_SOURCE_DIR}/protos/message.pb.cc"
)

# 创建公共静态库
add_library(rpc_common STATIC ${COMMON_SOURCES})

# 配置头文件搜索路径
target_include_directories(rpc_common
  PUBLIC
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/rpc_src
    ${PROJECT_SOURCE_DIR}/protos
    ${PROJECT_SOURCE_DIR}/utils
)

# 链接公共依赖
target_link_libraries(rpc_common
  PUBLIC
    Threads::Threads
    protobuf::libprotobuf
    fmt::fmt
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    zstd::libzstd
)

# 定义工程版本宏
target_compile_definitions(rpc_common
  PUBLIC
    RPC_TUTORIAL_VERSION="${PROJECT_VERSION}"
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
