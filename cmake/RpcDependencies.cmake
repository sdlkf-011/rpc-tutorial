include_guard()

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

# 优先使用依赖自身导出的目标；兼容系统 Protobuf 的 Find 模块。
find_package(Protobuf CONFIG QUIET)
if(NOT TARGET protobuf::libprotobuf)
    find_package(Protobuf MODULE REQUIRED)
endif()
if(NOT COMMAND protobuf_generate)
    include(FindProtobuf)
endif()
if(NOT TARGET protobuf::protoc OR NOT COMMAND protobuf_generate)
    message(FATAL_ERROR "Protobuf code generation requires protoc and protobuf_generate()")
endif()

find_package(fmt CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

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

# ZooKeeper 可由包配置、pkg-config 或显式头文件/库路径提供。
find_package(unofficial-zookeeper CONFIG QUIET)
if(TARGET unofficial::zookeeper::zookeeper)
    set(RPC_ZOOKEEPER_TARGET unofficial::zookeeper::zookeeper)
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(ZOOKEEPER QUIET IMPORTED_TARGET zookeeper-mt)
        if(NOT TARGET PkgConfig::ZOOKEEPER)
            pkg_check_modules(ZOOKEEPER QUIET IMPORTED_TARGET zookeeper_mt)
        endif()
    endif()
    if(TARGET PkgConfig::ZOOKEEPER)
        set(RPC_ZOOKEEPER_TARGET PkgConfig::ZOOKEEPER)
    else()
        find_path(ZOOKEEPER_INCLUDE_DIR NAMES zookeeper/zookeeper.h)
        find_library(ZOOKEEPER_LIBRARY NAMES zookeeper_mt zookeeper-mt zookeeper)
        if(NOT ZOOKEEPER_INCLUDE_DIR OR NOT ZOOKEEPER_LIBRARY)
            message(FATAL_ERROR
                "ZooKeeper C client was not found; install zookeeper[sync] or a system development package")
        endif()

        # IMPORTED 目标也可用于 try_compile，避免被解释成不存在的 -l 名称。
        add_library(rpc_zookeeper UNKNOWN IMPORTED)
        set_target_properties(rpc_zookeeper PROPERTIES
            IMPORTED_LOCATION "${ZOOKEEPER_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${ZOOKEEPER_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES Threads::Threads
        )
        get_filename_component(_rpc_zookeeper_lib_dir "${ZOOKEEPER_LIBRARY}" DIRECTORY)
        find_library(ZOOKEEPER_HASHTABLE_LIBRARY
            NAMES zookeeper_hashtable
            HINTS "${_rpc_zookeeper_lib_dir}"
            NO_DEFAULT_PATH
        )
        if(ZOOKEEPER_HASHTABLE_LIBRARY)
            set_property(TARGET rpc_zookeeper APPEND PROPERTY
                INTERFACE_LINK_LIBRARIES "${ZOOKEEPER_HASHTABLE_LIBRARY}")
        endif()
        mark_as_advanced(ZOOKEEPER_INCLUDE_DIR ZOOKEEPER_LIBRARY ZOOKEEPER_HASHTABLE_LIBRARY)
        set(RPC_ZOOKEEPER_TARGET rpc_zookeeper)
    endif()
endif()

# 服务发现仍使用同步 API。每次配置重新检查选中的 SDK，避免复用其他依赖的结果。
include(CMakePushCheckState)
include(CheckCXXSourceCompiles)
cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_DEFINITIONS -DTHREADED)
set(CMAKE_REQUIRED_LIBRARIES "${RPC_ZOOKEEPER_TARGET}" Threads::Threads)
unset(ZOOKEEPER_HAS_SYNC_API CACHE)
check_cxx_source_compiles([[
    #include <zookeeper/zookeeper.h>
    int main() { return zoo_exists(nullptr, "/", 0, nullptr); }
]] ZOOKEEPER_HAS_SYNC_API)
cmake_pop_check_state()
if(NOT ZOOKEEPER_HAS_SYNC_API)
    message(FATAL_ERROR "ZooKeeper synchronous API is required; install zookeeper[sync]")
endif()
