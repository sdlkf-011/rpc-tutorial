# rpc-tutorial

一个基于 C++17 的教学型 RPC 项目，包含客户端、服务端、连接管理、线程池、序列化、压缩、加密和 ZooKeeper 服务注册/发现等模块。

## 功能概览

- `rpc_server`：读取服务端配置并注册服务，接受 RPC 请求。
- `rpc_client`：从 ZooKeeper 发现服务并发送请求。
- `stress_test_client`：用于基础压力测试。
- 公共组件：TCP 网络通信、连接负载均衡、线程池、JSON/Protobuf 序列化、Zstd 压缩和 AES 加密。

## 环境要求

- Linux（代码使用 POSIX socket、`epoll` 和 pthread API）。
- CMake 3.20 或更高版本。
- 支持 C++17 的编译器（GCC 9+ 或 Clang 10+）。
- OpenSSL、Protobuf、fmt、spdlog、nlohmann-json、zstd、ZooKeeper C client 和 GoogleTest（测试需要）。
- 正常运行 RPC 服务需要 ZooKeeper 服务端，默认地址为 `localhost:2181`。

项目支持 vcpkg 和系统包管理器。使用 vcpkg 时可以显式设置 `VCPKG_ROOT`；如果
`vcpkg` 已在 `PATH` 中，构建脚本会自动找到它：

```bash
export VCPKG_ROOT=/path/to/vcpkg
${VCPKG_ROOT}/vcpkg install openssl protobuf fmt spdlog nlohmann-json zstd zookeeper[sync] gtest
```

其中 `zookeeper[sync]` 特性是必须的，因为服务注册使用 ZooKeeper 同步 API。

加密模块使用 OpenSSL AES-256-GCM。生产环境应通过 64 位十六进制字符串环境变量 `COOKRPC_MASTER_KEY_HEX` 提供 32 字节主密钥；未设置时会生成仅在当前进程生命周期内有效的随机密钥。

## 构建

推荐使用仓库提供的脚本。CMake 会使用当前依赖环境中的 `protoc`，并将生成文件放在
构建目录中，不会复用其他环境生成的源码树文件：

```bash
export VCPKG_ROOT=/path/to/vcpkg
./rpc_build.sh
```

也可以不使用 vcpkg，直接让 CMake 从系统前缀查找依赖：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_PREFIX_PATH=/path/to/dependency-prefix
cmake --build build --parallel
```

`rpc_build.sh` 会记录构建时使用的工具链、vcpkg triplet、依赖前缀和编译器。
这些设置变化（例如换电脑或切换 vcpkg）时，它会自动刷新 CMake 缓存，避免继续使用旧的绝对路径。
也可以通过 `BUILD_DIR` 为不同环境使用独立的构建目录。

构建产物位于 `build/`：`rpc_server`、`rpc_client` 和 `stress_test_client`。

## 运行

默认配置文件中的路径是相对于当前工作目录的，因此建议从 `build/` 目录启动程序：

```bash
# 终端 1：先启动 ZooKeeper，再启动服务端
cd build
./rpc_server ../config/rpc_server.json

# 终端 2：启动客户端
cd build
./rpc_client ../config/rpc_client.json
```

配置示例位于 `config/`：

- `rpc_server.json`：监听地址、端口、线程池、负载均衡和服务注册配置。
- `rpc_client.json`：ZooKeeper 地址、命名空间、服务端口和重试参数。
- `servers.json`：服务名、版本和注册节点列表。

运行前请根据部署环境修改 ZooKeeper 地址、监听地址和端口。

## 测试

开启 CTest 后构建 GoogleTest 测试目标：

```bash
cmake -S . -B build -DBUILD_TESTING=ON \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

当前测试覆盖负载均衡、序列化、RPC 协议、服务管理、线程池背压、AES 加密和 Zstd 压缩。

## 压力测试

压力测试使用多个独立长连接并发调用 `RpcService.Echo`，输出成功率、吞吐量以及平均、P50、P95、P99 和最大延迟。运行前需要启动 ZooKeeper，并确保客户端配置对应的服务已经注册：

```bash
./stress_test.sh --threads 8 --requests 50000 --payload-size 256
```

如果需要由脚本临时启动并在结束时关闭本地服务端，可以使用：

```bash
./stress_test.sh --start-server --threads 8 --requests 50000
```

使用 `--build` 可在压测前重新构建。执行 `./stress_test.sh --help` 可以查看所有参数及对应的环境变量。

## 头文件引用规则

项目内部头文件统一使用不带目录的形式，例如：

```cpp
#include "error_code.h"
```

`#include "core/error_code.h"` 把源码目录结构暴露给调用方，项目现在不再使用这种写法。CMake 在 `target_include_directories(rpc_common)` 中加入了 `rpc_src/core`、`rpc_src/network`、`rpc_src/protocol` 等模块目录，并将这些目录传递给链接 `rpc_common` 的目标。

编译器遇到双引号包含时，会先查找当前源文件目录，然后按编译命令中的 `-I`（CMake 的 include directory）顺序查找。因为编译命令包含：

```text
-I<项目>/rpc_src/core
-I<项目>/rpc_src/network
...
```

所以它能在 `rpc_src/core/error_code.h` 找到 `error_code.h`。这不是编译器自动知道项目结构，而是构建系统明确配置了头文件搜索路径。新增模块目录时，也应同步加入 `target_include_directories`，或采用项目中一致的 CMake 目录管理方式。

第三方库仍使用各自的标准写法，例如 `<nlohmann/json.hpp>`；Protobuf 生成文件中的 `google/protobuf/...` 引用也不应改成项目内部头文件名。

## 目录结构

```text
rpc_src/       RPC 核心源码，按网络、协议、序列化等模块划分
utils/         日志和通用工具
protos/        Protobuf 定义及生成代码
config/        服务端、客户端和注册中心配置示例
docs/          补充文档
```

## 开发约定

- 使用 C++17，提交前运行 `git diff --check`。
- 修改 `.proto` 后重新配置并构建；CMake 会自动重新生成对应的 C++ 文件。
- 如需单独生成源码，可运行 `./protos/compile_proto.sh`（输出到 `protos/`）。
- 新增内部头文件引用时使用 `#include "file.h"`，并确保对应目录在 CMake 的 include path 中。
- 配置文件中的敏感地址和部署参数不要提交为生产环境值。
