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

每个 `RpcClient` 独立拥有 ZooKeeper 发现会话，可以同时连接不同集群。
重载配置时先解析并校验，再连接新端点，成功后一起提交配置和会话；失败会保留
原配置和会话。服务端入口仍可使用 `ZkConnHandler::GetInstance()`。
旧会话由正在查询的 `shared_ptr` 快照持有，最后一个查询释放后才关闭；各会话的
watcher 只更新自身状态。选址直接使用本次查询返回的列表，不共享命名空间缓存。

`SetZkHost` 等 setter 仅设置首次连接或清理后重连使用的待连接配置；
已连接会话应通过 `InitZkConnHandler` 切换配置。`zk_retry_interval` 必须为正整数，
建连最多等待三个该间隔，认证失败或会话过期会提前结束等待。

## 测试

开启 CTest 后构建 GoogleTest 测试目标：

```bash
cmake -S . -B build -DBUILD_TESTING=ON \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

当前测试覆盖负载均衡、序列化、RPC 协议、服务管理、线程池背压、AES 加密和 Zstd 压缩。
Linux 下的 `zk_conn_handler_test` 用链接器替换 ZooKeeper C API，覆盖配置切换与失败回退、
旧查询及 watcher 生命周期、并发命名空间隔离和客户端独立会话，无需启动外部集群。

线程池单例的 `GetInstance()` 返回 `shared_ptr<ThreadPool>`，调用方必须持有句柄；
销毁期间返回空句柄，单例提交返回无效 future，初始化返回 `false`，统计接口仍可
读取旧池。`Destroy()` 在锁外排空并回收旧池，worker 或并发销毁者调用时返回 `false`。

`RequestShutdown()` 取消暂停、拒绝新任务并排空已接收任务；`WaitForTermination(timeout)`
等待退出，有限超时返回 `false` 并保留任务和资源，后续等待或析构完成回收。
`ShutDown(timeout)` 合并这两步，外部线程调用 `Stop()` 或析构时采用无限等待的排空语义。
worker 同步等待自身退出会返回 `false`。服务端先按配置限时等待，超时后记录日志并
继续等待在途任务结束，再销毁线程池和 ZooKeeper 会话；退出时间可能超过配置超时。

`thread_pool_test` 覆盖暂停后关闭、限时与超大期限等待、异常 future 和失败统计、
并发关闭及提交、反复扩缩容回收、单例销毁中的统计访问和持有句柄的生命周期。
`Stats::worker_handles` 包括已退出但未 join 和正在回收的 worker，用于检查回收上限。

服务端的 `Connection::Write` 成功表示完整响应帧已入队。事件线程负责 `send`、
短写偏移和连接关闭；发送遇到 EAGAIN 或耗尽每轮 64 KiB 预算后订阅 EPOLLOUT，
队列排空后取消写事件，macOS 使用对应的 EVFILT_WRITE。每个连接最多保留
64 帧、约 4 MiB 待发帧，达到任一上限就拒绝并排队关闭慢连接。通知和事件携带
连接标识，旧 fd 的迟到操作不会作用于复用该 fd 的连接。
客户端使用 `WriteWithTimeout` 同步发送，所有短写和 poll 重试共用一个单调时钟期限。
读取结果分别报告数据、EOF、错误和超时；先解析 EOF 前的数据，完整响应成功返回，
半帧关闭返回失败并记录连接关闭错误。

协议固定头部为 12 字节，body（含字段长度与错误码）最大 1 MiB。序列化与接收使用
同一限制，所有响应发送入口将超限结果或错误文本转换为保留序列号、清空载荷的短错误帧。
事件循环停止后停止接收新请求，最多用 3 秒刷新已接收任务的响应；到期关闭连接。
随后服务端继续等待线程池业务任务完成，保证任务所需对象仍然存活。

ZooKeeper 注册只把持久父路径或本会话、数据匹配的临时实例视为已注册。
其他会话占有实例时用 watch 等待删除并重新竞争创建，默认所有权期限 3 秒，
`RegisterService` 可指定 1 毫秒到 60 秒的期限（包括等待其他注册操作的锁）。
持久实例节点、临时父节点及本会话数据不匹配的实例均返回冲突失败。
会话过期后独立恢复线程重连并重注册已成功登记的服务；watcher 只更新状态并通知等待者。
实现依据 [epoll 文档](https://man7.org/linux/man-pages/man7/epoll.7.html)、
[poll 文档](https://man7.org/linux/man-pages/man2/poll.2.html) 和
[ZooKeeper 会话与 watch 说明](https://zookeeper.apache.org/doc/r3.9.2/zookeeperProgrammers.html)。

`network_test` 使用真实本地套接字测试慢读隔离、短写、发送期限、EOF、fd 复用和协议边界；
`service_registry_test` 通过 C API 替换确定性覆盖所有权、watch 竞争及过期恢复。

`RpcClientConfig` 的六个 setter 校验输入，非法值抛出 `invalid_argument` 并保留旧值。
文件加载先完整解析临时配置，全部字段合法后提交；失败保留全部旧配置。
两个端口的范围均为 1–65535，重试次数为 0–100，超时必须为正整数。
协议原有 5 项和序列化原有 6 项测试均已注册到 CTest。

压测脚本在切换目录前按调用者工作目录解析配置、构建目录及日志路径，支持含空格的路径：

```bash
./stress_test.sh --build-dir './out build' --client-config './my configs/client.json' \
  --server-config './my configs/server.json' --server-log './test logs/server.log' --start-server
```

`rpc_build.sh` 按 `VCPKG_TARGET_TRIPLET`、`VCPKG_DEFAULT_TRIPLET`、宿主默认值的优先级
选择 triplet，并将同一值传给 CMake 和环境签名；值变化时清除旧 CMake 缓存后重新配置。
这遵循 [vcpkg 的缓存要求](https://learn.microsoft.com/en-us/vcpkg/users/buildsystems/cmake-integration)。

启用可重复的真实环境验收：

```bash
cmake -S . -B build -DRPC_ENABLE_LIVE_TESTS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

`live_tcp_zookeeper` 需要 Java 和 ZooKeeper jars；缺失时显示跳过，可通过
`ZK_SERVER_CLASSPATH` 指定安装位置。它临时启动真实 ZooKeeper 和 RPC 事件循环，
验证慢读背压、真实服务发现及调用、隔离注册连接导致的服务端会话过期和自动重注册、
ZooKeeper 重启以及 TCP 服务重启后的可发现性，结束后回收测试进程。
可用 `python3 rpc_src/test/live_test.py --driver build/live_integration_test --artifacts .cache/live-test`
保留日志。ASan/UBSan 与 TSan 使用独立构建目录分别运行；在遇到 TSan 地址映射冲突的
Linux 环境中，该脚本的 `--no-aslr` 选项仅为驱动测试进程关闭 ASLR。

真实重启验收发现本机 ZooKeeper C client 3.8.5 的 `format_endpoint_info` 共享静态
日志缓冲区竞态，对应 [上游 ZOOKEEPER-4810 修复](https://github.com/apache/zookeeper/pull/2140)。
项目 C++ 测试的 TSan 不需要屏蔽该报告。旧依赖的真实重启测试应使用包含该修复的 C 库；
也可用 `prepare_zookeeper_validation.py --source <vcpkg-patched-zookeeper-client-c> --output .cache/zk-fixed`
在独立目录复制、应用上游 TLS 修复并构建，再用输出的 `unofficial-zookeeper_DIR`
配置 TSan 构建进行复验。该流程不修改已安装 SDK，也不抑制 TSan 报告。

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
