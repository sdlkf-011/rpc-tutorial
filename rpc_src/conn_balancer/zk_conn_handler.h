#ifndef RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ZK_CONN_HANDLER_H_
#define RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ZK_CONN_HANDLER_H_

#include <memory>
#include <mutex>
#include <atomic>
#include <chrono>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <vector>

#ifdef __cplusplus
extern "C"
{
  #endif
  
  #include <zookeeper/zookeeper.h>
  #include <zookeeper/zookeeper_version.h>
  #include <zookeeper/proto.h>
  
  #ifdef __cplusplus
}
#endif

#include "service_registry.h"

// 前置声明
class LoadBalancer;

namespace cookrpc {

// 前置声明
class ServiceRegistryConfig;

// ZooKeeper 连接处理器
class ZkConnHandler {
 public:
  // 获取单例实例 
  static ZkConnHandler& GetInstance() {
    static ZkConnHandler instance;
    return instance;
  }

  ZkConnHandler(const ZkConnHandler&) = delete;
  ZkConnHandler& operator=(const ZkConnHandler&) = delete;
  ZkConnHandler(ZkConnHandler&&) = delete;
  ZkConnHandler& operator=(ZkConnHandler&&) = delete;

  // 初始化 ZooKeeper 连接处理器
  bool InitZkConnHandler(const nlohmann::json& config);

  // 获取指定命名空间下的所有服务器列表
  std::vector<std::string> GetAllServers(const std::string& zk_namespace);

  // 获取单个服务器地址
  std::string GetServer(const std::string& zk_namespace);

  // 注册单个服务
  bool RegisterService(const std::string& service_name,
                       const std::string& service_address);
  
  // 从配置文件批量注册服务
  bool ReisterServicesFromConfig(const cookrpc::ServiceRegistryConfig* registry_config);

  // 获取 ServiceRegistry 实例
  std::shared_ptr<ServiceRegistry> GetServiceRegistry() {
    std::lock_guard<std::mutex> lock(mutex_);
    return service_registry_;
  }

  std::shared_ptr<const ServiceRegistry> GetServiceRegistry() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return service_registry_;
  }

  bool HasServiceRegistry() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return service_registry_ != nullptr;
  }

  // 获取或创建 ServiceRegistry 实例
  std::shared_ptr<ServiceRegistry> GetOrCreateServiceRegistry();

  // 配置设置
  void SetZkHost(const std::string& host);
  void SetZkPort(int port);
  void SetZkRetryInterval(int seconds);
  void SetZkNamespace(const std::string& zk_namespace);

  // 确保 ZooKeeper 连接可用
  bool EnsureZkConnection();

  // 从 ZooKeeper 更新服务器列表
  void UpdateServersFromZk(const std::string& zk_namespace);

  // 清除资源
  void CleanUp();

  ~ZkConnHandler();
 private:
  ZkConnHandler()
    : running_(false), zk_client_(nullptr), service_registry_(nullptr) {}

  // 全局 Watcher 回调函数
  static void GlobalWatcher(zhandle_t* zh, int type, int state,
                            const char* path, void* watcherCtx);

  // 创建 ServiceRegistry 实例
  bool CreateServiceRegistryIfNeeded();

  // 初始化 ZooKeeper 客户端
  bool InitZkClient();

  std::atomic<bool> running_; // 运行状态
  std::chrono::seconds retry_interval_; // 重试间隔

  mutable std::mutex mutex_; // 保护成员变量和 ZooKeeper 客户端生命周期
  std::mutex servers_mutex_; // 保护服务器列表
  std::vector<std::string> servers_; // 缓存的服务器列表

  zhandle_t* zk_client_; // Zookeeper 服务端句柄

  // 参数配置
  std::string zk_host_; // Zookeeper 主机地址
  int zk_port_; // Zookeeper 端口
  std::string zk_namespace_; // 命名空间

  std::shared_ptr<ServiceRegistry> service_registry_; // ServiceRegistry实例
};
} // namespace cookrpc

#endif // RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ZK_CONN_HANDLER_H_
