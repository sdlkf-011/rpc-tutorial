#ifndef RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ZK_CONN_HANDLER_H_
#define RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ZK_CONN_HANDLER_H_

#include <memory>
#include <mutex>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

#include <zookeeper/proto.h>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper_version.h>

#ifdef __cplusplus
}
#endif

#include "service_registry.h"

namespace cookrpc {

// 前置声明
class ServiceRegistryConfig;

// ZooKeeper 连接处理器
class ZkConnHandler {
 public:
  // 客户端独占发现器；服务端入口仍可使用下面的单例
  ZkConnHandler() = default;

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
  bool ReisterServicesFromConfig(
      const cookrpc::ServiceRegistryConfig* registry_config);

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

  // 设置待连接配置；EnsureZkConnection 首次连接时使用
  // 已连接会话的配置切换必须通过 InitZkConnHandler 事务提交
  void SetZkHost(const std::string& host);
  void SetZkPort(int port);
  void SetZkRetryInterval(int seconds);
  void SetZkNamespace(const std::string& zk_namespace);

  // 确保 ZooKeeper 连接可用
  bool EnsureZkConnection();

  // 清除资源
  void CleanUp();

  ~ZkConnHandler();

 private:
  struct Configuration {
    std::string host = "localhost";
    int port = 2181;
    std::string zk_namespace = "/cookrpc";
    int retry_interval_seconds = 5;

    std::string Endpoint() const;
    bool IsValid() const;
  };

  // 句柄和 watcher 上下文一起存活，最后一个查询释放快照后关闭
  struct Session;

  // 全局 Watcher 回调函数
  static void GlobalWatcher(zhandle_t* zh, int type, int state,
                            const char* path, void* watcherCtx);

  static std::shared_ptr<Session> CreateSession(const Configuration& config);
  bool CommitConfiguration(Configuration config);
  std::shared_ptr<Session> GetConnectedSession();

  mutable std::mutex mutex_;  // 保护配置、会话和注册器快照
  std::mutex update_mutex_;   // 串行化配置事务、惰性创建和清理
  Configuration config_;
  Configuration pending_config_;
  std::shared_ptr<Session> session_;

  std::shared_ptr<ServiceRegistry> service_registry_;  // ServiceRegistry实例
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ZK_CONN_HANDLER_H_
