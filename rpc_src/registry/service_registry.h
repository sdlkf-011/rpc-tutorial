#ifndef RPC_TUTORIAL_RPC_SRC_REGISTRY_SERVICE_REGISTRY_H_
#define RPC_TUTORIAL_RPC_SRC_REGISTRY_SERVICE_REGISTRY_H_

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>

#include <zookeeper/zookeeper.h>

namespace cookrpc {

// 基于 ZooKeeper 的服务注册中心。
//
// 持久节点表示服务，临时子节点表示正在运行的服务实例。
// 会话结束时，ZooKeeper 会自动删除该会话创建的临时节点。
class ServiceRegistry {
 public:
  explicit ServiceRegistry(const std::string& zk_hosts);
  ~ServiceRegistry();

  ServiceRegistry(const ServiceRegistry&) = delete;
  ServiceRegistry& operator=(const ServiceRegistry&) = delete;
  ServiceRegistry(ServiceRegistry&&) = delete;
  ServiceRegistry& operator=(ServiceRegistry&&) = delete;

  // 将 service_address 注册到 service_name 对应的节点下。
  // 未连接或 ZooKeeper 拒绝操作时返回 false。
  bool RegisterService(const std::string& service_name,
                       const std::string& service_address);

  bool IsConnected() const;

 private:
  static void GlobalWatcher(zhandle_t* handle, int type, int state,
                            const char* path, void* watcher_context);

  bool CreateNode(const std::string& path, const std::string& data, int flags);
  bool EnsurePath(const std::string& path);

  inline static constexpr char kRootPath[] = "/cookrpc";
  inline static constexpr int kSessionTimeoutMs = 30000;
  inline static constexpr int kConnectionTimeoutMs = 5000;

  zhandle_t* zk_handle_ = nullptr;
  std::atomic<bool> is_connected_{false};
  mutable std::mutex connection_mutex_;
  std::condition_variable connection_cv_;
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_REGISTRY_SERVICE_REGISTRY_H_
