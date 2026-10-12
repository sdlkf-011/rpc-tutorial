#ifndef RPC_TUTORIAL_RPC_SRC_REGISTRY_SERVICE_REGISTRY_H_
#define RPC_TUTORIAL_RPC_SRC_REGISTRY_SERVICE_REGISTRY_H_

#include <zookeeper/zookeeper.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

namespace cookrpc {

class ServiceRegistry {
 public:
  explicit ServiceRegistry(const std::string& zk_hosts);
  ~ServiceRegistry();
  ServiceRegistry(const ServiceRegistry&) = delete;
  ServiceRegistry& operator=(const ServiceRegistry&) = delete;
  ServiceRegistry(ServiceRegistry&&) = delete;
  ServiceRegistry& operator=(ServiceRegistry&&) = delete;

  // 只在取得临时节点所有权后成功；其他会话占有时设置 watch 并限时等待。
  bool RegisterService(const std::string& service_name,
                       const std::string& service_address,
                       std::chrono::milliseconds ownership_timeout =
                           std::chrono::milliseconds(3000));
  bool IsConnected() const;

 private:
  struct RecoverySignal;
  struct Session;
  struct Registration;
  struct Operation;
  enum class OperationKind { kCreate, kExists, kWatchExists, kGet };
  static void CompleteString(int, const char*, const void*);
  static void CompleteStat(int, const Stat*, const void*);
  static void CompleteData(int, const char*, int, const Stat*, const void*);
  std::shared_ptr<Operation> Execute(const std::shared_ptr<Session>&,
                                     OperationKind, const std::string&,
                                     const std::string&, int,
                                     std::chrono::steady_clock::time_point);
  static void GlobalWatcher(zhandle_t*, int type, int state, const char*,
                            void* context);
  std::shared_ptr<Session> CreateSession();
  std::shared_ptr<Session> Snapshot() const;
  bool EnsurePath(const std::shared_ptr<Session>& session,
                  const std::string& path,
                  std::chrono::steady_clock::time_point deadline);
  bool RegisterOnSession(const std::shared_ptr<Session>& session,
                         const std::string& service_name,
                         const std::string& service_address,
                         std::chrono::steady_clock::time_point deadline);
  void Recover();

  inline static constexpr char kRootPath[] = "/cookrpc";
  const std::string endpoint_;
  std::shared_ptr<RecoverySignal> signal_;
  mutable std::mutex session_mutex_;
  std::shared_ptr<Session> session_;
  // 注册和恢复串行化，watcher 不获取该锁或执行 ZooKeeper 操作。
  std::mutex registration_mutex_;
  std::condition_variable registration_cv_;
  bool registration_busy_ = false;
  std::unordered_map<std::string, std::pair<std::string, std::string>>
      registrations_;
  bool needs_restore_ = false;
  std::thread recovery_thread_;
};
}  // namespace cookrpc
#endif
