#ifndef RPC_TUTORIAL_RPC_SRC_SERVICE_SERVICE_MANAGER_H_
#define RPC_TUTORIAL_RPC_SRC_SERVICE_SERVICE_MANAGER_H_

#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "log_manager.h"
#include "service.h"
#include "thread_pool_singleton.h"

namespace cookrpc {

// 服务管理器
class ServiceManager {
 public:
  static ServiceManager& GetInstance() {
    static ServiceManager instance;
    return instance;
  }

  // 注册服务
  bool RegisterService(std::shared_ptr<Service> service) {
    if (service == nullptr) {
      LOG_ERROR("Cannot register a null service");
      return false;
    }

    std::string service_name;
    try {
      service_name = service->GetServiceName();
    } catch (const std::exception& error) {
      LOG_ERROR("Failed to get service name during registration: {}",
                error.what());
      return false;
    } catch (...) {
      LOG_ERROR("Failed to get service name during registration: unknown "
                "exception");
      return false;
    }

    if (service_name.empty()) {
      LOG_ERROR("Cannot register a service with an empty name");
      return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const bool inserted =
        services_.emplace(service_name, std::move(service)).second;
    if (!inserted) {
      LOG_ERROR("Service is already registered: {}", service_name);
      return false;
    }

    LOG_INFO("Service registered successfully: {}", service_name);
    return true;
  }

  // 获取服务
  std::shared_ptr<Service> GetService(const std::string& service_name) {
    if (service_name.empty()) {
      return nullptr;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto iterator = services_.find(service_name);
    return iterator == services_.end() ? nullptr : iterator->second;
  }

  // 同步处理 RPC 请求
  bool HandleRpcRequest(const std::string& service_name,
                        const std::string& method_name,
                        const std::string& args,
                        std::string& result) {
    return HandleRpcRequestSync(service_name, method_name, args, result);
  }

  // 异步处理 RPC 请求
  std::future<bool> HandleRpcRequestAsync(const std::string& service_name,
                                          const std::string& method_name,
                                          const std::string& args,
                                          std::string& result) {
    // 调用方必须保证 result 的生命周期持续到返回的 future 就绪。
    return meeting_ctrl::ThreadPoolSingleton::Enqueue(
        meeting_ctrl::TaskPriority::kNormal,
        [this, service_name, method_name, args, &result] {
          return HandleRpcRequestSync(service_name, method_name, args, result);
        });
  }

  // 获取线程池统计信息
  meeting_ctrl::ThreadPool::Stats GetThreadPoolStats() const {
    return meeting_ctrl::ThreadPoolSingleton::GetStats();
  }

  ServiceManager(const ServiceManager&) = delete;
  ServiceManager& operator=(const ServiceManager&) = delete;

 private:
  ServiceManager() = default;

  // 同步处理 RPC 请求的内部实现
  bool HandleRpcRequestSync(const std::string& service_name,
                            const std::string& method_name,
                            const std::string& args,
                            std::string& result) {
    const std::shared_ptr<Service> service = GetService(service_name);
    if (service == nullptr) {
      result.clear();
      LOG_ERROR("Service not found: {}", service_name);
      return false;
    }

    try {
      return service->HandleRequest(method_name, args, result);
    } catch (const std::exception& error) {
      result.clear();
      LOG_ERROR("Service {} failed to handle method {}: {}", service_name,
                method_name, error.what());
    } catch (...) {
      result.clear();
      LOG_ERROR("Service {} failed to handle method {}: unknown exception",
                service_name, method_name);
    }
    return false;
  }

  // 服务映射表
  std::unordered_map<std::string, std::shared_ptr<Service>> services_;

  // 互斥锁
  mutable std::mutex mutex_;
};
} // namespace cookrpc

#endif // RPC_TUTORIAL_RPC_SRC_SERVICE_SERVICE_MANAGER_H_
