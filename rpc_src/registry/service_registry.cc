#include "service_registry.h"

#include <chrono>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>

#include "log_manager.h"

namespace cookrpc {
namespace {

bool IsValidPathSegment(const std::string& value) {
  return !value.empty() && value != "." && value != ".." &&
         value.find('/') == std::string::npos &&
         value.find('\0') == std::string::npos;
}
}  // namespace

ServiceRegistry::ServiceRegistry(const std::string& zk_hosts) {
  if (zk_hosts.empty()) {
    throw std::invalid_argument("ZooKeeper hosts must not be empty");
  }

  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
  zk_handle_ = zookeeper_init(zk_hosts.c_str(), GlobalWatcher,
                              kSessionTimeoutMs, nullptr, this, 0);
  if (zk_handle_ == nullptr) {
    throw std::runtime_error("Failed to initialize ZooKeeper client");
  }

  // zookeeper_init() 异步建立连接，此处限时等待会话 Watcher。
  // 保留客户端句柄，以便后续重连时继续更新连接状态。
  std::unique_lock<std::mutex> lock(connection_mutex_);
  const bool connected = connection_cv_.wait_for(
    lock, std::chrono::milliseconds(kConnectionTimeoutMs), [this] {
    return is_connected_.load(std::memory_order_acquire);
  });

  if (!connected) {
    LOG_WARN("Timed out connecting to ZooKeeper: {}", zk_hosts);
  }
}

ServiceRegistry::~ServiceRegistry() {
  if (zk_handle_ == nullptr) {
    return;
  }

  // 关闭会话后，ZooKeeper 会自动删除本会话创建的
  // 所有临时节点。
  const int result = zookeeper_close(zk_handle_);
  if (result != ZOK) {
    LOG_WARN("Failed to close ZooKeeper connection: {}", zerror(result));
  }
  zk_handle_ = nullptr;
  is_connected_.store(false, std::memory_order_release);
}

void ServiceRegistry::GlobalWatcher(zhandle_t*, int type, int state,
                                    const char*, void* watcher_context) {
  if (watcher_context == nullptr || type != ZOO_SESSION_EVENT) {
    return;
  }

  auto* registry = static_cast<ServiceRegistry*>(watcher_context);
  const bool connected = state == ZOO_CONNECTED_STATE;
  {
    std::lock_guard<std::mutex> lock(registry->connection_mutex_);
    registry->is_connected_.store(connected, std::memory_order_release);
  }
  registry->connection_cv_.notify_all();

  if (connected) {
    LOG_INFO("ZooKeeper session connected");
  } else if (state == ZOO_EXPIRED_SESSION_STATE) {
    LOG_ERROR("ZooKeeper session expired");
  } else if (state == ZOO_AUTH_FAILED_STATE) {
    LOG_ERROR("ZooKeeper authentication failed");
  }
}

bool ServiceRegistry::RegisterService(const std::string& service_name,
                                      const std::string& service_address) {
  if (!IsConnected()) {
    LOG_ERROR("Cannot register service while ZooKeeper is disconnected");
    return false;
  }
  
  if (!IsValidPathSegment(service_name) ||
      !IsValidPathSegment(service_address)) {
    LOG_ERROR("Invalid service name or address: service={}, address={}",
              service_name, service_address);
    return false;
  }

  // 服务节点持久存在；实例节点只在当前客户端
  // 会话期间存在。
  const std::string service_path = std::string(kRootPath) + "/" + service_name;
  if (!EnsurePath(service_path)) {
    return false;
  }

  const std::string instance_path = service_path + "/" + service_address;
  if (!CreateNode(instance_path, service_address, ZOO_EPHEMERAL)) {
    return false;
  }

  LOG_INFO("Registered service instance: service={}, address={}", service_name,
           service_address);
  return true;
}

bool ServiceRegistry::IsConnected() const {
  return zk_handle_ != nullptr &&
         is_connected_.load(std::memory_order_acquire);
}

bool ServiceRegistry::CreateNode(const std::string& path,
                                 const std::string& data, int flags) {
  if (zk_handle_ == nullptr) {
    LOG_ERROR("Cannot create ZooKeeper node without a client handle");
    return false;
  }
  if (data.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    LOG_ERROR("ZooKeeper node data is too large: {}", path);
    return false;
  }

  const int result = zoo_create(
      zk_handle_, path.c_str(), data.data(), static_cast<int>(data.size()),
      &ZOO_OPEN_ACL_UNSAFE, flags, nullptr, 0);
  if (result == ZOK) {
    return true;
  }

  if (result == ZNODEEXISTS) {
    // 当前阶段将重复注册和父路径的并发创建
    // 都视为幂等操作。
    LOG_WARN("ZooKeeper node already exists: {}", path);
    return true;
  }

  LOG_ERROR("Failed to create ZooKeeper node {}: {}", path, zerror(result));
  return false;
}

bool ServiceRegistry::EnsurePath(const std::string& path) {
  if (path.empty() || path.front() != '/') {
    LOG_ERROR("ZooKeeper path must be absolute: {}", path);
    return false;
  }
  if (path == "/") {
    return true;
  }

  // 先递归创建父节点，同时初始化 /cookrpc 根节点。
  const std::size_t separator = path.find_last_of('/');
  if (separator > 0 && !EnsurePath(path.substr(0, separator))) {
    return false;
  }

  Stat stat{};
  const int exists_result = zoo_exists(zk_handle_, path.c_str(), 0, &stat);
  if (exists_result == ZOK) {
    return true;
  }

  if (exists_result != ZNONODE) {
    LOG_ERROR("Failed to check ZooKeeper path {}: {}", path,
              zerror(exists_result));
    return false;
  }

  // exists/create 之间可能有其他进程创建同一路径。
  // CreateNode 会将 ZNODEEXISTS 视为成功。
  return CreateNode(path, "", 0);
}
}  // namespace cookrpc
