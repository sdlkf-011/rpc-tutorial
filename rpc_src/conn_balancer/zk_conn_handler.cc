#include "zk_conn_handler.h"

#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <utility>
#include <vector>

#include "config_validation.h"
#include "load_balancer.h"
#include "log_manager.h"
#include "registry_config.h"

namespace cookrpc {

struct ZkConnHandler::Session {
  zhandle_t* handle = nullptr;
  std::mutex state_mutex;
  std::condition_variable state_cv;
  int state = ZOO_CONNECTING_STATE;

  ~Session() {
    // close 可能等待 watcher；此时不持有状态锁或发现器的锁
    // 上下文成员在 close 返回前始终存活
    if (handle != nullptr) {
      zookeeper_close(handle);
    }
  }

  bool WaitConnected(int retry_interval_seconds) {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait_for(lock, std::chrono::seconds(3LL * retry_interval_seconds),
                      [this] {
                        return state == ZOO_CONNECTED_STATE ||
                               state == ZOO_EXPIRED_SESSION_STATE ||
                               state == ZOO_AUTH_FAILED_STATE;
                      });
    return state == ZOO_CONNECTED_STATE;
  }

  bool IsTerminal() {
    std::lock_guard<std::mutex> lock(state_mutex);
    return state == ZOO_EXPIRED_SESSION_STATE || state == ZOO_AUTH_FAILED_STATE;
  }
};

std::string ZkConnHandler::Configuration::Endpoint() const {
  return host + ":" + std::to_string(port);
}

bool ZkConnHandler::Configuration::IsValid() const {
  return !host.empty() && host.find('\0') == std::string::npos && port > 0 &&
         port <= 65535 && !zk_namespace.empty() &&
         zk_namespace.front() == '/' &&
         zk_namespace.find('\0') == std::string::npos &&
         retry_interval_seconds > 0;
}

std::shared_ptr<ZkConnHandler::Session> ZkConnHandler::CreateSession(
    const Configuration& config) {
  auto session = std::make_shared<Session>();
  const std::string endpoint = config.Endpoint();
  // 每个句柄只更新自己的 Session，回调不会访问 handler 或其他会话。
  session->handle = zookeeper_init(endpoint.c_str(), GlobalWatcher, 30000,
                                   nullptr, session.get(), 0);
  if (!session->handle ||
      !session->WaitConnected(config.retry_interval_seconds)) {
    LOG_ERROR("Failed to connect to ZooKeeper at {}", endpoint);
    return nullptr;
  }
  return session;
}

bool ZkConnHandler::InitZkConnHandler(const nlohmann::json& config) {
  try {
    if (!config.is_object() || config.empty()) {
      LOG_ERROR("Empty or invalid ZooKeeper configuration");
      return false;
    }
    // 先完整解析和校验，任何失败均不修改已提交配置或句柄
    Configuration candidate;
    candidate.host = config.value("zk_host", "localhost");
    candidate.port =
        config_validation::ReadInteger(config, "zk_port", 2181, 1, 65535);
    candidate.zk_namespace = config.value("zk_namespace", "/cookrpc");
    candidate.retry_interval_seconds = config_validation::ReadInteger(
        config, "zk_retry_interval", 5, 1, INT32_MAX);
    if (!candidate.IsValid()) {
      LOG_ERROR("Invalid ZooKeeper configuration");
      return false;
    }
    return CommitConfiguration(std::move(candidate));
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to initialize ZooKeeper discovery: {}", e.what());
    return false;
  }
}

bool ZkConnHandler::CommitConfiguration(Configuration config) {
  // 将待销毁对象声明在锁之前，保证锁释放后才关闭旧会话
  std::shared_ptr<Session> session;
  std::shared_ptr<ServiceRegistry> old_registry;
  std::lock_guard<std::mutex> update_lock(update_mutex_);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (config.Endpoint() == config_.Endpoint() && session_ &&
        !session_->IsTerminal()) {
      session = session_;
    }
  }
  if (session) {
    if (!session->WaitConnected(config.retry_interval_seconds)) {
      return false;
    }
  } else {
    session = CreateSession(config);
    if (!session) {
      return false;
    }
  }

  // 拷贝可能分配内存，须在提交之前完成
  Configuration pending = config;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session != session_) {
      old_registry = std::move(service_registry_);
    }
    std::swap(config_, config);
    std::swap(pending_config_, pending);
    session_.swap(session);
  }
  return true;
}

std::shared_ptr<ZkConnHandler::Session> ZkConnHandler::GetConnectedSession() {
  std::shared_ptr<Session> session;
  Configuration config;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    session = session_;
    config = config_;
  }
  if (session) {
    if (session->WaitConnected(config.retry_interval_seconds)) {
      return session;
    }
    if (!session->IsTerminal()) {
      return nullptr;
    }
  }

  // 首次惰性连接和失效会话重建与配置切换、清理互斥
  std::shared_ptr<Session> old_session;
  std::lock_guard<std::mutex> update_lock(update_mutex_);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    old_session = std::move(session);
    session = session_;
    config = session ? config_ : pending_config_;
  }
  if (session && !session->IsTerminal()) {
    return session->WaitConnected(config.retry_interval_seconds) ? session
                                                                 : nullptr;
  }
  if (!config.IsValid()) {
    return nullptr;
  }
  auto replacement = CreateSession(config);
  if (!replacement) {
    return nullptr;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    config_ = std::move(config);
    session_ = replacement;
  }
  return replacement;
}

bool ZkConnHandler::EnsureZkConnection() {
  return GetConnectedSession() != nullptr;
}

std::vector<std::string> ZkConnHandler::GetAllServers(
    const std::string& zk_namespace) {
  const auto session = GetConnectedSession();
  if (!session) {
    LOG_ERROR("ZooKeeper connection not available");
    return {};
  }

  // 查询全程持有同一会话快照，配置切换与 CleanUp 不会关闭它。
  struct Children {
    String_vector value{0, nullptr};
    ~Children() { deallocate_String_vector(&value); }
  } children;

  try {
    int rc = zoo_get_children(session->handle, zk_namespace.c_str(), 0,
                              &children.value);
    if (rc != ZOK) {
      LOG_ERROR("Failed to get children: {}", zerror(rc));
      return {};
    }
    std::vector<std::string> servers;
    for (int i = 0; i < children.value.count; ++i) {
      const std::string node_path = (zk_namespace == "/" ? "" : zk_namespace) +
                                    "/" + children.value.data[i];
      char buffer[1024];
      int buffer_len = sizeof(buffer);
      rc = zoo_get(session->handle, node_path.c_str(), 0, buffer, &buffer_len,
                   nullptr);
      if (rc == ZOK && buffer_len > 0 &&
          buffer_len <= static_cast<int>(sizeof(buffer))) {
        servers.emplace_back(buffer, buffer_len);
      } else {
        LOG_WARN("Failed to get data for node: {}, error: {}", node_path,
                 zerror(rc));
      }
    }
    return servers;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception in GetAllServers: {}", e.what());
    return {};
  }
}

std::string ZkConnHandler::GetServer(const std::string& zk_namespace) {
  // 本次命名空间对应的局部列表直接交给策略，避免跨请求共享缓存。
  const auto servers = GetAllServers(zk_namespace);
  return LoadBalancer::SelectServer(servers);
}

bool ZkConnHandler::RegisterService(const std::string& service_name,
                                    const std::string& service_address) {
  try {
    // 获取或创建 ServiceRegistry 实例
    const std::shared_ptr<ServiceRegistry> registry =
        GetOrCreateServiceRegistry();
    if (!registry) {
      LOG_ERROR(
          "Failed to get or create ServiceRegistry for service registration");
      return false;
    }

    // 执行服务注册
    if (registry->RegisterService(service_name, service_address)) {
      return true;
    } else {
      LOG_ERROR("Failed to register service: {} at {}", service_name,
                service_address);
      return false;
    }
  } catch (const std::exception& e) {
    LOG_ERROR("Exception during service registration: {}", e.what());
    return false;
  }
}

bool ZkConnHandler::ReisterServicesFromConfig(
    const cookrpc::ServiceRegistryConfig* registry_config) {
  // 没有配置不算错误
  if (!registry_config || registry_config->GetRegistryNodesSize() == 0) {
    LOG_WARN(
        "No registry configuration found, skipping ZooKeeper registration");
    return true;
  }

  try {
    // 获取或创建 ServiceRegistry 实例
    const std::shared_ptr<ServiceRegistry> registry =
        GetOrCreateServiceRegistry();
    if (!registry) {
      LOG_ERROR(
          "Failed to get or create ServiceRegistry for batch service "
          "registration");
      return false;
    }

    // 注册配置文件中的所有服务节点
    const auto& registry_nodes = registry_config->GetRegistryNodes();
    const std::string& service_name = registry_config->GetServiceName();

    bool all_success = true;
    for (const auto& node : registry_nodes) {
      std::string service_address =
          node.address + ":" + std::to_string(node.port);
      if (registry->RegisterService(service_name, service_address)) {
        LOG_INFO("Successfully registered service: {} at {}", service_name,
                 service_address);
      } else {
        LOG_ERROR("Failed to register service: {} at {}", service_name,
                  service_address);
        all_success = false;
      }
    }

    return all_success;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception during service registration: {}", e.what());
    return false;
  }
}

std::shared_ptr<ServiceRegistry> ZkConnHandler::GetOrCreateServiceRegistry() {
  std::shared_ptr<ServiceRegistry> registry;
  std::lock_guard<std::mutex> update_lock(update_mutex_);
  Configuration config;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (service_registry_) {
      return service_registry_;
    }
    config = config_;
  }
  try {
    // 建连期间不持有快照锁；事务锁保证注册器与提交的端点一致。
    registry = std::make_shared<ServiceRegistry>(config.Endpoint());
    if (!registry->IsConnected()) {
      return nullptr;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    service_registry_ = registry;
    return registry;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception creating ServiceRegistry: {}", e.what());
    return nullptr;
  }
}

void ZkConnHandler::SetZkHost(const std::string& host) {
  std::lock_guard<std::mutex> update_lock(update_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);
  pending_config_.host = host;
}

void ZkConnHandler::SetZkPort(int port) {
  std::lock_guard<std::mutex> update_lock(update_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);
  pending_config_.port = port;
}

void ZkConnHandler::SetZkRetryInterval(int seconds) {
  std::lock_guard<std::mutex> update_lock(update_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);
  pending_config_.retry_interval_seconds = seconds;
}

void ZkConnHandler::SetZkNamespace(const std::string& zk_namespace) {
  std::lock_guard<std::mutex> update_lock(update_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);
  pending_config_.zk_namespace = zk_namespace;
}

void ZkConnHandler::CleanUp() {
  std::shared_ptr<Session> old_session;
  std::shared_ptr<ServiceRegistry> old_registry;
  {
    std::lock_guard<std::mutex> update_lock(update_mutex_);
    std::lock_guard<std::mutex> lock(mutex_);
    old_session = std::move(session_);
    old_registry = std::move(service_registry_);
  }

  // 最后一个所有者在所有锁之外关闭句柄
}

ZkConnHandler::~ZkConnHandler() { CleanUp(); }

void ZkConnHandler::GlobalWatcher(zhandle_t*, int type, int state, const char*,
                                  void* watcher_context) {
  if (type != ZOO_SESSION_EVENT || watcher_context == nullptr) {
    return;
  }
  auto* session = static_cast<Session*>(watcher_context);
  {
    std::lock_guard<std::mutex> lock(session->state_mutex);
    session->state = state;
  }
  session->state_cv.notify_all();
}
}  // namespace cookrpc
