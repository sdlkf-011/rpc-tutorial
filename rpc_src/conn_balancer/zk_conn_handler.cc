#include "zk_conn_handler.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>

#include "log_manager.h"
#include "load_config/registry_config.h"
#include "conn_balancer/load_balancer.h"

namespace cookrpc {

bool ZkConnHandler::InitZkConnHandler(const nlohmann::json& config) {
  try {
    // 检查配置的有效性
    if (config.empty()) {
      LOG_ERROR("Empty configuration");
      return false;
    }
  
    // 安全的读取参数配置
    try {
      std::string zk_host = config.value("zk_host", "loaclhost");
      SetZkHost(zk_host);

      int zk_port = config.value("zk_port", 2181);
      SetZkPort(zk_port);

      SetZkNamespace(config.value("zk_namespace", "/cookrpc"));
      SetZkRetryInterval(config.value("zk_retry_interval", 5));
    } catch (const nlohmann::json::exception& e) {
      LOG_ERROR("Failed to parse configuration: {}", e.what());
      CleanUp();
      return false;
    }

    // 尝试建立连接
    bool is_connected = EnsureZkConnection();

    if (!zk_client_) {
      LOG_ERROR("Failed to initialize ZooKeeper client: {}", strerror(errno));
      return false;
    }

    if (!is_connected) {
      LOG_ERROR("Failed to connect to ZooKeeper after retries");
      return false;
    }

    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception in initZkConnHandler: {}", e.what());
    CleanUp();
    return false;
  }
}

std::vector<std::string> ZkConnHandler::GetAllServers(const std::string& zk_namespace) {
  // 确保 ZooKeeper 连接可用
  if (!EnsureZkConnection()) {
    LOG_ERROR("ZooKeeper connection not available");
    return {};
  }

  std::lock_guard<std::mutex> lock(servers_mutex_);

  try {
    struct String_vector children = {0, nullptr};

    // 获取指定路径下的所有子节点
    int rc = zoo_get_children(zk_client_, zk_namespace.c_str(), 0, &children);

    if (rc != ZOK) {
      LOG_ERROR("Failed to get chilren: {}", zerror(rc));
      return {};
    }

    std::vector<std::string> servers;

    // 遍历每个子节点, 获取服务器地址
    for (int i = 0; i < children.count; ++i) {
      std::string node_path = zk_namespace + "/" + children.data[i];
      char buffer[1024];
      int buffer_len = sizeof(buffer);

      // 获取节点的数量
      int rc = zoo_get(zk_client_, node_path.c_str(), 0, buffer, &buffer_len, nullptr);
      if (rc == ZOK && buffer_len > 0) {
        std::string server_ip(buffer, buffer_len);
        servers.push_back(server_ip);
      } else {
        LOG_WARN("Failed to get data for node: {}, error: {}", node_path, zerror(rc));
      }
    }

    // 释放 ZooKeeper 分配的内容
    deallocate_String_vector(&children);
    return servers;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception in getAllServers: {}", e.what());
    return {};
  }
}

std::string ZkConnHandler::GetServer(const std::string& zk_namespace) {
  // 更新服务器
  UpdateServersFromZk(zk_namespace);

  std::lock_guard<std::mutex> lock(servers_mutex_);

  if (servers_.empty()) {
    LOG_ERROR("No acailable servers");
    return "";
  }

  // 使用负载均衡器选择服务器
  std::string server = LoadBalancer::SelectServer(servers_);
  return server;
}

bool ZkConnHandler::RegisterService(const std::string& service_name,
                                    const std::string& service_address) {
  try {
    // 获取或创建 ServiceRegistry 实例
    ServiceRegistry* registry = GetOrCreateServiceRegistry();
    if (!registry) {
      LOG_ERROR("Failed to get or create ServiceRegistry for service registration");
      return false;
    }

    // 执行服务注册
    if (registry->RegisterService(service_name, service_address)) {
      return true;
    } else {
      LOG_ERROR("Failed to register service: {} at {}", service_name, service_address);
      return false;
    }
  } catch (const std::exception& e) {
    LOG_ERROR("Exception during service registration: {}", e.what());
    return false;
  }                                      
}

bool ZkConnHandler::ReisterServicesFromConfig(const cookrpc::ServiceRegistryConfig* registry_config) {
  // 没有配置不算错误
  if (!registry_config || registry_config->GetRegistryNodesSize() == 0) {
    LOG_WARN("No registry configuration found, skipping ZooKeeper registration");
    return true;
  }

  try {
    // 获取或创建 ServiceRegistry 实例
    ServiceRegistry* registry = GetOrCreateServiceRegistry();
    if (!registry) {
      LOG_ERROR("Failed to get or create ServiceRegistry for batch service registration");
      return false;
    }

    // 注册配置文件中的所有服务节点
    const auto& registry_nodes = registry_config->GetRegistryNodes();
    const std::string& service_name = registry_config->GetServiceName();

    bool all_success = true;
    for (const auto& node : registry_nodes) {
      std::string service_address = node.address + ":" + std::to_string(node.port);
      if (registry->RegisterService(service_name, service_address)) {
        LOG_INFO("Successfully registered service: {} at {}", service_name, service_address);
      } else {
        LOG_ERROR("Failed to register service: {} at {}", service_name, service_address);
        all_success = false;
      }
    }

    return all_success;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception during service registration: {}", e.what());
    return false;
  }
}

ServiceRegistry* ZkConnHandler::GetOrCreateServiceRegistry() {
  // 无锁检查
  if (!service_registry_) {
    std::lock_guard<std::mutex> lock(mutex_);
    // 有锁检查
    if (!service_registry_) {
      if (!CreateServiceRegistryIfNeeded()) {
        return nullptr;
      }
    }
  }
  return service_registry_.get();
}

void ZkConnHandler::SetZkHost(const std::string& host) {
  std::lock_guard<std::mutex> lock(mutex_);
  try {
    if (host.empty()) {
      LOG_WARN("Empty host provided, using default");
      zk_host_ = "localhost";
    } else {
      zk_host_ = host;
    }
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to set ZooKeeper host: {}", e.what());
    zk_host_ = "localhost";
  }
}

void ZkConnHandler::SetZkPort(int port) {
  std::lock_guard<std::mutex> lock(mutex_);
  zk_port_ = (port > 0 && port < 65536) ? port : 2181;
}

void ZkConnHandler::SetZkRetryInterval(int seconds) {
  retry_interval_ = std::chrono::seconds(seconds);
}

void ZkConnHandler::SetZkNamespace(const std::string& zk_namespace) {
  zk_namespace_ = zk_namespace;
}

bool ZkConnHandler::EnsureZkConnection() {
  // zookeeper_init() 只创建句柄，实际连接由 ZooKeeper 后台线程异步完成。
  if (!InitZkClient()) {
    return false;
  }

  // 等待连接建立
  int retry_count = 0;
  const int max_retries = 3;

  while (retry_count++ < max_retries) {
    int state = zoo_state(zk_client_);
    const char* state_desc;

    // 状态描述
    if (state == ZOO_CONNECTED_STATE) {
      state_desc = "CONNECTED";
    } else if (state == ZOO_CONNECTING_STATE) {
      state_desc = "CONNECTING";
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      state_desc = "EXPIRED";
    } else if (state == ZOO_AUTH_FAILED_STATE) {
      state_desc = "AUTH_FAILED";
    } else if (state == ZOO_ASSOCIATING_STATE) {
      state_desc = "ASSOCIATING";
    } else {
      state_desc = "UNKNOWN";
      // 诊断信息
      LOG_WARN("Unexpected state value: {} (0x{:x})", state, state);
      LOG_WARN("Client handle valid: {}", (zk_client_ != nullptr));

      if (zk_client_) {
        // 尝试一个简单的操作测试连接
        struct Stat stat{};
        int rc = zoo_exists(zk_client_, "/", 0, &stat);
        LOG_WARN("zoo_exists test result: {}", rc);
      }
    }

    // 连接成功
    if (state == ZOO_CONNECTED_STATE) {
      return true;
    }

    // 不可恢复的错误状态
    if (state == ZOO_EXPIRED_SESSION_STATE ||
        state == ZOO_AUTH_FAILED_STATE ||
        (state != ZOO_CONNECTED_STATE && retry_count > 10)) {
      LOG_ERROR("Connection failed with state: {} ({})", state, state_desc);
      CleanUp();
      return false;
    }

    // 短暂等待后重试
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  LOG_ERROR("Failed to connect after {} retries", max_retries);
  CleanUp();
  return false;
}

void ZkConnHandler::UpdateServersFromZk(const std::string& zk_namespace) {
  auto new_servers = GetAllServers(zk_namespace);
  {
    std::lock_guard<std::mutex> lock(servers_mutex_);
    servers_ = std::move(new_servers);
  }
}

void ZkConnHandler::CleanUp() {
  // 通过原子标志确保在多线程环境中清理操作只被执行一次
  static std::atomic<bool> cleanup_done{false};
  if (cleanup_done.exchange(true)) {
    return;
  }

  // 清理 ServiceRegistry 实例
  service_registry_.reset();

  // 清理 ZooKeeper 客户端连接
  if (zk_client_) {
    try {
      zookeeper_close(zk_client_);
      zk_client_ = nullptr;
    } catch (const std::exception& e) {
      zk_client_ = nullptr;
    } catch(...) {
      zk_client_ = nullptr;
    }
  }

  // 重置运行状态
  running_ = false;
}

ZkConnHandler::~ZkConnHandler() {
  CleanUp();
}

void ZkConnHandler::GlobalWatcher(zhandle_t* zh, int type, int state,
                            const char* path, void* watcherCtx) {
  (void)zh;
  (void)path;

  // 目前主要处理会话事件
  if (type == ZOO_SESSION_EVENT) {
    auto* handler = static_cast<ZkConnHandler*>(watcherCtx);
    if (handler) {
      handler->running_.store(state == ZOO_CONNECTED_STATE,
                              std::memory_order_release);
    }

    const char* state_desc;

    // 状态描述，便于日志记录和调试
    if (state == ZOO_CONNECTED_STATE) {
      state_desc = "CONNECTED";
    } else if (state == ZOO_CONNECTING_STATE) {
      state_desc = "CONNECTING";
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      state_desc = "EXPIRED";
    } else if (state == ZOO_AUTH_FAILED_STATE) {
      state_desc = "AUTH_FAILED";
    } else if (state == ZOO_ASSOCIATING_STATE) {
      state_desc = "ASSOCIATING";
    } else {
      state_desc = "UNKNOWN";
    }

    LOG_INFO("ZooKeeper session state changed: {}", state_desc);
  }
}

bool ZkConnHandler::CreateServiceRegistryIfNeeded() {
  try {
    // 构建 ZooKeeper 连接字符串
    std::string zk_connection = zk_host_ + ":" + std::to_string(zk_port_);

    // 创建 ServiceRegistry 实例
    service_registry_ = std::make_unique<ServiceRegistry>(zk_connection);

    // 等待连接创建
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 验证连接状态
    if (!service_registry_->IsConnected()) {
      LOG_ERROR("Failed to connect ServiceRegistry to ZooKeeper at {}", zk_connection);
      service_registry_.reset();
      return false;
    }

    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception creating ServiceRegistry: {}", e.what());
    service_registry_.reset();
    return false;
  }
}

bool ZkConnHandler::InitZkClient() {
  std::lock_guard<std::mutex> lock(mutex_);

  // 惰性初始化可能被多个请求同时触发，已有句柄时直接复用。
  if (zk_client_ != nullptr) {
    return true;
  }

  if (zk_host_.empty() || zk_port_ <= 0 || zk_port_ >= 65536) {
    LOG_ERROR("Invalid ZooKeeper endpoint: host='{}', port={}", zk_host_,
              zk_port_);
    return false;
  }

  const std::string connection =
      zk_host_ + ":" + std::to_string(zk_port_);

  running_.store(false, std::memory_order_release);
  errno = 0;
  zhandle_t* client = zookeeper_init(
      connection.c_str(), GlobalWatcher, 30000, nullptr, this, 0);
  if (client == nullptr) {
    const int error_number = errno;
    LOG_ERROR("Failed to initialize ZooKeeper client for {}: {} (errno: {})",
              connection, std::strerror(error_number), error_number);
    return false;
  }

  zk_client_ = client;
  LOG_INFO("ZooKeeper client initialized for {}", connection);
  return true;
}
} // namespace cookrpc
