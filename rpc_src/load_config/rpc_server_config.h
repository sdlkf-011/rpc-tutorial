#ifndef RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_RPC_SERVER_CONFIG_H_
#define RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_RPC_SERVER_CONFIG_H_

#include <cstdint>
#include <memory>
#include <string>

#include "registry_config.h"
#include "thread_pool_config.h"

namespace cookrpc {

class RpcServerConfig {
 public:
  static RpcServerConfig& GetInstance() {
    static RpcServerConfig instance;
    return instance;
  }

  // 初始化
  bool InitRpcServerConfig(const std::string& config_file);

  // Setter
  void SetServersNameProfix(const std::string& servers_name_prefix) {
    servers_name_prefix_ = servers_name_prefix;
  }

  void SetServersIp(const std::string& servers_ip) { servers_ip_ = servers_ip; }

  void SetServersPort(uint16_t servers_port) { servers_port_ = servers_port; }

  void SetTimeout(uint32_t timeout) { timeout_ = timeout; }

  void SetMaxConnections(uint32_t max_connections) {
    max_connections_ = max_connections;
  }

  // Getter
  const std::string& GetServersNameProfix() const {
    return servers_name_prefix_;
  }

  const std::string& GetServersIp() const { return servers_ip_; }

  uint16_t GetServersPort() const { return servers_port_; }

  uint32_t GetTimeout() const { return timeout_; }

  uint32_t GetMaxConnections() const { return max_connections_; }

  // 获取子配置
  const ServiceRegistryConfig* GetServiceRegistryConfig() const {
    return service_registry_config_.get();
  }

  const ThreadPoolConfig* GetThreadPoolConfig() const {
    return thread_pool_config_.get();
  }

  RpcServerConfig(const RpcServerConfig&) = delete;
  RpcServerConfig& operator=(const RpcServerConfig&) = delete;
  RpcServerConfig(RpcServerConfig&&) = delete;
  RpcServerConfig& operator=(RpcServerConfig&&) = delete;

 private:
  // 基础配置
  uint32_t max_connections_ = 0;
  std::string servers_ip_;
  uint16_t servers_port_ = 0;
  uint32_t timeout_ = 0;
  std::string servers_name_prefix_;

  // 子配置对象
  std::unique_ptr<ThreadPoolConfig> thread_pool_config_;
  std::unique_ptr<ServiceRegistryConfig> service_registry_config_;

  // 私有构造函数
  RpcServerConfig()
      : thread_pool_config_(std::make_unique<ThreadPoolConfig>()),
        service_registry_config_(std::make_unique<ServiceRegistryConfig>()) {}
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_RPC_SERVER_CONFIG_H_
