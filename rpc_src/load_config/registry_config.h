#ifndef RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_REGISTRY_CONFIG_H_
#define RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_REGISTRY_CONFIG_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cookrpc {

// 负载均衡策略枚举
enum class LoadBalanceStrategy {
  kRoundRobin, // 轮询
  kRandom, // 随机
  kWeightRoundRobin, // 加权轮询
};

// 注册节点信息
struct RegistryInfo {
  std::string address;
  uint16_t port;
};

class ServiceRegistryConfig {
 public:
  ServiceRegistryConfig() = default;
  ~ServiceRegistryConfig() = default;

  // 从文件初始化
  bool InitRegistryConfig(const std::string& config_file);

  // Getter/Setter
  const std::vector<RegistryInfo>& GetRegistryNodes() const { 
    return registry_nodes_; 
  }

  size_t GetRegistryNodesSize() const {
    return registry_nodes_.size();
  }

  void SetServiceName(const std::string& service_name) {
    service_name_ = service_name;
  }

  const std::string& GetServiceName() const {
    return service_name_;
  }

  void SetServiceVersion(const std::string& service_version) {
    service_version_ = service_version;
  }

  const std::string& GetServiceVersion() const {
    return service_version_;
  }

  // 禁止拷贝和移动
  ServiceRegistryConfig(const ServiceRegistryConfig&) = delete;
  ServiceRegistryConfig& operator=(const ServiceRegistryConfig&) = delete;
  ServiceRegistryConfig(ServiceRegistryConfig&&) = delete;
  ServiceRegistryConfig& operator=(ServiceRegistryConfig&&) = delete;
 private:
  std::vector<RegistryInfo> registry_nodes_; // 注册节点列表
  std::string service_name_; // 服务名称
  std::string service_version_; // 服务版本
};
} // namespace cookrpc

#endif // RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_REGISTRY_CONFIG_H_
