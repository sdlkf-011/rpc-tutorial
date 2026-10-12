#include "registry_config.h"

#include <fstream>
#include <nlohmann/json.hpp>

#include "config_validation.h"
#include "log_manager.h"
#include "utils_fun.h"

namespace cookrpc {
bool ServiceRegistryConfig::InitRegistryConfig(const std::string& filename) {
  try {
    std::ifstream file(filename);
    if (!file.is_open()) return false;
    const auto json = nlohmann::json::parse(file);
    auto name =
        config_validation::ReadString(json, "service_name", "cookrpc_service");
    auto version =
        config_validation::ReadString(json, "service_version", "1.0.0");
    config_validation::Require(
        name != "." && name != ".." && name.find('/') == std::string::npos,
        "Service name must be a path segment");
    std::vector<RegistryInfo> nodes;
    if (json.contains("registry_nodes")) {
      config_validation::Require(json.at("registry_nodes").is_array(),
                                 "Registry nodes must be an array");
      for (const auto& node : json.at("registry_nodes")) {
        auto address = config_validation::ReadString(node, "address", "");
        config_validation::Require(IsValidIpAddress(address),
                                   "Invalid registry IP address");
        const auto port =
            config_validation::ReadInteger(node, "port", 0, 1, 65535);
        // 缺省端口同样必须校验，不能悄悄跳过错误节点。
        config_validation::Require(port > 0, "Registry port is required");
        nodes.push_back({std::move(address), static_cast<uint16_t>(port)});
      }
    }
    service_name_.swap(name);
    service_version_.swap(version);
    registry_nodes_.swap(nodes);
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("Failed to load registry configuration: {}", error.what());
    return false;
  }
}
}  // namespace cookrpc
