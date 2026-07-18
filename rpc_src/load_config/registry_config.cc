#include "registry_config.h"

#include <exception>
#include <fstream>
#include <string>
#include <nlohmann/json_fwd.hpp>
#include <nlohmann/json.hpp>

#include "log_manager.h"

namespace cookrpc {

bool ServiceRegistryConfig::InitRegistryConfig(const std::string& config_file) {
  try {
    // 打开文件
    std::ifstream file(config_file);
    if (!file.is_open()) {
      LOG_ERROR("Failed to open registry config file: {}", config_file);
      return false;
    }

    // 解析 json
    nlohmann::json config = nlohmann::json::parse(file);

    // 加载基础信息
    SetServiceName(config.value("service_name", "cookrpc_service"));
    SetServiceVersion(config.value("service_version", "1.0.0"));

    // 加载注册节点列表
    if (config.contains("registry_nodes") && config["registry_nodes"].is_array()) {
      for (const auto& node : config["registry_nodes"]) {
        RegistryInfo info;
        info.address = node.value("address", "");
        info.port = node.value("port", 0);

        // 验证节点信息
        if (info.address.empty() || info.port == 0) {
          LOG_WARN("Invalid registry node, skipping");
          continue;
        }
        registry_nodes_.push_back(info);
      }
    }

    LOG_INFO("Registry config loaded: service={}, version={}, nodes={}",
             service_name_, service_version_, registry_nodes_.size());
    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to parse registry config: {}", e.what());
    return false;
  }
}
} // namespace cookrpc
