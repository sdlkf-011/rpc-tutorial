#include "rpc_server_config.h"

#include <exception>
#include <fstream>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <string>

#include "log_manager.h"
#include "utils_fun.h"

namespace cookrpc {

bool RpcServerConfig::InitRpcServerConfig(const std::string& config_file) {
  try {
    // 打开文件
    std::ifstream file(config_file);
    if (!file.is_open()) {
      LOG_ERROR("Failed to open config file: {}", config_file);
      return false;
    }

    // 解析 JSON
    nlohmann::json config = nlohmann::json::parse(file);

    // 加载基本配置
    SetServersNameProfix(config.value("servers_name_prefix", "cookrpc"));
    SetServersIp(config.value("servers_ip", "0.0.0.0"));
    SetServersPort(config.value("servers_port", 8989));
    SetMaxConnections(config.value("max_connections", 800000));
    SetTimeout(config.value("timeout", 5000));

    // 验证 IP 地址
    if (servers_ip_ != "0.0.0.0" && !IsValidIpAddress(servers_ip_)) {
      LOG_ERROR("Invalid server IP address: {}", servers_ip_);
      return false;
    }

    //  验证端口
    if (servers_port_ == 0) {
      LOG_ERROR("Server port cannot be zero");
      return false;
    }

    // 初始化线程池配置
    if (config.contains("thread_pool")) {
      if (!thread_pool_config_->InitThreadPoolConfig(config["thread_pool"])) {
        LOG_ERROR("Failed to initialize thread pool config");
        return false;
      }
    }

    // 初始化注册配置
    if (config.contains("register_config")) {
      if (!service_registry_config_->InitRegistryConfig(
              config.value("register_config", "../config/servers.json"))) {
        LOG_ERROR("Failed to initialize registry config");
        return false;
      }
    }

    LOG_INFO(
        "RpcServer config loaded: name={}, ip={}, port={}, timeout={}ms, "
        "max_conn={}",
        servers_name_prefix_, servers_ip_, servers_port_, timeout_,
        max_connections_);
    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Error initializing server config: {}", e.what());
    return false;
  }
}
}  // namespace cookrpc
