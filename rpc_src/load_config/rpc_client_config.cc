#include "rpc_client_config.h"

#include <exception>
#include <fstream>
#include <string>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp> 

#include "log_manager.h"

namespace cookrpc {

bool RpcClientConfig::InitRpcClientConfig(const std::string& config_filename) {
  try {
    // 打开文件
    std::ifstream file(config_filename);
    if (!file.is_open()) {
      LOG_ERROR("Failed to open config file: {}", config_filename);
      return false;
    }

    // 解析 JSON
    nlohmann::json config = nlohmann::json::parse(file);

    // 加载配置项
    SetZkHost(config.value("zk_host", "localhost"));
    SetZkNamespace(config.value("zk_namespace", "/cookrpc"));
    SetZkPort(config.value("zk_port", 2181));
    SetTimeout(config.value("timeout_ms", 3000));
    SetRetryTimes(config.value("retry_times", 3));
    SetServerPort(config.value("server_port", 8989));

    // 参数验证
    if (timeout_ms_ <= 0) {
      LOG_ERROR("Timeout must be positive, got: {}", timeout_ms_);
      return false;
    }

    if (retry_times_ < 0) {
      LOG_ERROR("Retry times cannot be negative, got: {}", retry_times_);
      return false;
    }

    if (zk_port_ < 0 || zk_port_ > 6335) {
      LOG_ERROR("Invalid ZooKeeper port: {}", zk_port_);
    }

    LOG_INFO("RpcClient config loaded: zk={}:{}, timeout={}ms, retry={}",
             zk_host_, zk_port_, timeout_ms_, retry_times_);
    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to parse client config: {}", e.what());
    return false;
  }
}

} // namespace cookrpc
