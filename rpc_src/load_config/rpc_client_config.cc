#include "rpc_client_config.h"

#include <fstream>
#include <limits>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>

#include "log_manager.h"

namespace cookrpc {
namespace {
void Require(bool valid, const char* message) {
  if (!valid) throw std::invalid_argument(message);
}
int ReadInteger(const nlohmann::json& config, const char* key, int fallback) {
  if (!config.contains(key)) return fallback;
  const auto& value = config.at(key);
  Require(value.is_number_integer(), "Configuration field must be an integer");
  if (value.is_number_unsigned()) {
    const auto integer = value.get<uint64_t>();
    Require(integer <= static_cast<uint64_t>(std::numeric_limits<int>::max()),
            "Configuration integer is out of range");
    return static_cast<int>(integer);
  }
  const auto integer = value.get<int64_t>();
  Require(integer >= std::numeric_limits<int>::min() &&
              integer <= std::numeric_limits<int>::max(),
          "Configuration integer is out of range");
  return static_cast<int>(integer);
}
}  // namespace

void RpcClientConfig::SetZkHost(const std::string& host) {
  Require(!host.empty() && host.find('\0') == std::string::npos,
          "ZooKeeper host must not be empty or contain NUL");
  zk_host_ = host;
}
void RpcClientConfig::SetZkNamespace(const std::string& value) {
  Require(!value.empty() && value.front() == '/' &&
              value.find('\0') == std::string::npos,
          "ZooKeeper namespace must be an absolute path without NUL");
  zk_namespace_ = value;
}
void RpcClientConfig::SetZkPort(int port) {
  Require(port > 0 && port <= 65535,
          "ZooKeeper port must be between 1 and 65535");
  zk_port_ = port;
}
void RpcClientConfig::SetTimeout(int timeout) {
  Require(timeout > 0, "Timeout must be positive");
  timeout_ms_ = timeout;
}
void RpcClientConfig::SetRetryTimes(int retries) {
  Require(retries >= 0 && retries <= 100,
          "Retry count must be between 0 and 100");
  retry_times_ = retries;
}
void RpcClientConfig::SetServerPort(int port) {
  Require(port > 0 && port <= 65535, "Server port must be between 1 and 65535");
  server_port_ = port;
}

bool RpcClientConfig::InitRpcClientConfig(const std::string& filename) {
  try {
    std::ifstream file(filename);
    if (!file.is_open()) return false;
    const auto json = nlohmann::json::parse(file);
    Require(json.is_object(), "Client configuration must be an object");
    RpcClientConfig candidate;
    candidate.SetZkHost(json.value("zk_host", "localhost"));
    candidate.SetZkNamespace(json.value("zk_namespace", "/cookrpc"));
    candidate.SetZkPort(ReadInteger(json, "zk_port", 2181));
    candidate.SetTimeout(ReadInteger(json, "timeout_ms", 3000));
    candidate.SetRetryTimes(ReadInteger(json, "retry_times", 3));
    candidate.SetServerPort(ReadInteger(json, "server_port", 8989));
    // 提交只进行无异常的交换和整数赋值，失败不会留下半份新配置。
    zk_host_.swap(candidate.zk_host_);
    zk_namespace_.swap(candidate.zk_namespace_);
    zk_port_ = candidate.zk_port_;
    timeout_ms_ = candidate.timeout_ms_;
    retry_times_ = candidate.retry_times_;
    server_port_ = candidate.server_port_;
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("Failed to load client configuration: {}", error.what());
    return false;
  }
}
}  // namespace cookrpc
