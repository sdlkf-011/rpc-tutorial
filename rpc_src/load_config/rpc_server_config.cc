#include "rpc_server_config.h"

#include <fstream>
#include <nlohmann/json.hpp>

#include "config_validation.h"
#include "log_manager.h"
#include "transport_codec.h"
#include "utils_fun.h"

namespace cookrpc {
bool RpcServerConfig::InitRpcServerConfig(const std::string& filename) {
  try {
    std::ifstream file(filename);
    if (!file.is_open()) return false;
    const auto json = nlohmann::json::parse(file);
    using config_validation::ReadInteger;

    // 包含所有子配置的候选对象；提交前不改变当前对象或子对象。
    RpcServerConfig candidate;
    candidate.transport_options_ = TransportOptions::Parse(
        json.value("transport", nlohmann::json::object()));
    candidate.rate_limit_options_ = RateLimitOptions::Parse(
        json.value("rate_limit", nlohmann::json::object()));

    // 提交前验证加密密钥，避免配置已生效却无法建立传输编码器。
    const TransportCodec validated_transport(candidate.transport_options_);
    candidate.servers_name_prefix_ =
        config_validation::ReadString(json, "servers_name_prefix", "cookrpc");
    candidate.servers_ip_ =
        config_validation::ReadString(json, "servers_ip", "0.0.0.0");
    config_validation::Require(IsValidIpAddress(candidate.servers_ip_),
                               "Invalid server IP address");
    candidate.servers_port_ = static_cast<uint16_t>(
        ReadInteger(json, "servers_port", 8989, 1, 65535));
    candidate.max_connections_ = static_cast<uint32_t>(
        ReadInteger(json, "max_connections", 10000, 1, INT32_MAX));
    candidate.listen_backlog_ = static_cast<uint32_t>(
        ReadInteger(json, "listen_backlog", 1024, 1, INT32_MAX));
    candidate.timeout_ =
        static_cast<uint32_t>(ReadInteger(json, "timeout", 5000, 1, INT32_MAX));
    if (!candidate.thread_pool_config_->InitThreadPoolConfig(
            json.value("thread_pool", nlohmann::json::object())))
      return false;
    if (json.contains("register_config") &&
        !candidate.service_registry_config_->InitRegistryConfig(
            config_validation::ReadString(json, "register_config", "")))
      return false;
    servers_name_prefix_.swap(candidate.servers_name_prefix_);
    servers_ip_.swap(candidate.servers_ip_);
    servers_port_ = candidate.servers_port_;
    max_connections_ = candidate.max_connections_;
    listen_backlog_ = candidate.listen_backlog_;
    timeout_ = candidate.timeout_;
    transport_options_ = candidate.transport_options_;
    rate_limit_options_ = candidate.rate_limit_options_;
    thread_pool_config_.swap(candidate.thread_pool_config_);
    service_registry_config_.swap(candidate.service_registry_config_);
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("Failed to load server configuration: {}", error.what());
    return false;
  }
}
}  // namespace cookrpc
