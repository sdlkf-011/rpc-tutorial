#include "rpc_options.h"

#include "config_validation.h"

namespace cookrpc {
namespace {
bool ReadBool(const nlohmann::json& config, const char* key) {
  // 禁止把整数、字符串等值隐式转换成开关，省略字段时默认关闭。
  config_validation::RequireObject(config);
  if (!config.contains(key)) return false;
  config_validation::Require(config.at(key).is_boolean(),
                             "Configuration field must be boolean");
  return config.at(key).get<bool>();
}
}  // namespace

TransportOptions TransportOptions::Parse(const nlohmann::json& config) {
  TransportOptions options;
  options.compression = ReadBool(config, "compression");
  options.encryption = ReadBool(config, "encryption");
  options.compression_threshold = config_validation::ReadInteger(
      config, "compression_threshold", 1024, 0, 1024 * 1024);
  return options;
}

RateLimitOptions RateLimitOptions::Parse(const nlohmann::json& config) {
  RateLimitOptions options;
  options.requests_per_second = config_validation::ReadInteger(
      config, "requests_per_second", 0, 0, 1000000000);
  // 未指定突发容量时允许积攒一秒的配额；启用限流后不能使用空桶容量。
  options.burst = config_validation::ReadInteger(
      config, "burst", options.requests_per_second, 0, 1000000000);
  config_validation::Require(
      options.requests_per_second == 0 || options.burst > 0,
      "Enabled rate limiter requires a positive burst");
  return options;
}

CircuitBreakerOptions CircuitBreakerOptions::Parse(
    const nlohmann::json& config) {
  CircuitBreakerOptions options;
  options.failure_threshold = config_validation::ReadInteger(
      config, "failure_threshold", 5, 0, 1000000);
  options.reset_timeout_ms = config_validation::ReadInteger(
      config, "reset_timeout_ms", 5000, 1, INT32_MAX);
  return options;
}
}  // namespace cookrpc
