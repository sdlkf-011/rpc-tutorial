#ifndef RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_CONFIG_VALIDATION_H_
#define RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_CONFIG_VALIDATION_H_

#include <cstdint>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>

namespace cookrpc::config_validation {
inline void Require(bool valid, const char* message) {
  if (!valid) throw std::invalid_argument(message);
}
inline void RequireObject(const nlohmann::json& config) {
  Require(config.is_object(), "Configuration must be an object");
}
// 校验原始 JSON 类型和宽整数范围后才转换，禁止布尔、浮点和整数回绕。
inline int64_t ReadInteger(const nlohmann::json& config, const char* key,
                           int64_t fallback, int64_t minimum, int64_t maximum) {
  RequireObject(config);
  if (!config.contains(key)) {
    Require(fallback >= minimum && fallback <= maximum,
            "Required configuration integer is missing");
    return fallback;
  }
  const auto& value = config.at(key);
  Require(value.is_number_integer(), "Configuration field must be an integer");
  int64_t integer;
  if (value.is_number_unsigned()) {
    const auto number = value.get<uint64_t>();
    Require(number <= static_cast<uint64_t>(maximum),
            "Configuration integer is out of range");
    integer = static_cast<int64_t>(number);
  } else {
    integer = value.get<int64_t>();
  }
  Require(integer >= minimum && integer <= maximum,
          "Configuration integer is out of range");
  return integer;
}
inline std::string ReadString(const nlohmann::json& config, const char* key,
                              const char* fallback) {
  RequireObject(config);
  std::string value = config.value(key, fallback);
  Require(!value.empty() && value.find('\0') == std::string::npos,
          "Configuration string must not be empty or contain NUL");
  return value;
}
}  // namespace cookrpc::config_validation
#endif
