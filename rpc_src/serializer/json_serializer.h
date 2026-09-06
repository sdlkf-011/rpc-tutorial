#ifndef RPC_TUTORIAL_RPC_SRC_SERIALIZER_JSON_SERIALIZER_H_
#define RPC_TUTORIAL_RPC_SRC_SERIALIZER_JSON_SERIALIZER_H_

#include <exception>
#include <nlohmann/json.hpp>
#include <string>
#include <utility>

#include "log_manager.h"

namespace cookrpc {

// 基于 nlohmann::json 的通用 JSON 序列化器。
// 自定义类型需要通过 ADL 提供 to_json/from_json 转换函数。
class JsonSerializer {
 public:
  // 将对象序列化到 buffer；失败时记录错误、清空 buffer 并返回 false。
  template <typename T>
  static bool Serialize(const T& data, std::string& buffer) {
    try {
      buffer = nlohmann::json(data).dump();
      return true;
    } catch (const std::exception& error) {
      LOG_ERROR("JSON serialization error: {}", error.what());
    } catch (...) {
      LOG_ERROR("JSON serialization failed with an unknown error");
    }

    buffer.clear();
    return false;
  }

  // 字符串便捷接口；序列化失败时返回空字符串。
  template <typename T>
  static std::string Serialize(const T& data) {
    std::string buffer;
    Serialize(data, buffer);
    return buffer;
  }

  // 将 JSON 文本反序列化为对象；失败时保持 data 原值不变。
  template <typename T>
  static bool Deserialize(const std::string& buffer, T& data) {
    try {
      // 先完成解析和类型转换，再更新输出对象，避免暴露中间状态。
      T parsed_data = nlohmann::json::parse(buffer).template get<T>();
      data = std::move(parsed_data);
      return true;
    } catch (const std::exception& error) {
      LOG_ERROR("JSON deserialization error: {}", error.what());
    } catch (...) {
      LOG_ERROR("JSON deserialization failed with an unknown error");
    }

    return false;
  }
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_SERIALIZER_JSON_SERIALIZER_H_
