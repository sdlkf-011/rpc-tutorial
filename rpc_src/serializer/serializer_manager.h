#ifndef RPC_TUTORIAL_RPC_SRC_SERIALIZER_SERIALIZER_MANAGER_H_
#define RPC_TUTORIAL_RPC_SRC_SERIALIZER_SERIALIZER_MANAGER_H_

#include <string>
#include <type_traits>

#include <google/protobuf/message_lite.h>

#include "json_serializer.h"
#include "log_manager.h"
#include "protobuf_serializer.h"

namespace cookrpc {

// SerializerManager 支持的线格式。
enum class SerializeType {
  JSON,
  PROTOBUF,
};

// 根据数据类型和指定线格式分派到 JSON 或 Protobuf 序列化器。
// MessageLite 派生类型只允许使用 PROTOBUF，其余类型使用 JSON。
class SerializerManager {
 public:
  // 分派序列化操作；类型与格式不匹配时清空 buffer 并返回 false。
  template <typename T>
  static bool Serialize(SerializeType type, const T& data,
                        std::string& buffer) {
    if constexpr (IsProtobufMessage<T>()) {
      if (type == SerializeType::PROTOBUF) {
        return ProtobufSerializer::Serialize(data, buffer);
      }
    } else {
      if (type == SerializeType::JSON) {
        return JsonSerializer::Serialize(data, buffer);
      }
    }

    LOG_ERROR("Serializer type does not match the data type");
    buffer.clear();
    return false;
  }

  // 字符串便捷接口；序列化失败时返回空字符串。
  template <typename T>
  static std::string Serialize(SerializeType type, const T& data) {
    std::string buffer;
    Serialize(type, data, buffer);
    return buffer;
  }

  // 分派反序列化操作；类型与格式不匹配时保持 data 原值不变。
  template <typename T>
  static bool Deserialize(SerializeType type, const std::string& buffer,
                          T& data) {
    if constexpr (IsProtobufMessage<T>()) {
      if (type == SerializeType::PROTOBUF) {
        return ProtobufSerializer::Deserialize(buffer, data);
      }
    } else {
      if (type == SerializeType::JSON) {
        return JsonSerializer::Deserialize(buffer, data);
      }
    }

    LOG_ERROR("Deserializer type does not match the data type");
    return false;
  }

 private:
  // 在编译期识别由 protoc 生成的 MessageLite 派生类型。
  template <typename T>
  static constexpr bool IsProtobufMessage() {
    return std::is_base_of_v<google::protobuf::MessageLite,
                             std::remove_cv_t<std::remove_reference_t<T>>>;
  }
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_SERIALIZER_SERIALIZER_MANAGER_H_
