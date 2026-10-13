#ifndef RPC_TUTORIAL_RPC_SRC_SERIALIZER_SERIALIZER_MANAGER_H_
#define RPC_TUTORIAL_RPC_SRC_SERIALIZER_SERIALIZER_MANAGER_H_

#include <google/protobuf/message_lite.h>

#include <string>
#include <type_traits>

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
  // 按类型类别检查线格式，不读取业务数据，供调用方在建连和发送前校验。
  template <typename T>
  static constexpr bool IsCompatible(SerializeType type) {
    return IsProtobufMessage<T>() ? type == SerializeType::PROTOBUF
                                  : type == SerializeType::JSON;
  }

  // 分派序列化操作；类型与格式不匹配时清空 buffer 并返回 false。
  template <typename T>
  static bool Serialize(SerializeType type, const T& data,
                        std::string& buffer) {
    if (IsCompatible<T>(type)) {
      if constexpr (IsProtobufMessage<T>()) {
        return ProtobufSerializer::Serialize(data, buffer);
      } else {
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
    if (IsCompatible<T>(type)) {
      if constexpr (IsProtobufMessage<T>()) {
        return ProtobufSerializer::Deserialize(buffer, data);
      } else {
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
