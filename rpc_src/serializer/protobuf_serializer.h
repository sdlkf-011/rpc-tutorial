#ifndef RPC_TUTORIAL_RPC_SRC_SERIALIZER_PROTOBUF_SERIALIZER_H_
#define RPC_TUTORIAL_RPC_SRC_SERIALIZER_PROTOBUF_SERIALIZER_H_

#include <google/protobuf/message_lite.h>

#include <exception>
#include <memory>
#include <string>

#include "log_manager.h"

namespace cookrpc {

// 基于 MessageLite 接口的 Protobuf 二进制序列化器，兼容所有生成消息。
class ProtobufSerializer {
 public:
  // 将消息序列化到 buffer；失败时记录错误、清空 buffer 并返回 false。
  static bool Serialize(const google::protobuf::MessageLite& data,
                        std::string& buffer) {
    try {
      buffer.clear();
      if (data.SerializeToString(&buffer)) {
        return true;
      }
      LOG_ERROR("Failed to serialize protobuf message");
    } catch (const std::exception& error) {
      LOG_ERROR("Protobuf serialization error: {}", error.what());
    } catch (...) {
      LOG_ERROR("Protobuf serialization failed with an unknown error");
    }

    buffer.clear();
    return false;
  }

  // 字符串便捷接口；序列化失败时返回空字符串。
  static std::string Serialize(const google::protobuf::MessageLite& data) {
    std::string buffer;
    Serialize(data, buffer);
    return buffer;
  }

  // 从二进制数据解析消息；失败时保持 data 原值不变。
  static bool Deserialize(const std::string& buffer,
                          google::protobuf::MessageLite& data) {
    try {
      // 使用同类型临时消息解析，避免失败的解析过程污染目标消息。
      std::unique_ptr<google::protobuf::MessageLite> parsed_data(data.New());
      if (!parsed_data || !parsed_data->ParseFromString(buffer)) {
        LOG_ERROR("Failed to parse protobuf message");
        return false;
      }

      // 解析成功后再提交结果；临时消息由 data.New() 保证类型一致。
      data.Clear();
      data.CheckTypeAndMergeFrom(*parsed_data);
      return true;
    } catch (const std::exception& error) {
      LOG_ERROR("Protobuf deserialization error: {}", error.what());
    } catch (...) {
      LOG_ERROR("Protobuf deserialization failed with an unknown error");
    }

    return false;
  }
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_SERIALIZER_PROTOBUF_SERIALIZER_H_
