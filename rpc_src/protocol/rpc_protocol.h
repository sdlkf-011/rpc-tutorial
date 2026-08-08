#ifndef RPC_TUTORIAL_RPC_SRC_PROTOCOL_RPC_PROTOCOL_H_
#define RPC_TUTORIAL_RPC_SRC_PROTOCOL_RPC_PROTOCOL_H_

#include <cstdint>
#include <string>
#include <sys/types.h>

namespace cookrpc {

// RPC 消息头
struct RpcHeader {
  uint32_t magic_number; // 魔数
  uint32_t message_length; // 消息体长度
  uint32_t sequence_id; // 序列号

  static const uint32_t MAGIC; // 魔数常量定义
};

// RPC 消息基类
class RpcMessage {
 public:
  virtual ~RpcMessage() = default;

  // 序列化消息到字符串
  virtual bool Serialize(std::string& out) const = 0;
 
  // 序列化消息到字符串
  virtual bool Deserialize(const std::string& in) = 0;

  // 序列号访问
  uint32_t GetSequenceId() const {
    return sequence_id_;
  }

  void SetSequenceId(uint32_t sequence_id) {
    sequence_id_ = sequence_id;
  }
 protected:
  uint32_t sequence_id_ = 0; // 序列号
};

// RPC 请求消息
class RpcRequest : public RpcMessage {
 public:
  // Setter 方法
  void SetPayload(const std::string& payload) {
    payload_ = payload;
  }

  void SetServiceName(const std::string& service_name) {
    service_name_ = service_name;
  }

  void SetMethodName(const std::string& method_name) {
    method_name_ = method_name;
  }

  // Getter 方法
  std::string GetPayload() const {
    return payload_;
  }

  uint32_t GetSequenceId() const {
    return sequence_id_;
  }

  std::string GetSeviceName() const {
    return service_name_;
  }

  std::string GetServiceName() const {
    return service_name_;
  }

  std::string GetMethodName() const {
    return method_name_;
  }

  // 实现基类的纯虚函数
  bool Serialize(std::string& out) const override;
  bool Deserialize(const std::string& in) override;
 private:
  std::string service_name_; // 服务名
  std::string method_name_; // 方法名
  std::string payload_; // 序列化后的参数数据
};

// RPC 响应消息
class RpcResponse : public RpcMessage {
 public:
 
 // Getter 方法
 std::string GetResultData() const {
   return result_data_;
  }
  
  std::string GetErrorMessage() const {
    return error_message_;
  }
  
  uint32_t GetErrorCode() const {
    return error_code_;
  }
  
  // Setter 方法
  void SetResultData(const std::string& result_data) {
    result_data_ = result_data;
  }
  
  void SetErrorMessage(const std::string& error_message) {
    error_message_ = error_message;
  }
  
  void SetErrorCode(uint32_t error_code) {
    error_code_ = error_code;
  }
  
  // 实现基类的纯虚函数
  bool Serialize(std::string& out) const override;
  bool Deserialize(const std::string& in) override;
 private:
  std::string result_data_; // 序列化后的结果数据
  std::string error_message_; // 错误消息
  uint32_t error_code_ = 0; // 错误码
};
} // namespace cookrpc

#endif // RPC_TUTORIAL_RPC_SRC_PROTOCOL_RPC_PROTOCOL_H_
