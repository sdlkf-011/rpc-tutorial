#ifndef RPC_TUTORIAL_CORE_ERROR_CODE_H_
#define RPC_TUTORIAL_CORE_ERROR_CODE_H_

#include <cstdint>
#include <string>
#include <unordered_map>

namespace cookrpc {

// RPC 错误码枚举
enum class ErrorCode : uint32_t {
  // 成功
  kSuccess = 0,

  // 系统级错误 (1-999)
  kSystemError = 1,
  kUnknownError = 2,
  kOperationTimeout = 3,
  kMemoryAllocationError = 4,

  // 网络相关错误 (1000-1999)
  kNetworkError = 1000,
  kConnectionLost = 1001,
  kSocketError = 1002,
  kBindFailed = 1003,
  kListenFailed = 1004,
  kConnectFailed = 1005,
  kSendFailed = 1006,
  kReceiveFailed = 1007,

  // 序列化相关错误 (2000-2999)
  kSerializeError = 2000,
  kDeserializeError = 2001,
  kInvalidMessageFormat = 2002,
  kProtocolError = 2003,

  // 服务相关错误 (3000-3999)
  kServiceNotFound = 3000,
  kMethodNotFound = 3001,
  kServiceAlreadyExists = 3002,
  kInvalidServiceName = 3003,
  kServiceUnavailable = 3004,

  // 参数相关错误 (4000-4999)
  kInvalidArgument = 4000,
  kArgumentOutOfRange = 4001,
  kInvalidConfig = 4002,

  // 其他业务错误 (5000-5999)
  kBusinessError = 5000,

  // 客户端错误 (6000-6999)
  kInvalidRequest = 6000,
  kInvalidMethod = 6001,
  kInternalError = 6002,
};

// 错误信息类
class Error {
 public:
  static std::string GetErrorMessage(ErrorCode code) {
    static const std::unordered_map<ErrorCode, std::string> error_message {
      {ErrorCode::kSuccess, "Success"},
      {ErrorCode::kSystemError, "System error"},
      {ErrorCode::kUnknownError, "Unknown error"},
      {ErrorCode::kOperationTimeout, "Operation timeout"},
      {ErrorCode::kMemoryAllocationError, "Memory allocation failed"},

      {ErrorCode::kNetworkError, "Network error"},
      {ErrorCode::kConnectionLost, "Connection lost"},
      {ErrorCode::kSocketError, "Socket error"},
      {ErrorCode::kBindFailed, "Bind failed"},
      {ErrorCode::kListenFailed, "Listen failed"},
      {ErrorCode::kConnectFailed, "Connect failed"},
      {ErrorCode::kSendFailed, "Send failed"},
      {ErrorCode::kReceiveFailed, "Receive failed"},

      {ErrorCode::kSerializeError, "Serialize error"},
      {ErrorCode::kDeserializeError, "Deserialize error"},
      {ErrorCode::kInvalidMessageFormat, "Invalid message format"},
      {ErrorCode::kProtocolError, "Protocol error"},

      {ErrorCode::kServiceNotFound, "Service not found"},
      {ErrorCode::kMethodNotFound, "Method not found"},
      {ErrorCode::kServiceAlreadyExists, "Service already exists"},
      {ErrorCode::kInvalidServiceName, "Invalid service name"},
      {ErrorCode::kServiceUnavailable, "Service unavailable"},

      {ErrorCode::kInvalidArgument, "Invalid argument"},
      {ErrorCode::kArgumentOutOfRange, "Argument out of range"},
      {ErrorCode::kInvalidConfig, "Invalid configuration"},

      {ErrorCode::kBusinessError, "Business error"},
    };
    auto it = error_message.find(code);
    return it != error_message.end() ? it->second : "Unknown error code";
  }
};

// 错误结果类
class ErrorResult {
 public:
  ErrorResult() : code_(ErrorCode::kSuccess) {}
  explicit ErrorResult(ErrorCode code) : code_(code) {}
  ErrorResult(ErrorCode code, const std::string& message)
    : code_(code), message_(message) {}
  
    bool IsOk() const {
      return code_ == ErrorCode::kSuccess;
    }

    ErrorCode Code() const {
      return code_;
    }

    std::string ToString() const {
      if (message_.empty()){
        return Error::GetErrorMessage(code_);
      }
      return Error::GetErrorMessage(code_) + ":" + message_;
    }
 private:
  ErrorCode code_;
  std::string message_;
};

} // namespace cookrpc

#endif // RPC_TUTORIAL_CORE_ERROR_CODE_H_
