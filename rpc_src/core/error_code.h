#ifndef RPC_TUTORIAL_RPC_SRC_CORE_ERROR_CODE_H_
#define RPC_TUTORIAL_RPC_SRC_CORE_ERROR_CODE_H_

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

namespace cookrpc {

// RPC 统一错误码。
// 不同类别使用独立区间，便于日志与监控聚合。
enum class ErrorCode : uint32_t {
  kSuccess = 0,

  // 系统错误：1-999。
  kSystemError = 1,
  kUnknownError = 2,
  kOperationTimeout = 3,
  kMemoryAllocationError = 4,

  // 网络错误：1000-1999。
  kNetworkError = 1000,
  kConnectionLost = 1001,
  kSocketError = 1002,
  kBindFailed = 1003,
  kListenFailed = 1004,
  kConnectFailed = 1005,
  kSendFailed = 1006,
  kReceiveFailed = 1007,

  // 序列化与协议错误：2000-2999。
  kSerializeError = 2000,
  kDeserializeError = 2001,
  kInvalidMessageFormat = 2002,
  kProtocolError = 2003,

  // 服务错误：3000-3999。
  kServiceNotFound = 3000,
  kMethodNotFound = 3001,
  kServiceAlreadyExists = 3002,
  kInvalidServiceName = 3003,
  kServiceUnavailable = 3004,

  // 参数错误：4000-4999。
  kInvalidArgument = 4000,
  kArgumentOutOfRange = 4001,
  kInvalidConfig = 4002,

  // 业务错误：5000-5999。
  kBusinessError = 5000,

  // 客户端请求错误：6000-6999。
  kInvalidRequest = 6000,
  kInvalidMethod = 6001,
  kInternalError = 6002,
};

// 将错误码转换为稳定、可记录的错误文本。
class Error {
 public:
  static std::string GetErrorMessage(ErrorCode code) {
    static const std::unordered_map<ErrorCode, std::string> kErrorMessages = {
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
        {ErrorCode::kInvalidRequest, "Invalid request"},
        {ErrorCode::kInvalidMethod, "Invalid method"},
        {ErrorCode::kInternalError, "Internal error"},
    };
    const auto iterator = kErrorMessages.find(code);
    return iterator == kErrorMessages.end() ? "Unknown error code"
                                            : iterator->second;
  }
};

// 封装错误码和可选上下文，供上层统一判断和输出。
class ErrorResult {
 public:
  ErrorResult() = default;
  explicit ErrorResult(ErrorCode code) : code_(code) {}
  ErrorResult(ErrorCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  bool IsOk() const { return code_ == ErrorCode::kSuccess; }

  ErrorCode Code() const { return code_; }

  std::string ToString() const {
    const std::string default_message = Error::GetErrorMessage(code_);
    return message_.empty() ? default_message
                            : default_message + ": " + message_;
  }

 private:
  ErrorCode code_ = ErrorCode::kSuccess;
  std::string message_;
};

}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CORE_ERROR_CODE_H_
