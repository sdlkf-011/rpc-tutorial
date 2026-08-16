#ifndef RPC_TUTORIAL_RPC_SRC_CORE_RPC_CLIENT_H_
#define RPC_TUTORIAL_RPC_SRC_CORE_RPC_CLIENT_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "core/error_code.h"
#include "log_manager.h"
#include "network/connection.h"
#include "protocol/rpc_protocol.h"
#include "serializer/serializer_manager.h"

namespace cookrpc {

// RPC 客户端在单条 TCP 连接上串行执行调用。
// 无需额外响应分发线程即可准确匹配请求与响应。
class RpcClient {
 public:
  explicit RpcClient(
      const std::string& config_path = "../config/rpc_client.json");
  ~RpcClient();

  RpcClient(const RpcClient&) = delete;
  RpcClient& operator=(const RpcClient&) = delete;
  RpcClient(RpcClient&&) = delete;
  RpcClient& operator=(RpcClient&&) = delete;

  bool LoadConfig(const std::string& config_path);
  bool Connect();
  void Disconnect();

  template <typename Response, typename Request>
  bool Call(const std::string& service_name, const std::string& method_name,
            const Request& request, SerializeType serialize_type,
            Response& response) {
    std::lock_guard<std::mutex> lock(mutex_);

    try {
      if (service_name.empty() || method_name.empty()) {
        CLIENT_ERROR("Service name and method name must not be empty");
        return false;
      }
      if (!CheckConnection()) {
        return false;
      }

      const uint32_t sequence_id = GenerateSequenceId();
      std::string request_frame;
      if (!PrepareRequest(service_name, method_name, request, serialize_type,
                          sequence_id, request_frame) ||
          !SendRequest(request_frame)) {
        return false;
      }
      return ProcessResponse(sequence_id, serialize_type, response);
    } catch (const std::exception& error) {
      CLIENT_ERROR("RPC call failed: {}", error.what());
    } catch (...) {
      CLIENT_ERROR("RPC call failed with an unknown error");
    }
    return false;
  }

  // 异步任务捕获当前对象。
  // 调用方必须保证客户端生命周期长于 future。
  template <typename Response, typename Request>
  std::future<Response> AsyncCall(const std::string& service_name,
                                  const std::string& method_name,
                                  const Request& request,
                                  SerializeType serialize_type) {
    return std::async(
        std::launch::async,
        [this, service_name, method_name, request, serialize_type] {
          Response response{};
          if (!Call(service_name, method_name, request, serialize_type,
                    response)) {
            throw std::runtime_error("RPC call failed");
          }
          return response;
        });
  }

  bool IsConnected() const {
    return is_connected_.load(std::memory_order_acquire);
  }

 private:
  // 连接生命周期模块。
  bool ConnectUnlocked();
  void DisconnectUnlocked();
  bool Reconnect();
  bool CheckConnection();

  // 服务发现与套接字连接模块。
  std::string GetServerAddress() const;
  bool ParseServerAddress(const std::string& server_address,
                          std::string& server_ip, uint16_t& server_port) const;
  bool InitSocket(int& fd) const;
  bool TryConnect(int fd, const std::string& server_ip, uint16_t server_port,
                  int attempt) const;
  bool WaitForConnection(int fd, int attempt) const;

  // 请求编号与协议封装模块。
  uint32_t GenerateSequenceId();

  template <typename Request>
  bool PrepareRequest(const std::string& service_name,
                      const std::string& method_name, const Request& request,
                      SerializeType serialize_type, uint32_t sequence_id,
                      std::string& request_frame) const {
    std::string payload;
    if (!SerializerManager::Serialize(serialize_type, request, payload)) {
      CLIENT_ERROR("Failed to serialize RPC request payload");
      return false;
    }

    RpcRequest rpc_request;
    rpc_request.SetServiceName(service_name);
    rpc_request.SetMethodName(method_name);
    rpc_request.SetPayload(payload);
    rpc_request.SetSequenceId(sequence_id);
    if (!rpc_request.Serialize(request_frame)) {
      CLIENT_ERROR("Failed to serialize RPC request frame");
      return false;
    }

    constexpr size_t kHeaderSize = 3 * sizeof(uint32_t);
    if (request_frame.size() > Connection::MAX_MESSAGE_SIZE + kHeaderSize) {
      CLIENT_ERROR("RPC request frame exceeds the maximum size");
      request_frame.clear();
      return false;
    }
    return true;
  }

  // 响应收帧与反序列化模块。
  bool SendRequest(const std::string& request_frame);
  bool ReadResponseFrame(std::string& response_frame);

  template <typename Response>
  bool ProcessResponse(uint32_t expected_sequence_id,
                       SerializeType serialize_type, Response& response) {
    std::string response_frame;
    if (!ReadResponseFrame(response_frame)) {
      return false;
    }

    RpcResponse rpc_response;
    if (!rpc_response.Deserialize(response_frame)) {
      CLIENT_ERROR("Failed to deserialize RPC response frame");
      DisconnectUnlocked();
      return false;
    }
    if (rpc_response.GetSequenceId() != expected_sequence_id) {
      CLIENT_ERROR("RPC response sequence mismatch: expected {}, got {}",
                   expected_sequence_id, rpc_response.GetSequenceId());
      // 序列错位后无法安全判断后续帧归属。
      // 主动重连以恢复请求边界。
      DisconnectUnlocked();
      return false;
    }
    if (rpc_response.GetErrorCode() !=
        static_cast<uint32_t>(ErrorCode::kSuccess)) {
      CLIENT_ERROR("RPC server returned error {}: {}",
                   rpc_response.GetErrorCode(),
                   rpc_response.GetErrorMessage());
      return false;
    }
    if (!SerializerManager::Deserialize(
            serialize_type, rpc_response.GetResultData(), response)) {
      CLIENT_ERROR("Failed to deserialize RPC response payload");
      return false;
    }
    return true;
  }

  // 配置状态。
  int timeout_ms_ = 3000;
  int retry_times_ = 3;
  int default_server_port_ = 8989;
  std::string zk_namespace_;
  bool is_configured_ = false;

  // TCP 连接与接收缓存。
  int socket_fd_ = -1;
  std::shared_ptr<Connection> connection_;
  std::string pending_response_data_;
  mutable std::mutex mutex_;

  // 并发控制与请求序列。
  std::atomic<uint32_t> sequence_id_{0};
  std::atomic<bool> is_connected_{false};
};

}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CORE_RPC_CLIENT_H_
