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

#include "connection.h"
#include "error_code.h"
#include "log_manager.h"
#include "request_controls.h"
#include "rpc_protocol.h"
#include "serializer_manager.h"
#include "transport_codec.h"

namespace cookrpc {

class ZkConnHandler;

// RPC 客户端在单条 TCP 连接上串行执行调用。
// 无需额外响应分发线程即可准确匹配请求与响应。
namespace detail {
class RpcClientSession {
 public:
  // 使用指定配置文件创建客户端；未提供路径时使用默认配置。
  explicit RpcClientSession(
      const std::string& config_path = "../config/rpc_client.json");
  // 释放连接及其相关资源。
  ~RpcClientSession();

  RpcClientSession(const RpcClientSession&) = delete;
  RpcClientSession& operator=(const RpcClientSession&) = delete;
  RpcClientSession(RpcClientSession&&) = delete;
  RpcClientSession& operator=(RpcClientSession&&) = delete;

  // 加载客户端配置，成功后可调用 Connect 建立连接。
  bool LoadConfig(const std::string& config_path);
  // 按配置连接服务发现组件并建立 TCP 连接。
  bool Connect();
  // 关闭当前连接；可安全重复调用。
  void Disconnect();

  // 在当前连接上发送请求并同步等待对应响应。
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

      const std::string endpoint = endpoint_;
      if (!breaker_.TryAcquire(endpoint)) {
        CLIENT_WARN("Circuit breaker rejected endpoint {}", endpoint);
        return false;
      }
      // 所有返回路径都更新熔断状态；中性结果用于参数、序列化等本地失败。
      struct Completion {
        CircuitBreaker& breaker;
        const std::string& endpoint;
        CircuitBreaker::Outcome outcome = CircuitBreaker::Outcome::kNeutral;
        ~Completion() { breaker.Finish(endpoint, outcome); }
      } completion{breaker_, endpoint};
      remote_failure_ = false;
      const uint32_t sequence_id = GenerateSequenceId();
      std::string request_frame;
      if (!PrepareRequest(service_name, method_name, request, serialize_type,
                          sequence_id, request_frame))
        return false;
      // 请求开始发送后，异常或提前退出默认视为远端通信失败。
      completion.outcome = CircuitBreaker::Outcome::kFailure;
      if (!SendRequest(request_frame)) {
        if (!remote_failure_)
          completion.outcome = CircuitBreaker::Outcome::kNeutral;
        return false;
      }
      const bool success =
          ProcessResponse(sequence_id, serialize_type, response);
      completion.outcome = remote_failure_ ? CircuitBreaker::Outcome::kFailure
                                           : CircuitBreaker::Outcome::kHealthy;
      if (remote_failure_) DisconnectUnlocked();
      return success;
    } catch (const std::exception& error) {
      CLIENT_ERROR("RPC call failed: {}", error.what());
    } catch (...) {
      CLIENT_ERROR("RPC call failed with an unknown error");
    }
    return false;
  }

  // 返回客户端当前是否持有可用连接。
  bool IsConnected() const {
    return is_connected_.load(std::memory_order_acquire);
  }

 private:
  // 连接生命周期管理模块。
  bool ConnectUnlocked();
  void DisconnectUnlocked();
  bool Reconnect();
  bool CheckConnection();

  // 服务发现与套接字连接模块。
  std::string GetServerAddress();
  bool ParseServerAddress(const std::string& server_address,
                          std::string& server_ip, uint16_t& server_port) const;
  bool InitSocket(int& fd) const;
  bool TryConnect(int fd, const std::string& server_ip, uint16_t server_port,
                  int attempt) const;
  bool WaitForConnection(int fd, int attempt) const;

  // 请求编号生成与协议封装模块。
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

    constexpr size_t kHeaderSize = RpcHeader::kHeaderSize;
    if (request_frame.size() > Connection::MAX_MESSAGE_SIZE + kHeaderSize) {
      CLIENT_ERROR("RPC request frame exceeds the maximum size");
      request_frame.clear();
      return false;
    }
    return true;
  }

  // 响应收帧、协议解析与结果反序列化模块。
  bool SendRequest(const std::string& request_frame);
  bool ReadResponseFrame(std::string& response_frame);

  template <typename Response>
  bool ProcessResponse(uint32_t expected_sequence_id,
                       SerializeType serialize_type, Response& response) {
    std::string response_frame;
    if (!ReadResponseFrame(response_frame)) {
      remote_failure_ = true;
      return false;
    }

    RpcResponse rpc_response;
    if (!rpc_response.Deserialize(response_frame)) {
      CLIENT_ERROR("Failed to deserialize RPC response frame");
      remote_failure_ = true;
      DisconnectUnlocked();
      return false;
    }
    if (rpc_response.GetSequenceId() != expected_sequence_id) {
      CLIENT_ERROR("RPC response sequence mismatch: expected {}, got {}",
                   expected_sequence_id, rpc_response.GetSequenceId());
      // 序列错位后无法安全判断后续帧归属。
      // 主动重连以恢复请求边界。
      remote_failure_ = true;
      DisconnectUnlocked();
      return false;
    }
    if (rpc_response.GetErrorCode() !=
        static_cast<uint32_t>(ErrorCode::kSuccess)) {
      // 业务拒绝和限流仍证明连接健康；服务不可用和内部故障计入熔断。
      remote_failure_ =
          rpc_response.GetErrorCode() ==
              static_cast<uint32_t>(ErrorCode::kServiceUnavailable) ||
          rpc_response.GetErrorCode() ==
              static_cast<uint32_t>(ErrorCode::kInternalError);
      CLIENT_ERROR("RPC server returned error {}: {}",
                   rpc_response.GetErrorCode(), rpc_response.GetErrorMessage());
      return false;
    }
    if (!SerializerManager::Deserialize(
            serialize_type, rpc_response.GetResultData(), response)) {
      remote_failure_ = true;
      CLIENT_ERROR("Failed to deserialize RPC response payload");
      return false;
    }
    return true;
  }

  // 客户端配置与服务发现状态。
  std::shared_ptr<const TransportCodec> transport_ =
      std::make_shared<TransportCodec>();
  CircuitBreaker breaker_;
  std::string endpoint_;
  bool remote_failure_ = false;
  int timeout_ms_ = 3000;
  int retry_times_ = 3;
  int default_server_port_ = 8989;
  std::string zk_namespace_;
  bool is_configured_ = false;
  std::unique_ptr<ZkConnHandler> discovery_;

  // TCP 连接及待读取响应缓存。
  int socket_fd_ = -1;
  std::shared_ptr<Connection> connection_;
  std::string pending_response_data_;
  mutable std::mutex mutex_;

  // 调用并发控制与请求序列状态。
  std::atomic<uint32_t> sequence_id_{0};
  std::atomic<bool> is_connected_{false};
};

}  // namespace detail

// 客户端对象只持有共享会话，异步任务独立拥有会话及其连接资源。
class RpcClient {
 public:
  explicit RpcClient(const std::string& path = "../config/rpc_client.json")
      : session_(std::make_shared<detail::RpcClientSession>(path)) {}
  ~RpcClient() = default;
  RpcClient(const RpcClient&) = delete;
  RpcClient& operator=(const RpcClient&) = delete;
  RpcClient(RpcClient&&) = delete;
  RpcClient& operator=(RpcClient&&) = delete;
  bool LoadConfig(const std::string& path) {
    return session_->LoadConfig(path);
  }
  bool Connect() { return session_->Connect(); }
  void Disconnect() { session_->Disconnect(); }
  bool IsConnected() const { return session_->IsConnected(); }
  template <typename Response, typename Request>
  bool Call(const std::string& service, const std::string& method,
            const Request& request, SerializeType type, Response& response) {
    return session_->Call(service, method, request, type, response);
  }
  template <typename Response, typename Request>
  std::future<Response> AsyncCall(const std::string& service,
                                  const std::string& method,
                                  const Request& request, SerializeType type) {
    // 捕获会话而非 this，客户端对象销毁后，已发起的任务仍能安全完成。
    return std::async(std::launch::async, [session = session_, service, method,
                                           request, type] {
      Response response{};
      if (!session->Call(service, method, request, type, response))
        throw std::runtime_error("RPC call failed");
      return response;
    });
  }

 private:
  std::shared_ptr<detail::RpcClientSession> session_;
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CORE_RPC_CLIENT_H_
