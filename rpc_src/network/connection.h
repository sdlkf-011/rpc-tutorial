#ifndef RPC_TUTORIAL_RPC_SRC_NETWORK_CONNECTION_H_
#define RPC_TUTORIAL_RPC_SRC_NETWORK_CONNECTION_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "rpc_protocol.h"

namespace cookrpc {

// 前置声明
class RpcMessage;
class RpcRequest;
class RpcResponse;

// 单个 TCP 连接封装：管理非阻塞收发缓冲、RPC 帧解析和生命周期回调。
class Connection : public std::enable_shared_from_this<Connection> {
 public:
  enum class State { CONNECTED, DISCONNECTING, DISCONNECTED };

  // 定义消息回调函数类型
  using MessageCallback = std::function<void(const std::shared_ptr<Connection>&,
                                             const RpcRequest&)>;
  using CloseCallback = std::function<void(const std::shared_ptr<Connection>&)>;

  explicit Connection(int fd);
  ~Connection();

  // 基础 IO 操作
  bool Read();
  bool Write(const std::string& data);
  bool Write(const RpcResponse& response);
  void Close();
  bool IsValid() const {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    return fd_ >= 0 && state_ == State::CONNECTED;
  }
  bool ReadWithTimeout(int timeout_ms);

  // 获取连接信息
  int GetFd() const {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    return fd_;
  }
  const std::string& GetPeerAddress() const { return peer_addr_; }
  uint16_t GetPeerPort() const { return peer_port_; }
  State GetState() const {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    return state_;
  }

  // 获取读缓冲区数据（客户端使用）
  std::string GetReadBufferData() {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    std::string data(read_buffer_.begin(), read_buffer_.end());
    read_buffer_.clear();  // 清空缓冲区
    return data;
  }

  // 设置回调
  void SetMessageCallback(const MessageCallback& cb) {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    message_callback_ = cb;
  }
  void SetCloseCallback(const CloseCallback& cb) {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    close_callback_ = cb;
  }

  static const size_t MAX_MESSAGE_SIZE = 1024 * 1024;  // 1MB
  // 从读缓冲区提取完整 RPC 帧并触发消息回调。
  bool ProcessMessage();

 private:
  void HandleMessage(const RpcRequest& request);
  void HandleError();
  bool SendInBuffer();
  int fd_ = -1;
  std::string peer_addr_ = "";
  uint16_t peer_port_ = 0;

  std::vector<char> read_buffer_;
  std::vector<char> write_buffer_;

  MessageCallback message_callback_ = nullptr;
  CloseCallback close_callback_ = nullptr;

  static const size_t MAX_BUFFER_SIZE = 65536;

  State state_ = State::DISCONNECTED;
  // 统一保护套接字生命周期和读写缓冲区，允许错误处理递归进入 Close。
  mutable std::recursive_mutex io_mutex_;
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_NETWORK_CONNECTION_H_
