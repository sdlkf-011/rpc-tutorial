#ifndef RPC_TUTORIAL_RPC_SRC_NETWORK_CONNECTION_H_
#define RPC_TUTORIAL_RPC_SRC_NETWORK_CONNECTION_H_

#include <cstddef>
#include <cstdint>
#include <deque>
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
  struct ReadResult {
    size_t bytes_received = 0;
    bool eof = false;
    bool error = false;
    bool timeout = false;
  };
  enum class FlushResult { kDrained, kPending, kError };
  using EventCallback = std::function<bool(const std::shared_ptr<Connection>&)>;

  ReadResult Read();
  bool Write(const std::string& data);
  bool Write(const RpcResponse& response);
  void Close();
  bool IsValid() const {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    return fd_ >= 0 && state_ == State::CONNECTED && !close_requested_;
  }
  ReadResult ReadWithTimeout(int timeout_ms);
  bool WriteWithTimeout(const std::string& data, int timeout_ms);
  // 事件循环绑定后，Write 的成功表示完整帧已入队，只有事件线程刷新和关闭 fd。
  void SetEventCallback(EventCallback callback);
  FlushResult FlushWrites(size_t budget);
  void BeginRequest();
  void FinishRequest();
  bool ShouldClose() const;
  bool IsIdle() const;
  bool HasReadEof() const;
  size_t PendingWriteBytes() const;
  uint64_t GetId() const { return id_; }
  static constexpr size_t kMaxWriteBytes = 4 * RpcHeader::kMaxFrameSize;
  static constexpr size_t kMaxWriteFrames = 64;

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

  static constexpr size_t MAX_MESSAGE_SIZE = RpcHeader::kMaxBodySize;
  // 从读缓冲区提取完整 RPC 帧并触发消息回调。
  bool ProcessMessage();

 private:
  void HandleMessage(const RpcRequest& request);
  void HandleError();
  friend class MessageCycle;
  bool QueueFrame(std::string data);
  void CloseFromEventLoop();
  bool NotifyEvent();
  int fd_ = -1;
  std::string peer_addr_ = "";
  uint16_t peer_port_ = 0;

  std::vector<char> read_buffer_;
  std::deque<std::string> write_frames_;
  size_t write_offset_ = 0;
  size_t write_bytes_ = 0;
  size_t in_flight_ = 0;
  bool read_eof_ = false;
  bool close_requested_ = false;
  bool event_managed_ = false;
  EventCallback event_callback_;
  const uint64_t id_;

  MessageCallback message_callback_ = nullptr;
  CloseCallback close_callback_ = nullptr;

  static const size_t MAX_BUFFER_SIZE = 65536;

  State state_ = State::DISCONNECTED;
  // 统一保护套接字生命周期和读写缓冲区，允许错误处理递归进入 Close。
  mutable std::recursive_mutex io_mutex_;
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_NETWORK_CONNECTION_H_
