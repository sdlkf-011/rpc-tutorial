#ifndef RPC_TUTORIAL_RPC_SRC_NETWORK_MESSAGE_CYCLE_H_
#define RPC_TUTORIAL_RPC_SRC_NETWORK_MESSAGE_CYCLE_H_

#include <netinet/tcp.h>
#include <pthread.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// 系统相关头文件
#ifdef __APPLE__
#include <sys/event.h>
#include <sys/time.h>
#include <sys/types.h>
#else
#include <sys/epoll.h>
#endif

#include "connection_manager.h"
#include "error_code.h"
#include "request_controls.h"
#include "rpc_protocol.h"
#include "transport_codec.h"

namespace cookrpc {

// 跨平台网络事件循环：统一处理监听 fd、客户端读事件及 RPC 分发。
class MessageCycle {
 public:
  explicit MessageCycle(ConnectionManager* manager,
                        size_t max_connections = 10000,
                        TransportOptions transport = {},
                        RateLimitOptions rate_limit = {});
  virtual ~MessageCycle();

  // 注册/注销监听文件描述符。
  bool AddListenFd(int fd);
  bool RemoveListenFd(int fd);
  size_t GetListenFdCount() const;
  // 启动事件循环（通常在线程中调用）并停止循环。
  void Loop();
  void Stop();
  void HandleRpcRequest(const std::shared_ptr<Connection>& conn,
                        const RpcRequest& request);
  // 异步处理RPC请求
  void HandleRpcRequestAsync(const std::shared_ptr<Connection>& conn,
                             const RpcRequest& request);

 private:
  struct Notifications;
  struct Client {
    std::shared_ptr<Connection> connection;
    bool subscribed = true;
    bool read_enabled = true;
    bool write_enabled = false;
  };
  std::shared_ptr<Notifications> notifications_;
  std::shared_ptr<const TransportCodec> transport_;
  RequestLimiter limiter_;
  std::unordered_map<uint64_t, Client> clients_;
  std::atomic<bool> stop_requested_{false};
  std::mutex loop_mutex_;
  std::condition_variable loop_cv_;
  static constexpr size_t kSendBudget = 64 * 1024;
  static constexpr size_t kAcceptBudget = 64;
  const size_t max_connections_;
  struct Listener {
    bool paused = false;
    std::chrono::steady_clock::time_point resume_at{};
    std::chrono::milliseconds retry_delay{100};
  };
  std::map<int, Listener> listen_fds_;
  std::atomic<bool> running_;
  mutable std::mutex mutex_;

  // 平台相关的事件循环文件描述符
#ifdef __APPLE__
  int kqueue_fd_;
#else
  int epoll_fd_;
#endif

  // 平台相关的事件处理函数
#ifdef __APPLE__
  void HandleKqueueEvents(struct kevent* events, int nfds);
#else
  void HandleEpollEvents(struct epoll_event* events, int nfds);
#endif

  void HandleNewConnection(int fd);
  bool ConfigureListener(int fd, bool enabled);
  void PauseListener(int fd, int error);
  int RetryPausedListeners();
  void HandleClientData(uint64_t id);
  // 在解压、解密前限流，再将完整帧与连接快照提交给工作线程。
  void HandleRpcFrame(const std::shared_ptr<Connection>& conn,
                      std::string frame);
  void FlushConnection(uint64_t id);
  bool UpdateInterest(uint64_t id);
  void RemoveConnection(uint64_t id);
  void ProcessNotifications();

  ConnectionManager* connection_manager_;
  static const int MAX_EVENTS = 1024;
  void RemoveInvalidFileDescriptor(int fd);
  bool IsValidFileDescriptor(int fd);
  bool ValidateRequest(const RpcRequest& request);

  // 发送成功响应,多封装一层，增加代码可读性
  void SendSuccessResponse(const std::shared_ptr<Connection>& conn,
                           uint64_t sequence_id, const std::string& result);

  // 发送错误响应
  void SendErrorResponse(const std::shared_ptr<Connection>& conn,
                         uint64_t sequence_id, cookrpc::ErrorCode error_code,
                         const std::string& error_message);

  // 通用响应发送方法（内部使用）
  void SendResponse(const std::shared_ptr<Connection>& conn,
                    const RpcResponse& response,
                    const std::string& response_type);

  // 内部同步处理RPC请求方法
  void HandleRpcRequestSync(const std::shared_ptr<Connection>& conn,
                            const RpcRequest& request);
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_NETWORK_MESSAGE_CYCLE_H_
