#include <netinet/tcp.h>
#include <pthread.h>

#include <cstdint>
#include <memory>
#include <atomic>
#include <mutex>
#include <set>
#include <string>

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
#include "rpc_protocol.h"

namespace cookrpc {
  
class MessageCycle {
 public:
  MessageCycle(ConnectionManager* manager);
  virtual ~MessageCycle();
  
  bool AddListenFd(int fd);
  bool RemoveListenFd(int fd);
  size_t GetListenFdCount() const;
  void Loop();
  void Stop();
  void HandleRpcRequest(const std::shared_ptr<Connection>& conn,
                        const RpcRequest& request);
  // 异步处理RPC请求
  void HandleRpcRequestAsync(const std::shared_ptr<Connection>& conn,
                             const RpcRequest& request);

 private:
  pthread_t thread_id_;
  std::set<int> listen_fds_;
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
  void HandleClientData(int fd);
  void RemoveConnection(int fd);

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
} // namespace cookrpc
