#include "message_cycle.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <future>
#include <memory>
#include <string>

#include "log_manager.h"
#include "service_manager.h"
#include "thread_pool_singleton.h"

namespace cookrpc {
namespace {

// 事件循环辅助模块：为监听和客户端 fd 统一配置非阻塞及 CLOEXEC。
bool SetNonBlockingAndCloseOnExec(int fd) {
  const int status_flags = fcntl(fd, F_GETFL, 0);
  if (status_flags == -1 ||
      fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) == -1) {
    return false;
  }
  const int descriptor_flags = fcntl(fd, F_GETFD, 0);
  return descriptor_flags != -1 &&
         fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != -1;
}

ErrorCode ValidateRpcRequest(const RpcRequest& request) {
  // 请求校验模块：在进入业务层前检查服务名、方法名和载荷大小。
  if (request.GetServiceName().empty()) {
    return ErrorCode::kInvalidServiceName;
  }
  if (request.GetMethodName().empty()) {
    return ErrorCode::kInvalidMethod;
  }
  if (request.GetPayload().size() > Connection::MAX_MESSAGE_SIZE) {
    return ErrorCode::kInvalidRequest;
  }
  return ErrorCode::kSuccess;
}

void ProcessRpcRequest(const std::shared_ptr<Connection>& connection,
                       const RpcRequest& request) {
  // 请求执行模块：调用服务管理器处理业务，并将成功或错误封装成响应。
  if (connection == nullptr || !connection->IsValid()) {
    return;
  }

  RpcResponse response;
  // 响应沿用请求序列号，客户端据此匹配异步返回结果。
  response.SetSequenceId(request.GetSequenceId());
  ErrorCode error_code = ValidateRpcRequest(request);
  if (error_code == ErrorCode::kSuccess) {
    ServiceManager& service_manager = ServiceManager::GetInstance();
    if (service_manager.GetService(request.GetServiceName()) == nullptr) {
      // 先确认服务已注册，再调用处理器，避免无效请求进入业务层。
      error_code = ErrorCode::kServiceNotFound;
    } else {
      std::string result;
      // 通过服务管理器执行实际业务方法，避免网络层绕过已注册服务。
      if (service_manager.HandleRpcRequest(request.GetServiceName(),
                                           request.GetMethodName(),
                                           request.GetPayload(), result)) {
        response.SetResultData(result);
      } else {
        error_code = ErrorCode::kBusinessError;
        response.SetErrorMessage(
            result.empty() ? Error::GetErrorMessage(error_code) : result);
      }
    }
  }

  response.SetErrorCode(static_cast<uint32_t>(error_code));
  // 统一补齐错误文本，保证客户端即使只读取错误码也能获得可读信息。
  if (error_code != ErrorCode::kSuccess && response.GetErrorMessage().empty()) {
    response.SetErrorMessage(Error::GetErrorMessage(error_code));
  }

  if (!connection->Write(response)) {
    LOG_ERROR("Failed to send RPC response with sequence id {}",
              request.GetSequenceId());
  }
}

}  // namespace

MessageCycle::MessageCycle(ConnectionManager* manager)
    : thread_id_(),
      running_(false),
#ifdef __APPLE__
      kqueue_fd_(kqueue()),
#else
      epoll_fd_(epoll_create1(EPOLL_CLOEXEC)),
#endif
      // 事件后端初始化模块：按平台创建 kqueue/epoll，并绑定连接管理器。
      connection_manager_(manager == nullptr ? &ConnectionManager::GetInstance()
                                             : manager) {
#ifdef __APPLE__
  if (kqueue_fd_ < 0) {
    LOG_ERROR("Failed to create kqueue: {}", std::strerror(errno));
  }
#else
  if (epoll_fd_ < 0) {
    LOG_ERROR("Failed to create epoll: {}", std::strerror(errno));
  }
#endif
}

MessageCycle::~MessageCycle() {
  Stop();
  connection_manager_->CloseAll();
#ifdef __APPLE__
  if (kqueue_fd_ >= 0) {
    close(kqueue_fd_);
    kqueue_fd_ = -1;
  }
#else
  if (epoll_fd_ >= 0) {
    close(epoll_fd_);
    epoll_fd_ = -1;
  }
#endif
}

bool MessageCycle::AddListenFd(int fd) {
  // 监听注册模块：将监听 fd 加入平台事件后端和本地集合。
  if (!IsValidFileDescriptor(fd) || !SetNonBlockingAndCloseOnExec(fd)) {
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  // 集合和事件后端在同一临界区更新，避免并发增删产生重复注册。
  if (listen_fds_.find(fd) != listen_fds_.end()) {
    return true;
  }

#ifdef __APPLE__
  if (kqueue_fd_ < 0) {
    return false;
  }
  struct kevent event;
  EV_SET(&event, fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
  if (kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) < 0) {
    LOG_ERROR("Failed to add listen fd {} to kqueue: {}", fd,
              std::strerror(errno));
    return false;
  }
#else
  if (epoll_fd_ < 0) {
    return false;
  }
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.fd = fd;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
    LOG_ERROR("Failed to add listen fd {} to epoll: {}", fd,
              std::strerror(errno));
    return false;
  }
#endif

  listen_fds_.insert(fd);
  return true;
}

bool MessageCycle::RemoveListenFd(int fd) {
  // 监听移除模块：取消事件订阅并从本地集合删除监听 fd。
  std::lock_guard<std::mutex> lock(mutex_);
  if (listen_fds_.erase(fd) == 0) {
    return false;
  }

#ifdef __APPLE__
  struct kevent event;
  EV_SET(&event, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
  if (kqueue_fd_ >= 0 &&
      kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) < 0 &&
      errno != ENOENT && errno != EBADF) {
    LOG_WARN("Failed to remove listen fd {} from kqueue: {}", fd,
             std::strerror(errno));
  }
#else
  if (epoll_fd_ >= 0 && epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0 &&
      errno != ENOENT && errno != EBADF) {
    LOG_WARN("Failed to remove listen fd {} from epoll: {}", fd,
             std::strerror(errno));
  }
#endif
  return true;
}

size_t MessageCycle::GetListenFdCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return listen_fds_.size();
}

void MessageCycle::Loop() {
  // 主事件循环模块：等待 I/O 事件并分发到连接建立或数据处理路径。
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    // 已有循环运行时拒绝重复启动，防止多个线程同时消费同一后端。
    return;
  }
  thread_id_ = pthread_self();

#ifdef __APPLE__
  if (kqueue_fd_ < 0) {
    running_.store(false);
    return;
  }
  struct kevent events[MAX_EVENTS];
  while (running_.load()) {
    timespec timeout{};
    timeout.tv_nsec = 100 * 1000 * 1000;
    // 定期超时返回使 Stop 无需额外唤醒 fd 也能及时生效。
    const int event_count =
        kevent(kqueue_fd_, nullptr, 0, events, MAX_EVENTS, &timeout);
    if (event_count < 0) {
      if (errno == EINTR) {
        continue;
      }
      LOG_ERROR("kqueue wait failed: {}", std::strerror(errno));
      break;
    }
    HandleKqueueEvents(events, event_count);
  }
#else
  if (epoll_fd_ < 0) {
    running_.store(false);
    return;
  }
  epoll_event events[MAX_EVENTS];
  while (running_.load()) {
    // 100ms 超时兼顾事件响应速度和停止标志检查。
    const int event_count = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);
    if (event_count < 0) {
      if (errno == EINTR) {
        continue;
      }
      LOG_ERROR("epoll wait failed: {}", std::strerror(errno));
      break;
    }
    HandleEpollEvents(events, event_count);
  }
#endif
  running_.store(false);
}

void MessageCycle::Stop() { running_.store(false); }

#ifdef __APPLE__
void MessageCycle::HandleKqueueEvents(struct kevent* events, int nfds) {
  for (int index = 0; index < nfds; ++index) {
    const int fd = static_cast<int>(events[index].ident);
    bool is_listen_fd = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      is_listen_fd = listen_fds_.find(fd) != listen_fds_.end();
    }
    if ((events[index].flags & EV_ERROR) != 0) {
      RemoveInvalidFileDescriptor(fd);
    } else if (is_listen_fd) {
      HandleNewConnection(fd);
    } else if (events[index].filter == EVFILT_READ &&
               (events[index].data > 0 ||
                (events[index].flags & EV_EOF) == 0)) {
      HandleClientData(fd);
    }
    if (!is_listen_fd && (events[index].flags & EV_EOF) != 0) {
      RemoveConnection(fd);
    }
  }
}
#else
void MessageCycle::HandleEpollEvents(struct epoll_event* events, int nfds) {
  for (int index = 0; index < nfds; ++index) {
    const int fd = events[index].data.fd;
    bool is_listen_fd = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      is_listen_fd = listen_fds_.find(fd) != listen_fds_.end();
    }

    if (is_listen_fd) {
      if ((events[index].events & EPOLLIN) != 0) {
        HandleNewConnection(fd);
      }
      if ((events[index].events & (EPOLLERR | EPOLLHUP)) != 0) {
        RemoveInvalidFileDescriptor(fd);
      }
      continue;
    }

    if ((events[index].events & EPOLLIN) != 0) {
      HandleClientData(fd);
    }
    if ((events[index].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
      RemoveConnection(fd);
    }
  }
}
#endif

void MessageCycle::HandleNewConnection(int fd) {
  // 新连接模块：排空监听队列，配置客户端 fd，注册回调后加入事件循环。
  while (running_.load()) {
    int client_fd = -1;
    do {
      client_fd = accept(fd, nullptr, nullptr);
    } while (client_fd < 0 && errno == EINTR);

    if (client_fd < 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        LOG_ERROR("Failed to accept connection on fd {}: {}", fd,
                  std::strerror(errno));
      }
      return;
    }

    if (!SetNonBlockingAndCloseOnExec(client_fd)) {
      LOG_ERROR("Failed to configure client fd {}: {}", client_fd,
                std::strerror(errno));
      close(client_fd);
      continue;
    }

    const int enabled = 1;
    if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                   sizeof(enabled)) < 0 ||
        setsockopt(client_fd, SOL_SOCKET, SO_KEEPALIVE, &enabled,
                   sizeof(enabled)) < 0) {
      LOG_ERROR("Failed to set client socket options for fd {}: {}", client_fd,
                std::strerror(errno));
      close(client_fd);
      continue;
    }

#ifdef SO_NOSIGPIPE
    if (setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
                   sizeof(enabled)) < 0) {
      LOG_ERROR("Failed to set SO_NOSIGPIPE for client fd {}: {}", client_fd,
                std::strerror(errno));
      close(client_fd);
      continue;
    }
#endif

    auto connection = std::make_shared<Connection>(client_fd);
    // 回调捕获 this，析构前必须先 Stop 并清理连接，保证生命周期安全。
    connection->SetMessageCallback(
        [this](const std::shared_ptr<Connection>& conn,
               const RpcRequest& request) { HandleRpcRequest(conn, request); });
    connection->SetCloseCallback(
        [this](const std::shared_ptr<Connection>& conn) {
          RemoveConnection(conn->GetFd());
        });

#ifdef __APPLE__
    struct kevent event;
    EV_SET(&event, client_fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
    if (kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) < 0) {
      LOG_ERROR("Failed to add client fd {} to kqueue: {}", client_fd,
                std::strerror(errno));
      connection->Close();
      continue;
    }
#else
    epoll_event event{};
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.fd = client_fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &event) < 0) {
      LOG_ERROR("Failed to add client fd {} to epoll: {}", client_fd,
                std::strerror(errno));
      connection->Close();
      continue;
    }
#endif
    connection_manager_->AddConnection(std::move(connection));
  }
}

void MessageCycle::HandleClientData(int fd) {
  // 客户端读事件模块：取出连接并读取数据，失败时统一移除连接。
  const std::shared_ptr<Connection> connection =
      connection_manager_->GetConnection(fd);
  if (connection == nullptr || !connection->Read()) {
    RemoveConnection(fd);
  }
}

void MessageCycle::RemoveConnection(int fd) {
  // 客户端移除模块：先从事件后端注销，再交给连接管理器关闭。
#ifdef __APPLE__
  if (kqueue_fd_ >= 0 && fd >= 0) {
    struct kevent event;
    EV_SET(&event, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    if (kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) < 0 &&
        errno != ENOENT && errno != EBADF) {
      LOG_WARN("Failed to remove client fd {} from kqueue: {}", fd,
               std::strerror(errno));
    }
  }
#else
  if (epoll_fd_ >= 0 && fd >= 0 &&
      epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0 && errno != ENOENT &&
      errno != EBADF) {
    LOG_WARN("Failed to remove client fd {} from epoll: {}", fd,
             std::strerror(errno));
  }
#endif
  connection_manager_->RemoveConnection(fd);
}

void MessageCycle::RemoveInvalidFileDescriptor(int fd) {
  // 无效 fd 清理模块：根据 fd 类型分别移除监听器或客户端连接。
  bool is_listen_fd = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    is_listen_fd = listen_fds_.find(fd) != listen_fds_.end();
  }
  if (is_listen_fd) {
    RemoveListenFd(fd);
  } else {
    RemoveConnection(fd);
  }
}

bool MessageCycle::IsValidFileDescriptor(int fd) {
  return fd >= 0 && (fcntl(fd, F_GETFD) != -1 || errno != EBADF);
}

bool MessageCycle::ValidateRequest(const RpcRequest& request) {
  return ValidateRpcRequest(request) == ErrorCode::kSuccess;
}

void MessageCycle::HandleRpcRequest(const std::shared_ptr<Connection>& conn,
                                    const RpcRequest& request) {
  HandleRpcRequestAsync(conn, request);
}

void MessageCycle::HandleRpcRequestAsync(
    const std::shared_ptr<Connection>& conn, const RpcRequest& request) {
  // 异步请求模块：将业务处理投递线程池，避免事件线程被慢请求阻塞。
  std::future<void> task = meeting_ctrl::ThreadPoolSingleton::Enqueue(
      meeting_ctrl::TaskPriority::kNormal,
      [conn, request] { ProcessRpcRequest(conn, request); });
  if (!task.valid()) {
    // 线程池拒绝任务时立即返回服务不可用，避免客户端无限等待。
    SendErrorResponse(conn, request.GetSequenceId(),
                      ErrorCode::kServiceUnavailable,
                      Error::GetErrorMessage(ErrorCode::kServiceUnavailable));
  }
}

void MessageCycle::HandleRpcRequestSync(const std::shared_ptr<Connection>& conn,
                                        const RpcRequest& request) {
  ProcessRpcRequest(conn, request);
}

void MessageCycle::SendSuccessResponse(const std::shared_ptr<Connection>& conn,
                                       uint64_t sequence_id,
                                       const std::string& result) {
  RpcResponse response;
  response.SetSequenceId(static_cast<uint32_t>(sequence_id));
  response.SetErrorCode(static_cast<uint32_t>(ErrorCode::kSuccess));
  response.SetResultData(result);
  SendResponse(conn, response, "success");
}

void MessageCycle::SendErrorResponse(const std::shared_ptr<Connection>& conn,
                                     uint64_t sequence_id, ErrorCode error_code,
                                     const std::string& error_message) {
  RpcResponse response;
  response.SetSequenceId(static_cast<uint32_t>(sequence_id));
  response.SetErrorCode(static_cast<uint32_t>(error_code));
  response.SetErrorMessage(error_message);
  SendResponse(conn, response, "error");
}

void MessageCycle::SendResponse(const std::shared_ptr<Connection>& conn,
                                const RpcResponse& response,
                                const std::string& response_type) {
  // 响应发送模块：统一执行序列化发送并记录成功或失败日志。
  if (conn == nullptr || !conn->Write(response)) {
    LOG_ERROR("Failed to send {} RPC response", response_type);
    return;
  }
  LOG_DEBUG("Sent {} RPC response with sequence id {}", response_type,
            response.GetSequenceId());
}
}  // namespace cookrpc
