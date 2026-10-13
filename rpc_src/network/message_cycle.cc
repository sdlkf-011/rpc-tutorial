#include "message_cycle.h"

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <deque>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "log_manager.h"
#include "service_manager.h"
#include "thread_pool_singleton.h"

namespace cookrpc {
namespace {

constexpr uint64_t kListenTag = uint64_t{1} << 63;

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

struct MessageCycle::Notifications {
  std::mutex mutex;
  std::deque<std::shared_ptr<Connection>> ready;
  std::unordered_set<uint64_t> queued;
  bool active = true;
  int pipe_fds[2]{-1, -1};

  Notifications() {
    if (pipe(pipe_fds) != 0)
      throw std::runtime_error("Failed to create event wakeup pipe");
    if (!SetNonBlockingAndCloseOnExec(pipe_fds[0]) ||
        !SetNonBlockingAndCloseOnExec(pipe_fds[1])) {
      close(pipe_fds[0]);
      close(pipe_fds[1]);
      throw std::runtime_error("Failed to configure event wakeup pipe");
    }
  }
  ~Notifications() {
    close(pipe_fds[0]);
    close(pipe_fds[1]);
  }
  void Wake() {
    const char byte = 1;
    ssize_t rc;
    do {
      rc = write(pipe_fds[1], &byte, 1);
    } while (rc < 0 && errno == EINTR);
    // EAGAIN 表示已有唤醒数据待消费，无需等待。
  }
  bool Notify(const std::shared_ptr<Connection>& connection) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (!active) return false;
      if (queued.insert(connection->GetId()).second)
        ready.push_back(connection);
    }
    Wake();
    return true;
  }
  void DrainPipe() {
    char bytes[1024];
    while (read(pipe_fds[0], bytes, sizeof(bytes)) > 0) {
    }
  }
};

MessageCycle::MessageCycle(ConnectionManager* manager, size_t max_connections)
    : notifications_(std::make_shared<Notifications>()),
      max_connections_(max_connections),
      running_(false),
#ifdef __APPLE__
      kqueue_fd_(kqueue()),
#else
      epoll_fd_(epoll_create1(EPOLL_CLOEXEC)),
#endif
      connection_manager_(manager ? manager
                                  : &ConnectionManager::GetInstance()) {
#ifdef __APPLE__
  struct kevent event;
  EV_SET(&event, notifications_->pipe_fds[0], EVFILT_READ, EV_ADD | EV_ENABLE,
         0, 0, nullptr);
  if (kqueue_fd_ < 0 ||
      kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) < 0) {
    if (kqueue_fd_ >= 0) close(kqueue_fd_);
    throw std::runtime_error("Failed to initialize kqueue");
  }
#else
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.u64 = 0;
  if (epoll_fd_ < 0 || epoll_ctl(epoll_fd_, EPOLL_CTL_ADD,
                                 notifications_->pipe_fds[0], &event) < 0) {
    if (epoll_fd_ >= 0) close(epoll_fd_);
    throw std::runtime_error("Failed to initialize epoll");
  }
#endif
}

MessageCycle::~MessageCycle() {
  Stop();
  {
    std::unique_lock<std::mutex> lock(loop_mutex_);
    loop_cv_.wait(lock, [this] { return !running_.load(); });
  }
  {
    std::lock_guard<std::mutex> lock(notifications_->mutex);
    notifications_->active = false;
  }
#ifdef __APPLE__
  close(kqueue_fd_);
#else
  close(epoll_fd_);
#endif
}

// 调用者持有 mutex_，监听注册、暂停和恢复使用相同的事件标识。
bool MessageCycle::ConfigureListener(int fd, bool enabled) {
#ifdef __APPLE__
  struct kevent event;
  EV_SET(&event, fd, EVFILT_READ, enabled ? EV_ADD | EV_ENABLE : EV_DISABLE, 0,
         0, reinterpret_cast<void*>(kListenTag | static_cast<uint64_t>(fd)));
  return kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) == 0;
#else
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.u64 = kListenTag | static_cast<uint64_t>(fd);
  return epoll_ctl(epoll_fd_, enabled ? EPOLL_CTL_ADD : EPOLL_CTL_DEL, fd,
                   &event) == 0;
#endif
}

bool MessageCycle::AddListenFd(int fd) {
  if (!IsValidFileDescriptor(fd) || !SetNonBlockingAndCloseOnExec(fd))
    return false;
  std::lock_guard<std::mutex> lock(mutex_);
  const auto inserted = listen_fds_.try_emplace(fd);
  if (!inserted.second) return true;
  if (!ConfigureListener(fd, true)) {
    listen_fds_.erase(inserted.first);
    LOG_ERROR("Failed to register listener {}: {}", fd, std::strerror(errno));
    return false;
  }
  notifications_->Wake();
  return true;
}

void MessageCycle::PauseListener(int fd, int error) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = listen_fds_.find(fd);
  if (found == listen_fds_.end() || found->second.paused) return;
  if (!ConfigureListener(fd, false)) {
    LOG_ERROR("Failed to pause listener {}: {}", fd, std::strerror(errno));
    Stop();  // 后端无法取消订阅时退出，避免在持续可读事件上空转。
    return;
  }
  auto& listener = found->second;
  listener.paused = true;
  listener.resume_at = std::chrono::steady_clock::now() + listener.retry_delay;
  LOG_WARN("Pausing listener {} after {} for {}ms", fd, std::strerror(error),
           listener.retry_delay.count());
  listener.retry_delay =
      std::min(listener.retry_delay * 2, std::chrono::milliseconds(1000));
}

int MessageCycle::RetryPausedListeners() {
  std::lock_guard<std::mutex> lock(mutex_);
  int timeout_ms = 100;
  const auto now = std::chrono::steady_clock::now();
  for (auto it = listen_fds_.begin(); it != listen_fds_.end();) {
    auto& listener = it->second;
    if (listener.paused && !stop_requested_.load()) {
      if (now >= listener.resume_at) {
        if (ConfigureListener(it->first, true)) {
          listener.paused = false;
        } else if (errno == EBADF || errno == ENOENT) {
          it = listen_fds_.erase(it);
          continue;
        } else {
          listener.resume_at = now + listener.retry_delay;
          listener.retry_delay = std::min(listener.retry_delay * 2,
                                          std::chrono::milliseconds(1000));
        }
      }
      if (listener.paused) {
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                listener.resume_at - now)
                .count();
        timeout_ms = std::min(
            timeout_ms, static_cast<int>(std::max<int64_t>(1, remaining)));
      }
    }
    ++it;
  }
  return timeout_ms;
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
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) return;
  bool draining = false;
  auto drain_deadline = std::chrono::steady_clock::time_point::max();
  while (true) {
    ProcessNotifications();
    const int event_timeout_ms = RetryPausedListeners();
    if (stop_requested_.load()) {
      if (!draining) {
        draining = true;
        drain_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        std::vector<int> listeners;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          for (const auto& listener : listen_fds_)
            listeners.push_back(listener.first);
        }
        for (int fd : listeners) RemoveListenFd(fd);
        std::vector<uint64_t> ids;
        for (const auto& entry : clients_) ids.push_back(entry.first);
        for (auto id : ids) UpdateInterest(id);
      }
      std::vector<uint64_t> idle;
      for (const auto& entry : clients_)
        if (entry.second.connection->IsIdle()) idle.push_back(entry.first);
      for (auto id : idle) RemoveConnection(id);
      if (clients_.empty() ||
          std::chrono::steady_clock::now() >= drain_deadline)
        break;
    }
#ifdef __APPLE__
    struct kevent events[MAX_EVENTS];
    timespec timeout{0, event_timeout_ms * 1000 * 1000};
    const int count =
        kevent(kqueue_fd_, nullptr, 0, events, MAX_EVENTS, &timeout);
#else
    epoll_event events[MAX_EVENTS];
    const int count =
        epoll_wait(epoll_fd_, events, MAX_EVENTS, event_timeout_ms);
#endif
    if (count < 0) {
      if (errno == EINTR) continue;
      LOG_ERROR("Event wait failed: {}", std::strerror(errno));
      break;
    }
#ifdef __APPLE__
    HandleKqueueEvents(events, count);
#else
    HandleEpollEvents(events, count);
#endif
  }
  while (!clients_.empty()) RemoveConnection(clients_.begin()->first);
  {
    std::lock_guard<std::mutex> lock(notifications_->mutex);
    notifications_->active = false;
    notifications_->ready.clear();
    notifications_->queued.clear();
  }
  {
    std::lock_guard<std::mutex> lock(loop_mutex_);
    running_.store(false);
  }
  loop_cv_.notify_all();
}

void MessageCycle::Stop() {
  stop_requested_.store(true);
  notifications_->Wake();
}

void MessageCycle::ProcessNotifications() {
  std::deque<std::shared_ptr<Connection>> ready;
  {
    std::lock_guard<std::mutex> lock(notifications_->mutex);
    ready.swap(notifications_->ready);
    notifications_->queued.clear();
  }
  for (const auto& connection : ready) {
    // 对象标识和 epoll/kqueue 中的标识一致；旧 fd 或旧回调不作用于新连接。
    const auto found = clients_.find(connection->GetId());
    if (found != clients_.end() && found->second.connection == connection)
      FlushConnection(found->first);
  }
}

#ifdef __APPLE__
void MessageCycle::HandleKqueueEvents(struct kevent* events, int count) {
  for (int i = 0; i < count; ++i) {
    const uint64_t id = reinterpret_cast<uintptr_t>(events[i].udata);
    if (id == 0) {
      notifications_->DrainPipe();
      continue;
    }
    if (id & kListenTag) {
      const int fd = static_cast<int>(id & ~kListenTag);
      if (!stop_requested_.load() && !(events[i].flags & EV_ERROR))
        HandleNewConnection(fd);
      continue;
    }
    if (clients_.find(id) == clients_.end()) continue;
    if (events[i].filter == EVFILT_READ && !stop_requested_.load())
      HandleClientData(id);
    if (events[i].filter == EVFILT_WRITE) FlushConnection(id);
    if (events[i].flags & EV_ERROR) RemoveConnection(id);
  }
}
#else
void MessageCycle::HandleEpollEvents(struct epoll_event* events, int count) {
  for (int i = 0; i < count; ++i) {
    const uint64_t id = events[i].data.u64;
    const uint32_t flags = events[i].events;
    if (id == 0) {
      notifications_->DrainPipe();
      continue;
    }
    if (id & kListenTag) {
      const int fd = static_cast<int>(id & ~kListenTag);
      if (!stop_requested_.load() && (flags & EPOLLIN)) HandleNewConnection(fd);
      if (flags & (EPOLLERR | EPOLLHUP)) RemoveListenFd(fd);
      continue;
    }
    if (clients_.find(id) == clients_.end()) continue;
    // 可读与挂断一起发生时先读取，EOF 由 Connection 显式报告。
    if (!stop_requested_.load() &&
        (flags & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)))
      HandleClientData(id);
    if (flags & EPOLLOUT) FlushConnection(id);
    if (flags & EPOLLERR) RemoveConnection(id);
  }
}
#endif

void MessageCycle::HandleNewConnection(int fd) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = listen_fds_.find(fd);
    if (found == listen_fds_.end() || found->second.paused) return;
  }
  // LT 后端会再次报告可读；每轮限制 accept 次数，给已有连接处理机会。
  for (size_t accepted = 0;
       accepted < kAcceptBudget && running_.load() && !stop_requested_.load();
       ++accepted) {
    const int client_fd = accept(fd, nullptr, nullptr);

    if (client_fd < 0) {
      const int error = errno;
      if (error == EINTR || error == ECONNABORTED || error == EPROTO) continue;
      if (error == EAGAIN || error == EWOULDBLOCK) return;
      if (error == EBADF || error == EINVAL || error == ENOTSOCK) {
        LOG_ERROR("Removing invalid listener {}: {}", fd, std::strerror(error));
        RemoveListenFd(fd);
        return;
      }
      PauseListener(fd, error);
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto found = listen_fds_.find(fd);
      if (found != listen_fds_.end())
        found->second.retry_delay = std::chrono::milliseconds(100);
    }
    if (clients_.size() >= max_connections_) {
      close(client_fd);
      continue;
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
    const auto notifications = notifications_;
    connection->SetEventCallback(
        [notifications](const std::shared_ptr<Connection>& conn) {
          return notifications->Notify(conn);
        });
    const uint64_t id = connection->GetId();
    clients_.emplace(id, Client{connection, false, false, false});
    if (!UpdateInterest(id)) {
      RemoveConnection(id);
      continue;
    }
    connection_manager_->AddConnection(std::move(connection));
  }
}

void MessageCycle::HandleClientData(uint64_t id) {
  const auto found = clients_.find(id);
  if (found == clients_.end()) return;
  const auto connection = found->second.connection;
  const auto result = connection->Read();
  if (result.error || connection->ShouldClose()) {
    RemoveConnection(id);
    return;
  }
  if (result.eof && !UpdateInterest(id)) RemoveConnection(id);
}

void MessageCycle::FlushConnection(uint64_t id) {
  const auto found = clients_.find(id);
  if (found == clients_.end()) return;
  const auto connection = found->second.connection;
  if (connection->ShouldClose() ||
      connection->FlushWrites(kSendBudget) == Connection::FlushResult::kError ||
      connection->ShouldClose()) {
    RemoveConnection(id);
    return;
  }
  if (!UpdateInterest(id)) RemoveConnection(id);
}

bool MessageCycle::UpdateInterest(uint64_t id) {
  auto found = clients_.find(id);
  if (found == clients_.end()) return false;
  auto& client = found->second;
  const int fd = client.connection->GetFd();
  const bool read = !stop_requested_.load() && !client.connection->HasReadEof();
  const bool write = client.connection->PendingWriteBytes() != 0;
#ifdef __APPLE__
  struct kevent changes[2];
  int count = 0;
  if (read != client.read_enabled) {
    EV_SET(&changes[count], fd, EVFILT_READ,
           read ? EV_ADD | EV_ENABLE : EV_DELETE, 0, 0,
           reinterpret_cast<void*>(id));
    ++count;
  }
  if (write != client.write_enabled) {
    EV_SET(&changes[count], fd, EVFILT_WRITE,
           write ? EV_ADD | EV_ENABLE : EV_DELETE, 0, 0,
           reinterpret_cast<void*>(id));
    ++count;
  }
  if (count && kevent(kqueue_fd_, changes, count, nullptr, 0, nullptr) < 0)
    return false;
#else
  if (!read && !write) {
    if (client.subscribed &&
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) < 0)
      return false;
    client.subscribed = false;
  } else {
    epoll_event event{};
    event.events = (read ? uint32_t{EPOLLIN | EPOLLRDHUP} : 0u) |
                   (write ? uint32_t{EPOLLOUT} : 0u);
    event.data.u64 = id;
    if (epoll_ctl(epoll_fd_, client.subscribed ? EPOLL_CTL_MOD : EPOLL_CTL_ADD,
                  fd, &event) < 0)
      return false;
    client.subscribed = true;
  }
#endif
  client.read_enabled = read;
  client.write_enabled = write;
  return true;
}

void MessageCycle::RemoveConnection(uint64_t id) {
  const auto found = clients_.find(id);
  if (found == clients_.end()) return;
  const auto connection = found->second.connection;
  const int fd = connection->GetFd();
#ifdef __APPLE__
  struct kevent changes[2];
  int count = 0;
  if (found->second.read_enabled) {
    EV_SET(&changes[count], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    ++count;
  }
  if (found->second.write_enabled) {
    EV_SET(&changes[count], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ++count;
  }
  if (count) kevent(kqueue_fd_, changes, count, nullptr, 0, nullptr);
#else
  if (found->second.subscribed)
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
#endif
  clients_.erase(found);
  connection->SetMessageCallback(nullptr);
  connection->CloseFromEventLoop();
  connection_manager_->RemoveConnection(fd, connection);
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
    const auto connection = connection_manager_->GetConnection(fd);
    if (connection) RemoveConnection(connection->GetId());
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
  conn->BeginRequest();
  std::future<void> task = meeting_ctrl::ThreadPoolSingleton::Enqueue(
      meeting_ctrl::TaskPriority::kNormal, [conn, request] {
        struct Done {
          std::shared_ptr<Connection> conn;
          ~Done() { conn->FinishRequest(); }
        } done{conn};
        ProcessRpcRequest(conn, request);
      });
  if (!task.valid()) {
    conn->FinishRequest();
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
  LOG_DEBUG("Queued {} RPC response with sequence id {}", response_type,
            response.GetSequenceId());
}
}  // namespace cookrpc
