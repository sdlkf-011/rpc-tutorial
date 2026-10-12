#include "connection.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <limits>
#include <string>
#include <utility>

#include "error_code.h"
#include "log_manager.h"

namespace cookrpc {
namespace {

// 帧解析辅助模块：读取 RPC 头部中的网络字节序整数。
constexpr size_t kHeaderSize = RpcHeader::kHeaderSize;
std::atomic<uint64_t> next_connection_id{1};
#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif
int RemainingMs(std::chrono::steady_clock::time_point deadline) {
  const auto now = std::chrono::steady_clock::now();
  if (now >= deadline) return 0;
  const auto remaining =
      std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
  return static_cast<int>(
      std::min<int64_t>(std::max<int64_t>(1, remaining.count()),
                        std::numeric_limits<int>::max()));
}

uint32_t ReadNetworkUint32(const char* data) {
  const auto* bytes = reinterpret_cast<const unsigned char*>(data);
  return (static_cast<uint32_t>(bytes[0]) << 24U) |
         (static_cast<uint32_t>(bytes[1]) << 16U) |
         (static_cast<uint32_t>(bytes[2]) << 8U) |
         static_cast<uint32_t>(bytes[3]);
}
}  // namespace

Connection::Connection(int fd) : fd_(fd), id_(next_connection_id.fetch_add(1)) {
  // 连接初始化模块：配置非阻塞属性，读取对端地址并进入已连接状态。
  if (fd_ < 0) {
    return;
  }

  const int status_flags = fcntl(fd_, F_GETFL, 0);
  // 非阻塞模式让事件循环不会因单个连接暂时无数据而停顿。
  if (status_flags == -1 ||
      fcntl(fd_, F_SETFL, status_flags | O_NONBLOCK) == -1) {
    LOG_ERROR("Failed to configure connection fd {}: {}", fd_,
              std::strerror(errno));
    close(fd_);
    fd_ = -1;
    return;
  }

#ifdef SO_NOSIGPIPE
  const int enabled = 1;
  if (setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) <
      0) {
    LOG_ERROR("Failed to set SO_NOSIGPIPE for connection fd {}: {}", fd_,
              std::strerror(errno));
    close(fd_);
    fd_ = -1;
    return;
  }
#endif

  sockaddr_storage peer_address{};
  // getpeername 仅用于日志和诊断；获取失败不影响连接继续工作。
  socklen_t address_length = sizeof(peer_address);
  if (getpeername(fd_, reinterpret_cast<sockaddr*>(&peer_address),
                  &address_length) == 0) {
    char address_buffer[INET6_ADDRSTRLEN] = {};
    const void* address = nullptr;
    if (peer_address.ss_family == AF_INET) {
      const auto* ipv4 = reinterpret_cast<const sockaddr_in*>(&peer_address);
      address = &ipv4->sin_addr;
      peer_port_ = ntohs(ipv4->sin_port);
    } else if (peer_address.ss_family == AF_INET6) {
      const auto* ipv6 = reinterpret_cast<const sockaddr_in6*>(&peer_address);
      address = &ipv6->sin6_addr;
      peer_port_ = ntohs(ipv6->sin6_port);
    }
    if (address != nullptr &&
        inet_ntop(peer_address.ss_family, address, address_buffer,
                  sizeof(address_buffer)) != nullptr) {
      peer_addr_ = address_buffer;
    }
  }

  state_ = State::CONNECTED;
}

Connection::~Connection() {
  // 资源释放模块：关闭套接字并将状态标记为断开。
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  if (fd_ >= 0) {
    shutdown(fd_, SHUT_RDWR);
    close(fd_);
    fd_ = -1;
  }
  state_ = State::DISCONNECTED;
}

Connection::ReadResult Connection::Read() {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  ReadResult result;
  if (!IsValid()) {
    result.error = true;
    return result;
  }
  if (read_eof_) {
    result.eof = true;
    return result;
  }
  char buffer[MAX_BUFFER_SIZE];
  // 每轮限制接收量；LT 后端将在下一轮继续报告可读。
  constexpr size_t kReadBudget = 256 * 1024;
  while (result.bytes_received < kReadBudget) {
    size_t capacity =
        std::min(sizeof(buffer), kReadBudget - result.bytes_received);
    if (!message_callback_) {
      capacity =
          std::min(capacity, RpcHeader::kMaxFrameSize - read_buffer_.size());
      if (capacity == 0) return result;
    }
    const ssize_t received = recv(fd_, buffer, capacity, 0);
    if (received > 0) {
      result.bytes_received += static_cast<size_t>(received);
      read_buffer_.insert(read_buffer_.end(), buffer, buffer + received);
      if (message_callback_ && !ProcessMessage()) {
        result.error = true;
        return result;
      }
      continue;
    }
    if (received == 0) {
      read_eof_ = result.eof = true;
      return result;
    }
    if (errno == EINTR) continue;
    if (errno == EAGAIN || errno == EWOULDBLOCK) return result;
    result.error = true;
    return result;
  }
  return result;
}

Connection::ReadResult Connection::ReadWithTimeout(int timeout_ms) {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  ReadResult result;
  if (!IsValid() || timeout_ms < 0) {
    result.error = true;
    return result;
  }
  if (read_eof_) {
    result.eof = true;
    return result;
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  pollfd descriptor{fd_, POLLIN, 0};
  int rc;
  do {
    rc = poll(&descriptor, 1, RemainingMs(deadline));
  } while (rc < 0 && errno == EINTR && RemainingMs(deadline) > 0);
  if (rc == 0) {
    result.timeout = true;
    return result;
  }
  if (rc < 0 || (descriptor.revents & POLLNVAL)) {
    result.error = true;
    return result;
  }
  // HUP/ERR 可能与尚未读取的数据一起出现，先 recv 再解释 EOF 或错误。
  if (descriptor.revents & (POLLIN | POLLHUP | POLLERR)) return Read();
  result.timeout = true;
  return result;
}

void Connection::SetEventCallback(EventCallback callback) {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  event_managed_ = true;
  event_callback_ = std::move(callback);
}

bool Connection::NotifyEvent() {
  EventCallback callback;
  {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    callback = event_callback_;
  }
  const auto self = weak_from_this().lock();
  return callback && self && callback(self);
}

bool Connection::QueueFrame(std::string data) {
  bool accepted = false;
  {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    if (!IsValid()) return false;
    if (write_frames_.size() >= kMaxWriteFrames ||
        data.size() > kMaxWriteBytes - write_bytes_) {
      // 容量不足时关闭慢连接；不接受一半帧，也不无限占用工作线程。
      close_requested_ = true;
    } else {
      write_frames_.push_back(std::move(data));
      write_bytes_ += write_frames_.back().size();
      accepted = true;
    }
  }
  return NotifyEvent() && accepted;
}

bool Connection::Write(const std::string& data) {
  bool managed;
  {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    managed = event_managed_;
  }
  if (!managed) return WriteWithTimeout(data, 3000);
  if (data.size() >= kHeaderSize &&
      ReadNetworkUint32(data.data()) == RpcHeader::MAGIC &&
      ReadNetworkUint32(data.data() + sizeof(uint32_t)) ==
          data.size() - kHeaderSize &&
      data.size() > RpcHeader::kMaxFrameSize) {
    RpcResponse error;
    error.SetSequenceId(ReadNetworkUint32(data.data() + 2 * sizeof(uint32_t)));
    error.SetErrorCode(static_cast<uint32_t>(ErrorCode::kProtocolError));
    error.SetErrorMessage("RPC response exceeds the 1 MiB body limit");
    std::string frame;
    return error.SerializeForSend(frame) && QueueFrame(std::move(frame));
  }
  RpcResponse response;
  // 原始帧入口也必须遵守协议长度与字段校验，不允许绕过响应检查。
  return response.Deserialize(data) && QueueFrame(data);
}

bool Connection::Write(const RpcResponse& response) {
  std::string data;
  return response.SerializeForSend(data) && Write(data);
}

bool Connection::WriteWithTimeout(const std::string& data, int timeout_ms) {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  if (event_managed_ || !IsValid() || timeout_ms < 0 ||
      data.size() > RpcHeader::kMaxFrameSize)
    return false;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  size_t offset = 0;
  while (offset < data.size()) {
    if (RemainingMs(deadline) == 0) {
      Close();
      return false;
    }
    const ssize_t sent =
        send(fd_, data.data() + offset, data.size() - offset, kSendFlags);
    if (sent > 0) {
      offset += static_cast<size_t>(sent);
      continue;
    }
    if (sent < 0 && errno == EINTR) continue;
    if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      pollfd descriptor{fd_, POLLOUT, 0};
      int rc;
      do {
        rc = poll(&descriptor, 1, RemainingMs(deadline));
      } while (rc < 0 && errno == EINTR && RemainingMs(deadline) > 0);
      if (rc > 0 && (descriptor.revents & POLLOUT) &&
          !(descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)))
        continue;
    }
    Close();
    return false;
  }
  return true;
}

Connection::FlushResult Connection::FlushWrites(size_t budget) {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  if (!IsValid()) return FlushResult::kError;
  while (!write_frames_.empty() && budget > 0) {
    const auto& frame = write_frames_.front();
    const size_t length = std::min(budget, frame.size() - write_offset_);
    const ssize_t sent =
        send(fd_, frame.data() + write_offset_, length, kSendFlags);
    if (sent > 0) {
      write_offset_ += static_cast<size_t>(sent);
      budget -= static_cast<size_t>(sent);
      if (write_offset_ == frame.size()) {
        write_bytes_ -= frame.size();
        write_frames_.pop_front();
        write_offset_ = 0;
      }
      continue;
    }
    if (sent < 0 && errno == EINTR) continue;
    if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
      return FlushResult::kPending;
    return FlushResult::kError;
  }
  return write_frames_.empty() ? FlushResult::kDrained : FlushResult::kPending;
}

void Connection::BeginRequest() {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  ++in_flight_;
}
void Connection::FinishRequest() {
  {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    --in_flight_;
  }
  NotifyEvent();
}
bool Connection::HasReadEof() const {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  return read_eof_;
}
bool Connection::ShouldClose() const {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  return close_requested_ ||
         (read_eof_ && (!read_buffer_.empty() ||
                        (in_flight_ == 0 && write_frames_.empty())));
}
size_t Connection::PendingWriteBytes() const {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  return write_bytes_ - write_offset_;
}
bool Connection::IsIdle() const {
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  return in_flight_ == 0 && write_frames_.empty();
}

bool Connection::ProcessMessage() {
  // RPC 帧解析模块：校验魔数和长度，按完整帧反序列化并触发业务回调。
  while (read_buffer_.size() >= kHeaderSize) {
    // 只有收齐固定头部后才能读取 body_size，避免越界访问。
    const uint32_t magic_number = ReadNetworkUint32(read_buffer_.data());
    const uint32_t body_size =
        ReadNetworkUint32(read_buffer_.data() + sizeof(uint32_t));
    if (magic_number != RpcHeader::MAGIC || body_size > MAX_MESSAGE_SIZE) {
      LOG_ERROR("Invalid RPC frame from {}:{}", peer_addr_, peer_port_);
      HandleError();
      return false;
    }

    const size_t frame_size = kHeaderSize + static_cast<size_t>(body_size);
    // 未收齐完整帧时保留缓冲区，等待下一次读事件继续拼接。
    if (read_buffer_.size() < frame_size) {
      return true;
    }

    std::string frame(read_buffer_.data(), frame_size);
    RpcRequest request;
    if (!request.Deserialize(frame)) {
      HandleError();
      return false;
    }

    // 先移除完整帧再调用业务回调，回调可以安全地关闭当前连接。
    read_buffer_.erase(
        read_buffer_.begin(),
        read_buffer_.begin() + static_cast<ptrdiff_t>(frame_size));
    // 先从缓冲移除当前帧，再回调业务，允许回调安全关闭连接。
    HandleMessage(request);
    if (!IsValid()) {
      return false;
    }
  }
  return true;
}

void Connection::HandleMessage(const RpcRequest& request) {
  // 消息分发模块：将已验证的请求交给上层注册的回调处理。
  if (message_callback_ != nullptr) {
    message_callback_(shared_from_this(), request);
  }
}

void Connection::HandleError() { Close(); }

void Connection::Close() {
  bool managed;
  {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    if (state_ == State::DISCONNECTED) return;
    managed = event_managed_;
    if (managed) close_requested_ = true;
  }
  if (managed) {
    NotifyEvent();
    return;
  }
  CloseFromEventLoop();
}

void Connection::CloseFromEventLoop() {
  CloseCallback callback;
  {
    std::lock_guard<std::recursive_mutex> lock(io_mutex_);
    if (state_ == State::DISCONNECTED) return;
    state_ = State::DISCONNECTING;
    const int fd = std::exchange(fd_, -1);
    if (fd >= 0) {
      shutdown(fd, SHUT_RDWR);
      close(fd);
    }
    write_frames_.clear();
    write_bytes_ = write_offset_ = 0;
    state_ = State::DISCONNECTED;
    event_callback_ = nullptr;
    callback = std::move(close_callback_);
  }
  if (callback) {
    const auto self = weak_from_this().lock();
    if (self) callback(self);
  }
}
}  // namespace cookrpc
