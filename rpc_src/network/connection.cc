#include "connection.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <string>

#include "log_manager.h"

namespace cookrpc {
namespace {

// 帧解析辅助模块：读取 RPC 头部中的网络字节序整数。
constexpr size_t kHeaderSize = 3 * sizeof(uint32_t);

uint32_t ReadNetworkUint32(const char* data) {
  const auto* bytes = reinterpret_cast<const unsigned char*>(data);
  return (static_cast<uint32_t>(bytes[0]) << 24U) |
         (static_cast<uint32_t>(bytes[1]) << 16U) |
         (static_cast<uint32_t>(bytes[2]) << 8U) |
         static_cast<uint32_t>(bytes[3]);
}
}  // namespace

Connection::Connection(int fd) : fd_(fd) {
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

bool Connection::Read() {
  // 非阻塞接收模块：持续读取可用数据，累积到缓冲区后解析完整帧。
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  if (!IsValid()) {
    return false;
  }

  char buffer[MAX_BUFFER_SIZE];
  while (true) {
    // 一次事件可能对应多次 recv，循环直到内核返回 EAGAIN。
    const ssize_t bytes_read = recv(fd_, buffer, sizeof(buffer), 0);
    if (bytes_read > 0) {
      // TCP 是字节流，单次 recv 可能只得到半帧或多帧，必须先累计。
      read_buffer_.insert(read_buffer_.end(), buffer, buffer + bytes_read);
      if (message_callback_ != nullptr) {
        if (!ProcessMessage()) {
          return false;
        }
      } else if (read_buffer_.size() > MAX_MESSAGE_SIZE + kHeaderSize) {
        // 没有消息回调时仍限制缓冲增长，防止恶意客户端耗尽内存。
        LOG_ERROR("Read buffer from {}:{} exceeds maximum message size",
                  peer_addr_, peer_port_);
        HandleError();
        return false;
      }
      continue;
    }

    if (bytes_read == 0) {
      // 对端有序关闭，立即进入统一关闭流程并通知上层。
      Close();
      return false;
    }
    if (errno == EINTR) {
      // 系统调用被信号打断时重试，不把瞬时中断误判为网络故障。
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return true;
    }

    LOG_ERROR("Failed to receive data from {}:{}: {}", peer_addr_, peer_port_,
              std::strerror(errno));
    HandleError();
    return false;
  }
}

bool Connection::ReadWithTimeout(int timeout_ms) {
  // 超时读取模块：先用 poll 等待可读事件，再复用 Read 完成接收解析。
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  if (!IsValid() || timeout_ms < 0) {
    return false;
  }

  pollfd descriptor{};
  descriptor.fd = fd_;
  descriptor.events = POLLIN;
  int result = 0;
  do {
    result = poll(&descriptor, 1, timeout_ms);
  } while (result < 0 && errno == EINTR);

  if (result == 0) {
    // 超时不视为连接错误，由调用方决定后续重试或关闭。
    return false;
  }
  if (result < 0 || (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL))) {
    // poll 错误、挂断或无效 fd 均表示连接不可继续使用。
    HandleError();
    return false;
  }
  return (descriptor.revents & POLLIN) != 0 && Read();
}

bool Connection::Write(const std::string& data) {
  // 发送缓冲模块：追加待发数据，并处理部分写入和暂时不可写情况。
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  if (data.empty()) {
    return IsValid();
  }

  if (!IsValid()) {
    return false;
  }
  try {
    // 先复制到缓冲区再发送，兼容 send 只写出部分数据的情况。
    write_buffer_.insert(write_buffer_.end(), data.begin(), data.end());
  } catch (...) {
    LOG_ERROR("Failed to buffer outgoing data for {}:{}", peer_addr_,
              peer_port_);
    return false;
  }
  return SendInBuffer();
}

bool Connection::Write(const RpcResponse& response) {
  std::string data;
  if (!response.Serialize(data)) {
    return false;
  }
  return Write(data);
}

bool Connection::SendInBuffer() {
  // 缓冲刷新模块：循环 send 直到全部发送，必要时等待 POLLOUT 事件。
  size_t offset = 0;
  while (offset < write_buffer_.size()) {
#ifdef MSG_NOSIGNAL
    constexpr int kSendFlags = MSG_NOSIGNAL;
#else
    constexpr int kSendFlags = 0;
#endif
    const ssize_t bytes_sent = send(fd_, write_buffer_.data() + offset,
                                    write_buffer_.size() - offset, kSendFlags);
    if (bytes_sent > 0) {
      offset += static_cast<size_t>(bytes_sent);
      continue;
    }
    if (bytes_sent < 0 && errno == EINTR) {
      continue;
    }
    if (bytes_sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      // 发送缓冲已满时等待最多一秒可写，避免忙等占用 CPU。
      pollfd descriptor{};
      descriptor.fd = fd_;
      descriptor.events = POLLOUT;
      int poll_result = 0;
      do {
        poll_result = poll(&descriptor, 1, 1000);
      } while (poll_result < 0 && errno == EINTR);
      if (poll_result > 0 && (descriptor.revents & POLLOUT) != 0) {
        continue;
      }
    }

    LOG_ERROR("Failed to send data to {}:{}: {}", peer_addr_, peer_port_,
              std::strerror(errno));
    write_buffer_.erase(write_buffer_.begin(),
                        write_buffer_.begin() + static_cast<ptrdiff_t>(offset));
    HandleError();
    return false;
  }

  write_buffer_.clear();
  return true;
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
  // 幂等关闭模块：状态机防止重复关闭，并在释放套接字后通知管理者。
  std::lock_guard<std::recursive_mutex> lock(io_mutex_);
  if (state_ == State::DISCONNECTED || state_ == State::DISCONNECTING) {
    return;
  }

  state_ = State::DISCONNECTING;
  const int fd = fd_;
  // 先切换状态再关闭 fd，使并发/递归调用观察到正在关闭状态。
  if (fd >= 0) {
    shutdown(fd, SHUT_RDWR);
    close(fd);
  }
  state_ = State::DISCONNECTED;

  if (close_callback_ != nullptr) {
    const std::shared_ptr<Connection> self = weak_from_this().lock();
    if (self != nullptr) {
      close_callback_(self);
    }
  }
  fd_ = -1;
}
}  // namespace cookrpc
