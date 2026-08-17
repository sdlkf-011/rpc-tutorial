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
#include <thread>

#include "log_manager.h"

namespace cookrpc {
namespace {

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
  if (fd_ < 0) {
    return;
  }

  const int status_flags = fcntl(fd_, F_GETFL, 0);
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
  if (fd_ >= 0) {
    shutdown(fd_, SHUT_RDWR);
    close(fd_);
    fd_ = -1;
  }
  state_ = State::DISCONNECTED;
}

bool Connection::Read() {
  if (!IsValid()) {
    return false;
  }

  char buffer[MAX_BUFFER_SIZE];
  while (true) {
    const ssize_t bytes_read = recv(fd_, buffer, sizeof(buffer), 0);
    if (bytes_read > 0) {
      read_buffer_.insert(read_buffer_.end(), buffer, buffer + bytes_read);
      if (message_callback_ != nullptr) {
        if (!ProcessMessage()) {
          return false;
        }
      } else if (read_buffer_.size() > MAX_MESSAGE_SIZE + kHeaderSize) {
        LOG_ERROR("Read buffer from {}:{} exceeds maximum message size",
                  peer_addr_, peer_port_);
        HandleError();
        return false;
      }
      continue;
    }

    if (bytes_read == 0) {
      Close();
      return false;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return true;
    }

    LOG_ERROR("Failed to receive data from {}:{}: {}", peer_addr_,
              peer_port_, std::strerror(errno));
    HandleError();
    return false;
  }
}

bool Connection::ReadWithTimeout(int timeout_ms) {
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
    return false;
  }
  if (result < 0 || (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL))) {
    HandleError();
    return false;
  }
  return (descriptor.revents & POLLIN) != 0 && Read();
}

bool Connection::Write(const std::string& data) {
  if (data.empty()) {
    return IsValid();
  }

  bool expected = false;
  while (!is_writing_.compare_exchange_weak(expected, true,
                                             std::memory_order_acquire)) {
    if (!IsValid()) {
      return false;
    }
    expected = false;
    std::this_thread::yield();
  }

  if (!IsValid()) {
    is_writing_.store(false, std::memory_order_release);
    return false;
  }
  try {
    write_buffer_.insert(write_buffer_.end(), data.begin(), data.end());
  } catch (...) {
    is_writing_.store(false, std::memory_order_release);
    LOG_ERROR("Failed to buffer outgoing data for {}:{}", peer_addr_,
              peer_port_);
    return false;
  }
  const bool succeeded = SendInBuffer();
  is_writing_.store(false, std::memory_order_release);
  return succeeded;
}

bool Connection::Write(const RpcResponse& response) {
  std::string data;
  if (!response.Serialize(data)) {
    return false;
  }
  return Write(data);
}

bool Connection::SendInBuffer() {
  size_t offset = 0;
  while (offset < write_buffer_.size()) {
#ifdef MSG_NOSIGNAL
    constexpr int kSendFlags = MSG_NOSIGNAL;
#else
    constexpr int kSendFlags = 0;
#endif
    const ssize_t bytes_sent =
        send(fd_, write_buffer_.data() + offset,
             write_buffer_.size() - offset, kSendFlags);
    if (bytes_sent > 0) {
      offset += static_cast<size_t>(bytes_sent);
      continue;
    }
    if (bytes_sent < 0 && errno == EINTR) {
      continue;
    }
    if (bytes_sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
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
  while (read_buffer_.size() >= kHeaderSize) {
    const uint32_t magic_number = ReadNetworkUint32(read_buffer_.data());
    const uint32_t body_size =
        ReadNetworkUint32(read_buffer_.data() + sizeof(uint32_t));
    if (magic_number != RpcHeader::MAGIC || body_size > MAX_MESSAGE_SIZE) {
      LOG_ERROR("Invalid RPC frame from {}:{}", peer_addr_, peer_port_);
      HandleError();
      return false;
    }

    const size_t frame_size = kHeaderSize + static_cast<size_t>(body_size);
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
    read_buffer_.erase(read_buffer_.begin(),
                       read_buffer_.begin() + static_cast<ptrdiff_t>(frame_size));
    HandleMessage(request);
    if (!IsValid()) {
      return false;
    }
  }
  return true;
}

void Connection::HandleMessage(const RpcRequest& request) {
  if (message_callback_ != nullptr) {
    message_callback_(shared_from_this(), request);
  }
}

void Connection::HandleError() { 
  Close(); 
}

void Connection::Close() {
  if (state_ == State::DISCONNECTED || state_ == State::DISCONNECTING) {
    return;
  }

  state_ = State::DISCONNECTING;
  const int fd = fd_;
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
