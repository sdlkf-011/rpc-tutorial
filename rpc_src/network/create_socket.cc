#include "network/create_socket.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <limits>
#include <memory>
#include <string>

#include "log_manager.h"

namespace cookrpc {
namespace {

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
}  // namespace

std::shared_ptr<CreateSocket> CreateSocket::Create(
    const std::string& servers_name_prefix, uint16_t servers_port,
    int servers_max_connections, int socket_timeout_ms,
    const std::string& servers_ip) {
  auto socket = std::shared_ptr<CreateSocket>(new CreateSocket(
      servers_name_prefix, servers_port, servers_max_connections,
      socket_timeout_ms, servers_ip));
  return socket->Init() ? socket : nullptr;
}

std::shared_ptr<CreateSocket> CreateSocket::Create(
    const RpcServerConfig& config) {
  if (config.GetMaxConnections() >
          static_cast<uint32_t>(std::numeric_limits<int>::max()) ||
      config.GetTimeout() >
          static_cast<uint32_t>(std::numeric_limits<int>::max())) {
    LOG_ERROR("RPC server socket configuration exceeds supported range");
    return nullptr;
  }

  return Create(config.GetServersNameProfix(), config.GetServersPort(),
                static_cast<int>(config.GetMaxConnections()),
                static_cast<int>(config.GetTimeout()), config.GetServersIp());
}

CreateSocket::CreateSocket(const std::string& servers_name_prefix,
                           uint16_t servers_port,
                           int servers_max_connections,
                           int socket_timeout_ms,
                           const std::string& servers_ip)
    : fd_(-1),
      is_init_(false),
      servers_name_prefix_(servers_name_prefix),
      servers_ip_(servers_ip),
      servers_port_(servers_port),
      servers_max_connections_(servers_max_connections),
      socket_timeout_ms_(socket_timeout_ms) {}

CreateSocket::CreateSocket(const RpcServerConfig& config)
    : CreateSocket(
          config.GetServersNameProfix(), config.GetServersPort(),
          config.GetMaxConnections() >
                  static_cast<uint32_t>(std::numeric_limits<int>::max())
              ? -1
              : static_cast<int>(config.GetMaxConnections()),
          config.GetTimeout() >
                  static_cast<uint32_t>(std::numeric_limits<int>::max())
              ? -1
              : static_cast<int>(config.GetTimeout()),
          config.GetServersIp()) {}

CreateSocket::~CreateSocket() {
  if (fd_ >= 0) {
    close(fd_);
    fd_ = -1;
  }
  is_init_ = false;
}

bool CreateSocket::Init() {
  if (is_init_) {
    return true;
  }
  if (servers_ip_.empty() || servers_port_ == 0 ||
      servers_max_connections_ <= 0 || socket_timeout_ms_ < 0) {
    LOG_ERROR("Invalid RPC server socket configuration");
    return false;
  }

  fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (fd_ < 0) {
    LOG_ERROR("Failed to create server socket: {}", std::strerror(errno));
    return false;
  }

  if (!SetSocketOpt()) {
    close(fd_);
    fd_ = -1;
    return false;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(servers_port_);
  if (inet_pton(AF_INET, servers_ip_.c_str(), &address.sin_addr) != 1) {
    LOG_ERROR("Invalid server IPv4 address: {}", servers_ip_);
    close(fd_);
    fd_ = -1;
    return false;
  }

  if (bind(fd_, reinterpret_cast<const sockaddr*>(&address),
           sizeof(address)) < 0) {
    LOG_ERROR("Failed to bind {}:{}: {}", servers_ip_, servers_port_,
              std::strerror(errno));
    close(fd_);
    fd_ = -1;
    return false;
  }

  if (listen(fd_, servers_max_connections_) < 0) {
    LOG_ERROR("Failed to listen on {}:{}: {}", servers_ip_, servers_port_,
              std::strerror(errno));
    close(fd_);
    fd_ = -1;
    return false;
  }

  is_init_ = true;
  LOG_INFO("RPC server socket {} listening on {}:{}", servers_name_prefix_,
           servers_ip_, servers_port_);
  return true;
}

bool CreateSocket::SetSocketOpt() {
  const int enabled = 1;
  if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) <
      0) {
    LOG_ERROR("Failed to set SO_REUSEADDR: {}", std::strerror(errno));
    return false;
  }

#ifdef SO_REUSEPORT
  if (setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &enabled, sizeof(enabled)) <
      0) {
    LOG_WARN("Failed to set SO_REUSEPORT: {}", std::strerror(errno));
  }
#endif

  if (!SetNonBlockingAndCloseOnExec(fd_)) {
    LOG_ERROR("Failed to configure server socket flags: {}",
              std::strerror(errno));
    return false;
  }
  return true;
}

bool CreateSocket::SetClientSocketOpt(int client_fd) {
  const int enabled = 1;
  if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                 sizeof(enabled)) < 0 ||
      setsockopt(client_fd, SOL_SOCKET, SO_KEEPALIVE, &enabled,
                 sizeof(enabled)) < 0) {
    LOG_ERROR("Failed to configure accepted socket: {}", std::strerror(errno));
    return false;
  }

#ifdef SO_NOSIGPIPE
  if (setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
                 sizeof(enabled)) < 0) {
    LOG_ERROR("Failed to set SO_NOSIGPIPE: {}", std::strerror(errno));
    return false;
  }
#endif

  if (socket_timeout_ms_ > 0) {
    timeval timeout{};
    timeout.tv_sec = socket_timeout_ms_ / 1000;
    timeout.tv_usec = (socket_timeout_ms_ % 1000) * 1000;
    if (setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout)) < 0 ||
        setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                   sizeof(timeout)) < 0) {
      LOG_ERROR("Failed to set client socket timeout: {}",
                std::strerror(errno));
      return false;
    }
  }

  if (!SetNonBlockingAndCloseOnExec(client_fd)) {
    LOG_ERROR("Failed to configure accepted socket flags: {}",
              std::strerror(errno));
    return false;
  }
  return true;
}

int CreateSocket::Accept() {
  if (!is_init_ || fd_ < 0) {
    errno = EBADF;
    return -1;
  }

  int client_fd = -1;
  do {
    client_fd = accept(fd_, nullptr, nullptr);
  } while (client_fd < 0 && errno == EINTR);

  if (client_fd < 0) {
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      LOG_ERROR("Failed to accept client connection: {}", std::strerror(errno));
    }
    return -1;
  }

  if (!SetClientSocketOpt(client_fd)) {
    close(client_fd);
    return -1;
  }
  return client_fd;
}
}  // namespace cookrpc
