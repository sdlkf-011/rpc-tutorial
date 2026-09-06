#include "create_socket.h"

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

// 套接字属性辅助模块：统一设置非阻塞和 exec 时自动关闭标志。
bool SetNonBlockingAndCloseOnExec(int fd) {
  // 分两次 fcntl 设置状态位和描述符位，避免覆盖已有标志。
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
  // 工厂创建模块：构造监听器后立即初始化，失败返回空指针。
  auto socket = std::shared_ptr<CreateSocket>(
      new CreateSocket(servers_name_prefix, servers_port,
                       servers_max_connections, socket_timeout_ms, servers_ip));
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
                           uint16_t servers_port, int servers_max_connections,
                           int socket_timeout_ms, const std::string& servers_ip)
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
  // 监听初始化模块：校验配置，创建 IPv4 TCP 套接字并完成 bind/listen。
  if (is_init_) {
    // Init 可重复调用，已初始化时直接复用现有监听 socket。
    return true;
  }
  if (servers_ip_.empty() || servers_port_ == 0 ||
      servers_max_connections_ <= 0 || socket_timeout_ms_ < 0) {
    LOG_ERROR("Invalid RPC server socket configuration");
    return false;
  }

  fd_ = socket(AF_INET, SOCK_STREAM, 0);
  // 先创建 IPv4/TCP 监听端点，后续再绑定具体地址。
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
    // inet_pton 同时完成格式校验和文本到二进制地址转换。
    LOG_ERROR("Invalid server IPv4 address: {}", servers_ip_);
    close(fd_);
    fd_ = -1;
    return false;
  }

  if (bind(fd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) <
      0) {
    // bind 将监听 socket 固定到配置的本地 IP 和端口。
    LOG_ERROR("Failed to bind {}:{}: {}", servers_ip_, servers_port_,
              std::strerror(errno));
    close(fd_);
    fd_ = -1;
    return false;
  }

  if (listen(fd_, servers_max_connections_) < 0) {
    // backlog 控制内核等待队列上可暂存的未处理连接数量。
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
  // 监听套接字选项模块：开启地址/端口复用并配置非阻塞属性。
  const int enabled = 1;
  if (setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) <
      0) {
    // 允许服务重启后快速重新绑定近期使用过的端口。
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
  // 客户端套接字选项模块：设置低延迟、保活、超时及跨平台防 SIGPIPE。
  const int enabled = 1;
  if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                 sizeof(enabled)) < 0 ||
      setsockopt(client_fd, SOL_SOCKET, SO_KEEPALIVE, &enabled,
                 sizeof(enabled)) < 0) {
    // 关闭 Nagle 并开启保活，降低 RPC 小报文延迟并发现断链。
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
    // 将毫秒配置拆分为 timeval，分别应用于收发超时。
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
  // 接收连接模块：处理 EINTR/EAGAIN，并为新连接应用统一套接字选项。
  if (!is_init_ || fd_ < 0) {
    errno = EBADF;
    return -1;
  }

  int client_fd = -1;
  do {
    // accept 可能被信号打断，重试直到获得连接或真实错误。
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
