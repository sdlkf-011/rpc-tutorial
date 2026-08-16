#include "core/rpc_client.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>

#include "conn_balancer/load_balancer.h"
#include "conn_balancer/zk_conn_handler.h"

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

bool IsValidPort(int port) { return port > 0 && port <= 65535; }

}  // namespace

RpcClient::RpcClient(const std::string& config_path) {
  if (!LoadConfig(config_path)) {
    CLIENT_ERROR("Failed to initialize RPC client from {}", config_path);
  }
}

RpcClient::~RpcClient() { Disconnect(); }

// 配置模块：
// 校验配置后初始化负载均衡与 ZooKeeper 服务发现。
bool RpcClient::LoadConfig(const std::string& config_path) {
  std::lock_guard<std::mutex> lock(mutex_);

  try {
    std::ifstream config_file(config_path);
    if (!config_file.is_open()) {
      CLIENT_ERROR("Failed to open client config: {}", config_path);
      return false;
    }

    const nlohmann::json config = nlohmann::json::parse(config_file);
    const std::string zk_host = config.value("zk_host", "localhost");
    const int zk_port = config.value("zk_port", 2181);
    const std::string zk_namespace =
        config.value("zk_namespace", "/cookrpc");
    const int timeout_ms = config.value("timeout_ms", 3000);
    const int retry_times = config.value("retry_times", 3);
    const int server_port = config.value("server_port", 8989);
    const std::string load_balance_strategy =
        config.value("load_balance_strategy", "random");

    if (zk_host.empty() || zk_namespace.empty() ||
        zk_namespace.front() != '/' || !IsValidPort(zk_port) ||
        !IsValidPort(server_port) || timeout_ms <= 0 || retry_times < 0 ||
        retry_times > 100) {
      CLIENT_ERROR("Invalid RPC client configuration in {}", config_path);
      return false;
    }

    if (!LoadBalancer::InitBalancer(load_balance_strategy)) {
      CLIENT_WARN("Using fallback load balancing strategy");
    }
    if (!ZkConnHandler::GetInstance().InitZkConnHandler(config)) {
      CLIENT_ERROR("Failed to initialize ZooKeeper service discovery");
      return false;
    }

    DisconnectUnlocked();
    timeout_ms_ = timeout_ms;
    retry_times_ = retry_times;
    default_server_port_ = server_port;
    zk_namespace_ = zk_namespace;
    is_configured_ = true;
    CLIENT_INFO("RPC client configured for ZooKeeper {}:{} namespace {}",
                zk_host, zk_port, zk_namespace_);
    return true;
  } catch (const std::exception& error) {
    CLIENT_ERROR("Failed to load client config {}: {}", config_path,
                 error.what());
  }
  return false;
}

bool RpcClient::Connect() {
  std::lock_guard<std::mutex> lock(mutex_);
  return ConnectUnlocked();
}

// 连接模块：
// 每次重试重新发现服务并创建套接字。
// 避免复用失败的 fd。
bool RpcClient::ConnectUnlocked() {
  if (!is_configured_) {
    CLIENT_ERROR("RPC client is not configured");
    return false;
  }
  if (connection_ != nullptr && connection_->IsValid()) {
    is_connected_.store(true, std::memory_order_release);
    return true;
  }

  DisconnectUnlocked();
  const int maximum_attempts = retry_times_ + 1;
  for (int attempt = 1; attempt <= maximum_attempts; ++attempt) {
    const std::string server_address = GetServerAddress();
    std::string server_ip;
    uint16_t server_port = 0;
    if (!ParseServerAddress(server_address, server_ip, server_port)) {
      CLIENT_ERROR("Invalid server address from service discovery: {}",
                   server_address);
    } else {
      int fd = -1;
      if (InitSocket(fd) && TryConnect(fd, server_ip, server_port, attempt)) {
        auto connection = std::make_shared<Connection>(fd);
        // Connection 从构造开始接管 fd。
        // 后续失败也由其析构函数关闭。
        fd = -1;
        if (connection->IsValid()) {
          socket_fd_ = connection->GetFd();
          connection_ = std::move(connection);
          pending_response_data_.clear();
          is_connected_.store(true, std::memory_order_release);
          CLIENT_INFO("Connected to RPC server {}:{}", server_ip,
                      server_port);
          return true;
        }
      }
      if (fd >= 0) {
        close(fd);
      }
    }

    if (attempt < maximum_attempts) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  CLIENT_ERROR("Failed to connect to an RPC server after {} attempts",
               maximum_attempts);
  return false;
}

void RpcClient::Disconnect() {
  std::lock_guard<std::mutex> lock(mutex_);
  DisconnectUnlocked();
}

void RpcClient::DisconnectUnlocked() {
  is_connected_.store(false, std::memory_order_release);
  pending_response_data_.clear();
  if (connection_ != nullptr) {
    connection_->Close();
    connection_.reset();
  } else if (socket_fd_ >= 0) {
    close(socket_fd_);
  }
  socket_fd_ = -1;
}

bool RpcClient::Reconnect() {
  DisconnectUnlocked();
  return ConnectUnlocked();
}

bool RpcClient::CheckConnection() {
  if (is_connected_.load(std::memory_order_acquire) &&
      connection_ != nullptr && connection_->IsValid()) {
    return true;
  }
  CLIENT_WARN("RPC connection is unavailable; reconnecting");
  return Reconnect();
}

std::string RpcClient::GetServerAddress() const {
  if (zk_namespace_.empty()) {
    return {};
  }
  return ZkConnHandler::GetInstance().GetServer(zk_namespace_);
}

// 地址解析模块：注册中心可返回“IPv4:端口”或仅 IPv4。
bool RpcClient::ParseServerAddress(const std::string& server_address,
                                   std::string& server_ip,
                                   uint16_t& server_port) const {
  if (server_address.empty()) {
    return false;
  }

  const size_t separator = server_address.rfind(':');
  server_ip = separator == std::string::npos
                  ? server_address
                  : server_address.substr(0, separator);
  int parsed_port = default_server_port_;
  if (separator != std::string::npos) {
    const std::string port_text = server_address.substr(separator + 1);
    if (port_text.empty()) {
      return false;
    }
    size_t parsed_characters = 0;
    try {
      parsed_port = std::stoi(port_text, &parsed_characters);
    } catch (const std::exception&) {
      return false;
    }
    if (parsed_characters != port_text.size()) {
      return false;
    }
  }

  in_addr address{};
  if (server_ip.empty() ||
      inet_pton(AF_INET, server_ip.c_str(), &address) != 1 ||
      !IsValidPort(parsed_port)) {
    return false;
  }
  server_port = static_cast<uint16_t>(parsed_port);
  return true;
}

bool RpcClient::InitSocket(int& fd) const {
  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    CLIENT_ERROR("Failed to create client socket: {}", std::strerror(errno));
    return false;
  }

  const int status_flags = fcntl(fd, F_GETFL, 0);
  const int descriptor_flags = fcntl(fd, F_GETFD, 0);
  if (status_flags < 0 || descriptor_flags < 0 ||
      fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) < 0 ||
      fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) < 0) {
    CLIENT_ERROR("Failed to configure client socket: {}",
                 std::strerror(errno));
    close(fd);
    fd = -1;
    return false;
  }

  const int enabled = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) < 0 ||
      setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled)) < 0) {
    CLIENT_ERROR("Failed to set client socket options: {}",
                 std::strerror(errno));
    close(fd);
    fd = -1;
    return false;
  }
#ifdef SO_NOSIGPIPE
  if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) < 0) {
    CLIENT_ERROR("Failed to enable SO_NOSIGPIPE: {}", std::strerror(errno));
    close(fd);
    fd = -1;
    return false;
  }
#endif
  return true;
}

// 非阻塞建连模块：poll 统一实现连接超时。
bool RpcClient::TryConnect(int fd, const std::string& server_ip,
                           uint16_t server_port, int attempt) const {
  sockaddr_in server_address{};
  server_address.sin_family = AF_INET;
  server_address.sin_port = htons(server_port);
  if (inet_pton(AF_INET, server_ip.c_str(), &server_address.sin_addr) != 1) {
    return false;
  }

  if (connect(fd, reinterpret_cast<const sockaddr*>(&server_address),
              sizeof(server_address)) == 0) {
    return true;
  }
  if (errno == EINPROGRESS) {
    return WaitForConnection(fd, attempt);
  }

  CLIENT_WARN("Connection attempt {} failed: {}", attempt,
              std::strerror(errno));
  return false;
}

bool RpcClient::WaitForConnection(int fd, int attempt) const {
  pollfd descriptor{};
  descriptor.fd = fd;
  descriptor.events = POLLOUT;

  int poll_result = 0;
  do {
    poll_result = poll(&descriptor, 1, timeout_ms_);
  } while (poll_result < 0 && errno == EINTR);

  if (poll_result == 0) {
    CLIENT_WARN("Connection attempt {} timed out", attempt);
    return false;
  }
  if (poll_result < 0 ||
      (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
    CLIENT_WARN("Connection attempt {} failed while polling", attempt);
    return false;
  }

  int socket_error = 0;
  socklen_t error_length = sizeof(socket_error);
  if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_length) < 0 ||
      socket_error != 0) {
    CLIENT_WARN("Connection attempt {} failed: {}", attempt,
                std::strerror(socket_error == 0 ? errno : socket_error));
    return false;
  }
  return true;
}

uint32_t RpcClient::GenerateSequenceId() {
  uint32_t sequence_id =
      sequence_id_.fetch_add(1, std::memory_order_relaxed) + 1;
  if (sequence_id == 0) {
    sequence_id = sequence_id_.fetch_add(1, std::memory_order_relaxed) + 1;
  }
  return sequence_id;
}

// 请求发送模块：Connection 负责处理短写和 SIGPIPE。
bool RpcClient::SendRequest(const std::string& request_frame) {
  if (connection_ == nullptr || !connection_->Write(request_frame)) {
    CLIENT_ERROR("Failed to send RPC request");
    if (connection_ == nullptr || !connection_->IsValid()) {
      DisconnectUnlocked();
    }
    return false;
  }
  return true;
}

// 响应收帧模块：
// 累积 TCP 字节流，按协议头声明的长度提取完整帧。
bool RpcClient::ReadResponseFrame(std::string& response_frame) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms_);

  while (true) {
    if (pending_response_data_.size() >= kHeaderSize) {
      const uint32_t magic_number =
          ReadNetworkUint32(pending_response_data_.data());
      const uint32_t body_size = ReadNetworkUint32(
          pending_response_data_.data() + sizeof(uint32_t));
      if (magic_number != RpcHeader::MAGIC ||
          body_size > Connection::MAX_MESSAGE_SIZE) {
        CLIENT_ERROR("Received an invalid RPC response frame");
        DisconnectUnlocked();
        return false;
      }

      const size_t frame_size = kHeaderSize + static_cast<size_t>(body_size);
      if (pending_response_data_.size() >= frame_size) {
        response_frame.assign(pending_response_data_.data(), frame_size);
        pending_response_data_.erase(0, frame_size);
        return true;
      }
    }

    if (connection_ == nullptr || !connection_->IsValid()) {
      CLIENT_ERROR("RPC connection closed while waiting for a response");
      DisconnectUnlocked();
      return false;
    }

    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      CLIENT_ERROR("Timed out waiting for an RPC response");
      // 超时后旧响应仍可能到达。
      // 关闭连接可避免污染下一次调用。
      DisconnectUnlocked();
      return false;
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - now);
    if (remaining.count() <= 0) {
      remaining = std::chrono::milliseconds(1);
    }
    const int remaining_ms = static_cast<int>(
        std::min<int64_t>(remaining.count(), std::numeric_limits<int>::max()));
    if (!connection_->ReadWithTimeout(remaining_ms)) {
      if (!connection_->IsValid()) {
        DisconnectUnlocked();
      } else {
        CLIENT_ERROR("Timed out waiting for an RPC response");
        DisconnectUnlocked();
      }
      return false;
    }

    std::string received_data = connection_->GetReadBufferData();
    if (received_data.empty()) {
      continue;
    }
    pending_response_data_.append(received_data);
    if (pending_response_data_.size() >
        Connection::MAX_MESSAGE_SIZE + kHeaderSize) {
      CLIENT_ERROR("RPC response buffer exceeds the maximum size");
      DisconnectUnlocked();
      return false;
    }
  }
}

}  // namespace cookrpc
