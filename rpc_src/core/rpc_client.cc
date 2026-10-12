#include "rpc_client.h"

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
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <utility>

#include "config_validation.h"
#include "load_balancer.h"
#include "zk_conn_handler.h"

namespace cookrpc {
namespace {

// 协议帧与端口校验辅助函数：负责解析网络字节序字段并验证端口范围。
constexpr size_t kHeaderSize = RpcHeader::kHeaderSize;

uint32_t ReadNetworkUint32(const char* data) {
  const auto* bytes = reinterpret_cast<const unsigned char*>(data);
  return (static_cast<uint32_t>(bytes[0]) << 24U) |
         (static_cast<uint32_t>(bytes[1]) << 16U) |
         (static_cast<uint32_t>(bytes[2]) << 8U) |
         static_cast<uint32_t>(bytes[3]);
}

bool IsValidPort(int port) { return port > 0 && port <= 65535; }

}  // namespace

// 生命周期模块：构造时加载配置，析构时释放连接资源。
RpcClient::RpcClient(const std::string& config_path)
    : discovery_(std::make_unique<ZkConnHandler>()) {
  if (!LoadConfig(config_path)) {
    CLIENT_ERROR("Failed to initialize RPC client from {}", config_path);
  }
}

RpcClient::~RpcClient() { Disconnect(); }

// 配置模块：读取客户端参数，校验取值，并初始化负载均衡和 ZooKeeper 服务发现。
bool RpcClient::LoadConfig(const std::string& config_path) {
  std::lock_guard<std::mutex> lock(mutex_);

  try {
    // 配置文件使用 JSON 读取；缺省字段采用与示例配置一致的默认值。
    std::ifstream config_file(config_path);
    if (!config_file.is_open()) {
      CLIENT_ERROR("Failed to open client config: {}", config_path);
      return false;
    }

    const nlohmann::json config = nlohmann::json::parse(config_file);
    const std::string zk_host = config.value("zk_host", "localhost");
    const int zk_port =
        config_validation::ReadInteger(config, "zk_port", 2181, 1, 65535);
    std::string zk_namespace = config.value("zk_namespace", "/cookrpc");
    const int timeout_ms = config_validation::ReadInteger(config, "timeout_ms",
                                                          3000, 1, INT32_MAX);
    const int retry_times =
        config_validation::ReadInteger(config, "retry_times", 3, 0, 100);
    const int server_port =
        config_validation::ReadInteger(config, "server_port", 8989, 1, 65535);
    const std::string load_balance_strategy =
        config.value("load_balance_strategy", "random");

    // ZooKeeper 命名空间必须是绝对路径，端口、超时和重试次数必须在安全范围内。
    if (zk_host.empty() || zk_namespace.empty() ||
        zk_namespace.front() != '/' || !IsValidPort(zk_port) ||
        !IsValidPort(server_port) || timeout_ms <= 0 || retry_times < 0 ||
        retry_times > 100) {
      CLIENT_ERROR("Invalid RPC client configuration in {}", config_path);
      return false;
    }

    // 先初始化服务发现，成功后再提交客户端配置，避免留下半初始化状态。
    if (!discovery_->InitZkConnHandler(config)) {
      CLIENT_ERROR("Failed to initialize ZooKeeper service discovery");
      return false;
    }

    if (!LoadBalancer::InitBalancer(load_balance_strategy)) {
      CLIENT_WARN("Using fallback load balancing strategy");
    }

    DisconnectUnlocked();
    timeout_ms_ = timeout_ms;
    retry_times_ = retry_times;
    default_server_port_ = server_port;
    zk_namespace_ = std::move(zk_namespace);
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

// 连接建立模块：每次重试重新发现服务并创建套接字，避免复用失败的文件描述符。
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
    // 每次尝试都重新从注册中心取地址，适应服务实例变化或故障转移。
    const std::string server_address = GetServerAddress();
    std::string server_ip;
    uint16_t server_port = 0;
    if (!ParseServerAddress(server_address, server_ip, server_port)) {
      CLIENT_ERROR("Invalid server address from service discovery: {}",
                   server_address);
    } else {
      int fd = -1;
      if (InitSocket(fd) && TryConnect(fd, server_ip, server_port, attempt)) {
        // Connection 从构造开始接管成功的 fd，后续由 shared_ptr
        // 和析构函数负责关闭。
        auto connection = std::make_shared<Connection>(fd);
        fd = -1;
        if (connection->IsValid()) {
          socket_fd_ = connection->GetFd();
          connection_ = std::move(connection);
          pending_response_data_.clear();
          is_connected_.store(true, std::memory_order_release);
          CLIENT_INFO("Connected to RPC server {}:{}", server_ip, server_port);
          return true;
        }
      }
      if (fd >= 0) {
        close(fd);
      }
    }

    if (attempt < maximum_attempts) {
      // 短暂退避，避免服务暂时不可用时立即密集重试。
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
  // 先标记不可用并清空残留字节，防止旧响应被下一次调用误读。
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

// 重连与健康检查模块：发现连接失效时清理旧状态并重新执行服务发现和建连。
bool RpcClient::Reconnect() {
  DisconnectUnlocked();
  return ConnectUnlocked();
}

bool RpcClient::CheckConnection() {
  if (is_connected_.load(std::memory_order_acquire) && connection_ != nullptr &&
      connection_->IsValid()) {
    return true;
  }
  CLIENT_WARN("RPC connection is unavailable; reconnecting");
  return Reconnect();
}

std::string RpcClient::GetServerAddress() const {
  if (zk_namespace_.empty()) {
    return {};
  }
  return discovery_->GetServer(zk_namespace_);
}

// 服务发现与地址解析模块：从注册中心获取地址，并兼容“IPv4:端口”和仅 IPv4
// 两种格式。
bool RpcClient::ParseServerAddress(const std::string& server_address,
                                   std::string& server_ip,
                                   uint16_t& server_port) const {
  if (server_address.empty()) {
    return false;
  }

  const size_t separator = server_address.rfind(':');
  // 未携带端口时使用配置中的默认端口；带端口时要求整段文本均为数字。
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
  // inet_pton 同时完成 IPv4 格式校验，避免向 connect 传入非法地址。
  if (server_ip.empty() ||
      inet_pton(AF_INET, server_ip.c_str(), &address) != 1 ||
      !IsValidPort(parsed_port)) {
    return false;
  }
  server_port = static_cast<uint16_t>(parsed_port);
  return true;
}

// Socket 初始化模块：创建 IPv4 TCP 套接字，设置非阻塞、
// close-on-exec、低延迟和保活选项。
bool RpcClient::InitSocket(int& fd) const {
  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    CLIENT_ERROR("Failed to create client socket: {}", std::strerror(errno));
    return false;
  }

  const int status_flags = fcntl(fd, F_GETFL, 0);
  const int descriptor_flags = fcntl(fd, F_GETFD, 0);
  // 非阻塞模式用于实现可控连接超时，FD_CLOEXEC 防止子进程继承连接。
  if (status_flags < 0 || descriptor_flags < 0 ||
      fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) < 0 ||
      fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) < 0) {
    CLIENT_ERROR("Failed to configure client socket: {}", std::strerror(errno));
    close(fd);
    fd = -1;
    return false;
  }

  const int enabled = 1;
  // TCP_NODELAY 降低小 RPC 请求延迟，SO_KEEPALIVE 用于探测长期失效连接。
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

// 非阻塞连接模块：先发起异步 connect，再通过 poll 统一实现连接超时和错误检查。
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
    // 非阻塞 connect 的正常结果是 EINPROGRESS，交由 poll 等待最终状态。
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

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms_);
  int poll_result = 0;
  // EINTR 只表示被信号打断，重试等待；其他错误直接结束本次连接尝试。
  do {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      poll_result = 0;
      break;
    }
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    poll_result =
        poll(&descriptor, 1,
             static_cast<int>(std::max<int64_t>(1, remaining.count())));
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
  // poll 返回可写不代表 connect 成功，必须通过 SO_ERROR 获取真实结果。
  if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_length) < 0 ||
      socket_error != 0) {
    CLIENT_WARN("Connection attempt {} failed: {}", attempt,
                std::strerror(socket_error == 0 ? errno : socket_error));
    return false;
  }
  return true;
}

// 请求标识模块：以原子计数器生成非零序列号，用于匹配请求和响应。
uint32_t RpcClient::GenerateSequenceId() {
  uint32_t sequence_id =
      sequence_id_.fetch_add(1, std::memory_order_relaxed) + 1;
  if (sequence_id == 0) {
    // 跳过回绕到 0 的值，0 保留为无效序列号。
    sequence_id = sequence_id_.fetch_add(1, std::memory_order_relaxed) + 1;
  }
  return sequence_id;
}

// 请求发送模块：生成的完整请求帧交给 Connection，后者负责短写和 SIGPIPE 处理。
bool RpcClient::SendRequest(const std::string& request_frame) {
  if (connection_ == nullptr ||
      !connection_->WriteWithTimeout(request_frame, timeout_ms_)) {
    CLIENT_ERROR("Failed to send RPC request");
    if (connection_ == nullptr || !connection_->IsValid()) {
      DisconnectUnlocked();
    }
    return false;
  }
  return true;
}

// 响应接收与拆帧模块：累积 TCP 字节流，校验协议头，并按声明长度提取完整响应帧。
bool RpcClient::ReadResponseFrame(std::string& response_frame) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms_);

  bool terminal = false;
  bool read_timeout = false;
  while (true) {
    if (pending_response_data_.size() >= kHeaderSize) {
      // 只有收到完整协议头后才能读取 body 长度，避免越界访问。
      const uint32_t magic_number =
          ReadNetworkUint32(pending_response_data_.data());
      const uint32_t body_size =
          ReadNetworkUint32(pending_response_data_.data() + sizeof(uint32_t));
      if (magic_number != RpcHeader::MAGIC ||
          body_size > Connection::MAX_MESSAGE_SIZE) {
        CLIENT_ERROR("Received an invalid RPC response frame");
        DisconnectUnlocked();
        return false;
      }

      const size_t frame_size = kHeaderSize + static_cast<size_t>(body_size);
      if (pending_response_data_.size() >= frame_size) {
        // 提取一帧并保留后续字节，支持一次 read 收到多帧的情况。
        response_frame.assign(pending_response_data_.data(), frame_size);
        pending_response_data_.erase(0, frame_size);
        if (terminal || (connection_ && connection_->HasReadEof()))
          DisconnectUnlocked();
        return true;
      }
    }

    if (terminal) {
      CLIENT_ERROR(
          read_timeout
              ? "Timed out waiting for an RPC response"
              : "RPC connection closed before a complete response arrived");
      DisconnectUnlocked();
      return false;
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
    auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    if (remaining.count() <= 0) {
      remaining = std::chrono::milliseconds(1);
    }
    const int remaining_ms = static_cast<int>(
        std::min<int64_t>(remaining.count(), std::numeric_limits<int>::max()));
    // 每次读取只等待剩余期限，确保多次分段读取不会突破总超时时间。
    const auto result = connection_->ReadWithTimeout(remaining_ms);
    std::string received_data = connection_->GetReadBufferData();
    pending_response_data_.append(received_data);
    // 下一轮先解析新增数据，再处理 EOF/错误/超时。
    terminal = result.eof || result.error || result.timeout;
    read_timeout = result.timeout;
    // 限制累积缓冲区大小，防止异常对端导致无界内存增长。
    if (pending_response_data_.size() >
        Connection::MAX_MESSAGE_SIZE + kHeaderSize) {
      CLIENT_ERROR("RPC response buffer exceeds the maximum size");
      DisconnectUnlocked();
      return false;
    }
  }
}

}  // namespace cookrpc
