#include <netinet/tcp.h>

#include <cstdint>
#include <memory>
#include <string>

#include "rpc_server_config.h"

namespace cookrpc {

// 监听套接字封装：负责服务端 TCP socket 的创建、配置和接收连接。
class CreateSocket {
 public:
  static std::shared_ptr<CreateSocket> Create(
      const std::string& servers_name_prefix, uint16_t servers_port,
      int servers_max_connections, int socket_timeout_ms,
      const std::string& servers_ip);

  static std::shared_ptr<CreateSocket> Create(
      const cookrpc::RpcServerConfig& config);

  ~CreateSocket();

  int GetFd() const { return fd_; }

  const std::string& GetIp() const { return servers_ip_; }

  uint16_t GetPort() const { return servers_port_; }

  CreateSocket(const std::string& servers_name_prefix, uint16_t servers_port,
               int servers_max_connections, int socket_timeout_ms,
               const std::string& servers_ip);

  CreateSocket(const cookrpc::RpcServerConfig& config);

  // 从监听队列接收一个已配置的客户端文件描述符。
  int Accept();

 private:
  bool Init();
  bool SetSocketOpt();
  bool SetClientSocketOpt(int client_fd);

  int fd_;
  bool is_init_;
  std::string servers_name_prefix_;
  std::string servers_ip_;
  uint16_t servers_port_;
  int servers_max_connections_;
  int socket_timeout_ms_;
};
}  // namespace cookrpc
