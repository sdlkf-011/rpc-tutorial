#ifndef RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_RPC_CLIENT_CONFIG_H_
#define RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_RPC_CLIENT_CONFIG_H_

#include <string>

namespace cookrpc {

class RpcClientConfig {
 public:
  // 单例获取
  static RpcClientConfig& GetInstance() {
    static RpcClientConfig instance;
    return instance;
  }

  // 初始化
  bool InitRpcClientConfig(const std::string& config_filename);

  // Setter 方法
  void SetZkHost(const std::string& zk_host);
  void SetZkNamespace(const std::string& zk_namespace);
  void SetZkPort(int zk_port);
  void SetTimeout(int timeout_ms);
  void SetRetryTimes(int retry_times);
  void SetServerPort(int server_port);

  // Getter 方法
  const std::string& GetZkHost() const { return zk_host_; }

  const std::string& GetZkNamespace() const { return zk_namespace_; }

  int GetZkPort() const { return zk_port_; }

  int GetTimeout() const { return timeout_ms_; }

  int GetRetryTimes() const { return retry_times_; }

  int GetServerPort() const { return server_port_; }

  RpcClientConfig(const RpcClientConfig&) = delete;
  RpcClientConfig& operator=(const RpcClientConfig&) = delete;
  RpcClientConfig(RpcClientConfig&&) = delete;
  RpcClientConfig& operator=(RpcClientConfig&&) = delete;

 private:
  std::string zk_host_;
  std::string zk_namespace_;
  int zk_port_;
  int timeout_ms_ = 3000;  // 默认超时 3 秒
  int retry_times_ = 3;    // 默认重试 3 秒
  int server_port_ = 0;

  RpcClientConfig() = default;
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_LOAD_CONFIG_RPC_CLIENT_CONFIG_H_
