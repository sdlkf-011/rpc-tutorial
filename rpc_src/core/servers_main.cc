#include "log_manager.h"

#include <iostream>

#include "load_config/rpc_server_config.h"

int main() {
  // 初始化日志系统
  if (!Logger::GetInstance().Init("../logs")) {
    std::cerr << "Failed to initialize logger" << std::endl;
    return 1;
  }

  // 加载服务器配置
  auto& server_config = cookrpc::RpcServerConfig::GetInstance();
  if (!server_config.InitRpcServerConfig("../config/rpc_server.json")) {
    LOG_FATAL("Failed to load server config");
    return 1;
  }

  // 使用配置启动服务器
  LOG_INFO("Starting RPC server on {}:{}", server_config.GetServersIp(),
           server_config.GetServersPort());

  // 获取线程池配置
  auto* thread_pool_config = server_config.GetThreadPoolConfig();
  LOG_INFO("Thread pool: max={}, core={}", thread_pool_config->GetMaxThreads(),
           thread_pool_config->GetCoreThreads());

  return 0;
}
