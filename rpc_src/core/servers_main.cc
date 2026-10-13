#include <pthread.h>
#include <signal.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>

#include "config_validation.h"
#include "create_socket.h"
#include "log_manager.h"
#include "message_cycle.h"
#include "rpc_server_config.h"
#include "rpc_service.h"
#include "service_manager.h"
#include "thread_pool_singleton.h"
#include "zk_conn_handler.h"

namespace {

constexpr char kDefaultConfigPath[] = "config/rpc_server.json";

// 屏蔽终止信号，交由专用线程通过 sigwait 统一处理。
bool BlockTerminationSignals(sigset_t& termination_signals) {
  sigemptyset(&termination_signals);
  sigaddset(&termination_signals, SIGINT);
  sigaddset(&termination_signals, SIGTERM);
  const int result = pthread_sigmask(SIG_BLOCK, &termination_signals, nullptr);
  if (result != 0) {
    LOG_ERROR("Failed to block termination signals: {}", std::strerror(result));
    return false;
  }
  return true;
}

// 将项目线程池配置转换为线程池库所需的配置结构。
meeting_ctrl::ThreadPoolStruct BuildThreadPoolConfig(
    const cookrpc::ThreadPoolConfig& config) {
  meeting_ctrl::ThreadPoolStruct thread_pool_config;
  thread_pool_config.core_threads = config.GetCoreThreads();
  thread_pool_config.max_threads = config.GetMaxThreads();
  thread_pool_config.max_queue_size = config.GetQueueSize();
  thread_pool_config.keep_alive_time =
      std::chrono::seconds(config.GetKeepAliveTime());
  return thread_pool_config;
}

// 初始化 ZooKeeper 连接并发布配置中的 RPC 服务实例。
bool InitializeServiceRegistry(
    const std::string& config_path,
    const cookrpc::ServiceRegistryConfig* registry_config) {
  try {
    std::ifstream config_file(config_path);
    if (!config_file.is_open()) {
      LOG_ERROR("Failed to reopen server config: {}", config_path);
      return false;
    }

    const nlohmann::json config = nlohmann::json::parse(config_file);
    if (!config.contains("scheduler") || !config["scheduler"].is_object()) {
      LOG_ERROR("Server config does not contain a scheduler object");
      return false;
    }

    // 调度配置使用毫秒，ZooKeeper 处理器使用秒。
    // 此处完成单位适配。
    nlohmann::json scheduler_config = config["scheduler"];
    const int retry_interval_ms = cookrpc::config_validation::ReadInteger(
        scheduler_config, "retry_interval", 1000, 1, INT32_MAX);
    scheduler_config["zk_retry_interval"] =
        std::max(1, retry_interval_ms / 1000);

    auto& zk_handler = cookrpc::ZkConnHandler::GetInstance();
    if (!zk_handler.InitZkConnHandler(scheduler_config)) {
      LOG_ERROR("Failed to initialize ZooKeeper connection");
      return false;
    }
    if (!zk_handler.ReisterServicesFromConfig(registry_config)) {
      LOG_ERROR("Failed to register RPC service instances");
      return false;
    }
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("Failed to initialize service registry: {}", error.what());
    return false;
  }
}

// 等待在途任务完成后，按顺序关闭并销毁线程池。
void ShutdownThreadPool(uint32_t timeout_ms) {
  meeting_ctrl::ThreadPoolSingleton::RequestShutdown();
  if (!meeting_ctrl::ThreadPoolSingleton::WaitForTermination(
          std::chrono::milliseconds(timeout_ms))) {
    // 普通 C++ 任务不能安全强停。保留其依赖资源，显式继续等待后再退出。
    LOG_WARN(
        "Thread pool shutdown timed out; waiting for accepted tasks to finish");
    meeting_ctrl::ThreadPoolSingleton::WaitForTermination();
  }
  meeting_ctrl::ThreadPoolSingleton::Destroy();
}

}  // namespace

int main(int argc, char* argv[]) {
  // 初始化日志与进程信号模块。
  if (!Logger::GetInstance().Init("logs")) {
    std::cerr << "Failed to initialize logger" << std::endl;
    return 1;
  }

  sigset_t termination_signals;
  if (!BlockTerminationSignals(termination_signals)) {
    return 1;
  }

  // 加载服务端、线程池和服务注册配置。
  const std::string config_path = argc > 1 ? argv[1] : kDefaultConfigPath;
  auto& server_config = cookrpc::RpcServerConfig::GetInstance();
  if (!server_config.InitRpcServerConfig(config_path)) {
    LOG_FATAL("Failed to load server config");
    return 1;
  }

  const auto* thread_config = server_config.GetThreadPoolConfig();
  if (thread_config == nullptr || !meeting_ctrl::ThreadPoolSingleton::Init(
                                      BuildThreadPoolConfig(*thread_config))) {
    LOG_FATAL("Failed to initialize RPC worker thread pool");
    return 1;
  }

  // 注册业务服务，供服务管理模块统一查找与分发。
  if (!cookrpc::ServiceManager::GetInstance().RegisterService(
          std::make_shared<cookrpc::RpcService>())) {
    LOG_FATAL("Failed to register RpcService");
    meeting_ctrl::ThreadPoolSingleton::Destroy();
    return 1;
  }

  // 创建监听套接字并接入现有事件循环。
  const std::shared_ptr<cookrpc::CreateSocket> server_socket =
      cookrpc::CreateSocket::Create(server_config);
  if (server_socket == nullptr) {
    LOG_FATAL("Failed to create RPC server socket");
    meeting_ctrl::ThreadPoolSingleton::Destroy();
    return 1;
  }

  // 启动时固定传输模式与进程总限流配额，后续连接共享同一份配置。
  cookrpc::MessageCycle message_cycle(
      &cookrpc::ConnectionManager::GetInstance(),
      server_config.GetMaxConnections(), server_config.GetTransportOptions(),
      server_config.GetRateLimitOptions());
  if (!message_cycle.AddListenFd(server_socket->GetFd())) {
    LOG_FATAL("Failed to add server socket to the message cycle");
    meeting_ctrl::ThreadPoolSingleton::Destroy();
    return 1;
  }

  // 监听就绪后再发布服务。
  // 防止客户端发现尚未可用的实例。
  if (!InitializeServiceRegistry(config_path,
                                 server_config.GetServiceRegistryConfig())) {
    LOG_FATAL("Failed to publish RPC service");
    meeting_ctrl::ThreadPoolSingleton::Destroy();
    return 1;
  }

  LOG_INFO("RPC server started on {}:{}", server_config.GetServersIp(),
           server_config.GetServersPort());

  // sigwait 在线程上下文中处理退出信号。
  // 此时可以安全调用事件循环停止接口。
  std::atomic<bool> event_loop_finished{false};
  std::thread signal_thread([&] {
    int received_signal = 0;
    if (sigwait(&termination_signals, &received_signal) == 0) {
      LOG_INFO("Received signal {}, stopping RPC server", received_signal);
      // Stop 可能先于 Loop 启动。
      // 因此持续通知直至事件循环真正退出。
      while (!event_loop_finished.load(std::memory_order_acquire)) {
        message_cycle.Stop();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
  });

  message_cycle.Loop();
  event_loop_finished.store(true, std::memory_order_release);

  // 事件循环异常结束时唤醒信号线程。
  // 确保主进程可以完成资源回收。
  pthread_kill(signal_thread.native_handle(), SIGTERM);
  signal_thread.join();

  ShutdownThreadPool(server_config.GetTimeout());
  cookrpc::ZkConnHandler::GetInstance().CleanUp();
  LOG_INFO("RPC server stopped");
  return 0;
}
