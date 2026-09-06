#include "thread_pool_config.h"

#include <exception>
#include <nlohmann/json_fwd.hpp>

#include "log_manager.h"

namespace cookrpc {

bool ThreadPoolConfig::InitThreadPoolConfig(const nlohmann::json& config) {
  try {
    // 使用 value() 方法提供的默认值
    SetMaxthreads(config.value("max_threads", 10));
    SetQueueSize(config.value("queue_size", 10000));
    SetKeepAliveTime(config.value("keep_alive_time", 60));
    SetCoreThreads(config.value("core_threads", 8));

    // 参数验证
    if (core_threads_ > max_threads_) {
      LOG_ERROR("Core threads ({}) cannot exceed max threads ({})",
                core_threads_, max_threads_);
      return false;
    }

    if (max_threads_ == 0) {
      LOG_ERROR("Max threads cannot be zero");
      return false;
    }

    LOG_INFO(
        "ThreadPool config loaded: max={}, queue={}, keepalive={}s, core={}",
        max_threads_, queue_size_, keep_alive_time_, core_threads_);

    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Failed to parse thread pool config: {}", e.what());
    return false;
  }
}
}  // namespace cookrpc
