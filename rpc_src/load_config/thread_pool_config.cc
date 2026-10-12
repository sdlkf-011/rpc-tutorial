#include "thread_pool_config.h"

#include "config_validation.h"
#include "log_manager.h"

namespace cookrpc {
bool ThreadPoolConfig::InitThreadPoolConfig(const nlohmann::json& config) {
  try {
    using config_validation::ReadInteger;
    const auto max_threads =
        ReadInteger(config, "max_threads", 10, 1, UINT16_MAX);
    const auto core_threads =
        ReadInteger(config, "core_threads", 8, 1, UINT16_MAX);
    const auto queue_size =
        ReadInteger(config, "queue_size", 10000, 0, UINT32_MAX);
    const auto keep_alive =
        ReadInteger(config, "keep_alive_time", 60, 1, UINT32_MAX);
    config_validation::Require(core_threads <= max_threads,
                               "Core threads cannot exceed max threads");
    max_threads_ = static_cast<uint16_t>(max_threads);
    core_threads_ = static_cast<uint16_t>(core_threads);
    queue_size_ = static_cast<uint32_t>(queue_size);
    keep_alive_time_ = static_cast<uint32_t>(keep_alive);
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("Failed to load thread pool configuration: {}", error.what());
    return false;
  }
}
}  // namespace cookrpc
