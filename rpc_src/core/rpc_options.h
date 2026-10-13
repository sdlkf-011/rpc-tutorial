#ifndef RPC_TUTORIAL_RPC_SRC_CORE_RPC_OPTIONS_H_
#define RPC_TUTORIAL_RPC_SRC_CORE_RPC_OPTIONS_H_

#include <cstddef>
#include <nlohmann/json.hpp>

namespace cookrpc {
// 两端必须使用相同的压缩、加密模式；阈值按变换前的 body 字节数计算。
struct TransportOptions {
  bool compression = false;
  bool encryption = false;
  size_t compression_threshold = 1024;  // 达到阈值才尝试压缩，0 表示总是尝试。
  static TransportOptions Parse(const nlohmann::json& config);
};

// 服务端进程共享一份令牌桶配额，不按连接或业务方法分别计数。
struct RateLimitOptions {
  int requests_per_second = 0;  // 每秒补充的令牌数，0 表示关闭限流。
  int burst = 0;  // 桶容量及启动时的令牌数，限流启用时必须为正数。
  static RateLimitOptions Parse(const nlohmann::json& config);
};

struct CircuitBreakerOptions {
  int failure_threshold = 5;  // 连续故障达到此值后熔断，0 表示关闭熔断。
  int reset_timeout_ms = 5000;  // 熔断冷却时长，单位为毫秒。
  static CircuitBreakerOptions Parse(const nlohmann::json& config);
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CORE_RPC_OPTIONS_H_
