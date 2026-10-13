#ifndef RPC_TUTORIAL_RPC_SRC_CORE_REQUEST_CONTROLS_H_
#define RPC_TUTORIAL_RPC_SRC_CORE_REQUEST_CONTROLS_H_

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "rpc_options.h"

namespace cookrpc {
// 使用单调时钟补充令牌，桶容量约束突发量；内部锁保护并发扣减。
class RequestLimiter {
 public:
  using Clock = std::chrono::steady_clock;
  explicit RequestLimiter(RateLimitOptions options = {},
                          Clock::time_point now = Clock::now());
  bool TryAcquire(Clock::time_point now = Clock::now());

 private:
  RateLimitOptions options_;
  double tokens_;
  Clock::time_point updated_;
  std::mutex mutex_;
};

// 每个实例地址维护独立状态，调用方通过客户端会话锁串行访问。
class CircuitBreaker {
 public:
  using Clock = std::chrono::steady_clock;
  // 有效业务响应视为健康；本地参数或编码失败不反映远端健康状态。
  enum class Outcome { kHealthy, kFailure, kNeutral };
  explicit CircuitBreaker(CircuitBreakerOptions options = {});
  // 只检查可选性，不占用半开探测名额，供负载均衡筛选候选实例。
  bool Available(const std::string& endpoint,
                 Clock::time_point now = Clock::now()) const;
  // 冷却结束后占用唯一探测名额，必须通过 Finish 归还并更新状态。
  bool TryAcquire(const std::string& endpoint,
                  Clock::time_point now = Clock::now());
  void Finish(const std::string& endpoint, Outcome outcome,
              Clock::time_point now = Clock::now());
  // 按本次发现快照清理消失的实例，避免长期累积失效地址。
  void ForgetMissing(const std::vector<std::string>& endpoints);

 private:
  struct Entry {
    int failures = 0;
    bool open = false;  // 打开时拒绝请求，冷却结束后允许进入半开探测。
    bool probing = false;  // 半开时最多允许一个未完成的探测。
    Clock::time_point opened_at{};
  };
  CircuitBreakerOptions options_;
  std::unordered_map<std::string, Entry> entries_;
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CORE_REQUEST_CONTROLS_H_
