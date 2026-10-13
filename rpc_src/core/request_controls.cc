#include "request_controls.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>

namespace cookrpc {
RequestLimiter::RequestLimiter(RateLimitOptions options, Clock::time_point now)
    : options_(options), tokens_(options.burst), updated_(now) {
  if (options.requests_per_second < 0 || options.burst < 0 ||
      (options.requests_per_second && !options.burst))
    throw std::invalid_argument("Invalid rate limiter options");
}

bool RequestLimiter::TryAcquire(Clock::time_point now) {
  if (!options_.requests_per_second) return true;
  std::lock_guard<std::mutex> lock(mutex_);
  if (now > updated_) {
    // 保留不足一个令牌的时间收益，同时避免长时间空闲积攒无限配额。
    const double elapsed =
        std::chrono::duration<double>(now - updated_).count();
    tokens_ = std::min(double(options_.burst),
                       tokens_ + elapsed * options_.requests_per_second);
    updated_ = now;
  }
  if (tokens_ < 1) return false;
  tokens_ -= 1;
  return true;
}

CircuitBreaker::CircuitBreaker(CircuitBreakerOptions options)
    : options_(options) {
  if (options.failure_threshold < 0 || options.reset_timeout_ms <= 0)
    throw std::invalid_argument("Invalid circuit breaker options");
}

bool CircuitBreaker::Available(const std::string& endpoint,
                               Clock::time_point now) const {
  if (!options_.failure_threshold) return true;
  const auto found = entries_.find(endpoint);
  return found == entries_.end() || !found->second.open ||
         (!found->second.probing &&
          now - found->second.opened_at >=
              std::chrono::milliseconds(options_.reset_timeout_ms));
}

bool CircuitBreaker::TryAcquire(const std::string& endpoint,
                                Clock::time_point now) {
  if (!Available(endpoint, now)) return false;
  if (!options_.failure_threshold) return true;
  auto& entry = entries_[endpoint];
  // 打开状态只有冷却结束且没有其他探测时才能通过 Available 检查。
  if (entry.open) entry.probing = true;
  return true;
}

void CircuitBreaker::Finish(const std::string& endpoint, Outcome outcome,
                            Clock::time_point now) {
  if (!options_.failure_threshold) return;
  auto& entry = entries_[endpoint];
  if (outcome == Outcome::kHealthy) {
    // 收到有效业务错误或限流响应也说明实例可通信，清除连续故障计数。
    entry = Entry{};
  } else if (outcome == Outcome::kFailure) {
    if (entry.open || ++entry.failures >= options_.failure_threshold) {
      // 半开探测失败立即重新熔断，并从本次失败重新计算冷却期限。
      entry.open = true;
      entry.opened_at = now;
    }
  }
  // 本地失败的中性结果不改变故障历史，但必须释放探测名额。
  entry.probing = false;
}

void CircuitBreaker::ForgetMissing(const std::vector<std::string>& endpoints) {
  const std::unordered_set<std::string> present(endpoints.begin(),
                                                endpoints.end());
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (!present.count(it->first))
      it = entries_.erase(it);
    else
      ++it;
  }
}
}  // namespace cookrpc
