#ifndef RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_WEIGHT_H_
#define RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_WEIGHT_H_

#include <atomic>
#include <cstddef>
#include <string>
#include <vector>

#include "load_balancer.h"

namespace cookrpc {

class WeightRoundRobinLoadBalancer : public LoadBalancer {
 public:
  WeightRoundRobinLoadBalancer() : current_index_(0) {}

  std::string Select(const std::vector<std::string>& instances) override {
    // 空列表检查
    if (instances.empty()) {
      return "";
    }

    // 当前所有权重均为 1；用单次原子递增保证并发调用不会丢失轮次
    const size_t index =
        (current_index_.fetch_add(1, std::memory_order_relaxed) + 1) %
        instances.size();
    return instances[index];
  }

 private:
  std::atomic<size_t> current_index_;  // 当前索引
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_WEIGHT_H_
