#ifndef RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ROUND_H_
#define RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ROUND_H_

#include <atomic>
#include <cstddef>
#include <string>
#include <vector>

#include "load_balancer.h"

namespace cookrpc {

// 轮询负载均衡器
class RoundRobinLoadBalancer : public LoadBalancer {
 public:
  RoundRobinLoadBalancer() : current_index_(0) {}

  std::string Select(const std::vector<std::string>& instances) override {
    // 空列表检查
    if (instances.empty()) {
      return  "";
    }

    // 获取当前索引并递增
    size_t index = current_index_.fetch_add(1) % instances.size();

    // 返回对应实例
    return instances[index];
  }
 private:
  std::atomic<size_t> current_index_; // 原子计数器
};
} // namespace cookrpc

#endif // RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_ROUND_H_
