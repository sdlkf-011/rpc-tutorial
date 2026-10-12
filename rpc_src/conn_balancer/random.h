#ifndef RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_RANDOM_H_
#define RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_RANDOM_H_

#include <cstddef>
#include <mutex>
#include <random>
#include <string>
#include <vector>

#include "load_balancer.h"

namespace cookrpc {

// 随机负载均衡器
class RandomLoadBalancer : public LoadBalancer {
 public:
  RandomLoadBalancer() : rd_(), gen_(rd_()) {}

  std::string Select(const std::vector<std::string>& instances) override {
    // 空列表检查
    if (instances.empty()) {
      return "";
    }

    // 生成 [0, instances.size() - 1] 范围的随机索引
    std::lock_guard<std::mutex> lock(mutex_);
    std::uniform_int_distribution<size_t> dis(0, instances.size() - 1);
    size_t index = dis(gen_);

    // 返回随机选中的实例
    return instances[index];
  }

 private:
  std::random_device rd_;  // 真随机数生成器
  std::mt19937 gen_;       // Mersenne Twister 伪随机数生成器
  std::mutex mutex_;       // 保护随机数引擎的内部状态
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_RANDOM_H_
