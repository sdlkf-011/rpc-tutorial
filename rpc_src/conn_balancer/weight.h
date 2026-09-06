#ifndef RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_WEIGHT_H_
#define RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_WEIGHT_H_

#include <atomic>
#include <cstddef>
#include <map>
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

    // 设置所有权重为 1
    std::vector<int> weights(instances.size(), 1);

    // 计算总权重
    int total_weight = 0;
    for (int weight : weights) {
      total_weight += weight;
    }

    // 递增计算器并取模
    current_index_ = (current_index_ + 1) % total_weight;

    // 根据累积权重找到对应实例
    int current_weight = 0;
    for (size_t i = 0; i < instances.size(); ++i) {
      current_weight += weights[i];
      if (current_index_ < current_weight) {
        return instances[i];
      }
    }

    // 降级: 返回第一个实例
    return instances[0];
  }

 private:
  std::atomic<int> current_index_;      // 当前索引
  std::map<std::string, int> weights_;  // 实例权重映射

  // 设置实例权重
  void SetWeight(const std::string& instance, int weight) {
    weights_[instance] = weight;
  }
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_WEIGHT_H_
