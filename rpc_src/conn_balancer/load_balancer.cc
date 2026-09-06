#include "load_balancer.h"

#include <cmath>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "log_manager.h"
#include "random.h"
#include "round.h"
#include "weight.h"

namespace cookrpc {

// 静态成员初始化
std::mutex LoadBalancer::mutex_;
std::shared_ptr<LoadBalancer> LoadBalancer::instance_ = nullptr;

namespace {

// 负载均衡器工厂
struct BalancerFactory {
  static std::shared_ptr<LoadBalancer> CreateRandom() {
    return std::make_shared<RandomLoadBalancer>();
  }

  static std::shared_ptr<LoadBalancer> CreateRoundRobin() {
    return std::make_shared<RoundRobinLoadBalancer>();
  }

  static std::shared_ptr<LoadBalancer> CreateWeighted() {
    return std::make_shared<WeightRoundRobinLoadBalancer>();
  }

  // 获取工厂映射表
  static const std::unordered_map<
      std::string, std::function<std::shared_ptr<LoadBalancer>()>>&
  GetMap() {
    static const std::unordered_map<
        std::string, std::function<std::shared_ptr<LoadBalancer>()>>
        map = {{"random", CreateRandom},
               {"round", CreateRoundRobin},
               {"weight", CreateWeighted}};
    return map;
  }
};
}  // namespace

std::shared_ptr<LoadBalancer> LoadBalancer::GetInstance() {
  if (!instance_) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!instance_) {
      // 默认使用随机负载均衡器
      instance_ = BalancerFactory::CreateRandom();
    }
  }
  return instance_;
}

bool LoadBalancer::InitBalancer(const std::string& type) {
  std::lock_guard<std::mutex> lock(mutex_);

  // 从工厂映射列表中查找对应的创建函数
  const auto& balancer_map = BalancerFactory::GetMap();
  auto it = balancer_map.find(type);

  if (it != balancer_map.end()) {
    try {
      // 调用工厂函数创建实例
      instance_ = it->second();
      LOG_INFO("Initialized {} load balancer", type);
      return true;
    } catch (std::exception& e) {
      LOG_ERROR("Failed to initialize {} load balancer: {}", type, e.what());

      // 降级为随机负载均衡器
      instance_ = BalancerFactory::CreateRandom();
      return false;
    }
  }

  // 未知类型，降级为随机负载均衡器
  LOG_WARN("Unknown load balancer type: {}, falling back to random", type);
  instance_ = BalancerFactory::CreateRandom();
  return false;
}

std::string LoadBalancer::SelectServer(
    const std::vector<std::string>& servers) {
  // 确保初始化
  if (!instance_) {
    LOG_WARN("Load balancer not initialized, using default random balancer");
    std::lock_guard<std::mutex> lock(mutex_);
    if (!instance_) {
      instance_ = std::make_shared<RandomLoadBalancer>();
    }
  }

  // 空列表检查
  if (servers.empty()) {
    LOG_ERROR("No instances available for load balancing");
    return "";
  }

  // 调用具体策略的 Select 方法
  try {
    return instance_->Select(servers);
  } catch (std::exception& e) {
    LOG_ERROR("Error in load balancer select: {}", e.what());
    return servers[0];
  }
}
}  // namespace cookrpc
