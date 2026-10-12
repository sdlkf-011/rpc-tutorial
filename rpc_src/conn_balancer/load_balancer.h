#ifndef RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_LOAD_BALANCER_H_
#define RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_LOAD_BALANCER_H_

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace cookrpc {

// 前置声明
class RandomLoadBalancer;
class RoundRobinLoadBalancer;
class WeightRoundRobinLoadBalancer;

class LoadBalancer {
 public:
  virtual ~LoadBalancer() = default;

  // 从实例列表中选择一个服务实例
  virtual std::string Select(const std::vector<std::string>& instances) = 0;

  // 获取全局单例实例
  static std::shared_ptr<LoadBalancer> GetInstance();

  // 初始化负载均衡器
  static bool InitBalancer(const std::string& type = "random");

  // 从服务器列表中选择一个服务器
  static std::string SelectServer(const std::vector<std::string>& servers);

 protected:
  // 允许子类访问构造函数
  LoadBalancer() noexcept = default;

 private:
  static std::shared_ptr<LoadBalancer> instance_;  // 单例实例
  static std::mutex mutex_;  // 保护单例的所有读写

  LoadBalancer(const LoadBalancer&) = delete;
  LoadBalancer& operator=(const LoadBalancer&) = delete;
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CONN_BALANCER_LOAD_BALANCER_H_
