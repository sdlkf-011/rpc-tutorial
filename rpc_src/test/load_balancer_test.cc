#include "load_balancer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "random.h"
#include "round.h"
#include "weight.h"

namespace cookrpc {
namespace {

// 负载均衡测试数据：使用三个固定节点验证选择顺序和候选范围。
const std::vector<std::string> kInstances = {
    "10.0.0.1:8000",
    "10.0.0.2:8000",
    "10.0.0.3:8000",
};

// 轮询策略测试模块：验证空集合处理、顺序选择和周期回绕。
// 验证轮询策略面对空候选集合返回空结果。
TEST(RoundRobinLoadBalancerTest, ReturnsEmptyStringForEmptyInstances) {
  RoundRobinLoadBalancer balancer;

  EXPECT_TRUE(balancer.Select({}).empty());
}

// 验证轮询按顺序选择节点，并在末尾回到第一个节点。
TEST(RoundRobinLoadBalancerTest, SelectsInstancesInOrderAndWrapsAround) {
  RoundRobinLoadBalancer balancer;

  EXPECT_EQ(balancer.Select(kInstances), kInstances[0]);
  EXPECT_EQ(balancer.Select(kInstances), kInstances[1]);
  EXPECT_EQ(balancer.Select(kInstances), kInstances[2]);
  EXPECT_EQ(balancer.Select(kInstances), kInstances[0]);
}

// 随机策略测试模块：只验证结果合法，不对不可预测的随机顺序做断言。
// 验证随机策略面对空候选集合返回空结果。
TEST(RandomLoadBalancerTest, ReturnsEmptyStringForEmptyInstances) {
  RandomLoadBalancer balancer;

  EXPECT_TRUE(balancer.Select({}).empty());
}

// 验证随机选择结果始终属于输入的候选节点集合。
TEST(RandomLoadBalancerTest, AlwaysSelectsAnExistingInstance) {
  RandomLoadBalancer balancer;

  // 随机算法不应断言具体顺序，只验证每次结果都来自候选实例集合。
  for (int i = 0; i < 256; ++i) {
    const std::string selected = balancer.Select(kInstances);
    EXPECT_NE(std::find(kInstances.begin(), kInstances.end(), selected),
              kInstances.end());
  }
}

// 验证候选节点只有一个时随机策略始终选择该节点。
TEST(RandomLoadBalancerTest, AlwaysReturnsTheOnlyInstance) {
  RandomLoadBalancer balancer;
  const std::vector<std::string> single_instance = {"10.0.0.1:8000"};

  EXPECT_EQ(balancer.Select(single_instance), single_instance[0]);
}

// 加权轮询测试模块：验证默认权重下的均匀分配行为。
// 验证加权轮询策略面对空候选集合返回空结果。
TEST(WeightRoundRobinLoadBalancerTest, ReturnsEmptyStringForEmptyInstances) {
  WeightRoundRobinLoadBalancer balancer;

  EXPECT_TRUE(balancer.Select({}).empty());
}

// 验证默认相同权重下，完整周期内节点获得均匀分配。
TEST(WeightRoundRobinLoadBalancerTest, DistributesUniformWeightsEvenly) {
  WeightRoundRobinLoadBalancer balancer;
  std::vector<std::string> selections;

  // 当前实现为所有实例设置相同权重，因此完整的两个周期应等量命中。
  for (std::size_t i = 0; i < kInstances.size() * 2; ++i) {
    selections.push_back(balancer.Select(kInstances));
  }

  for (const auto& instance : kInstances) {
    EXPECT_EQ(std::count(selections.begin(), selections.end(), instance), 2);
  }
}

// 管理器测试模块：验证策略初始化、未知策略回退、委托和单例语义。
// 验证管理器能够创建并保存每一种受支持的均衡策略。
TEST(LoadBalancerTest, CreatesEverySupportedStrategy) {
  ASSERT_TRUE(LoadBalancer::InitBalancer("random"));
  EXPECT_NE(std::dynamic_pointer_cast<RandomLoadBalancer>(
                LoadBalancer::GetInstance()),
            nullptr);

  ASSERT_TRUE(LoadBalancer::InitBalancer("round"));
  EXPECT_NE(std::dynamic_pointer_cast<RoundRobinLoadBalancer>(
                LoadBalancer::GetInstance()),
            nullptr);

  ASSERT_TRUE(LoadBalancer::InitBalancer("weight"));
  EXPECT_NE(std::dynamic_pointer_cast<WeightRoundRobinLoadBalancer>(
                LoadBalancer::GetInstance()),
            nullptr);
}

// 验证未知策略初始化失败时回退到随机策略。
TEST(LoadBalancerTest, FallsBackToRandomForUnknownStrategy) {
  EXPECT_FALSE(LoadBalancer::InitBalancer("unknown"));
  EXPECT_NE(std::dynamic_pointer_cast<RandomLoadBalancer>(
                LoadBalancer::GetInstance()),
            nullptr);
}

// 验证管理器将节点选择请求委托给当前已初始化的策略。
TEST(LoadBalancerTest, SelectServerDelegatesToInitializedStrategy) {
  ASSERT_TRUE(LoadBalancer::InitBalancer("round"));

  EXPECT_EQ(LoadBalancer::SelectServer(kInstances), kInstances[0]);
  EXPECT_EQ(LoadBalancer::SelectServer(kInstances), kInstances[1]);
  EXPECT_EQ(LoadBalancer::SelectServer(kInstances), kInstances[2]);
  EXPECT_TRUE(LoadBalancer::SelectServer({}).empty());
}

// 验证管理器单例接口返回同一个共享策略对象。
TEST(LoadBalancerTest, GetInstanceReturnsTheSameSharedInstance) {
  ASSERT_TRUE(LoadBalancer::InitBalancer("random"));

  const std::shared_ptr<LoadBalancer> first = LoadBalancer::GetInstance();
  const std::shared_ptr<LoadBalancer> second = LoadBalancer::GetInstance();

  EXPECT_EQ(first, second);
}

}  // namespace
}  // namespace cookrpc
