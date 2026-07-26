#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "conn_balancer/load_balancer.h"
#include "conn_balancer/random.h"
#include "conn_balancer/round.h"
#include "conn_balancer/weight.h"

namespace cookrpc {
namespace {

const std::vector<std::string> kInstances = {
    "10.0.0.1:8000",
    "10.0.0.2:8000",
    "10.0.0.3:8000",
};

TEST(RoundRobinLoadBalancerTest, ReturnsEmptyStringForEmptyInstances) {
  RoundRobinLoadBalancer balancer;

  EXPECT_TRUE(balancer.Select({}).empty());
}

TEST(RoundRobinLoadBalancerTest, SelectsInstancesInOrderAndWrapsAround) {
  RoundRobinLoadBalancer balancer;

  EXPECT_EQ(balancer.Select(kInstances), kInstances[0]);
  EXPECT_EQ(balancer.Select(kInstances), kInstances[1]);
  EXPECT_EQ(balancer.Select(kInstances), kInstances[2]);
  EXPECT_EQ(balancer.Select(kInstances), kInstances[0]);
}

TEST(RandomLoadBalancerTest, ReturnsEmptyStringForEmptyInstances) {
  RandomLoadBalancer balancer;

  EXPECT_TRUE(balancer.Select({}).empty());
}

TEST(RandomLoadBalancerTest, AlwaysSelectsAnExistingInstance) {
  RandomLoadBalancer balancer;

  // 随机算法不应断言具体顺序，只验证每次结果都来自候选实例集合。
  for (int i = 0; i < 256; ++i) {
    const std::string selected = balancer.Select(kInstances);
    EXPECT_NE(std::find(kInstances.begin(), kInstances.end(), selected),
              kInstances.end());
  }
}

TEST(RandomLoadBalancerTest, AlwaysReturnsTheOnlyInstance) {
  RandomLoadBalancer balancer;
  const std::vector<std::string> single_instance = {"10.0.0.1:8000"};

  EXPECT_EQ(balancer.Select(single_instance), single_instance[0]);
}

TEST(WeightRoundRobinLoadBalancerTest, ReturnsEmptyStringForEmptyInstances) {
  WeightRoundRobinLoadBalancer balancer;

  EXPECT_TRUE(balancer.Select({}).empty());
}

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

TEST(LoadBalancerTest, FallsBackToRandomForUnknownStrategy) {
  EXPECT_FALSE(LoadBalancer::InitBalancer("unknown"));
  EXPECT_NE(std::dynamic_pointer_cast<RandomLoadBalancer>(
                LoadBalancer::GetInstance()),
            nullptr);
}

TEST(LoadBalancerTest, SelectServerDelegatesToInitializedStrategy) {
  ASSERT_TRUE(LoadBalancer::InitBalancer("round"));

  EXPECT_EQ(LoadBalancer::SelectServer(kInstances), kInstances[0]);
  EXPECT_EQ(LoadBalancer::SelectServer(kInstances), kInstances[1]);
  EXPECT_EQ(LoadBalancer::SelectServer(kInstances), kInstances[2]);
  EXPECT_TRUE(LoadBalancer::SelectServer({}).empty());
}

TEST(LoadBalancerTest, GetInstanceReturnsTheSameSharedInstance) {
  ASSERT_TRUE(LoadBalancer::InitBalancer("random"));

  const std::shared_ptr<LoadBalancer> first = LoadBalancer::GetInstance();
  const std::shared_ptr<LoadBalancer> second = LoadBalancer::GetInstance();

  EXPECT_EQ(first, second);
}

}  // namespace
}  // namespace cookrpc
