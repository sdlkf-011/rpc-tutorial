#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <limits>
#include <nlohmann/json.hpp>

#include "rpc_server_config.h"

namespace cookrpc {
namespace {
class ServerConfigTest : public testing::Test {
 protected:
  RpcServerConfig& config = RpcServerConfig::GetInstance();
  const std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      ("rpc-server-config-" + std::to_string(getpid()) + ".json");
  const std::filesystem::path registry_path = path.string() + ".registry";
  nlohmann::json Good() const {
    return {{"servers_ip", "127.0.0.1"},
            {"servers_port", 8989},
            {"max_connections", 32},
            {"timeout", 5000},
            {"thread_pool",
             {{"core_threads", 1},
              {"max_threads", 2},
              {"queue_size", 10},
              {"keep_alive_time", 1}}}};
  }
  bool Load(const nlohmann::json& json) {
    std::ofstream(path) << json;
    return config.InitRpcServerConfig(path.string());
  }
  void SetUp() override { ASSERT_TRUE(Load(Good())); }
  void TearDown() override {
    std::filesystem::remove(path);
    std::filesystem::remove(registry_path);
  }
};

TEST_F(ServerConfigTest, RejectsNarrowingBooleanFractionalAndNegativeNumbers) {
  for (const auto& bad :
       {nlohmann::json(-1), nlohmann::json(65537), nlohmann::json(true),
        nlohmann::json(8989.5),
        nlohmann::json(std::numeric_limits<uint64_t>::max())}) {
    auto candidate = Good();
    candidate["servers_port"] = bad;
    EXPECT_FALSE(Load(candidate)) << bad;
    EXPECT_EQ(config.GetServersPort(), 8989);
  }
  for (const auto& field : {"max_connections", "timeout"}) {
    for (const auto& bad :
         {nlohmann::json(-1), nlohmann::json(true), nlohmann::json(1.5),
          nlohmann::json(uint64_t{1} << 40)}) {
      auto candidate = Good();
      candidate[field] = bad;
      EXPECT_FALSE(Load(candidate)) << field << '=' << bad;
    }
  }
}

TEST_F(ServerConfigTest, FailedIpOrChildConfigPreservesTheWholeConfiguration) {
  const auto* pool = config.GetThreadPoolConfig();
  const auto* registry = config.GetServiceRegistryConfig();
  for (int fault = 0; fault < 3; ++fault) {
    auto candidate = Good();
    candidate["servers_port"] = 9999;
    candidate["servers_name_prefix"] = "new";
    if (fault == 0) candidate["servers_ip"] = "invalid";
    if (fault == 1) candidate["thread_pool"]["core_threads"] = 3;
    if (fault == 2) candidate["register_config"] = registry_path.string();
    EXPECT_FALSE(Load(candidate));
    EXPECT_EQ(config.GetServersPort(), 8989);
    EXPECT_EQ(config.GetServersIp(), "127.0.0.1");
    EXPECT_EQ(config.GetServersNameProfix(), "cookrpc");
    EXPECT_EQ(config.GetThreadPoolConfig(), pool);
    EXPECT_EQ(config.GetServiceRegistryConfig(), registry);
    EXPECT_EQ(pool->GetCoreThreads(), 1);
    EXPECT_EQ(pool->GetQueueSize(), 10u);
  }
}

TEST_F(ServerConfigTest, ThreadPoolRejectsInvalidNumbersWithoutPartialChanges) {
  ThreadPoolConfig pool;
  ASSERT_TRUE(pool.InitThreadPoolConfig(Good()["thread_pool"]));
  for (const auto& field :
       {"core_threads", "max_threads", "queue_size", "keep_alive_time"}) {
    for (const auto& bad :
         {nlohmann::json(-1), nlohmann::json(true), nlohmann::json(1.5),
          nlohmann::json(uint64_t{1} << 40)}) {
      auto candidate = Good()["thread_pool"];
      candidate[field] = bad;
      EXPECT_FALSE(pool.InitThreadPoolConfig(candidate)) << field << '=' << bad;
      EXPECT_EQ(pool.GetCoreThreads(), 1);
      EXPECT_EQ(pool.GetMaxThreads(), 2);
      EXPECT_EQ(pool.GetQueueSize(), 10u);
      EXPECT_EQ(pool.GetKeepAliveTime(), 1u);
    }
  }
  auto candidate = Good()["thread_pool"];
  candidate["max_threads"] = 65537;
  EXPECT_FALSE(pool.InitThreadPoolConfig(candidate));
}

TEST_F(ServerConfigTest,
       RegistryReloadReplacesNodesAndRejectsInvalidNodeAtomically) {
  ServiceRegistryConfig registry;
  const auto load = [&](const nlohmann::json& json) {
    std::ofstream(registry_path) << json;
    return registry.InitRegistryConfig(registry_path.string());
  };
  auto json = nlohmann::json{
      {"service_name", "A"},
      {"registry_nodes",
       nlohmann::json::array({{{"address", "127.0.0.1"}, {"port", 8989}}})}};
  ASSERT_TRUE(load(json));
  json["service_name"] = "B";
  json["registry_nodes"][0]["port"] = 8990;
  ASSERT_TRUE(load(json));
  ASSERT_EQ(registry.GetRegistryNodesSize(), 1u);
  EXPECT_EQ(registry.GetRegistryNodes()[0].port, 8990);
  for (const auto& bad : {nlohmann::json(-1), nlohmann::json(65537),
                          nlohmann::json(true), nlohmann::json(8990.5)}) {
    auto invalid = json;
    invalid["service_name"] = "C";
    invalid["registry_nodes"].push_back(
        {{"address", "127.0.0.2"}, {"port", bad}});
    EXPECT_FALSE(load(invalid));
    EXPECT_EQ(registry.GetServiceName(), "B");
    ASSERT_EQ(registry.GetRegistryNodesSize(), 1u);
    EXPECT_EQ(registry.GetRegistryNodes()[0].port, 8990);
  }
}
}  // namespace
}  // namespace cookrpc
