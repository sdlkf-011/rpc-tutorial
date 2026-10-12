#include "rpc_client_config.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace cookrpc {
namespace {
class ClientConfigTest : public testing::Test {
 protected:
  RpcClientConfig& config = RpcClientConfig::GetInstance();
  std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      ("rpc-client-config-" + std::to_string(getpid()) + ".json");
  void SetUp() override {
    config.SetZkHost("cluster-A");
    config.SetZkNamespace("/old");
    config.SetZkPort(2181);
    config.SetTimeout(4000);
    config.SetRetryTimes(2);
    config.SetServerPort(8000);
  }
  void TearDown() override { std::filesystem::remove(path); }
  bool Load(const nlohmann::json& value) {
    {
      std::ofstream out(path);
      out << value;
    }
    return config.InitRpcClientConfig(path.string());
  }
  void ExpectOld() {
    EXPECT_EQ(config.GetZkHost(), "cluster-A");
    EXPECT_EQ(config.GetZkNamespace(), "/old");
    EXPECT_EQ(config.GetZkPort(), 2181);
    EXPECT_EQ(config.GetTimeout(), 4000);
    EXPECT_EQ(config.GetRetryTimes(), 2);
    EXPECT_EQ(config.GetServerPort(), 8000);
  }
};

TEST_F(ClientConfigTest, AllSixSettersAreDefinedAndUpdateValues) {
  config.SetZkHost("cluster-B");
  config.SetZkNamespace("/new");
  config.SetZkPort(65535);
  config.SetTimeout(4500);
  config.SetRetryTimes(7);
  config.SetServerPort(65000);
  EXPECT_EQ(config.GetZkHost(), "cluster-B");
  EXPECT_EQ(config.GetZkNamespace(), "/new");
  EXPECT_EQ(config.GetZkPort(), 65535);
  EXPECT_EQ(config.GetTimeout(), 4500);
  EXPECT_EQ(config.GetRetryTimes(), 7);
  EXPECT_EQ(config.GetServerPort(), 65000);
}

TEST_F(ClientConfigTest, PortsAbove6335AndBothBoundariesAreAccepted) {
  for (int port : {1, 6336, 65000, 65535}) {
    ASSERT_TRUE(Load({{"zk_port", port}, {"server_port", port}}));
    EXPECT_EQ(config.GetZkPort(), port);
    EXPECT_EQ(config.GetServerPort(), port);
  }
}

TEST_F(ClientConfigTest, InvalidValuesAndTypesLeaveAllOldFieldsIntact) {
  for (const auto& item : std::vector<std::pair<std::string, nlohmann::json>>{
           {"zk_host", ""},
           {"zk_namespace", "relative"},
           {"zk_namespace", ""},
           {"zk_port", 0},
           {"zk_port", -1},
           {"zk_port", 65536},
           {"server_port", 0},
           {"server_port", -1},
           {"server_port", 65536},
           {"timeout_ms", 0},
           {"timeout_ms", -1},
           {"retry_times", -1},
           {"retry_times", 101},
           {"zk_port", "2181"},
           {"zk_port", 2181.5},
           {"zk_port", true},
           {"zk_port", uint64_t{4294969477}},
           {"timeout_ms", uint64_t{4294970296}}}) {
    nlohmann::json candidate{{"zk_host", "cluster-B"}, {"zk_namespace", "/new"},
                             {"zk_port", 2182},        {"server_port", 8001},
                             {"timeout_ms", 5000},     {"retry_times", 4}};
    candidate[item.first] = item.second;
    EXPECT_FALSE(Load(candidate)) << item.first << "=" << item.second;
    ExpectOld();
  }
}

TEST_F(ClientConfigTest, DefaultsAndMalformedFileAreTransactional) {
  ASSERT_TRUE(Load(nlohmann::json::object()));
  EXPECT_EQ(config.GetZkHost(), "localhost");
  EXPECT_EQ(config.GetZkPort(), 2181);
  EXPECT_EQ(config.GetZkNamespace(), "/cookrpc");
  EXPECT_EQ(config.GetServerPort(), 8989);
  SetUp();
  {
    std::ofstream out(path);
    out << "{ invalid json";
  }
  EXPECT_FALSE(config.InitRpcClientConfig(path.string()));
  ExpectOld();
  EXPECT_FALSE(Load(nlohmann::json::array()));
  ExpectOld();
  EXPECT_FALSE(config.InitRpcClientConfig(path.string() + "missing"));
  ExpectOld();
}

TEST_F(ClientConfigTest, InvalidDirectSettersThrowAndPreserveThePreviousValue) {
  EXPECT_THROW(config.SetZkHost(""), std::invalid_argument);
  EXPECT_THROW(config.SetZkNamespace("relative"), std::invalid_argument);
  EXPECT_THROW(config.SetZkPort(65536), std::invalid_argument);
  EXPECT_THROW(config.SetTimeout(0), std::invalid_argument);
  EXPECT_THROW(config.SetRetryTimes(-1), std::invalid_argument);
  EXPECT_THROW(config.SetServerPort(0), std::invalid_argument);
  ExpectOld();
}
}  // namespace
}  // namespace cookrpc
