#include "zk_conn_handler.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <vector>

#include "load_balancer.h"
#include "rpc_client.h"

// 通过链接器 --wrap 替换 C API，测试真实发现器和客户端代码，
// 无需启动 ZooKeeper；可精确控制建连、查询与 watcher 的交错。
namespace {
using namespace std::chrono_literals;

struct FakeSession {
  std::string endpoint;
  watcher_fn watcher;
  void* context;
  bool closed = false;
};

std::mutex fake_mutex;
std::condition_variable fake_cv;
std::vector<std::unique_ptr<FakeSession>> sessions;
std::vector<std::string> queried_endpoints;
bool block_query = false;
bool query_entered = false;
bool release_query = false;
int queries_after_close = 0;
int child_lists = 0;

zhandle_t* Handle(FakeSession* session) {
  return reinterpret_cast<zhandle_t*>(session);
}

FakeSession* Session(zhandle_t* handle) {
  return reinterpret_cast<FakeSession*>(handle);
}

void Emit(FakeSession* session, int state) {
  session->watcher(Handle(session), ZOO_SESSION_EVENT, state, nullptr,
                   session->context);
}

FakeSession* FindSession(const std::string& endpoint) {
  std::lock_guard<std::mutex> lock(fake_mutex);
  for (auto it = sessions.rbegin(); it != sessions.rend(); ++it) {
    if ((*it)->endpoint == endpoint) {
      return it->get();
    }
  }
  return nullptr;
}

bool IsClosed(FakeSession* session) {
  std::lock_guard<std::mutex> lock(fake_mutex);
  return session->closed;
}

bool WaitForQuery() {
  std::unique_lock<std::mutex> lock(fake_mutex);
  return fake_cv.wait_for(lock, 2s, [] { return query_entered; });
}

void ReleaseQuery() {
  std::lock_guard<std::mutex> lock(fake_mutex);
  release_query = true;
  fake_cv.notify_all();
}

nlohmann::json Config(const std::string& host) {
  return {{"zk_host", host},
          {"zk_port", 2181},
          {"zk_namespace", "/alpha"},
          {"zk_retry_interval", 1},
          {"retry_times", 0}};
}

class DiscoveryTest : public testing::Test {
 protected:
  void SetUp() override {
    std::lock_guard<std::mutex> lock(fake_mutex);
    sessions.clear();
    queried_endpoints.clear();
    block_query = query_entered = release_query = false;
    queries_after_close = child_lists = 0;
  }

  void TearDown() override {
    std::lock_guard<std::mutex> lock(fake_mutex);
    for (const auto& session : sessions) {
      EXPECT_TRUE(session->closed);
    }
    EXPECT_EQ(queries_after_close, 0);
    EXPECT_EQ(child_lists, 0);
  }
};
}  // namespace

extern "C" {
zhandle_t* __wrap_zookeeper_init(const char* host, watcher_fn watcher, int,
                                 const clientid_t*, void* context, int) {
  const std::string endpoint(host);
  if (endpoint == "init-failure:2181") {
    return nullptr;
  }
  auto session = std::make_unique<FakeSession>();
  session->endpoint = endpoint;
  session->watcher = watcher;
  session->context = context;
  FakeSession* raw = session.get();
  {
    std::lock_guard<std::mutex> lock(fake_mutex);
    sessions.push_back(std::move(session));
    fake_cv.notify_all();
  }
  if (endpoint == "auth-failure:2181") {
    Emit(raw, ZOO_AUTH_FAILED_STATE);
  } else if (endpoint != "pending:2181" && endpoint != "timeout:2181") {
    // 覆盖 init 返回句柄之前就触发 watcher 的情况。
    Emit(raw, ZOO_CONNECTED_STATE);
  }
  return Handle(raw);
}

int __wrap_zookeeper_close(zhandle_t* handle) {
  auto* session = Session(handle);
  // 模拟 close 等待后台 watcher 完成：持有 watcher 锁关闭会死锁。
  std::thread callback([session] { Emit(session, ZOO_EXPIRED_SESSION_STATE); });
  callback.join();
  std::lock_guard<std::mutex> lock(fake_mutex);
  EXPECT_FALSE(session->closed);
  session->closed = true;
  return ZOK;
}

int __wrap_zoo_get_children(zhandle_t* handle, const char* path, int,
                            String_vector* children) {
  auto* session = Session(handle);
  std::unique_lock<std::mutex> lock(fake_mutex);
  if (session->closed) {
    ++queries_after_close;
    return ZINVALIDSTATE;
  }
  queried_endpoints.push_back(session->endpoint);
  if (block_query && !query_entered) {
    query_entered = true;
    fake_cv.notify_all();
    fake_cv.wait(lock, [] { return release_query; });
  }
  if (std::string(path) == "/missing") {
    return ZNONODE;
  }
  children->count = 1;
  children->data = new char*[1];
  children->data[0] = new char[5];
  std::memcpy(children->data[0], "node", 5);
  ++child_lists;
  return ZOK;
}

int __wrap_deallocate_String_vector(String_vector* children) {
  std::lock_guard<std::mutex> lock(fake_mutex);
  for (int i = 0; i < children->count; ++i) {
    delete[] children->data[i];
  }
  if (children->data) {
    --child_lists;
  }
  delete[] children->data;
  children->data = nullptr;
  children->count = 0;
  return ZOK;
}

int __wrap_zoo_get(zhandle_t* handle, const char* path, int, char* buffer,
                   int* length, Stat*) {
  auto* session = Session(handle);
  std::lock_guard<std::mutex> lock(fake_mutex);
  if (session->closed) {
    ++queries_after_close;
    return ZINVALIDSTATE;
  }
  const std::string data = session->endpoint + std::string(path);
  if (static_cast<int>(data.size()) > *length) {
    return ZMARSHALLINGERROR;
  }
  std::memcpy(buffer, data.data(), data.size());
  *length = static_cast<int>(data.size());
  return ZOK;
}

int __wrap_zoo_exists(zhandle_t*, const char*, int, Stat*) { return ZOK; }
int __wrap_zoo_create(zhandle_t*, const char*, const char*, int,
                      const ACL_vector*, int, char*, int) {
  return ZOK;
}
}  // extern "C"

namespace {
TEST_F(DiscoveryTest, SwitchesEndpointAndReusesSessionForSameEndpoint) {
  cookrpc::ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  EXPECT_EQ(handler.GetServer("/alpha"), "A:2181/alpha/node");
  auto* old_session = FindSession("A:2181");
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  EXPECT_EQ(FindSession("A:2181"), old_session);
  ASSERT_TRUE(handler.InitZkConnHandler(Config("B")));
  EXPECT_TRUE(IsClosed(old_session));
  EXPECT_EQ(handler.GetServer("/alpha"), "B:2181/alpha/node");
}

TEST_F(DiscoveryTest, InvalidOrFailedConfigurationPreservesSessionAndRegistry) {
  cookrpc::ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  const auto registry = handler.GetOrCreateServiceRegistry();
  ASSERT_NE(registry, nullptr);
  auto* old_session = FindSession("A:2181");
  for (const auto& bad :
       {nlohmann::json{{"zk_host", "B"}, {"zk_port", "invalid"}},
        nlohmann::json{{"zk_host", "B"}, {"zk_port", 0}},
        nlohmann::json{{"zk_host", "B"}, {"zk_namespace", "relative"}},
        nlohmann::json{{"zk_host", "B"}, {"zk_retry_interval", -1}},
        Config("init-failure"), Config("auth-failure"), Config("timeout")}) {
    EXPECT_FALSE(handler.InitZkConnHandler(bad));
    EXPECT_EQ(handler.GetServer("/alpha"), "A:2181/alpha/node");
    EXPECT_EQ(handler.GetServiceRegistry(), registry);
    EXPECT_FALSE(IsClosed(old_session));
  }
  handler.CleanUp();
  // 清理保留最后一次提交的配置，惰性重连仍必须使用 A。
  EXPECT_EQ(handler.GetServer("/alpha"), "A:2181/alpha/node");
}

TEST_F(DiscoveryTest, NewConnectionIsCommittedOnlyAfterConnected) {
  cookrpc::ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  auto switch_result = std::async(std::launch::async, [&] {
    return handler.InitZkConnHandler(Config("pending"));
  });
  bool created;
  {
    std::unique_lock<std::mutex> lock(fake_mutex);
    created = fake_cv.wait_for(lock, 2s, [] { return sessions.size() == 2; });
  }
  EXPECT_TRUE(created);
  EXPECT_EQ(handler.GetServer("/alpha"), "A:2181/alpha/node");
  if (auto* pending = FindSession("pending:2181")) {
    Emit(pending, ZOO_CONNECTED_STATE);
  }
  ASSERT_TRUE(switch_result.get());
  EXPECT_EQ(handler.GetServer("/alpha"), "pending:2181/alpha/node");
}

TEST_F(DiscoveryTest, OldQueryOwnsItsHandleAndOldWatcherCannotChangeNewState) {
  cookrpc::ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  auto* old_session = FindSession("A:2181");
  {
    std::lock_guard<std::mutex> lock(fake_mutex);
    block_query = true;
  }
  auto query = std::async(std::launch::async,
                          [&] { return handler.GetServer("/alpha"); });
  const bool entered = WaitForQuery();
  EXPECT_TRUE(entered);
  EXPECT_TRUE(handler.InitZkConnHandler(Config("B")));
  auto* new_session = FindSession("B:2181");
  EXPECT_FALSE(IsClosed(old_session));
  Emit(old_session, ZOO_EXPIRED_SESSION_STATE);
  EXPECT_TRUE(handler.EnsureZkConnection());
  EXPECT_EQ(FindSession("B:2181"), new_session);
  EXPECT_EQ(handler.GetServer("/beta"), "B:2181/beta/node");
  ReleaseQuery();
  EXPECT_EQ(query.get(), "A:2181/alpha/node");
  EXPECT_TRUE(IsClosed(old_session));
}

TEST_F(DiscoveryTest, CleanupDefersCloseUntilQueriesReleaseTheirSnapshots) {
  cookrpc::ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  auto* old_session = FindSession("A:2181");
  {
    std::lock_guard<std::mutex> lock(fake_mutex);
    block_query = true;
  }
  auto query = std::async(std::launch::async,
                          [&] { return handler.GetServer("/alpha"); });
  EXPECT_TRUE(WaitForQuery());
  handler.CleanUp();
  EXPECT_FALSE(IsClosed(old_session));
  ReleaseQuery();
  EXPECT_EQ(query.get(), "A:2181/alpha/node");
  EXPECT_TRUE(IsClosed(old_session));
}

TEST_F(DiscoveryTest, ConcurrentNamespacesAndStrategyChangesNeverMixAddresses) {
  cookrpc::ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  std::atomic<bool> start{false};
  std::atomic<int> invalid{0};
  std::vector<std::thread> workers;
  for (int worker = 0; worker < 8; ++worker) {
    workers.emplace_back([&, worker] {
      const std::string path = worker % 2 ? "/alpha" : "/beta";
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < 1000; ++i) {
        if (handler.GetServer(path) != "A:2181" + path + "/node") {
          ++invalid;
        }
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(
        cookrpc::LoadBalancer::InitBalancer(i % 2 ? "round" : "random"));
  }
  for (auto& worker : workers) {
    worker.join();
  }
  EXPECT_EQ(invalid.load(), 0);
  EXPECT_TRUE(handler.GetServer("/missing").empty());
}

TEST_F(DiscoveryTest, IndependentDiscoveryOwnersKeepSeparateClusters) {
  cookrpc::ZkConnHandler first;
  cookrpc::ZkConnHandler second;
  ASSERT_TRUE(first.InitZkConnHandler(Config("A")));
  ASSERT_TRUE(second.InitZkConnHandler(Config("B")));
  EXPECT_EQ(first.GetServer("/alpha"), "A:2181/alpha/node");
  EXPECT_EQ(second.GetServer("/alpha"), "B:2181/alpha/node");
  second.CleanUp();
  EXPECT_EQ(first.GetServer("/alpha"), "A:2181/alpha/node");
}

TEST_F(DiscoveryTest, ExpiredSessionReconnectsUsingCommittedEndpoint) {
  cookrpc::ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  auto* expired = FindSession("A:2181");
  Emit(expired, ZOO_EXPIRED_SESSION_STATE);
  EXPECT_EQ(handler.GetServer("/alpha"), "A:2181/alpha/node");
  EXPECT_NE(FindSession("A:2181"), expired);
  EXPECT_TRUE(IsClosed(expired));
}

TEST_F(DiscoveryTest, RegistryFollowsCommittedEndpointWithOwnedSnapshots) {
  cookrpc::ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler(Config("A")));
  auto old_registry = handler.GetOrCreateServiceRegistry();
  ASSERT_NE(old_registry, nullptr);
  auto* old_registry_session = FindSession("A:2181");
  ASSERT_TRUE(handler.InitZkConnHandler(Config("B")));
  EXPECT_FALSE(handler.HasServiceRegistry());
  const auto new_registry = handler.GetOrCreateServiceRegistry();
  ASSERT_NE(new_registry, nullptr);
  EXPECT_NE(old_registry, new_registry);
  EXPECT_EQ(FindSession("B:2181")->endpoint, "B:2181");
  EXPECT_FALSE(IsClosed(old_registry_session));
  old_registry.reset();
  EXPECT_TRUE(IsClosed(old_registry_session));
}

TEST_F(DiscoveryTest,
       RpcClientsOwnDiscoveryAndFailedReloadPreservesOldCluster) {
  const auto path =
      std::filesystem::temp_directory_path() /
      ("rpc-discovery-test-" +
       std::to_string(
           std::chrono::steady_clock::now().time_since_epoch().count()) +
       ".json");
  struct RemoveFile {
    std::filesystem::path path;
    ~RemoveFile() { std::filesystem::remove(path); }
  } remove{path};
  const auto write = [&](const nlohmann::json& config) {
    std::ofstream file(path);
    file << config;
  };
  write(Config("A"));
  cookrpc::RpcClient first(path.string());
  write(Config("B"));
  cookrpc::RpcClient second(path.string());
  // Fake 返回不可解析的 TCP 地址，Connect 会停在发现结果处；
  // 检查实际查询的句柄，无需建立网络连接。
  EXPECT_FALSE(first.Connect());
  EXPECT_FALSE(second.Connect());
  {
    std::lock_guard<std::mutex> lock(fake_mutex);
    ASSERT_EQ(queried_endpoints.size(), 2u);
    EXPECT_EQ(queried_endpoints[0], "A:2181");
    EXPECT_EQ(queried_endpoints[1], "B:2181");
  }
  auto failed = Config("auth-failure");
  failed["load_balance_strategy"] = "round";
  const auto strategy = cookrpc::LoadBalancer::GetInstance();
  write(failed);
  EXPECT_FALSE(first.LoadConfig(path.string()));
  EXPECT_EQ(cookrpc::LoadBalancer::GetInstance(), strategy);
  EXPECT_FALSE(first.Connect());
  {
    std::lock_guard<std::mutex> lock(fake_mutex);
    EXPECT_EQ(queried_endpoints.back(), "A:2181");
  }
  write(Config("C"));
  ASSERT_TRUE(first.LoadConfig(path.string()));
  EXPECT_FALSE(first.Connect());
  EXPECT_FALSE(second.Connect());
  {
    std::lock_guard<std::mutex> lock(fake_mutex);
    EXPECT_EQ(queried_endpoints[queried_endpoints.size() - 2], "C:2181");
    EXPECT_EQ(queried_endpoints.back(), "B:2181");
  }
}
}  // namespace

TEST_F(DiscoveryTest,
       RpcClientRejectsInvalidNumericTypesBeforeSwitchingCluster) {
  const auto path =
      std::filesystem::temp_directory_path() /
      ("rpc-client-numeric-" + std::to_string(getpid()) + ".json");
  struct Cleanup {
    std::filesystem::path path;
    ~Cleanup() { std::filesystem::remove(path); }
  } cleanup{path};
  std::ofstream(path) << Config("A");
  cookrpc::RpcClient client(path.string());
  for (const auto* field : {"zk_port", "server_port", "timeout_ms",
                            "retry_times", "zk_retry_interval"}) {
    for (const auto& bad :
         {nlohmann::json(true), nlohmann::json(1.5),
          nlohmann::json(std::numeric_limits<uint64_t>::max())}) {
      auto candidate = Config("B");
      candidate[field] = bad;
      std::ofstream(path) << candidate;
      EXPECT_FALSE(client.LoadConfig(path.string())) << field << '=' << bad;
    }
  }
}
