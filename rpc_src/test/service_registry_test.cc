#include "service_registry.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace {
using namespace std::chrono_literals;
struct Handle {
  clientid_t id{};
  watcher_fn watcher;
  void* context;
  bool closed = false;
  bool expired = false;
  int callbacks = 0;
};

struct Watch {
  Handle* handle;
  watcher_fn fn;
  void* context;
};

struct Node {
  int64_t owner = 0;
  std::string data;
};
std::mutex mutex;
std::condition_variable cv;
std::vector<std::unique_ptr<Handle>> handles;
std::unordered_map<std::string, Node> nodes;
std::unordered_map<std::string, std::vector<Watch>> watches;
bool vanish_before_watch = false;
bool replace_during_get = false;
size_t watch_count = 0;
struct Pending {
  Handle* handle;
  std::function<void(int)> complete;
};
std::vector<Pending> pending_operations;
bool defer_create = false;
bool defer_get = false;
size_t canceled_completions = 0;
bool hold_close = false;
bool close_entered = false;
bool release_close = false;
bool reject_connections = false;

Handle* Cast(zhandle_t* handle) { return reinterpret_cast<Handle*>(handle); }
zhandle_t* Cast(Handle* handle) { return reinterpret_cast<zhandle_t*>(handle); }
void Emit(Watch watch, int type, int state) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    if (watch.handle->closed) return;
    ++watch.handle->callbacks;
  }
  watch.fn(Cast(watch.handle), type, state, nullptr, watch.context);
  {
    std::lock_guard<std::mutex> lock(mutex);
    --watch.handle->callbacks;
  }
  cv.notify_all();
}

std::vector<Watch> EraseLocked(const std::string& path) {
  nodes.erase(path);
  auto found = watches.find(path);
  if (found == watches.end()) return {};
  auto result = std::move(found->second);
  watches.erase(found);
  return result;
}

void Delete(const std::string& path) {
  std::vector<Watch> pending;
  {
    std::lock_guard<std::mutex> lock(mutex);
    pending = EraseLocked(path);
  }
  for (auto watch : pending)
    Emit(watch, ZOO_DELETED_EVENT, ZOO_CONNECTED_STATE);
}

void Expire(Handle* handle) {
  std::vector<Watch> pending;
  {
    std::lock_guard<std::mutex> lock(mutex);
    handle->expired = true;
    std::vector<std::string> paths;
    for (auto& node : nodes)
      if (node.second.owner == handle->id.client_id)
        paths.push_back(node.first);
    for (const auto& path : paths) {
      auto events = EraseLocked(path);
      pending.insert(pending.end(), events.begin(), events.end());
    }
  }
  Emit({handle, handle->watcher, handle->context}, ZOO_SESSION_EVENT,
       ZOO_EXPIRED_SESSION_STATE);
  for (auto watch : pending)
    Emit(watch, ZOO_DELETED_EVENT, ZOO_CONNECTED_STATE);
}

void Fill(Stat* stat, const Node& node) {
  if (stat) {
    *stat = {};
    stat->ephemeralOwner = node.owner;
    stat->dataLength = node.data.size();
  }
}
const std::string path = "/cookrpc/service/127.0.0.1:8000";
Node SnapshotNode() {
  std::lock_guard<std::mutex> lock(mutex);
  return nodes.at(path);
}

bool WaitForWatch(size_t count = 1) {
  std::unique_lock<std::mutex> lock(mutex);
  return cv.wait_for(lock, 2s, [&] { return watch_count >= count; });
}
}  // namespace
extern "C" {
zhandle_t* __wrap_zookeeper_init(const char*, watcher_fn watcher, int,
                                 const clientid_t*, void* context, int) {
  {
    std::lock_guard<std::mutex> lock(mutex);
    if (reject_connections) return nullptr;
  }
  auto handle = std::make_unique<Handle>();
  handle->watcher = watcher;
  handle->context = context;
  auto* raw = handle.get();
  {
    std::lock_guard<std::mutex> lock(mutex);
    raw->id.client_id = 100 + handles.size();
    handles.push_back(std::move(handle));
  }
  Emit({raw, watcher, context}, ZOO_SESSION_EVENT, ZOO_CONNECTED_STATE);
  return Cast(raw);
}

int __wrap_zookeeper_close(zhandle_t* value) {
  auto* handle = Cast(value);
  std::vector<Watch> pending;
  std::vector<Pending> canceled;
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_FALSE(handle->closed);
    if (hold_close) {
      close_entered = true;
      cv.notify_all();
      cv.wait(lock, [] { return release_close; });
    }
    handle->closed = true;
    for (auto it = pending_operations.begin();
         it != pending_operations.end();) {
      if (it->handle == handle) {
        canceled.push_back(std::move(*it));
        it = pending_operations.erase(it);
      } else {
        ++it;
      }
    }
    canceled_completions += canceled.size();
    cv.wait(lock, [&] { return handle->callbacks == 0; });
    std::vector<std::string> paths;
    for (const auto& node : nodes)
      if (node.second.owner == handle->id.client_id)
        paths.push_back(node.first);
    for (const auto& path : paths) {
      auto events = EraseLocked(path);
      pending.insert(pending.end(), events.begin(), events.end());
    }
  }
  for (auto& operation : canceled) operation.complete(ZCLOSING);
  for (auto watch : pending)
    Emit(watch, ZOO_DELETED_EVENT, ZOO_CONNECTED_STATE);
  cv.notify_all();
  return ZOK;
}
const clientid_t* __wrap_zoo_client_id(zhandle_t* handle) {
  return &Cast(handle)->id;
}

int __wrap_zoo_create(zhandle_t* value, const char* path, const char* data,
                      int length, const ACL_vector*, int flags, char*, int) {
  std::lock_guard<std::mutex> lock(mutex);
  auto* handle = Cast(value);
  if (handle->closed || handle->expired) return ZINVALIDSTATE;
  if (nodes.count(path)) return ZNODEEXISTS;
  nodes[path] = {flags & ZOO_EPHEMERAL ? handle->id.client_id : 0,
                 std::string(data, length)};
  cv.notify_all();
  return ZOK;
}

int __wrap_zoo_exists(zhandle_t*, const char* path, int, Stat* stat) {
  std::lock_guard<std::mutex> lock(mutex);
  auto found = nodes.find(path);
  if (found == nodes.end()) return ZNONODE;
  Fill(stat, found->second);
  return ZOK;
}

int __wrap_zoo_wexists(zhandle_t* value, const char* path, watcher_fn fn,
                       void* context, Stat* stat) {
  std::lock_guard<std::mutex> lock(mutex);
  ++watch_count;
  cv.notify_all();
  if (vanish_before_watch) {
    vanish_before_watch = false;
    nodes.erase(path);
  }
  watches[path].push_back({Cast(value), fn, context});
  auto found = nodes.find(path);
  if (found == nodes.end()) return ZNONODE;
  Fill(stat, found->second);
  return ZOK;
}

int __wrap_zoo_get(zhandle_t*, const char* path, int, char* buffer, int* length,
                   Stat* stat) {
  std::vector<Watch> pending;
  {
    std::lock_guard<std::mutex> lock(mutex);
    auto found = nodes.find(path);
    if (found == nodes.end()) return ZNONODE;
    if (replace_during_get) {
      replace_during_get = false;
      found->second.owner = 999;
      auto watches_found = watches.find(path);
      if (watches_found != watches.end()) {
        pending = std::move(watches_found->second);
        watches.erase(watches_found);
      }
    }
    Fill(stat, found->second);
    const int copied = std::min<int>(*length, found->second.data.size());
    std::memcpy(buffer, found->second.data.data(), copied);
    *length = copied;
  }
  for (auto watch : pending)
    Emit(watch, ZOO_DELETED_EVENT, ZOO_CONNECTED_STATE);
  return ZOK;
}
int __wrap_zoo_acreate(zhandle_t* handle, const char* path, const char* data,
                       int length, const ACL_vector* acl, int flags,
                       string_completion_t complete, const void* context) {
  const int rc =
      __wrap_zoo_create(handle, path, data, length, acl, flags, nullptr, 0);
  {
    std::lock_guard<std::mutex> lock(mutex);
    if ((flags & ZOO_EPHEMERAL) && defer_create) {
      defer_create = false;
      pending_operations.push_back(
          {Cast(handle), [complete, context](int result) {
             complete(result, nullptr, context);
           }});
      cv.notify_all();
      return ZOK;
    }
  }
  complete(rc, path, context);
  return ZOK;
}
int __wrap_zoo_aexists(zhandle_t* handle, const char* path, int watch,
                       stat_completion_t complete, const void* context) {
  Stat stat{};
  const int rc = __wrap_zoo_exists(handle, path, watch, &stat);
  complete(rc, &stat, context);
  return ZOK;
}
int __wrap_zoo_awexists(zhandle_t* handle, const char* path, watcher_fn watcher,
                        void* watcher_context, stat_completion_t complete,
                        const void* context) {
  Stat stat{};
  const int rc =
      __wrap_zoo_wexists(handle, path, watcher, watcher_context, &stat);
  complete(rc, &stat, context);
  return ZOK;
}
int __wrap_zoo_aget(zhandle_t* handle, const char* path, int watch,
                    data_completion_t complete, const void* context) {
  char data[2048];
  int length = sizeof(data);
  Stat stat{};
  const int rc = __wrap_zoo_get(handle, path, watch, data, &length, &stat);
  {
    std::lock_guard<std::mutex> lock(mutex);
    if (defer_get) {
      defer_get = false;
      pending_operations.push_back(
          {Cast(handle), [complete, context](int result) {
             complete(result, nullptr, 0, nullptr, context);
           }});
      cv.notify_all();
      return ZOK;
    }
  }
  complete(rc, data, length, &stat, context);
  return ZOK;
}
}

namespace cookrpc {
namespace {
class RegistryTest : public testing::Test {
 protected:
  void SetUp() override {
    std::lock_guard<std::mutex> lock(mutex);
    handles.clear();
    nodes.clear();
    watches.clear();
    vanish_before_watch = replace_during_get = false;
    watch_count = 0;
    defer_create = defer_get = false;
    pending_operations.clear();
    canceled_completions = 0;
    hold_close = close_entered = release_close = reject_connections = false;
  }
  void TearDown() override {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto& handle : handles) EXPECT_TRUE(handle->closed);
    EXPECT_TRUE(pending_operations.empty());
  }
};

TEST_F(RegistryTest,
       ExistingPersistentParentsAndOwnMatchingInstanceAreIdempotent) {
  ServiceRegistry registry("fake");
  ASSERT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
  const auto owner = SnapshotNode().owner;
  ASSERT_NE(owner, 0);
  EXPECT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
  EXPECT_EQ(SnapshotNode().owner, owner);
}

TEST_F(RegistryTest, ForeignSessionCannotReportSuccessBeforeOwnershipTransfer) {
  auto old = std::make_unique<ServiceRegistry>("fake");
  ASSERT_TRUE(old->RegisterService("service", "127.0.0.1:8000"));
  const auto old_owner = SnapshotNode().owner;
  ServiceRegistry next("fake");
  EXPECT_FALSE(next.RegisterService("service", "127.0.0.1:8000", 20ms));
  EXPECT_EQ(SnapshotNode().owner, old_owner);
  auto registration = std::async(std::launch::async, [&] {
    return next.RegisterService("service", "127.0.0.1:8000", 2s);
  });
  EXPECT_TRUE(WaitForWatch(2));
  EXPECT_EQ(registration.wait_for(20ms), std::future_status::timeout);
  old.reset();
  EXPECT_TRUE(registration.get());
  EXPECT_NE(SnapshotNode().owner, old_owner);
  EXPECT_EQ(SnapshotNode().data, "127.0.0.1:8000");
}

TEST_F(RegistryTest, PersistentInstanceAndEphemeralParentAreConflicts) {
  ServiceRegistry registry("fake");
  {
    std::lock_guard<std::mutex> lock(mutex);
    nodes[path] = {0, "127.0.0.1:8000"};
  }
  EXPECT_FALSE(registry.RegisterService("service", "127.0.0.1:8000"));
  {
    std::lock_guard<std::mutex> lock(mutex);
    nodes.erase(path);
    nodes["/cookrpc/service"].owner = 777;
  }
  EXPECT_FALSE(registry.RegisterService("service", "127.0.0.1:8000"));
}

TEST_F(RegistryTest, OwnInstanceWithDifferentAddressDataIsRejected) {
  ServiceRegistry registry("fake");
  ASSERT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
  {
    std::lock_guard<std::mutex> lock(mutex);
    nodes[path].data = "other address";
  }
  EXPECT_FALSE(registry.RegisterService("service", "127.0.0.1:8000"));
}

TEST_F(RegistryTest, NodeDisappearingBeforeWatchIsCreatedByTheNewSession) {
  auto old = std::make_unique<ServiceRegistry>("fake");
  ASSERT_TRUE(old->RegisterService("service", "127.0.0.1:8000"));
  const auto old_owner = SnapshotNode().owner;
  ServiceRegistry next("fake");
  {
    std::lock_guard<std::mutex> lock(mutex);
    vanish_before_watch = true;
  }
  ASSERT_TRUE(next.RegisterService("service", "127.0.0.1:8000"));
  const auto new_owner = SnapshotNode().owner;
  EXPECT_NE(old_owner, new_owner);
  old.reset();
  EXPECT_EQ(SnapshotNode().owner, new_owner);
}

TEST_F(RegistryTest, OwnershipChangedBetweenExistsAndGetIsNotAccepted) {
  ServiceRegistry registry("fake");
  ASSERT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
  {
    std::lock_guard<std::mutex> lock(mutex);
    replace_during_get = true;
  }
  EXPECT_FALSE(registry.RegisterService("service", "127.0.0.1:8000", 20ms));
  EXPECT_EQ(SnapshotNode().owner, 999);
}

TEST_F(RegistryTest,
       RecreatedForeignNodeRequiresAnotherWatchAndOwnershipCheck) {
  ServiceRegistry registry("fake");
  {
    std::lock_guard<std::mutex> lock(mutex);
    nodes[path] = {999, "127.0.0.1:8000"};
  }
  auto registration = std::async(std::launch::async, [&] {
    return registry.RegisterService("service", "127.0.0.1:8000", 2s);
  });
  EXPECT_TRUE(WaitForWatch());
  std::vector<Watch> pending;
  {
    std::lock_guard<std::mutex> lock(mutex);
    pending = EraseLocked(path);
    nodes[path] = {888, "127.0.0.1:8000"};
  }
  for (auto watch : pending)
    Emit(watch, ZOO_DELETED_EVENT, ZOO_CONNECTED_STATE);
  EXPECT_TRUE(WaitForWatch(2));
  EXPECT_EQ(registration.wait_for(20ms), std::future_status::timeout);
  Delete(path);
  EXPECT_TRUE(registration.get());
  EXPECT_NE(SnapshotNode().owner, 888);
}

TEST_F(RegistryTest,
       ExpiredSessionReconnectsAndReregistersWithoutWatcherDoingIo) {
  ServiceRegistry registry("fake");
  ASSERT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
  const auto old_owner = SnapshotNode().owner;
  Handle* old;
  {
    std::lock_guard<std::mutex> lock(mutex);
    old = handles.front().get();
  }
  Expire(old);
  bool restored = false;
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      restored =
          nodes.count(path) && nodes.at(path).owner != old_owner && old->closed;
    }
    if (restored) break;
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_TRUE(restored);
  EXPECT_TRUE(registry.IsConnected());
  EXPECT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
}

TEST_F(RegistryTest, RegistrationDeadlineIncludesWaitingForAnotherRegistrar) {
  ServiceRegistry registry("fake");
  {
    std::lock_guard<std::mutex> lock(mutex);
    nodes[path] = {999, "127.0.0.1:8000"};
  }
  auto blocked = std::async(std::launch::async, [&] {
    return registry.RegisterService("service", "127.0.0.1:8000", 2s);
  });
  EXPECT_TRUE(WaitForWatch());
  const auto began = std::chrono::steady_clock::now();
  EXPECT_FALSE(registry.RegisterService("service", "127.0.0.1:8001", 20ms));
  EXPECT_LT(std::chrono::steady_clock::now() - began, 300ms);
  Delete(path);
  EXPECT_TRUE(blocked.get());
}
TEST_F(RegistryTest,
       CreateDeadlineRetiresUnknownOutcomeAndRestoresOnlyCommittedNodes) {
  ServiceRegistry registry("fake");
  ASSERT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
  const auto old_owner = SnapshotNode().owner;
  {
    std::lock_guard<std::mutex> lock(mutex);
    defer_create = true;
  }
  const auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(registry.RegisterService("service", "127.0.0.1:8001", 40ms));
  EXPECT_LT(std::chrono::steady_clock::now() - start, 300ms);
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 2s, [&] {
      return canceled_completions == 1 && nodes.count(path) &&
             nodes.at(path).owner != old_owner &&
             !nodes.count("/cookrpc/service/127.0.0.1:8001");
    }));
  }
  EXPECT_TRUE(registry.RegisterService("service", "127.0.0.1:8001"));
}

TEST_F(RegistryTest, ReconnectFailureStillClosesTheUncertainSession) {
  ServiceRegistry registry("fake");
  Handle* old;
  {
    std::lock_guard<std::mutex> lock(mutex);
    old = handles.front().get();
    defer_create = reject_connections = true;
  }
  EXPECT_FALSE(registry.RegisterService("service", "127.0.0.1:8000", 40ms));
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, 2s,
                            [&] { return old->closed && !nodes.count(path); }));
    reject_connections = false;
  }
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!registry.IsConnected() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();
  EXPECT_TRUE(registry.IsConnected());
  EXPECT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
}

TEST_F(RegistryTest, LateSuccessfulCreateCannotBecomeAConfirmedRegistration) {
  ServiceRegistry registry("fake");
  ASSERT_TRUE(registry.RegisterService("service", "127.0.0.1:8000"));
  const auto old_owner = SnapshotNode().owner;
  {
    std::lock_guard<std::mutex> lock(mutex);
    defer_create = hold_close = true;
  }
  EXPECT_FALSE(registry.RegisterService("service", "127.0.0.1:8001", 40ms));
  std::function<void(int)> complete;
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, 2s, [] { return close_entered; }));
    if (!pending_operations.empty()) {
      complete = std::move(pending_operations.front().complete);
      pending_operations.erase(pending_operations.begin());
    }
  }
  if (complete) complete(ZOK);  // SDK reports success after the public timeout.
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_close = true;
    hold_close = false;
  }
  cv.notify_all();
  EXPECT_TRUE(static_cast<bool>(complete));
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, 2s, [&] {
      return nodes.count(path) && nodes.at(path).owner != old_owner &&
             !nodes.count("/cookrpc/service/127.0.0.1:8001");
    }));
  }
  ASSERT_TRUE(registry.RegisterService("service", "127.0.0.1:8001"));
  const auto owner = SnapshotNode().owner;
  EXPECT_NE(owner, old_owner);
}

TEST_F(RegistryTest,
       ReadDeadlineAndPendingCallbackRemainSafeDuringDestruction) {
  auto registry = std::make_unique<ServiceRegistry>("fake");
  ASSERT_TRUE(registry->RegisterService("service", "127.0.0.1:8000"));
  {
    std::lock_guard<std::mutex> lock(mutex);
    defer_get = true;
  }
  const auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(registry->RegisterService("service", "127.0.0.1:8000", 40ms));
  EXPECT_LT(std::chrono::steady_clock::now() - start, 300ms);
  registry.reset();
  std::lock_guard<std::mutex> lock(mutex);
  EXPECT_EQ(canceled_completions, 1u);
  EXPECT_TRUE(pending_operations.empty());
}

}  // namespace
}  // namespace cookrpc
