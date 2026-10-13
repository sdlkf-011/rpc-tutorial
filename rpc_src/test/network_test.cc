#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <zookeeper/zookeeper.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

#include "connection.h"
#include "connection_manager.h"
#include "message.pb.h"
#include "message_cycle.h"
#include "rpc_client.h"
#include "service_manager.h"
#include "thread_pool_singleton.h"
#include "zk_conn_handler.h"

namespace {
using namespace std::chrono_literals;
std::atomic<int> discovered_port{0};
std::mutex discovery_data_mutex;
std::string discovery_address_override;
struct DiscoveryNode {
  std::string name;
  std::string address;
  int result = ZOK;
};
// 可选快照用于模拟成功空列表、部分节点失败和目录查询失败。
std::optional<std::vector<DiscoveryNode>> discovery_nodes;
int discovery_children_result = ZOK;
std::atomic<int> accept_error_fd{-1}, accept_error_code{0};
std::mutex accept_error_mutex;
std::condition_variable accept_error_cv;
std::vector<std::chrono::steady_clock::time_point> accept_error_times;
std::atomic<bool> monitor_accepts{false};
std::atomic<size_t> accept_batch{0}, largest_accept_batch{0};
thread_local bool interrupt_connect_poll = false;
thread_local int connect_interrupts = 0;
thread_local std::vector<int> connect_poll_timeouts;
struct FakeZk {
  watcher_fn watcher;
  void* context;
};
}  // namespace
extern "C" {
int __real_accept(int, sockaddr*, socklen_t*);
int __wrap_accept(int fd, sockaddr* address, socklen_t* length) {
  const int error = accept_error_code.load();
  if (fd == accept_error_fd.load() && error != 0) {
    {
      std::lock_guard<std::mutex> lock(accept_error_mutex);
      if (accept_error_times.size() < 32)
        accept_error_times.push_back(std::chrono::steady_clock::now());
    }
    accept_error_cv.notify_all();
    errno = error;
    return -1;
  }
  const int accepted = __real_accept(fd, address, length);
  if (accepted >= 0 && monitor_accepts.load()) ++accept_batch;
  return accepted;
}
int __real_epoll_wait(int, epoll_event*, int, int);
int __wrap_epoll_wait(int fd, epoll_event* events, int count, int timeout) {
  if (monitor_accepts.load()) {
    const auto batch = accept_batch.exchange(0);
    auto largest = largest_accept_batch.load();
    while (batch > largest &&
           !largest_accept_batch.compare_exchange_weak(largest, batch)) {
    }
  }
  return __real_epoll_wait(fd, events, count, timeout);
}
int __real_poll(pollfd*, nfds_t, int);
int __wrap_poll(pollfd* fds, nfds_t count, int timeout) {
  if (interrupt_connect_poll && count == 1 && fds[0].events == POLLOUT) {
    connect_poll_timeouts.push_back(timeout);
    if (connect_interrupts++ < 3) {
      std::this_thread::sleep_for(10ms);
      errno = EINTR;
      return -1;
    }
    return 0;
  }
  return __real_poll(fds, count, timeout);
}
zhandle_t* __wrap_zookeeper_init(const char*, watcher_fn fn, int,
                                 const clientid_t*, void* context, int) {
  auto* handle = new FakeZk{fn, context};
  fn(reinterpret_cast<zhandle_t*>(handle), ZOO_SESSION_EVENT,
     ZOO_CONNECTED_STATE, nullptr, context);
  return reinterpret_cast<zhandle_t*>(handle);
}
int __wrap_zookeeper_close(zhandle_t* handle) {
  delete reinterpret_cast<FakeZk*>(handle);
  return ZOK;
}
int __wrap_zoo_get_children(zhandle_t*, const char*, int,
                            String_vector* children) {
  std::lock_guard<std::mutex> lock(discovery_data_mutex);
  if (discovery_children_result != ZOK) return discovery_children_result;
  const std::vector<DiscoveryNode> nodes =
      discovery_nodes.value_or(std::vector<DiscoveryNode>{{"node", ""}});
  children->count = static_cast<int>(nodes.size());
  children->data = nodes.empty() ? nullptr : new char*[nodes.size()];
  for (size_t i = 0; i < nodes.size(); ++i) {
    children->data[i] = new char[nodes[i].name.size() + 1];
    std::memcpy(children->data[i], nodes[i].name.c_str(),
                nodes[i].name.size() + 1);
  }
  return ZOK;
}
int __wrap_deallocate_String_vector(String_vector* children) {
  for (int i = 0; i < children->count; ++i) delete[] children->data[i];
  delete[] children->data;
  children->data = nullptr;
  children->count = 0;
  return ZOK;
}
int __wrap_zoo_get(zhandle_t*, const char* path, int, char* buffer, int* length,
                   Stat*) {
  std::string data;
  {
    std::lock_guard<std::mutex> lock(discovery_data_mutex);
    if (discovery_nodes) {
      const std::string node =
          std::string(path).substr(std::string(path).rfind('/') + 1);
      for (const auto& entry : *discovery_nodes) {
        if (entry.name != node) continue;
        if (entry.result != ZOK) return entry.result;
        if (static_cast<int>(entry.address.size()) > *length)
          return ZMARSHALLINGERROR;
        std::memcpy(buffer, entry.address.data(), entry.address.size());
        *length = static_cast<int>(entry.address.size());
        return ZOK;
      }
      return ZNONODE;
    }
    data = discovery_address_override.empty()
               ? "127.0.0.1:" + std::to_string(discovered_port.load())
               : discovery_address_override;
  }
  if (static_cast<int>(data.size()) > *length) return ZMARSHALLINGERROR;
  std::memcpy(buffer, data.data(), data.size());
  *length = data.size();
  return ZOK;
}
}

namespace cookrpc {
namespace {
using namespace std::chrono_literals;
struct Fd {
  int value = -1;
  explicit Fd(int fd = -1) : value(fd) {}
  ~Fd() {
    if (value >= 0) close(value);
  }
  int Release() { return std::exchange(value, -1); }
};
struct Listener {
  Fd fd{socket(AF_INET, SOCK_STREAM, 0)};
  uint16_t port = 0;
  explicit Listener(int backlog = 32) {
    if (fd.value < 0) throw std::runtime_error("socket failed");
    const int small = 4096;
    setsockopt(fd.value, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd.value, reinterpret_cast<sockaddr*>(&address),
             sizeof(address)) != 0 ||
        listen(fd.value, backlog) != 0)
      throw std::runtime_error("listen failed");
    socklen_t length = sizeof(address);
    getsockname(fd.value, reinterpret_cast<sockaddr*>(&address), &length);
    port = ntohs(address.sin_port);
  }
  void Connect(int fd) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
        0)
      throw std::runtime_error("connect failed");
  }
  std::shared_ptr<Connection> Client(bool slow = false) {
    Fd client(socket(AF_INET, SOCK_STREAM, 0));
    const int small = 4096;
    if (slow)
      setsockopt(client.value, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    Connect(client.value);
    return std::make_shared<Connection>(client.Release());
  }
};
std::string Request(const std::string& method, uint32_t id) {
  RpcRequest request;
  request.SetServiceName("NetworkRegressionService");
  request.SetMethodName(method);
  request.SetSequenceId(id);
  request.SetPayload("payload");
  std::string frame;
  if (!request.Serialize(frame)) throw std::runtime_error("serialize failed");
  return frame;
}
std::string ReadFrame(const std::shared_ptr<Connection>& connection) {
  std::string data;
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto read = connection->ReadWithTimeout(100);
    data += connection->GetReadBufferData();
    if (data.size() >= RpcHeader::kHeaderSize) {
      uint32_t network;
      std::memcpy(&network, data.data() + 4, 4);
      const size_t size = RpcHeader::kHeaderSize + ntohl(network);
      if (size <= data.size()) return data.substr(0, size);
    }
    if (read.error || read.eof) break;
  }
  return {};
}
struct Gate {
  std::promise<void> started;
  std::promise<void> release;
  std::shared_future<void> signal = release.get_future().share();
  bool open = false;
  void Open() {
    if (!open) {
      open = true;
      release.set_value();
    }
  }
  ~Gate() { Open(); }
};
class NetworkService : public Service {
 public:
  std::mutex mutex;
  std::shared_ptr<Gate> gate;
  std::string GetServiceName() const override {
    return "NetworkRegressionService";
  }
  bool HandleRequest(const std::string& method, const std::string&,
                     std::string& result) override {
    if (method == "delay") {
      std::shared_ptr<Gate> current;
      {
        std::lock_guard<std::mutex> lock(mutex);
        current = gate;
      }
      current->started.set_value();
      current->signal.wait();
    }
    result = method == "boundary"
                 ? std::string(RpcHeader::kMaxBodySize - 12, 'x')
             : method == "huge" || method == "error"
                 ? std::string(2 * RpcHeader::kMaxBodySize, 'x')
                 : "small";
    return method != "error";
  }
};
std::shared_ptr<NetworkService> TestService() {
  static auto service = [] {
    auto value = std::make_shared<NetworkService>();
    if (!ServiceManager::GetInstance().RegisterService(value))
      throw std::runtime_error("service registration failed");
    return value;
  }();
  return service;
}
template <typename Predicate>
bool Eventually(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}

class NetworkTest : public testing::Test {
 protected:
  Listener listener;
  std::unique_ptr<MessageCycle> cycle;
  std::thread loop;
  size_t connection_limit = 10000;
  TransportOptions transport_options;
  RateLimitOptions rate_limit_options;
  void SetUp() override {
    {
      std::lock_guard<std::mutex> lock(discovery_data_mutex);
      discovery_nodes.reset();
      discovery_children_result = ZOK;
    }
    TestService();
    meeting_ctrl::ThreadPoolSingleton::Destroy();
    ASSERT_TRUE(meeting_ctrl::ThreadPoolSingleton::Init(1));
    cycle = std::make_unique<MessageCycle>(
        nullptr, connection_limit, transport_options, rate_limit_options);
    ASSERT_TRUE(cycle->AddListenFd(listener.fd.value));
    loop = std::thread([this] { cycle->Loop(); });
  }
  void TearDown() override {
    accept_error_code = 0;
    accept_error_fd = -1;
    auto service = TestService();
    {
      std::lock_guard<std::mutex> lock(service->mutex);
      if (service->gate) service->gate->Open();
    }
    if (cycle) cycle->Stop();
    if (loop.joinable()) loop.join();
    cycle.reset();
    EXPECT_TRUE(meeting_ctrl::ThreadPoolSingleton::Destroy());
    EXPECT_EQ(ConnectionManager::GetInstance().GetConnectionCount(), 0u);
    {
      std::lock_guard<std::mutex> lock(discovery_data_mutex);
      discovery_nodes.reset();
      discovery_children_result = ZOK;
    }
  }
};

TEST_F(NetworkTest, ResourceErrorsBackOffWhileExistingConnectionsRemainUsable) {
  auto existing = listener.Client();
  ASSERT_TRUE(existing->Write(Request("normal", 1)));
  ASSERT_FALSE(ReadFrame(existing).empty());
  uint32_t id = 2;
  for (int error : {EMFILE, ENFILE, ENOBUFS, ENOMEM}) {
    {
      std::lock_guard<std::mutex> lock(accept_error_mutex);
      accept_error_times.clear();
    }
    accept_error_fd = listener.fd.value;
    accept_error_code = error;
    auto pending = listener.Client();
    {
      std::unique_lock<std::mutex> lock(accept_error_mutex);
      ASSERT_TRUE(accept_error_cv.wait_for(
          lock, 2s, [] { return !accept_error_times.empty(); }));
    }
    ASSERT_TRUE(existing->Write(Request("normal", id++)));
    EXPECT_FALSE(ReadFrame(existing).empty());
    {
      std::unique_lock<std::mutex> lock(accept_error_mutex);
      ASSERT_TRUE(accept_error_cv.wait_for(
          lock, 2s, [] { return accept_error_times.size() >= 3; }));
      EXPECT_GE(accept_error_times[1] - accept_error_times[0], 90ms);
      EXPECT_GE(accept_error_times[2] - accept_error_times[1], 180ms);
    }
    EXPECT_EQ(cycle->GetListenFdCount(), 1u);
    accept_error_code = 0;
    ASSERT_TRUE(pending->Write(Request("normal", id++)));
    EXPECT_FALSE(ReadFrame(pending).empty());
    pending->Close();
    ASSERT_TRUE(Eventually([] {
      return ConnectionManager::GetInstance().GetConnectionCount() == 1;
    }));
  }
}

TEST_F(NetworkTest,
       RemovingPausedListenerCancelsItsRetryAndAllowsFreshRegistration) {
  {
    std::lock_guard<std::mutex> lock(accept_error_mutex);
    accept_error_times.clear();
  }
  accept_error_fd = listener.fd.value;
  accept_error_code = EMFILE;
  auto pending = listener.Client();
  {
    std::unique_lock<std::mutex> lock(accept_error_mutex);
    ASSERT_TRUE(accept_error_cv.wait_for(
        lock, 2s, [] { return !accept_error_times.empty(); }));
  }
  EXPECT_TRUE(cycle->RemoveListenFd(listener.fd.value));
  accept_error_code = 0;
  EXPECT_EQ(cycle->GetListenFdCount(), 0u);
  EXPECT_TRUE(pending->ReadWithTimeout(250).timeout);
  ASSERT_TRUE(cycle->AddListenFd(listener.fd.value));
  ASSERT_TRUE(pending->Write(Request("normal", 1)));
  EXPECT_FALSE(ReadFrame(pending).empty());
}

class ConnectionCapacityTest : public NetworkTest {
  void SetUp() override {
    connection_limit = 2;
    NetworkTest::SetUp();
  }
};
TEST_F(ConnectionCapacityTest,
       RejectsExcessConnectionsAndReusesReleasedCapacity) {
  auto first = listener.Client();
  auto second = listener.Client();
  ASSERT_TRUE(Eventually([] {
    return ConnectionManager::GetInstance().GetConnectionCount() == 2;
  }));
  auto excess = listener.Client();
  const auto rejected = excess->ReadWithTimeout(1000);
  EXPECT_TRUE(rejected.eof || rejected.error);
  EXPECT_EQ(ConnectionManager::GetInstance().GetConnectionCount(), 2u);
  ASSERT_TRUE(first->Write(Request("normal", 1)));
  EXPECT_FALSE(ReadFrame(first).empty());
  first->Close();
  ASSERT_TRUE(Eventually([] {
    return ConnectionManager::GetInstance().GetConnectionCount() == 1;
  }));
  auto next = listener.Client();
  ASSERT_TRUE(next->Write(Request("normal", 2)));
  EXPECT_FALSE(ReadFrame(next).empty());
  EXPECT_LE(ConnectionManager::GetInstance().GetConnectionCount(), 2u);
}

TEST(NetworkAdmissionTest, LargeAcceptBacklogYieldsBetweenBatches) {
  Listener listener(256);
  std::vector<std::shared_ptr<Connection>> clients;
  for (int i = 0; i < 128; ++i) clients.push_back(listener.Client());
  MessageCycle cycle(nullptr);
  ASSERT_TRUE(cycle.AddListenFd(listener.fd.value));
  accept_batch = largest_accept_batch = 0;
  monitor_accepts = true;
  std::thread loop([&] { cycle.Loop(); });
  struct Cleanup {
    MessageCycle& cycle;
    std::thread& loop;
    ~Cleanup() {
      monitor_accepts = false;
      cycle.Stop();
      loop.join();
    }
  } cleanup{cycle, loop};
  ASSERT_TRUE(Eventually([] {
    return ConnectionManager::GetInstance().GetConnectionCount() == 128;
  }));
  EXPECT_GT(largest_accept_batch.load(), 0u);
  EXPECT_LT(largest_accept_batch.load(), clients.size());
}

TEST(ConnectionTest, PollHangupKeepsDataAndReportsEofAlongsideIt) {
  int descriptors[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors), 0);
  auto reader = std::make_shared<Connection>(descriptors[0]);
  Fd writer(descriptors[1]);
  ASSERT_EQ(send(writer.value, "before EOF", 10, 0), 10);
  close(writer.Release());
  auto result = reader->ReadWithTimeout(100);
  EXPECT_EQ(result.bytes_received, 10u);
  EXPECT_TRUE(result.eof);
  EXPECT_FALSE(result.error);
  EXPECT_EQ(reader->GetReadBufferData(), "before EOF");
}

TEST(ConnectionTest, AsyncQueueIsBoundedAndDoesNotSendOnSubmittingThread) {
  int descriptors[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors), 0);
  auto sender = std::make_shared<Connection>(descriptors[0]);
  Fd peer(descriptors[1]);
  sender->SetEventCallback([](const auto&) { return true; });
  RpcResponse response;
  response.SetResultData(std::string(RpcHeader::kMaxBodySize - 12, 'x'));
  for (int i = 0; i < 4; ++i) EXPECT_TRUE(sender->Write(response));
  EXPECT_EQ(sender->PendingWriteBytes(), Connection::kMaxWriteBytes);
  char byte;
  EXPECT_EQ(recv(peer.value, &byte, 1, MSG_DONTWAIT), -1);
  EXPECT_FALSE(sender->Write(response));
  EXPECT_TRUE(sender->ShouldClose());
  EXPECT_GE(sender->GetFd(), 0);  // 排队关闭不由提交线程执行 close。
}

TEST(ConnectionTest, SyncSendUsesOneTotalDeadlineUnderBackpressure) {
  int descriptors[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors), 0);
  auto sender = std::make_shared<Connection>(descriptors[0]);
  Fd peer(descriptors[1]);
  const int small = 4096;
  setsockopt(sender->GetFd(), SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
  auto begin = std::chrono::steady_clock::now();
  EXPECT_FALSE(
      sender->WriteWithTimeout(std::string(RpcHeader::kMaxFrameSize, 'x'), 30));
  EXPECT_LT(std::chrono::steady_clock::now() - begin, 500ms);
  EXPECT_FALSE(sender->IsValid());
}

TEST(ConnectionTest, ShortWritesAndBudgetPreserveTheCompleteFrame) {
  int descriptors[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors), 0);
  auto sender = std::make_shared<Connection>(descriptors[0]);
  auto reader = std::make_shared<Connection>(descriptors[1]);
  int small = 4096;
  setsockopt(sender->GetFd(), SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
  sender->SetEventCallback([](const auto&) { return true; });
  RpcResponse response;
  response.SetSequenceId(93);
  response.SetResultData(std::string(50000, 'x'));
  std::string expected;
  ASSERT_TRUE(response.Serialize(expected));
  ASSERT_TRUE(sender->Write(response));
  EXPECT_EQ(sender->FlushWrites(1024), Connection::FlushResult::kPending);
  EXPECT_EQ(sender->PendingWriteBytes(), expected.size() - 1024);
  EXPECT_EQ(sender->FlushWrites(64 * 1024), Connection::FlushResult::kPending);
  std::string received;
  ASSERT_TRUE(Eventually([&] {
    reader->Read();
    received += reader->GetReadBufferData();
    EXPECT_NE(sender->FlushWrites(1024), Connection::FlushResult::kError);
    return sender->PendingWriteBytes() == 0;
  }));
  reader->Read();
  received += reader->GetReadBufferData();
  EXPECT_EQ(received, expected);
}

TEST_F(NetworkTest, SlowReaderDoesNotBlockOtherConnectionsOrTheOnlyWorker) {
  auto slow = listener.Client(true);
  ASSERT_TRUE(slow->Write(Request("boundary", 1)));
  ASSERT_TRUE(Eventually([] {
    return meeting_ctrl::ThreadPoolSingleton::GetStats().tasks_completed >= 1;
  }));
  auto fast = listener.Client();
  const auto began = std::chrono::steady_clock::now();
  ASSERT_TRUE(fast->Write(Request("small", 2)));
  RpcResponse response;
  ASSERT_TRUE(response.Deserialize(ReadFrame(fast)));
  EXPECT_EQ(response.GetResultData(), "small");
  EXPECT_EQ(response.GetSequenceId(), 2u);
  EXPECT_LT(std::chrono::steady_clock::now() - began, 500ms);
}

TEST_F(NetworkTest,
       BoundaryAndOversizedResultAndErrorAllReturnParseableFrames) {
  for (const auto& method : {"boundary", "huge", "error"}) {
    auto client = listener.Client();
    ASSERT_TRUE(client->Write(Request(method, 42)));
    RpcResponse response;
    const auto frame = ReadFrame(client);
    ASSERT_TRUE(response.Deserialize(frame));
    EXPECT_EQ(response.GetSequenceId(), 42u);
    if (std::string(method) == "boundary") {
      EXPECT_EQ(frame.size(), RpcHeader::kMaxFrameSize);
      EXPECT_EQ(response.GetErrorCode(), 0u);
    } else {
      EXPECT_EQ(response.GetErrorCode(),
                static_cast<uint32_t>(ErrorCode::kProtocolError));
      EXPECT_TRUE(response.GetResultData().empty());
      EXPECT_LT(frame.size(), 256u);
    }
  }
}

TEST_F(NetworkTest, ReadHalfCloseStillReceivesTheAcceptedRequestResponse) {
  auto client = listener.Client();
  ASSERT_TRUE(client->Write(Request("small", 8)));
  shutdown(client->GetFd(), SHUT_WR);
  RpcResponse response;
  ASSERT_TRUE(response.Deserialize(ReadFrame(client)));
  EXPECT_EQ(response.GetSequenceId(), 8u);
  EXPECT_EQ(response.GetResultData(), "small");
}

TEST_F(NetworkTest, LateWorkerResponseAndCloseCannotAffectAReusedServerFd) {
  auto gate = std::make_shared<Gate>();
  {
    auto service = TestService();
    std::lock_guard<std::mutex> lock(service->mutex);
    service->gate = gate;
  }
  auto old = listener.Client();
  ASSERT_TRUE(old->Write(Request("delay", 1)));
  ASSERT_EQ(gate->started.get_future().wait_for(2s), std::future_status::ready);
  const auto connections = ConnectionManager::GetInstance().GetConnections();
  ASSERT_EQ(connections.size(), 1u);
  auto old_server = connections[0];
  const int reused_fd = old_server->GetFd();
  Fd new_client(socket(AF_INET, SOCK_STREAM, 0));
  old_server->Close();
  ASSERT_TRUE(Eventually([&] { return old_server->GetFd() == -1; }));
  listener.Connect(new_client.value);
  auto current = std::make_shared<Connection>(new_client.Release());
  ASSERT_TRUE(Eventually([] {
    return ConnectionManager::GetInstance().GetConnectionCount() == 1;
  }));
  auto new_server = ConnectionManager::GetInstance().GetConnections()[0];
  EXPECT_EQ(new_server->GetFd(), reused_fd);
  EXPECT_NE(new_server->GetId(), old_server->GetId());
  ASSERT_TRUE(current->Write(Request("small", 2)));
  gate->Open();
  old_server->Close();
  RpcResponse response;
  ASSERT_TRUE(response.Deserialize(ReadFrame(current)));
  EXPECT_EQ(response.GetSequenceId(), 2u);
  EXPECT_EQ(response.GetResultData(), "small");
}

TEST_F(NetworkTest, StopFlushesAcceptedResponseBeforeClosing) {
  auto gate = std::make_shared<Gate>();
  {
    auto service = TestService();
    std::lock_guard<std::mutex> lock(service->mutex);
    service->gate = gate;
  }
  auto client = listener.Client();
  ASSERT_TRUE(client->Write(Request("delay", 18)));
  ASSERT_EQ(gate->started.get_future().wait_for(2s), std::future_status::ready);
  cycle->Stop();
  gate->Open();
  RpcResponse response;
  ASSERT_TRUE(response.Deserialize(ReadFrame(client)));
  EXPECT_EQ(response.GetSequenceId(), 18u);
}

void ClientWithClosingPeer(bool partial) {
  Listener listener;
  discovered_port.store(listener.port);
  const auto path = std::filesystem::temp_directory_path() /
                    ("rpc-eof-" + std::to_string(getpid()) + ".json");
  {
    std::ofstream out(path);
    out << nlohmann::json{
        {"zk_host", "fake"}, {"retry_times", 0}, {"timeout_ms", 1000}};
  }
  RpcClient client(path.string());
  std::filesystem::remove(path);
  auto server = std::async(std::launch::async, [&] {
    auto peer = std::make_shared<Connection>(
        accept(listener.fd.value, nullptr, nullptr));
    RpcRequest request;
    const auto input = ReadFrame(peer);
    if (!request.Deserialize(input)) return false;
    RpcResponse response;
    response.SetSequenceId(request.GetSequenceId());
    response.SetResultData("\"done\"");
    std::string frame;
    response.Serialize(frame);
    if (partial) frame.resize(frame.size() / 2);
    const bool sent = peer->WriteWithTimeout(frame, 1000);
    peer->Close();
    return sent;
  });
  ASSERT_TRUE(client.Connect());
  std::string response;
  EXPECT_EQ(client.Call("service", "Echo", std::string("hello"),
                        SerializeType::JSON, response),
            !partial);
  if (!partial) {
    EXPECT_EQ(response, "done");
  }
  EXPECT_TRUE(server.get());
}
TEST(RpcClientNetworkTest, CompleteResponseImmediatelyFollowedByCloseSucceeds) {
  ClientWithClosingPeer(false);
}
TEST(RpcClientNetworkTest, HalfResponseImmediatelyFollowedByCloseFails) {
  ClientWithClosingPeer(true);
}

struct ConnectWaitTag {
  using type = bool (detail::RpcClientSession::*)(int, int) const;
  friend type ConnectWait(ConnectWaitTag);
};
template <class Tag, typename Tag::type Member>
struct ExposeMember {
  friend typename Tag::type ConnectWait(Tag) { return Member; }
};
template struct ExposeMember<ConnectWaitTag,
                             &detail::RpcClientSession::WaitForConnection>;
TEST(RpcClientNetworkTest, ConnectInterruptionsConsumeOneTotalDeadline) {
  const auto path =
      std::filesystem::temp_directory_path() /
      ("rpc-connect-deadline-" + std::to_string(getpid()) + ".json");
  struct Cleanup {
    std::filesystem::path path;
    ~Cleanup() { std::filesystem::remove(path); }
  } cleanup{path};
  std::ofstream(path) << nlohmann::json{{"timeout_ms", 100}};
  detail::RpcClientSession client(path.string());
  connect_interrupts = 0;
  connect_poll_timeouts.clear();
  interrupt_connect_poll = true;
  const bool connected = (client.*ConnectWait(ConnectWaitTag{}))(-1, 1);
  interrupt_connect_poll = false;
  EXPECT_FALSE(connected);
  ASSERT_EQ(connect_poll_timeouts.size(), 4u);
  EXPECT_LT(connect_poll_timeouts.back(), connect_poll_timeouts.front() - 15);
  EXPECT_LE(connect_poll_timeouts.front(), 100);
}

TEST(RpcClientNetworkTest,
     DiscoveryRejectsNoncanonicalAndNulContainingIpv4Text) {
  const auto path = std::filesystem::temp_directory_path() /
                    ("rpc-ip-validation-" + std::to_string(getpid()) + ".json");
  struct Cleanup {
    std::filesystem::path path;
    ~Cleanup() {
      std::filesystem::remove(path);
      std::lock_guard<std::mutex> lock(discovery_data_mutex);
      discovery_address_override.clear();
    }
  } cleanup{path};
  std::ofstream(path) << nlohmann::json{{"timeout_ms", 100},
                                        {"retry_times", 0}};
  Listener listener;
  RpcClient client(path.string());
  for (const auto& ip : std::vector<std::string>{
           "127.000.0.1", "127.0.0.1 ", std::string("127.0.0.1\0bad", 13)}) {
    {
      std::lock_guard<std::mutex> lock(discovery_data_mutex);
      discovery_address_override = ip + ":" + std::to_string(listener.port);
    }
    EXPECT_FALSE(client.Connect());
    client.Disconnect();
  }
  {
    std::lock_guard<std::mutex> lock(discovery_data_mutex);
    discovery_address_override = "127.0.0.1:" + std::to_string(listener.port);
  }
  EXPECT_TRUE(client.Connect());
}

TEST(ConnectionTest, RawOversizedResponseAlsoBecomesAShortError) {
  int descriptors[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors), 0);
  auto sender = std::make_shared<Connection>(descriptors[0]);
  auto reader = std::make_shared<Connection>(descriptors[1]);
  sender->SetEventCallback([](const auto&) { return true; });
  std::string frame(RpcHeader::kMaxFrameSize + 1, 'x');
  for (const auto& field : {std::pair<size_t, uint32_t>{0, RpcHeader::MAGIC},
                            {4, RpcHeader::kMaxBodySize + 1},
                            {8, 29}}) {
    const uint32_t value = htonl(field.second);
    std::memcpy(frame.data() + field.first, &value, sizeof(value));
  }
  ASSERT_TRUE(sender->Write(frame));
  EXPECT_EQ(sender->FlushWrites(65536), Connection::FlushResult::kDrained);
  RpcResponse response;
  ASSERT_TRUE(response.Deserialize(ReadFrame(reader)));
  EXPECT_EQ(response.GetSequenceId(), 29u);
  EXPECT_EQ(response.GetErrorCode(),
            static_cast<uint32_t>(ErrorCode::kProtocolError));
  EXPECT_TRUE(response.GetResultData().empty());
}

// 每个用例独立设置并恢复测试密钥，编码器在构造时保存自己的密钥快照。
struct SharedKey {
  std::optional<std::string> previous;
  SharedKey() {
    if (const char* key = std::getenv("COOKRPC_MASTER_KEY_HEX")) previous = key;
    setenv("COOKRPC_MASTER_KEY_HEX", std::string(64, '1').c_str(), 1);
  }
  ~SharedKey() {
    if (previous)
      setenv("COOKRPC_MASTER_KEY_HEX", previous->c_str(), 1);
    else
      unsetenv("COOKRPC_MASTER_KEY_HEX");
  }
};
class EchoTestService : public Service {
 public:
  std::atomic<size_t> requests{0};
  explicit EchoTestService(std::string name, std::shared_ptr<Gate> gate = {})
      : name_(std::move(name)), gate_(std::move(gate)) {}
  std::string GetServiceName() const override { return name_; }
  bool HandleRequest(const std::string&, const std::string& args,
                     std::string& result) override {
    ++requests;
    if (gate_) {
      gate_->started.set_value();
      gate_->signal.wait();
    }
    result = args;
    return true;
  }

 private:
  std::string name_;
  std::shared_ptr<Gate> gate_;
};
// 使用真实 TCP 端口，仅替换 ZooKeeper 查询结果，保持客户端收发实现不变。
struct ClientConfigFile {
  std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      ("rpc-feature-client-" + std::to_string(getpid()) + ".json");
  ClientConfigFile(uint16_t port,
                   nlohmann::json extra = nlohmann::json::object()) {
    discovered_port = port;
    extra["zk_host"] = "fake";
    extra["retry_times"] = 0;
    extra["timeout_ms"] = 1000;
    std::ofstream(path) << extra;
  }
  ~ClientConfigFile() { std::filesystem::remove(path); }
};
class SecuredNetworkTest : public NetworkTest {
 protected:
  SharedKey key;
  void SetUp() override {
    transport_options = {true, true, 0};
    NetworkTest::SetUp();
  }
};
TEST_F(SecuredNetworkTest,
       ClientAndServerCompressEncryptAndDecodeTheActualRpcCall) {
  ASSERT_TRUE(ServiceManager::GetInstance().RegisterService(
      std::make_shared<EchoTestService>("SecureEcho")));
  ClientConfigFile config(listener.port, {{"transport",
                                           {{"compression", true},
                                            {"encryption", true},
                                            {"compression_threshold", 0}}}});
  RpcClient client(config.path.string());
  nlohmann::json request{{"message", std::string(16000, 'x')}}, response;
  ASSERT_TRUE(client.Call("SecureEcho", "Echo", request, SerializeType::JSON,
                          response));
  EXPECT_EQ(response, request);
}
TEST_F(SecuredNetworkTest, IncorrectKeyOrPlainModeIsRejectedByTheServer) {
  ClientConfigFile config(
      listener.port,
      {{"transport", {{"compression", true}, {"encryption", true}}}});
  setenv("COOKRPC_MASTER_KEY_HEX", std::string(64, '2').c_str(), 1);
  RpcClient wrong(config.path.string());
  setenv("COOKRPC_MASTER_KEY_HEX", std::string(64, '1').c_str(), 1);
  std::string response;
  EXPECT_FALSE(wrong.Call("SecureEcho", "Echo", std::string("hello"),
                          SerializeType::JSON, response));
  ClientConfigFile plain_config(listener.port);
  RpcClient plain(plain_config.path.string());
  EXPECT_FALSE(plain.Call("SecureEcho", "Echo", std::string("hello"),
                          SerializeType::JSON, response));
}
TEST_F(SecuredNetworkTest,
       OversizedBusinessResponseReturnsAnAuthenticatedError) {
  TransportCodec codec(transport_options);
  std::string wire, plain;
  ASSERT_TRUE(codec.Encode(Request("huge", 73), wire));
  auto client = listener.Client();
  ASSERT_TRUE(client->WriteWithTimeout(wire, 1000));
  ASSERT_TRUE(codec.Decode(ReadFrame(client), plain));
  RpcResponse response;
  ASSERT_TRUE(response.Deserialize(plain));
  EXPECT_EQ(response.GetSequenceId(), 73u);
  EXPECT_EQ(response.GetErrorCode(),
            static_cast<uint32_t>(ErrorCode::kProtocolError));
  EXPECT_TRUE(response.GetResultData().empty());
}
class EncryptedNetworkTest : public SecuredNetworkTest {
 protected:
  void SetUp() override {
    transport_options = {false, true, 1024};
    NetworkTest::SetUp();
  }
};
TEST_F(EncryptedNetworkTest,
       EncryptionExpansionReturnsAnErrorInsteadOfDroppingBoundaryResult) {
  // 原始响应刚好达到 body 上限；加密膨胀后必须回送可认证的短错误。
  TransportCodec codec(transport_options);
  std::string wire, plain;
  ASSERT_TRUE(codec.Encode(Request("boundary", 74), wire));
  auto client = listener.Client();
  ASSERT_TRUE(client->WriteWithTimeout(wire, 1000));
  ASSERT_TRUE(codec.Decode(ReadFrame(client), plain));
  RpcResponse response;
  ASSERT_TRUE(response.Deserialize(plain));
  EXPECT_EQ(response.GetSequenceId(), 74u);
  EXPECT_EQ(response.GetErrorCode(),
            static_cast<uint32_t>(ErrorCode::kProtocolError));
  EXPECT_TRUE(response.GetResultData().empty());
}
class LimitedNetworkTest : public NetworkTest {
 protected:
  void SetUp() override {
    rate_limit_options = {1, 1};
    NetworkTest::SetUp();
  }
};
TEST_F(LimitedNetworkTest,
       ExcessRequestReturnsRateLimitedAndDoesNotTripClientBreaker) {
  // 将熔断阈值设为 1，确认有效限流响应不会被错误计为实例故障。
  ASSERT_TRUE(ServiceManager::GetInstance().RegisterService(
      std::make_shared<EchoTestService>("LimitedEcho")));
  ClientConfigFile config(listener.port,
                          {{"circuit_breaker", {{"failure_threshold", 1}}}});
  RpcClient client(config.path.string());
  std::string response;
  ASSERT_TRUE(client.Call("LimitedEcho", "Echo", std::string("first"),
                          SerializeType::JSON, response));
  EXPECT_EQ(response, "first");
  for (int i = 0; i < 3; ++i) {
    EXPECT_FALSE(client.Call("LimitedEcho", "Echo", std::string("excess"),
                             SerializeType::JSON, response));
    EXPECT_TRUE(client.IsConnected());
  }
}
TEST_F(LimitedNetworkTest,
       RateLimitErrorPreservesSequenceAndDoesNotConsumeWorkerQueue) {
  auto client = listener.Client();
  ASSERT_TRUE(client->Write(Request("normal", 81)));
  ASSERT_FALSE(ReadFrame(client).empty());
  ASSERT_TRUE(client->Write(Request("normal", 82)));
  RpcResponse response;
  ASSERT_TRUE(response.Deserialize(ReadFrame(client)));
  EXPECT_EQ(response.GetSequenceId(), 82u);
  EXPECT_EQ(response.GetErrorCode(),
            static_cast<uint32_t>(ErrorCode::kRateLimited));
  EXPECT_EQ(meeting_ctrl::ThreadPoolSingleton::GetQueueSize(), 0u);
}
TEST_F(NetworkTest,
       AsyncCallOwnsSessionAfterClientIsDestroyedWhileResponseIsBlocked) {
  auto gate = std::make_shared<Gate>();
  ASSERT_TRUE(ServiceManager::GetInstance().RegisterService(
      std::make_shared<EchoTestService>("AsyncLifetimeEcho", gate)));
  ClientConfigFile config(listener.port);
  auto client = std::make_unique<RpcClient>(config.path.string());
  auto pending = client->AsyncCall<std::string>("AsyncLifetimeEcho", "Echo",
                                                std::string("retained"),
                                                SerializeType::JSON);
  // 断言提前失败时也释放屏障，避免 future 析构无限等待业务返回。
  struct Release {
    std::shared_ptr<Gate> gate;
    ~Release() { gate->Open(); }
  } release{gate};
  ASSERT_EQ(gate->started.get_future().wait_for(2s), std::future_status::ready);
  // 确认请求已在业务线程中等待后销毁客户端，复现任务持有悬空对象的交错。
  client.reset();
  gate->Open();
  ASSERT_EQ(pending.wait_for(2s), std::future_status::ready);
  EXPECT_EQ(pending.get(), "retained");
}

TEST_F(SecuredNetworkTest, InvalidEncryptedReloadPreservesTheWorkingSession) {
  ASSERT_TRUE(ServiceManager::GetInstance().RegisterService(
      std::make_shared<EchoTestService>("ReloadEcho")));
  ClientConfigFile config(
      listener.port,
      {{"transport", {{"compression", true}, {"encryption", true}}}});
  RpcClient client(config.path.string());
  std::string response;
  ASSERT_TRUE(client.Call("ReloadEcho", "Echo", std::string("before"),
                          SerializeType::JSON, response));
  // 新配置因缺失密钥失败；当前连接仍使用已保存的密钥继续通信。
  unsetenv("COOKRPC_MASTER_KEY_HEX");
  EXPECT_FALSE(client.LoadConfig(config.path.string()));
  EXPECT_TRUE(client.IsConnected());
  EXPECT_TRUE(client.Call("ReloadEcho", "Echo", std::string("after"),
                          SerializeType::JSON, response));
  EXPECT_EQ(response, "after");
}
TEST_F(SecuredNetworkTest,
       ReplayingAnEncryptedRequestOnTheSameConnectionIsRejected) {
  TransportCodec codec(transport_options);
  std::string wire, plain;
  ASSERT_TRUE(codec.Encode(Request("normal", 83), wire));
  auto client = listener.Client();
  ASSERT_TRUE(client->WriteWithTimeout(wire, 1000));
  ASSERT_TRUE(codec.Decode(ReadFrame(client), plain));
  ASSERT_TRUE(client->WriteWithTimeout(wire, 1000));
  const auto result = client->ReadWithTimeout(1000);
  EXPECT_TRUE(result.eof || result.error);
  EXPECT_EQ(result.bytes_received, 0u);
}
TEST(RpcClientNetworkTest,
     CircuitBreakerAvoidsOpenInstanceAndRecoversAfterAProbe) {
  // 对端第一次读取请求后关闭连接，第二次连接回送正常响应作为恢复探测。
  Listener listener;
  ClientConfigFile config(
      listener.port,
      {{"circuit_breaker",
        {{"failure_threshold", 1}, {"reset_timeout_ms", 1000}}}});
  RpcClient client(config.path.string());
  std::atomic<int> accepts{0};
  auto server = std::async(std::launch::async, [&] {
    for (int i = 0; i < 2; ++i) {
      pollfd pending{listener.fd.value, POLLIN, 0};
      if (poll(&pending, 1, 5000) <= 0) return false;
      auto peer = std::make_shared<Connection>(
          accept(listener.fd.value, nullptr, nullptr));
      ++accepts;
      RpcRequest request;
      if (!request.Deserialize(ReadFrame(peer))) return false;
      if (i == 0) {
        peer->Close();
      } else {
        RpcResponse response;
        response.SetSequenceId(request.GetSequenceId());
        response.SetResultData("\"recovered\"");
        if (!peer->Write(response)) return false;
      }
    }
    return true;
  });
  std::string response;
  EXPECT_FALSE(client.Call("service", "Echo", std::string("first"),
                           SerializeType::JSON, response));
  EXPECT_FALSE(client.Call("service", "Echo", std::string("blocked"),
                           SerializeType::JSON, response));
  // 冷却期间连 TCP 建连都应跳过，不能仅在收发阶段拒绝。
  EXPECT_EQ(accepts.load(), 1);
  std::this_thread::sleep_for(1100ms);
  EXPECT_TRUE(client.Call("service", "Echo", std::string("probe"),
                          SerializeType::JSON, response));
  EXPECT_EQ(response, "recovered");
  EXPECT_EQ(accepts.load(), 2);
  EXPECT_TRUE(server.get());
}

TEST_F(NetworkTest, LocalResponseFormatMismatchDoesNotSendOrTripTheBreaker) {
  auto service = std::make_shared<EchoTestService>("LocalFormatEcho");
  ASSERT_TRUE(ServiceManager::GetInstance().RegisterService(service));
  ClientConfigFile config(listener.port,
                          {{"circuit_breaker", {{"failure_threshold", 1}}}});
  RpcClient client(config.path.string());
  std::string response;
  ASSERT_TRUE(client.Call("LocalFormatEcho", "Echo", std::string("before"),
                          SerializeType::JSON, response));
  rpc::tutorial::RpcResponse wrong_response;
  wrong_response.set_payload("unchanged");
  for (int i = 0; i < 6; ++i) {
    EXPECT_FALSE(client.Call("LocalFormatEcho", "Echo", std::string("invalid"),
                             SerializeType::JSON, wrong_response));
    EXPECT_TRUE(client.IsConnected());
  }
  // 响应类型不兼容必须在发包前拒绝，不改变目标对象或实例健康状态。
  EXPECT_EQ(service->requests.load(), 1u);
  EXPECT_EQ(wrong_response.payload(), "unchanged");
  rpc::tutorial::RpcRequest protobuf_request;
  protobuf_request.set_payload("protobuf request");
  EXPECT_FALSE(client.Call("LocalFormatEcho", "Echo", protobuf_request,
                           SerializeType::PROTOBUF, response));
  EXPECT_TRUE(client.IsConnected());
  EXPECT_EQ(service->requests.load(), 1u);
  ASSERT_TRUE(client.Call("LocalFormatEcho", "Echo", std::string("after"),
                          SerializeType::JSON, response));
  EXPECT_EQ(response, "after");
  EXPECT_EQ(service->requests.load(), 2u);
  // 兼容的 Protobuf 调用仍应正常执行，不能把类型检查变成统一拒绝。
  rpc::tutorial::RpcRequest protobuf_response;
  ASSERT_TRUE(client.Call("LocalFormatEcho", "Echo", protobuf_request,
                          SerializeType::PROTOBUF, protobuf_response));
  EXPECT_EQ(protobuf_response.payload(), "protobuf request");
  EXPECT_EQ(service->requests.load(), 3u);
}

TEST_F(NetworkTest, PartialDiscoveryKeepsMissingEndpointInItsCooldown) {
  Listener failing;
  const std::string address_a = "127.0.0.1:" + std::to_string(failing.port);
  const std::string address_b = "127.0.0.1:" + std::to_string(listener.port);
  auto set_nodes = [](std::vector<DiscoveryNode> nodes) {
    std::lock_guard<std::mutex> lock(discovery_data_mutex);
    discovery_nodes = std::move(nodes);
  };
  ASSERT_TRUE(ServiceManager::GetInstance().RegisterService(
      std::make_shared<EchoTestService>("PartialDiscoveryEcho")));
  set_nodes({{"A", address_a}});
  ClientConfigFile config(
      listener.port,
      {{"circuit_breaker",
        {{"failure_threshold", 1}, {"reset_timeout_ms", 60000}}}});
  RpcClient client(config.path.string());
  auto server = std::async(std::launch::async, [&] {
    pollfd ready{failing.fd.value, POLLIN, 0};
    if (poll(&ready, 1, 2000) <= 0) return false;
    auto peer = std::make_shared<Connection>(
        accept(failing.fd.value, nullptr, nullptr));
    if (ReadFrame(peer).empty()) return false;
    peer->Close();
    return true;
  });
  std::string response;
  EXPECT_FALSE(client.Call("PartialDiscoveryEcho", "Echo", std::string("fails"),
                           SerializeType::JSON, response));
  ASSERT_TRUE(server.get());
  // A 读取失败，B 仍可使用，但此部分结果不能清理 A 的熔断历史。
  set_nodes({{"A", address_a, ZCONNECTIONLOSS}, {"B", address_b}});
  ASSERT_TRUE(client.Call("PartialDiscoveryEcho", "Echo",
                          std::string("healthy B"), SerializeType::JSON,
                          response));
  EXPECT_EQ(response, "healthy B");
  client.Disconnect();
  // 恢复后的完整列表仍包含 A，必须继续遵守原来的 60 秒冷却。
  set_nodes({{"A", address_a}});
  EXPECT_FALSE(client.Connect());
  client.Disconnect();
  // 完整的成功空列表才证明节点已移除，可清理历史并接纳重新出现的实例。
  set_nodes({});
  EXPECT_FALSE(client.Connect());
  set_nodes({{"A", address_a}});
  EXPECT_TRUE(client.Connect());
}

TEST_F(NetworkTest,
       DiscoverySnapshotDistinguishesPartialFailureFromEmptySuccess) {
  ZkConnHandler handler;
  ASSERT_TRUE(handler.InitZkConnHandler({{"zk_host", "fake"}}));
  const auto set_nodes = [](std::vector<DiscoveryNode> nodes) {
    std::lock_guard<std::mutex> lock(discovery_data_mutex);
    discovery_nodes = std::move(nodes);
  };
  for (int failure : {ZCONNECTIONLOSS, ZNONODE, ZMARSHALLINGERROR}) {
    set_nodes({{"A", "address A", failure}, {"B", "address B"}});
    const auto partial = handler.GetAllServersSnapshot("/cookrpc");
    EXPECT_FALSE(partial.complete);
    EXPECT_EQ(partial.servers, std::vector<std::string>{"address B"});
    // 旧接口继续提供本轮可读地址，避免改变已有调用方的选址行为。
    EXPECT_EQ(handler.GetAllServers("/cookrpc"), partial.servers);
  }
  set_nodes({{"A", ""}, {"B", "address B"}});
  EXPECT_FALSE(handler.GetAllServersSnapshot("/cookrpc").complete);
  set_nodes({{"A", "address A"}, {"B", "address B"}});
  const auto complete = handler.GetAllServersSnapshot("/cookrpc");
  EXPECT_TRUE(complete.complete);
  EXPECT_EQ(complete.servers,
            (std::vector<std::string>{"address A", "address B"}));
  set_nodes({});
  const auto empty = handler.GetAllServersSnapshot("/cookrpc");
  EXPECT_TRUE(empty.complete);
  EXPECT_TRUE(empty.servers.empty());
  {
    std::lock_guard<std::mutex> lock(discovery_data_mutex);
    discovery_children_result = ZCONNECTIONLOSS;
  }
  const auto failed = handler.GetAllServersSnapshot("/cookrpc");
  EXPECT_FALSE(failed.complete);
  EXPECT_TRUE(failed.servers.empty());
}
}  // namespace
}  // namespace cookrpc
