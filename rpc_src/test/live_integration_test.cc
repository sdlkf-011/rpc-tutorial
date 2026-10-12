// 由 live_test.py 驱动：真实 TCP、ZooKeeper、网络隔离、重启，无 C API 替换。
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <thread>

#include "connection.h"
#include "message_cycle.h"
#include "rpc_client.h"
#include "service_manager.h"
#include "service_registry.h"
#include "thread_pool_singleton.h"

namespace {
using namespace std::chrono_literals;
void Require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}
struct Observer {
  std::string endpoint;
  zhandle_t* handle = nullptr;
  std::mutex mutex;
  std::condition_variable cv;
  bool connected = false;
  static void Watch(zhandle_t*, int type, int state, const char*,
                    void* context) {
    if (type != ZOO_SESSION_EVENT) return;
    auto* self = static_cast<Observer*>(context);
    std::lock_guard<std::mutex> lock(self->mutex);
    self->connected = state == ZOO_CONNECTED_STATE;
    self->cv.notify_all();
  }
  explicit Observer(std::string host) : endpoint(std::move(host)) { Connect(); }
  void Connect() {
    if (handle) zookeeper_close(handle);
    {
      std::lock_guard<std::mutex> lock(mutex);
      connected = false;
    }
    handle = zookeeper_init(endpoint.c_str(), Watch, 30000, nullptr, this, 0);
    Require(handle != nullptr, "observer initialization failed");
    std::unique_lock<std::mutex> lock(mutex);
    Require(cv.wait_for(lock, 5s, [&] { return connected; }),
            "observer connection failed");
  }
  int Exists(const std::string& path, Stat& stat) {
    bool live;
    {
      std::lock_guard<std::mutex> lock(mutex);
      live = connected;
    }
    if (!live) Connect();
    int rc = zoo_exists(handle, path.c_str(), 0, &stat);
    if (rc == ZINVALIDSTATE || rc == ZSESSIONEXPIRED) {
      Connect();
      rc = zoo_exists(handle, path.c_str(), 0, &stat);
    }
    return rc;
  }
  ~Observer() {
    if (handle) zookeeper_close(handle);
  }
};
class EchoService : public cookrpc::Service {
 public:
  std::string name;
  explicit EchoService(std::string value) : name(std::move(value)) {}
  std::string GetServiceName() const override { return name; }
  bool HandleRequest(const std::string& method, const std::string& args,
                     std::string& result) override {
    result = method == "large"
                 ? std::string(cookrpc::RpcHeader::kMaxBodySize - 12, 'x')
                 : args;
    return true;
  }
};
struct Server {
  uint16_t port = 0;
  int listener = -1;
  std::unique_ptr<cookrpc::MessageCycle> cycle;
  std::thread loop;
  explicit Server(uint16_t desired = 0) { Start(desired); }
  void Start(uint16_t desired) {
    listener = socket(AF_INET, SOCK_STREAM, 0);
    Require(listener >= 0, "TCP socket failed");
    const int enabled = 1, small = 4096;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
    setsockopt(listener, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(desired);
    Require(bind(listener, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) == 0,
            "TCP bind failed");
    Require(listen(listener, 32) == 0, "TCP listen failed");
    socklen_t size = sizeof(address);
    getsockname(listener, reinterpret_cast<sockaddr*>(&address), &size);
    port = ntohs(address.sin_port);
    Require(meeting_ctrl::ThreadPoolSingleton::Init(1),
            "thread pool init failed");
    cycle = std::make_unique<cookrpc::MessageCycle>(nullptr);
    Require(cycle->AddListenFd(listener), "listen registration failed");
    loop = std::thread([this] { cycle->Loop(); });
  }
  void Stop() {
    if (cycle) cycle->Stop();
    if (loop.joinable()) loop.join();
    cycle.reset();
    meeting_ctrl::ThreadPoolSingleton::Destroy();
    if (listener >= 0) close(std::exchange(listener, -1));
  }
  void Restart() {
    const auto previous = port;
    Stop();
    Start(previous);
  }
  ~Server() { Stop(); }
};
void Report(const std::string& event, int64_t owner = 0) {
  std::cout << "@rpc-live "
            << nlohmann::json{{"event", event}, {"owner", owner}}.dump()
            << std::endl;
}
template <typename Predicate>
void Await(Predicate predicate, const char* message) {
  const auto deadline = std::chrono::steady_clock::now() + 15s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return;
    std::this_thread::sleep_for(30ms);
  }
  throw std::runtime_error(message);
}
}  // namespace
int main(int argc, char** argv) {
  try {
    Require(argc == 4,
            "expected ZooKeeper endpoint, proxy endpoint, and work directory");
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    const std::string endpoint = argv[1], proxy = argv[2];
    const auto work = std::filesystem::path(argv[3]);
    const auto name = "LiveService-" + std::to_string(getpid());
    Require(cookrpc::ServiceManager::GetInstance().RegisterService(
                std::make_shared<EchoService>(name)),
            "service init failed");
    Server server;
    const std::string address = "127.0.0.1:" + std::to_string(server.port);
    const std::string space = "/cookrpc/" + name, path = space + "/" + address;
    auto registry = std::make_unique<cookrpc::ServiceRegistry>(proxy);
    Require(registry->RegisterService(name, address),
            "initial service registration failed");
    Observer observer(endpoint);
    Stat stat{};
    Require(observer.Exists(path, stat) == ZOK && stat.ephemeralOwner != 0,
            "initial ephemeral node absent");
    int64_t owner = stat.ephemeralOwner;
    const auto config = work / "client.json";
    {
      std::ofstream out(config);
      out << nlohmann::json{
          {"zk_host", "127.0.0.1"},
          {"zk_port", std::stoi(endpoint.substr(endpoint.find(':') + 1))},
          {"zk_namespace", space},
          {"zk_retry_interval", 1},
          {"timeout_ms", 1000},
          {"retry_times", 0}};
    }
    const auto echo = [&] {
      cookrpc::RpcClient client(config.string());
      Require(client.Connect(), "service not discoverable");
      std::string response;
      Require(client.Call(name, "Echo", std::string("real TCP and ZooKeeper"),
                          cookrpc::SerializeType::JSON, response),
              "real RPC call failed");
      Require(response == "real TCP and ZooKeeper", "RPC result mismatch");
    };
    Report("ready", owner);
    std::string command;
    while (std::getline(std::cin, command)) {
      if (command == "tcp") {
        auto slow_fd = socket(AF_INET, SOCK_STREAM, 0);
        Require(slow_fd >= 0, "slow TCP socket failed");
        const int small = 4096;
        setsockopt(slow_fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(server.port);
        Require(connect(slow_fd, reinterpret_cast<sockaddr*>(&address),
                        sizeof(address)) == 0,
                "slow connect failed");
        auto slow = std::make_shared<cookrpc::Connection>(slow_fd);
        cookrpc::RpcRequest request;
        request.SetServiceName(name);
        request.SetMethodName("large");
        request.SetSequenceId(1);
        std::string frame;
        const auto completed =
            meeting_ctrl::ThreadPoolSingleton::GetStats().tasks_completed;
        Require(request.Serialize(frame) && slow->Write(frame),
                "slow request failed");
        Await(
            [&] {
              return meeting_ctrl::ThreadPoolSingleton::GetStats()
                         .tasks_completed > completed;
            },
            "slow reader occupied only worker");
        const auto start = std::chrono::steady_clock::now();
        echo();
        Require(std::chrono::steady_clock::now() - start < 500ms,
                "slow reader blocked fast connection");
        Report("tcp");
      } else if (command == "absent") {
        Await([&] { return observer.Exists(path, stat) == ZNONODE; },
              "isolated session did not expire");
        Report("absent", owner);
      } else if (command == "recovered" || command == "available") {
        Await(
            [&] {
              return registry->IsConnected() &&
                     observer.Exists(path, stat) == ZOK &&
                     (command != "recovered" || stat.ephemeralOwner != owner);
            },
            "service registration did not recover");
        Require(stat.ephemeralOwner != 0, "instance became persistent");
        owner = stat.ephemeralOwner;
        echo();
        Report(command, owner);
      } else if (command == "restart-tcp") {
        registry.reset();
        server.Restart();
        registry = std::make_unique<cookrpc::ServiceRegistry>(proxy);
        Require(registry->RegisterService(name, address),
                "RPC restart registration failed");
        Require(
            observer.Exists(path, stat) == ZOK && stat.ephemeralOwner != owner,
            "RPC restart did not own new node");
        owner = stat.ephemeralOwner;
        echo();
        Report(command, owner);
      } else if (command == "stop") {
        Report("stopped");
        break;
      } else
        throw std::runtime_error("unknown live-test command");
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "LIVE FAILURE: " << error.what() << '\n';
    return 1;
  }
}
