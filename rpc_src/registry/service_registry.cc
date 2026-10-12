#include "service_registry.h"

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "log_manager.h"

namespace cookrpc {
namespace {
using Clock = std::chrono::steady_clock;
bool IsValidPathSegment(const std::string& value) {
  return !value.empty() && value != "." && value != ".." &&
         value.find('/') == std::string::npos &&
         value.find('\0') == std::string::npos;
}
}  // namespace

struct ServiceRegistry::RecoverySignal {
  std::mutex mutex;
  std::condition_variable cv;
  std::atomic<bool> stopped{false};
  bool changed = false;
};

struct ServiceRegistry::Session {
  zhandle_t* handle = nullptr;
  std::shared_ptr<RecoverySignal> signal;
  std::mutex mutex;
  std::condition_variable cv;
  int state = ZOO_CONNECTING_STATE;
  uint64_t generation = 0;
  explicit Session(std::shared_ptr<RecoverySignal> recovery)
      : signal(std::move(recovery)) {}
  ~Session() {
    if (handle) zookeeper_close(handle);
  }
  bool Connected() {
    std::lock_guard<std::mutex> lock(mutex);
    return state == ZOO_CONNECTED_STATE;
  }
  bool Terminal() {
    std::lock_guard<std::mutex> lock(mutex);
    return state == ZOO_EXPIRED_SESSION_STATE || state == ZOO_AUTH_FAILED_STATE;
  }
};

// 条件变量控制限时取得注册所有权；ZooKeeper 调用期间不持有该互斥量。
struct ServiceRegistry::Registration {
  ServiceRegistry& registry;
  bool acquired = false;
  Registration(ServiceRegistry& owner, Clock::time_point deadline)
      : registry(owner) {
    std::unique_lock<std::mutex> lock(registry.registration_mutex_);
    const auto ready = [&] {
      return !registry.registration_busy_ || registry.signal_->stopped.load();
    };
    if (deadline == Clock::time_point::max())
      registry.registration_cv_.wait(lock, ready);
    else if (!registry.registration_cv_.wait_until(lock, deadline, ready))
      return;
    if (registry.signal_->stopped.load()) return;
    registry.registration_busy_ = acquired = true;
  }
  ~Registration() {
    if (!acquired) return;
    {
      std::lock_guard<std::mutex> lock(registry.registration_mutex_);
      registry.registration_busy_ = false;
    }
    registry.registration_cv_.notify_all();
  }
};

ServiceRegistry::ServiceRegistry(const std::string& zk_hosts)
    : endpoint_(zk_hosts), signal_(std::make_shared<RecoverySignal>()) {
  if (endpoint_.empty())
    throw std::invalid_argument("ZooKeeper hosts must not be empty");
  zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
  session_ = CreateSession();
  if (!session_)
    throw std::runtime_error("Failed to connect to ZooKeeper registry");
  recovery_thread_ = std::thread([this] { Recover(); });
}

std::shared_ptr<ServiceRegistry::Session> ServiceRegistry::CreateSession() {
  const auto session = std::make_shared<Session>(signal_);
  session->handle = zookeeper_init(endpoint_.c_str(), GlobalWatcher, 30000,
                                   nullptr, session.get(), 0);
  if (!session->handle) return nullptr;
  std::unique_lock<std::mutex> lock(session->mutex);
  session->cv.wait_for(lock, std::chrono::seconds(5), [&] {
    return session->state == ZOO_CONNECTED_STATE ||
           session->state == ZOO_EXPIRED_SESSION_STATE ||
           session->state == ZOO_AUTH_FAILED_STATE || signal_->stopped.load();
  });
  return session->state == ZOO_CONNECTED_STATE && !signal_->stopped.load()
             ? session
             : nullptr;
}

ServiceRegistry::~ServiceRegistry() {
  {
    std::lock_guard<std::mutex> lock(signal_->mutex);
    signal_->stopped.store(true);
  }
  signal_->cv.notify_all();
  {
    std::lock_guard<std::mutex> lock(registration_mutex_);
    registration_cv_.notify_all();
  }
  const auto session = Snapshot();
  if (session) {
    std::lock_guard<std::mutex> lock(session->mutex);
    session->cv.notify_all();
  }
  recovery_thread_.join();
  // 关闭会话时不持有 watcher 需要的锁，上下文随 Session 一起释放。
  session_.reset();
}

std::shared_ptr<ServiceRegistry::Session> ServiceRegistry::Snapshot() const {
  std::lock_guard<std::mutex> lock(session_mutex_);
  return session_;
}
bool ServiceRegistry::IsConnected() const {
  const auto session = Snapshot();
  return session && session->Connected();
}

void ServiceRegistry::GlobalWatcher(zhandle_t*, int type, int state,
                                    const char*, void* context) {
  if (!context) return;
  auto* session = static_cast<Session*>(context);
  {
    std::lock_guard<std::mutex> lock(session->mutex);
    if (type == ZOO_SESSION_EVENT) session->state = state;
    ++session->generation;
  }
  session->cv.notify_all();
  {
    std::lock_guard<std::mutex> lock(session->signal->mutex);
    session->signal->changed = true;
  }
  session->signal->cv.notify_all();
}

bool ServiceRegistry::EnsurePath(const std::shared_ptr<Session>& session,
                                 const std::string& path,
                                 Clock::time_point deadline) {
  if (path == "/") return true;
  const auto separator = path.find_last_of('/');
  if (separator > 0 &&
      !EnsurePath(session, path.substr(0, separator), deadline))
    return false;
  while (Clock::now() < deadline && session->Connected() &&
         !signal_->stopped.load()) {
    int rc = zoo_create(session->handle, path.c_str(), "", 0,
                        &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
    if (rc == ZOK) return true;
    if (rc == ZNONODE && separator > 0) {
      if (!EnsurePath(session, path.substr(0, separator), deadline))
        return false;
      continue;
    }
    if (rc != ZNODEEXISTS) return false;
    Stat stat{};
    rc = zoo_exists(session->handle, path.c_str(), 0, &stat);
    if (rc == ZNONODE) continue;
    if (rc == ZOK && stat.ephemeralOwner == 0) return true;
    LOG_ERROR("Registry parent path is not persistent: {}", path);
    return false;
  }
  return false;
}

bool ServiceRegistry::RegisterOnSession(const std::shared_ptr<Session>& session,
                                        const std::string& service_name,
                                        const std::string& address,
                                        Clock::time_point deadline) {
  const std::string parent = std::string(kRootPath) + "/" + service_name;
  if (!EnsurePath(session, parent, deadline)) return false;
  const std::string path = parent + "/" + address;
  while (Clock::now() < deadline && session->Connected() &&
         !signal_->stopped.load()) {
    int rc = zoo_create(session->handle, path.c_str(), address.data(),
                        static_cast<int>(address.size()), &ZOO_OPEN_ACL_UNSAFE,
                        ZOO_EPHEMERAL, nullptr, 0);
    if (rc == ZOK) return true;
    if (rc == ZNONODE) {
      if (!EnsurePath(session, parent, deadline)) return false;
      continue;
    }
    if (rc != ZNODEEXISTS) return false;

    uint64_t observed;
    {
      std::lock_guard<std::mutex> lock(session->mutex);
      observed = session->generation;
    }
    Stat stat{};
    // 使用稳定的 Session 上下文，期限结束后遗留的 watch 也不会访问栈内存。
    rc = zoo_wexists(session->handle, path.c_str(), GlobalWatcher,
                     session.get(), &stat);
    if (rc == ZNONODE) continue;
    if (rc != ZOK) return false;
    if (stat.ephemeralOwner == 0) {
      LOG_ERROR("Persistent node occupies registry instance path: {}", path);
      return false;
    }
    const clientid_t* id = zoo_client_id(session->handle);
    if (!id || id->client_id == 0) return false;
    if (stat.ephemeralOwner == id->client_id) {
      std::vector<char> data(address.size() + 1);
      int length = static_cast<int>(data.size());
      rc = zoo_get(session->handle, path.c_str(), 0, data.data(), &length,
                   &stat);
      if (rc == ZNONODE) continue;
      if (rc != ZOK) return false;
      // get 返回的 Stat 再次验证所有权，覆盖 exists/get 之间的节点重建竞争。
      if (stat.ephemeralOwner == id->client_id) {
        return stat.dataLength == static_cast<int>(address.size()) &&
               length == static_cast<int>(address.size()) &&
               std::memcmp(data.data(), address.data(), address.size()) == 0;
      }
      if (stat.ephemeralOwner == 0) return false;
    }
    std::unique_lock<std::mutex> lock(session->mutex);
    session->cv.wait_until(lock, deadline, [&] {
      return session->generation != observed ||
             session->state != ZOO_CONNECTED_STATE || signal_->stopped.load();
    });
    // 标准 watch 是一次性的：每次变化后重新检查、设置 watch、竞争创建。
  }
  LOG_WARN("Timed out acquiring registry instance ownership: {}", path);
  return false;
}

bool ServiceRegistry::RegisterService(const std::string& name,
                                      const std::string& address,
                                      std::chrono::milliseconds timeout) {
  if (!IsValidPathSegment(name) || !IsValidPathSegment(address) ||
      address.size() > 1024 || timeout <= std::chrono::milliseconds::zero() ||
      timeout > std::chrono::seconds(60))
    return false;
  const auto deadline = Clock::now() + timeout;
  Registration registration(*this, deadline);
  if (!registration.acquired) return false;
  const auto session = Snapshot();
  if (!session || !session->Connected() ||
      !RegisterOnSession(session, name, address, deadline))
    return false;
  registrations_[name + "/" + address] = {name, address};
  return true;
}

void ServiceRegistry::Recover() {
  bool retry = false;
  while (!signal_->stopped.load()) {
    {
      std::unique_lock<std::mutex> lock(signal_->mutex);
      const auto changed = [&] {
        return signal_->changed || signal_->stopped.load();
      };
      if (retry)
        signal_->cv.wait_for(lock, std::chrono::milliseconds(100),
                             [&] { return signal_->stopped.load(); });
      else
        signal_->cv.wait(lock, changed);
      signal_->changed = false;
    }
    if (signal_->stopped.load()) break;
    try {
      // 旧会话的最后一个快照在注册锁释放后才关闭。
      auto session = Snapshot();
      std::shared_ptr<Session> retired;
      Registration registration(*this, Clock::time_point::max());
      if (!registration.acquired) break;
      if (signal_->stopped.load()) break;
      if (session && session->Terminal()) {
        auto replacement = CreateSession();
        if (!replacement) {
          retry = true;
          continue;
        }
        {
          std::lock_guard<std::mutex> session_lock(session_mutex_);
          session_.swap(replacement);
        }
        retired = std::move(replacement);
        session = Snapshot();
        needs_restore_ = true;
      }
      retry = !session || !session->Connected();
      if (retry || !needs_restore_) continue;
      needs_restore_ = false;
      for (const auto& registration : registrations_) {
        if (!RegisterOnSession(session, registration.second.first,
                               registration.second.second,
                               Clock::now() + std::chrono::seconds(3)))
          needs_restore_ = true;
      }
      retry = needs_restore_;
    } catch (const std::exception& error) {
      LOG_ERROR("Registry recovery failed: {}", error.what());
      retry = true;
    }
  }
}
}  // namespace cookrpc
