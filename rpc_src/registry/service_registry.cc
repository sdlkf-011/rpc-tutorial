#include "service_registry.h"

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <stdexcept>

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
  bool retired = false;
  explicit Session(std::shared_ptr<RecoverySignal> recovery)
      : signal(std::move(recovery)) {}
  ~Session() {
    if (handle) zookeeper_close(handle);
  }
  bool Connected() {
    std::lock_guard<std::mutex> lock(mutex);
    return !retired && state == ZOO_CONNECTED_STATE;
  }
  bool Terminal() {
    std::lock_guard<std::mutex> lock(mutex);
    return retired || state == ZOO_EXPIRED_SESSION_STATE ||
           state == ZOO_AUTH_FAILED_STATE;
  }
  void Retire() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      retired = true;
    }
    cv.notify_all();
    {
      std::lock_guard<std::mutex> lock(signal->mutex);
      signal->changed = true;
    }
    signal->cv.notify_all();
  }
};

// SDK completion owns a shared_ptr until completion (including close
// cancellation). It only publishes results: no Session/Registry reference and
// no callback-side I/O.
struct ServiceRegistry::Operation {
  explicit Operation(std::shared_ptr<RecoverySignal> recovery)
      : signal(std::move(recovery)) {}
  std::shared_ptr<RecoverySignal> signal;
  bool done = false;  // all result fields are protected by signal->mutex
  int rc = ZSYSTEMERROR;
  Stat stat{};
  std::array<char, 1024> data{};
  int length = 0;
};

void ServiceRegistry::CompleteData(int rc, const char* data, int length,
                                   const Stat* stat, const void* context) {
  std::unique_ptr<std::shared_ptr<Operation>> owner(
      static_cast<std::shared_ptr<Operation>*>(const_cast<void*>(context)));
  const auto operation = *owner;
  {
    std::lock_guard<std::mutex> lock(operation->signal->mutex);
    operation->rc = rc;
    if (stat) operation->stat = *stat;
    if (rc == ZOK && data) {
      if (length < 0 || length > static_cast<int>(operation->data.size())) {
        operation->rc = ZMARSHALLINGERROR;
      } else {
        operation->length = length;
        std::memcpy(operation->data.data(), data, static_cast<size_t>(length));
      }
    }
    operation->done = true;
  }
  operation->signal->cv.notify_all();
}
void ServiceRegistry::CompleteString(int rc, const char*, const void* context) {
  CompleteData(rc, nullptr, 0, nullptr, context);
}
void ServiceRegistry::CompleteStat(int rc, const Stat* stat,
                                   const void* context) {
  CompleteData(rc, nullptr, 0, stat, context);
}

std::shared_ptr<ServiceRegistry::Operation> ServiceRegistry::Execute(
    const std::shared_ptr<Session>& session, OperationKind kind,
    const std::string& path, const std::string& value, int flags,
    Clock::time_point deadline) {
  if (Clock::now() >= deadline || signal_->stopped.load() ||
      !session->Connected())
    return nullptr;
  auto operation = std::make_shared<Operation>(signal_);
  auto* context = new std::shared_ptr<Operation>(operation);
  int rc = ZBADARGUMENTS;
  switch (kind) {
    case OperationKind::kCreate:
      rc = zoo_acreate(session->handle, path.c_str(), value.data(),
                       static_cast<int>(value.size()), &ZOO_OPEN_ACL_UNSAFE,
                       flags, CompleteString, context);
      break;
    case OperationKind::kExists:
      rc = zoo_aexists(session->handle, path.c_str(), 0, CompleteStat, context);
      break;
    case OperationKind::kWatchExists:
      rc = zoo_awexists(session->handle, path.c_str(), GlobalWatcher,
                        session.get(), CompleteStat, context);
      break;
    case OperationKind::kGet:
      rc = zoo_aget(session->handle, path.c_str(), 0, CompleteData, context);
      break;
  }
  if (rc != ZOK) {
    // An enqueue failure has no completion callback.
    delete context;
    operation->rc = rc;
    operation->done = true;
  }
  std::unique_lock<std::mutex> lock(signal_->mutex);
  const bool finished = signal_->cv.wait_until(lock, deadline, [&] {
    return operation->done || signal_->stopped.load();
  });
  const bool uncertain =
      !finished || !operation->done || Clock::now() >= deadline ||
      operation->rc == ZCONNECTIONLOSS || operation->rc == ZOPERATIONTIMEOUT;
  if (uncertain || signal_->stopped.load()) {
    lock.unlock();
    // Timeout cannot cancel a create. Retire the whole session in recovery,
    // clearing any uncommitted ephemeral nodes without blocking this caller.
    session->Retire();
    return nullptr;
  }
  return operation;
}

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
    auto result =
        Execute(session, OperationKind::kCreate, path, "", 0, deadline);
    if (!result) return false;
    int rc = result->rc;
    if (rc == ZOK) return true;
    if (rc == ZNONODE && separator > 0) {
      if (!EnsurePath(session, path.substr(0, separator), deadline))
        return false;
      continue;
    }
    if (rc != ZNODEEXISTS) return false;
    Stat stat{};
    result = Execute(session, OperationKind::kExists, path, "", 0, deadline);
    if (!result) return false;
    rc = result->rc;
    stat = result->stat;
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
    auto result = Execute(session, OperationKind::kCreate, path, address,
                          ZOO_EPHEMERAL, deadline);
    if (!result) return false;
    int rc = result->rc;
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
    result =
        Execute(session, OperationKind::kWatchExists, path, "", 0, deadline);
    if (!result) return false;
    rc = result->rc;
    stat = result->stat;
    if (rc == ZNONODE) continue;
    if (rc != ZOK) return false;
    if (stat.ephemeralOwner == 0) {
      LOG_ERROR("Persistent node occupies registry instance path: {}", path);
      return false;
    }
    const clientid_t* id = zoo_client_id(session->handle);
    if (!id || id->client_id == 0) return false;
    if (stat.ephemeralOwner == id->client_id) {
      result = Execute(session, OperationKind::kGet, path, "", 0, deadline);
      if (!result) return false;
      rc = result->rc;
      stat = result->stat;
      if (rc == ZNONODE) continue;
      if (rc != ZOK) return false;
      // get 返回的 Stat 再次验证所有权，覆盖 exists/get 之间的节点重建竞争。
      if (stat.ephemeralOwner == id->client_id) {
        return stat.dataLength == static_cast<int>(address.size()) &&
               result->length == static_cast<int>(address.size()) &&
               std::memcmp(result->data.data(), address.data(),
                           address.size()) == 0;
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
  if (!session || !session->Connected()) return false;
  // Allocate the recovery entry before issuing any create. Failed attempts are
  // removed, so only confirmed registrations are replayed by recovery.
  const std::string key = name + "/" + address;
  bool inserted = false;
  try {
    inserted = registrations_.try_emplace(key, name, address).second;
    if (RegisterOnSession(session, name, address, deadline)) {
      if (Clock::now() < deadline) return true;
      session->Retire();
    }
  } catch (const std::exception& error) {
    session->Retire();
    LOG_ERROR("Failed to register service: {}", error.what());
  }
  if (inserted) registrations_.erase(key);
  return false;
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
      {
        Registration registration(*this, Clock::time_point::max());
        if (!registration.acquired || signal_->stopped.load()) break;
        session = Snapshot();
        if (session && session->Terminal()) {
          {
            std::lock_guard<std::mutex> session_lock(session_mutex_);
            retired = std::move(session_);
          }
          session.reset();
          needs_restore_ = true;
        }
      }
      // Closing is outside registration/watch locks, and precedes replay:
      // a retired (but unexpired) session may still own the instance paths.
      retired.reset();
      Registration registration(*this, Clock::time_point::max());
      if (!registration.acquired || signal_->stopped.load()) break;
      session = Snapshot();
      if (!session) {
        auto replacement = CreateSession();
        if (!replacement) {
          retry = true;
          continue;
        }
        {
          std::lock_guard<std::mutex> session_lock(session_mutex_);
          session_ = replacement;
        }
        session = std::move(replacement);
      }
      retry = !session->Connected();
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
