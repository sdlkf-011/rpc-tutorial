#include "thread_pool_singleton.h"

namespace meeting_ctrl {
std::mutex ThreadPoolSingleton::mutex_;
std::shared_ptr<ThreadPool> ThreadPoolSingleton::instance_;
bool ThreadPoolSingleton::destroying_ = false;

bool ThreadPoolSingleton::Init(size_t threads) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ || destroying_) {
    return false;
  }
  instance_ = std::make_shared<ThreadPool>(threads);
  return true;
}

bool ThreadPoolSingleton::Init(const ThreadPoolStruct& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ || destroying_) {
    return false;
  }
  instance_ = std::make_shared<ThreadPool>(config);
  return true;
}

std::shared_ptr<ThreadPool> ThreadPoolSingleton::GetInstance() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (destroying_) {
    return nullptr;
  }
  if (!instance_) {
    instance_ = std::make_shared<ThreadPool>();
  }
  return instance_;
}

std::shared_ptr<ThreadPool> ThreadPoolSingleton::GetInstance(
    const ThreadPoolStruct& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (destroying_) {
    return nullptr;
  }
  if (!instance_) {
    instance_ = std::make_shared<ThreadPool>(config);
  }
  return instance_;
}

std::shared_ptr<ThreadPool> ThreadPoolSingleton::Snapshot() {
  std::lock_guard<std::mutex> lock(mutex_);
  // 销毁期间保留旧池供统计接口访问，不触发惰性初始化。
  return instance_;
}

ThreadPool::Stats ThreadPoolSingleton::GetStats() {
  const auto pool = Snapshot();
  return pool ? pool->GetStats() : ThreadPool::Stats{};
}
size_t ThreadPoolSingleton::GetQueueSize() {
  const auto pool = Snapshot();
  return pool ? pool->QueueSize() : 0;
}
size_t ThreadPoolSingleton::GetPoolSize() {
  const auto pool = Snapshot();
  return pool ? pool->Size() : 0;
}
bool ThreadPoolSingleton::Pause() {
  const auto pool = Snapshot();
  return pool && pool->Pause();
}
bool ThreadPoolSingleton::Resume() {
  const auto pool = Snapshot();
  return pool && pool->Resume();
}
bool ThreadPoolSingleton::RequestShutdown() {
  const auto pool = Snapshot();
  if (!pool) {
    return false;
  }
  pool->RequestShutdown();
  return true;
}
bool ThreadPoolSingleton::WaitForTermination(
    std::chrono::milliseconds timeout) {
  const auto pool = Snapshot();
  return pool && pool->WaitForTermination(timeout);
}
bool ThreadPoolSingleton::Shutdown(std::chrono::milliseconds timeout) {
  const auto pool = Snapshot();
  return pool && pool->ShutDown(timeout);
}

bool ThreadPoolSingleton::Destroy() {
  const auto pool = Snapshot();
  if (pool && pool->IsWorkerThread()) {
    // 当前任务不能等待自己，且不能移除全局所有者后留下自析构风险。
    return false;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (destroying_ || pool != instance_) {
      return false;
    }
    if (!pool) {
      return true;
    }
    destroying_ = true;
  }

  // 不持有全局锁，关闭中的任务可照常读取统计信息。
  pool->RequestShutdown();
  pool->WaitForTermination();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    instance_.reset();  // 局部快照保证对象在解锁后才可能析构。
    destroying_ = false;
  }
  return true;
}

bool ThreadPoolSingleton::Exists() { return Snapshot() != nullptr; }
ThreadPoolState ThreadPoolSingleton::GetState() {
  const auto pool = Snapshot();
  return pool ? pool->GetState() : ThreadPoolState::kStopped;
}
}  // namespace meeting_ctrl
