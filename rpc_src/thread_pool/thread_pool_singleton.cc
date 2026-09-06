#include "thread_pool_singleton.h"

#include <memory>
#include <mutex>

namespace meeting_ctrl {

std::unique_ptr<ThreadPool> ThreadPoolSingleton::instance_;
std::mutex ThreadPoolSingleton::mutex_;

bool ThreadPoolSingleton::Init(size_t threads) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ != nullptr) {
    return false;
  }
  instance_ = std::make_unique<ThreadPool>(threads);
  return true;
}

bool ThreadPoolSingleton::Init(const ThreadPoolStruct& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ != nullptr) {
    return false;
  }
  instance_ = std::make_unique<ThreadPool>(config);
  return true;
}

ThreadPool& ThreadPoolSingleton::GetInstance() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ == nullptr) {
    instance_ = std::make_unique<ThreadPool>();
  }
  return *instance_;
}

ThreadPool& ThreadPoolSingleton::GetInstance(const ThreadPoolStruct& config) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ == nullptr) {
    instance_ = std::make_unique<ThreadPool>(config);
  }
  return *instance_;
}

ThreadPool::Stats ThreadPoolSingleton::GetStats() {
  std::lock_guard<std::mutex> lock(mutex_);
  return instance_ == nullptr ? ThreadPool::Stats{} : instance_->GetStats();
}

size_t ThreadPoolSingleton::GetQueueSize() {
  std::lock_guard<std::mutex> lock(mutex_);
  return instance_ == nullptr ? 0 : instance_->QueueSize();
}

size_t ThreadPoolSingleton::GetPoolSize() {
  std::lock_guard<std::mutex> lock(mutex_);
  return instance_ == nullptr ? 0 : instance_->Size();
}

bool ThreadPoolSingleton::Pause() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ == nullptr ||
      instance_->GetState() != ThreadPoolState::kRunning) {
    return false;
  }
  instance_->Pause();
  return true;
}

bool ThreadPoolSingleton::Resume() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ == nullptr ||
      instance_->GetState() != ThreadPoolState::kPaused) {
    return false;
  }
  instance_->Resume();
  return true;
}

bool ThreadPoolSingleton::Shutdown(std::chrono::milliseconds timeout) {
  std::lock_guard<std::mutex> lock(mutex_);
  return instance_ != nullptr && instance_->ShutDown(timeout);
}

void ThreadPoolSingleton::Destroy() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (instance_ != nullptr) {
    instance_->Stop();
    instance_.reset();
  }
}

bool ThreadPoolSingleton::Exists() {
  std::lock_guard<std::mutex> lock(mutex_);
  return instance_ != nullptr;
}

ThreadPoolState ThreadPoolSingleton::GetState() {
  std::lock_guard<std::mutex> lock(mutex_);
  return instance_ == nullptr ? ThreadPoolState::kStopped
                              : instance_->GetState();
}
}  // namespace meeting_ctrl
