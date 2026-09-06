#include "thread_pool.h"

#include <chrono>
#include <cstddef>
#include <exception>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#include "log_manager.h"

namespace meeting_ctrl {
namespace {

size_t NormalizeThreadCount(size_t count) { return count == 0 ? 1 : count; }

void JoinWorkers(std::vector<std::thread>* workers) {
  for (std::thread& worker : *workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}
}  // namespace

ThreadPool::ThreadPool(size_t threads)
    : current_threads_(0),
      stop_(false),
      state_(ThreadPoolState::kRunning),
      tasks_completed_(0),
      tasks_failed_(0),
      active_threads_(0),
      total_task_time_(0.0) {
  const size_t thread_count = NormalizeThreadCount(threads);
  config_.core_threads = thread_count;
  config_.max_threads = thread_count;
  config_.max_queue_size = thread_count;

  workers_.reserve(config_.max_threads);
  thread_last_active_.resize(config_.max_threads);

  for (size_t i = 0; i < thread_count; ++i) {
    current_threads_.fetch_add(1);
    workers_.emplace_back(&ThreadPool::WorkerThread, this);
    thread_last_active_[i] = std::chrono::steady_clock::now();
  }
}

ThreadPool::ThreadPool(const ThreadPoolStruct& config)
    : current_threads_(0),
      config_(config),
      stop_(false),
      state_(ThreadPoolState::kRunning),
      tasks_completed_(0),
      tasks_failed_(0),
      active_threads_(0),
      total_task_time_(0.0) {
  config_.core_threads = NormalizeThreadCount(config_.core_threads);
  config_.max_threads = NormalizeThreadCount(config_.max_threads);
  if (config_.max_threads < config_.core_threads) {
    config_.max_threads = config_.core_threads;
  }

  workers_.reserve(config_.max_threads);
  thread_last_active_.resize(config_.max_threads);

  for (size_t i = 0; i < config_.core_threads; ++i) {
    current_threads_.fetch_add(1);
    workers_.emplace_back(&ThreadPool::WorkerThread, this);
    thread_last_active_[i] = std::chrono::steady_clock::now();
  }
}

void ThreadPool::WorkerThread() {
  std::ostringstream thread_id_stream;
  thread_id_stream << std::this_thread::get_id();
  const std::string thread_id = thread_id_stream.str();
  while (true) {
    TaskWrapper task;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      const bool signaled =
          condition_.wait_for(lock, config_.keep_alive_time, [this] {
            return stop_ || ((state_ == ThreadPoolState::kRunning ||
                              state_ == ThreadPoolState::kShuttingDown) &&
                             !tasks_.empty());
          });

      // 非核心线程在空闲超时后退出，核心线程始终保留。
      if (!signaled && current_threads_.load() > config_.core_threads) {
        current_threads_.fetch_sub(1);
        LOG_INFO(
            "[ThreadPool] Worker thread {} exited after idle timeout, "
            "remaining threads: {}",
            thread_id, current_threads_.load());
        return;
      }

      if ((stop_ || state_ == ThreadPoolState::kStopped) && tasks_.empty()) {
        return;
      }

      if (state_ == ThreadPoolState::kPaused || tasks_.empty()) {
        continue;
      }

      task = std::move(const_cast<TaskWrapper&>(tasks_.top()));
      tasks_.pop();

      LOG_DEBUG(
          "[ThreadPool] Thread {} picked up task, queue size: {}, "
          "active threads: {}",
          thread_id, tasks_.size(), active_threads_.load());
    }

    if (!task.Valid()) {
      continue;
    }

    const auto start_time = std::chrono::steady_clock::now();
    active_threads_.fetch_add(1);
    bool succeeded = false;

    try {
      task.Execute();
      succeeded = true;
    } catch (const std::exception& error) {
      LOG_ERROR("[ThreadPool] Thread {} task failed: {}", thread_id,
                error.what());
    } catch (...) {
      LOG_ERROR("[ThreadPool] Thread {} task failed with an unknown exception",
                thread_id);
    }

    const auto end_time = std::chrono::steady_clock::now();
    const double duration_ms =
        std::chrono::duration<double, std::milli>(end_time - start_time)
            .count();
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      total_task_time_ += duration_ms;
    }

    if (succeeded) {
      tasks_completed_.fetch_add(1);
    } else {
      tasks_failed_.fetch_add(1);
    }

    active_threads_.fetch_sub(1);

    LOG_DEBUG("[ThreadPool] Thread {} completed task in {} ms", thread_id,
              duration_ms);

    condition_.notify_all();
  }
}

void ThreadPool::AdjustThreadCount() {
  // 调用方持有 queue_mutex_，根据积压任务数按需增加一个工作线程。
  if (current_threads_.load() >= config_.max_threads ||
      tasks_.size() <= current_threads_.load() || stop_ ||
      state_ != ThreadPoolState::kRunning) {
    return;
  }

  current_threads_.fetch_add(1);
  try {
    workers_.emplace_back(&ThreadPool::WorkerThread, this);
  } catch (...) {
    current_threads_.fetch_sub(1);
    throw;
  }
}

void ThreadPool::Pause() {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  if (state_ == ThreadPoolState::kRunning) {
    state_ = ThreadPoolState::kPaused;
    condition_.notify_all();
  }
}

void ThreadPool::Resume() {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  if (state_ == ThreadPoolState::kPaused) {
    state_ = ThreadPoolState::kRunning;
    condition_.notify_all();
  }
}

ThreadPool::Stats ThreadPool::GetStats() const {
  Stats stats;
  stats.tasks_completed = tasks_completed_.load();
  stats.tasks_failed = tasks_failed_.load();
  stats.active_threads = active_threads_.load();
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    const size_t total_tasks = stats.tasks_completed + stats.tasks_failed;
    stats.avg_task_time_ms =
        total_tasks == 0 ? 0.0 : total_task_time_ / total_tasks;
  }
  return stats;
}

size_t ThreadPool::QueueSize() const noexcept {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return tasks_.size();
}

bool ThreadPool::ShutDown(std::chrono::milliseconds wait_timeout_ms) {
  std::unique_lock<std::mutex> lifecycle_lock(lifecycle_mutex_);
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    if (state_ == ThreadPoolState::kStopped) {
      lifecycle_lock.unlock();
      JoinWorkers(&workers_);
      current_threads_.store(0);
      return true;
    }
    state_ = ThreadPoolState::kShuttingDown;
    condition_.notify_all();
  }

  std::unique_lock<std::mutex> lock(queue_mutex_);
  const auto drained = [this] {
    return tasks_.empty() && active_threads_.load() == 0;
  };

  bool completed = drained();
  if (!completed) {
    completed = condition_.wait_for(lock, wait_timeout_ms, drained);
  }

  // 超时后丢弃尚未开始的任务，正在执行的任务仍会被等待至结束。
  stop_ = true;
  if (!completed) {
    while (!tasks_.empty()) {
      tasks_.pop();
    }
  }

  state_ = ThreadPoolState::kStopped;
  condition_.notify_all();
  lock.unlock();

  JoinWorkers(&workers_);
  current_threads_.store(0);
  return completed;
}

void ThreadPool::Stop() {
  std::unique_lock<std::mutex> lifecycle_lock(lifecycle_mutex_);
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    stop_ = true;
    state_ = ThreadPoolState::kStopped;
    while (!tasks_.empty()) {
      tasks_.pop();
    }
    condition_.notify_all();
  }
  JoinWorkers(&workers_);
  current_threads_.store(0);
}

ThreadPoolState ThreadPool::GetState() const noexcept { return state_.load(); }

ThreadPool::~ThreadPool() { Stop(); }
}  // namespace meeting_ctrl
