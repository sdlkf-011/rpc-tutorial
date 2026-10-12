#include "thread_pool.h"

#include <algorithm>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "log_manager.h"

namespace meeting_ctrl {
namespace {
using Clock = std::chrono::steady_clock;

// 先比较可表示的剩余时间，避免将巨大毫秒值转换成纳秒或相加时溢出。
Clock::time_point Deadline(std::chrono::milliseconds timeout) {
  const auto now = Clock::now();
  if (timeout <= std::chrono::milliseconds::zero()) {
    return now;
  }
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      Clock::time_point::max() - now);
  return timeout >= remaining ? Clock::time_point::max() : now + timeout;
}

template <typename Predicate>
bool Wait(std::condition_variable& cv, std::unique_lock<std::mutex>& lock,
          std::chrono::milliseconds timeout, Predicate predicate) {
  if (timeout == std::chrono::milliseconds::max()) {
    cv.wait(lock, predicate);
    return true;
  }
  return cv.wait_until(lock, Deadline(timeout), predicate);
}

ThreadPoolStruct FixedConfig(size_t threads) {
  ThreadPoolStruct config;
  config.core_threads = std::max(size_t{1}, threads);
  config.max_threads = config.core_threads;
  config.max_queue_size = config.core_threads;
  return config;
}
}  // namespace

struct ThreadPool::Worker {
  std::thread thread;
  bool exited = false;  // 在 queue_mutex 下读写，所有权转移后由回收者持有。
};

struct ThreadPool::State {
  explicit State(ThreadPoolStruct settings) : config(settings) {
    config.core_threads = std::max(size_t{1}, config.core_threads);
    config.max_threads = std::max(config.core_threads, config.max_threads);
    config.keep_alive_time =
        std::max(std::chrono::milliseconds{1}, config.keep_alive_time);
    workers.reserve(config.max_threads);
  }

  ThreadPoolStruct config;
  mutable std::mutex queue_mutex;
  std::condition_variable condition;
  std::vector<std::unique_ptr<Worker>> workers;
  size_t reaping_workers = 0;
  size_t current_threads = 0;
  size_t active_threads = 0;
  ThreadPoolState status = ThreadPoolState::kRunning;
  std::priority_queue<TaskWrapper> tasks;

  mutable std::mutex stats_mutex;
  size_t tasks_completed = 0;
  size_t tasks_failed = 0;
  size_t timed_tasks = 0;
  double total_task_time = 0;
};

thread_local const ThreadPool::State* ThreadPool::worker_state_ = nullptr;

ThreadPool::TaskResult::~TaskResult() {
  std::lock_guard<std::mutex> lock(state->stats_mutex);
  if (succeeded) {
    ++state->tasks_completed;
  } else {
    ++state->tasks_failed;
  }
}

ThreadPool::ThreadPool(size_t threads) : ThreadPool(FixedConfig(threads)) {}

ThreadPool::ThreadPool(const ThreadPoolStruct& config)
    : state_(std::make_shared<State>(config)) {
  try {
    std::lock_guard<std::mutex> lock(state_->queue_mutex);
    for (size_t i = 0; i < state_->config.core_threads; ++i) {
      AddWorkerLocked(state_);
    }
  } catch (...) {
    // 已创建的线程必须退出并回收后，才能继续传播构造失败。
    RequestShutdown();
    WaitForTermination();
    throw;
  }
}

void ThreadPool::AddWorkerLocked(const std::shared_ptr<State>& state) {
  auto worker = std::make_unique<Worker>();
  Worker* raw = worker.get();
  state->workers.push_back(std::move(worker));
  ++state->current_threads;
  try {
    raw->thread = std::thread([state, raw] { WorkerThread(state, raw); });
  } catch (...) {
    --state->current_threads;
    state->workers.pop_back();
    throw;
  }
}

void ThreadPool::AdjustThreadCountLocked(const std::shared_ptr<State>& state) {
  // 将正在回收的句柄计入上限，避免并发提交时退出线程积累无界。
  if (state->status != ThreadPoolState::kRunning ||
      state->workers.size() + state->reaping_workers >=
          state->config.max_threads ||
      state->tasks.size() <= state->current_threads - state->active_threads) {
    return;
  }
  try {
    AddWorkerLocked(state);
  } catch (const std::exception& error) {
    // 已接收的任务仍由现有 worker 执行，不能在入队后将它报告为提交失败。
    LOG_ERROR("[ThreadPool] Failed to add worker: {}", error.what());
  }
}

void ThreadPool::JoinRetired(const std::shared_ptr<State>& state,
                             std::vector<std::unique_ptr<Worker>> workers) {
  // 每个句柄只由锁内移出它的调用者负责 join，期间不持有队列锁。
  for (auto& worker : workers) {
    worker->thread.join();
  }
  const size_t count = workers.size();
  workers.clear();
  {
    std::lock_guard<std::mutex> lock(state->queue_mutex);
    state->reaping_workers -= count;
    if (state->status == ThreadPoolState::kShuttingDown &&
        state->current_threads == 0 && state->workers.empty() &&
        state->reaping_workers == 0) {
      state->status = ThreadPoolState::kStopped;
    }
  }
  state->condition.notify_all();
}

bool ThreadPool::SubmitTask(TaskWrapper task) {
  const auto state = state_;
  std::vector<std::unique_ptr<Worker>> retired;
  bool accepted = false;
  try {
    std::lock_guard<std::mutex> lock(state->queue_mutex);
    // 回收和关闭在同一把锁内转移 worker 所有权。
    const auto exited =
        std::count_if(state->workers.begin(), state->workers.end(),
                      [](const auto& worker) { return worker->exited; });
    // 分配失败发生在所有权移动之前，不会析构 joinable 的线程。
    retired.reserve(exited);
    for (auto it = state->workers.begin(); it != state->workers.end();) {
      if ((*it)->exited) {
        retired.push_back(std::move(*it));
        it = state->workers.erase(it);
      } else {
        ++it;
      }
    }
    state->reaping_workers += retired.size();
    if ((state->status == ThreadPoolState::kRunning ||
         state->status == ThreadPoolState::kPaused) &&
        (state->config.max_queue_size == 0 ||
         state->tasks.size() < state->config.max_queue_size)) {
      state->tasks.push(std::move(task));
      accepted = true;
      AdjustThreadCountLocked(state);
    }
  } catch (...) {
    JoinRetired(state, std::move(retired));
    throw;
  }
  state->condition.notify_all();
  JoinRetired(state, std::move(retired));
  if (accepted) {
    std::lock_guard<std::mutex> lock(state->queue_mutex);
    AdjustThreadCountLocked(state);
  }
  return accepted;
}

void ThreadPool::WorkerThread(const std::shared_ptr<State>& state,
                              Worker* worker) {
  worker_state_ = state.get();
  for (;;) {
    TaskWrapper task;
    {
      std::unique_lock<std::mutex> lock(state->queue_mutex);
      const bool signaled =
          Wait(state->condition, lock, state->config.keep_alive_time, [&] {
            return state->status == ThreadPoolState::kShuttingDown ||
                   state->status == ThreadPoolState::kStopped ||
                   (state->status == ThreadPoolState::kRunning &&
                    !state->tasks.empty());
          });
      const bool closing = state->status == ThreadPoolState::kShuttingDown ||
                           state->status == ThreadPoolState::kStopped;
      if ((closing && state->tasks.empty()) ||
          (!signaled && state->current_threads > state->config.core_threads)) {
        worker->exited = true;
        --state->current_threads;
        lock.unlock();
        state->condition.notify_all();
        worker_state_ = nullptr;
        return;
      }
      if (state->status == ThreadPoolState::kPaused || state->tasks.empty()) {
        continue;
      }
      task = std::move(const_cast<TaskWrapper&>(state->tasks.top()));
      state->tasks.pop();
      // 出队和执行中计数必须一起提交，关闭观察不到中间空隙。
      ++state->active_threads;
    }

    const auto started = Clock::now();
    try {
      task.Execute();
    } catch (const std::exception& error) {
      LOG_ERROR("[ThreadPool] Task wrapper failed: {}", error.what());
    } catch (...) {
      LOG_ERROR("[ThreadPool] Task wrapper failed with unknown exception");
    }
    const double elapsed =
        std::chrono::duration<double, std::milli>(Clock::now() - started)
            .count();
    {
      std::lock_guard<std::mutex> lock(state->stats_mutex);
      state->total_task_time += elapsed;
      ++state->timed_tasks;
    }
    {
      std::lock_guard<std::mutex> lock(state->queue_mutex);
      --state->active_threads;
    }
    state->condition.notify_all();
  }
}

bool ThreadPool::Pause() {
  std::lock_guard<std::mutex> lock(state_->queue_mutex);
  if (state_->status != ThreadPoolState::kRunning) {
    return false;
  }
  state_->status = ThreadPoolState::kPaused;
  state_->condition.notify_all();
  return true;
}

bool ThreadPool::Resume() {
  std::lock_guard<std::mutex> lock(state_->queue_mutex);
  if (state_->status != ThreadPoolState::kPaused) {
    return false;
  }
  state_->status = ThreadPoolState::kRunning;
  state_->condition.notify_all();
  return true;
}

ThreadPool::Stats ThreadPool::GetStats() const {
  Stats stats;
  {
    std::lock_guard<std::mutex> lock(state_->stats_mutex);
    stats.tasks_completed = state_->tasks_completed;
    stats.tasks_failed = state_->tasks_failed;
    stats.avg_task_time_ms =
        state_->timed_tasks == 0
            ? 0
            : state_->total_task_time / state_->timed_tasks;
  }
  {
    std::lock_guard<std::mutex> lock(state_->queue_mutex);
    stats.active_threads = state_->active_threads;
    stats.worker_handles = state_->workers.size() + state_->reaping_workers;
  }
  return stats;
}

size_t ThreadPool::Size() const noexcept {
  std::lock_guard<std::mutex> lock(state_->queue_mutex);
  return state_->current_threads;
}
size_t ThreadPool::QueueSize() const noexcept {
  std::lock_guard<std::mutex> lock(state_->queue_mutex);
  return state_->tasks.size();
}
ThreadPoolState ThreadPool::GetState() const noexcept {
  std::lock_guard<std::mutex> lock(state_->queue_mutex);
  return state_->status;
}
bool ThreadPool::IsWorkerThread() const noexcept {
  return worker_state_ == state_.get();
}

void ThreadPool::RequestShutdown() {
  {
    std::lock_guard<std::mutex> lock(state_->queue_mutex);
    if (state_->status != ThreadPoolState::kStopped) {
      state_->status = ThreadPoolState::kShuttingDown;
    }
  }
  state_->condition.notify_all();
}

bool ThreadPool::WaitForTermination(std::chrono::milliseconds timeout) {
  if (IsWorkerThread()) {
    return false;
  }
  const auto state = state_;
  std::vector<std::unique_ptr<Worker>> retired;
  {
    std::unique_lock<std::mutex> lock(state->queue_mutex);
    const bool finished = Wait(state->condition, lock, timeout, [&] {
      return state->current_threads == 0 && state->reaping_workers == 0;
    });
    if (!finished) {
      return false;
    }
    retired.swap(state->workers);
    state->reaping_workers += retired.size();
  }
  JoinRetired(state, std::move(retired));
  return true;
}

bool ThreadPool::ShutDown(std::chrono::milliseconds timeout) {
  RequestShutdown();
  return WaitForTermination(timeout);
}
void ThreadPool::Stop() { ShutDown(); }

ThreadPool::~ThreadPool() {
  RequestShutdown();
  if (!IsWorkerThread()) {
    WaitForTermination();
    return;
  }
  // 最后一个 shared_ptr 也可能由业务任务释放。由独立回收者 join，
  // worker 持有 State，避免在自身线程中等待或访问已析构的 ThreadPool。
  const auto state = state_;
  std::vector<std::unique_ptr<Worker>> retired;
  {
    std::lock_guard<std::mutex> lock(state->queue_mutex);
    retired.swap(state->workers);
    state->reaping_workers += retired.size();
  }
  std::thread([state, retired = std::move(retired)]() mutable {
    JoinRetired(state, std::move(retired));
  }).detach();
}
}  // namespace meeting_ctrl
