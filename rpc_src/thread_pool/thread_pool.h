#ifndef RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_H_
#define RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "log_manager.h"

namespace meeting_ctrl {

// 任务优先级
enum class TaskPriority {
  kLow, // 低优先级任务
  kNormal, // 普通优先级任务
  kHigh // 高优先级任务
};

// 线程池生命周期状态
enum class ThreadPoolState {
  kRunning, // 正常运行状态
  kPaused, // 暂停状态
  kShuttingDown, // 优雅关闭中
  kStopped // 已停止
};

// 线程池运行参数
struct ThreadPoolStruct {
  size_t core_threads = std::thread::hardware_concurrency(); // 核心线程数
  size_t max_threads = std::thread::hardware_concurrency() * 2; // 最大线程数
  size_t max_queue_size = 1000; // 最大队列大小
  std::chrono::milliseconds keep_alive_time{60000}; // 非核心线程空闲超时时间
};

// 将可调用对象和优先级封装为队列元素
class TaskWrapper {
 public:
  TaskWrapper() : priority_(TaskPriority::kNormal) {}

  TaskWrapper(std::function<void()>&& task, TaskPriority priority)
      : task_(std::move(task)), priority_(priority) {}

  void Execute() {
    if (Valid()) {
      task_();
    }
  }

  bool Valid() const {
    return static_cast<bool>(task_);
  }

  bool operator<(const TaskWrapper& other) const {
    return priority_ < other.priority_;
  }

 private:
  std::function<void()> task_; // 任务函数对象
  TaskPriority priority_; // 任务优先级
};

// 支持优先级、暂停恢复和优雅关闭的线程池
class ThreadPool {
 public:
  struct Stats {
    size_t tasks_completed = 0; // 已完成任务数
    size_t tasks_failed = 0; // 失败任务数
    double avg_task_time_ms = 0; // 平均任务执行时间
    size_t active_threads = 0; // 当前活跃线程数
  };

  explicit ThreadPool(size_t threads = std::thread::hardware_concurrency());
  explicit ThreadPool(const ThreadPoolStruct& config);

  template <typename F, typename... Args>
  auto Enqueue(TaskPriority priority, F&& f, Args&&... args)
      -> std::future<std::invoke_result_t<F, Args...>>;

  // 暂停只停止取出任务，已入队任务会在恢复后继续执行
  void Pause();
  void Resume();

  Stats GetStats() const;

  size_t Size() const noexcept {
    return current_threads_.load();
  }

  size_t QueueSize() const noexcept;

  // 等待队列排空；超时后丢弃尚未开始的任务
  bool ShutDown(std::chrono::milliseconds wait_timeout_ms =
                    std::chrono::milliseconds::max());
  void Stop();

  ThreadPoolState GetState() const noexcept;

  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

 private:
  void WorkerThread();
  void AdjustThreadCount();

  std::vector<std::thread> workers_; // 工作线程容器
  std::atomic<size_t> current_threads_{0}; // 当前线程数
  std::vector<std::chrono::steady_clock::time_point> thread_last_active_; // 线程最后活跃时间

  std::priority_queue<TaskWrapper> tasks_; // 优先级任务队列
  ThreadPoolStruct config_; // 线程池配置参数

  mutable std::mutex queue_mutex_;  // 保护任务队列和停止标志
  mutable std::mutex lifecycle_mutex_;  // 串行化关闭和销毁
  std::condition_variable condition_; // 任务可用条件变量
  bool stop_{false}; // 停止标志

  std::atomic<ThreadPoolState> state_{ThreadPoolState::kRunning}; // 线程池状态

  mutable std::mutex stats_mutex_; // 统计信息互斥锁
  std::atomic<size_t> tasks_completed_{0};  // 已完成任务数
  std::atomic<size_t> tasks_failed_{0}; // 失败任务数
  std::atomic<size_t> active_threads_{0}; // 当前活跃线程数
  double total_task_time_{0}; // 总任务执行时间
};

template <typename F, typename... Args>
auto ThreadPool::Enqueue(TaskPriority priority, F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>> {
  using return_type = std::invoke_result_t<F, Args...>;
  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));
  std::future<return_type> result = task->get_future();

  {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    if (stop_ || state_ == ThreadPoolState::kStopped ||
        state_ == ThreadPoolState::kShuttingDown) {
      return {};
    }

    if (config_.max_queue_size > 0) {
      // 队列满时立即拒绝任务，由调用方执行背压或返回服务不可用。
      if (tasks_.size() >= config_.max_queue_size) {
        return {};
      }
    }

    tasks_.emplace([task] { (*task)(); }, priority);
    const std::string priority_string =
        priority == TaskPriority::kHigh
            ? "HIGH"
            : priority == TaskPriority::kNormal ? "NORMAL" : "LOW";
    LOG_DEBUG("[ThreadPool] Enqueuing task with priority: {}, current queue "
              "size: {}/{}, active threads: {}/{}",
              priority_string, tasks_.size(), config_.max_queue_size,
              active_threads_.load(), current_threads_.load());

    if (tasks_.size() > active_threads_ &&
        current_threads_ < config_.max_threads) {
      AdjustThreadCount();
    }
    condition_.notify_one();
  }
  return result;
}
}  // namespace meeting_ctrl

#endif  // RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_H_
