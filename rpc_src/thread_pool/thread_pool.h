#ifndef RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_H_
#define RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_H_

#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace meeting_ctrl {

enum class TaskPriority { kLow, kNormal, kHigh };
enum class ThreadPoolState { kRunning, kPaused, kShuttingDown, kStopped };

struct ThreadPoolStruct {
  size_t core_threads = std::thread::hardware_concurrency();
  size_t max_threads = std::thread::hardware_concurrency() * 2;
  size_t max_queue_size = 1000;
  std::chrono::milliseconds keep_alive_time{60000};
};

class TaskWrapper {
 public:
  TaskWrapper() = default;
  TaskWrapper(std::function<void()>&& task, TaskPriority priority)
      : task_(std::move(task)), priority_(priority) {}
  bool Valid() const { return static_cast<bool>(task_); }
  void Execute() {
    if (Valid()) {
      task_();
    }
  }
  bool operator<(const TaskWrapper& other) const {
    return priority_ < other.priority_;
  }

 private:
  std::function<void()> task_;
  TaskPriority priority_ = TaskPriority::kNormal;
};

class ThreadPool {
 public:
  struct Stats {
    size_t tasks_completed = 0;
    size_t tasks_failed = 0;
    double avg_task_time_ms = 0;
    size_t active_threads = 0;
    // 包括尚未 join 的退出线程和正在回收的线程，用于检查回收是否有界。
    size_t worker_handles = 0;
  };

  explicit ThreadPool(size_t threads = std::thread::hardware_concurrency());
  explicit ThreadPool(const ThreadPoolStruct& config);

  template <typename F, typename... Args>
  auto Enqueue(TaskPriority priority, F&& f,
               Args&&... args) -> std::future<std::invoke_result_t<F, Args...>>;

  bool Pause();
  bool Resume();
  Stats GetStats() const;
  size_t Size() const noexcept;
  size_t QueueSize() const noexcept;
  ThreadPoolState GetState() const noexcept;
  bool IsWorkerThread() const noexcept;

  // 只请求关闭：取消暂停、拒绝新任务，并排空已接收任务；可由 worker 调用。
  void RequestShutdown();
  // 有限等待到期返回 false，不丢任务、不在超时路径 join。
  // worker 调用直接返回 false，避免同步等待自身退出。
  bool WaitForTermination(
      std::chrono::milliseconds timeout = std::chrono::milliseconds::max());
  bool ShutDown(
      std::chrono::milliseconds timeout = std::chrono::milliseconds::max());
  // 兼容原入口；外部调用等同默认 ShutDown，worker 只发出关闭请求。
  void Stop();
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

 private:
  // worker 和业务包装持有运行状态，独立于外层 ThreadPool 句柄的生命周期。
  struct State;
  struct Worker;
  struct TaskResult {
    std::shared_ptr<State> state;
    bool succeeded = true;
    ~TaskResult();
  };

  bool SubmitTask(TaskWrapper task);
  static void AddWorkerLocked(const std::shared_ptr<State>& state);
  static void AdjustThreadCountLocked(const std::shared_ptr<State>& state);
  static void WorkerThread(const std::shared_ptr<State>& state, Worker* worker);
  static void JoinRetired(const std::shared_ptr<State>& state,
                          std::vector<std::unique_ptr<Worker>> workers);
  static thread_local const State* worker_state_;
  std::shared_ptr<State> state_;
};

template <typename F, typename... Args>
auto ThreadPool::Enqueue(TaskPriority priority, F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>> {
  using Result = std::invoke_result_t<F, Args...>;
  auto task = std::make_shared<std::packaged_task<Result()>>(
      [state = state_,
       work = std::bind(std::forward<F>(f),
                        std::forward<Args>(args)...)]() mutable -> Result {
        TaskResult outcome{state};
        try {
          return work();
        } catch (...) {
          outcome.succeeded = false;
          // packaged_task 保存原始异常；统计发生在业务包装内。
          throw;
        }
      });
  auto result = task->get_future();
  if (!SubmitTask(TaskWrapper([task] { (*task)(); }, priority))) {
    return {};
  }
  return result;
}
}  // namespace meeting_ctrl

#endif  // RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_H_
