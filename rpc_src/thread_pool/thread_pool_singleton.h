#ifndef RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_SINGLETON_H_
#define RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_SINGLETON_H_

#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <utility>

#include "thread_pool.h"

namespace meeting_ctrl {

// 线程池全局单例访问入口
class ThreadPoolSingleton {
 public:
  // 初始化线程池；重复初始化返回 false
  static bool Init(size_t threads = std::thread::hardware_concurrency());
  static bool Init(const ThreadPoolStruct& config);

  static ThreadPool& GetInstance();
  static ThreadPool& GetInstance(const ThreadPoolStruct& config);

  template <typename F, typename... Args>
  static auto Enqueue(TaskPriority priority, F&& f, Args&&... args)
      -> std::future<std::invoke_result_t<F, Args...>> {
    return GetInstance().Enqueue(priority, std::forward<F>(f),
                                 std::forward<Args>(args)...);
  }

  static ThreadPool::Stats GetStats();
  static size_t GetQueueSize();
  static size_t GetPoolSize();

  static bool Pause();
  static bool Resume();
  static bool Shutdown(
      std::chrono::milliseconds timeout = std::chrono::milliseconds::max());

  static void Destroy();
  static bool Exists();
  static ThreadPoolState GetState();

  ThreadPoolSingleton(const ThreadPoolSingleton&) = delete;
  ThreadPoolSingleton& operator=(const ThreadPoolSingleton&) = delete;

 private:
  ThreadPoolSingleton() = default;

  static std::unique_ptr<ThreadPool> instance_;
  static std::mutex mutex_;  // 全局互斥锁
};
}  // namespace meeting_ctrl

#endif  // RPC_TUTORIAL_RPC_SRC_THREAD_POOL_THREAD_POOL_SINGLETON_H_
