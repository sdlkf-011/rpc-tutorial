#include "thread_pool.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>

namespace meeting_ctrl {
namespace {

// 线程池容量测试模块：验证工作线程、任务队列满载时的快速拒绝和安全关闭。
// 验证线程池队列满时快速拒绝新任务，并能安全释放和关闭线程。
TEST(ThreadPoolTest, RejectsNewTaskImmediatelyWhenQueueIsFull) {
  ThreadPoolStruct config;
  config.core_threads = 1;
  config.max_threads = 1;
  config.max_queue_size = 1;
  config.keep_alive_time = std::chrono::seconds(1);
  ThreadPool pool(config);

  std::promise<void> worker_started;
  std::promise<void> release_worker;
  std::shared_future<void> release_signal = release_worker.get_future().share();

  // 第一个任务占用唯一工作线程，使第二个任务稳定地留在队列中。
  std::future<int> running =
      pool.Enqueue(TaskPriority::kNormal, [&worker_started, release_signal] {
        worker_started.set_value();
        release_signal.wait();
        return 1;
      });
  ASSERT_TRUE(running.valid());
  worker_started.get_future().wait();

  std::future<int> queued =
      pool.Enqueue(TaskPriority::kNormal, [] { return 2; });
  if (!queued.valid()) {
    // 失败路径也必须释放首个任务，避免线程池析构时等待被阻塞的工作线程。
    release_worker.set_value();
    EXPECT_EQ(running.get(), 1);
    pool.Stop();
    FAIL() << "队列仍有容量时不应拒绝任务";
  }

  const auto enqueue_started = std::chrono::steady_clock::now();
  std::future<int> rejected =
      pool.Enqueue(TaskPriority::kNormal, [] { return 3; });
  const auto enqueue_elapsed =
      std::chrono::steady_clock::now() - enqueue_started;

  EXPECT_FALSE(rejected.valid());
  EXPECT_LT(enqueue_elapsed, std::chrono::milliseconds(100));

  release_worker.set_value();
  EXPECT_EQ(running.get(), 1);
  EXPECT_EQ(queued.get(), 2);
  EXPECT_TRUE(pool.ShutDown(std::chrono::seconds(1)));
  // 关闭完成后线程池不应继续报告已经退出的工作线程。
  EXPECT_EQ(pool.Size(), 0);
}

}  // namespace
}  // namespace meeting_ctrl
