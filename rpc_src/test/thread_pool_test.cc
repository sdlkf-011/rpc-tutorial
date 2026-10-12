#include "thread_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "thread_pool_singleton.h"

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

using namespace std::chrono_literals;

ThreadPoolStruct Config(size_t maximum = 1) {
  ThreadPoolStruct config;
  config.core_threads = 1;
  config.max_threads = maximum;
  config.max_queue_size = 0;
  config.keep_alive_time = 10ms;
  return config;
}

// Gate 在池之后声明，失败路径也先释放业务任务，再由析构回收线程。
struct Gate {
  std::promise<void> promise;
  std::shared_future<void> signal = promise.get_future().share();
  bool opened = false;
  void Open() {
    if (!opened) {
      opened = true;
      promise.set_value();
    }
  }
  ~Gate() { Open(); }
};

template <typename Predicate>
bool Eventually(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  do {
    if (predicate()) return true;
    std::this_thread::sleep_for(1ms);
  } while (std::chrono::steady_clock::now() < deadline);
  return predicate();
}

TEST(ThreadPoolTest, DefaultShutdownDrainsPausedTasks) {
  ThreadPool pool(Config());
  ASSERT_TRUE(pool.Pause());
  std::vector<std::future<int>> results;
  for (int i = 0; i < 30; ++i) {
    results.push_back(pool.Enqueue(TaskPriority::kNormal, [i] { return i; }));
    ASSERT_TRUE(results.back().valid());
  }
  EXPECT_EQ(pool.QueueSize(), 30u);
  EXPECT_TRUE(pool.ShutDown());
  for (int i = 0; i < 30; ++i) EXPECT_EQ(results[i].get(), i);
  EXPECT_EQ(pool.GetStats().tasks_completed, 30u);
  EXPECT_EQ(pool.GetState(), ThreadPoolState::kStopped);
  EXPECT_EQ(pool.GetStats().worker_handles, 0u);
}

TEST(ThreadPoolTest, TimeoutReturnsWithoutJoiningAndKeepsAllAcceptedTasks) {
  ThreadPool pool(Config());
  Gate gate;
  auto started = std::make_shared<std::promise<void>>();
  auto running =
      pool.Enqueue(TaskPriority::kNormal, [started, signal = gate.signal] {
        started->set_value();
        signal.wait();
        return 1;
      });
  ASSERT_EQ(started->get_future().wait_for(2s), std::future_status::ready);
  auto queued = pool.Enqueue(TaskPriority::kNormal, [] { return 2; });
  auto close =
      std::async(std::launch::async, [&] { return pool.ShutDown(20ms); });
  const auto returned = close.wait_for(500ms);
  // 老实现会在有限等待到期后仍 join；先释放它，使失败也能结束。
  if (returned != std::future_status::ready) gate.Open();
  EXPECT_EQ(returned, std::future_status::ready);
  EXPECT_FALSE(close.get());
  EXPECT_EQ(pool.GetState(), ThreadPoolState::kShuttingDown);
  EXPECT_FALSE(pool.Enqueue(TaskPriority::kNormal, [] {}).valid());
  EXPECT_FALSE(pool.WaitForTermination(0ms));
  gate.Open();
  EXPECT_EQ(running.get(), 1);
  EXPECT_EQ(queued.get(), 2);
  EXPECT_TRUE(pool.WaitForTermination());
  EXPECT_TRUE(pool.ShutDown(0ms));
  EXPECT_EQ(pool.GetStats().tasks_completed, 2u);
}

TEST(ThreadPoolTest, NearMaximumFiniteTimeoutDoesNotOverflow) {
  ThreadPool pool(Config());
  Gate gate;
  auto running = pool.Enqueue(TaskPriority::kNormal,
                              [signal = gate.signal] { signal.wait(); });
  auto queued = pool.Enqueue(TaskPriority::kNormal, [] { return 7; });
  auto close = std::async(std::launch::async, [&] {
    return pool.ShutDown(std::chrono::milliseconds::max() - 1ms);
  });
  EXPECT_EQ(close.wait_for(20ms), std::future_status::timeout);
  gate.Open();
  EXPECT_TRUE(close.get());
  running.get();
  EXPECT_EQ(queued.get(), 7);
}

TEST(ThreadPoolTest, MaximumIdleWaitSleepsUntilWorkOrShutdown) {
  auto config = Config();
  config.keep_alive_time = std::chrono::milliseconds::max();
  ThreadPool pool(config);
  auto result = pool.Enqueue(TaskPriority::kNormal, [] { return 19; });
  EXPECT_EQ(result.get(), 19);
  EXPECT_TRUE(pool.ShutDown());
}

struct BusinessError : std::runtime_error {
  BusinessError() : std::runtime_error("business failure") {}
};

TEST(ThreadPoolTest, FuturesKeepOriginalExceptionsAndFailureStatsAgree) {
  ThreadPool pool(Config());
  auto typed = pool.Enqueue(TaskPriority::kNormal,
                            []() -> int { throw BusinessError(); });
  auto untyped = pool.Enqueue(TaskPriority::kNormal, [] { throw 37; });
  auto success = pool.Enqueue(TaskPriority::kNormal, [] { return 9; });
  try {
    typed.get();
    FAIL() << "expected original BusinessError";
  } catch (const BusinessError& error) {
    EXPECT_STREQ(error.what(), "business failure");
  }
  try {
    untyped.get();
    FAIL() << "expected original integer exception";
  } catch (int value) {
    EXPECT_EQ(value, 37);
  }
  EXPECT_EQ(success.get(), 9);
  EXPECT_TRUE(pool.ShutDown());
  EXPECT_EQ(pool.GetStats().tasks_failed, 2u);
  EXPECT_EQ(pool.GetStats().tasks_completed, 1u);
}

TEST(ThreadPoolTest, WorkerCannotSynchronouslyWaitForItself) {
  ThreadPool pool(Config());
  auto result = pool.Enqueue(TaskPriority::kNormal, [&] {
    EXPECT_EQ(pool.GetStats().active_threads, 1u);
    return pool.ShutDown();
  });
  EXPECT_FALSE(result.get());
  EXPECT_TRUE(pool.WaitForTermination());
}

TEST(ThreadPoolTest, ConcurrentShutdownCallersJoinWorkersOnlyOnce) {
  ThreadPool pool(Config(4));
  ASSERT_TRUE(pool.Pause());
  std::vector<std::future<int>> tasks;
  for (int i = 0; i < 100; ++i) {
    tasks.push_back(pool.Enqueue(TaskPriority::kNormal, [i] { return i; }));
  }
  std::vector<std::future<bool>> closers;
  for (int i = 0; i < 8; ++i) {
    closers.push_back(
        std::async(std::launch::async, [&] { return pool.ShutDown(); }));
  }
  for (auto& closer : closers) EXPECT_TRUE(closer.get());
  for (int i = 0; i < 100; ++i) EXPECT_EQ(tasks[i].get(), i);
  EXPECT_EQ(pool.Size(), 0u);
  EXPECT_EQ(pool.GetStats().worker_handles, 0u);
}

TEST(ThreadPoolTest, RepeatedExpansionAndIdleShrinkKeepWorkerHandlesBounded) {
  ThreadPool pool(Config(4));
  for (int cycle = 0; cycle < 20; ++cycle) {
    Gate gate;
    std::vector<std::future<void>> tasks;
    for (int i = 0; i < 8; ++i) {
      tasks.push_back(pool.Enqueue(TaskPriority::kNormal,
                                   [signal = gate.signal] { signal.wait(); }));
    }
    EXPECT_TRUE(Eventually([&] { return pool.Size() == 4; }));
    EXPECT_LE(pool.GetStats().worker_handles, 4u);
    gate.Open();
    for (auto& task : tasks) task.get();
    EXPECT_TRUE(Eventually([&] { return pool.Size() == 1; }));
    auto sweep = pool.Enqueue(TaskPriority::kNormal, [] {});
    sweep.get();
    EXPECT_EQ(pool.GetStats().worker_handles, 1u);
  }
  EXPECT_TRUE(pool.ShutDown());
  EXPECT_EQ(pool.GetStats().worker_handles, 0u);
}

TEST(ThreadPoolTest, ConcurrentSubmissionsAndCloseShareWorkerOwnership) {
  auto config = Config(6);
  config.keep_alive_time = 1ms;
  ThreadPool pool(config);
  std::atomic<int> accepted{0};
  std::atomic<int> executed{0};
  std::vector<std::thread> producers;
  for (int producer = 0; producer < 6; ++producer) {
    producers.emplace_back([&] {
      for (int i = 0; i < 300; ++i) {
        auto task = pool.Enqueue(TaskPriority::kNormal, [&] { ++executed; });
        if (task.valid()) {
          ++accepted;
          task.get();
        }
        EXPECT_LE(pool.GetStats().worker_handles, 6u);
        if (i % 5 == 0) std::this_thread::sleep_for(1ms);
      }
    });
  }
  EXPECT_TRUE(Eventually([&] { return accepted.load() >= 100; }));
  auto close = std::async(std::launch::async, [&] { return pool.ShutDown(); });
  auto also_close =
      std::async(std::launch::async, [&] { return pool.ShutDown(); });
  for (auto& producer : producers) producer.join();
  EXPECT_TRUE(close.get());
  EXPECT_TRUE(also_close.get());
  EXPECT_EQ(accepted.load(), executed.load());
  EXPECT_EQ(pool.GetStats().tasks_completed,
            static_cast<size_t>(accepted.load()));
  EXPECT_EQ(pool.GetStats().worker_handles, 0u);
}

TEST(ThreadPoolTest, LastOwnerReleasedOnWorkerKeepsAcceptedTasksAlive) {
  auto pool = std::make_shared<ThreadPool>(Config());
  Gate gate;
  auto first = pool->Enqueue(
      TaskPriority::kNormal, [owner = pool, signal = gate.signal]() mutable {
        signal.wait();
        owner.reset();  // 池在 worker 内析构，运行状态仍归 worker 所有。
        return 11;
      });
  auto tail = pool->Enqueue(TaskPriority::kNormal, [] { return 22; });
  std::weak_ptr<ThreadPool> weak = pool;
  pool.reset();
  gate.Open();
  EXPECT_EQ(first.get(), 11);
  EXPECT_EQ(tail.get(), 22);
  EXPECT_TRUE(weak.expired());
}

class ThreadPoolSingletonTest : public testing::Test {
 protected:
  void SetUp() override { ASSERT_TRUE(ThreadPoolSingleton::Destroy()); }
  void TearDown() override { EXPECT_TRUE(ThreadPoolSingleton::Destroy()); }
};

TEST_F(ThreadPoolSingletonTest, SnapshotOwnsLifetimeAcrossDestroy) {
  ASSERT_TRUE(ThreadPoolSingleton::Init(Config()));
  const auto pool = ThreadPoolSingleton::GetInstance();
  ASSERT_NE(pool, nullptr);
  EXPECT_TRUE(ThreadPoolSingleton::Destroy());
  EXPECT_FALSE(ThreadPoolSingleton::Exists());
  EXPECT_EQ(pool->GetState(), ThreadPoolState::kStopped);
  EXPECT_FALSE(pool->Enqueue(TaskPriority::kNormal, [] {}).valid());
  EXPECT_TRUE(ThreadPoolSingleton::Init(Config()));
  EXPECT_NE(ThreadPoolSingleton::GetInstance(), pool);
}

TEST_F(ThreadPoolSingletonTest,
       DestroyAllowsTaskStatsAndBlocksInitializationAndSubmission) {
  ASSERT_TRUE(ThreadPoolSingleton::Init(Config()));
  const auto pool = ThreadPoolSingleton::GetInstance();
  Gate gate;
  auto started = std::make_shared<std::promise<void>>();
  auto task = ThreadPoolSingleton::Enqueue(
      TaskPriority::kNormal, [started, signal = gate.signal] {
        started->set_value();
        signal.wait();
        const auto stats = ThreadPoolSingleton::GetStats();
        EXPECT_EQ(stats.active_threads, 1u);
        EXPECT_EQ(ThreadPoolSingleton::GetPoolSize(), 1u);
        EXPECT_EQ(ThreadPoolSingleton::GetQueueSize(), 0u);
        EXPECT_EQ(ThreadPoolSingleton::GetState(),
                  ThreadPoolState::kShuttingDown);
        return 31;
      });
  ASSERT_EQ(started->get_future().wait_for(2s), std::future_status::ready);
  auto destroy = std::async(std::launch::async,
                            [] { return ThreadPoolSingleton::Destroy(); });
  EXPECT_TRUE(
      Eventually([] { return ThreadPoolSingleton::GetInstance() == nullptr; }));
  EXPECT_TRUE(ThreadPoolSingleton::Exists());
  EXPECT_FALSE(ThreadPoolSingleton::Init(Config()));
  EXPECT_EQ(ThreadPoolSingleton::GetInstance(Config()), nullptr);
  EXPECT_FALSE(
      ThreadPoolSingleton::Enqueue(TaskPriority::kNormal, [] {}).valid());
  EXPECT_FALSE(ThreadPoolSingleton::Destroy());
  gate.Open();
  EXPECT_EQ(task.get(), 31);
  EXPECT_TRUE(destroy.get());
  EXPECT_EQ(pool->GetStats().tasks_completed, 1u);
  EXPECT_FALSE(ThreadPoolSingleton::Exists());
}

TEST_F(ThreadPoolSingletonTest,
       ShutdownDoesNotHoldGlobalLockWhileTasksReadStats) {
  ASSERT_TRUE(ThreadPoolSingleton::Init(Config()));
  Gate gate;
  auto task = ThreadPoolSingleton::Enqueue(
      TaskPriority::kNormal, [signal = gate.signal] {
        signal.wait();
        return ThreadPoolSingleton::GetStats().active_threads;
      });
  auto close = std::async(std::launch::async,
                          [] { return ThreadPoolSingleton::Shutdown(); });
  EXPECT_TRUE(Eventually([] {
    return ThreadPoolSingleton::GetState() == ThreadPoolState::kShuttingDown;
  }));
  gate.Open();
  EXPECT_EQ(task.get(), 1u);
  EXPECT_TRUE(close.get());
}

TEST_F(ThreadPoolSingletonTest, WorkerDestroyIsRejectedWithoutSelfJoin) {
  ASSERT_TRUE(ThreadPoolSingleton::Init(Config()));
  auto result = ThreadPoolSingleton::Enqueue(
      TaskPriority::kNormal, [] { return ThreadPoolSingleton::Destroy(); });
  EXPECT_FALSE(result.get());
  EXPECT_TRUE(ThreadPoolSingleton::Exists());
  EXPECT_EQ(ThreadPoolSingleton::GetState(), ThreadPoolState::kRunning);
}

TEST_F(ThreadPoolSingletonTest,
       TimedShutdownKeepsOldPoolUntilLaterWaitAndDestroy) {
  ASSERT_TRUE(ThreadPoolSingleton::Init(Config()));
  Gate gate;
  auto started = std::make_shared<std::promise<void>>();
  auto task = ThreadPoolSingleton::Enqueue(TaskPriority::kNormal,
                                           [started, signal = gate.signal] {
                                             started->set_value();
                                             signal.wait();
                                           });
  ASSERT_EQ(started->get_future().wait_for(2s), std::future_status::ready);
  EXPECT_FALSE(ThreadPoolSingleton::Shutdown(10ms));
  EXPECT_TRUE(ThreadPoolSingleton::Exists());
  EXPECT_EQ(ThreadPoolSingleton::GetStats().active_threads, 1u);
  EXPECT_FALSE(
      ThreadPoolSingleton::Enqueue(TaskPriority::kNormal, [] {}).valid());
  gate.Open();
  task.get();
  EXPECT_TRUE(ThreadPoolSingleton::WaitForTermination());
  EXPECT_TRUE(ThreadPoolSingleton::Destroy());
}
}  // namespace
}  // namespace meeting_ctrl
