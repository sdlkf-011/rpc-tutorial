#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "log_manager.h"
#include "rpc_client.h"

namespace {

// 默认值与上限模块：限制并发数、请求数和报文大小，避免误操作占满资源。
constexpr char kDefaultConfigPath[] = "config/rpc_client.json";
constexpr size_t kDefaultThreads = 10;
constexpr uint64_t kDefaultRequests = 100;
constexpr size_t kDefaultPayloadSize = 64;
constexpr size_t kMaximumThreads = 1024;
constexpr uint64_t kMaximumRequests = 10000000;
constexpr uint64_t kMaximumDurationSeconds = 86400;
constexpr size_t kLatencyBuckets = 64 * 64;
constexpr size_t kMaximumPayloadSize = 1024 * 1024 - 4096;

enum class RequestMode { kPerClient, kTotal, kDuration };

// requests 在次数模式表示每客户端数量或合计数量。
struct Options {
  std::string config_path = kDefaultConfigPath;
  size_t threads = kDefaultThreads;
  uint64_t requests = kDefaultRequests;
  RequestMode mode = RequestMode::kPerClient;
  uint64_t duration_seconds = 0;
  size_t payload_size = kDefaultPayloadSize;
};

// 工作线程结果模块：每个线程独占结果，避免在请求热路径上加锁。
struct WorkerResult {
  uint64_t succeeded = 0;
  uint64_t failed = 0;
  std::vector<uint64_t> latencies_us;
  // 时长模式按计数直方图统计全部调用，内存不随请求数增长。
  std::array<uint64_t, kLatencyBuckets> latency_histogram{};
  long double latency_sum_us = 0;
  uint64_t maximum_latency_us = 0;
};

size_t LatencyBucket(uint64_t latency_us) {
  // 128 微秒以内精确计数，之后每个指数区间细分为 64 档。
  if (latency_us < 128) return static_cast<size_t>(latency_us);
  size_t shift = 0;
  while (latency_us >= 128) {
    latency_us >>= 1;
    ++shift;
  }
  return (shift + 1) * 64 + static_cast<size_t>(latency_us - 64);
}

uint64_t HistogramPercentile(const WorkerResult& result, double percentile) {
  const uint64_t target = static_cast<uint64_t>(
      std::ceil(percentile * static_cast<double>(result.succeeded + result.failed)));
  if (target == 0) return 0;
  uint64_t cumulative = 0;
  for (size_t index = 0; index < result.latency_histogram.size(); ++index) {
    cumulative += result.latency_histogram[index];
    if (cumulative < target) continue;
    if (index < 128) return index;
    const size_t shift = index / 64 - 1;
    const uint64_t upper_bound = ((uint64_t{64} + index % 64) << shift) +
                                 ((uint64_t{1} << shift) - 1);
    return std::min(upper_bound, result.maximum_latency_us);
  }
  return result.maximum_latency_us;
}

// 参数解析模块：提供明确的范围校验，避免异常参数耗尽连接或内存。
void PrintUsage(const char* program_name) {
  // 帮助信息模块：集中描述命令行参数及其默认配置，便于快速启动压测。
  std::cout
      << "Usage: " << program_name << " [options]\n\n"
      << "Options:\n"
      << "  --config PATH       RPC client config path (default: "
      << kDefaultConfigPath << ")\n"
      << "  --threads N         Concurrent client connections (default: "
      << kDefaultThreads << ")\n"
      << "  --requests-per-client N\n"
      << "                      Requests per client (default: "
      << kDefaultRequests << ")\n"
      << "  --requests N        Total requests across all clients (legacy mode)\n"
      << "  --duration N        Run for N seconds (1-86400)\n"
      << "  --payload-size N    Echo message size in bytes (default: "
      << kDefaultPayloadSize << ")\n"
      << "  --help              Show this help message\n";
}

bool ParseUnsigned(const std::string& text, uint64_t maximum, uint64_t& value) {
  // 无符号参数解析模块：拒绝负数、零、溢出值以及尾随非数字字符。
  if (text.empty() || text.front() == '-') {
    return false;
  }

  size_t parsed_characters = 0;
  try {
    const unsigned long long parsed = std::stoull(text, &parsed_characters, 10);
    if (parsed_characters != text.size() || parsed == 0 || parsed > maximum) {
      return false;
    }
    value = static_cast<uint64_t>(parsed);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool ParseOptions(int argc, char* argv[], Options& options) {
  bool mode_selected = false;
  // 命令行解析模块：逐项读取选项，并将校验后的值写入压测配置。
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    if (argument == "--help") {
      PrintUsage(argv[0]);
      std::exit(0);
    }
    if (index + 1 >= argc) {
      std::cerr << "Missing value for " << argument << '\n';
      return false;
    }

    const std::string value = argv[++index];
    uint64_t parsed_value = 0;
    if (argument == "--config") {
      if (value.empty()) {
        std::cerr << "Config path must not be empty\n";
        return false;
      }
      options.config_path = value;
    } else if (argument == "--threads") {
      if (!ParseUnsigned(value, kMaximumThreads, parsed_value)) {
        std::cerr << "Threads must be in the range [1, " << kMaximumThreads
                  << "]\n";
        return false;
      }
      options.threads = static_cast<size_t>(parsed_value);
    } else if (argument == "--requests" || argument == "--requests-per-client" ||
               argument == "--duration") {
      const RequestMode mode =
          argument == "--duration" ? RequestMode::kDuration
          : argument == "--requests" ? RequestMode::kTotal
                                     : RequestMode::kPerClient;
      if (mode_selected && options.mode != mode) {
        std::cerr << "Request count and duration modes are mutually exclusive\n";
        return false;
      }
      const uint64_t maximum = mode == RequestMode::kDuration
                                   ? kMaximumDurationSeconds
                                   : kMaximumRequests;
      if (!ParseUnsigned(value, maximum, parsed_value)) {
        std::cerr << argument << " must be in the range [1, " << maximum
                  << "]\n";
        return false;
      }
      options.mode = mode;
      mode_selected = true;
      if (mode == RequestMode::kDuration)
        options.duration_seconds = parsed_value;
      else
        options.requests = parsed_value;
    } else if (argument == "--payload-size") {
      if (!ParseUnsigned(value, kMaximumPayloadSize, parsed_value)) {
        std::cerr << "Payload size must be in the range [1, "
                  << kMaximumPayloadSize << "]\n";
        return false;
      }
      options.payload_size = static_cast<size_t>(parsed_value);
    } else {
      std::cerr << "Unknown option: " << argument << '\n';
      return false;
    }
  }
  if (options.mode == RequestMode::kPerClient &&
      options.requests > kMaximumRequests / options.threads) {
    std::cerr << "Total requests must not exceed " << kMaximumRequests << '\n';
    return false;
  }
  return true;
}

// 指标计算模块：使用最近秩法计算延迟分位数。
uint64_t Percentile(const std::vector<uint64_t>& sorted_values,
                    double percentile) {
  // 分位数模块：输入需已排序，采用最近秩法定位 P50/P95/P99 样本。
  if (sorted_values.empty()) {
    return 0;
  }
  const size_t index =
      static_cast<size_t>(
          std::ceil(percentile * static_cast<double>(sorted_values.size()))) -
      1;
  return sorted_values[std::min(index, sorted_values.size() - 1)];
}

void PrintResults(const Options& options, WorkerResult& result,
                  std::chrono::steady_clock::duration elapsed) {
  // 结果展示模块：计算吞吐率、成功率、平均延迟及主要延迟分位数。
  const double elapsed_seconds = std::chrono::duration<double>(elapsed).count();
  const uint64_t completed = result.succeeded + result.failed;
  const double requests_per_second =
      elapsed_seconds > 0.0 ? static_cast<double>(completed) / elapsed_seconds
                            : 0.0;
  const double success_rate = completed > 0
                                  ? 100.0 * static_cast<double>(result.succeeded) /
                                        static_cast<double>(completed)
                                  : 0.0;

  std::sort(result.latencies_us.begin(), result.latencies_us.end());
  const double average_latency =
      completed == 0
          ? 0.0
          : static_cast<double>(result.latency_sum_us / completed);
  const auto percentile = [&](double value) {
    return options.mode == RequestMode::kDuration
               ? HistogramPercentile(result, value)
               : Percentile(result.latencies_us, value);
  };

  std::cout << std::fixed << std::setprecision(2)
            << "\n========== RPC Stress Test Results ==========\n"
            << "Concurrency       : " << options.threads << '\n'
            << "Payload size      : " << options.payload_size << " bytes\n"
            << "Completed requests: " << completed << '\n'
            << "Successful        : " << result.succeeded << '\n'
            << "Failed            : " << result.failed << '\n'
            << "Success rate      : " << success_rate << "%\n"
            << "Elapsed           : " << elapsed_seconds << " s\n"
            << "Throughput        : " << requests_per_second << " req/s\n"
            << "Average latency   : " << average_latency / 1000.0 << " ms\n"
            << "P50 latency       : "
            << percentile(0.50) / 1000.0 << " ms\n"
            << "P95 latency       : "
            << percentile(0.95) / 1000.0 << " ms\n"
            << "P99 latency       : "
            << percentile(0.99) / 1000.0 << " ms\n"
            << "Maximum latency   : "
            << result.maximum_latency_us / 1000.0 << " ms\n";
  if (options.mode == RequestMode::kDuration)
    std::cout << "Duration target   : " << options.duration_seconds << " s\n"
              << "Percentiles       : approximate (latency histogram)\n";
  else if (options.mode == RequestMode::kPerClient)
    std::cout << "Requests/client   : " << options.requests << '\n';
  std::cout << "=============================================\n";
}

}  // namespace

int main(int argc, char* argv[]) {
  // 启动配置模块：解析参数并初始化日志，失败时尽早退出并返回错误码。
  Options options;
  if (!ParseOptions(argc, argv, options)) {
    PrintUsage(argv[0]);
    return 2;
  }
  if (!Logger::GetInstance().Init("logs")) {
    std::cerr << "Failed to initialize logger\n";
    return 1;
  }

  // 连接准备模块：每个并发线程独占 RpcClient 和长连接，避免客户端
  // 内部的串行互斥锁把并发请求退化为单连接串行请求。
  const size_t worker_count =
      options.mode == RequestMode::kTotal
          ? static_cast<size_t>(std::min<uint64_t>(options.threads, options.requests))
          : options.threads;
  options.threads = worker_count;
  std::vector<std::unique_ptr<cookrpc::RpcClient>> clients;
  clients.reserve(worker_count);
  for (size_t index = 0; index < worker_count; ++index) {
    auto client = std::make_unique<cookrpc::RpcClient>(options.config_path);
    if (!client->Connect()) {
      std::cerr << "Failed to connect stress-test client " << index + 1
                << " of " << worker_count << '\n';
      return 1;
    }
    clients.push_back(std::move(client));
  }

  // 并发执行模块：原子序号动态分配请求，条件变量确保所有线程统一起跑。
  std::atomic<uint64_t> next_request{0};
  std::atomic<bool> cancelled{false};
  std::mutex start_mutex;
  std::condition_variable start_condition;
  bool started = false;
  std::chrono::steady_clock::time_point deadline;
  std::vector<WorkerResult> worker_results(worker_count);
  std::vector<std::thread> workers;
  workers.reserve(worker_count);

  const std::string payload(options.payload_size, 'x');
  try {
    // 工作线程创建模块：每个线程绑定一个客户端连接和独立统计槽位。
    for (size_t worker_index = 0; worker_index < worker_count; ++worker_index) {
      workers.emplace_back([&, worker_index] {
        try {
          // 统一起跑屏障：所有线程创建完成后才开始抢占请求序号。
          {
            std::unique_lock<std::mutex> lock(start_mutex);
            start_condition.wait(lock, [&] { return started; });
          }
          if (cancelled.load(std::memory_order_acquire)) {
            return;
          }

          WorkerResult& result = worker_results[worker_index];
          // 预留本线程可能产生的样本容量，减少压测过程中的动态扩容。
          if (options.mode != RequestMode::kDuration) {
            const uint64_t reserve_count =
                options.mode == RequestMode::kPerClient
                    ? options.requests
                    : options.requests / worker_count + 1;
            result.latencies_us.reserve(static_cast<size_t>(reserve_count));
          }

          uint64_t client_requests = 0;
          while (!cancelled.load(std::memory_order_relaxed)) {
            if (options.mode == RequestMode::kPerClient &&
                client_requests >= options.requests)
              break;
            if (options.mode == RequestMode::kDuration &&
                std::chrono::steady_clock::now() >= deadline)
              break;
            // 原子序号分配保证每个请求只被一个线程执行，无需全局互斥锁。
            const uint64_t request_id =
                next_request.fetch_add(1, std::memory_order_relaxed);
            if (options.mode == RequestMode::kTotal &&
                request_id >= options.requests) {
              break;
            }

            const nlohmann::json request = {{"message", payload},
                                            {"request_id", request_id}};
            nlohmann::json response;
            // 仅测量 RPC 调用耗时，不包含请求构造和结果汇总等外围操作。
            const auto request_started = std::chrono::steady_clock::now();
            const bool called = clients[worker_index]->Call(
                "RpcService", "Echo", request, cookrpc::SerializeType::JSON,
                response);
            const auto request_finished = std::chrono::steady_clock::now();
            const auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    request_finished - request_started);
            const uint64_t latency_us = static_cast<uint64_t>(latency.count());
            if (options.mode == RequestMode::kDuration)
              ++result.latency_histogram[LatencyBucket(latency_us)];
            else
              result.latencies_us.push_back(latency_us);
            result.latency_sum_us += latency_us;
            result.maximum_latency_us =
                std::max(result.maximum_latency_us, latency_us);
            ++client_requests;

            // 响应校验模块：不仅检查调用返回值，还校验业务 Echo 返回的消息。
            const auto echo = response.is_object()
                                  ? response.find("received_message")
                                  : response.end();
            if (called && echo != response.end() && echo->is_string() &&
                echo->get_ref<const std::string&>() == payload) {
              ++result.succeeded;
            } else {
              ++result.failed;
            }
          }
        } catch (const std::exception& error) {
          // 工作线程中的异常不会传播给创建线程；显式取消并确保 main
          // 回收所有线程。
          cancelled.store(true, std::memory_order_release);
          std::cerr << "Stress-test worker failed: " << error.what() << '\n';
        } catch (...) {
          cancelled.store(true, std::memory_order_release);
          std::cerr << "Stress-test worker failed with an unknown error\n";
        }
      });
    }
  } catch (const std::exception& error) {
    // 线程启动失败模块：唤醒并回收已创建线程，避免 joinable 线程触发终止。
    cancelled.store(true, std::memory_order_release);
    {
      std::lock_guard<std::mutex> lock(start_mutex);
      started = true;
    }
    start_condition.notify_all();
    for (std::thread& worker : workers) {
      worker.join();
    }
    std::cerr << "Failed to create stress-test workers: " << error.what()
              << '\n';
    return 1;
  }

  const auto test_started = std::chrono::steady_clock::now();
  // 释放起跑屏障并记录测试时间窗口；随后等待所有线程自然完成。
  {
    std::lock_guard<std::mutex> lock(start_mutex);
    deadline = test_started + std::chrono::seconds(options.duration_seconds);
    started = true;
  }
  start_condition.notify_all();
  for (std::thread& worker : workers) {
    worker.join();
  }
  const auto test_finished = std::chrono::steady_clock::now();

  // 结果汇总模块：线程结束后再合并计数和延迟样本，不影响压测热路径。
  WorkerResult summary;
  for (WorkerResult& result : worker_results) {
    summary.succeeded += result.succeeded;
    summary.failed += result.failed;
    summary.latency_sum_us += result.latency_sum_us;
    summary.maximum_latency_us =
        std::max(summary.maximum_latency_us, result.maximum_latency_us);
    summary.latencies_us.insert(summary.latencies_us.end(),
                               result.latencies_us.begin(), result.latencies_us.end());
    if (options.mode == RequestMode::kDuration)
      for (size_t index = 0; index < kLatencyBuckets; ++index)
        summary.latency_histogram[index] += result.latency_histogram[index];
  }

  PrintResults(options, summary, test_finished - test_started);
  // 只有全部请求成功且数量吻合时才返回成功，便于脚本判断压测结果。
  const uint64_t expected = options.mode == RequestMode::kPerClient
                                ? options.requests * worker_count
                                : options.requests;
  const bool finished = options.mode == RequestMode::kDuration
                            ? summary.succeeded > 0
                            : summary.succeeded == expected;
  return !cancelled.load() && summary.failed == 0 && finished ? 0 : 1;
}
