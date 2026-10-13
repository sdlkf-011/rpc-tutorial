#ifndef RPC_TUTORIAL_UTILS_LOG_MANAGER_H_
#define RPC_TUTORIAL_UTILS_LOG_MANAGER_H_

#include <fmt/base.h>
#include <fmt/format.h>
#include <spdlog/common.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

/**
 * @brief 基础日志宏定义
 * TRACE  最详细的日志级别，用于追踪程序执行流程​
 * DEBUG  调试信息，用于开发调试
 * INFO   一般信息，记录程序正常运行状态
 * WARN   警告信息，表示潜在的问题
 * ERROR  错误信息，表示程序运行出错
 * FATAL  致命错误，程序无法继续运行
 */
#define LOG_TRACE(...) SPDLOG_TRACE(__VA_ARGS__)
#define LOG_DEBUG(...) SPDLOG_DEBUG(__VA_ARGS__)
#define LOG_INFO(...) SPDLOG_INFO(__VA_ARGS__)
#define LOG_WARN(...) SPDLOG_WARN(__VA_ARGS__)
#define LOG_ERROR(...) SPDLOG_ERROR(__VA_ARGS__)
#define LOG_FATAL(...) SPDLOG_CRITICAL(__VA_ARGS__)

/**
 * @brief 客户端专用日志宏
 * 添加[CLIENT]前缀，用于区分客户端日志
 */
#define CLIENT_INFO(...) SPDLOG_INFO("[CLIENT] {}", fmt::format(__VA_ARGS__))
#define CLIENT_WARN(...) SPDLOG_WARN("[CLIENT] {}", fmt::format(__VA_ARGS__))
#define CLIENT_ERROR(...) SPDLOG_ERROR("[CLIENT] {}", fmt::format(__VA_ARGS__))
#define CLIENT_DEBUG(...) SPDLOG_DEBUG("[CLIENT] {}", fmt::format(__VA_ARGS__))
#define CLIENT_FATAL(...) \
  SPDLOG_CRITICAL("[CLIENT] {}", fmt::format(__VA_ARGS__))

/**
 * @brief 线程ID格式化器
 * 在日志中格式化输出线程ID
 * fmt库中没有提供std::thread::id的特化,需要自己实现
 */
template <>
struct fmt::formatter<std::thread::id> : fmt::formatter<std::string> {
  template <typename FormatContext>
  auto format(const std::thread::id& id, FormatContext& ctx) const {
    std::ostringstream oss;
    oss << id;
    return fmt::formatter<std::string>::format(oss.str(), ctx);
  }
};

class Logger {
 public:
  // 获取Logger单例实例
  static Logger& GetInstance() {
    static Logger instance;
    return instance;
  }

  // 初始化日志系统
  bool Init(const std::string& log_dir = "logs") {
    try {
      // 创建日志目录
      std::filesystem::path log_path(log_dir);
      if (!std::filesystem::exists(log_path)) {
        if (!std::filesystem::create_directories(log_path)) {
          std::cerr << "Failed to create log directory: " << log_path
                    << std::endl;
          return false;
        }
      }

      // 创建控制台的文件日志记录器
      auto console_sink =
          std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
      if (!console_sink) {
        std::cerr << "Failed to create console sink" << console_sink
                  << std::endl;
        return false;
      }

      std::string log_file = log_dir + "/cookrpc.log";
      auto file_sink =
          std::make_shared<spdlog::sinks::daily_file_sink_mt>(log_file, 0, 0);
      if (!file_sink) {
        std::cerr << "Failed to create file sink" << file_sink << std::endl;
        return false;
      }

      // 将两个sink组合到一个logger中
      std::vector<spdlog::sink_ptr> sinks{console_sink, file_sink};
      logger_ =
          std::make_shared<spdlog::logger>("main", sinks.begin(), sinks.end());
      if (!logger_) {
        std::cerr << "Failed to create looger" << std::endl;
        return false;
      }

      // 设置日志格式
      logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
      logger_->set_level(spdlog::level::trace);

      // 设置为默认logger
      spdlog::set_default_logger(logger_);

      // 测试日志是否可写
      try {
        // LOG_INFO("Logger initialized successfully");
      } catch (const std::exception& e) {
        std::cerr << "Failed to write test log: " << e.what() << std::endl;
        return false;
      }

      return true;
    } catch (const std::exception& e) {
      std::cerr << "Logger initialization failed: " << e.what() << std::endl;
      return false;
    }
  }

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

 private:
  Logger() = default;
  ~Logger() = default;

  std::shared_ptr<spdlog::logger> logger_;  // spdlog日志记录实例
};

#endif  // RPC_TUTORIAL_UTILS_LOG_MANAGER_H_
