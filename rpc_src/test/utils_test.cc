#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "log_manager.h"
#include "utils_fun.h"

TEST(IpAddressTest, ConfigurationAndSocketParsingAgree) {
  for (const auto& ip : std::vector<std::string>{
           "0.0.0.0", "127.0.0.1", "255.255.255.255", "127.000.0.1", "01.2.3.4",
           "256.0.0.1", "127.0.0.1 ", "127.1", "", "::1"}) {
    in_addr address{};
    EXPECT_EQ(IsValidIpAddress(ip),
              inet_pton(AF_INET, ip.c_str(), &address) == 1)
        << ip;
  }
  EXPECT_FALSE(IsValidIpAddress(std::string("127.0.0.1\0bad", 13)));
}

TEST(LoggerTest, CrossingMidnightWritesToANewDatedFile) {
  const auto directory = std::filesystem::temp_directory_path() /
                         ("rpc-log-rollover-" + std::to_string(getpid()));
  const auto previous = spdlog::default_logger();
  struct Cleanup {
    std::filesystem::path path;
    std::shared_ptr<spdlog::logger> previous;
    ~Cleanup() {
      spdlog::set_default_logger(previous);
      std::error_code ignored;
      std::filesystem::remove_all(path, ignored);
    }
  } cleanup{directory, previous};
  ASSERT_TRUE(Logger::GetInstance().Init(directory.string()));
  const auto logger = spdlog::default_logger();
  logger->info("before-rollover");
  logger->flush();
  const spdlog::details::log_msg message(
      spdlog::log_clock::now() + std::chrono::hours(24), spdlog::source_loc{},
      "rollover", spdlog::level::info, "after-rollover");
  for (const auto& sink : logger->sinks()) {
    sink->log(message);
    sink->flush();
  }
  std::map<std::string, std::string> logs;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    std::ifstream file(entry.path());
    logs.emplace(entry.path().filename().string(),
                 std::string(std::istreambuf_iterator<char>(file), {}));
  }
  ASSERT_EQ(logs.size(), 2u);
  size_t before = 0, after = 0;
  for (const auto& log : logs) {
    const bool first = log.second.find("before-rollover") != std::string::npos;
    const bool second = log.second.find("after-rollover") != std::string::npos;
    EXPECT_NE(first, second);
    before += first;
    after += second;
  }
  EXPECT_EQ(before, 1u);
  EXPECT_EQ(after, 1u);
}
