#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "request_controls.h"
#include "transport_codec.h"
#include "zstd_compress.h"

namespace cookrpc {
namespace {
using namespace std::chrono_literals;
// 使用固定测试密钥，并恢复调用环境，避免测试顺序影响其他密钥场景。
struct KeyEnvironment {
  std::optional<std::string> previous;
  KeyEnvironment() {
    if (const char* value = std::getenv("COOKRPC_MASTER_KEY_HEX"))
      previous = value;
    setenv("COOKRPC_MASTER_KEY_HEX", std::string(64, '1').c_str(), 1);
  }
  ~KeyEnvironment() {
    if (previous)
      setenv("COOKRPC_MASTER_KEY_HEX", previous->c_str(), 1);
    else
      unsetenv("COOKRPC_MASTER_KEY_HEX");
  }
};
// 重复字节确保有明显压缩收益，同时保留真实的请求字段和序列号。
std::string Frame() {
  RpcRequest request;
  request.SetSequenceId(37);
  request.SetServiceName("service");
  request.SetMethodName("Echo");
  request.SetPayload(std::string(16000, 'x'));
  std::string frame;
  EXPECT_TRUE(request.Serialize(frame));
  return frame;
}
TEST(TransportTest, PlainCompressionEncryptionAndCombinedRoundTrip) {
  KeyEnvironment key;
  const auto plain = Frame();
  for (bool compression : {false, true}) {
    for (bool encryption : {false, true}) {
      TransportCodec codec({compression, encryption, 1024});
      TransportCodec server({compression, encryption, 1024},
                            TransportCodec::Role::kServer);
      std::string wire, decoded;
      ASSERT_TRUE(codec.Encode(plain, wire));
      ASSERT_TRUE(server.Decode(wire, decoded));
      EXPECT_EQ(decoded, plain);
      if (compression) {
        EXPECT_LT(wire.size(), plain.size());
      }
      if (encryption) {
        EXPECT_EQ(wire.find(std::string(64, 'x')), std::string::npos);
      }
      ASSERT_TRUE(server.Encode(plain, wire));
      ASSERT_TRUE(codec.Decode(wire, decoded));
      EXPECT_EQ(decoded, plain);
    }
  }
}
TEST(TransportTest, RejectsModifiedSequenceFlagsCiphertextAndWrongKey) {
  KeyEnvironment key;
  TransportCodec codec({true, true, 1024});
  TransportCodec server({true, true, 1024}, TransportCodec::Role::kServer);
  std::string wire, decoded;
  ASSERT_TRUE(codec.Encode(Frame(), wire));
  ASSERT_TRUE(server.Decode(wire, decoded));
  // 客户端只接收响应方向的认证数据，不能把请求密文反射为响应。
  EXPECT_FALSE(codec.Decode(wire, decoded));
  for (size_t position : {size_t{8}, size_t{12}, wire.size() - 5}) {
    auto modified = wire;
    modified[position] ^= 1;
    decoded = "stale";
    EXPECT_FALSE(server.Decode(modified, decoded));
    EXPECT_TRUE(decoded.empty());
  }
  setenv("COOKRPC_MASTER_KEY_HEX", std::string(64, '2').c_str(), 1);
  TransportCodec wrong_key({true, true, 1024}, TransportCodec::Role::kServer);
  EXPECT_FALSE(wrong_key.Decode(wire, decoded));
  EXPECT_TRUE(decoded.empty());
}
TEST(TransportTest, RejectsMissingInvalidKeysAndModeMismatchWithoutDowngrade) {
  KeyEnvironment key;
  unsetenv("COOKRPC_MASTER_KEY_HEX");
  EXPECT_THROW(TransportCodec(TransportOptions{false, true, 1024}),
               std::invalid_argument);
  setenv("COOKRPC_MASTER_KEY_HEX", "invalid", 1);
  EXPECT_THROW(TransportCodec(TransportOptions{false, true, 1024}),
               std::invalid_argument);
  setenv("COOKRPC_MASTER_KEY_HEX", std::string(64, '1').c_str(), 1);
  TransportCodec secured({true, true, 1024}), plain;
  std::string wire, decoded;
  ASSERT_TRUE(secured.Encode(Frame(), wire));
  EXPECT_FALSE(plain.Decode(wire, decoded));
  EXPECT_FALSE(secured.Decode(Frame(), decoded));
}
TEST(TransportTest, RejectsDecompressionBombAndWireSizeOverflow) {
  TransportCodec codec({true, false, 0});
  std::string compressed;
  // 线上压缩包很小，但声明的解压结果超过上限，必须在分配前拒绝。
  ASSERT_TRUE(ZstdCompress::GetInstance().CompressString(
      std::string(RpcHeader::kMaxBodySize + 1, 'x'), compressed));
  auto plain = Frame();
  std::string wire, decoded;
  ASSERT_TRUE(codec.Encode(plain, wire));
  wire.resize(13);
  wire += compressed;
  const auto length = uint32_t(wire.size() - 12);
  for (size_t i = 0; i < 4; ++i) wire[4 + i] = char(length >> (24 - 8 * i));
  EXPECT_FALSE(codec.Decode(wire, decoded));
  KeyEnvironment key;
  TransportCodec encrypted({false, true, 1024});
  RpcResponse response;
  response.SetResultData(std::string(RpcHeader::kMaxBodySize - 12, 'x'));
  ASSERT_TRUE(response.Serialize(plain));
  EXPECT_FALSE(encrypted.Encode(plain, wire));
  EXPECT_TRUE(wire.empty());
}
TEST(RequestLimiterTest, BurstRefillAndDisableUseMonotonicTime) {
  // 注入单调时间验证补充边界，避免依赖线程调度或真实等待。
  const auto start = RequestLimiter::Clock::time_point{};
  RequestLimiter limiter({2, 2}, start);
  EXPECT_TRUE(limiter.TryAcquire(start));
  EXPECT_TRUE(limiter.TryAcquire(start));
  EXPECT_FALSE(limiter.TryAcquire(start));
  EXPECT_FALSE(limiter.TryAcquire(start + 499ms));
  EXPECT_TRUE(limiter.TryAcquire(start + 500ms));
  EXPECT_FALSE(limiter.TryAcquire(start + 500ms));
  EXPECT_TRUE(limiter.TryAcquire(start + 100s));
  EXPECT_TRUE(limiter.TryAcquire(start + 100s));
  EXPECT_FALSE(limiter.TryAcquire(start + 100s));
  RequestLimiter disabled;
  for (int i = 0; i < 100; ++i) EXPECT_TRUE(disabled.TryAcquire(start));
}
TEST(CircuitBreakerTest, EndpointIsolationSingleProbeAndRecovery) {
  // 验证 A 熔断不会影响 B，半开只允许一个探测，失败后重新冷却。
  const auto start = CircuitBreaker::Clock::time_point{};
  CircuitBreaker breaker({2, 1000});
  for (int i = 0; i < 2; ++i) {
    ASSERT_TRUE(breaker.TryAcquire("A", start));
    breaker.Finish("A", CircuitBreaker::Outcome::kFailure, start);
  }
  EXPECT_FALSE(breaker.TryAcquire("A", start + 999ms));
  EXPECT_TRUE(breaker.TryAcquire("B", start));
  breaker.Finish("B", CircuitBreaker::Outcome::kHealthy, start);
  EXPECT_TRUE(breaker.TryAcquire("A", start + 1000ms));
  EXPECT_FALSE(breaker.TryAcquire("A", start + 1000ms));
  breaker.Finish("A", CircuitBreaker::Outcome::kFailure, start + 1000ms);
  EXPECT_FALSE(breaker.Available("A", start + 1999ms));
  EXPECT_TRUE(breaker.TryAcquire("A", start + 2000ms));
  breaker.Finish("A", CircuitBreaker::Outcome::kHealthy, start + 2000ms);
  EXPECT_TRUE(breaker.TryAcquire("A", start + 2000ms));
}
TEST(CircuitBreakerTest, NeutralProbeCanRetryAndMissingEndpointsAreForgotten) {
  const auto start = CircuitBreaker::Clock::time_point{};
  CircuitBreaker breaker({1, 1000});
  ASSERT_TRUE(breaker.TryAcquire("A", start));
  breaker.Finish("A", CircuitBreaker::Outcome::kFailure, start);
  ASSERT_TRUE(breaker.TryAcquire("A", start + 1000ms));
  breaker.Finish("A", CircuitBreaker::Outcome::kNeutral, start + 1000ms);
  EXPECT_TRUE(breaker.TryAcquire("A", start + 1000ms));
  breaker.ForgetMissing({"B"});
  EXPECT_TRUE(breaker.TryAcquire("A", start));
}
TEST(RuntimeOptionsTest, RejectsWrongTypesAndInvalidEnabledLimits) {
  EXPECT_THROW(TransportOptions::Parse({{"compression", 1}}),
               std::invalid_argument);
  EXPECT_THROW(TransportOptions::Parse({{"compression_threshold", -1}}),
               std::invalid_argument);
  EXPECT_THROW(
      RateLimitOptions::Parse({{"requests_per_second", 1}, {"burst", 0}}),
      std::invalid_argument);
  EXPECT_THROW(CircuitBreakerOptions::Parse({{"failure_threshold", 1.5}}),
               std::invalid_argument);
  EXPECT_THROW(CircuitBreakerOptions::Parse({{"reset_timeout_ms", 0}}),
               std::invalid_argument);
}
}  // namespace
}  // namespace cookrpc
