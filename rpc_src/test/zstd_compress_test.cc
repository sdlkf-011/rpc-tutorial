#include "zstd_compress.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <string>
#include <vector>

namespace cookrpc {
namespace {

// ZSTD 压缩测试模块：覆盖单例、文本/二进制往返、级别选择和非法输入。
// 验证压缩器单例接口在多次获取时返回同一个对象。
TEST(ZstdCompressTest, GetInstanceReturnsSameObject) {
  // 单例在进程生命周期内必须保持唯一，避免压缩器状态被重复创建。
  ZstdCompress& first = ZstdCompress::GetInstance();
  ZstdCompress& second = ZstdCompress::GetInstance();

  EXPECT_EQ(&first, &second);
}

// 验证空字符串压缩和解压成功，并清空调用方目标缓冲区。
TEST(ZstdCompressTest, EmptyStringClearsDestination) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  std::string compressed = "stale compressed data";
  std::string decompressed = "stale decompressed data";

  // 空输入应被视为合法数据，并清理调用方复用的目标缓冲区。
  EXPECT_TRUE(compressor.CompressString("", compressed));
  EXPECT_TRUE(compressed.empty());
  EXPECT_TRUE(compressor.DecompressString("", decompressed));
  EXPECT_TRUE(decompressed.empty());
}

// 验证文本及嵌入空字节的字符串能够无损压缩和解压。
TEST(ZstdCompressTest, StringRoundTripPreservesTextAndEmbeddedNulls) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  std::string original = "cookrpc 中文数据";
  original.append("\0binary\0tail", 12);
  std::string compressed;
  std::string decompressed;

  // std::string 可能包含 '\0'，往返测试需要同时验证长度和逐字节内容。
  ASSERT_TRUE(compressor.CompressString(original, compressed));
  ASSERT_FALSE(compressed.empty());
  ASSERT_TRUE(compressor.DecompressString(compressed, decompressed));
  EXPECT_EQ(decompressed, original);
}

// 验证全部公开压缩级别都能生成可正确解压的 ZSTD 数据。
TEST(ZstdCompressTest, EveryCompressionLevelSupportsRoundTrip) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  const std::array<Level, 5> levels = {
      Level::kFastest, Level::kDefault, Level::kBetter,
      Level::kBest,    Level::kMax,
  };
  const std::string original(4096, 'a');

  // 每个公开压缩级别都应生成可被同一封装正确解压的 ZSTD 帧。
  for (const Level level : levels) {
    SCOPED_TRACE(static_cast<int>(level));
    std::string compressed;
    std::string decompressed;

    ASSERT_TRUE(compressor.CompressString(original, compressed, level));
    ASSERT_TRUE(compressor.DecompressString(compressed, decompressed));
    EXPECT_EQ(decompressed, original);
  }
}

// 验证二进制数据的每种字节值都能在压缩往返后保持不变。
TEST(ZstdCompressTest, BinaryRoundTripPreservesEveryByteValue) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  std::vector<char> original(256);
  for (std::size_t index = 0; index < original.size(); ++index) {
    original[index] = static_cast<char>(index);
  }
  std::vector<char> compressed;
  std::vector<char> decompressed;

  // 0 到 255 的全部字节值可覆盖文本测试无法覆盖的任意二进制内容。
  ASSERT_TRUE(
      compressor.CompressData(original.data(), original.size(), compressed));
  ASSERT_FALSE(compressed.empty());
  EXPECT_LE(compressed.size(), compressor.GetCompressBound(original.size()));
  ASSERT_TRUE(compressor.DecompressData(compressed.data(), compressed.size(),
                                        decompressed));
  EXPECT_EQ(decompressed, original);
}

// 验证空指针或零长度二进制输入安全处理并清空输出。
TEST(ZstdCompressTest, EmptyBinaryInputClearsDestination) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  const char placeholder = 'x';
  std::vector<char> destination = {'s', 't', 'a', 'l', 'e'};

  // 空指针或零长度都表示空数据，且不应读取传入的地址。
  EXPECT_TRUE(compressor.CompressData(nullptr, 1, destination));
  EXPECT_TRUE(destination.empty());
  destination.assign({'s', 't', 'a', 'l', 'e'});
  EXPECT_TRUE(compressor.CompressData(&placeholder, 0, destination));
  EXPECT_TRUE(destination.empty());
  destination.assign({'s', 't', 'a', 'l', 'e'});
  EXPECT_TRUE(compressor.DecompressData(nullptr, 1, destination));
  EXPECT_TRUE(destination.empty());
  destination.assign({'s', 't', 'a', 'l', 'e'});
  EXPECT_TRUE(compressor.DecompressData(&placeholder, 0, destination));
  EXPECT_TRUE(destination.empty());
}

// 验证非 ZSTD 帧输入被拒绝，不会伪装成成功解压结果。
TEST(ZstdCompressTest, RejectsInvalidCompressedInput) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  const std::string invalid_frame = "this is not a zstd frame";
  std::string string_output;
  std::vector<char> data_output;

  // 非 ZSTD 帧必须显式失败，避免调用方把无效数据当作成功结果使用。
  EXPECT_FALSE(compressor.DecompressString(invalid_frame, string_output));
  EXPECT_FALSE(compressor.DecompressData(invalid_frame.data(),
                                         invalid_frame.size(), data_output));
}

// 验证压缩结果不会超过库报告的最大边界。
TEST(ZstdCompressTest, CompressBoundCanHoldCompressedOutput) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  const std::string original(1024, 'z');
  std::string compressed;

  ASSERT_TRUE(compressor.CompressString(original, compressed));
  EXPECT_LE(compressed.size(), compressor.GetCompressBound(original.size()));
}

}  // namespace
}  // namespace cookrpc

TEST(ZstdDecompressionLimits, RejectsHugeDeclaredLengthBeforeAllocating) {
  std::string frame("\x28\xb5\x2f\xfd\xe0", 5);
  const uint64_t size = uint64_t{1} << 40;
  for (int i = 0; i < 8; ++i)
    frame.push_back(static_cast<char>(size >> (8 * i)));
  frame.append("\1\0\0", 3);
  std::string output = "stale";
  bool ok = true;
  EXPECT_NO_THROW(ok = cookrpc::ZstdCompress::GetInstance().DecompressString(
                      frame, output));
  EXPECT_FALSE(ok);
  EXPECT_TRUE(output.empty());
}

TEST(ZstdDecompressionLimits, AcceptsBoundaryAndRejectsOversizedOutput) {
  auto& codec = cookrpc::ZstdCompress::GetInstance();
  std::string compressed, output;
  ASSERT_TRUE(codec.CompressString(std::string(1024 * 1024, 'x'), compressed));
  ASSERT_TRUE(codec.DecompressString(compressed, output));
  EXPECT_EQ(output.size(), 1024 * 1024u);
  ASSERT_TRUE(
      codec.CompressString(std::string(1024 * 1024 + 1, 'x'), compressed));
  EXPECT_FALSE(codec.DecompressString(compressed, output));
  EXPECT_TRUE(output.empty());
  std::vector<char> binary;
  EXPECT_FALSE(
      codec.DecompressData(compressed.data(), compressed.size(), binary));
  EXPECT_TRUE(binary.empty());
}

TEST(ZstdDecompressionLimits, CallerCanChooseABoundedOutputLimit) {
  auto& codec = cookrpc::ZstdCompress::GetInstance();
  std::string compressed, output;
  ASSERT_TRUE(codec.CompressString(std::string(256, 'x'), compressed));
  EXPECT_FALSE(codec.DecompressString(compressed, output, 255));
  EXPECT_TRUE(output.empty());
  ASSERT_TRUE(codec.DecompressString(compressed, output, 256));
  EXPECT_EQ(output.size(), 256u);
  std::vector<char> binary;
  EXPECT_FALSE(
      codec.DecompressData(compressed.data(), compressed.size(), binary, 255));
  ASSERT_TRUE(
      codec.DecompressData(compressed.data(), compressed.size(), binary, 256));
  EXPECT_EQ(binary.size(), 256u);
}
