#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <string>
#include <vector>

#include "compress_data/zstd_compress.h"

namespace cookrpc {
namespace {

TEST(ZstdCompressTest, GetInstanceReturnsSameObject) {
  // 单例在进程生命周期内必须保持唯一，避免压缩器状态被重复创建。
  ZstdCompress& first = ZstdCompress::GetInstance();
  ZstdCompress& second = ZstdCompress::GetInstance();

  EXPECT_EQ(&first, &second);
}

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

TEST(ZstdCompressTest, EveryCompressionLevelSupportsRoundTrip) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  const std::array<Level, 5> levels = {
      Level::kFastest,
      Level::kDefault,
      Level::kBetter,
      Level::kBest,
      Level::kMax,
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

TEST(ZstdCompressTest, BinaryRoundTripPreservesEveryByteValue) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  std::vector<char> original(256);
  for (std::size_t index = 0; index < original.size(); ++index) {
    original[index] = static_cast<char>(index);
  }
  std::vector<char> compressed;
  std::vector<char> decompressed;

  // 0 到 255 的全部字节值可覆盖文本测试无法覆盖的任意二进制内容。
  ASSERT_TRUE(compressor.CompressData(original.data(), original.size(),
                                      compressed));
  ASSERT_FALSE(compressed.empty());
  EXPECT_LE(compressed.size(), compressor.GetCompressBound(original.size()));
  ASSERT_TRUE(compressor.DecompressData(compressed.data(), compressed.size(),
                                        decompressed));
  EXPECT_EQ(decompressed, original);
}

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

TEST(ZstdCompressTest, CompressBoundCanHoldCompressedOutput) {
  ZstdCompress& compressor = ZstdCompress::GetInstance();
  const std::string original(1024, 'z');
  std::string compressed;

  ASSERT_TRUE(compressor.CompressString(original, compressed));
  EXPECT_LE(compressed.size(), compressor.GetCompressBound(original.size()));
}

}  // namespace
}  // namespace cookrpc
