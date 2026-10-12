#ifndef RPC_TUTORIAL_RPC_SRC_COMPRESS_DATA_ZSTD_COMPRESS_H_
#define RPC_TUTORIAL_RPC_SRC_COMPRESS_DATA_ZSTD_COMPRESS_H_

#include <zstd.h>

#include <cstddef>
#include <string>
#include <vector>

namespace cookrpc {

// ZSTD 压缩级别
enum class Level {
  kFastest = 1,  // 最快速
  kDefault = 3,  // 默认级别
  kBetter = 7,   // 较好压缩率
  kBest = 19,    // 最佳压缩率
  kMax = 22      // 最大压缩率
};

// Zstandard 压缩封装类
class ZstdCompress {
 public:
  static constexpr size_t kDefaultMaxOutputSize = 1024 * 1024;
  static ZstdCompress& GetInstance() {
    static ZstdCompress instance;
    return instance;
  }

  // 压缩字符串
  bool CompressString(const std::string& src, std::string& dst,
                      Level level = Level::kDefault);

  // 分配前检查输出上限；解压失败（包括分配异常）清空 dst 并返回 false。
  bool DecompressString(const std::string& src, std::string& dst,
                        size_t max_output_size = kDefaultMaxOutputSize);

  // 压缩二进制数据
  bool CompressData(const char* src, size_t src_len, std::vector<char>& dst,
                    Level level = Level::kDefault);

  // 解压二进制数据
  bool DecompressData(const char* src, size_t src_len, std::vector<char>& dst,
                      size_t max_output_size = kDefaultMaxOutputSize);

  // 获取压缩后的最大可能大小
  size_t GetCompressBound(size_t src_size) const {
    return ZSTD_compressBound(src_size);
  }

  ZstdCompress(const ZstdCompress&) = delete;
  ZstdCompress& operator=(const ZstdCompress&) = delete;
  ZstdCompress(ZstdCompress&&) = delete;
  ZstdCompress& operator=(ZstdCompress&&) = delete;

 private:
  ZstdCompress() = default;
  ~ZstdCompress();
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_COMPRESS_DATA_ZSTD_COMPRESS_H_
