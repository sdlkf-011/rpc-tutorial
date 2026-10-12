#include "zstd_compress.h"

#include <zstd.h>

#include <cstddef>
#include <limits>
#include <string>

#include "log_manager.h"

namespace cookrpc {

ZstdCompress::~ZstdCompress() = default;

bool ZstdCompress::CompressString(const std::string& src, std::string& dst,
                                  Level level) {
  // 处理空字符串
  if (src.empty()) {
    dst.clear();
    return true;
  }

  // 计算压缩后的最大可能大小
  size_t const compressed_size = ZSTD_compressBound(src.size());

  // 预分配目标缓冲区
  dst.resize(compressed_size);

  // 执行 ZSTD 压缩
  size_t const actual_size =
      ZSTD_compress(dst.data(), compressed_size, src.data(), src.size(),
                    static_cast<int>(level));

  // 检查压缩是否成功
  if (ZSTD_isError(actual_size)) {
    LOG_ERROR("ZSTD compression failed: {}", ZSTD_getErrorName(actual_size));
    return false;
  }

  // 调整目标字符串为实际压缩后的大小
  dst.resize(actual_size);
  return true;
}

bool ZstdCompress::DecompressString(const std::string& src, std::string& dst,
                                    size_t max_output_size) {
  try {
    if (src.empty()) {
      dst.clear();
      return true;
    }
    const auto size = ZSTD_getFrameContentSize(src.data(), src.size());
    if (size == ZSTD_CONTENTSIZE_ERROR || size == ZSTD_CONTENTSIZE_UNKNOWN ||
        size > max_output_size || size > std::numeric_limits<size_t>::max()) {
      dst.clear();
      return false;
    }
    std::string output(static_cast<size_t>(size), '\0');
    const auto actual =
        ZSTD_decompress(output.data(), output.size(), src.data(), src.size());
    if (ZSTD_isError(actual) || actual != size) {
      dst.clear();
      return false;
    }
    dst.swap(output);
    return true;
  } catch (const std::exception&) {
    dst.clear();
    return false;
  }
}

bool ZstdCompress::CompressData(const char* src, size_t src_len,
                                std::vector<char>& dst, Level level) {
  // 处理空数据或空指针
  if (!src || src_len == 0) {
    dst.clear();
    return true;
  }

  // 计算压缩后的最大可能大小
  size_t const compressed_size = ZSTD_compressBound(src_len);
  dst.resize(compressed_size);

  // 执行压缩
  size_t const actual_size = ZSTD_compress(dst.data(), compressed_size, src,
                                           src_len, static_cast<int>(level));

  // 检查压缩结果
  if (ZSTD_isError(actual_size)) {
    return false;
  }

  // 调整容器大小为实际压缩后的大小
  dst.resize(actual_size);
  return true;
}

bool ZstdCompress::DecompressData(const char* src, size_t src_len,
                                  std::vector<char>& dst,
                                  size_t max_output_size) {
  try {
    if (!src || src_len == 0) {
      dst.clear();
      return true;
    }
    const auto size = ZSTD_getFrameContentSize(src, src_len);
    if (size == ZSTD_CONTENTSIZE_ERROR || size == ZSTD_CONTENTSIZE_UNKNOWN ||
        size > max_output_size || size > std::numeric_limits<size_t>::max()) {
      dst.clear();
      return false;
    }
    std::vector<char> output(static_cast<size_t>(size));
    const auto actual =
        ZSTD_decompress(output.data(), output.size(), src, src_len);
    if (ZSTD_isError(actual) || actual != size) {
      dst.clear();
      return false;
    }
    dst.swap(output);
    return true;
  } catch (const std::exception&) {
    dst.clear();
    return false;
  }
}
}  // namespace cookrpc
