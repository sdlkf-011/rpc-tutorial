#include "zstd_compress.h"

#include <cstddef>
#include <string>
#include <zstd.h>

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
  size_t const actual_size = ZSTD_compress(
    dst.data(), 
    compressed_size, 
    src.data(), 
    src.size(), 
    static_cast<int>(level)
  );

  // 检查压缩是否成功
  if (ZSTD_isError(actual_size)) {
    LOG_ERROR("ZSTD compression failed: {}", ZSTD_getErrorName(actual_size));
    return false;
  }

  // 调整目标字符串为实际压缩后的大小
  dst.resize(actual_size);
  return true;
}

bool ZstdCompress::DecompressString(const std::string& src, std::string& dst) {
  // 处理空字符串
  if (src.empty()) {
    dst.clear();
    return true;
  }

  // 获取解压后的原始数据大小
  unsigned long long const decompressed_size = 
    ZSTD_getFrameContentSize(src.data(), src.size());
  
  // 检查是否能够获取有效的解压大小
  if (decompressed_size == ZSTD_CONTENTSIZE_ERROR || 
      decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
    return false;
  }

  // 分配足够的空间用于解压
  dst.resize(decompressed_size);

  // 执行 ZSTD 解压
  size_t const actual_size = ZSTD_decompress(
    dst.data(), 
    decompressed_size, 
    src.data(), 
    src.size()
  );

  // 检查解压是否成功
  if (ZSTD_isError(actual_size)) {
    return false;
  }

  // 调整目标字符串为实际压缩后的大小
  dst.resize(actual_size);
  return true;
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
  size_t const actual_size = ZSTD_compress(
    dst.data(), 
    compressed_size, 
    src, 
    src_len, 
    static_cast<int>(level)
  );

  // 检查压缩结果
  if (ZSTD_isError(actual_size)) {
    return false;
  }

  // 调整容器大小为实际压缩后的大小
  dst.resize(actual_size);
  return true;
}

bool ZstdCompress::DecompressData(const char* src, size_t src_len,
                      std::vector<char>& dst) {
  // 处理空数据或空指针
  if (!src || src_len == 0) {
    dst.clear();
    return true;
  }

  // 获取解压后的原始数据大小
  unsigned long long const decompressed_size = 
    ZSTD_getFrameContentSize(src, src_len);
  
  // 检查有效性
  if (decompressed_size == ZSTD_CONTENTSIZE_ERROR || 
      decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
    return false;
  }

  // 分配空间
  dst.resize(decompressed_size);

  // 执行 ZSTD 解压
  size_t const actual_size = ZSTD_decompress(
    dst.data(),
    decompressed_size,
    src,
    src_len
  );

  // 检查解压是否成功
  if (ZSTD_isError(actual_size)) {
    return false;
  }

  // 调整容器大小
  dst.resize(actual_size);
  return true;
}
} // namespace cookrpc
