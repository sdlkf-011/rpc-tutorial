#include "transport_codec.h"

#include <cstdlib>
#include <stdexcept>

#include "zstd_compress.h"

namespace cookrpc {
namespace {
// 分别表示实际压缩、加密，以及发送方是否启用压缩模式。
// 小包即使没有实际压缩，也保留模式标记，以检测两端配置不一致。
constexpr unsigned char kCompressed = 1, kEncrypted = 2,
                        kCompressionEnabled = 4;
uint32_t Read32(const std::string& data, size_t offset) {
  // 按网络字节序逐字节读取，避免未对齐访问和主机字节序差异。
  const auto* p = reinterpret_cast<const unsigned char*>(data.data() + offset);
  return uint32_t(p[0]) << 24 | uint32_t(p[1]) << 16 | uint32_t(p[2]) << 8 |
         p[3];
}
void Append32(std::string& data, uint32_t value) {
  for (int shift : {24, 16, 8, 0}) data.push_back(char(value >> shift));
}
bool ValidFrame(const std::string& frame) {
  // 先检查完整头部与上限，再读取长度字段；拒绝截断帧和尾随数据。
  return frame.size() >= RpcHeader::kHeaderSize &&
         frame.size() <= RpcHeader::kMaxFrameSize &&
         Read32(frame, 4) == frame.size() - RpcHeader::kHeaderSize;
}
std::string Header(uint32_t magic, size_t length, uint32_t sequence) {
  std::string header;
  Append32(header, magic);
  Append32(header, static_cast<uint32_t>(length));
  Append32(header, sequence);
  return header;
}
}  // namespace
TransportCodec::TransportCodec(TransportOptions options, Role role)
    : options_(options), role_(role) {
  if (options.compression_threshold > RpcHeader::kMaxBodySize)
    throw std::invalid_argument("Invalid compression threshold");
  if (options.encryption) {
    // 跨进程通信必须显式提供共享密钥，不使用 AES 单例的随机缺省密钥。
    const char* key = std::getenv("COOKRPC_MASTER_KEY_HEX");
    if (!key)
      throw std::invalid_argument(
          "Encrypted RPC requires COOKRPC_MASTER_KEY_HEX");
    cipher_ = std::make_shared<AesEncrypt>(std::string(key));
  }
}
bool TransportCodec::Encode(const std::string& plain, std::string& wire) const {
  wire.clear();
  try {
    if (!ValidFrame(plain) || Read32(plain, 0) != RpcHeader::MAGIC)
      return false;
    if (!options_.compression && !options_.encryption) {
      wire = plain;
      return true;
    }
    std::string body = plain.substr(RpcHeader::kHeaderSize);
    unsigned char flags = options_.compression ? kCompressionEnabled : 0;
    if (options_.compression && body.size() >= options_.compression_threshold) {
      std::string compressed;
      if (!ZstdCompress::GetInstance().CompressString(body, compressed))
        return false;
      if (compressed.size() < body.size()) {
        // 仅在确有体积收益时标记实际压缩，接收方据此决定是否解压。
        body.swap(compressed);
        flags |= kCompressed;
      }
    }
    size_t encoded_size = body.size();
    if (options_.encryption) {
      flags |= kEncrypted;
      // 二进制密文额外包含版本、12 字节 nonce、16 字节 tag，共 29 字节。
      // 按 Base64 编码规则预估长度，在分配密文前检查线上 body 上限。
      encoded_size = 4 * ((body.size() + 29 + 2) / 3);
    }
    if (encoded_size + 1 > RpcHeader::kMaxBodySize) return false;
    std::string header = Header(kMagic, encoded_size + 1, Read32(plain, 8));
    header.push_back(char(flags));
    if (cipher_) {
      std::string encrypted;
      // 将方向、帧长、序列号和变换标志作为附加认证数据，和 body 一起认证。
      const std::string domain =
          role_ == Role::kClient ? "cookrpc/request/1" : "cookrpc/response/1";
      if (!cipher_->Encrypt(body, encrypted, domain + header)) return false;
      body.swap(encrypted);
    }
    wire = header + body;
    return true;
  } catch (...) {
    wire.clear();
    return false;
  }
}
bool TransportCodec::Decode(const std::string& wire, std::string& plain) const {
  plain.clear();
  try {
    if (!ValidFrame(wire)) return false;
    if (!options_.compression && !options_.encryption) {
      if (Read32(wire, 0) != RpcHeader::MAGIC) return false;
      plain = wire;
      return true;
    }
    if (Read32(wire, 0) != kMagic || wire.size() <= RpcHeader::kHeaderSize + 1)
      return false;
    const auto flags = static_cast<unsigned char>(wire[RpcHeader::kHeaderSize]);
    // 拒绝未知标志和模式不匹配，尤其不允许加密连接静默降级为明文。
    if ((flags & ~7U) || bool(flags & kEncrypted) != options_.encryption ||
        bool(flags & kCompressionEnabled) != options_.compression ||
        ((flags & kCompressed) && !options_.compression))
      return false;
    std::string body = wire.substr(RpcHeader::kHeaderSize + 1);
    if (cipher_) {
      // 先验证认证标签，再允许解压和业务解析，失败输出不能被消费。
      std::string decrypted;
      const std::string domain =
          role_ == Role::kClient ? "cookrpc/response/1" : "cookrpc/request/1";
      if (!cipher_->Decrypt(
              body, decrypted,
              domain + wire.substr(0, RpcHeader::kHeaderSize + 1)))
        return false;
      body.swap(decrypted);
    }
    if (flags & kCompressed) {
      // 在分配解压缓冲区之前检查声明大小，防止小压缩包绕过内存上限。
      std::string decompressed;
      if (!ZstdCompress::GetInstance().DecompressString(
              body, decompressed, RpcHeader::kMaxBodySize))
        return false;
      body.swap(decompressed);
    }
    if (body.size() > RpcHeader::kMaxBodySize) return false;
    // 业务协议只接收原始格式，传输变换和业务字段解析保持分离。
    plain = Header(RpcHeader::MAGIC, body.size(), Read32(wire, 8)) + body;
    return true;
  } catch (...) {
    plain.clear();
    return false;
  }
}
}  // namespace cookrpc
