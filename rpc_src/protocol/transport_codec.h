#ifndef RPC_TUTORIAL_RPC_SRC_PROTOCOL_TRANSPORT_CODEC_H_
#define RPC_TUTORIAL_RPC_SRC_PROTOCOL_TRANSPORT_CODEC_H_

#include <memory>
#include <string>

#include "aes_encrypt.h"
#include "rpc_options.h"
#include "rpc_protocol.h"

namespace cookrpc {
// CRP1 保留 12 字节头部，body 为一字节标志及压缩、加密后的数据。
// 不启用变换时保留 CRPC 格式；解码后恢复为业务协议使用的 CRPC 帧。
class TransportCodec {
 public:
  static constexpr uint32_t kMagic = 0x43525031U;
  // 角色决定认证方向，避免把请求密文反射为响应，或将响应冒充请求。
  enum class Role { kClient, kServer };
  explicit TransportCodec(TransportOptions options = {},
                          Role role = Role::kClient);
  bool Encrypted() const { return options_.encryption; }
  // 逻辑 body 和线上 body 均限制为 1 MiB，失败时清空输出。
  bool Encode(const std::string& plain_frame, std::string& wire_frame) const;
  bool Decode(const std::string& wire_frame, std::string& plain_frame) const;
  static bool IsMagic(uint32_t magic) {
    return magic == RpcHeader::MAGIC || magic == kMagic;
  }

 private:
  TransportOptions options_;
  Role role_;
  // 显式密钥在构造时校验并固定，后续环境变量变化不影响已建立的会话。
  std::shared_ptr<const AesEncrypt> cipher_;
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_PROTOCOL_TRANSPORT_CODEC_H_
