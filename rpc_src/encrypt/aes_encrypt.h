#ifndef RPC_TUTORIAL_RPC_SRC_ENCRYPT_AES_ENCRYPT_H_
#define RPC_TUTORIAL_RPC_SRC_ENCRYPT_AES_ENCRYPT_H_

#include <array>
#include <cstddef>
#include <string>

namespace cookrpc {

class AesEncrypt {
 public:
  static AesEncrypt& GetInstance() {
    static AesEncrypt instance;
    return instance;
  }

  // 加密数据
  bool Encrypt(const std::string& data, std::string& ciphertext);

  // 解密数据
  bool Decrypt(const std::string& encrypted_data, std::string& plaintext);

  AesEncrypt(const AesEncrypt&) = delete;
  AesEncrypt& operator=(const AesEncrypt&) = delete;

 private:
  AesEncrypt();
  ~AesEncrypt();

  // Base64 编解码
  std::string Base64Encode(const std::string& input) const;
  std::string Base64Decode(const std::string& input) const;

  static constexpr size_t KEY_LENGTH = 32;              // AES-256 密钥长度
  std::array<unsigned char, KEY_LENGTH> master_key_{};  // AES-256 主密钥
  bool key_initialized_ = false;
  static constexpr size_t NONCE_LENGTH = 12;  // GCM 推荐随机 nonce 长度
  static constexpr size_t TAG_LENGTH = 16;    // GCM 认证标签长度
};
}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_ENCRYPT_AES_ENCRYPT_H_
