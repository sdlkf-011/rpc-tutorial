#ifndef RPC_TUTORIAL_RPC_SRC_ENCRYPT_AES_ENCRYPT_H_
#define RPC_TUTORIAL_RPC_SRC_ENCRYPT_AES_ENCRYPT_H_

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
  ~AesEncrypt() = default;

  // Base64 编解码
  std::string Base64Encode(const std::string& input) const;
  std::string Base64Decode(const std::string& input) const;

  // 移位加密/解密算法
  std::string ShiftEncrypt(const std::string& input, const std::string& key) const;
  std::string ShiftDecrypt(const std::string& input, const std::string& key) const;

  // 生成随机密钥(测试用)
  std::string GenerateKey() const;

  std::string master_key_; // 主密钥
  static constexpr size_t KEY_LENGTH = 32; // 会话密钥长度
  static constexpr size_t BLOCK_SIZE = 16; // 加密块大小
};
} // namespace cookrpc

#endif // RPC_TUTORIAL_RPC_SRC_ENCRYPT_AES_ENCRYPT_H_
