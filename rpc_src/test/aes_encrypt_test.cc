#include "aes_encrypt.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

namespace cookrpc {
namespace {

// AES 单元测试模块：覆盖单例、输入校验、文本/二进制往返及认证完整性。
// 验证加密器通过单例接口返回同一个对象。
TEST(AesEncryptTest, ReturnsSameSingletonInstance) {
  AesEncrypt& first = AesEncrypt::GetInstance();
  AesEncrypt& second = AesEncrypt::GetInstance();

  EXPECT_EQ(&first, &second);
}

// 验证空明文被拒绝且不会生成密文。
TEST(AesEncryptTest, RejectsEmptyPlaintext) {
  std::string ciphertext;

  EXPECT_FALSE(AesEncrypt::GetInstance().Encrypt("", ciphertext));
}

// 验证空输入、错误内容和非法 Base64 密文均会被拒绝。
TEST(AesEncryptTest, RejectsInvalidCiphertexts) {
  std::string plaintext;
  AesEncrypt& encryptor = AesEncrypt::GetInstance();

  EXPECT_FALSE(encryptor.Decrypt("", plaintext));
  EXPECT_FALSE(encryptor.Decrypt("Y29va3JwYw==", plaintext));
  EXPECT_FALSE(encryptor.Decrypt(std::string(48, '!'), plaintext));
}

// 验证 ASCII 与 UTF-8 文本加密后能够完整解密还原。
TEST(AesEncryptTest, RoundTripsTextPayloads) {
  const std::vector<std::string> payloads = {
      "a", "ab", "abc", "CookRPC encryption unit test", u8"支持 UTF-8 文本",
  };
  AesEncrypt& encryptor = AesEncrypt::GetInstance();

  for (const std::string& payload : payloads) {
    SCOPED_TRACE("payload_size=" + std::to_string(payload.size()));
    std::string ciphertext;
    std::string plaintext;

    ASSERT_TRUE(encryptor.Encrypt(payload, ciphertext));
    ASSERT_TRUE(encryptor.Decrypt(ciphertext, plaintext));
    EXPECT_EQ(plaintext, payload);
  }
}

// 验证包含空字节和高位字节的二进制载荷可无损往返。
TEST(AesEncryptTest, RoundTripsBinaryPayload) {
  std::string payload;
  payload.reserve(512);
  // 连续写入两轮全部字节值，覆盖空字节和最高位字节。
  for (std::size_t index = 0; index < 512; ++index) {
    payload.push_back(static_cast<char>(index % 256));
  }

  std::string ciphertext;
  std::string plaintext;
  AesEncrypt& encryptor = AesEncrypt::GetInstance();

  ASSERT_TRUE(encryptor.Encrypt(payload, ciphertext));
  ASSERT_TRUE(encryptor.Decrypt(ciphertext, plaintext));
  EXPECT_EQ(plaintext, payload);
}

// 验证相同明文因随机 nonce 产生不同密文且都能解密。
TEST(AesEncryptTest, ProducesIndependentCiphertexts) {
  const std::string payload = "the same plaintext";
  std::string first_ciphertext;
  std::string second_ciphertext;
  AesEncrypt& encryptor = AesEncrypt::GetInstance();

  ASSERT_TRUE(encryptor.Encrypt(payload, first_ciphertext));
  ASSERT_TRUE(encryptor.Encrypt(payload, second_ciphertext));

  // 每次加密都应使用独立随机 nonce，避免相同明文产生相同密文。
  EXPECT_NE(first_ciphertext, second_ciphertext);

  std::string first_plaintext;
  std::string second_plaintext;
  ASSERT_TRUE(encryptor.Decrypt(first_ciphertext, first_plaintext));
  ASSERT_TRUE(encryptor.Decrypt(second_ciphertext, second_plaintext));
  EXPECT_EQ(first_plaintext, payload);
  EXPECT_EQ(second_plaintext, payload);
}

// 验证篡改密文会触发 GCM 认证失败并清空明文输出。
TEST(AesEncryptTest, RejectsTamperedCiphertext) {
  std::string ciphertext;
  AesEncrypt& encryptor = AesEncrypt::GetInstance();
  ASSERT_TRUE(encryptor.Encrypt("authenticated payload", ciphertext));
  ASSERT_GT(ciphertext.size(), 8U);

  const size_t changed_index = ciphertext.size() / 2;
  ciphertext[changed_index] = ciphertext[changed_index] == 'A' ? 'B' : 'A';
  std::string plaintext = "stale plaintext";

  EXPECT_FALSE(encryptor.Decrypt(ciphertext, plaintext));
  EXPECT_TRUE(plaintext.empty());
}

// 验证输出使用合法 Base64 字符集并满足四字符对齐。
TEST(AesEncryptTest, ProducesBase64Ciphertext) {
  const std::string base64_alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
  std::string ciphertext;

  ASSERT_TRUE(AesEncrypt::GetInstance().Encrypt("CookRPC", ciphertext));
  EXPECT_EQ(ciphertext.size() % 4, 0U);
  EXPECT_TRUE(std::all_of(
      ciphertext.begin(), ciphertext.end(), [&base64_alphabet](char character) {
        return base64_alphabet.find(character) != std::string::npos;
      }));
}

}  // namespace
}  // namespace cookrpc
