#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "aes_encrypt.h"

namespace cookrpc {
namespace {

TEST(AesEncryptTest, ReturnsSameSingletonInstance) {
  AesEncrypt& first = AesEncrypt::GetInstance();
  AesEncrypt& second = AesEncrypt::GetInstance();

  EXPECT_EQ(&first, &second);
}

TEST(AesEncryptTest, RejectsEmptyPlaintext) {
  std::string ciphertext;

  EXPECT_FALSE(AesEncrypt::GetInstance().Encrypt("", ciphertext));
}

TEST(AesEncryptTest, RejectsInvalidCiphertexts) {
  std::string plaintext;
  AesEncrypt& encryptor = AesEncrypt::GetInstance();

  EXPECT_FALSE(encryptor.Decrypt("", plaintext));
  EXPECT_FALSE(encryptor.Decrypt("Y29va3JwYw==", plaintext));
  EXPECT_FALSE(encryptor.Decrypt(std::string(48, '!'), plaintext));
}

TEST(AesEncryptTest, RoundTripsTextPayloads) {
  const std::vector<std::string> payloads = {
      "a",
      "ab",
      "abc",
      "CookRPC encryption unit test",
      u8"支持 UTF-8 文本",
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

TEST(AesEncryptTest, RoundTripsBinaryPayload) {
  std::string payload;
  payload.reserve(512);
  // 连续写入两轮全部字节值，覆盖空字节、最高位字节及密钥循环使用。
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

TEST(AesEncryptTest, ProducesIndependentCiphertexts) {
  const std::string payload = "the same plaintext";
  std::string first_ciphertext;
  std::string second_ciphertext;
  AesEncrypt& encryptor = AesEncrypt::GetInstance();

  ASSERT_TRUE(encryptor.Encrypt(payload, first_ciphertext));
  ASSERT_TRUE(encryptor.Encrypt(payload, second_ciphertext));

  // 每次加密都应使用独立会话密钥，避免相同明文产生可预测密文。
  EXPECT_NE(first_ciphertext, second_ciphertext);

  std::string first_plaintext;
  std::string second_plaintext;
  ASSERT_TRUE(encryptor.Decrypt(first_ciphertext, first_plaintext));
  ASSERT_TRUE(encryptor.Decrypt(second_ciphertext, second_plaintext));
  EXPECT_EQ(first_plaintext, payload);
  EXPECT_EQ(second_plaintext, payload);
}

TEST(AesEncryptTest, ProducesBase64Ciphertext) {
  const std::string base64_alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
  std::string ciphertext;

  ASSERT_TRUE(AesEncrypt::GetInstance().Encrypt("CookRPC", ciphertext));
  EXPECT_EQ(ciphertext.size() % 4, 0U);
  EXPECT_TRUE(std::all_of(ciphertext.begin(), ciphertext.end(),
                          [&base64_alphabet](char character) {
                            return base64_alphabet.find(character) !=
                                   std::string::npos;
                          }));
}

}  // namespace
}  // namespace cookrpc
