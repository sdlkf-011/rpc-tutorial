#include "aes_encrypt.h"

#include <cstddef>
#include <exception>
#include <random>
#include <string>
#include <vector>

#include "log_manager.h"

namespace cookrpc {
namespace {
// Base64 字符表
const std::string BASE64_CHARS =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// 生成指定长度的随机字节序列
std::string GenerateRandomBytes(size_t length) {
  std::string result;
  result.reserve(length);

  std::random_device rd;
  std::mt19937 gen(rd()); //使用随机设备作为种子
  std::uniform_int_distribution<> dis(0, 255); // 0-255 的均匀分布

  for (size_t i = 0; i < length; ++i) {
    result.push_back(static_cast<char>(dis(gen)));
  }
  return result;
}
} // namespace

std::string AesEncrypt::Base64Encode(const std::string& input) const {
  std::string ret;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3]; // 存储 3 个原始字节
  unsigned char char_array_4[4]; // 存储 4 个编码后的 6 位值

  const unsigned char* bytes_to_encode =
    reinterpret_cast<const unsigned char*>(input.data());
  size_t in_len = input.length();

  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      // 将 3 个字节转换为 4 个 6 位值
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) +
                        ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) +
                        ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      // 将 6 位值转换为 Base64 字符
      for (i = 0; i < 4; ++i) {
        ret += BASE64_CHARS[char_array_4[i]];
      }
      i = 0;
    }
  }

  // 处理不足 3 位字节的剩余数据
  if (i) {
    // 用 '\0' 补齐到 3 字节
    for (j = i; j < 3; ++j) {
      char_array_3[j] = '\0';
    }

    // 转换为 6位值
    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) +
                      ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) +
                      ((char_array_3[2] & 0xc0) >> 6);

    // 只编码有效的部分
    for (j = 0; j < i + 1; ++j) {
      ret += BASE64_CHARS[char_array_4[j]];
    }

    // 用 '=' 填充到 4 的倍数
    while ((i++ < 3)) {
      ret += '=';
    }
  }
  return ret;
}

std::string AesEncrypt::Base64Decode(const std::string& input) const {
  std::string ret;
  // 建立 Base64 字符到索引的映射表
  std::vector<int> base64_map(256, -1);
  for (size_t i = 0; i < BASE64_CHARS.size(); ++i) {
    base64_map[BASE64_CHARS[i]] = i;
  }

  if (input.empty() || input.size() % 4 != 0) {
    return {};
  }

  std::size_t padding_size = 0;
  bool padding_started = false;
  for (char character : input) {
    if (character == '=') {
      padding_started = true;
      ++padding_size;
      continue;
    }

    const auto byte = static_cast<unsigned char>(character);
    if (padding_started || base64_map[byte] < 0) {
      return {};
    }
  }
  if (padding_size > 2) {
    return {};
  }

  size_t in_len = input.size();
  int i = 0;
  int j = 0;
  int in_ = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];

  while (in_len-- && input[in_] != '=') {
    char_array_4[i++] = input[in_];
    in_++;
    if (i == 4) {
      // 将 Base64 字符转换为 6 位值
      for (i = 0; i < 4; ++i) {
        char_array_4[i] = base64_map[char_array_4[i]];
      }

      // 将 4 个 6 位值转换为 3 个 8 位字节
      char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
      char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
      char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

      for (i = 0; i < 3; ++i) {
        ret += char_array_3[i];
      }
      i = 0;
    }
  }

  // 处理剩余的字符
  if (i) {
    for (j = i; j < 4; ++j) {
      char_array_4[j] = 0;
    }

    for (j = 0; j < 4; ++j) {
      char_array_4[j] = base64_map[char_array_4[j]];
    }

    char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
    char_array_3[1] = ((char_array_4[1] & 0xf) << 4) +
                      ((char_array_4[2] & 0x3c) >> 2);
    char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

    // 只取有效的字节
    for (j = 0; j < i - 1; ++j) {
      ret += char_array_3[j];
    }
  }
  return ret;
}

std::string AesEncrypt::ShiftEncrypt(const std::string& input, const std::string& key) const {
  std::string result;
  result.reserve(input.size());

  for (size_t i = 0; i < input.size(); ++i) {
    unsigned char c = input[i]; // 原始字符
    unsigned char k = key[i % key.size()]; // 循环使用密钥
    unsigned char shift = i % 256; // 位置相关的偏移量

    result.push_back(static_cast<char>((c + k + shift) % 256));
  }
  return result;
}

std::string AesEncrypt::ShiftDecrypt(const std::string& input, const std::string& key) const {
  std::string result;
  result.reserve(input.size());

  for (size_t i = 0; i < input.size(); ++i) {
    unsigned char c = input[i]; // 原始字符
    unsigned char k = key[i % key.size()]; // 循环使用密钥
    unsigned char shift = i % 256; // 位置相关的偏移量

    result.push_back(static_cast<char>((c - k - shift) % 256));
  }
  return result;
}

AesEncrypt::AesEncrypt() {
  master_key_ = "CookRPC_Secret_Key_2024_Production!@#$%^&*";
}

bool AesEncrypt::Encrypt(const std::string& data, std::string& ciphertext) {
  try {
    // 输入验证
    if (data.empty()) {
      LOG_ERROR("Input data is empty");
      return false;
    }

    // 生成随机会话密钥
    std::string session_key = GenerateRandomBytes(KEY_LENGTH);

    // 使用会话密钥加密实际数据
    std::string encrypted = ShiftEncrypt(data, session_key);

    // 使用主密钥加密会话密钥
    std::string encrypted_key = ShiftEncrypt(session_key, master_key_);

    // 组合加密后的密钥和数据
    std::string combined = encrypted_key + encrypted;

    // Base64 编码最终结果
    ciphertext = Base64Encode(combined);

    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Encrypion failed: {}", e.what());
    return false;
  }
}

bool AesEncrypt::Decrypt(const std::string& encrypted_data, std::string& plaintext) {
  try {
    // Base64 解码
    std::string decoded = Base64Decode(encrypted_data);

    // 验证数据长度
    if (decoded.size() <= KEY_LENGTH) {
      LOG_ERROR("Decoded data too short: {} bytes, need > {} bytes",
                decoded.size(), KEY_LENGTH);
      return false;
    }

    // 分离加密的密钥和数据
    std::string encrypted_key = decoded.substr(0, KEY_LENGTH); // 前 32 字节
    std::string encrypted_data_part = decoded.substr(KEY_LENGTH);

    // 解密会话密钥
    std::string session_key = ShiftDecrypt(encrypted_key, master_key_);

    // 解密实际数据
    plaintext = ShiftDecrypt(encrypted_data_part, session_key);

    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Decryption failed: {}", e.what());
    return false;
  }
}
} // namespace cookrpc
