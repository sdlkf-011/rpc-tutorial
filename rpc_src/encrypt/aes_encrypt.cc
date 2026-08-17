#include "aes_encrypt.h"

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <array>
#include <climits>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <memory>
#include <string>

#include "log_manager.h"

namespace cookrpc {
namespace {

constexpr unsigned char kCiphertextVersion = 1;
constexpr char kMasterKeyEnvironment[] = "COOKRPC_MASTER_KEY_HEX";
const std::string kBase64Characters =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

struct CipherContextDeleter {
  void operator()(EVP_CIPHER_CTX* context) const {
    EVP_CIPHER_CTX_free(context);
  }
};

using CipherContext =
    std::unique_ptr<EVP_CIPHER_CTX, CipherContextDeleter>;

int HexDigitValue(char character) {
  if (character >= '0' && character <= '9') {
    return character - '0';
  }
  if (character >= 'a' && character <= 'f') {
    return character - 'a' + 10;
  }
  if (character >= 'A' && character <= 'F') {
    return character - 'A' + 10;
  }
  return -1;
}

bool DecodeMasterKey(const char* encoded,
                     std::array<unsigned char, 32>& key) {
  if (encoded == nullptr || std::char_traits<char>::length(encoded) != 64) {
    return false;
  }

  for (size_t index = 0; index < key.size(); ++index) {
    const int high = HexDigitValue(encoded[index * 2]);
    const int low = HexDigitValue(encoded[index * 2 + 1]);
    if (high < 0 || low < 0) {
      return false;
    }
    key[index] = static_cast<unsigned char>((high << 4) | low);
  }
  return true;
}

}  // namespace

AesEncrypt::AesEncrypt() {
  const char* configured_key = std::getenv(kMasterKeyEnvironment);
  if (configured_key != nullptr) {
    key_initialized_ = DecodeMasterKey(configured_key, master_key_);
    if (!key_initialized_) {
      LOG_ERROR("{} must contain exactly 64 hexadecimal characters",
                kMasterKeyEnvironment);
    }
    return;
  }

  // 未配置持久主密钥时生成进程级随机密钥，避免源码中出现硬编码密钥。
  key_initialized_ =
      RAND_bytes(master_key_.data(), static_cast<int>(master_key_.size())) == 1;
  if (key_initialized_) {
    LOG_WARN("{} is not set; using an ephemeral process-local encryption key",
             kMasterKeyEnvironment);
  } else {
    LOG_ERROR("Failed to generate an AES-256 master key");
  }
}

AesEncrypt::~AesEncrypt() {
  OPENSSL_cleanse(master_key_.data(), master_key_.size());
}

// Base64 编码用于把二进制密文转换为便于传输和存储的文本。
std::string AesEncrypt::Base64Encode(const std::string& input) const {
  if (input.empty() || input.size() > static_cast<size_t>(INT_MAX)) {
    return {};
  }

  std::string output(4 * ((input.size() + 2) / 3), '\0');
  const int encoded_size = EVP_EncodeBlock(
      reinterpret_cast<unsigned char*>(output.data()),
      reinterpret_cast<const unsigned char*>(input.data()),
      static_cast<int>(input.size()));
  if (encoded_size <= 0) {
    return {};
  }
  output.resize(static_cast<size_t>(encoded_size));
  return output;
}

// Base64 解码先严格校验字符和尾部填充，再交给 OpenSSL 解码。
std::string AesEncrypt::Base64Decode(const std::string& input) const {
  if (input.empty() || input.size() % 4 != 0 ||
      input.size() > static_cast<size_t>(INT_MAX)) {
    return {};
  }

  size_t padding_size = 0;
  bool padding_started = false;
  for (const char character : input) {
    if (character == '=') {
      padding_started = true;
      ++padding_size;
      continue;
    }
    if (padding_started ||
        kBase64Characters.find(character) == std::string::npos) {
      return {};
    }
  }
  if (padding_size > 2) {
    return {};
  }

  std::string output(3 * (input.size() / 4), '\0');
  const int decoded_size = EVP_DecodeBlock(
      reinterpret_cast<unsigned char*>(output.data()),
      reinterpret_cast<const unsigned char*>(input.data()),
      static_cast<int>(input.size()));
  if (decoded_size < 0 ||
      static_cast<size_t>(decoded_size) < padding_size) {
    return {};
  }
  output.resize(static_cast<size_t>(decoded_size) - padding_size);
  return output;
}

bool AesEncrypt::Encrypt(const std::string& data, std::string& ciphertext) {
  ciphertext.clear();
  try {
    // 输入验证
    if (data.empty()) {
      LOG_ERROR("Input data is empty");
      return false;
    }
    if (!key_initialized_ || data.size() > static_cast<size_t>(INT_MAX)) {
      LOG_ERROR("AES-256-GCM encryption key or input is invalid");
      return false;
    }

    std::array<unsigned char, NONCE_LENGTH> nonce{};
    std::array<unsigned char, TAG_LENGTH> tag{};
    if (RAND_bytes(nonce.data(), static_cast<int>(nonce.size())) != 1) {
      LOG_ERROR("Failed to generate an AES-GCM nonce");
      return false;
    }

    CipherContext context(EVP_CIPHER_CTX_new());
    if (context == nullptr ||
        EVP_EncryptInit_ex(context.get(), EVP_aes_256_gcm(), nullptr, nullptr,
                           nullptr) != 1 ||
        EVP_CIPHER_CTX_ctrl(context.get(), EVP_CTRL_GCM_SET_IVLEN,
                            static_cast<int>(nonce.size()), nullptr) != 1 ||
        EVP_EncryptInit_ex(context.get(), nullptr, nullptr, master_key_.data(),
                           nonce.data()) != 1) {
      LOG_ERROR("Failed to initialize AES-256-GCM encryption");
      return false;
    }

    int output_size = 0;
    int encrypted_size = 0;
    if (EVP_EncryptUpdate(context.get(), nullptr, &output_size,
                          &kCiphertextVersion, 1) != 1) {
      LOG_ERROR("Failed to authenticate the ciphertext version");
      return false;
    }

    std::string encrypted(data.size(), '\0');
    if (EVP_EncryptUpdate(
            context.get(),
            reinterpret_cast<unsigned char*>(encrypted.data()), &output_size,
            reinterpret_cast<const unsigned char*>(data.data()),
            static_cast<int>(data.size())) != 1) {
      LOG_ERROR("AES-256-GCM encryption failed");
      return false;
    }
    encrypted_size = output_size;
    if (EVP_EncryptFinal_ex(
            context.get(),
            reinterpret_cast<unsigned char*>(encrypted.data()) +
                encrypted_size,
            &output_size) != 1 ||
        EVP_CIPHER_CTX_ctrl(context.get(), EVP_CTRL_GCM_GET_TAG,
                            static_cast<int>(tag.size()), tag.data()) != 1) {
      LOG_ERROR("Failed to finalize AES-256-GCM encryption");
      return false;
    }
    encrypted_size += output_size;
    encrypted.resize(static_cast<size_t>(encrypted_size));

    // 密文格式：[版本][随机 nonce][密文][GCM 认证标签]。
    std::string binary;
    binary.reserve(1 + nonce.size() + encrypted.size() + tag.size());
    binary.push_back(static_cast<char>(kCiphertextVersion));
    binary.append(reinterpret_cast<const char*>(nonce.data()), nonce.size());
    binary.append(encrypted);
    binary.append(reinterpret_cast<const char*>(tag.data()), tag.size());
    ciphertext = Base64Encode(binary);
    return !ciphertext.empty();
  } catch (const std::exception& error) {
    ciphertext.clear();
    LOG_ERROR("Encryption failed: {}", error.what());
    return false;
  }
}

bool AesEncrypt::Decrypt(const std::string& encrypted_data,
                         std::string& plaintext) {
  plaintext.clear();
  try {
    // Base64 解码
    const std::string decoded = Base64Decode(encrypted_data);

    // 验证版本以及 nonce、密文和认证标签的最小长度。
    const size_t minimum_size = 1 + NONCE_LENGTH + 1 + TAG_LENGTH;
    if (!key_initialized_ || decoded.size() < minimum_size ||
        static_cast<unsigned char>(decoded.front()) != kCiphertextVersion) {
      LOG_ERROR("Invalid AES-256-GCM ciphertext format");
      return false;
    }

    const auto* nonce = reinterpret_cast<const unsigned char*>(
        decoded.data() + 1);
    const size_t encrypted_size =
        decoded.size() - 1 - NONCE_LENGTH - TAG_LENGTH;
    if (encrypted_size > static_cast<size_t>(INT_MAX)) {
      LOG_ERROR("AES-256-GCM ciphertext is too large");
      return false;
    }
    const auto* encrypted = reinterpret_cast<const unsigned char*>(
        decoded.data() + 1 + NONCE_LENGTH);
    const auto* tag = reinterpret_cast<const unsigned char*>(
        decoded.data() + decoded.size() - TAG_LENGTH);

    CipherContext context(EVP_CIPHER_CTX_new());
    if (context == nullptr ||
        EVP_DecryptInit_ex(context.get(), EVP_aes_256_gcm(), nullptr, nullptr,
                           nullptr) != 1 ||
        EVP_CIPHER_CTX_ctrl(context.get(), EVP_CTRL_GCM_SET_IVLEN,
                            static_cast<int>(NONCE_LENGTH), nullptr) != 1 ||
        EVP_DecryptInit_ex(context.get(), nullptr, nullptr, master_key_.data(),
                           nonce) != 1) {
      LOG_ERROR("Failed to initialize AES-256-GCM decryption");
      return false;
    }

    int output_size = 0;
    int plaintext_size = 0;
    if (EVP_DecryptUpdate(context.get(), nullptr, &output_size,
                          &kCiphertextVersion, 1) != 1) {
      LOG_ERROR("Failed to authenticate the ciphertext version");
      return false;
    }

    plaintext.resize(encrypted_size);
    if (EVP_DecryptUpdate(
            context.get(),
            reinterpret_cast<unsigned char*>(plaintext.data()), &output_size,
            encrypted, static_cast<int>(encrypted_size)) != 1) {
      plaintext.clear();
      LOG_ERROR("AES-256-GCM decryption failed");
      return false;
    }
    plaintext_size = output_size;

    if (EVP_CIPHER_CTX_ctrl(
            context.get(), EVP_CTRL_GCM_SET_TAG, static_cast<int>(TAG_LENGTH),
            const_cast<unsigned char*>(tag)) != 1 ||
        EVP_DecryptFinal_ex(
            context.get(),
            reinterpret_cast<unsigned char*>(plaintext.data()) +
                plaintext_size,
            &output_size) != 1) {
      plaintext.clear();
      LOG_WARN("AES-256-GCM authentication failed");
      return false;
    }

    plaintext_size += output_size;
    plaintext.resize(static_cast<size_t>(plaintext_size));
    return true;
  } catch (const std::exception& error) {
    plaintext.clear();
    LOG_ERROR("Decryption failed: {}", error.what());
    return false;
  }
}
}  // namespace cookrpc
