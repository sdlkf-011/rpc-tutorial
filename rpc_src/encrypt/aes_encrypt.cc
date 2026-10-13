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
#include <stdexcept>
#include <string>

#include "log_manager.h"

namespace cookrpc {
namespace {

// 密文协议常量：版本号用于未来格式升级，环境变量用于注入持久化主密钥。
constexpr unsigned char kCiphertextVersion = 1;
constexpr char kMasterKeyEnvironment[] = "COOKRPC_MASTER_KEY_HEX";
const std::string kBase64Characters =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

struct CipherContextDeleter {
  // 使用 unique_ptr 自动释放 OpenSSL 上下文，避免异常或提前返回时泄漏。
  void operator()(EVP_CIPHER_CTX* context) const {
    EVP_CIPHER_CTX_free(context);
  }
};

using CipherContext = std::unique_ptr<EVP_CIPHER_CTX, CipherContextDeleter>;

int HexDigitValue(char character) {
  // 将单个十六进制字符转换为 0~15；非法字符统一返回 -1。
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

bool DecodeMasterKey(const char* encoded, std::array<unsigned char, 32>& key) {
  // 主密钥必须是 32 字节（64 个字符）的十六进制文本，逐字节解码。
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
  // 优先读取部署注入的主密钥；未配置时仅生成当前进程有效的随机密钥。
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

AesEncrypt::AesEncrypt(const std::string& key_hex) {
  // 独立密钥实例用于传输会话；构造失败时由配置加载器保留旧配置。
  if (key_hex.find('\0') != std::string::npos ||
      !DecodeMasterKey(key_hex.c_str(), master_key_))
    throw std::invalid_argument(
        "AES key must contain exactly 64 hexadecimal characters");
  key_initialized_ = true;
}

AesEncrypt::~AesEncrypt() {
  // 对密钥内存做显式清零，降低进程退出前残留敏感数据的风险。
  OPENSSL_cleanse(master_key_.data(), master_key_.size());
}

// Base64 编码用于把二进制密文转换为便于传输和存储的文本。
std::string AesEncrypt::Base64Encode(const std::string& input) const {
  // EVP_EncodeBlock 要求输入长度可转换为 int，因此先做边界检查。
  if (input.empty() || input.size() > static_cast<size_t>(INT_MAX)) {
    return {};
  }

  std::string output(4 * ((input.size() + 2) / 3), '\0');
  const int encoded_size =
      EVP_EncodeBlock(reinterpret_cast<unsigned char*>(output.data()),
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
  // Base64 输入必须是 4 的倍数，并限制填充只能出现在末尾且最多两个。
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
  const int decoded_size =
      EVP_DecodeBlock(reinterpret_cast<unsigned char*>(output.data()),
                      reinterpret_cast<const unsigned char*>(input.data()),
                      static_cast<int>(input.size()));
  if (decoded_size < 0 || static_cast<size_t>(decoded_size) < padding_size) {
    return {};
  }
  output.resize(static_cast<size_t>(decoded_size) - padding_size);
  return output;
}

bool AesEncrypt::Encrypt(const std::string& data, std::string& ciphertext,
                         const std::string& associated_data) const {
  ciphertext.clear();
  try {
    // 模块一：校验输入和运行时密钥状态。
    if (data.empty()) {
      LOG_ERROR("Input data is empty");
      return false;
    }
    if (!key_initialized_ || data.size() > static_cast<size_t>(INT_MAX) ||
        associated_data.size() > static_cast<size_t>(INT_MAX)) {
      LOG_ERROR("AES-256-GCM encryption key or input is invalid");
      return false;
    }

    std::array<unsigned char, NONCE_LENGTH> nonce{};
    std::array<unsigned char, TAG_LENGTH> tag{};
    // 模块二：为每条消息生成唯一随机 nonce；GCM 禁止同一密钥复用 nonce。
    if (RAND_bytes(nonce.data(), static_cast<int>(nonce.size())) != 1) {
      LOG_ERROR("Failed to generate an AES-GCM nonce");
      return false;
    }

    CipherContext context(EVP_CIPHER_CTX_new());
    // 模块三：初始化 AES-256-GCM，并显式设置 12 字节 nonce 长度。
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
    // 模块四：将协议版本作为 AAD 认证但不加密，防止版本字段被篡改。
    if (EVP_EncryptUpdate(context.get(), nullptr, &output_size,
                          &kCiphertextVersion, 1) != 1) {
      LOG_ERROR("Failed to authenticate the ciphertext version");
      return false;
    }
    // 调用方可附加帧头和方向等元数据，防止仅修改外层字段绕过认证。
    if (!associated_data.empty() &&
        EVP_EncryptUpdate(
            context.get(), nullptr, &output_size,
            reinterpret_cast<const unsigned char*>(associated_data.data()),
            static_cast<int>(associated_data.size())) != 1)
      return false;

    std::string encrypted(data.size(), '\0');
    // 模块五：执行实际加密；GCM 通常不产生额外填充，输出长度不超过输入。
    if (EVP_EncryptUpdate(
            context.get(), reinterpret_cast<unsigned char*>(encrypted.data()),
            &output_size, reinterpret_cast<const unsigned char*>(data.data()),
            static_cast<int>(data.size())) != 1) {
      LOG_ERROR("AES-256-GCM encryption failed");
      return false;
    }
    encrypted_size = output_size;
    if (EVP_EncryptFinal_ex(
            context.get(),
            reinterpret_cast<unsigned char*>(encrypted.data()) + encrypted_size,
            &output_size) != 1 ||
        EVP_CIPHER_CTX_ctrl(context.get(), EVP_CTRL_GCM_GET_TAG,
                            static_cast<int>(tag.size()), tag.data()) != 1) {
      LOG_ERROR("Failed to finalize AES-256-GCM encryption");
      return false;
    }
    encrypted_size += output_size;
    encrypted.resize(static_cast<size_t>(encrypted_size));

    // 模块六：组装二进制协议：[版本][随机 nonce][密文][GCM 认证标签]，
    // 再用 Base64 转成可安全传输和存储的文本。
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
                         std::string& plaintext,
                         const std::string& associated_data) const {
  plaintext.clear();
  try {
    // 模块一：Base64 解码；失败时返回空字符串，后续格式校验会拒绝该输入。
    const std::string decoded = Base64Decode(encrypted_data);

    // 模块二：验证版本，并确保 nonce、至少一个密文字节和认证标签完整存在。
    const size_t minimum_size = 1 + NONCE_LENGTH + 1 + TAG_LENGTH;
    if (!key_initialized_ || decoded.size() < minimum_size ||
        associated_data.size() > static_cast<size_t>(INT_MAX) ||
        static_cast<unsigned char>(decoded.front()) != kCiphertextVersion) {
      LOG_ERROR("Invalid AES-256-GCM ciphertext format");
      return false;
    }

    const auto* nonce =
        reinterpret_cast<const unsigned char*>(decoded.data() + 1);
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
    // 模块三：按密文中的 nonce 初始化 AES-256-GCM 解密上下文。
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
    // 模块四：以同样的 AAD 认证版本字段，确保协议元数据未被修改。
    if (EVP_DecryptUpdate(context.get(), nullptr, &output_size,
                          &kCiphertextVersion, 1) != 1) {
      LOG_ERROR("Failed to authenticate the ciphertext version");
      return false;
    }
    // 解密端必须以相同顺序提供元数据，后续标签校验才会通过。
    if (!associated_data.empty() &&
        EVP_DecryptUpdate(
            context.get(), nullptr, &output_size,
            reinterpret_cast<const unsigned char*>(associated_data.data()),
            static_cast<int>(associated_data.size())) != 1)
      return false;

    plaintext.resize(encrypted_size);
    // 模块五：先解密得到候选明文；此时尚未确认其真实性，不能对外使用。
    if (EVP_DecryptUpdate(
            context.get(), reinterpret_cast<unsigned char*>(plaintext.data()),
            &output_size, encrypted, static_cast<int>(encrypted_size)) != 1) {
      plaintext.clear();
      LOG_ERROR("AES-256-GCM decryption failed");
      return false;
    }
    plaintext_size = output_size;

    if (EVP_CIPHER_CTX_ctrl(context.get(), EVP_CTRL_GCM_SET_TAG,
                            static_cast<int>(TAG_LENGTH),
                            const_cast<unsigned char*>(tag)) != 1 ||
        EVP_DecryptFinal_ex(
            context.get(),
            reinterpret_cast<unsigned char*>(plaintext.data()) + plaintext_size,
            &output_size) != 1) {
      // GCM 标签校验失败表示密钥、nonce、密文或 AAD 被篡改。
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
