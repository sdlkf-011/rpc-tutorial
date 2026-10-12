#include "rpc_protocol.h"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <utility>

#include "error_code.h"
#include "log_manager.h"

namespace cookrpc {
namespace {

constexpr std::size_t kUint32Size = sizeof(std::uint32_t);
// 头部固定为 magic number、body length、sequence id 三个 uint32_t。
constexpr std::size_t kHeaderSize = RpcHeader::kHeaderSize;
// 请求包含三个字符串长度，响应包含 error code 和两个字符串长度。
constexpr std::size_t kRequestFixedBodySize = 3 * kUint32Size;
constexpr std::size_t kResponseFixedBodySize = 3 * kUint32Size;

// 协议中的整数统一使用网络字节序，避免通信双方的主机字节序影响线格式。
void AppendUint32(std::string& output, std::uint32_t value) {
  output.push_back(static_cast<char>((value >> 24U) & 0xffU));
  output.push_back(static_cast<char>((value >> 16U) & 0xffU));
  output.push_back(static_cast<char>((value >> 8U) & 0xffU));
  output.push_back(static_cast<char>(value & 0xffU));
}

bool ReadUint32(const std::string& input, std::size_t& offset,
                std::uint32_t& value) {
  // 先检查剩余字节数，防止构造不完整的数据包导致越界读取。
  if (offset > input.size() || input.size() - offset < kUint32Size) {
    return false;
  }

  const auto* bytes =
      reinterpret_cast<const unsigned char*>(input.data() + offset);
  value = (static_cast<std::uint32_t>(bytes[0]) << 24U) |
          (static_cast<std::uint32_t>(bytes[1]) << 16U) |
          (static_cast<std::uint32_t>(bytes[2]) << 8U) |
          static_cast<std::uint32_t>(bytes[3]);
  // offset 始终指向下一个待解析字段，便于调用方连续读取。
  offset += kUint32Size;
  return true;
}

// 字符串字段在线格式中表示为：4 字节长度 + 原始字节数据。
// std::string 可以保存 '\0'，因此该函数同样适用于 protobuf 等二进制载荷。
void AppendField(std::string& output, const std::string& field) {
  AppendUint32(output, static_cast<std::uint32_t>(field.size()));
  output.append(field);
}

bool ReadField(const std::string& input, std::size_t& offset,
               std::string& field) {
  std::uint32_t field_size = 0;
  // 字段声明长度不能超过当前帧的剩余长度。
  if (!ReadUint32(input, offset, field_size) || offset > input.size() ||
      static_cast<std::size_t>(field_size) > input.size() - offset) {
    return false;
  }

  field.assign(input.data() + offset, static_cast<std::size_t>(field_size));
  offset += static_cast<std::size_t>(field_size);
  return true;
}

// message_length 只有 32 位。使用减法形式检查可同时避免 size_t 加法溢出。
bool CalculateBodySize(std::size_t fixed_size, const std::string& first,
                       const std::string& second, std::size_t& body_size) {
  constexpr std::size_t kMaximumBodySize = RpcHeader::kMaxBodySize;

  body_size = fixed_size;
  for (const std::string* field : {&first, &second}) {
    if (field->size() > kMaximumBodySize - body_size) {
      return false;
    }
    body_size += field->size();
  }
  return true;
}

bool CalculateBodySize(std::size_t fixed_size, const std::string& first,
                       const std::string& second, const std::string& third,
                       std::size_t& body_size) {
  // 复用双字段版本后再累加请求特有的第三个字段。
  if (!CalculateBodySize(fixed_size, first, second, body_size)) {
    return false;
  }

  constexpr std::size_t kMaximumBodySize = RpcHeader::kMaxBodySize;
  if (third.size() > kMaximumBodySize - body_size) {
    return false;
  }
  body_size += third.size();
  return true;
}

// 写入所有消息共用的固定头部；body_size 已在调用处校验可由 uint32_t 表示。
void AppendHeader(std::string& output, std::size_t body_size,
                  std::uint32_t sequence_id) {
  AppendUint32(output, RpcHeader::MAGIC);
  AppendUint32(output, static_cast<std::uint32_t>(body_size));
  AppendUint32(output, sequence_id);
}

bool ReadHeader(const std::string& input, std::size_t& offset,
                std::uint32_t& sequence_id) {
  std::uint32_t magic_number = 0;
  std::uint32_t body_size = 0;
  if (!ReadUint32(input, offset, magic_number) ||
      !ReadUint32(input, offset, body_size) ||
      !ReadUint32(input, offset, sequence_id)) {
    return false;
  }

  // Deserialize 处理且仅处理一帧，长度必须精确匹配，尾随数据同样视为错误。
  return magic_number == RpcHeader::MAGIC &&
         body_size <= RpcHeader::kMaxBodySize &&
         static_cast<std::size_t>(body_size) == input.size() - offset;
}

}  // namespace

// ASCII "CRPC"，在抓包或十六进制转储中能够直接识别协议帧。
const std::uint32_t RpcHeader::MAGIC = 0x43525043U;

// 请求帧布局：
// [header][service length][service][method length][method][payload
// length][payload]
bool RpcRequest::Serialize(std::string& out) const {
  try {
    std::size_t body_size = 0;
    if (!CalculateBodySize(kRequestFixedBodySize, service_name_, method_name_,
                           payload_, body_size)) {
      LOG_ERROR("RPC request is too large to serialize");
      out.clear();
      return false;
    }

    // 先在临时缓冲区中构造完整帧，成功后再替换调用方的输出。
    std::string serialized;
    serialized.reserve(kHeaderSize + body_size);
    AppendHeader(serialized, body_size, sequence_id_);
    AppendField(serialized, service_name_);
    AppendField(serialized, method_name_);
    AppendField(serialized, payload_);
    out = std::move(serialized);
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("RPC request serialization error: {}", error.what());
  } catch (...) {
    LOG_ERROR("RPC request serialization failed with an unknown error");
  }

  out.clear();
  return false;
}

// 按 Serialize 的字段顺序解析请求；任何一个字段不完整都会拒绝整帧。
bool RpcRequest::Deserialize(const std::string& in) {
  try {
    std::size_t offset = 0;
    std::uint32_t sequence_id = 0;
    std::string service_name;
    std::string method_name;
    std::string payload;

    if (!ReadHeader(in, offset, sequence_id) ||
        !ReadField(in, offset, service_name) ||
        !ReadField(in, offset, method_name) ||
        !ReadField(in, offset, payload) || offset != in.size()) {
      LOG_ERROR("Invalid RPC request frame");
      return false;
    }

    // 所有字段解析完成后再提交，失败时调用方仍可继续使用原对象。
    sequence_id_ = sequence_id;
    service_name_ = std::move(service_name);
    method_name_ = std::move(method_name);
    payload_ = std::move(payload);
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("RPC request deserialization error: {}", error.what());
  } catch (...) {
    LOG_ERROR("RPC request deserialization failed with an unknown error");
  }

  return false;
}

// 响应帧布局：
// [header][error code][error length][error message][result length][result data]
bool RpcResponse::Serialize(std::string& out) const {
  try {
    std::size_t body_size = 0;
    if (!CalculateBodySize(kResponseFixedBodySize, error_message_, result_data_,
                           body_size)) {
      LOG_ERROR("RPC response is too large to serialize");
      out.clear();
      return false;
    }

    // reserve 一次性分配完整帧所需空间，后续追加字段不会反复扩容。
    std::string serialized;
    serialized.reserve(kHeaderSize + body_size);
    AppendHeader(serialized, body_size, sequence_id_);
    AppendUint32(serialized, error_code_);
    AppendField(serialized, error_message_);
    AppendField(serialized, result_data_);
    out = std::move(serialized);
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("RPC response serialization error: {}", error.what());
  } catch (...) {
    LOG_ERROR("RPC response serialization failed with an unknown error");
  }

  out.clear();
  return false;
}

bool RpcResponse::SerializeForSend(std::string& out) const {
  if (Serialize(out)) {
    return true;
  }
  RpcResponse error;
  error.SetSequenceId(sequence_id_);
  error.SetErrorCode(static_cast<uint32_t>(ErrorCode::kProtocolError));
  error.SetErrorMessage("RPC response exceeds the 1 MiB body limit");
  return error.Serialize(out);
}

// 响应解析成功后一次性提交所有字段，失败时保持原响应对象不变。
bool RpcResponse::Deserialize(const std::string& in) {
  try {
    std::size_t offset = 0;
    std::uint32_t sequence_id = 0;
    std::uint32_t error_code = 0;
    std::string error_message;
    std::string result_data;

    if (!ReadHeader(in, offset, sequence_id) ||
        !ReadUint32(in, offset, error_code) ||
        !ReadField(in, offset, error_message) ||
        !ReadField(in, offset, result_data) || offset != in.size()) {
      LOG_ERROR("Invalid RPC response frame");
      return false;
    }

    // 到达这里说明头部、字段长度以及最终游标位置均已通过校验。
    sequence_id_ = sequence_id;
    error_code_ = error_code;
    error_message_ = std::move(error_message);
    result_data_ = std::move(result_data);
    return true;
  } catch (const std::exception& error) {
    LOG_ERROR("RPC response deserialization error: {}", error.what());
  } catch (...) {
    LOG_ERROR("RPC response deserialization failed with an unknown error");
  }

  return false;
}
}  // namespace cookrpc
