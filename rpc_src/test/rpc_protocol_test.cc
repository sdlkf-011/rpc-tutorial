#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "rpc_protocol.h"

namespace cookrpc {
namespace {

std::uint32_t ReadUint32(const std::string& input, std::size_t offset) {
  const auto* bytes = reinterpret_cast<const unsigned char*>(
      input.data() + offset);
  return (static_cast<std::uint32_t>(bytes[0]) << 24U) |
         (static_cast<std::uint32_t>(bytes[1]) << 16U) |
         (static_cast<std::uint32_t>(bytes[2]) << 8U) |
         static_cast<std::uint32_t>(bytes[3]);
}

void WriteUint32(std::string& output, std::size_t offset,
                 std::uint32_t value) {
  output[offset] = static_cast<char>((value >> 24U) & 0xffU);
  output[offset + 1] = static_cast<char>((value >> 16U) & 0xffU);
  output[offset + 2] = static_cast<char>((value >> 8U) & 0xffU);
  output[offset + 3] = static_cast<char>(value & 0xffU);
}

TEST(RpcRequestTest, RoundTripsAllFieldsIncludingBinaryPayload) {
  RpcRequest source;
  source.SetSequenceId(0x10203040U);
  source.SetServiceName("UserService");
  source.SetMethodName("GetUser");
  source.SetPayload(std::string("request\0payload", 15));

  std::string frame;
  ASSERT_TRUE(source.Serialize(frame));

  RpcRequest result;
  ASSERT_TRUE(result.Deserialize(frame));
  EXPECT_EQ(result.GetSequenceId(), source.GetSequenceId());
  EXPECT_EQ(result.GetServiceName(), source.GetServiceName());
  EXPECT_EQ(result.GetMethodName(), source.GetMethodName());
  EXPECT_EQ(result.GetPayload(), source.GetPayload());
}

TEST(RpcRequestTest, UsesStableNetworkByteOrderLayout) {
  RpcRequest request;
  request.SetSequenceId(0x01020304U);
  request.SetServiceName("svc");
  request.SetMethodName("run");
  request.SetPayload("data");

  std::string frame;
  ASSERT_TRUE(request.Serialize(frame));
  ASSERT_EQ(frame.size(), 34U);

  EXPECT_EQ(ReadUint32(frame, 0), RpcHeader::MAGIC);
  EXPECT_EQ(ReadUint32(frame, 4), 22U);
  EXPECT_EQ(ReadUint32(frame, 8), 0x01020304U);
  EXPECT_EQ(ReadUint32(frame, 12), 3U);
  EXPECT_EQ(frame.substr(16, 3), "svc");
  EXPECT_EQ(ReadUint32(frame, 19), 3U);
  EXPECT_EQ(frame.substr(23, 3), "run");
  EXPECT_EQ(ReadUint32(frame, 26), 4U);
  EXPECT_EQ(frame.substr(30), "data");
}

TEST(RpcResponseTest, RoundTripsSuccessAndErrorFields) {
  RpcResponse source;
  source.SetSequenceId(99U);
  source.SetErrorCode(2003U);
  source.SetErrorMessage("protocol error");
  source.SetResultData(std::string("result\0data", 11));

  std::string frame = "old contents";
  ASSERT_TRUE(source.Serialize(frame));

  RpcResponse result;
  ASSERT_TRUE(result.Deserialize(frame));
  EXPECT_EQ(result.GetSequenceId(), 99U);
  EXPECT_EQ(result.GetErrorCode(), 2003U);
  EXPECT_EQ(result.GetErrorMessage(), "protocol error");
  EXPECT_EQ(result.GetResultData(), std::string("result\0data", 11));
}

TEST(RpcRequestTest, RejectsMalformedFramesWithoutChangingDestination) {
  RpcRequest source;
  source.SetSequenceId(42U);
  source.SetServiceName("Service");
  source.SetMethodName("Method");
  source.SetPayload("payload");
  std::string valid_frame;
  ASSERT_TRUE(source.Serialize(valid_frame));

  RpcRequest destination;
  destination.SetSequenceId(7U);
  destination.SetServiceName("ExistingService");
  destination.SetMethodName("ExistingMethod");
  destination.SetPayload("existing payload");

  std::string bad_magic = valid_frame;
  WriteUint32(bad_magic, 0, RpcHeader::MAGIC + 1U);
  EXPECT_FALSE(destination.Deserialize(bad_magic));

  std::string bad_body_size = valid_frame;
  WriteUint32(bad_body_size, 4, ReadUint32(valid_frame, 4) + 1U);
  EXPECT_FALSE(destination.Deserialize(bad_body_size));

  std::string bad_field_size = valid_frame;
  WriteUint32(bad_field_size, 12, 0xffffffffU);
  EXPECT_FALSE(destination.Deserialize(bad_field_size));

  EXPECT_FALSE(destination.Deserialize(
      valid_frame.substr(0, valid_frame.size() - 1)));
  EXPECT_FALSE(destination.Deserialize(valid_frame + "trailing data"));

  EXPECT_EQ(destination.GetSequenceId(), 7U);
  EXPECT_EQ(destination.GetServiceName(), "ExistingService");
  EXPECT_EQ(destination.GetMethodName(), "ExistingMethod");
  EXPECT_EQ(destination.GetPayload(), "existing payload");
}

TEST(RpcResponseTest, RejectsMalformedFramesWithoutChangingDestination) {
  RpcResponse source;
  source.SetSequenceId(42U);
  source.SetErrorCode(5U);
  source.SetErrorMessage("error");
  source.SetResultData("result");
  std::string frame;
  ASSERT_TRUE(source.Serialize(frame));

  RpcResponse destination;
  destination.SetSequenceId(7U);
  destination.SetErrorCode(8U);
  destination.SetErrorMessage("existing error");
  destination.SetResultData("existing result");

  // 响应体中 error code 后的第一个 uint32_t 是 error message 长度。
  WriteUint32(frame, 16, 0xffffffffU);
  EXPECT_FALSE(destination.Deserialize(frame));
  EXPECT_EQ(destination.GetSequenceId(), 7U);
  EXPECT_EQ(destination.GetErrorCode(), 8U);
  EXPECT_EQ(destination.GetErrorMessage(), "existing error");
  EXPECT_EQ(destination.GetResultData(), "existing result");
}

}  // namespace
}  // namespace cookrpc
