#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include <nlohmann/json.hpp>

#include "message.pb.h"
#include "json_serializer.h"
#include "protobuf_serializer.h"
#include "serializer_manager.h"

namespace cookrpc {
namespace {

struct JsonPayload {
  std::uint64_t request_id = 0;
  std::string method;

  bool operator==(const JsonPayload& other) const {
    return request_id == other.request_id && method == other.method;
  }
};

void to_json(nlohmann::json& json, const JsonPayload& payload) {
  json = nlohmann::json{{"request_id", payload.request_id},
                        {"method", payload.method}};
}

void from_json(const nlohmann::json& json, JsonPayload& payload) {
  json.at("request_id").get_to(payload.request_id);
  json.at("method").get_to(payload.method);
}

TEST(JsonSerializerTest, RoundTripsCustomType) {
  const JsonPayload source{42, "GetUser"};
  std::string buffer;

  ASSERT_TRUE(JsonSerializer::Serialize(source, buffer));

  JsonPayload result;
  ASSERT_TRUE(JsonSerializer::Deserialize(buffer, result));
  EXPECT_EQ(result, source);
}

TEST(JsonSerializerTest, KeepsDestinationOnInvalidInput) {
  JsonPayload destination{7, "ExistingMethod"};

  EXPECT_FALSE(JsonSerializer::Deserialize("not-json", destination));
  EXPECT_EQ(destination, (JsonPayload{7, "ExistingMethod"}));
}

TEST(ProtobufSerializerTest, RoundTripsGeneratedMessage) {
  rpc::tutorial::RpcRequest source;
  source.set_request_id(42);
  source.set_service_name("UserService");
  source.set_method_name("GetUser");
  source.set_payload("request-body");
  std::string buffer;

  ASSERT_TRUE(ProtobufSerializer::Serialize(source, buffer));

  rpc::tutorial::RpcRequest result;
  ASSERT_TRUE(ProtobufSerializer::Deserialize(buffer, result));
  EXPECT_EQ(result.SerializeAsString(), source.SerializeAsString());
}

TEST(ProtobufSerializerTest, KeepsDestinationOnInvalidInput) {
  rpc::tutorial::RpcRequest destination;
  destination.set_request_id(7);
  destination.set_method_name("ExistingMethod");

  EXPECT_FALSE(ProtobufSerializer::Deserialize(std::string(1, '\x80'),
                                               destination));
  EXPECT_EQ(destination.request_id(), 7);
  EXPECT_EQ(destination.method_name(), "ExistingMethod");
}

TEST(SerializerManagerTest, SelectsSerializerForDataType) {
  const JsonPayload json_source{42, "GetUser"};
  std::string json_buffer;
  ASSERT_TRUE(SerializerManager::Serialize(SerializeType::JSON, json_source,
                                           json_buffer));
  JsonPayload json_result;
  EXPECT_TRUE(SerializerManager::Deserialize(SerializeType::JSON, json_buffer,
                                             json_result));
  EXPECT_EQ(json_result, json_source);

  rpc::tutorial::RpcRequest protobuf_source;
  protobuf_source.set_request_id(99);
  std::string protobuf_buffer;
  ASSERT_TRUE(SerializerManager::Serialize(SerializeType::PROTOBUF,
                                           protobuf_source, protobuf_buffer));
  rpc::tutorial::RpcRequest protobuf_result;
  EXPECT_TRUE(SerializerManager::Deserialize(SerializeType::PROTOBUF,
                                             protobuf_buffer,
                                             protobuf_result));
  EXPECT_EQ(protobuf_result.request_id(), 99);
}

TEST(SerializerManagerTest, RejectsMismatchedSerializerType) {
  const JsonPayload json_source{42, "GetUser"};
  std::string buffer = "existing";
  EXPECT_FALSE(SerializerManager::Serialize(SerializeType::PROTOBUF,
                                            json_source, buffer));
  EXPECT_TRUE(buffer.empty());

  rpc::tutorial::RpcRequest protobuf_source;
  EXPECT_FALSE(SerializerManager::Serialize(SerializeType::JSON,
                                            protobuf_source, buffer));
}

}  // namespace
}  // namespace cookrpc
