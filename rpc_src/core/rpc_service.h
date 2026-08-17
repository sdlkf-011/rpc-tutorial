#ifndef RPC_TUTORIAL_RPC_SRC_CORE_RPC_SERVICE_H_
#define RPC_TUTORIAL_RPC_SRC_CORE_RPC_SERVICE_H_

#include <exception>
#include <string>

#include <nlohmann/json.hpp>

#include "log_manager.h"
#include "service/service.h"

namespace cookrpc {

// 示例 RPC 服务，用于验证服务注册和方法分发链路。
class RpcService : public Service {
 public:
  std::string GetServiceName() const override { return "RpcService"; }

  bool HandleRequest(const std::string& method_name, const std::string& args,
                     std::string& result) override {
    // 当前示例仅开放 Echo，未知方法返回结构化错误信息。
    if (method_name != "Echo") {
      LOG_ERROR("Unknown RpcService method: {}", method_name);
      result = nlohmann::json{{"error", "Unknown method"}}.dump();
      return false;
    }

    // 先完成 JSON 解析，再一次性生成结果。
    // 避免返回部分响应。
    try {
      const nlohmann::json request = nlohmann::json::parse(args);
      result = nlohmann::json{
          {"echo", "Hello from cookrpc"},
          {"received_message", request.value("message", "")},
      }.dump();
      return true;
    } catch (const std::exception& error) {
      LOG_ERROR("Failed to process Echo request: {}", error.what());
      result = nlohmann::json{{"error", "Invalid request format"}}.dump();
      return false;
    }
  }
};

}  // namespace cookrpc

#endif  // RPC_TUTORIAL_RPC_SRC_CORE_RPC_SERVICE_H_
