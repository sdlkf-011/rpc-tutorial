#ifndef RPC_TUTORIAL_RPC_SRC_CORE_RPC_SERVICE_H_
#define RPC_TUTORIAL_RPC_SRC_CORE_RPC_SERVICE_H_

#include <exception>
#include <string>

#include <nlohmann/json.hpp>

#include "log_manager.h"
#include "service/service.h"

namespace cookrpc {

class RpcService : public Service {
 public:
  std::string GetServiceName() const override {
    return "RpcService";
  }

  bool HandleRequest(const std::string& method_name, const std::string& args,
                     std::string& result) override {
    if (method_name == "Echo") {
      // 解析请求参数
      try {
        const nlohmann::json request = nlohmann::json::parse(args);

        // 创建JSON响应
        nlohmann::json response;
        response["echo"] =
            "hahah i am rpc server, welcome to C++ training camp, come on";
        response["received_message"] = request.value("message", "");

        // 序列化为字符串返回
        result = response.dump();
        return true;
      } catch (const std::exception& e) {
        LOG_ERROR("Failed to process Echo request: {}", e.what());

        // 返回错误的 JSON 响应
        nlohmann::json error_response;
        error_response["error"] = "Invalid request format";
        result = error_response.dump();
        return false;
      }
    }

    LOG_ERROR("Unknown method: {}", method_name);
    nlohmann::json error_response;
    error_response["error"] = "Unknown method";
    result = error_response.dump();
    return false;
  }
};
} // namespace cookrpc

#endif // RPC_TUTORIAL_RPC_SRC_CORE_RPC_SERVICE_H_
