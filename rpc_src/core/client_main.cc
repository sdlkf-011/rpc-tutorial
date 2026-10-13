#include <iostream>
#include <nlohmann/json.hpp>
#include <string>

#include "log_manager.h"
#include "rpc_client.h"

namespace {

constexpr char kDefaultConfigPath[] = "config/rpc_client.json";
constexpr char kDefaultMessage[] = "Hello, cookrpc";

}  // namespace

int main(int argc, char* argv[]) {
  // 初始化日志模块。
  // 客户端和公共组件共用同一个日志实例。
  if (!Logger::GetInstance().Init("logs")) {
    std::cerr << "Failed to initialize logger" << std::endl;
    return 1;
  }

  // 命令行允许覆盖配置文件和 Echo 消息，便于本地联调。
  const std::string config_path = argc > 1 ? argv[1] : kDefaultConfigPath;
  const std::string message = argc > 2 ? argv[2] : kDefaultMessage;

  cookrpc::RpcClient client(config_path);
  if (!client.Connect()) {
    CLIENT_FATAL("Failed to connect to an RPC server");
    return 1;
  }

  // 使用 JSON 序列化完成一次同步 Echo 调用。
  const nlohmann::json request = {{"message", message}};
  nlohmann::json response;
  if (!client.Call("RpcService", "Echo", request, cookrpc::SerializeType::JSON,
                   response)) {
    CLIENT_FATAL("Echo RPC call failed");
    return 1;
  }

  std::cout << response.dump(2) << std::endl;
  return 0;
}
