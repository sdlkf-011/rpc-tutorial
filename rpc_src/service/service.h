#ifndef RPC_TUTORIAL_RPC_SRC_SERVICE_SERVICE_H_
#define RPC_TUTORIAL_RPC_SRC_SERVICE_SERVICE_H_

#include <string>

namespace cookrpc {

// RPC 服务抽象接口
class Service {
 public:
  virtual ~Service() = default;

  // 获取服务名称
  virtual std::string GetServiceName() const = 0;

  // 处理请求
  virtual bool HandleRequest(const std::string& method_name,
                             const std::string& args, std::string& result) = 0;
};
} // namespace cookrpc

#endif // RPC_TUTORIAL_RPC_SRC_SERVICE_SERVICE_H_
