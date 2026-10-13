#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "service_manager.h"
#include "thread_pool_singleton.h"

namespace cookrpc {
namespace {

// 服务故障模式模块：用于模拟正常返回、标准异常和未知异常。
enum class FailureMode {
  kNone,
  kStandardException,
  kUnknownException,
};

// 测试服务模块：记录请求参数，并可控地返回结果或触发异常。
class TestService final : public Service {
 public:
  explicit TestService(std::string service_name)
      : service_name_(std::move(service_name)) {}

  std::string GetServiceName() const override {
    ThrowIfRequested(name_failure_);
    return service_name_;
  }

  bool HandleRequest(const std::string& method_name, const std::string& args,
                     std::string& result) override {
    last_method_name_ = method_name;
    last_args_ = args;
    ThrowIfRequested(handle_failure_);
    result = response_;
    return handle_result_;
  }

  void SetNameFailure(FailureMode failure) { name_failure_ = failure; }
  void SetHandleFailure(FailureMode failure) { handle_failure_ = failure; }
  void SetResponse(std::string response) { response_ = std::move(response); }
  void SetHandleResult(bool result) { handle_result_ = result; }

  const std::string& LastMethodName() const { return last_method_name_; }
  const std::string& LastArgs() const { return last_args_; }

 private:
  static void ThrowIfRequested(FailureMode failure) {
    if (failure == FailureMode::kStandardException) {
      throw std::runtime_error("test service failure");
    }
    if (failure == FailureMode::kUnknownException) {
      throw 42;
    }
  }

  std::string service_name_;
  std::string response_;
  std::string last_method_name_;
  std::string last_args_;
  FailureMode name_failure_ = FailureMode::kNone;
  FailureMode handle_failure_ = FailureMode::kNone;
  bool handle_result_ = true;
};

// 服务管理器测试模块：覆盖注册、查找、同步/异步转发和异常清理。
// 验证服务管理器拒绝空指针和空名称服务。
TEST(ServiceManagerTest, RejectsNullAndEmptyNamedServices) {
  ServiceManager& manager = ServiceManager::GetInstance();

  EXPECT_FALSE(manager.RegisterService(nullptr));
  EXPECT_FALSE(manager.RegisterService(std::make_shared<TestService>("")));
}

// 验证读取服务名称抛出任意异常时注册失败且不写入管理器。
TEST(ServiceManagerTest, RejectsServicesWhenReadingTheirNameThrows) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto standard_failure =
      std::make_shared<TestService>("ServiceNameStandardFailure");
  standard_failure->SetNameFailure(FailureMode::kStandardException);
  auto unknown_failure =
      std::make_shared<TestService>("ServiceNameUnknownFailure");
  unknown_failure->SetNameFailure(FailureMode::kUnknownException);

  EXPECT_FALSE(manager.RegisterService(standard_failure));
  EXPECT_FALSE(manager.RegisterService(unknown_failure));
  EXPECT_EQ(manager.GetService("ServiceNameStandardFailure"), nullptr);
  EXPECT_EQ(manager.GetService("ServiceNameUnknownFailure"), nullptr);
}

// 验证合法服务可注册，并能按名称查找或正确返回不存在结果。
TEST(ServiceManagerTest, RegistersAndRetrievesService) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto service = std::make_shared<TestService>("LookupService");

  ASSERT_TRUE(manager.RegisterService(service));

  EXPECT_EQ(manager.GetService("LookupService"), service);
  EXPECT_EQ(manager.GetService(""), nullptr);
  EXPECT_EQ(manager.GetService("MissingLookupService"), nullptr);
}

// 验证重复名称注册失败，同时保留最初注册的服务对象。
TEST(ServiceManagerTest, RejectsDuplicateNameAndKeepsOriginalService) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto original = std::make_shared<TestService>("DuplicateService");
  auto duplicate = std::make_shared<TestService>("DuplicateService");

  ASSERT_TRUE(manager.RegisterService(original));

  EXPECT_FALSE(manager.RegisterService(duplicate));
  EXPECT_EQ(manager.GetService("DuplicateService"), original);
}

// 验证同步请求转发方法名、参数和服务返回结果均保持一致。
TEST(ServiceManagerTest, ForwardsSynchronousRequestAndResult) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto service = std::make_shared<TestService>("SynchronousService");
  service->SetResponse("response payload");
  ASSERT_TRUE(manager.RegisterService(service));
  std::string result;

  EXPECT_TRUE(manager.HandleRpcRequest("SynchronousService", "Echo",
                                       "request payload", result));
  EXPECT_EQ(service->LastMethodName(), "Echo");
  EXPECT_EQ(service->LastArgs(), "request payload");
  EXPECT_EQ(result, "response payload");
}

// 验证处理器返回失败时管理器传递失败状态及其输出内容。
TEST(ServiceManagerTest, ReturnsHandlerFailureWithoutChangingItsOutput) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto service = std::make_shared<TestService>("FailingResultService");
  service->SetHandleResult(false);
  service->SetResponse("failure details");
  ASSERT_TRUE(manager.RegisterService(service));
  std::string result;

  EXPECT_FALSE(
      manager.HandleRpcRequest("FailingResultService", "Fail", "", result));
  EXPECT_EQ(result, "failure details");
}

// 验证目标服务不存在时请求失败并清空过期结果。
TEST(ServiceManagerTest, ClearsResultWhenServiceDoesNotExist) {
  std::string result = "stale result";

  EXPECT_FALSE(ServiceManager::GetInstance().HandleRpcRequest(
      "MissingRequestService", "Echo", "request", result));
  EXPECT_TRUE(result.empty());
}

// 验证处理器抛出标准或未知异常时请求失败并清空结果。
TEST(ServiceManagerTest, ClearsResultWhenHandlerThrows) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto standard_failure =
      std::make_shared<TestService>("HandlerStandardFailure");
  standard_failure->SetHandleFailure(FailureMode::kStandardException);
  auto unknown_failure = std::make_shared<TestService>("HandlerUnknownFailure");
  unknown_failure->SetHandleFailure(FailureMode::kUnknownException);
  ASSERT_TRUE(manager.RegisterService(standard_failure));
  ASSERT_TRUE(manager.RegisterService(unknown_failure));
  std::string standard_result = "stale standard result";
  std::string unknown_result = "stale unknown result";

  EXPECT_FALSE(manager.HandleRpcRequest("HandlerStandardFailure", "Run", "",
                                        standard_result));
  EXPECT_FALSE(manager.HandleRpcRequest("HandlerUnknownFailure", "Run", "",
                                        unknown_result));
  EXPECT_TRUE(standard_result.empty());
  EXPECT_TRUE(unknown_result.empty());
}

// 验证异步请求正确提交到线程池，并最终返回服务处理结果。
TEST(ServiceManagerTest, ForwardsAsynchronousRequest) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto service = std::make_shared<TestService>("AsynchronousService");
  service->SetResponse("async response");
  ASSERT_TRUE(manager.RegisterService(service));

  // 单线程足以验证异步分发，并避免测试按机器核心数创建大量工作线程。
  ASSERT_TRUE(meeting_ctrl::ThreadPoolSingleton::Init(1));
  std::string result;
  auto handled = manager.HandleRpcRequestAsync("AsynchronousService",
                                               "AsyncEcho", "async request");

  ASSERT_TRUE(handled.valid());
  // 返回结果自身持有字符串，无需保证外部输出引用在任务期间存活。
  const auto completed = handled.get();
  EXPECT_TRUE(completed.success);
  result = completed.data;
  EXPECT_EQ(service->LastMethodName(), "AsyncEcho");
  EXPECT_EQ(service->LastArgs(), "async request");
  EXPECT_EQ(result, "async response");

  // 结果由 future 持有；完成后销毁线程池。
  meeting_ctrl::ThreadPoolSingleton::Destroy();
}

}  // namespace
}  // namespace cookrpc
