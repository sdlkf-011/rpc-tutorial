#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "service/service_manager.h"
#include "thread_pool/thread_pool_singleton.h"

namespace cookrpc {
namespace {

enum class FailureMode {
  kNone,
  kStandardException,
  kUnknownException,
};

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

TEST(ServiceManagerTest, RejectsNullAndEmptyNamedServices) {
  ServiceManager& manager = ServiceManager::GetInstance();

  EXPECT_FALSE(manager.RegisterService(nullptr));
  EXPECT_FALSE(manager.RegisterService(std::make_shared<TestService>("")));
}

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

TEST(ServiceManagerTest, RegistersAndRetrievesService) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto service = std::make_shared<TestService>("LookupService");

  ASSERT_TRUE(manager.RegisterService(service));

  EXPECT_EQ(manager.GetService("LookupService"), service);
  EXPECT_EQ(manager.GetService(""), nullptr);
  EXPECT_EQ(manager.GetService("MissingLookupService"), nullptr);
}

TEST(ServiceManagerTest, RejectsDuplicateNameAndKeepsOriginalService) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto original = std::make_shared<TestService>("DuplicateService");
  auto duplicate = std::make_shared<TestService>("DuplicateService");

  ASSERT_TRUE(manager.RegisterService(original));

  EXPECT_FALSE(manager.RegisterService(duplicate));
  EXPECT_EQ(manager.GetService("DuplicateService"), original);
}

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

TEST(ServiceManagerTest, ReturnsHandlerFailureWithoutChangingItsOutput) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto service = std::make_shared<TestService>("FailingResultService");
  service->SetHandleResult(false);
  service->SetResponse("failure details");
  ASSERT_TRUE(manager.RegisterService(service));
  std::string result;

  EXPECT_FALSE(manager.HandleRpcRequest("FailingResultService", "Fail", "",
                                        result));
  EXPECT_EQ(result, "failure details");
}

TEST(ServiceManagerTest, ClearsResultWhenServiceDoesNotExist) {
  std::string result = "stale result";

  EXPECT_FALSE(ServiceManager::GetInstance().HandleRpcRequest(
      "MissingRequestService", "Echo", "request", result));
  EXPECT_TRUE(result.empty());
}

TEST(ServiceManagerTest, ClearsResultWhenHandlerThrows) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto standard_failure =
      std::make_shared<TestService>("HandlerStandardFailure");
  standard_failure->SetHandleFailure(FailureMode::kStandardException);
  auto unknown_failure =
      std::make_shared<TestService>("HandlerUnknownFailure");
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

TEST(ServiceManagerTest, ForwardsAsynchronousRequest) {
  ServiceManager& manager = ServiceManager::GetInstance();
  auto service = std::make_shared<TestService>("AsynchronousService");
  service->SetResponse("async response");
  ASSERT_TRUE(manager.RegisterService(service));

  // 单线程足以验证异步分发，并避免测试按机器核心数创建大量工作线程。
  ASSERT_TRUE(meeting_ctrl::ThreadPoolSingleton::Init(1));
  std::string result;
  std::future<bool> handled = manager.HandleRpcRequestAsync(
      "AsynchronousService", "AsyncEcho", "async request", result);

  ASSERT_TRUE(handled.valid());
  EXPECT_TRUE(handled.get());
  EXPECT_EQ(service->LastMethodName(), "AsyncEcho");
  EXPECT_EQ(service->LastArgs(), "async request");
  EXPECT_EQ(result, "async response");

  // future 完成后再销毁线程池，确保引用传入的 result 已不再被后台访问。
  meeting_ctrl::ThreadPoolSingleton::Destroy();
}

}  // namespace
}  // namespace cookrpc
