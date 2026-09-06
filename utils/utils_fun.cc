#include "utils_fun.h"

#include <regex>
#include <string>

// IPv4 地址校验模块：检查文本格式，并验证每个字节处于 0~255 范围。
bool IsValidIpAddress(const std::string& ip) {
  // 使用四段十进制数字和点号组成的严格模式，避免接受额外前后缀。
  const std::regex ip_pattern(R"((\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}))");
  std::smatch match;

  // regex_match 要求整个输入匹配；捕获组分别对应 IPv4 的四个八位段。
  if (std::regex_match(ip, match, ip_pattern)) {
    for (int i = 1; i <= 4; ++i) {
      // 正则只限制最多三位数字，这里进一步排除 256~999 等非法八位段。
      int octet = std::stoi(match[i].str());
      if (octet < 0 || octet > 255) {
        return false;
      }
    }
    return true;
  }
  return false;
}
