#include "utils_fun.h"

#include <arpa/inet.h>

// 配置、监听和连接采用同一套 IPv4 文本语法，拒绝前导零和嵌入 NUL。
bool IsValidIpAddress(const std::string& ip) {
  if (ip.empty() || ip.find('\0') != std::string::npos) return false;
  in_addr address{};
  return inet_pton(AF_INET, ip.c_str(), &address) == 1;
}
