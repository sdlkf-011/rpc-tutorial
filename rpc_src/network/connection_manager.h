#ifndef RPC_TUTORIAL_RPC_SRC_NETWORK_CONNECTION_MANAGER_H_
#define RPC_TUTORIAL_RPC_SRC_NETWORK_CONNECTION_MANAGER_H_

#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "connection.h"

namespace cookrpc {

// 连接管理器：以线程安全映射统一维护所有活动网络连接。
class ConnectionManager {
 public:
  static ConnectionManager& GetInstance() {
    static ConnectionManager instance;
    return instance;
  }

  // 添加或替换指定文件描述符对应的连接。
  void AddConnection(std::shared_ptr<Connection> conn);
  // 从管理器移除并关闭指定文件描述符的连接。
  void RemoveConnection(int fd);
  void RemoveConnection(int fd, const std::shared_ptr<Connection>& expected);
  // 查找连接并返回共享所有权。
  std::shared_ptr<Connection> GetConnection(int fd);
  // 获取当前活动连接数量。
  size_t GetConnectionCount() const;
  std::vector<std::shared_ptr<Connection>> GetConnections() const;
  // 关闭并清空全部活动连接。
  void CloseAll();

  // 禁用拷贝和移动
  ~ConnectionManager();
  ConnectionManager(const ConnectionManager&) = delete;
  ConnectionManager& operator=(const ConnectionManager&) = delete;
  ConnectionManager(ConnectionManager&&) = delete;
  ConnectionManager& operator=(ConnectionManager&&) = delete;

 private:
  // 连接管理
  ConnectionManager() = default;
  std::unordered_map<int, std::shared_ptr<Connection>> connections_;
  mutable std::mutex mutex_;
};
}  // namespace cookrpc
#endif
