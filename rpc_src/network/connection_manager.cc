#include "connection_manager.h"

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace cookrpc {

// 连接生命周期管理模块：集中维护 fd 到 Connection 的线程安全映射。
ConnectionManager::~ConnectionManager() { CloseAll(); }

void ConnectionManager::AddConnection(std::shared_ptr<Connection> conn) {
  // 添加/替换模块：在锁内更新映射，锁外关闭被替换连接避免回调死锁。
  if (conn == nullptr || !conn->IsValid()) {
    return;
  }

  std::shared_ptr<Connection> replaced_connection;
  const int fd = conn->GetFd();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iterator = connections_.find(fd);
    if (iterator != connections_.end()) {
      replaced_connection = std::move(iterator->second);
      iterator->second = std::move(conn);
    } else {
      connections_.emplace(fd, std::move(conn));
    }
  }

  // Close 可能触发回调，因此必须在释放 manager 锁后执行。
  if (replaced_connection != nullptr) {
    replaced_connection->SetCloseCallback(nullptr);
    replaced_connection->Close();
  }
}

void ConnectionManager::RemoveConnection(int fd) {
  // 删除模块：先从映射摘除连接，再在锁外执行关闭操作。
  std::shared_ptr<Connection> connection;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iterator = connections_.find(fd);
    if (iterator == connections_.end()) {
      return;
    }
    connection = std::move(iterator->second);
    connections_.erase(iterator);
  }

  if (connection != nullptr) {
    connection->Close();
  }
}

std::shared_ptr<Connection> ConnectionManager::GetConnection(int fd) {
  // 查找模块：返回共享所有权，保证调用期间连接对象不会提前析构。
  std::lock_guard<std::mutex> lock(mutex_);
  const auto iterator = connections_.find(fd);
  return iterator == connections_.end() ? nullptr : iterator->second;
}

size_t ConnectionManager::GetConnectionCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return connections_.size();
}

void ConnectionManager::CloseAll() {
  // 批量关闭模块：交换出映射后逐个关闭，缩短全局锁持有时间。
  std::unordered_map<int, std::shared_ptr<Connection>> connections;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    connections.swap(connections_);
  }

  for (auto& entry : connections) {
    if (entry.second != nullptr) {
      entry.second->Close();
    }
  }
}
}  // namespace cookrpc
