#include "connection_manager.h"

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace cookrpc {

ConnectionManager::~ConnectionManager() { CloseAll(); }

void ConnectionManager::AddConnection(std::shared_ptr<Connection> conn) {
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
  std::lock_guard<std::mutex> lock(mutex_);
  const auto iterator = connections_.find(fd);
  return iterator == connections_.end() ? nullptr : iterator->second;
}

size_t ConnectionManager::GetConnectionCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return connections_.size();
}

void ConnectionManager::CloseAll() {
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
