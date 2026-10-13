// 仅替换发现 API；压测客户端及 TCP 编解码使用生产实现。
#include <zookeeper/zookeeper.h>

#include <cstdlib>
#include <cstring>

extern "C" {
zhandle_t* __wrap_zookeeper_init(const char*, watcher_fn watcher, int,
                                 const clientid_t*, void* context, int) {
  auto* handle = reinterpret_cast<zhandle_t*>(new int(1));
  watcher(handle, ZOO_SESSION_EVENT, ZOO_CONNECTED_STATE, nullptr, context);
  return handle;
}
int __wrap_zookeeper_close(zhandle_t* handle) {
  delete reinterpret_cast<int*>(handle);
  return ZOK;
}
int __wrap_zoo_get_children(zhandle_t*, const char*, int,
                            String_vector* children) {
  children->count = 1;
  children->data = new char*[1];
  children->data[0] = new char[5];
  std::memcpy(children->data[0], "node", 5);
  return ZOK;
}
void __wrap_deallocate_String_vector(String_vector* children) {
  for (int i = 0; i < children->count; ++i) delete[] children->data[i];
  delete[] children->data;
  children->count = 0;
  children->data = nullptr;
}
int __wrap_zoo_get(zhandle_t*, const char*, int, char* buffer, int* length,
                   Stat*) {
  const char* address = std::getenv("RPC_STRESS_TEST_BACKEND");
  if (!address || std::strlen(address) > static_cast<size_t>(*length))
    return ZMARSHALLINGERROR;
  *length = static_cast<int>(std::strlen(address));
  std::memcpy(buffer, address, *length);
  return ZOK;
}
}
