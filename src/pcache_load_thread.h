#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "client.h"
#include "pcache.h"
// #include "thread.h"

namespace kiwi {

class PCacheLoadThread {
 public:
  PCacheLoadThread(int zset_cache_start_direction, int zset_cache_field_num_per_key);
  ~PCacheLoadThread();

  uint64_t AsyncLoadKeysNum(void) { return async_load_keys_num_; }
  uint32_t WaittingLoadKeysNum(void) { return waitting_load_keys_num_; }
  void Push(const char key_type, std::string& key, PClient* client);

 private:
  bool LoadKV(std::string& key, PClient* client);
  bool LoadHash(std::string& key, PClient* client);
  bool LoadList(std::string& key, PClient* client);
  bool LoadSet(std::string& key, PClient* client);
  bool LoadZset(std::string& key, PClient* client);
  bool LoadKey(const char key_type, std::string& key, PClient* client);
  void ThreadMain();

 private:
  std::atomic_bool should_exit_;
  std::deque<std::tuple<const char, std::string, PClient*>> loadkeys_queue_;

  pstd::CondVar loadkeys_cond_;
  pstd::Mutex loadkeys_mutex_;

  std::unordered_map<std::string, std::string> loadkeys_map_;
  pstd::Mutex loadkeys_map_mutex_;
  std::atomic_uint64_t async_load_keys_num_;
  std::atomic_uint32_t waitting_load_keys_num_;
  std::shared_ptr<PCache> cache_;
  // currently only take effects to zset
  int zset_cache_start_direction_;
  int zset_cache_field_num_per_key_;

  std::thread thread_;
};
}  // namespace kiwi
