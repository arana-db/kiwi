/*
 * Copyright (c) 2024-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */


#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include "pcache.h"
// #include "include/pika_define.h"
#include "thread.h"
#include "db.h"
// #include "storage/storage.h"

namespace pikiwidb{

class PCacheLoadThread : public Thread {
 public:
  PCacheLoadThread(int zset_cache_start_direction, int zset_cache_field_num_per_key);
  ~PCacheLoadThread() override;

  uint64_t AsyncLoadKeysNum(void) { return async_load_keys_num_; }
  uint32_t WaittingLoadKeysNum(void) { return waitting_load_keys_num_; }
  void Push(const char key_type, std::string& key, const std::shared_ptr<DB>& db);

 private:
  bool LoadKV(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadHash(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadList(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadSet(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadZset(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadKey(const char key_type, std::string& key, const std::shared_ptr<DB>& db);
  virtual void* ThreadMain() override;

 private:
  std::atomic_bool should_exit_;
  std::deque<std::tuple<const char, std::string, const std::shared_ptr<pikiwidb::DB>>> loadkeys_queue_;

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
  
};
}  // namespace cache
