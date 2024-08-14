/*
 * Copyright (c) 2024-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "pcache_load_thread.h"
#include "pcache.h"
#include "pstd/log.h"
#include "pstd/scope_record_lock.h"
#include "store.h"

namespace pikiwidb {

PCacheLoadThread::PCacheLoadThread(int zset_cache_start_direction, int zset_cache_field_num_per_key)
    : should_exit_(false),
      loadkeys_cond_(),
      async_load_keys_num_(0),
      waitting_load_keys_num_(0),
      zset_cache_start_direction_(zset_cache_start_direction),
      zset_cache_field_num_per_key_(zset_cache_field_num_per_key) {
  set_thread_name("PCacheLoadThread");
}

PCacheLoadThread::~PCacheLoadThread() {
  {
    std::lock_guard lq(loadkeys_mutex_);
    should_exit_ = true;
    loadkeys_cond_.notify_all();
  }

  StopThread();
}

void PCacheLoadThread::Push(const char key_type, std::string& key, PClient* client) {
  std::unique_lock lq(loadkeys_mutex_);
  std::unique_lock lm(loadkeys_map_mutex_);
  if (CACHE_LOAD_QUEUE_MAX_SIZE < loadkeys_queue_.size()) {
    // 5s to print logs once
    static uint64_t last_log_time_us = 0;
    if (pstd::NowMicros() - last_log_time_us > 5000000) {
      WARN("PCacheLoadThread::Push waiting... ");
      last_log_time_us = pstd::NowMicros();
    }
    return;
  }

  if (loadkeys_map_.find(key) == loadkeys_map_.end()) {
    std::tuple<const char, std::string, PClient*> ktuple = std::make_tuple(key_type, key, client);
    loadkeys_queue_.push_back(ktuple);
    loadkeys_map_[key] = std::string("");
    loadkeys_cond_.notify_all();
  }
}

bool PCacheLoadThread::LoadKV(std::string& key, PClient* client) {
  std::string value;
  int64_t ttl = -1;
  rocksdb::Status s = PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->GetWithTTL(key, &value, &ttl);
  if (!s.ok()) {
    WARN("load kv failed, key={}", key);
    return false;
  }
  PSTORE.GetBackend(client->GetCurrentDB())->GetCache()->WriteKVToCache(key, value, ttl);
  return true;
}

// bool PCacheLoadThread::LoadHash(std::string& key, PClient* client) {
//   int32_t len = 0;
//   db->storage()->HLen(key, &len);
//   if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
//     return false;
//   }

//   std::vector<storage::FieldValue> fvs;
//   int64_t ttl = -1;
//   rocksdb::Status s = db->storage()->HGetallWithTTL(key, &fvs, &ttl);
//   if (!s.ok()) {
//     LOG(WARNING) << "load hash failed, key=" << key;
//     return false;
//   }
//   db->cache()->WriteHashToCache(key, fvs, ttl);
//   return true;
// }

bool PCacheLoadThread::LoadList(std::string& key, PClient* client) {
  uint64_t len = 0;
  PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->LLen(key, &len);
  if (len <= 0 || CACHE_VALUE_ITEM_MAX_SIZE < len) {
    WARN("can not load key, because item size: {} , beyond max item size: {} ", len, CACHE_VALUE_ITEM_MAX_SIZE);
    return false;
  }

  std::vector<std::string> values;
  int64_t ttl = -1;
  rocksdb::Status s = PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->LRangeWithTTL(key, 0, -1, &values, &ttl);
  if (!s.ok()) {
    WARN("load list failed, key= {}", key);
    return false;
  }
  PSTORE.GetBackend(client->GetCurrentDB())->GetCache()->WriteListToCache(key, values, ttl);
  return true;
}

// bool PCacheLoadThread::LoadSet(std::string& key,PClient* client) {
//   int32_t len = 0;
//   db->storage()->SCard(key, &len);
//   if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
//     LOG(WARNING) << "can not load key, because item size:" << len
//                  << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
//     return false;
//   }

//   std::vector<std::string> values;
//   int64_t ttl = -1;
//   rocksdb::Status s = db->storage()->SMembersWithTTL(key, &values, &ttl);
//   if (!s.ok()) {
//     LOG(WARNING) << "load set failed, key=" << key;
//     return false;
//   }
//   db->cache()->WriteSetToCache(key, values, ttl);
//   return true;
// }

// bool PCacheLoadThread::LoadZset(std::string& key, PClient* client) {
//   int32_t len = 0;
//   int start_index = 0;
//   int stop_index = -1;
//   db->storage()->ZCard(key, &len);
//   if (0 >= len) {
//     return false;
//   }

//   uint64_t cache_len = 0;
//   db->cache()->CacheZCard(key, &cache_len);
//   if (cache_len != 0) {
//     return true;
//   }
//   if (zset_cache_start_direction_ == cache::CACHE_START_FROM_BEGIN) {
//     if (zset_cache_field_num_per_key_ <= len) {
//       stop_index = zset_cache_field_num_per_key_ - 1;
//     }
//   } else if (zset_cache_start_direction_ == cache::CACHE_START_FROM_END) {
//     if (zset_cache_field_num_per_key_ <= len) {
//       start_index = len - zset_cache_field_num_per_key_;
//     }
//   }

//   std::vector<storage::ScoreMember> score_members;
//   int64_t ttl = -1;
//   rocksdb::Status s = db->storage()->ZRangeWithTTL(key, start_index, stop_index, &score_members, &ttl);
//   if (!s.ok()) {
//     LOG(WARNING) << "load zset failed, key=" << key;
//     return false;
//   }
//   db->cache()->WriteZSetToCache(key, score_members, ttl);
//   return true;
// }

bool PCacheLoadThread::LoadKey(const char key_type, std::string& key, PClient* client) {
  // @tobeChecked
  // 下面这行代码是pika实现中，分析pikiwidb中不再需要对DB上key锁，由Storage层来进行上锁（该两行留存，待确认无误后删除）
  // pstd::lock::ScopeRecordLock record_lock(db->LockMgr(), key);
  switch (key_type) {
    case 'k':
      return LoadKV(key, client);
    // case 'h':
    //   return LoadHash(key, client);
    case 'l':
      return LoadList(key, client);
    // case 's':
    //   return LoadSet(key, client);
    // case 'z':
    //   return LoadZset(key, client);
    default:
      WARN("PCacheLoadThread::LoadKey invalid key type : {}", key_type);
      return false;
  }
}

void* PCacheLoadThread::ThreadMain() {
  INFO("PCacheLoadThread::ThreadMain Start");

  while (!should_exit_) {
    std::deque<std::tuple<const char, std::string, PClient*>> load_keys;
    {
      std::unique_lock lq(loadkeys_mutex_);
      waitting_load_keys_num_ = loadkeys_queue_.size();
      while (!should_exit_ && loadkeys_queue_.size() <= 0) {
        loadkeys_cond_.wait(lq);
      }

      if (should_exit_) {
        return nullptr;
      }

      for (int i = 0; i < CACHE_LOAD_NUM_ONE_TIME; ++i) {
        if (!loadkeys_queue_.empty()) {
          load_keys.push_back(loadkeys_queue_.front());
          loadkeys_queue_.pop_front();
        }
      }
    }
    for (auto& load_key : load_keys) {
      if (LoadKey(std::get<0>(load_key), std::get<1>(load_key), std::get<2>(load_key))) {
        ++async_load_keys_num_;
      }

      std::unique_lock lm(loadkeys_map_mutex_);
      loadkeys_map_.erase(std::get<1>(load_key));
    }
  }

  return nullptr;
}
}  // namespace pikiwidb
