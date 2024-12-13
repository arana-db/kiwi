// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#pragma once

#include <atomic>

#include "common.h"
#include "net/net_options.h"

namespace kiwi {

class Options : public net::NetOptions {
 public:
  Options() = default;

  Options(const Options& other)
      : net::NetOptions(other),
        cfg_file_(other.cfg_file_),
        log_level_(other.log_level_),
        redis_compatible_mode(other.redis_compatible_mode.load()) {
    // NOTE: If there are member variables of pointer type, a deep copy needs to be performed here
  }

  Options& operator=(const Options& other) {
    if (this != &other) {
      cfg_file_ = other.cfg_file_;
      log_level_ = other.log_level_;
      redis_compatible_mode.store(other.redis_compatible_mode.load());
      // NOTE: If there are member variables of pointer type, a deep copy needs to be performed here
    }
    return *this;
  }

  ~Options() = default;

  void SetConfigName(const PString& cfg_file) { cfg_file_ = cfg_file; }

  const PString& GetConfigName() const { return cfg_file_; }

  void SetLogLevel(const PString& log_level) { log_level_ = log_level; }

  const PString& GetLogLevel() const { return log_level_; }

  void SetRedisCompatibleMode(bool mode) { redis_compatible_mode = mode; }

  bool GetRedisCompatibleMode() const { return redis_compatible_mode; }

 private:
  PString cfg_file_;
  PString log_level_;

  std::atomic<bool> redis_compatible_mode = false;
};

}  // namespace kiwi
