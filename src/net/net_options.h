// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

#pragma once

#include <cstdint>

namespace net {

class NetOptions {
 public:
  NetOptions() = default;

  NetOptions(const NetOptions& other) : rwSeparation_(other.rwSeparation_), threadNum_(other.threadNum_) {
    // NOTE: If there are member variables of pointer type, a deep copy needs to be performed here
  }

  NetOptions& operator=(const NetOptions& other) {
    if (this != &other) {
      rwSeparation_ = other.rwSeparation_;
      threadNum_ = other.threadNum_;
      // NOTE: If there are member variables of pointer type, a deep copy needs to be performed here
    }
    return *this;
  }

  ~NetOptions() = default;

  void SetThreadNum(int8_t threadNum) { threadNum_ = threadNum; }

  int8_t GetThreadNum() const { return threadNum_; }

  void SetRwSeparation(bool rwSeparation = true) { rwSeparation_ = rwSeparation; }

  bool GetRwSeparation() const { return rwSeparation_; }

 private:
  bool rwSeparation_ = true;  // Whether to separate read and write

  int8_t threadNum_ = 1;  // The number of threads
};

}  // namespace net
