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
  ~NetOptions() = default;

  void SetThreadNum(int8_t threadNum) { threadNum_ = threadNum; }

  int8_t GetThreadNum() const { return threadNum_; }

  void SetRwSeparation(bool rwSeparation = true) { rwSeparation_ = rwSeparation; }

  bool GetRwSeparation() const { return rwSeparation_; }

<<<<<<< HEAD
  void SetMaxClients(uint32_t maxClients) { maxClients_ = maxClients; }

  uint32_t GetMaxClients() const { return maxClients_; }
=======
  void SetOpTcpKeepAlive(uint32_t tcpKeepAlive) { tcpKeepAlive_ = tcpKeepAlive; }

  uint32_t GetOpTcpKeepAlive() const { return tcpKeepAlive_; }
>>>>>>> 9937125d749015d74eef320413e99668a4a4a12b

 private:
  bool rwSeparation_ = true;  // Whether to separate read and write

  int8_t threadNum_ = 1;  // The number of threads

<<<<<<< HEAD
  uint32_t maxClients_ = 1;  // The maximum number of connections(default 40000)
=======
  uint32_t tcpKeepAlive_ = 300;  // The timeout of the keepalive connection in seconds
>>>>>>> 9937125d749015d74eef320413e99668a4a4a12b
};

}  // namespace net
