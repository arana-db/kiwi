/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "listen_socket.h"
#include "socket_addr.h"

namespace net {

class ListenSocketsManager {
 public:
  ListenSocketsManager(bool Ipv6 = false) : listen_(ListenSocket::CreateTCPListen()), Ipv6_(Ipv6) {
    if (Ipv6_) {
      listenIpv6_ = std::shared_ptr<ListenSocket>(ListenSocket::CreateTCPListen());
    }
  }

  ~ListenSocketsManager() = default;

  void SetListenAddr(const SocketAddr &addr) { listen_->SetListenAddr(addr); }

  void SetListenAddrIpv6(const SocketAddr &addr) {
    Ipv6_ = true;
    listenIpv6_->SetListenAddr(addr);
  }

  int Init() {
    if (auto ret = listen_->Init() != static_cast<int>(NetListen::OK)) {
      return ret;
    }

    if (Ipv6_) {
      if (auto ret = listenIpv6_->Init() != static_cast<int>(NetListen::OK)) {
        return ret;
      }
    }

    return static_cast<int>(NetListen::OK);
  }

  void Reset() {
    listen_.reset(ListenSocket::CreateTCPListen());
    listenIpv6_.reset(ListenSocket::CreateTCPListen());
  }

  std::shared_ptr<ListenSocket> GetListenSocket() { return listen_; }

  std::shared_ptr<ListenSocket> GetListenSocketIpv6() { return listenIpv6_; }

 private:
  std::shared_ptr<ListenSocket> listen_;
  std::shared_ptr<ListenSocket> listenIpv6_;
  bool Ipv6_ = false;
};

}  // namespace net
