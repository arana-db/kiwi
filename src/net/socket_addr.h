/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <cstring>
#include <string>

namespace net {

struct SocketAddr {
  SocketAddr() { Clear(); }

  SocketAddr(const SocketAddr &other) { memcpy(&addr_, &other.addr_, sizeof(addr_)); }

  SocketAddr &operator=(const SocketAddr &other) {
    if (this != &other) {
      memcpy(&addr_, &other.addr_, sizeof(addr_));
    }
    return *this;
  }

  explicit SocketAddr(const sockaddr_in &addr) { Init(addr); }

  explicit SocketAddr(const sockaddr_in6 &addr) { Init(addr); }

  SocketAddr(const std::string &ip, uint16_t hostport) { Init(ip, hostport); }

  void Init(const sockaddr_in &addr) { memcpy(&addr_, &addr, sizeof(addr)); }

  void Init(const sockaddr_in6 &addr) { memcpy(&addr_, &addr, sizeof(addr)); }

  void Init(const std::string &ip, uint16_t hostPort) {
    if (::inet_pton(AF_INET, ip.c_str(), &addr_.addr4_.sin_addr) == 1) {
      addr_.addr4_.sin_family = AF_INET;
      addr_.addr4_.sin_port = htons(hostPort);
      return;
    }
    if (::inet_pton(AF_INET6, ip.c_str(), &addr_.addr6_.sin6_addr) == 1) {
      addr_.addr6_.sin6_family = AF_INET6;
      addr_.addr6_.sin6_port = htons(hostPort);
      return;
    }
  }

  const sockaddr *Get() const {
    if (IsIPV4()) {
      return reinterpret_cast<const sockaddr *>(&addr_.addr4_);
    }
    return reinterpret_cast<const sockaddr *>(&addr_.addr6_);
  }

  socklen_t Len() const {
    if (IsIPV4()) {
      return sizeof(addr_.addr4_);
    }
    return sizeof(addr_.addr6_);
  }

  std::string GetIP() const {
    if (IsIPV4()) {
      char ipv4_buf[INET_ADDRSTRLEN] = {0};
      if (::inet_ntop(AF_INET, &addr_.addr4_.sin_addr, ipv4_buf, sizeof(ipv4_buf))) {
        return ipv4_buf;
      }
    }
    char ipv6_buf[INET6_ADDRSTRLEN] = {0};
    if (::inet_ntop(AF_INET6, &addr_.addr6_.sin6_addr, ipv6_buf, sizeof(ipv6_buf))) {
      return ipv6_buf;
    }
    return "";
  }

  std::string GetIP(char *buf, socklen_t size) const {
    if (IsIPV4()) {
      ::inet_ntop(AF_INET, &addr_.addr4_.sin_addr, buf, size);
      return buf;
    }
    ::inet_ntop(AF_INET6, &addr_.addr6_.sin6_addr, buf, size);
    return buf;
  }

  uint16_t GetPort() const {
    if (IsIPV4()) {
      return ntohs(addr_.addr4_.sin_port);
    }
    return ntohs(addr_.addr6_.sin6_port);
  }

  bool IsValid() const { return IsIPV4() || IsIPV6(); }

  bool IsIPV6() const { return addr_.addr6_.sin6_family == AF_INET6; }

  bool IsIPV4() const { return addr_.addr4_.sin_family == AF_INET; }

  void Clear() { memset(&addr_, 0, sizeof(addr_)); }

  friend bool operator==(const SocketAddr &a, const SocketAddr &b) {
    if (a.IsIPV4() != b.IsIPV4()) {
      return false;
    }

    if (a.IsIPV4()) {
      return a.addr_.addr4_.sin_addr.s_addr == b.addr_.addr4_.sin_addr.s_addr &&
             a.addr_.addr4_.sin_port == b.addr_.addr4_.sin_port;
    }

    return memcmp(&a.addr_.addr6_.sin6_addr, &b.addr_.addr6_.sin6_addr, sizeof(in6_addr)) == 0 &&
           a.addr_.addr6_.sin6_port == b.addr_.addr6_.sin6_port;
  }

  friend bool operator!=(const SocketAddr &a, const SocketAddr &b) { return !(a == b); }

  union {
    sockaddr_in addr4_;
    sockaddr_in6 addr6_;
  } addr_;
};

}  // namespace net
