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

  SocketAddr(const SocketAddr &other) { memcpy(&addr_, &other.addr_, sizeof addr_); }

  SocketAddr &operator=(const SocketAddr &other) {
    if (this != &other) {
      memcpy(&addr_, &other.addr_, sizeof addr_);
    }
    return *this;
  }

  explicit SocketAddr(const sockaddr_in &addr) { Init(addr); }

  explicit SocketAddr(const sockaddr_in6 &addr) { Init(addr); }

  SocketAddr(uint32_t netip, uint16_t netport) { Init(netip, netport); }

  SocketAddr(const std::string &ip, uint16_t hostport) { Init(ip, hostport); }

  void Init(const sockaddr_in &addr) {
    Clear();
    memcpy(&addr_, &addr, sizeof(addr));
  }

  void Init(const sockaddr_in6 &addr) {
    Clear();
    memcpy(&addr_, &addr, sizeof(addr));
  }

  void Init(uint32_t netIp, uint16_t netPort) {
    addr_.addr4_.sin_family = AF_INET;
    addr_.addr4_.sin_addr.s_addr = netIp;
    addr_.addr4_.sin_port = netPort;
  }

  void Init(const std::string &ip, uint16_t hostPort) {
    if (::inet_pton(AF_INET6, ip.c_str(), &addr_.addr6_.sin6_addr) == 1) {
      addr_.addr6_.sin6_family = AF_INET6;
      addr_.addr6_.sin6_port = htons(hostPort);
    } else if (::inet_pton(AF_INET, ip.c_str(), &addr_.addr4_.sin_addr) == 1) {
      addr_.addr4_.sin_family = AF_INET;
      addr_.addr4_.sin_port = htons(hostPort);
    } else {
      Clear();
    }
  }

  /*const sockaddr_in &GetAddr() const { return addr4_; }*/

  const sockaddr_in &GetAddrIpv4() const { return addr_.addr4_; }

  const sockaddr_in6 &GetAddrIpv6() const { return addr_.addr6_; }

  std::string GetIP() const {
    char buf[INET6_ADDRSTRLEN] = {0};
    if (addr_.addr4_.sin_family == AF_INET) {
      return ::inet_ntoa(addr_.addr4_.sin_addr);
    } else if (addr_.addr6_.sin6_family == AF_INET6) {
      if (::inet_ntop(AF_INET6, &addr_.addr6_.sin6_addr, buf, sizeof(buf))) {
        return std::string(buf);
      }
    }
    return "";
  }

  /*std::string GetIP(char *buf, socklen_t size) const {*/
  /*  return ::inet_ntop(AF_INET, reinterpret_cast<const char *>(&addr4_.sin_addr), buf, size);*/
  /*}*/

  std::string GetIP(char *buf, socklen_t size) const {
    if (addr_.addr4_.sin_family == AF_INET) {
      // 处理 IPv4 地址
      if (::inet_ntop(AF_INET, &addr_.addr4_.sin_addr, buf, size)) {
        return std::string(buf);
      }
    } else if (addr_.addr6_.sin6_family == AF_INET6) {
      // 处理 IPv6 地址
      if (::inet_ntop(AF_INET6, &addr_.addr6_.sin6_addr, buf, size)) {
        return std::string(buf);
      }
    }
    return "";  // 返回空字符串表示地址无效
  }

  uint16_t GetPort() const {
    if (addr_.addr4_.sin_family == AF_INET) {
      return ntohs(addr_.addr4_.sin_port);
    } else if (addr_.addr6_.sin6_family == AF_INET6) {
      return ntohs(addr_.addr6_.sin6_port);
    }
    return 0;
  }

  bool IsValid() const { return addr_.addr4_.sin_family != 0 || addr_.addr6_.sin6_family != 0; }

  bool IsIpv6() const { return addr_.addr6_.sin6_family == AF_INET6; }

  bool IsIpv4() const { return addr_.addr4_.sin_family == AF_INET; }

  void Clear() { memset(&addr_, 0, sizeof addr_); }

  friend bool operator==(const SocketAddr &a, const SocketAddr &b) {
    if (a.addr_.addr4_.sin_family == AF_INET && b.addr_.addr4_.sin_family == AF_INET) {
      return a.addr_.addr4_.sin_addr.s_addr == b.addr_.addr4_.sin_addr.s_addr &&
             a.addr_.addr4_.sin_port == b.addr_.addr4_.sin_port;
    } else if (a.addr_.addr6_.sin6_family == AF_INET6 && b.addr_.addr6_.sin6_family == AF_INET6) {
      return memcmp(&a.addr_.addr6_.sin6_addr, &b.addr_.addr6_.sin6_addr, sizeof(in6_addr)) == 0 &&
             a.addr_.addr6_.sin6_port == b.addr_.addr6_.sin6_port;
    }
    return false;
  }

  friend bool operator!=(const SocketAddr &a, const SocketAddr &b) { return !(a == b); }

  union {
    sockaddr_in addr4_;
    sockaddr_in6 addr6_;
  } addr_;
};

}  // namespace net
