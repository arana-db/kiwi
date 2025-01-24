/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <fcntl.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include "base_socket.h"
#include "log.h"

namespace net {

int BaseSocket::CreateTCPSocket(const SocketAddr &addr) {
  if (addr.IsIPV6()) {
    return ::socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
  }

  return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
}

int BaseSocket::CreateUDPSocket() { return ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP); }

void BaseSocket::Close() {
  auto fd = Fd();
  if (fd_.compare_exchange_strong(fd, 0)) {
    ::shutdown(fd, SHUT_RDWR);
    ::close(fd);
  }
}

void BaseSocket::OnCreate() {
#ifndef HAVE_ACCEPT4
  SetNonBlock(true);
#endif
  SetNodelay();
  SetTcpKeepAlive();
  SetSndBuf();
  SetRcvBuf();
}

void BaseSocket::SetNonBlock(bool noBlock) {
  int flag = ::fcntl(Fd(), F_GETFL, 0);
  if (-1 == flag) {
    return;
  }
  if (noBlock) {
    flag |= O_NONBLOCK;
  } else {
    flag &= ~O_NONBLOCK;
  }
  if (::fcntl(Fd(), F_SETFL, flag) != -1) {
    noBlock_ = noBlock;
  } else {
    ERROR("SetNonBlock fd:{}, flag:{} error:{}", Fd(), flag, errno);
  }
}

void BaseSocket::SetNodelay() {
  int nodelay = 1;
  if (::setsockopt(Fd(), IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char *>(&nodelay), sizeof(int)) == -1) {
    WARN("SetNodelay fd:{} error:{}", Fd(), errno);
  }
}

void BaseSocket::SetTcpKeepAlive() {
  if (tcp_keep_alive_ == 0) {
    return;
  }

  int enabled = 1;
  uint32_t idle = tcp_keep_alive_;
  uint32_t intvl = idle / 3;
  int cnt = 3;

  if (setsockopt(Fd(), SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled)) == -1) {
    WARN("SetTcpKeepAlive fd:{} error:{}", Fd(), errno);
    return;
  }

#ifdef TCP_KEEPIDLE
  if (setsockopt(Fd(), IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle))) {
    WARN("SetTcpKeepAlive fd:{} error:{}", Fd(), errno);
    return;
  }
#elif defined(TCP_KEEPALIVE)
  /* support MacOS */
  if (setsockopt(Fd(), IPPROTO_TCP, TCP_KEEPALIVE, &idle, sizeof(idle))) {
    WARN("SetTcpKeepAlive fd:{} error:{}", Fd(), errno);
    return;
  }
#endif

#ifdef TCP_KEEPINTVL
  if (setsockopt(Fd(), IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl))) {
    WARN("SetTcpKeepAlive fd:{} error:{}", Fd(), errno);
    return;
  }
#endif

#ifdef TCP_KEEPCNT
  if (setsockopt(Fd(), IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt))) {
    WARN("SetTcpKeepAlive fd:{} error:{}", Fd(), errno);
    return;
  }
#endif
}

void BaseSocket::SetSndBuf(socklen_t winsize) {
  if (::setsockopt(Fd(), SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char *>(&winsize), sizeof(winsize)) == -1) {
    WARN("SetSndBuf fd:{} error:{}", Fd(), errno);
  }
}

void BaseSocket::SetRcvBuf(socklen_t winsize) {
  if (::setsockopt(Fd(), SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char *>(&winsize), sizeof(winsize)) == -1) {
    WARN("SetRcvBuf fd:{} error:{}", Fd(), errno);
  }
}

void BaseSocket::SetReuseAddr() {
  int reuse = 1;
  if (::setsockopt(Fd(), SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char *>(&reuse), sizeof(reuse)) == -1) {
    WARN("SetReuseAddr fd:{} error:{}", Fd(), errno);
  }
}

bool BaseSocket::SetReusePort() {
  int reuse = 1;
  if (::setsockopt(Fd(), SOL_SOCKET, SO_REUSEPORT, reinterpret_cast<const char *>(&reuse), sizeof(reuse)) != -1) {
    return true;
  }
  WARN("SetReusePort fd:{} error:{}", Fd(), errno);
  return false;
}

bool BaseSocket::GetLocalAddr(SocketAddr &addr) {
  sockaddr_in localAddr{};
  socklen_t len = sizeof(localAddr);

  if (0 == ::getsockname(Fd(), reinterpret_cast<struct sockaddr *>(&localAddr), &len)) {
    addr.Init(localAddr);
    return true;
  }
  return false;
}

bool BaseSocket::GetPeerAddr(SocketAddr &addr) {
  sockaddr_in remoteAddr{};
  socklen_t len = sizeof(remoteAddr);
  if (0 == ::getpeername(Fd(), reinterpret_cast<struct sockaddr *>(&remoteAddr), &len)) {
    addr.Init(remoteAddr);
    return true;
  }
  return false;
}

}  // namespace net
