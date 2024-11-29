/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <sstream>

#include "client_socket.h"

namespace net {

bool ClientSocket::Connect() {
  fd_ = CreateTCPSocketIpv4();
  if (fd_ == -1) {
    onConnectFail_("CreateTCPSocket open socket failed");
    return false;
  }
  SetNonBlock(true);
  SetNodelay();
  SetRcvBuf();
  SetSndBuf();

  int ret;
  if (addr_.IsIpv4()) {
    const sockaddr_in& addr = addr_.GetAddrIpv4();
    ret = connect(Fd(), reinterpret_cast<const sockaddr*>(&addr), sizeof(sockaddr_in));
  } else if (addr_.IsIpv6()) {
    const sockaddr_in6& addr = addr_.GetAddrIpv6();
    ret = connect(Fd(), reinterpret_cast<const sockaddr*>(&addr), sizeof(sockaddr_in6));
  } else {
    onConnectFail_("IP address is invalid");
    return false;
  }

  if (0 != ret) {
    if (EINPROGRESS == errno) {
      return true;
    }

    std::ostringstream oss;
    oss << "IP:" << addr_.GetIP() << " port:" << addr_.GetPort() << " connect failed with error: " << strerror(errno);
    Close();
    onConnectFail_(oss.str());
    return false;
  }

  return true;
}

}  // namespace net
