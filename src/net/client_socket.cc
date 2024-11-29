/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <sstream>

#include <sys/socket.h>
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

  auto addr = addr_.GetAddr();
  int ret = connect(Fd(), addr, addr_.GetAddrLen());

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
