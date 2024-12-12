/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "stream_socket.h"
#include "log.h"

namespace net {

int StreamSocket::OnReadable(const std::shared_ptr<Connection> &conn, std::string *readBuff) { return Read(readBuff); }

// return bytes that have not yet been sent
int StreamSocket::OnWritable() {
  if (sendData_.empty()) {
    if (!writeQueue_.Pop(sendData_)) {  // no data to send
      writeReady_.store(false);
      return NE_OK;
    }
  }
  size_t ret = ::write(Fd(), sendData_.c_str() + sendPos_, sendData_.size() - sendPos_);
  if (ret == -1) {
    if (EAGAIN == errno || EWOULDBLOCK == errno) {  // the socket buffer is full, waiting for the next write event
      WARN("StreamSocket fd: {} write buffer full", Fd());
      return NE_WAIT;
    }
    ERROR("StreamSocket fd: {} write error: {}", Fd(), errno);
    return NE_ERROR;
  }
  sendPos_ += ret;
  if (sendPos_ == sendData_.size()) {  // the current data has been sent
    sendPos_ = 0;
    sendData_.clear();
    // determine if there is still data in the queue
    if (writeQueue_.Empty()) {
      writeReady_.store(false);
      return NE_OK;
    }
  }
  return NE_WAIT;  // there is still data in the queue, waiting for the next write event
}

bool StreamSocket::SendPacket(std::string &&msg) {
  bool sendOver;
  do {
    sendOver = writeQueue_.Push(msg);
  } while (!sendOver);
  return !writeReady_.exchange(true);
}

// Read data from the socket
int StreamSocket::Read(std::string *readBuff) {
  char readBuffer[readBuffSize_];
  while (true) {
    int ret = static_cast<int>(::read(Fd(), readBuffer, readBuffSize_));
    if (ret == -1) {
      if (EAGAIN == errno || EWOULDBLOCK == errno || ECONNRESET == errno) {
        return NE_OK;
      } else {
        ERROR("StreamSocket fd: {} read error: {}", Fd(), errno);
        return NE_ERROR;
      }
    } else if (ret == 0) {
      return NE_CLOSE;
    }

    if (ret > 0) {
      readBuff->append(readBuffer, ret);
    }
    if (!NoBlock()) {
      break;
    }
  }

  return NE_OK;
}

}  // namespace net
