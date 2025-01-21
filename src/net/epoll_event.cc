/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "epoll_event.h"

#ifdef HAVE_EPOLL

#  include "callback_function.h"
#  include "log.h"

namespace net {

const int BaseEvent::EVENT_READ = EPOLLIN;
const int BaseEvent::EVENT_WRITE = EPOLLOUT;
const int BaseEvent::EVENT_ERROR = EPOLLERR;
const int BaseEvent::EVENT_HUB = EPOLLHUP;
const int BaseEvent::EVENT_NULL = 0;

bool EpollEvent::Init() {
  evFd_ = epoll_create1(0);
  if (evFd_ == -1) {  // If the epoll creation fails, return false
    ERROR("epoll_create1 error errno:{}", errno);
    return false;
  }
  if (mode_ & EVENT_MODE_READ) {  // Add the listen socket to epoll for read
    for (auto &listenSocket : listen_sockets_) {
      AddEvent(listenSocket->Fd(), listenSocket->Fd(), EVENT_READ);
    }
  }
  if (pipe(pipeFd_) == -1) {
    ERROR("pipe error errno:{}", errno);
    return false;
  }

  AddEvent(pipeFd_[0], pipeFd_[0], EVENT_READ);

  return true;
}

void EpollEvent::AddEvent(uint64_t id, int fd, int mask) {
  struct epoll_event ev {};
  ev.events = mask;
  ev.data.u64 = id;
  if (epoll_ctl(EvFd(), EPOLL_CTL_ADD, fd, &ev) == -1) {
    ERROR("AddEvent id:{},EvFd:{},fd:{}, epoll AddEvent error errno:{}", id, EvFd(), fd, errno);
  }
}

void EpollEvent::DelEvent(int fd) { epoll_ctl(EvFd(), EPOLL_CTL_DEL, fd, nullptr); }

void EpollEvent::EventPoll() {
  if (mode_ & EVENT_MODE_READ) {  // If it is a read multiplex, call EventRead
    EventRead();
  } else {  // If it is a write multiplex, call EventWrite
    EventWrite();
  }
}

void EpollEvent::AddWriteEvent(uint64_t id, int fd) {
  struct epoll_event ev {};
  ev.events = EVENT_WRITE;
  ev.data.u64 = id;
  if (mode_ & EVENT_MODE_READ) {  // If it is a read multiplex, modify the event
    ev.events |= EVENT_READ;
    if (epoll_ctl(EvFd(), EPOLL_CTL_MOD, fd, &ev) == -1) {
      ERROR("AddWriteEvent id:{},EvFd:{},fd:{}, epoll add RW error errno:{}", id, EvFd(), fd, errno);
    }
  } else {  // If it is a write multiplex, add the event
    if (epoll_ctl(EvFd(), EPOLL_CTL_MOD, fd, &ev) == -1) {
      ERROR("AddWriteEvent id:{},EvFd:{},fd:{}, epoll add W error errno:{}", id, EvFd(), fd, errno);
    }
  }
}

void EpollEvent::DelWriteEvent(uint64_t id, int fd) {
  struct epoll_event ev {};
  ev.data.u64 = id;
  if (mode_ & EVENT_MODE_READ) {  // If it is a read multiplex, modify the event to rea
    ev.events = EVENT_READ;
    if (epoll_ctl(EvFd(), EPOLL_CTL_MOD, fd, &ev) == -1) {
      ERROR("DelWriteEvent id:{},EvFd:{},fd:{}, EPOLL_CTL_MOD error errno:{}", id, EvFd(), fd, errno);
    }
  } else {
    ev.events = BaseEvent::EVENT_NULL;
    if (epoll_ctl(EvFd(), EPOLL_CTL_MOD, fd, &ev) == -1) {
      ERROR("DelWriteEvent id:{},EvFd:{},fd:{}, EPOLL_CTL_DEL error errno:{}", id, EvFd(), fd, errno);
    }
  }
}

void EpollEvent::EventRead() {
  struct epoll_event events[eventsSize];
  int waitInterval = -1;
  if (timer_) {
    waitInterval = static_cast<int>(timer_->Interval());
  }
  while (running_.load()) {
    int nfds = epoll_wait(EvFd(), events, eventsSize, waitInterval);
    for (int i = 0; i < nfds; ++i) {
      if ((events[i].events & EVENT_HUB) || (events[i].events & EVENT_ERROR)) {
        // If the event is an error event, call DoError
        DoError(events[i], "");
        continue;
      }
      std::shared_ptr<Connection> conn;
      if (events[i].events & EVENT_READ) {
        // If the event is less than the listen socket, it is a new connection
        // If getListenSocket is nullptr, it means the event is not a listen socket
        if (!getListenSocket(events[i].data.u64)) {
          conn = getConn_(events[i].data.u64);
        }
        DoRead(events[i], conn);
      }

      if ((mode_ & EVENT_MODE_WRITE) && events[i].events & EVENT_WRITE) {
        conn = getConn_(events[i].data.u64);
        if (!conn) {  // If the connection is empty, call DoError
          DoError(events[i], "connection is null");
          continue;
        }
        // If the event is a write event, call DoWrite
        DoWrite(events[i], conn);
      }
    }
    if (timer_) {
      timer_->OnTimer();
    }
  }
}

void EpollEvent::EventWrite() {
  struct epoll_event events[eventsSize];
  while (running_.load()) {
    int nfds = epoll_wait(EvFd(), events, eventsSize, -1);
    for (int i = 0; i < nfds; ++i) {
      if ((events[i].events & EVENT_HUB) || (events[i].events & EVENT_ERROR)) {
        DoError(events[i], "");
      }
      auto conn = getConn_(events[i].data.u64);
      if (!conn) {
        DoError(events[i], "connection is null");
        continue;
      }
      if (events[i].events & EVENT_WRITE) {
        DoWrite(events[i], conn);
      }
    }
  }
}

void EpollEvent::DoRead(const epoll_event &event, const std::shared_ptr<Connection> &conn) {
  if (auto listen_socket = getListenSocket(event.data.u64); listen_socket) {
    auto newConn = std::make_shared<Connection>(nullptr);
    auto connFd = listen_socket->OnReadable(newConn, nullptr);
    if (connFd < 0) {
      DoError(event, "accept error");
      return;
    }
    onCreate_(connFd, newConn);
  } else if (conn) {
    std::string readBuff;
    int ret = conn->net_event_->OnReadable(conn, &readBuff);
    if (ret == NE_ERROR) {
      DoError(event, "read error,errno: " + std::to_string(errno));
      return;
    } else if (ret == NE_CLOSE) {
      DoError(event, "");
      return;
    }
    onMessage_(event.data.u64, std::move(readBuff));
  } else {
    DoError(event, "connection is null");
  }
}

void EpollEvent::DoWrite(const epoll_event &event, const std::shared_ptr<Connection> &conn) {
  auto ret = conn->net_event_->OnWritable();
  if (ret == NE_ERROR) {
    DoError(event, "write error,errno: " + std::to_string(errno));
    return;
  }
  if (ret == NE_OK) {  // If the write is successful, delete the write event
    DelWriteEvent(event.data.u64, conn->fd_);
  }
}

void EpollEvent::DoError(const epoll_event &event, std::string &&err) { onClose_(event.data.u64, std::move(err)); }

}  // namespace net
#endif
