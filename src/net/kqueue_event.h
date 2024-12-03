/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "config.h"

#ifdef HAVE_KQUEUE

#  include <arpa/inet.h>
#  include <sys/event.h>
#  include <sys/socket.h>
#  include <unistd.h>
#  include <string>

#  include "base_event.h"

namespace net {

class KqueueEvent : public BaseEvent {
 public:
  explicit KqueueEvent(std::vector<std::shared_ptr<ListenSocket>> &listenSockets, int8_t mode)
      : BaseEvent(std::move(listenSockets), mode, BaseEvent::EVENT_TYPE_KQUEUE){};

  ~KqueueEvent() override { Close(); }

  bool Init() override;

  void AddEvent(uint64_t id, int fd, int mask) override;

  void DelEvent(int fd) override;

  void AddWriteEvent(uint64_t id, int fd) override;

  void DelWriteEvent(uint64_t id, int fd) override;

  void EventPoll() override;

  void EventRead();

  void EventWrite();

  void DoRead(const struct kevent &event, const std::shared_ptr<Connection> &conn);

  void DoWrite(const struct kevent &event, const std::shared_ptr<Connection> &conn);

  void DoError(const struct kevent &event, std::string &&err);

 private:
  const int eventsSize = 1024;
};

}  // namespace net

#endif
