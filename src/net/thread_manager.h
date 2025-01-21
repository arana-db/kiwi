/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "callback_function.h"
#include "config.h"
#include "io_thread.h"
#include "net_options.h"

#if defined(HAVE_EPOLL)

#  include "epoll_event.h"

#elif defined(HAVE_KQUEUE)

#  include "kqueue_event.h"

#endif

namespace net {

extern uint64_t getConnId();

template <typename T>
requires HasSetFdFunction<T>
class ThreadManager {
 public:
  explicit ThreadManager(int8_t index, NetOptions &net_options) : index_(index), net_options_(net_options) {}

  ~ThreadManager();

  // set new connect create before callback function
  void SetOnInit(const OnInit<T> &func) { on_init_ = func; }

  // set new connect create callback function
  void SetOnCreate(const OnCreate<T> &func) { on_create_ = func; }

  void SetOnConnect(const OnCreate<T> &func) { on_connect_ = func; }

  // set read message callback function
  void SetOnMessage(const OnMessage<T> &func) { on_message_ = func; }

  // set close connect callback function
  void SetOnClose(const OnClose<T> &func) { on_close_ = func; }

  // Start the thread and initialize the event
  bool Start(const std::vector<std::shared_ptr<ListenSocket>> &listenSockets, const std::shared_ptr<Timer> &timer);

  // Stop the thread
  void Stop();

  // Create a new connection callback function
  void OnNetEventCreate(int fd, const std::shared_ptr<Connection> &conn);

  // Read message callback function
  void OnNetEventMessage(uint64_t connId, std::string &&readData);

  // Close connection callback function
  void OnNetEventClose(uint64_t connId, std::string &&err);

  // Server actively closes the connection
  void CloseConnection(uint64_t connId);

  void TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> netEvent);

  void TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> netEvent, OnCreate<T> onConnect);

  void Wait();

  // Send message to the client
  void SendPacket(const T &conn, std::string &&msg);

 private:
  // Create read thread
  bool CreateReadThread(const std::vector<std::shared_ptr<ListenSocket>> &listenSockets,
                        const std::shared_ptr<Timer> &timer);

  // Create write thread if rwSeparation_ is true
  bool CreateWriteThread();

  uint64_t DoTCPConnect(T &t, int fd, const std::shared_ptr<Connection> &conn);

 private:
  const int8_t index_ = 0;            // The index of the thread
  uint32_t tcp_keep_alive_ = 300;     // The timeout of the keepalive connection in seconds
  std::atomic<bool> running_ = true;  // Whether the thread is running

  NetOptions net_options_;

  std::unique_ptr<IOThread> read_thread_;   // Read thread
  std::unique_ptr<IOThread> write_thread_;  // Write thread

  // All connections for the current thread
  std::unordered_map<uint64_t, std::pair<T, std::shared_ptr<Connection>>> connections_;

  std::shared_mutex mutex_;

  OnInit<T> on_init_;

  OnCreate<T> on_create_;

  OnCreate<T> on_connect_;

  OnMessage<T> on_message_;

  OnClose<T> on_close_;
};

template <typename T>
requires HasSetFdFunction<T>
ThreadManager<T>::~ThreadManager() {
  Stop();
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::Start(const std::vector<std::shared_ptr<ListenSocket>> &listenSockets,
                             const std::shared_ptr<Timer> &timer) {
  if (!CreateReadThread(listenSockets, timer)) {
    return false;
  }
  if (net_options_.GetRwSeparation()) {
    return CreateWriteThread();
  }
  return true;
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::Stop() {
  bool expected = true;
  if (running_.compare_exchange_strong(expected, false)) {
    read_thread_->Stop();
    if (net_options_.GetRwSeparation()) {
      write_thread_->Stop();
    }
  }
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventCreate(int fd, const std::shared_ptr<Connection> &conn) {
  T t;
  on_init_(&t);
  auto connId = getConnId();
  if constexpr (IsPointer_v<T>) {
    t->SetConnId(connId);
    t->SetThreadIndex(index_);
  } else {
    t.SetConnId(connId);
    t.SetThreadIndex(index_);
  }

  {
    std::lock_guard lock(mutex_);
    connections_.emplace(connId, std::make_pair(t, conn));
  }
  read_thread_->AddNewEvent(connId, fd, BaseEvent::EVENT_READ);
  if (write_thread_) {
    write_thread_->AddNewEvent(connId, fd, BaseEvent::EVENT_NULL);  // add null event to write_thread epoll
  }

  on_create_(connId, t, conn->addr_);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventMessage(uint64_t connId, std::string &&readData) {
  T t;
  {
    std::shared_lock lock(mutex_);
    auto iter = connections_.find(connId);
    if (iter == connections_.end()) {
      return;
    }
    t = iter->second.first;
  }
  on_message_(std::move(readData), t);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventClose(uint64_t connId, std::string &&err) {
  int fd;
  std::lock_guard lock(mutex_);
  auto iter = connections_.find(connId);
  if (iter == connections_.end()) {
    return;
  }
  fd = iter->second.second->fd_;

  read_thread_->CloseConnection(fd);
  if (net_options_.GetRwSeparation()) {
    write_thread_->CloseConnection(fd);
  }

  iter->second.second->netEvent_->Close();  // close socket
  on_close_(iter->second.first, std::move(err));
  connections_.erase(iter);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::CloseConnection(uint64_t connId) {
  OnNetEventClose(connId, "");
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> netEvent) {
  auto newConn = std::make_shared<Connection>(std::move(netEvent));
  newConn->addr_ = addr;
  T t;
  on_init_(&t);
  auto connId = DoTCPConnect(t, newConn->net_event_->Fd(), newConn);
  on_connect_(connId, t, addr);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> netEvent, OnCreate<T> onConnect) {
  auto newConn = std::make_shared<Connection>(std::move(netEvent));
  newConn->addr_ = addr;
  T t;
  on_init_(&t);
  auto connId = DoTCPConnect(t, newConn->net_event_->Fd(), newConn);
  onConnect(connId, t, addr);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::Wait() {
  read_thread_->Wait();
  if (net_options_.GetRwSeparation()) {
    write_thread_->Wait();
  }
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::SendPacket(const T &conn, std::string &&msg) {
  std::shared_lock lock(mutex_);
  uint64_t connId = 0;
  if constexpr (IsPointer_v<T>) {
    connId = conn->GetConnId();
  } else {
    connId = conn.GetConnId();
  }
  std::shared_ptr<Connection> connPtr;
  {
    auto iter = connections_.find(connId);
    if (iter == connections_.end()) {
      return;
    }
    connPtr = iter->second.second;
  }

  connPtr->net_event_->SendPacket(std::move(msg));
  if (net_options_.GetRwSeparation()) {
    write_thread_->SetWriteEvent(connId, connPtr->fd_);
  } else {
    read_thread_->SetWriteEvent(connId, connPtr->fd_);
  }
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::CreateReadThread(const std::vector<std::shared_ptr<ListenSocket>> &listenSockets,
                                        const std::shared_ptr<Timer> &timer) {
  std::shared_ptr<BaseEvent> event;
  int8_t eventMode = BaseEvent::EVENT_MODE_READ;
  if (!net_options_.GetRwSeparation()) {
    eventMode |= BaseEvent::EVENT_MODE_WRITE;
  }

#if defined(HAVE_EPOLL)
  event = std::make_shared<EpollEvent>(listenSockets, eventMode);
#elif defined(HAVE_KQUEUE)
  event = std::make_shared<KqueueEvent>(listenSockets, eventMode);
#endif

  event->AddTimer(timer);

  event->SetOnCreate(
      [this](uint64_t connId, const std::shared_ptr<Connection> &conn) { OnNetEventCreate(connId, conn); });

  event->SetOnMessage(
      [this](uint64_t connId, std::string &&readData) { OnNetEventMessage(connId, std::move(readData)); });

  event->SetOnClose([this](uint64_t connId, std::string &&err) { OnNetEventClose(connId, std::move(err)); });

  event->SetGetConn([this](uint64_t connId) -> std::shared_ptr<Connection> {
    std::shared_lock lock(mutex_);
    auto iter = connections_.find(connId);
    if (iter == connections_.end()) {
      return nullptr;
    }
    return iter->second.second;
  });

  read_thread_ = std::make_unique<IOThread>(event);
  return read_thread_->Run();
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::CreateWriteThread() {
  std::shared_ptr<BaseEvent> event;

#if defined(HAVE_EPOLL)
  event = std::make_shared<EpollEvent>(std::vector<std::shared_ptr<ListenSocket>>(), BaseEvent::EVENT_MODE_WRITE);
#elif defined(HAVE_KQUEUE)
  event = std::make_shared<KqueueEvent>(std::vector<std::shared_ptr<ListenSocket>>(), BaseEvent::EVENT_MODE_WRITE);
#endif

  event->SetOnClose([this](uint64_t connId, std::string &&msg) { OnNetEventClose(connId, std::move(msg)); });
  event->SetGetConn([this](uint64_t connId) -> std::shared_ptr<Connection> {
    std::shared_lock lock(mutex_);
    auto iter = connections_.find(connId);
    if (iter == connections_.end()) {
      return nullptr;
    }
    return iter->second.second;
  });

  write_thread_ = std::make_unique<IOThread>(event);
  return write_thread_->Run();
}

template <typename T>
requires HasSetFdFunction<T>
uint64_t ThreadManager<T>::DoTCPConnect(T &t, int fd, const std::shared_ptr<Connection> &conn) {
  auto connId = getConnId();
  if constexpr (IsPointer_v<T>) {
    t->SetConnId(connId);
    t->SetThreadIndex(index_);
  } else {
    t.SetConnId(connId);
    t.SetThreadIndex(index_);
  }
  conn->fd_ = fd;

  {
    std::lock_guard lock(mutex_);
    connections_.emplace(connId, std::make_pair(t, conn));
  }

  read_thread_->AddNewEvent(connId, fd, BaseEvent::EVENT_READ);
  return connId;
}

}  // namespace net
