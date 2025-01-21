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
  bool Start(const std::vector<std::shared_ptr<ListenSocket>> &listen_sockets, const std::shared_ptr<Timer> &timer);

  // Stop the thread
  void Stop();

  // Create a new connection callback function
  void OnNetEventCreate(int fd, const std::shared_ptr<Connection> &conn);

  // Read message callback function
  void OnNetEventMessage(uint64_t conn_id, std::string &&read_data);

  // Close connection callback function
  void OnNetEventClose(uint64_t conn_id, std::string &&err);

  // Server actively closes the connection
  void CloseConnection(uint64_t conn_id);

  void TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> net_event);

  void TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> net_event, OnCreate<T> on_connect);

  void Wait();

  // Send message to the client
  void SendPacket(const T &conn, std::string &&msg);

 private:
  // Create read thread
  bool CreateReadThread(const std::vector<std::shared_ptr<ListenSocket>> &listen_sockets,
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
bool ThreadManager<T>::Start(const std::vector<std::shared_ptr<ListenSocket>> &listen_sockets,
                             const std::shared_ptr<Timer> &timer) {
  if (!CreateReadThread(listen_sockets, timer)) {
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
  auto conn_id = getConnId();
  if constexpr (IsPointer_v<T>) {
    t->SetConnId(conn_id);
    t->SetThreadIndex(index_);
  } else {
    t.SetConnId(conn_id);
    t.SetThreadIndex(index_);
  }

  {
    std::lock_guard lock(mutex_);
    connections_.emplace(conn_id, std::make_pair(t, conn));
  }
  read_thread_->AddNewEvent(conn_id, fd, BaseEvent::EVENT_READ);
  if (write_thread_) {
    write_thread_->AddNewEvent(conn_id, fd, BaseEvent::EVENT_NULL);  // add null event to write_thread epoll
  }

  on_create_(conn_id, t, conn->addr_);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventMessage(uint64_t conn_id, std::string &&read_data) {
  T t;
  {
    std::shared_lock lock(mutex_);
    auto iter = connections_.find(conn_id);
    if (iter == connections_.end()) {
      return;
    }
    t = iter->second.first;
  }
  on_message_(std::move(read_data), t);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventClose(uint64_t conn_id, std::string &&err) {
  int fd;
  std::lock_guard lock(mutex_);
  auto iter = connections_.find(conn_id);
  if (iter == connections_.end()) {
    return;
  }
  fd = iter->second.second->fd_;

  read_thread_->CloseConnection(fd);
  if (net_options_.GetRwSeparation()) {
    write_thread_->CloseConnection(fd);
  }

  iter->second.second->net_event_->Close();  // close socket
  on_close_(iter->second.first, std::move(err));
  connections_.erase(iter);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::CloseConnection(uint64_t conn_id) {
  OnNetEventClose(conn_id, "");
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> net_event) {
  auto newConn = std::make_shared<Connection>(std::move(net_event));
  newConn->addr_ = addr;
  T t;
  on_init_(&t);
  auto connId = DoTCPConnect(t, newConn->net_event_->Fd(), newConn);
  on_connect_(connId, t, addr);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> net_event, OnCreate<T> on_connect) {
  auto new_conn = std::make_shared<Connection>(std::move(net_event));
  new_conn->addr_ = addr;
  T t;
  on_init_(&t);
  auto conn_id = DoTCPConnect(t, new_conn->net_event_->Fd(), new_conn);
  on_connect(conn_id, t, addr);
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
  uint64_t conn_id = 0;
  if constexpr (IsPointer_v<T>) {
    conn_id = conn->GetConnId();
  } else {
    conn_id = conn.GetConnId();
  }
  std::shared_ptr<Connection> conn_ptr;
  {
    auto iter = connections_.find(conn_id);
    if (iter == connections_.end()) {
      return;
    }
    conn_ptr = iter->second.second;
  }

  conn_ptr->net_event_->SendPacket(std::move(msg));
  if (net_options_.GetRwSeparation()) {
    write_thread_->SetWriteEvent(conn_id, conn_ptr->fd_);
  } else {
    read_thread_->SetWriteEvent(conn_id, conn_ptr->fd_);
  }
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::CreateReadThread(const std::vector<std::shared_ptr<ListenSocket>> &listen_sockets,
                                        const std::shared_ptr<Timer> &timer) {
  std::shared_ptr<BaseEvent> event;
  int8_t event_mode = BaseEvent::EVENT_MODE_READ;
  if (!net_options_.GetRwSeparation()) {
    event_mode |= BaseEvent::EVENT_MODE_WRITE;
  }

#if defined(HAVE_EPOLL)
  event = std::make_shared<EpollEvent>(listen_sockets, event_mode);
#elif defined(HAVE_KQUEUE)
  event = std::make_shared<KqueueEvent>(listen_sockets, event_mode);
#endif

  event->AddTimer(timer);

  event->SetOnCreate(
      [this](uint64_t conn_id, const std::shared_ptr<Connection> &conn) { OnNetEventCreate(conn_id, conn); });

  event->SetOnMessage(
      [this](uint64_t conn_id, std::string &&read_data) { OnNetEventMessage(conn_id, std::move(read_data)); });

  event->SetOnClose([this](uint64_t conn_id, std::string &&err) { OnNetEventClose(conn_id, std::move(err)); });

  event->SetGetConn([this](uint64_t conn_id) -> std::shared_ptr<Connection> {
    std::shared_lock lock(mutex_);
    auto iter = connections_.find(conn_id);
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

  event->SetOnClose([this](uint64_t conn_id, std::string &&msg) { OnNetEventClose(conn_id, std::move(msg)); });
  event->SetGetConn([this](uint64_t conn_id) -> std::shared_ptr<Connection> {
    std::shared_lock lock(mutex_);
    auto iter = connections_.find(conn_id);
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
  auto conn_id = getConnId();
  if constexpr (IsPointer_v<T>) {
    t->SetConnId(conn_id);
    t->SetThreadIndex(index_);
  } else {
    t.SetConnId(conn_id);
    t.SetThreadIndex(index_);
  }
  conn->fd_ = fd;

  {
    std::lock_guard lock(mutex_);
    connections_.emplace(conn_id, std::make_pair(t, conn));
  }

  read_thread_->AddNewEvent(conn_id, fd, BaseEvent::EVENT_READ);
  return conn_id;
}

}  // namespace net
