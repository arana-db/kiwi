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
  explicit ThreadManager(int8_t index, NetOptions &netOptions) : index_(index), netOptions_(netOptions) {}

  ~ThreadManager();

  // set new connect create before callback function
  void SetOnInit(const OnInit<T> &func) { onInit_ = func; }

  // set new connect create callback function
  void SetOnCreate(const OnCreate<T> &func) { onCreate_ = func; }

  void SetOnConnect(const OnCreate<T> &func) { onConnect_ = func; }

  // set read message callback function
  void SetOnMessage(const OnMessage<T> &func) { onMessage_ = func; }

  // set close connect callback function
  void SetOnClose(const OnClose<T> &func) { onClose_ = func; }

  // Start the thread and initialize the event
  bool Start(const std::shared_ptr<NetEvent> &listen, const std::shared_ptr<Timer> &timer);

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
  bool CreateReadThread(const std::shared_ptr<NetEvent> &listen, const std::shared_ptr<Timer> &timer);

  // Create write thread if rwSeparation_ is true
  bool CreateWriteThread();

  uint64_t DoTCPConnect(T &t, int fd, const std::shared_ptr<Connection> &conn);

  uint32_t get_client_count() const { return clientCount_.load(); }

  void client_count_increment() { clientCount_.fetch_add(1, std::memory_order_relaxed); }

  void client_count_decrement() { clientCount_.fetch_sub(1, std::memory_order_relaxed); }

 private:
  const int8_t index_ = 0;            // The index of the thread
  uint32_t tcpKeepAlive_ = 300;       // The timeout of the keepalive connection in seconds
  std::atomic<bool> running_ = true;  // Whether the thread is running

  NetOptions netOptions_;

  inline static std::atomic<uint32_t> clientCount_{0};

  std::unique_ptr<IOThread> readThread_;   // Read thread
  std::unique_ptr<IOThread> writeThread_;  // Write thread

  std::unordered_map<uint64_t, std::pair<T, std::shared_ptr<Connection>>>
      connections_;  // All connections for the current thread

  std::shared_mutex mutex_;

  OnInit<T> onInit_;

  OnCreate<T> onCreate_;

  OnCreate<T> onConnect_;

  OnMessage<T> onMessage_;

  OnClose<T> onClose_;
};

template <typename T>
requires HasSetFdFunction<T>
ThreadManager<T>::~ThreadManager() {
  Stop();
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::Start(const std::shared_ptr<NetEvent> &listen, const std::shared_ptr<Timer> &timer) {
  if (!CreateReadThread(listen, timer)) {
    return false;
  }
  if (netOptions_.GetRwSeparation()) {
    return CreateWriteThread();
  }
  return true;
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::Stop() {
  bool expected = true;
  if (running_.compare_exchange_strong(expected, false)) {
    readThread_->Stop();
    if (netOptions_.GetRwSeparation()) {
      writeThread_->Stop();
    }
  }
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::OnNetEventCreate(int fd, const std::shared_ptr<Connection> &conn) {
  if (!clientCount_.compare_exchange_strong(
          expected,
          expected + 1,
          std::memory_order_seq_cst,
          std::memory_order_seq_cst) ||
      expected >= netOptions_.GetMaxClients()) {
    INFO("Max client connetions, refuse new connection fd: %d", fd);
    std::string response = "-ERR max clients reached\r\n";
    ssize_t sent = ::send(fd, response.c_str(), response.size(), 0);
    if (sent < 0) {
        ERROR("Failed to send error response to fd: %d, errno: %d", fd, errno);
    }
    if (::close(fd) < 0) {
        ERROR("Failed to close fd: %d, errno: %d", fd, errno);
    }
    return;
  }

  T t;
  onInit_(&t);
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
  readThread_->AddNewEvent(connId, fd, BaseEvent::EVENT_READ);
  if (writeThread_) {
    writeThread_->AddNewEvent(connId, fd, BaseEvent::EVENT_NULL);  // add null event to write_thread epoll
  }

  onCreate_(connId, t, conn->addr_);
  client_count_increment();
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
  onMessage_(std::move(readData), t);
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

  readThread_->CloseConnection(fd);
  if (netOptions_.GetRwSeparation()) {
    writeThread_->CloseConnection(fd);
  }

  iter->second.second->netEvent_->Close();  // close socket
  onClose_(iter->second.first, std::move(err));
  connections_.erase(iter);
  client_count_decrement();
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
  onInit_(&t);
  auto connId = DoTCPConnect(t, newConn->netEvent_->Fd(), newConn);
  onConnect_(connId, t, addr);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::TCPConnect(const SocketAddr &addr, std::unique_ptr<NetEvent> netEvent, OnCreate<T> onConnect) {
  auto newConn = std::make_shared<Connection>(std::move(netEvent));
  newConn->addr_ = addr;
  T t;
  onInit_(&t);
  auto connId = DoTCPConnect(t, newConn->netEvent_->Fd(), newConn);
  onConnect(connId, t, addr);
}

template <typename T>
requires HasSetFdFunction<T>
void ThreadManager<T>::Wait() {
  readThread_->Wait();
  if (netOptions_.GetRwSeparation()) {
    writeThread_->Wait();
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

  connPtr->netEvent_->SendPacket(std::move(msg));
  if (netOptions_.GetRwSeparation()) {
    writeThread_->SetWriteEvent(connId, connPtr->fd_);
  } else {
    readThread_->SetWriteEvent(connId, connPtr->fd_);
  }
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::CreateReadThread(const std::shared_ptr<NetEvent> &listen, const std::shared_ptr<Timer> &timer) {
  std::shared_ptr<BaseEvent> event;
  int8_t eventMode = BaseEvent::EVENT_MODE_READ;
  if (!netOptions_.GetRwSeparation()) {
    eventMode |= BaseEvent::EVENT_MODE_WRITE;
  }

#if defined(HAVE_EPOLL)
  event = std::make_shared<EpollEvent>(listen, eventMode);
#elif defined(HAVE_KQUEUE)
  event = std::make_shared<KqueueEvent>(listen, eventMode);
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

  readThread_ = std::make_unique<IOThread>(event);
  return readThread_->Run();
}

template <typename T>
requires HasSetFdFunction<T>
bool ThreadManager<T>::CreateWriteThread() {
  std::shared_ptr<BaseEvent> event;

#if defined(HAVE_EPOLL)
  event = std::make_shared<EpollEvent>(nullptr, BaseEvent::EVENT_MODE_WRITE);
#elif defined(HAVE_KQUEUE)
  event = std::make_shared<KqueueEvent>(nullptr, BaseEvent::EVENT_MODE_WRITE);
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

  writeThread_ = std::make_unique<IOThread>(event);
  return writeThread_->Run();
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

  readThread_->AddNewEvent(connId, fd, BaseEvent::EVENT_READ);
  return connId;
}

}  // namespace net
