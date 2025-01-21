/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <sys/socket.h>
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <string>

#include "base_socket.h"
#include "callback_function.h"
#include "client_socket.h"
#include "io_thread.h"
#include "listen_socket.h"
#include "net_options.h"
#include "thread_manager.h"

namespace net {

template <typename T>
requires HasSetFdFunction<T>
class EventServer final {
 public:
  explicit EventServer(NetOptions netOptions) : opt_(netOptions) { threadsManager_.reserve(netOptions.GetThreadNum()); }

  ~EventServer() = default;

  void SetOnInit(OnInit<T> &&func) { onInit_ = std::move(func); }

  void SetOnCreate(OnCreate<T> &&func) { onCreate_ = std::move(func); }

  void SetOnConnect(OnCreate<T> &&func) { onConnect_ = std::move(func); }

  void SetOnMessage(OnMessage<T> &&func) { onMessage_ = std::move(func); }

  void SetOnClose(OnClose<T> &&func) { onClose_ = std::move(func); }

  inline void AddListenAddr(const SocketAddr &addr) { listen_addrs_.emplace_back(addr); }

  void InitTimer(int64_t interval) { timer_ = std::make_shared<Timer>(interval); }

  int64_t AddTimerTask(const std::shared_ptr<ITimerTask> &task) { return timer_->AddTask(task); }

  void DelTimerTask(int64_t timerId) { timer_->DelTask(timerId); }

  std::pair<bool, std::string> StartServer(int64_t interval = 0);

  std::pair<bool, std::string> StartClientServer();

  // Stop the server
  void StopServer();

  // Send message to the client
  void SendPacket(const T &conn, std::string &&msg);

  // Server Active close the connection
  void CloseConnection(const T &conn);

  // When the service is started, the main thread is blocked,
  // and when all the subthreads are finished, the function unblocks and returns
  void Wait() {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [this] { return !running_.load(); });
  }

  void TCPConnect(const SocketAddr &addr, OnCreate<T> onConnect, const std::function<void(std::string)> &cb);

  void TCPConnect(const SocketAddr &addr, const std::function<void(std::string)> &cb);

 private:
  int StartThreadManager(bool serverMode);

 private:
  OnInit<T> onInit_;  // The callback function used to initialize data before creating a connection

  OnCreate<T> onCreate_;  // The callback function when the connection is created

  OnCreate<T> onConnect_;  // The callback function when the client connection succeed

  OnMessage<T> onMessage_;  // The callback function when the message is received

  OnClose<T> onClose_;  // The callback function when the connection is closed

  std::vector<SocketAddr> listen_addrs_;  // The address to listen on

  std::atomic<bool> running_ = true;  // Whether the server is running

  NetOptions opt_;  // The option of the server

  std::vector<std::unique_ptr<ThreadManager<T>>> threadsManager_;

  std::mutex mtx_;
  std::condition_variable cv_;

  std::shared_ptr<Timer> timer_;
};

template <typename T>
requires HasSetFdFunction<T>
std::pair<bool, std::string> EventServer<T>::StartServer(int64_t interval) {
  if (opt_.GetThreadNum() <= 0) {
    return std::pair(false, "thread num must be greater than 0");
  }
  if (!onInit_) {
    return std::pair(false, "OnInit must be set");
  }
  if (!onCreate_) {
    return std::pair(false, "OnCreate must be set");
  }
  if (!onMessage_) {
    return std::pair(false, "OnMessage must be set");
  }
  if (!onClose_) {
    return std::pair(false, "OnClose must be set");
  }

  if (interval > 0 && !timer_) {
    InitTimer(interval);
  }

  for (int8_t i = 0; i < opt_.GetThreadNum(); ++i) {
    auto tm = std::make_unique<ThreadManager<T>>(i, opt_);
    tm->SetOnInit(onInit_);
    tm->SetOnCreate(onCreate_);
    tm->SetOnConnect(onConnect_);
    tm->SetOnMessage(onMessage_);
    tm->SetOnClose(onClose_);
    threadsManager_.emplace_back(std::move(tm));
  }

  if (StartThreadManager(true) != static_cast<int>(NetListen::OK)) {
    return std::pair(false, "StartThreadManager function error");
  }

  return std::pair(true, "");
}

template <typename T>
requires HasSetFdFunction<T>
std::pair<bool, std::string> EventServer<T>::StartClientServer() {
  if (opt_.GetThreadNum() <= 0) {
    return std::pair(false, "thread num must be greater than 0");
  }
  if (!onInit_) {
    return std::pair(false, "OnInit must be set");
  }
  if (!onConnect_) {
    return std::pair(false, "OnConnect must be set");
  }
  if (!onMessage_) {
    return std::pair(false, "OnMessage must be set");
  }
  if (!onClose_) {
    return std::pair(false, "OnClose must be set");
  }

  for (int8_t i = 0; i < opt_.GetThreadNum(); ++i) {
    auto tm = std::make_unique<ThreadManager<T>>(i, opt_);
    tm->SetOnInit(onInit_);
    tm->SetOnConnect(onConnect_);
    tm->SetOnMessage(onMessage_);
    tm->SetOnClose(onClose_);
    threadsManager_.emplace_back(std::move(tm));
  }

  if (StartThreadManager(false) != static_cast<int>(NetListen::OK)) {
    return std::pair(false, "StartThreadManager function error");
  }

  return std::pair(true, "");
}

template <typename T>
requires HasSetFdFunction<T>
void EventServer<T>::StopServer() {
  bool expected = true;
  if (running_.compare_exchange_strong(expected, false)) {
    for (const auto &thread : threadsManager_) {
      thread->Stop();
    }
  }
  cv_.notify_one();
}

template <typename T>
requires HasSetFdFunction<T>
void EventServer<T>::SendPacket(const T &conn, std::string &&msg) {
  int thIndex;
  if constexpr (IsPointer_v<T>) {
    thIndex = conn->GetThreadIndex();
  } else {
    thIndex = conn.GetThreadIndex();
  }
  threadsManager_[thIndex]->SendPacket(conn, std::move(msg));
}

template <typename T>
requires HasSetFdFunction<T>
void EventServer<T>::CloseConnection(const T &conn) {
  int thIndex;
  int connId;
  if constexpr (IsPointer_v<T>) {
    thIndex = conn->GetThreadIndex();
    connId = conn->GetConnId();
  } else {
    thIndex = conn.GetThreadIndex();
    connId = conn.GetConnId();
  }

  threadsManager_[thIndex]->CloseConnection(connId);
}

template <typename T>
requires HasSetFdFunction<T>
void EventServer<T>::TCPConnect(const SocketAddr &addr, OnCreate<T> onConnect,
                                const std::function<void(std::string)> &cb) {
  auto clientSocket = std::make_unique<ClientSocket>(addr);
  clientSocket->SetFailCallback(cb);
  if (!clientSocket->Connect()) {
    return;
  }

  threadsManager_[0]->TCPConnect(addr, std::move(clientSocket), onConnect);
}

template <typename T>
requires HasSetFdFunction<T>
void EventServer<T>::TCPConnect(const SocketAddr &addr, const std::function<void(std::string)> &cb) {
  auto clientSocket = std::make_unique<ClientSocket>(addr);
  clientSocket->SetFailCallback(cb);
  if (!clientSocket->Connect()) {
    return;
  }

  threadsManager_[0]->TCPConnect(addr, std::move(clientSocket));
}

template <typename T>
requires HasSetFdFunction<T>
int EventServer<T>::StartThreadManager(bool serverMode) {
  std::vector<std::shared_ptr<ListenSocket>> listen_sockets;
  auto tcpKeepAlive = opt_.GetOpTcpKeepAlive();

  if (serverMode) {
    for (auto &listenAddr : listen_addrs_) {
      std::shared_ptr<ListenSocket> listen(ListenSocket::CreateTCPListen());
      listen->SetListenAddr(listenAddr);
      listen->SetBSTcpKeepAlive(tcpKeepAlive);
      listen_sockets.push_back(listen);
      if (auto ret = (listen->Init() != static_cast<int>(NetListen::OK))) {
        listen_sockets.clear();  // Clean up all sockets
        return ret;
      }
    }
  }

  int i = 0;
  for (const auto &thread : threadsManager_) {
    if (i > 0 && ListenSocket::REUSE_PORT && serverMode) {
      for (auto &listen : listen_sockets) {
        auto listenAddr = listen->GetListenAddr();
        listen.reset(ListenSocket::CreateTCPListen());
        listen->SetListenAddr(listenAddr);
        listen->SetBSTcpKeepAlive(tcpKeepAlive);
        if (auto ret = (listen->Init() != static_cast<int>(NetListen::OK))) {
          listen_sockets.clear();  // Clean up all sockets
          return ret;
        }
      }
    }

    // timer only works in the first thread
    bool ret = i == 0 ? thread->Start(listen_sockets, timer_) : thread->Start(listen_sockets, nullptr);
    if (!ret) {
      return -1;
    }
    ++i;
  }

  return static_cast<int>(NetListen::OK);
}

}  // namespace net
