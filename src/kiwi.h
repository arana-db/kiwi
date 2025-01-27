// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

/*
  Designed a set of functions and variables associated with
  the kiwi server.
 */
#pragma once

#include "cmd_table_manager.h"
#include "cmd_thread_pool.h"
#include "common.h"
#include "net/event_server.h"
#include "options.h"

#define KIWI_VERSION "4.0.0"

#ifdef BUILD_DEBUG
#  define KIWI_BUILD_TYPE "DEBUG"
#else
#  define KIWI_BUILD_TYPE "RELEASE"
#endif

#ifndef KIWI_GIT_COMMIT_ID
#  define KIWI_GIT_COMMIT_ID "unknown"
#endif

#ifndef KIWI_BUILD_DATE
#  define KIWI_BUILD_DATE "unknown"
#endif

namespace kiwi {
class Raft;
}  // namespace kiwi

class KiwiDB final {
 public:
  KiwiDB() = default;
  ~KiwiDB() = default;

  bool ParseArgs(int ac, char* av[]);
  const PString& GetConfigName() const { return options_.GetConfigName(); }

  bool Init();
  void Run();
  //  void Recycle();
  void Stop();

  static void OnNewConnection(uint64_t connId, std::shared_ptr<kiwi::PClient>& client, const net::SocketAddr& addr);

  //  KiwiDB::CmdTableManager& GetCmdTableManager();
  uint32_t GetCmdID() { return ++cmd_id_; };

  void SubmitFast(const std::shared_ptr<kiwi::CmdThreadPoolTask>& runner) { cmd_threads_.SubmitFast(runner); }
  void SubmitSlow(const std::shared_ptr<kiwi::CmdThreadPoolTask>& runner) { cmd_threads_.SubmitSlow(runner); }

  void PushWriteTask(const std::shared_ptr<kiwi::PClient>& client) {
    std::string msg;
    client->Reply(msg);
    client->SendOver();
    event_server_->SendPacket(client, std::move(msg));
  }

  void SendPacket2Client(const std::shared_ptr<kiwi::PClient>& client, std::string&& msg) {
    event_server_->SendPacket(client, std::move(msg));
  }

  void CloseConnection(const std::shared_ptr<kiwi::PClient>& client) { event_server_->CloseConnection(client); }

  void TCPConnect(
      const net::SocketAddr& addr,
      const std::function<void(uint64_t, std::shared_ptr<kiwi::PClient>&, const net::SocketAddr&)>& onConnect,
      const std::function<void(std::string)>& cb);

  time_t GetStartTime() { return start_time_s_; }

 public:
  uint16_t port_{0};

  PString master_;
  uint16_t master_port_{0};

  kiwi::Options options_;

  static const uint32_t kRunidSize;

 private:
  kiwi::CmdThreadPool cmd_threads_;

  std::unique_ptr<net::EventServer<std::shared_ptr<kiwi::PClient>>> event_server_;
  uint32_t cmd_id_ = 0;

  time_t start_time_s_ = 0;
};

extern std::unique_ptr<KiwiDB> g_kiwi;
