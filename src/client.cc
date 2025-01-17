// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

/*
  Implemented a set of functions for interfacing with the client.
 */

#include <fmt/core.h>
#include <algorithm>
#include <memory>

#include "base_cmd.h"
#include "client.h"
#include "cmd_thread_pool_worker.h"
#include "config.h"
#include "env.h"
#include "kiwi.h"
#include "raft/raft.h"
#include "slow_log.h"
#include "std/log.h"
#include "std/std_string.h"

namespace kiwi {

extern kiwi::CmdTableManager cmd_table_manager_;

const ClientInfo ClientInfo::invalidClientInfo = {0, "", -1};

thread_local PClient* PClient::s_current = nullptr;

std::mutex monitors_mutex;
std::set<std::weak_ptr<PClient>, std::owner_less<std::weak_ptr<PClient> > > monitors;

void PClient::SetSubCmdName(const std::string& name) {
  subCmdName_ = name;
  std::transform(subCmdName_.begin(), subCmdName_.end(), subCmdName_.begin(), ::tolower);
}

std::string PClient::FullCmdName() const {
  if (subCmdName_.empty()) {
    return cmdName_;
  }
  return cmdName_ + "|" + subCmdName_;
}

static int ProcessMaster(const char* start, const char* end) {
  auto state = PREPL.GetMasterState();

  switch (state) {
    case kPReplStateConnected:
      // discard all requests before sync;
      // or continue serve with old data? TODO
      return static_cast<int>(end - start);
    case kPReplStateWaitAuth:
      if (end - start >= 5) {
        if (strncasecmp(start, "+OK\r\n", 5) == 0) {
          PClient::Current()->SetAuth();
          return 5;
        } else {
          assert(!!!"check masterauth config, master password maybe wrong");
        }
      } else {
        return 0;
      }
      break;

    case kPReplStateWaitReplconf:
      if (end - start >= 5) {
        if (strncasecmp(start, "+OK\r\n", 5) == 0) {
          return 5;
        } else {
          assert(!!!"check error: send replconf command");
        }
      } else {
        return 0;
      }
      break;

    case kPReplStateWaitRdb: {
      const char* ptr = start;
      // recv RDB file
      if (PREPL.GetRdbSize() == static_cast<std::size_t>(-1)) {
        ++ptr;  // skip $
        int s;
        if (PParseResult::kOK == GetIntUntilCRLF(ptr, end - ptr, s)) {
          assert(s > 0);  // check error for your masterauth or master config

          PREPL.SetRdbSize(s);
          INFO("recv rdb size {}", s);
        }
      } else {
        auto rdb = static_cast<std::size_t>(end - ptr);
        PREPL.SaveTmpRdb(ptr, rdb);
        ptr += rdb;
      }

      return static_cast<int>(ptr - start);
    }

    case kPReplStateOnline:
      break;

    default:
      assert(!!!"wrong master state");
  }

  return -1;  // do nothing
}

int PClient::HandlePacket(std::string&& data) {
  if (data.empty()) {
    return 0;
  }

  s_current = this;
  const char* start = data.data();
  int bytes = data.size();
  const char* const end = start + bytes;

  if (isPeerMaster()) {
    if (isClusterCmdTarget()) {
      // Proccees the packet at one turn.
      int len = RAFT_INST.ProcessClusterCmdResponse(this, start, bytes);  // @todo
      if (len > 0) {
        return len;
      }
    } else {
      // Proccees the packet at one turn.
      //  check slave state
      auto recved = ProcessMaster(start, end);
      if (recved != -1) {
        return recved;
      }
    }
  }

  auto parseRet = resp_parser_->Parse(std::move(data));
  if (parseRet == RespResult::ERROR) {
    ERROR("client {} IP:{} port:{} parse data error", GetUniqueID(), PeerIP(), PeerPort());
    return 0;
  }
  if (parseRet == RespResult::WAIT) {
    DEBUG("client {} IP:{} port:{} parse data wait", GetUniqueID(), PeerIP(), PeerPort());
    return 0;
  }

  auto params = resp_parser_->GetParams();
  if (params.empty()) {
    ERROR("client {} IP:{} port:{} parse data empty", GetUniqueID(), PeerIP(), PeerPort());
    return 0;
  }

  if (!auth_) {
    // auth and hello command can be executed without auth
    if (params[0][0] == kCmdNameAuth || params[0][0] == kCmdNameHello) {
      auto now = ::time(nullptr);
      if (now <= last_auth_ + 1) {
        // avoid guess password.
        g_kiwi->CloseConnection(shared_from_this());
        return 0;
      } else {
        last_auth_ = now;
      }
    } else {
      SetLineString("-NOAUTH Authentication required.");
      SendPacket();
      return 0;
    }
  }

  auto now = std::chrono::steady_clock::now();
  time_stat_->SetEnqueueTs(now);

  if (params.size() > 1) {  // if the size of the parameters is greater than 1，use slow thread execute
    g_kiwi->SubmitSlow(std::make_shared<CmdThreadPoolTask>(shared_from_this(), std::move(params)));
  } else {
    g_kiwi->SubmitFast(std::make_shared<CmdThreadPoolTask>(shared_from_this(), std::move(params)));
  }

  // Propagate(params, GetCurrentDB());

  // g_kiwi->SubmitFast(std::make_shared<CmdThreadPoolTask>(shared_from_this()));

  // check readonly slave and execute command
  //  PError err = PError_ok;
  //  if (PREPL.GetMasterState() != PReplState_none && !IsFlagOn(ClientFlag_master) &&
  //      (info->attr & PCommandAttr::PAttr_write)) {
  //    err = PError_readonlySlave;
  //    ReplyError(err, &reply_);
  //  } else {
  //    PSlowLog::Instance().Begin();
  //    err = PCommandTable::ExecuteCmd(params, info, IsFlagOn(ClientFlag_master) ? nullptr : &reply_);
  //    PSlowLog::Instance().EndAndStat(params);
  //  }
  //
  //  if (err == PError_ok && (info->attr & PAttr_write)) {
  //    Propagate(params);
  //  }

  return 1;
}

PClient* PClient::Current() { return s_current; }

PClient::PClient() {
  auth_ = false;
  reset();
  time_stat_ = std::make_shared<TimeStat>();
  resp_parser_ = std::make_unique<Resp2Parse>();
  resp_encode_ = std::make_unique<Resp2Encode>();
}

void PClient::OnConnect() {
  SetState(ClientState::kOK);
  if (isPeerMaster()) {
    PREPL.SetMasterState(kPReplStateConnected);
    PREPL.SetMaster(std::static_pointer_cast<PClient>(shared_from_this()));

    SetName("MasterConnection");
    SetFlag(kClientFlagMaster);

    if (g_config.master_auth.empty()) {
      SetAuth();
    }

    if (isClusterCmdTarget()) {
      RAFT_INST.SendNodeRequest(this);
    }
  } else {
    if (g_config.password.empty()) {
      SetAuth();
    }
  }
}

std::string PClient::PeerIP() const {
  if (!addr_.IsValid()) {
    ERROR("Invalid address detected for client {}", GetUniqueID());
    return "";
  }
  return addr_.GetIP();
}

int PClient::PeerPort() const {
  if (!addr_.IsValid()) {
    ERROR("Invalid address detected for client {}", GetUniqueID());
    return 0;
  }
  return addr_.GetPort();
}

bool PClient::SendPacket() {
  std::string str;
  resp_encode_->Reply(str);
  g_kiwi->SendPacket2Client(shared_from_this(), std::move(str));
  SendOver();
  return true;
}

bool PClient::SendPacket(std::string&& msg) {
  g_kiwi->SendPacket2Client(shared_from_this(), std::move(msg));
  SendOver();
  return true;
}

bool PClient::SendPacket(UnboundedBuffer& data) {
  g_kiwi->SendPacket2Client(shared_from_this(), std::move(data.ToString()));
  SendOver();
  return true;
}

void PClient::Close() { g_kiwi->CloseConnection(shared_from_this()); }

void PClient::OnClose() {
  SetState(ClientState::kClosed);
  reset();
}

void PClient::reset() { s_current = nullptr; }

bool PClient::isPeerMaster() const {
  const auto& repl_addr = PREPL.GetMasterAddr();
  return repl_addr.GetIP() == PeerIP() && repl_addr.GetPort() == PeerPort();
}

bool PClient::isClusterCmdTarget() const {
  return RAFT_INST.GetClusterCmdCtx().GetPeerIp() == PeerIP() && RAFT_INST.GetClusterCmdCtx().GetPort() == PeerPort();
}

uint64_t PClient::GetUniqueID() const { return GetConnId(); }

ClientInfo PClient::GetClientInfo() const { return {GetUniqueID(), PeerIP().c_str(), PeerPort()}; }

bool PClient::CheckTransation(std::vector<std::string>& param) {
  if (IsFlagOn(kClientFlagMulti)) {
    if (cmdName_ != kCmdNameMulti && cmdName_ != kCmdNameExec && cmdName_ != kCmdNameWatch &&
        cmdName_ != kCmdNameUnWatch && cmdName_ != kCmdNameDiscard) {
      if (!IsFlagOn(kClientFlagWrongExec)) {
        queue_cmds_.push_back(param);
      }
      INFO("queue cmd {}", cmdName_);
      this->SetRes(CmdRes::kQueued);
      g_kiwi->PushWriteTask(shared_from_this());
      return true;
    }
  }
  return false;
}

bool PClient::Watch(int dbno, const std::string& key) {
  DEBUG("Client {} watch {}, db {}", name_, key, dbno);
  return watch_keys_[dbno].insert(key).second;
}

bool PClient::NotifyDirty(int dbno, const std::string& key) {
  if (IsFlagOn(kClientFlagDirty)) {
    INFO("client is already dirty {}", GetUniqueID());
    return true;
  }
  if (watch_keys_[dbno].contains(key)) {
    INFO("{} client become dirty because key {} in db {}", GetUniqueID(), key, dbno);
    SetFlag(kClientFlagDirty);
    return true;
  } else {
    INFO("Dirty key is not exist: {}, because client unwatch before dirty", key);
  }

  return false;
}

bool PClient::Exec() {
  DEFER {
    this->ClearMulti();
    this->ClearWatch();
  };

  if (IsFlagOn(kClientFlagWrongExec)) {
    return false;
  }

  if (IsFlagOn(kClientFlagDirty)) {
    std::string message_ = "$-1\r\n";
    resp_encode_->Reply(message_);
    return true;
  }
  resp_encode_->ClearReply();
  AppendArrayLen(queue_cmds_.size());

  auto client = shared_from_this();
  for (auto& cmd : queue_cmds_) {
    SetCmdName(kstd::StringToLower(cmd[0]));
    SetArgv(cmd);
    kstd::StringToLower(client->cmdName_);
    auto [cmdPtr, ret] = cmd_table_manager_.GetCommand(client->CmdName(), client.get());

    auto cmdstat_map = GetCommandStatMap();
    CommandStatistics statistics;
    if (cmdstat_map->find(cmd[0]) == cmdstat_map->end()) {
      cmdstat_map->emplace(cmd[0], statistics);
    }
    auto now = std::chrono::steady_clock::now();
    GetTimeStat()->SetDequeueTs(now);
    cmdPtr->Execute(client.get());

    // Info Commandstats used
    now = std::chrono::steady_clock::now();
    GetTimeStat()->SetProcessDoneTs(now);
    (*cmdstat_map)[cmd[0]].cmd_count_.fetch_add(1);
    (*cmdstat_map)[cmd[0]].cmd_time_consuming_.fetch_add(GetTimeStat()->GetTotalTime());

    FeedMonitors(cmd);
  }

  g_kiwi->PushWriteTask(client);
  // Propagate(client->params_, GetCurrentDB());
  return true;
}

void PClient::ClearMulti() {
  queue_cmds_.clear();
  ClearFlag(kClientFlagMulti);
  ClearFlag(kClientFlagWrongExec);
}

void PClient::ClearWatch() {
  watch_keys_.clear();
  ClearFlag(kClientFlagDirty);
}

bool PClient::WaitFor(const std::string& key, const std::string* target) {
  bool succ = waiting_keys_.insert(key).second;

  if (succ && target) {
    if (!target_.empty()) {
      ERROR("Wait failed for key {}, because old target {}", key, target_);
      waiting_keys_.erase(key);
      return false;
    }

    target_ = *target;
  }

  return succ;
}

void PClient::SetSlaveInfo() { slave_info_ = std::make_unique<PSlaveInfo>(); }

void PClient::TransferToSlaveThreads() {
  // transfer to slave
  //  auto tcp_connection = getTcpConnection();
  //  if (!tcp_connection) {
  //    return;
  //  }

  //  auto loop = tcp_connection->GetEventLoop();
  //  auto loop_name = loop->GetName();
  //  if (loop_name.find("slave") == std::string::npos) {
  //    auto slave_loop = tcp_connection->SelectSlaveEventLoop();
  //    auto id = tcp_connection->GetUniqueId();
  //    auto event_object = loop->GetEventObject(id);
  //    auto del_conn = [loop, slave_loop, event_object]() {
  //      loop->Unregister(event_object);
  //      event_object->SetUniqueId(-1);
  //      auto tcp_connection = std::dynamic_pointer_cast<TcpConnection>(event_object);
  //      assert(tcp_connection);
  //      tcp_connection->ResetEventLoop(slave_loop);
  //
  //      auto add_conn = [slave_loop, event_object]() { slave_loop->Register(event_object, 0); };
  //      slave_loop->Execute(std::move(add_conn));
  //    };
  //    loop->Execute(std::move(del_conn));
  //  }
}

void PClient::AddToMonitor() {
  std::unique_lock<std::mutex> guard(monitors_mutex);
  monitors.insert(weak_from_this());
}

void PClient::FeedMonitors(const std::vector<std::string>& params) {
  assert(!params.empty());

  {
    std::unique_lock<std::mutex> guard(monitors_mutex);
    if (monitors.empty()) {
      return;
    }
  }

  fmt::memory_buffer buf;
  fmt::format_to(std::back_inserter(buf), "+[db{} {}:{}]: \"", s_current->GetCurrentDB(), s_current->PeerIP(),
                 s_current->PeerPort());

  for (const auto& e : params) {
    fmt::format_to(std::back_inserter(buf), "{} ", e);
  }

  // remove the last space
  if (!params.empty() && buf.size() > 0) {
    buf.resize(buf.size() - 1);
  }

  {
    std::unique_lock<std::mutex> guard(monitors_mutex);

    for (auto it = monitors.begin(); it != monitors.end();) {
      auto m = it->lock();
      if (m) {
        fmt::format_to(std::back_inserter(buf), "\"\r\n");
        m->SendPacket(fmt::to_string(buf));
        ++it;
      } else {
        it = monitors.erase(it);
      }
    }
  }
}

void PClient::SetKey(std::vector<std::string>& names) {
  keys_ = std::move(names);  // use std::move clear copy expense
}

std::unordered_map<std::string, CommandStatistics>* PClient::GetCommandStatMap() { return &cmdstat_map_; }

std::shared_ptr<TimeStat> PClient::GetTimeStat() { return time_stat_; }

}  // namespace kiwi
