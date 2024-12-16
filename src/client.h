// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

/*
  Defined a set of feedback codes, messages, and functions
  related to handling client commands.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <set>
#include <span>
#include <unordered_map>
#include <unordered_set>

#include "net/socket_addr.h"
#include "replication.h"
#include "resp/resp2_encode.h"
#include "resp/resp2_parse.h"
#include "storage/storage.h"

namespace kiwi {

struct CommandStatistics {
  CommandStatistics() = default;
  CommandStatistics(const CommandStatistics& other)
      : cmd_count_(other.cmd_count_.load()), cmd_time_consuming_(other.cmd_time_consuming_.load()) {}

  std::atomic<uint64_t> cmd_count_ = 0;
  std::atomic<uint64_t> cmd_time_consuming_ = 0;
};

struct TimeStat {
  using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

  TimeStat() = default;

  void Reset() {
    enqueue_ts_ = TimePoint::min();
    dequeue_ts_ = TimePoint::min();
    process_done_ts_ = TimePoint::min();
  }

  uint64_t GetTotalTime() const {
    return (process_done_ts_ > enqueue_ts_)
               ? std::chrono::duration_cast<std::chrono::milliseconds>(process_done_ts_ - enqueue_ts_).count()
               : 0;
  }

  void SetEnqueueTs(TimePoint now_time) { enqueue_ts_ = now_time; }
  void SetDequeueTs(TimePoint now_time) { dequeue_ts_ = now_time; }
  void SetProcessDoneTs(TimePoint now_time) { process_done_ts_ = now_time; }

  TimePoint enqueue_ts_ = TimePoint::min();
  TimePoint dequeue_ts_ = TimePoint::min();
  TimePoint process_done_ts_ = TimePoint::min();
};

enum ClientFlag : std::int8_t {
  kClientFlagMulti = (1 << 0),
  kClientFlagDirty = (1 << 1),
  kClientFlagWrongExec = (1 << 2),
  kClientFlagMaster = (1 << 3),
};

enum class ClientState : std::int8_t {
  kOK,
  kClosed,
};

class DB;
struct PSlaveInfo;

struct ClientInfo {
  uint64_t client_id;
  std::string ip;
  int port;
  static const ClientInfo invalidClientInfo;
  bool operator==(const ClientInfo& ci) const { return client_id == ci.client_id; }
};

class PClient : public std::enable_shared_from_this<PClient> {
 public:
  explicit PClient();

  void OnConnect();
  int HandlePacket(std::string&& data);

  ClientInfo GetClientInfo() const;

  //  bool SendPacket(const std::string& buf);
  //  bool SendPacket(const void* data, size_t size);
  bool SendPacket();
  bool SendPacket(std::string&& msg);
  bool SendPacket(UnboundedBuffer& data);
  void SendOver() { reset(); }

  // active close
  void Close();

  // on close callback
  void OnClose();

  std::string PeerIP() const;
  int PeerPort() const;

  // dbno
  void SetCurrentDB(int dbno) { dbno_ = dbno; }

  int GetCurrentDB() { return dbno_; }

  static PClient* Current();

  // multi
  void SetFlag(uint32_t flag) { flag_ |= flag; }
  void ClearFlag(uint32_t flag) { flag_ &= ~flag; }
  bool IsFlagOn(uint32_t flag) { return flag_ & flag; }
  void FlagExecWrong() {
    if (IsFlagOn(kClientFlagMulti)) {
      SetFlag(kClientFlagWrongExec);
    }
  }

  bool Watch(int dbno, const std::string& key);
  bool NotifyDirty(int dbno, const std::string& key);
  bool Exec();
  void ClearMulti();
  void ClearWatch();

  // reply
  void SetRes(CmdRes _ret, const std::string& content = "") { resp_encode_->SetRes(_ret, content); };
  void AppendArrayLen(int64_t ori) { resp_encode_->AppendArrayLen(ori); }
  void AppendArrayLen(uint64_t ori) { resp_encode_->AppendArrayLen(static_cast<int64_t>(ori)); }
  void AppendInteger(int64_t value) { resp_encode_->AppendInteger(value); }
  void AppendStringRaw(const std::string& value) { resp_encode_->AppendStringRaw(value); }
  void AppendSimpleString(const std::string& value) { resp_encode_->AppendSimpleString(value); }
  void AppendString(const std::string& value) { resp_encode_->AppendString(value); }
  void AppendStringVector(const std::vector<std::string>& strArray) {
    resp_encode_->AppendStringVector(strArray);
  };
  void AppendString(const char* value, int64_t size) { resp_encode_->AppendString(value, size); }
  void SetLineString(const std::string& value) { resp_encode_->SetLineString(value); }
  void Reply(std::string& str) { resp_encode_->Reply(str); }
  // reply

  // pubsub
  std::size_t Subscribe(const std::string& channel) { return channels_.insert(channel).second ? 1 : 0; }

  std::size_t UnSubscribe(const std::string& channel) { return channels_.erase(channel); }

  std::size_t PSubscribe(const std::string& channel) { return pattern_channels_.insert(channel).second ? 1 : 0; }

  std::size_t PUnSubscribe(const std::string& channel) { return pattern_channels_.erase(channel); }

  const std::unordered_set<std::string>& GetChannels() const { return channels_; }
  const std::unordered_set<std::string>& GetPatternChannels() const { return pattern_channels_; }
  std::size_t ChannelCount() const { return channels_.size(); }
  std::size_t PatternChannelCount() const { return pattern_channels_.size(); }

  bool WaitFor(const std::string& key, const std::string* target = nullptr);

  std::unordered_set<std::string> WaitingKeys() const { return waiting_keys_; }
  void ClearWaitingKeys() { waiting_keys_.clear(), target_.clear(); }
  const std::string& GetTarget() const { return target_; }

  void SetName(const std::string& name) { name_ = name; }
  const std::string& GetName() const { return name_; }
  void SetCmdName(const std::string& name) { cmdName_ = name; }
  const std::string& CmdName() const { return cmdName_; }
  void SetSubCmdName(const std::string& name);
  const std::string& SubCmdName() const { return subCmdName_; }
  std::string FullCmdName() const;  // the full name of the command, such as config set|get|rewrite
  void SetKey(const std::string& name) {
    keys_.clear();
    keys_.emplace_back(name);
  }
  void SetKey(std::vector<std::string>& names);
  const std::string& Key() const { return keys_.at(0); }
  const std::vector<std::string>& Keys() const { return keys_; }
  std::vector<storage::FieldValue>& Fvs() { return fvs_; }
  void ClearFvs() { fvs_.clear(); }
  std::vector<std::string>& Fields() { return fields_; }
  void ClearFields() { fields_.clear(); }

  void SetSlaveInfo();
  PSlaveInfo* GetSlaveInfo() const { return slave_info_.get(); }
  void TransferToSlaveThreads();
  void AddToMonitor();

  static void FeedMonitors(const std::vector<std::string>& params);

  void SetAuth() { auth_ = true; }
  bool GetAuth() const { return auth_; }
  uint64_t GetUniqueID() const;

  size_t ParamsSize() const { return argv_.size(); }

  ClientState State() const { return state_; }

  void SetState(ClientState state) { state_ = state; }

  void SetConnId(uint64_t id) { net_id_ = id; }
  uint64_t GetConnId() const { return net_id_; }
  void SetThreadIndex(int8_t index) { net_thread_index_ = index; }
  int8_t GetThreadIndex() const { return net_thread_index_; }
  void SetSocketAddr(const net::SocketAddr& addr) { addr_ = addr; }

  void SetArgv(std::vector<std::string>& argv) { argv_ = argv; }

  // Info Commandstats used
  std::unordered_map<std::string, CommandStatistics>* GetCommandStatMap();
  std::shared_ptr<TimeStat> GetTimeStat();

 private:
  void reset();
  bool isPeerMaster() const;
  bool isClusterCmdTarget() const;

 public:
  // All parameters of this command (including the command itself)
  // e.g：["set","key","value"]
  std::span<std::string> argv_;

 private:
  int dbno_ = 0;
  std::unique_ptr<RespParse> resp_parser_;
  std::unique_ptr<RespEncode> resp_encode_;

  std::unordered_set<std::string> channels_;
  std::unordered_set<std::string> pattern_channels_;

  uint32_t flag_ = 0;
  std::unordered_map<int32_t, std::unordered_set<std::string> > watch_keys_;
  std::vector<std::vector<std::string> > queue_cmds_;

  // blocked list
  std::unordered_set<std::string> waiting_keys_;
  std::string target_;

  // slave info from master view
  std::unique_ptr<PSlaveInfo> slave_info_;

  // name
  std::string name_;        // client name
  std::string subCmdName_;  // suchAs config set|get|rewrite
  std::string cmdName_;     // suchAs config
  std::vector<std::string> keys_;
  std::vector<storage::FieldValue> fvs_;
  std::vector<std::string> fields_;

  // auth
  bool auth_ = false;
  time_t last_auth_ = 0;

  ClientState state_;

  uint64_t net_id_ = 0;
  int8_t net_thread_index_ = 0;
  net::SocketAddr addr_;

  static thread_local PClient* s_current;

  /*
   * Info Commandstats used
   */
  std::unordered_map<std::string, CommandStatistics> cmdstat_map_;
  std::shared_ptr<TimeStat> time_stat_;
};
}  // namespace kiwi
