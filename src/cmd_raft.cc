/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_raft.h"

#include <cstdint>
#include <optional>
#include <string>
#include <fmt/format.h>

#include "praft/praft.h"
#include "pstd/log.h"
#include "pstd/pstd_string.h"
#include "brpc/channel.h"
#include "praft.pb.h"

#include "client.h"
#include "config.h"
#include "pikiwidb.h"
#include "replication.h"
#include "store.h"

namespace pikiwidb {

RaftNodeCmd::RaftNodeCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsRaft, kAclCategoryRaft) {}

bool RaftNodeCmd::DoInitial(PClient* client) {
  auto cmd = client->argv_[1];
  pstd::StringToUpper(cmd);

  if (cmd != kAddCmd && cmd != kRemoveCmd && cmd != kDoSnapshot) {
    client->SetRes(CmdRes::kErrOther, "RAFT.NODE supports ADD / REMOVE / DOSNAPSHOT only");
    return false;
  }
  group_id_ = client->argv_[2];
  praft_ = PSTORE.GetBackend(client->GetCurrentDB())->GetPRaft();
  return true;
}

void RaftNodeCmd::DoCmd(PClient* client) {
  assert(praft_);
  auto cmd = client->argv_[1];
  pstd::StringToUpper(cmd);
  if (cmd == kAddCmd) {
    DoCmdAdd(client);
  } else if (cmd == kRemoveCmd) {
    DoCmdRemove(client);
  } else if (cmd == kDoSnapshot) {
    assert(0);  // TODO(longfar): add group id in arguments
    DoCmdSnapshot(client);
  } else {
    client->SetRes(CmdRes::kErrOther, "RAFT.NODE supports ADD / REMOVE / DOSNAPSHOT only");
  }
}

void RaftNodeCmd::DoCmdAdd(PClient* client) {
  DEBUG("Received RAFT.NODE ADD cmd from {}", client->PeerIP());
  auto db = PSTORE.GetDBByGroupID(group_id_);
  assert(db);
  auto praft = db->GetPRaft();
  // Check whether it is a leader. If it is not a leader, return the leader information
  if (!praft->IsLeader()) {
    client->SetRes(CmdRes::kWrongLeader, praft_->GetLeaderID());
    return;
  }

  if (client->argv_.size() != 4) {
    client->SetRes(CmdRes::kWrongNum, client->CmdName());
    return;
  }

  // RedisRaft has nodeid, but in Braft, NodeId is IP:Port.
  // So we do not need to parse and use nodeid like redis;
  auto s = praft->AddPeer(client->argv_[3]);
  if (s.ok()) {
    client->SetRes(CmdRes::kOK);
  } else {
    client->SetRes(CmdRes::kErrOther, fmt::format("Failed to add peer: {}", s.error_str()));
  }
}

void RaftNodeCmd::DoCmdRemove(PClient* client) {
  // If the node has been initialized, it needs to close the previous initialization and rejoin the other group
  if (!praft_->IsInitialized()) {
    client->SetRes(CmdRes::kErrOther, "Don't already cluster member");
    return;
  }

  if (client->argv_.size() != 3) {
    client->SetRes(CmdRes::kWrongNum, client->CmdName());
    return;
  }

  // Check whether it is a leader. If it is not a leader, send remove request to leader
  if (!praft_->IsLeader()) {
    // Get the leader information
    braft::PeerId leader_peer_id(praft_->GetLeaderID());
    // @todo There will be an unreasonable address, need to consider how to deal with it
    if (leader_peer_id.is_empty()) {
      client->SetRes(CmdRes::kErrOther,
                     "The leader address of the cluster is incorrect, try again or delete the node from another node");
      return;
    }

    // Connect target
    std::string peer_ip = butil::ip2str(leader_peer_id.addr.ip).c_str();
    auto port = leader_peer_id.addr.port - pikiwidb::g_config.raft_port_offset;
    auto peer_id = client->argv_[2];
    auto ret =
        praft_->GetClusterCmdCtx().Set(ClusterCmdType::kRemove, client, std::move(peer_ip), port, std::move(peer_id));
    if (!ret) {  // other clients have removed
      return client->SetRes(CmdRes::kErrOther, "Other clients have removed");
    }
    praft_->GetClusterCmdCtx().ConnectTargetNode();
    INFO("Sent remove request to leader successfully");

    // Not reply any message here, we will reply after the connection is established.
    client->Clear();
    return;
  }

  auto s = praft_->RemovePeer(client->argv_[2]);
  if (s.ok()) {
    client->SetRes(CmdRes::kOK);
  } else {
    client->SetRes(CmdRes::kErrOther, fmt::format("Failed to remove peer: {}", s.error_str()));
  }
}

void RaftNodeCmd::DoCmdSnapshot(PClient* client) {
  auto s = praft_->DoSnapshot();
  if (s.ok()) {
    client->SetRes(CmdRes::kOK);
  }
}

RaftClusterCmd::RaftClusterCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsRaft, kAclCategoryRaft) {}

bool RaftClusterCmd::DoInitial(PClient* client) {
  auto cmd = client->argv_[1];
  pstd::StringToUpper(cmd);
  if (cmd != kInitCmd && cmd != kJoinCmd) {
    client->SetRes(CmdRes::kErrOther, "RAFT.CLUSTER supports INIT/JOIN only");
    return false;
  }
  praft_ = PSTORE.GetBackend(client->GetCurrentDB())->GetPRaft();
  return true;
}

void RaftClusterCmd::DoCmd(PClient* client) {
  assert(praft_);
  if (praft_->IsInitialized()) {
    return client->SetRes(CmdRes::kErrOther, "Already cluster member");
  }

  auto cmd = client->argv_[1];
  pstd::StringToUpper(cmd);
  if (cmd == kInitCmd) {
    DoCmdInit(client);
  } else {
    DoCmdJoin(client);
  }
}

void RaftClusterCmd::DoCmdInit(PClient* client) {
  if (client->argv_.size() != 2 && client->argv_.size() != 3) {
    return client->SetRes(CmdRes::kWrongNum, client->CmdName());
  }

  std::string group_id;
  if (client->argv_.size() == 3) {
    group_id = client->argv_[2];
    if (group_id.size() != RAFT_GROUPID_LEN) {
      return client->SetRes(CmdRes::kInvalidParameter,
                            "Cluster id must be " + std::to_string(RAFT_GROUPID_LEN) + " characters");
    }
  } else {
    group_id = pstd::RandomHexChars(RAFT_GROUPID_LEN);
  }
  PSTORE.AddRegion(group_id, client->GetCurrentDB());
  auto s = praft_->Init(group_id, false);
  if (!s.ok()) {
    return client->SetRes(CmdRes::kErrOther, fmt::format("Failed to init node: {}", s.error_str()));
  }
  client->SetLineString(fmt::format("+OK {}", group_id));
}

static inline std::optional<std::pair<std::string, int32_t>> GetIpAndPortFromEndPoint(const std::string& endpoint) {
  auto pos = endpoint.find(':');
  if (pos == std::string::npos) {
    return std::nullopt;
  }

  int32_t ret = 0;
  pstd::String2int(endpoint.substr(pos + 1), &ret);
  return {{endpoint.substr(0, pos), ret}};
}

void RaftClusterCmd::DoCmdJoin(PClient* client) {
  assert(client->argv_.size() == 4);
  auto group_id = client->argv_[2];
  auto addr = client->argv_[3];

  if (group_id.size() != RAFT_GROUPID_LEN) {
    return client->SetRes(CmdRes::kInvalidParameter,
                            "Cluster id must be " + std::to_string(RAFT_GROUPID_LEN) + " characters");
  }
  auto ip_port = GetIpAndPortFromEndPoint(addr);
  if (!ip_port.has_value()) {
    return client->SetRes(CmdRes::kErrOther, fmt::format("Invalid ip::port: {}", addr));
  }
  auto& [ip, port] = *ip_port;
  PSTORE.AddRegion(group_id, client->GetCurrentDB());
  auto s = praft_->Init(group_id, true);
  assert(s.ok());

  brpc::ChannelOptions options;
  options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
  options.max_retry = 0;
  options.connect_timeout_ms = 200;

  brpc::Channel add_node_channel;
  if (0 != add_node_channel.Init(addr.c_str(), &options)) {
    ERROR("Fail to init add_node_channel to praft service!");
    // 失败的情况下，应该取消 Init。
    // 并且 Remove Region。
    client->SetRes(CmdRes::kErrOther, "Fail to init channel.");
    return;
  }

  brpc::Controller cntl;
  NodeAddRequest request;
  NodeAddResponse response;
  auto end_point = fmt::format("{}:{}", ip, std::to_string(port));
  request.set_groupid(group_id);
  request.set_endpoint(end_point);
  request.set_index(client->GetCurrentDB());
  request.set_role(0); // 0 : !witness
  PRaftService_Stub stub(&add_node_channel);
  stub.AddNode(&cntl, &request, &response, NULL);
  
  if (cntl.Failed()) {
    client->SetRes(CmdRes::kErrOther, "Failed to send add node rpc");
    return;
  }
  if (response.success()) {
    client->SetRes(CmdRes::kOK, "Add Node Success");
    return;
  }
  // 这里需要删除 Region。并且取消 初始化 。
  client->SetRes(CmdRes::kErrOther, "Failed to Add Node");  

  // Not reply any message here, we will reply after the connection is established.
  // client->Clear();
}

}  // namespace pikiwidb
