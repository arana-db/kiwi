/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "transaction.h"
#include "client.h"
#include "log.h"

namespace pikiwidb {

PTransaction& PTransaction::Instance() {
  static PTransaction mt;
  return mt;
}

void PTransaction::Watch(PClient* client, int dbno, const PString& key) {
  if (client->Watch(dbno, key)) {
    Clients& cls = clients_[dbno][key];
    cls.push_back(std::static_pointer_cast<PClient>(client->shared_from_this()));
  }
}

bool PTransaction::Multi(PClient* client) {
  if (client->IsFlagOn(kClientFlagMulti)) {
    return false;
  }

  client->ClearMulti();
  client->SetFlag(kClientFlagMulti);
  return true;
}

bool PTransaction::Exec(PClient* client) { return client->Exec(); }

void PTransaction::Discard(PClient* client) {
  client->ClearMulti();
  client->ClearWatch();
}

void PTransaction::NotifyDirty(int dbno, const PString& key) {
  // std::map<int, std::map<PString, Clients>>  clients_;
  // using Clients = std::vector<std::weak_ptr<PClient>>;
  auto tmpDBIter = clients_.find(dbno);
  // 判断 db-id 对应的 clients 是否存在
  if (tmpDBIter == clients_.end()) {
    return;
  }

  // 取出 std::map<PString, Clients> 这个容器
  auto& dbWatchedKeys = tmpDBIter->second;
  auto it = dbWatchedKeys.find(key);
  // 如果没有找到这个 key 对应的 client, 说明这个 key 没有被 watch
  if (it == dbWatchedKeys.end()) {
    return;
  }

  // 取出这个被 watch 的一批连接池
  Clients& cls = it->second;
  for (auto itCli(cls.begin()); itCli != cls.end();) {
    auto client(itCli->lock());
    if (!client) {
      WARN("Erase not exist client when notify dirty key[{}]", key);
      itCli = cls.erase(itCli);
    } else {
      if (client->NotifyDirty(dbno, key)) {
        WARN("Erase dirty client {} when notify dirty key[{}]", client->GetName(), key);
        itCli = cls.erase(itCli);
      } else {
        ++itCli;
      }
    }
  }

  if (cls.empty()) {
    dbWatchedKeys.erase(it);
  }
}

void PTransaction::NotifyDirtyAll(int dbno) {
  if (dbno == -1) {
    for (auto& db_set : clients_) {
      for (auto& key_clients : db_set.second) {
        std::for_each(key_clients.second.begin(), key_clients.second.end(), [&](const std::weak_ptr<PClient>& wcli) {
          auto scli = wcli.lock();
          if (scli) {
            scli->SetFlag(kClientFlagDirty);
          }
        });
      }
    }
  } else {
    auto it = clients_.find(dbno);
    if (it != clients_.end()) {
      for (auto& key_clients : it->second) {
        std::for_each(key_clients.second.begin(), key_clients.second.end(), [&](const std::weak_ptr<PClient>& wcli) {
          auto scli = wcli.lock();
          if (scli) {
            scli->SetFlag(kClientFlagDirty);
          }
        });
      }
    }
  }
}

// multi commands
WatchCmd::WatchCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsReadonly, kAclCategoryRead | kAclCategoryTransaction) {}

bool WatchCmd::DoInitial(PClient* client) {
  client->SetKey(client->argv_[1]);
  return true;
}

void WatchCmd::DoCmd(PClient* client) {
  if (client->IsFlagOn(kClientFlagMulti)) {
    client->SetRes(CmdRes::kPErrorWatch);
    return;
  }

  std::for_each(++client->argv_.begin(), ++client->argv_.end(),
                [client](const PString& s) { PTransaction::Instance().Watch(client, client->GetCurrentDB(), s); });
  client->SetRes(CmdRes::kOK);
}

UnWatchCmd::UnWatchCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsReadonly, kAclCategoryRead | kAclCategoryTransaction) {}

bool UnWatchCmd::DoInitial(PClient* client) {
  client->SetKey(client->argv_[1]);
  return true;
}

void UnWatchCmd::DoCmd(PClient* client) {
  client->ClearWatch();
  client->SetRes(CmdRes::kOK);
}

MultiCmd::MultiCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsReadonly, kAclCategoryRead | kAclCategoryTransaction) {}

bool MultiCmd::DoInitial(PClient* client) {
  client->SetKey(client->argv_[1]);
  return true;
}

void MultiCmd::DoCmd(PClient* client) {
  if (PTransaction::Instance().Multi(client)) {
    client->SetRes(CmdRes::kOK);
  } else {
    client->SetRes(CmdRes::kErrOther, "MULTI calls can not be nested");
  }
}

ExecCmd::ExecCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsReadonly, kAclCategoryRead | kAclCategoryTransaction) {}

bool ExecCmd::DoInitial(PClient* client) {
  client->SetKey(client->argv_[1]);
  return true;
}

void ExecCmd::DoCmd(PClient* client) {
  if (!client->IsFlagOn(kClientFlagMulti)) {
    client->SetRes(CmdRes::kErrOther, "EXEC without MULTI");
    return;
  }
  if (!PTransaction::Instance().Exec(client)) {
    client->SetRes(CmdRes::kDirtyExec);
    return;
  }
}

DiscardCmd::DiscardCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsReadonly, kAclCategoryRead | kAclCategoryTransaction) {}

bool DiscardCmd::DoInitial(PClient* client) {
  client->SetKey(client->argv_[1]);
  return true;
}

void DiscardCmd::DoCmd(PClient* client) {
  if (!client->IsFlagOn(kClientFlagMulti)) {
    client->SetRes(CmdRes::kErrOther, "DISCARD without MULTI");
  } else {
    PTransaction::Instance().Discard(client);
    client->SetRes(CmdRes::kOK);
  }
}

}  // namespace pikiwidb
