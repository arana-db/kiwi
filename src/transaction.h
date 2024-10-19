/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "base_cmd.h"
#include "common.h"

namespace pikiwidb {

class PClient;
class PTransaction {
 public:
  static PTransaction &Instance();

  PTransaction(const PTransaction &) = delete;
  void operator=(const PTransaction &) = delete;

  void Watch(PClient *client, int dbno, const PString &key);
  bool Multi(PClient *client);
  bool Exec(PClient *client);
  void Discard(PClient *client);

  void NotifyDirty(int dbno, const PString &key);
  void NotifyDirtyAll(int dbno);

 private:
  PTransaction() = default;

  using Clients = std::vector<std::weak_ptr<PClient>>;
  using WatchedClients = std::map<int, std::map<PString, Clients>>;

  WatchedClients clients_;
};

class MultiCmd : public BaseCmd {
 public:
  MultiCmd(const std::string &name, int16_t arity);

 protected:
  bool DoInitial(PClient *client) override;

 private:
  void DoCmd(PClient *client) override;
};

class WatchCmd : public BaseCmd {
 public:
  WatchCmd(const std::string &name, int16_t arity);

 protected:
  bool DoInitial(PClient *client) override;

 private:
  void DoCmd(PClient *client) override;
};

class UnWatchCmd : public BaseCmd {
 public:
  UnWatchCmd(const std::string &name, int16_t arity);

 protected:
  bool DoInitial(PClient *client) override;

 private:
  void DoCmd(PClient *client) override;
};

class ExecCmd : public BaseCmd {
 public:
  ExecCmd(const std::string &name, int16_t arity);

 protected:
  bool DoInitial(PClient *client) override;

 private:
  void DoCmd(PClient *client) override;
};

class DiscardCmd : public BaseCmd {
 public:
  DiscardCmd(const std::string &name, int16_t arity);

 protected:
  bool DoInitial(PClient *client) override;

 private:
  void DoCmd(PClient *client) override;
};

}  // namespace pikiwidb
