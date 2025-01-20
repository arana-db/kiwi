// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

/*
  Implemented a set of functions and instructions for
  coordinating external commands and interfacing with RocksDB.
 */

#include "store.h"

#include <memory>
#include <string>

#include "config.h"
#include "db.h"
#include "std/log.h"
#include "std/std_string.h"
#include "transaction.h"

namespace kiwi {

Store::~Store() { INFO("STORE is closing..."); }

Store& Store::Instance() {
  static Store store;
  return store;
}

void Store::Init(int db_number) {
  db_number_ = db_number;
  backends_.reserve(db_number_);
  for (int i = 0; i < db_number_; i++) {
    auto db = std::make_unique<DB>(i, g_config.db_path);
    db->Open();
    backends_.push_back(std::move(db));
    INFO("Open DB_{} success!", i);
  }
  INFO("STORE Init success!");
}

void Store::HandleTaskSpecificDB(const TasksVector& tasks) {
  std::for_each(tasks.begin(), tasks.end(), [this](const auto& task) {
    if (task.db < 0 || task.db >= db_number_) {
      WARN("The database index is out of range.");
      return;
    }
    auto& db = backends_.at(task.db);
    switch (task.type) {
      case kCheckpoint: {
        if (auto s = task.args.find(kCheckpointPath); s == task.args.end()) {
          WARN("The critical parameter 'path' is missing for do a checkpoint.");
          return;
        }
        auto path = task.args.find(kCheckpointPath)->second;
        kstd::TrimSlash(path);
        db->CreateCheckpoint(path, task.sync);
        break;
      }
      case kLoadDBFromCheckpoint: {
        if (auto s = task.args.find(kCheckpointPath); s == task.args.end()) {
          WARN("The critical parameter 'path' is missing for load a checkpoint.");
          return;
        }
        auto path = task.args.find(kCheckpointPath)->second;
        kstd::TrimSlash(path);
        db->LoadDBFromCheckpoint(path, task.sync);
        break;
      }
      case kEmpty: {
        WARN("A empty task was passed in, not doing anything.");
        break;
      }
      default:
        break;
    }
  });
}

void Propagate(const std::vector<PString>& params, int dbno) {
  assert(!params.empty());
  //
  //  if (!g_dirtyKeys.empty()) {
  //    for (const auto& k : g_dirtyKeys) {
  //      PTransaction::Instance().NotifyDirty(PSTORE.GetDBNumber(), k);
  //
  //    }
  //    g_dirtyKeys.clear();
  //  } else if (params.size() > 1) {
  //    PTransaction::Instance().NotifyDirty(PSTORE.GetDBNumber(), params[1]);
  //  }
  if (params.size() > 1) {
    PTransaction::Instance().NotifyDirty(dbno, params[1]);
  }
}

void Propagate(int dbno, const std::vector<PString>& params) {
  PTransaction::Instance().NotifyDirtyAll(dbno);
  Propagate(params, dbno);
}

void signalModifiedKey(const std::vector<PString>& keys, int dbno) {
  if (keys.size() > 1) {
    for (const auto& key : keys) {
      PTransaction::Instance().NotifyDirty(dbno, key);
    }
  }
}
}  // namespace kiwi
