/*
 * Copyright (c) 2024-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

//
//  snapshot.cc

#include "snapshot.h"

#include "braft/local_file_meta.pb.h"
#include "braft/snapshot.h"
#include "butil/files/file_path.h"

#include "std/log.h"
#include "std/std_string.h"

#include "config.h"
#include "raft.h"
#include "store.h"

namespace kiwi {

braft::FileAdaptor* PosixFileSystemAdaptor::open(const std::string& path, int oflag,
                                                 const ::google::protobuf::Message* file_meta, butil::File::Error* e) {
  if ((oflag & IS_RDONLY) == 0) {  // This is a read operation
    bool snapshots_exists = false;
    std::string snapshot_path;
    int db_id = -1;

    // parse snapshot path
    butil::FilePath parse_snapshot_path(path);
    std::vector<std::string> components;
    bool is_find_db = false;
    parse_snapshot_path.GetComponents(&components);
    for (const auto& component : components) {
      snapshot_path += component + "/";

      if (is_find_db && kstd::String2int(component, &db_id)) {
        is_find_db = false;
      }

      if (component.find("snapshot_") != std::string::npos) {
        break;
      } else if (component == "db") {
        is_find_db = true;
      }
    }

    // check whether snapshots have been created
    std::lock_guard<braft::raft_mutex_t> guard(mutex_);
    if (!snapshot_path.empty()) {
      for (const auto& entry : std::filesystem::directory_iterator(snapshot_path)) {
        std::string filename = entry.path().filename().string();
        if (entry.is_regular_file() || entry.is_directory()) {
          if (filename != "." && filename != ".." && filename.find(RAFT_INST_SNAPSHOT_META_FILE) == std::string::npos) {
            // If the path directory contains files other than raft_snapshot_meta, snapshots have been generated
            snapshots_exists = true;
            break;
          }
        }
      }
    }

    // Snapshot generation
    if (!snapshots_exists) {
      assert(db_id >= 0);

      braft::LocalSnapshotMetaTable snapshot_meta_memtable;
      std::string meta_path = snapshot_path + "/" RAFT_INST_SNAPSHOT_META_FILE;
      INFO("start to generate snapshot in path {}", snapshot_path);
      braft::FileSystemAdaptor* fs = braft::default_file_system();
      assert(fs);
      snapshot_meta_memtable.load_from_file(fs, meta_path);

      TasksVector tasks(1, {TaskType::kCheckpoint, 0, {{TaskArg::kCheckpointPath, snapshot_path}}, true});
      STORE_INST.HandleTaskSpecificDB(tasks);
      AddAllFiles(snapshot_path, &snapshot_meta_memtable, snapshot_path);

      // update snapshot last log index and last_log_term
      auto& new_meta = const_cast<braft::SnapshotMeta&>(snapshot_meta_memtable.meta());
      auto last_log_index = STORE_INST.GetBackend(db_id)->GetStorage()->GetSmallestFlushedLogIndex();
      new_meta.set_last_included_index(last_log_index);
      auto last_log_term = RAFT_INST.GetTerm(last_log_index);
      new_meta.set_last_included_term(last_log_term);
      INFO("Succeed to fix db_{} snapshot meta: {}, {}", db_id, last_log_index, last_log_term);

      auto rc = snapshot_meta_memtable.save_to_file(fs, meta_path);
      if (rc == 0) {
        INFO("Succeed to save snapshot in path {}", snapshot_path);
      } else {
        ERROR("Fail to save snapshot in path {}", snapshot_path);
      }
      INFO("generate snapshot completed in path {}", snapshot_path);
    }
  }

  return braft::PosixFileSystemAdaptor::open(path, oflag, file_meta, e);
}

void PosixFileSystemAdaptor::AddAllFiles(const std::filesystem::path& dir,
                                         braft::LocalSnapshotMetaTable* snapshot_meta_memtable,
                                         const std::string& path) {
  assert(snapshot_meta_memtable);
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.is_directory()) {
      if (entry.path() != "." && entry.path() != "..") {
        INFO("dir_path = {}", entry.path().string());
        AddAllFiles(entry.path(), snapshot_meta_memtable, path);
      }
    } else {
      INFO("file_path = {}", std::filesystem::relative(entry.path(), path).string());
      braft::LocalFileMeta meta;
      if (snapshot_meta_memtable->add_file(std::filesystem::relative(entry.path(), path), meta) != 0) {
        WARN("Failed to add file");
      }
    }
  }
}

}  // namespace kiwi
