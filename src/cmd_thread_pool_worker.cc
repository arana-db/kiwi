// Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

/*
  Retrieve commands from the thread pool and execute them.
 */

#include "cmd_thread_pool_worker.h"
#include "client.h"
#include "env.h"
#include "kiwi.h"
#include "log.h"

namespace kiwi {

void CmdWorkThreadPoolWorker::Work() {
  while (running_) {
    LoadWork();
    for (const auto &task : self_task_) {
      if (task->Client()->State() != ClientState::kOK) {  // the client is closed
        continue;
      }
      auto [cmdPtr, ret] = cmd_table_manager_.GetCommand(task->CmdName(), task->Client().get());

      if (!cmdPtr) {
        if (ret == CmdRes::kUnknownCmd) {
          task->Client()->SetRes(CmdRes::kErrOther, "unknown command '" + task->CmdName() + "'");
        } else if (ret == CmdRes::kUnknownSubCmd) {
          task->Client()->SetRes(CmdRes::kErrOther, "unknown sub command '" + task->Client().get()->argv_[1] + "'");
        } else {
          task->Client()->SetRes(CmdRes::kInvalidParameter);
        }
        g_kiwi->PushWriteTask(task->Client());
        continue;
      }

      if (!cmdPtr->CheckArg(task->Client()->ParamsSize())) {
        task->Client()->SetRes(CmdRes::kWrongNum, task->CmdName());
        g_kiwi->PushWriteTask(task->Client());
        continue;
      }

      auto cmdstat_map = task->Client()->GetCommandStatMap();
      CommandStatistics statistics;
      if (cmdstat_map->find(task->CmdName()) == cmdstat_map->end()) {
        cmdstat_map->emplace(task->CmdName(), statistics);
      }
      auto now = std::chrono::steady_clock::now();
      task->Client()->GetTimeStat()->SetDequeueTs(now);
      task->Run(cmdPtr);

      // Info Commandstats used
      now = std::chrono::steady_clock::now();
      task->Client()->GetTimeStat()->SetProcessDoneTs(now);
      (*cmdstat_map)[task->CmdName()].cmd_count_.fetch_add(1);
      (*cmdstat_map)[task->CmdName()].cmd_time_consuming_.fetch_add(task->Client()->GetTimeStat()->GetTotalTime());

      g_kiwi->PushWriteTask(task->Client());
    }
    self_task_.clear();
  }
  INFO("worker [{}] goodbye...", name_);
}

void CmdWorkThreadPoolWorker::Stop() { running_ = false; }

void CmdFastWorker::LoadWork() {
  std::unique_lock lock(pool_->fast_mutex_);
  while (pool_->fast_tasks_.empty()) {
    if (!running_) {
      return;
    }
    pool_->fast_condition_.wait(lock);
  }

  if (pool_->fast_tasks_.empty()) {
    return;
  }
  const auto num = std::min(static_cast<int>(pool_->fast_tasks_.size()), once_task_);
  std::move(pool_->fast_tasks_.begin(), pool_->fast_tasks_.begin() + num, std::back_inserter(self_task_));
  pool_->fast_tasks_.erase(pool_->fast_tasks_.begin(), pool_->fast_tasks_.begin() + num);
}

void CmdSlowWorker::LoadWork() {
  {
    std::unique_lock lock(pool_->slow_mutex_);
    while (pool_->slow_tasks_.empty() && loop_more_) {  // loopMore is used to get the fast worker
      if (!running_) {
        return;
      }
      pool_->slow_condition_.wait_for(lock, std::chrono::milliseconds(wait_time_));
      loop_more_ = false;
    }

    const auto num = std::min(static_cast<int>(pool_->slow_tasks_.size()), once_task_);
    if (num > 0) {
      std::move(pool_->slow_tasks_.begin(), pool_->slow_tasks_.begin() + num, std::back_inserter(self_task_));
      pool_->slow_tasks_.erase(pool_->slow_tasks_.begin(), pool_->slow_tasks_.begin() + num);
      return;  // If the slow task is obtained, the fast task is no longer obtained
    }
  }

  {
    std::unique_lock lock(pool_->fast_mutex_);
    loop_more_ = true;

    const auto num = std::min(static_cast<int>(pool_->fast_tasks_.size()), once_task_);
    if (num > 0) {
      std::move(pool_->fast_tasks_.begin(), pool_->fast_tasks_.begin() + num, std::back_inserter(self_task_));
      pool_->fast_tasks_.erase(pool_->fast_tasks_.begin(), pool_->fast_tasks_.begin() + num);
    }
  }
}

}  // namespace kiwi
