/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "raft.pb.h"

namespace kiwi {

class Raft;

class DummyServiceImpl : public DummyService {
 public:
  explicit DummyServiceImpl(Raft* raft) : raft_(raft) {}
  void DummyMethod(::google::protobuf::RpcController* controller, const ::kiwi::DummyRequest* request,
                   ::kiwi::DummyResponse* response, ::google::protobuf::Closure* done) override {}

 private:
  Raft* raft_ = nullptr;
};

}  // namespace kiwi
