/*
 * Copyright (c) 2024-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "pd_service.h"

#include "pd_server.h"

namespace pikiwidb {
void PlacementDriverServiceImpl::CreateAllRegions(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                                                  const ::pikiwidb::CreateAllRegionsRequest* request,
                                                  ::pikiwidb::CreateAllRegionsResponse* response,
                                                  ::google::protobuf::Closure* done) {}

void PlacementDriverServiceImpl::DeleteAllRegions(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                                                  const ::pikiwidb::DeleteAllRegionsRequest* request,
                                                  ::pikiwidb::DeleteAllRegionsResponse* response,
                                                  ::google::protobuf::Closure* done) {}

void PlacementDriverServiceImpl::AddStore(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                                          const ::pikiwidb::AddStoreRequest* request,
                                          ::pikiwidb::AddStoreResponse* response, ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto& pd_server = PlacementDriverServer::Instance();
  auto [success, store_id] = pd_server.AddStore(request->ip(), request->port());
  if (!success) {
    response->set_success(false);
    return;
  }

  response->set_success(true);
  response->set_store_id(store_id);
}

void PlacementDriverServiceImpl::RemoveStore(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                                             const ::pikiwidb::RemoveStoreRequest* request,
                                             ::pikiwidb::RemoveStoreResponse* response,
                                             ::google::protobuf::Closure* done) {}

void PlacementDriverServiceImpl::GetClusterInfo(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                                                const ::pikiwidb::GetClusterInfoRequest* request,
                                                ::pikiwidb::GetClusterInfoResponse* response,
                                                ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto& pd_server = PlacementDriverServer::Instance();
  pd_server.GetClusterInfo(response);
}

void PlacementDriverServiceImpl::OpenPDScheduling(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                                                  const ::pikiwidb::OpenPDSchedulingRequest* request,
                                                  ::pikiwidb::OpenPDSchedulingResponse* response,
                                                  ::google::protobuf::Closure* done) {}

void PlacementDriverServiceImpl::ClosePDScheduling(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                                                   const ::pikiwidb::ClosePDSchedulingRequest* request,
                                                   ::pikiwidb::ClosePDSchedulingResponse* response,
                                                   ::google::protobuf::Closure* done) {}
}  // namespace pikiwidb