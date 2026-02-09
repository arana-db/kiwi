// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use conf::raft_type::{KiwiNode, KiwiTypeConfig};
use openraft::error::{NetworkError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::io;
use crate::raft_proto::raft_core_service_client::RaftCoreServiceClient;
use tonic::transport::Channel;
use tonic::Request as TonicRequest;

// 类型别名，简化 RaftNetwork 的返回类型
type NodeId = <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId;
type Node = KiwiNode;
type RPCErr = openraft::error::RPCError<NodeId, Node, RaftError<NodeId>>;
type RPCErrSnapshot = openraft::error::RPCError<
    NodeId,
    Node,
    RaftError<NodeId, openraft::error::InstallSnapshotError>,
>;

pub struct KiwiNetworkFactory;

impl KiwiNetworkFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for KiwiNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<KiwiTypeConfig> for KiwiNetworkFactory {
    type Network = KiwiNetwork;

    async fn new_client(&mut self, _target: NodeId, node: &Node) -> Self::Network {
        // Get or create gRPC client for the target node
        let addr = node.raft_addr.clone();
        
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .expect("Invalid gRPC endpoint")
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .connect_lazy();
        let client = RaftCoreServiceClient::new(endpoint);

        KiwiNetwork { client }
    }
}

pub struct KiwiNetwork {
    client: RaftCoreServiceClient<Channel>,
}

// Impl the RaftNetwork trait for KiwiNetwork according to openraft requirements
impl RaftNetwork<KiwiTypeConfig> for KiwiNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<KiwiTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCErr> {
        // OpenRaft → Proto
        let proto_req: crate::raft_proto::AppendEntriesRequest = rpc.into();

        // 调用 gRPC
        let response = self
            .client
            .append_entries(TonicRequest::new(proto_req))
            .await
            .map_err(|e| {
                RPCErr::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("gRPC error: {}", e),
                )))
            })?;

        let proto_resp = response.into_inner();

        // Proto → OpenRaft
        if proto_resp.success {
            Ok(openraft::raft::AppendEntriesResponse::Success)
        } else {
            Err(RPCErr::Network(NetworkError::new(&io::Error::other(
                "AppendEntries failed",
            ))))
        }
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<KiwiTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RPCErrSnapshot> {
        Err(RPCErrSnapshot::Network(NetworkError::new(&io::Error::new(
            io::ErrorKind::Unsupported,
            "Install snapshot not implemented",
        ))))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCErr> {
        // OpenRaft → Proto
        let proto_req: crate::raft_proto::VoteRequest = rpc.into();

        // 调用 gRPC
        let response = self
            .client
            .vote(TonicRequest::new(proto_req))
            .await
            .map_err(|e| {
                RPCErr::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("gRPC error: {}", e),
                )))
            })?;

        let proto_resp = response.into_inner();

        // Proto → OpenRaft
        (&proto_resp).try_into().map_err(|e| {
            RPCErr::Network(NetworkError::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to convert vote response: {}", e),
            )))
        })
    }
}
