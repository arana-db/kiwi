// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::raft_proto::raft_core_service_client::RaftCoreServiceClient;
use conf::raft_type::{KiwiNode, KiwiTypeConfig};
use openraft::error::{NetworkError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use std::io;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tonic::Request as TonicRequest;
use tonic::transport::Channel;

// 类型别名，简化 RaftNetwork 的返回类型
type NodeId = <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId;
type Node = KiwiNode;
type RPCErr = openraft::error::RPCError<NodeId, Node, RaftError<NodeId>>;
type RPCErrSnapshot = openraft::error::RPCError<
    NodeId,
    Node,
    RaftError<NodeId, openraft::error::InstallSnapshotError>,
>;

pub struct KiwiNetworkFactory {
    networks: Arc<RwLock<std::collections::HashMap<NodeId, Arc<KiwiNetwork>>>>,
}

impl KiwiNetworkFactory {
    pub fn new() -> Self {
        Self {
            networks: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
}

impl Default for KiwiNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<KiwiTypeConfig> for KiwiNetworkFactory {
    type Network = KiwiNetwork;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        // Get the read lock to check if a client already exists for the target node
        let networks = self.networks.read().await;
        if let Some(network) = networks.get(&target) {
            return KiwiNetwork {
                client: Arc::clone(&network.client),
            };
        }

        // Drop the read lock before acquiring the write lock
        drop(networks);

        // Acquire the write lock to create a new client for the target node
        let mut networks = self.networks.write().await;

        // Double-check if another async task has already created the client while we were waiting for the write lock
        if let Some(network) = networks.get(&target) {
            return KiwiNetwork {
                client: Arc::clone(&network.client),
            };
        }

        // Get or create gRPC client for the target node
        let addr = node.raft_addr.clone();

        // 地址格式错误时直接 panic，而不是静默 fallback 到无效地址
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .unwrap_or_else(|e| {
                panic!(
                    "invalid raft address '{}': failed to parse as gRPC endpoint: {}",
                    addr, e
                )
            });

        let endpoint = endpoint
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .connect_lazy();
        let client = RaftCoreServiceClient::new(endpoint);

        let network = KiwiNetwork {
            client: Arc::new(Mutex::new(client)),
        };

        // 存入缓存（存入 Arc 包装的版本以便共享）
        networks.insert(target, Arc::new(network.clone()));

        // 返回克隆而非 Arc
        network
    }
}

/// KiwiNetwork 实现 RaftNetwork trait，用于与远程 Raft 节点通信。
///
/// 注意：此结构体不能跨线程共享。内部使用 `Arc<Mutex<...>>` 来让 gRPC 客户端
/// 可在被多个 async task 共享的同时保持线程安全，但 Clone 实现是私有的，
/// 防止外部代码克隆并造成并发 mutability 问题。
pub struct KiwiNetwork {
    client: Arc<Mutex<RaftCoreServiceClient<Channel>>>,
}

// 私有 Clone 实现，仅供内部工厂使用
impl Clone for KiwiNetwork {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
        }
    }
}

// Impl the RaftNetwork trait for KiwiNetwork according to openraft requirements
impl RaftNetwork<KiwiTypeConfig> for KiwiNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<KiwiTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCErr> {
        // OpenRaft → Proto
        let proto_req: crate::raft_proto::AppendEntriesRequest = rpc
            .try_into()
            .map_err(|e| {
                RPCErr::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to convert AppendEntriesRequest: {}", e),
                )))
            })?;

        // 调用 gRPC
        let mut client = self.client.lock().await;
        let response = client
            .append_entries(TonicRequest::new(proto_req))
            .await
            .map_err(|e| {
                RPCErr::Network(NetworkError::new(&io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("gRPC error: {}", e),
                )))
            })?;

        let proto_resp = response.into_inner();

        if proto_resp.success {
            Ok(openraft::raft::AppendEntriesResponse::Success)
        } else {
            // TODO: 完善返回错误信息，目前仅返回 Conflict，后续可以根据实际情况返回更详细的错误类型
            Ok(openraft::raft::AppendEntriesResponse::Conflict)
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
        let mut client = self.client.lock().await;
        let response = client
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
