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

//! gRPC 网络层实现
//!
//! 提供 Raft 节点间的 gRPC 通信实现，包括连接缓存和重连机制。

use crate::grpc::GrpcClientError;
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
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tonic::Request as TonicRequest;
use tonic::transport::Channel;

/// 连接配置
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// 连接超时时间
    pub connect_timeout: Duration,
    /// 请求超时时间
    pub request_timeout: Duration,
    /// 最大重试次数
    pub max_retries: u32,
    /// 重试间隔
    pub retry_interval: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_interval: Duration::from_millis(100),
        }
    }
}

// 类型别名，简化 RaftNetwork 的返回类型
type NodeId = <KiwiTypeConfig as openraft::RaftTypeConfig>::NodeId;
type Node = KiwiNode;
type RPCErr = openraft::error::RPCError<NodeId, Node, RaftError<NodeId>>;
type RPCErrSnapshot = openraft::error::RPCError<
    NodeId,
    Node,
    RaftError<NodeId, openraft::error::InstallSnapshotError>,
>;

/// 网络工厂 - 管理所有节点的 gRPC 连接
///
/// 使用连接缓存避免频繁创建连接，支持重连机制。
pub struct KiwiNetworkFactory {
    networks: Arc<RwLock<std::collections::HashMap<NodeId, Arc<KiwiNetwork>>>>,
    /// 连接配置
    config: ConnectionConfig,
}

impl KiwiNetworkFactory {
    /// 创建新的网络工厂
    pub fn new() -> Self {
        Self::with_config(ConnectionConfig::default())
    }

    /// 使用自定义配置创建网络工厂
    pub fn with_config(config: ConnectionConfig) -> Self {
        Self {
            networks: Arc::new(RwLock::new(std::collections::HashMap::new())),
            config,
        }
    }

    /// 清除指定节点的连接缓存
    pub async fn invalidate(&self, target: NodeId) {
        let mut networks = self.networks.write().await;
        networks.remove(&target);
    }

    /// 清除所有连接缓存
    pub async fn clear_all(&self) {
        let mut networks = self.networks.write().await;
        networks.clear();
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
            if network.target == node.raft_addr {
                return KiwiNetwork {
                    client: Arc::clone(&network.client),
                    target: node.raft_addr.clone(),
                    config: self.config.clone(),
                };
            }
        }

        // Drop the read lock before acquiring the write lock
        drop(networks);

        // Acquire the write lock to create a new client for the target node
        let mut networks = self.networks.write().await;

        // Double-check if another async task has already created the client while we were waiting for the write lock
        if let Some(network) = networks.get(&target) {
            if network.target == node.raft_addr {
                return KiwiNetwork {
                    client: Arc::clone(&network.client),
                    target: node.raft_addr.clone(),
                    config: self.config.clone(),
                };
            }
        }

        networks.remove(&target);

        // Create new gRPC client for the target node
        let addr = node.raft_addr.clone();

        // 使用配置创建 endpoint
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", addr))
            .unwrap_or_else(|e| {
                panic!(
                    "invalid raft address '{}': failed to parse as gRPC endpoint: {}",
                    addr, e
                )
            });

        let endpoint = endpoint
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout)
            .connect_lazy();
        let client = RaftCoreServiceClient::new(endpoint);

        let network = KiwiNetwork {
            client: Arc::new(Mutex::new(client)),
            target: addr,
            config: self.config.clone(),
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
    /// 目标节点地址（用于错误日志）
    target: String,
    /// 连接配置
    config: ConnectionConfig,
}

// 私有 Clone 实现，仅供内部工厂使用
impl Clone for KiwiNetwork {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            target: self.target.clone(),
            config: self.config.clone(),
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
        let proto_req: crate::raft_proto::AppendEntriesRequest = rpc.try_into().map_err(|e| {
            RPCErr::Network(NetworkError::new(&io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to convert AppendEntriesRequest: {}", e),
            )))
        })?;

        // 调用 gRPC（带重试逻辑）
        let mut last_error = None;
        for attempt in 0..self.config.max_retries {
            if attempt > 0 {
                log::debug!(
                    "Retrying append_entries to {} (attempt {}/{})",
                    self.target,
                    attempt + 1,
                    self.config.max_retries
                );
                tokio::time::sleep(self.config.retry_interval).await;
            }

            let mut client = self.client.lock().await;
            match client
                .append_entries(TonicRequest::new(proto_req.clone()))
                .await
            {
                Ok(response) => {
                    let proto_resp = response.into_inner();
                    return (&proto_resp).try_into().map_err(|e| {
                        RPCErr::Network(NetworkError::new(&io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Failed to convert append_entries response: {}", e),
                        )))
                    });
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        // 所有重试都失败
        let err = last_error.unwrap();
        let grpc_err = GrpcClientError::RpcFailed {
            target: self.target.clone(),
            message: err.to_string(),
        };
        Err(RPCErr::Network(NetworkError::new(&io::Error::new(
            io::ErrorKind::ConnectionRefused,
            grpc_err.to_string(),
        ))))
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

        // 调用 gRPC（带重试逻辑）
        let mut last_error = None;
        for attempt in 0..self.config.max_retries {
            if attempt > 0 {
                log::debug!(
                    "Retrying vote to {} (attempt {}/{})",
                    self.target,
                    attempt + 1,
                    self.config.max_retries
                );
                tokio::time::sleep(self.config.retry_interval).await;
            }

            let mut client = self.client.lock().await;
            match client.vote(TonicRequest::new(proto_req.clone())).await {
                Ok(response) => {
                    let proto_resp = response.into_inner();
                    // Proto → OpenRaft
                    return (&proto_resp).try_into().map_err(|e| {
                        RPCErr::Network(NetworkError::new(&io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Failed to convert vote response: {}", e),
                        )))
                    });
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        // 所有重试都失败
        let err = last_error.unwrap();
        let grpc_err = GrpcClientError::RpcFailed {
            target: self.target.clone(),
            message: err.to_string(),
        };
        Err(RPCErr::Network(NetworkError::new(&io::Error::new(
            io::ErrorKind::ConnectionRefused,
            grpc_err.to_string(),
        ))))
    }
}
