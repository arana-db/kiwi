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

use conf::raft_type::{Binlog, BinlogResponse, KiwiNode, KiwiTypeConfig};
use openraft::{Config, Raft};
use std::path::PathBuf;
use std::sync::Arc;

use crate::log_store::LogStore;
use crate::network::KiwiNetworkFactory;
use crate::state_machine::KiwiStateMachine;
use storage::storage::Storage;
use crate::grpc::{create_core_service, create_admin_service, create_client_service, create_metrics_service};
use crate::raft_proto::raft_core_service_server::RaftCoreServiceServer;
use crate::raft_proto::raft_admin_service_server::RaftAdminServiceServer;
use crate::raft_proto::raft_client_service_server::RaftClientServiceServer;
use crate::raft_proto::raft_metrics_service_server::RaftMetricsServiceServer;

pub struct RaftApp {
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub raft: Raft<KiwiTypeConfig>,
    pub storage: Arc<Storage>,
}

impl RaftApp {
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        let guard = metrics.borrow();
        matches!(guard.current_leader, Some(id) if id == self.node_id)
    }

    pub fn get_leader(&self) -> Option<(u64, KiwiNode)> {
        let metrics = self.raft.metrics();
        let guard = metrics.borrow();
        if let Some(leader_id) = guard.current_leader {
            let membership = guard.membership_config.membership();
            if let Some(node) = membership.get_node(&leader_id) {
                return Some((leader_id, node.clone()));
            }
        }
        None
    }

    pub async fn client_write(&self, binlog: Binlog) -> Result<BinlogResponse, anyhow::Error> {
        let res = self.raft.client_write(binlog).await?;
        Ok(res.data)
    }

    /// 创建所有 gRPC 服务
    pub fn create_grpc_services(app: Arc<RaftApp>) -> (
        RaftCoreServiceServer<crate::grpc::core::RaftCoreServiceImpl>,
        RaftAdminServiceServer<crate::grpc::admin::RaftAdminServiceImpl>,
        RaftClientServiceServer<crate::grpc::client::RaftClientServiceImpl>,
        RaftMetricsServiceServer<crate::grpc::client::RaftMetricsServiceImpl>,
    ) {
        (
            create_core_service(app.raft.clone()),
            create_admin_service(app.clone()),
            create_client_service(app.clone()),
            create_metrics_service(app),
        )
    }
}

pub struct RaftConfig {
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub data_dir: PathBuf,
    pub heartbeat_interval: u64,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_addr: "127.0.0.1:8081".to_string(),
            resp_addr: "127.0.0.1:6379".to_string(),
            data_dir: PathBuf::from("/tmp/kiwi/raft"),
            // 心跳间隔：500ms，同时也是 RPC 超时时间
            // 太短会导致在网络延迟高时出现超时错误
            heartbeat_interval: 500,
            // 选举超时最小值：1500ms，应该 >= heartbeat_interval * 2
            election_timeout_min: 1500,
            // 选举超时最大值：3000ms
            election_timeout_max: 3000,
        }
    }
}

pub async fn create_raft_node(
    config: RaftConfig,
    storage: Arc<Storage>,
) -> Result<Arc<RaftApp>, anyhow::Error> {
    let raft_config = Config {
        heartbeat_interval: config.heartbeat_interval,
        election_timeout_min: config.election_timeout_min,
        election_timeout_max: config.election_timeout_max,
        ..Default::default()
    };
    let raft_config = Arc::new(raft_config.validate()?);

    let log_store_path = config.data_dir.join("raft_logs");
    std::fs::create_dir_all(&log_store_path)?;

    let log_store = LogStore::new();

    let state_machine = KiwiStateMachine::new(config.node_id, storage.clone());

    let network = KiwiNetworkFactory::new();

    let raft = Raft::new(
        config.node_id,
        raft_config,
        network,
        log_store,
        state_machine,
    )
    .await?;

    let app = Arc::new(RaftApp {
        node_id: config.node_id,
        raft_addr: config.raft_addr,
        resp_addr: config.resp_addr,
        raft,
        storage,
    });

    Ok(app)
}
