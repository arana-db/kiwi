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

use conf::raft_type::{Binlog, BinlogResponse, KiwiNode, KiwiTypeConfig};
use openraft::{Config, Raft, SnapshotPolicy};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::grpc::{
    create_admin_service, create_client_service, create_core_service, create_metrics_service,
};
use crate::log_store::LogStore;
use crate::log_store_rocksdb::RocksdbLogStore;
use crate::network::KiwiNetworkFactory;
use crate::raft_proto::raft_admin_service_server::RaftAdminServiceServer;
use crate::raft_proto::raft_client_service_server::RaftClientServiceServer;
use crate::raft_proto::raft_core_service_server::RaftCoreServiceServer;
use crate::raft_proto::raft_metrics_service_server::RaftMetricsServiceServer;
use crate::state_machine::{KiwiStateMachine, PauseController};
use storage::storage::Storage;

pub struct RaftApp {
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub raft: Raft<KiwiTypeConfig>,
    pub storage_swap: Arc<ArcSwap<Storage>>,
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
        let log_id = Some(res.log_id.index);
        Ok(BinlogResponse {
            success: res.data.success,
            message: res.data.message,
            log_id,
        })
    }

    pub fn create_grpc_services(
        app: Arc<RaftApp>,
    ) -> (
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
    pub db_path: PathBuf,
    pub heartbeat_interval: u64,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub use_memory_log_store: bool,
    pub snapshot_logs_threshold: u64,
    pub snapshot_max_chunk_size: u64,
    pub install_snapshot_timeout: u64,
    pub max_in_snapshot_log_to_keep: u64,
    pub replication_lag_threshold: u64,
}

const SNAPSHOT_LOGS_THRESHOLD: u64 = 5000;
const SNAPSHOT_MAX_CHUNK_SIZE: u64 = 3 * 1024 * 1024;
const INSTALL_SNAPSHOT_TIMEOUT: u64 = 200;
const MAX_IN_SNAPSHOT_LOG_TO_KEEP: u64 = 1000;
const REPLICATION_LAG_THRESHOLD: u64 = 5000;

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_addr: "127.0.0.1:8081".to_string(),
            resp_addr: "127.0.0.1:6379".to_string(),
            data_dir: PathBuf::from("/tmp/kiwi/raft"),
            db_path: PathBuf::from("/tmp/kiwi/db"),
            heartbeat_interval: 200,
            election_timeout_min: 500,
            election_timeout_max: 1500,
            use_memory_log_store: false,
            snapshot_logs_threshold: SNAPSHOT_LOGS_THRESHOLD,
            snapshot_max_chunk_size: SNAPSHOT_MAX_CHUNK_SIZE,
            install_snapshot_timeout: INSTALL_SNAPSHOT_TIMEOUT,
            max_in_snapshot_log_to_keep: MAX_IN_SNAPSHOT_LOG_TO_KEEP,
            replication_lag_threshold: REPLICATION_LAG_THRESHOLD,
        }
    }
}

fn build_raft_config(config: &RaftConfig) -> Result<Arc<Config>, anyhow::Error> {
    let raft_config = Config {
        heartbeat_interval: config.heartbeat_interval,
        election_timeout_min: config.election_timeout_min,
        election_timeout_max: config.election_timeout_max,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_threshold),
        replication_lag_threshold: config.replication_lag_threshold,
        snapshot_max_chunk_size: config.snapshot_max_chunk_size,
        install_snapshot_timeout: config.install_snapshot_timeout,
        max_in_snapshot_log_to_keep: config.max_in_snapshot_log_to_keep,
        ..Default::default()
    };
    Ok(Arc::new(raft_config.validate()?))
}

pub async fn create_raft_node(
    config: RaftConfig,
    storage_swap: Arc<ArcSwap<Storage>>,
    pause_controller: Option<Arc<dyn PauseController>>,
) -> Result<Arc<RaftApp>, anyhow::Error> {
    let raft_config = build_raft_config(&config)?;
    let snapshot_work_dir = config.data_dir.join("snapshots");
    fs::create_dir_all(&snapshot_work_dir)?;

    let mut state_machine = KiwiStateMachine::new(
        config.node_id,
        storage_swap.clone(),
        config.db_path.clone(),
        snapshot_work_dir,
    );

    if let Some(ctrl) = pause_controller {
        state_machine.set_pause_controller(ctrl);
    }

    let network = KiwiNetworkFactory::new();

    let raft = if config.use_memory_log_store {
        let log_store_path = config.data_dir.join("raft_logs");
        std::fs::create_dir_all(&log_store_path)?;
        let log_store = LogStore::new();
        Raft::new(
            config.node_id,
            raft_config,
            network,
            log_store,
            state_machine,
        )
        .await?
    } else {
        let log_store_path = config.data_dir.join("raft_logs_rocksdb");
        std::fs::create_dir_all(&log_store_path)?;
        let log_store = RocksdbLogStore::open(&log_store_path)?;
        Raft::new(
            config.node_id,
            raft_config,
            network,
            log_store,
            state_machine,
        )
        .await?
    };

    Ok(Arc::new(RaftApp {
        node_id: config.node_id,
        raft_addr: config.raft_addr,
        resp_addr: config.resp_addr,
        raft,
        storage_swap,
    }))
}
