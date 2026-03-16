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
use openraft::{Config, Raft};
use std::path::PathBuf;
use std::sync::Arc;

use crate::log_store::LogStore;
use crate::log_store_rocksdb::RocksdbLogStore;
use crate::network::KiwiNetworkFactory;
use crate::state_machine::KiwiStateMachine;
use storage::storage::Storage;

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
}

pub struct RaftConfig {
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub data_dir: PathBuf,
    pub heartbeat_interval: u64,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    /// 是否使用内存日志存储，默认 false（使用 RocksDB 持久化存储）
    pub use_memory_log_store: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_addr: "127.0.0.1:8081".to_string(),
            resp_addr: "127.0.0.1:6379".to_string(),
            data_dir: PathBuf::from("/tmp/kiwi/raft"),
            heartbeat_interval: 200,
            election_timeout_min: 500,
            election_timeout_max: 1000,
            use_memory_log_store: false,
        }
    }
}

/// 构建通用的 Raft 配置
fn build_raft_config(config: &RaftConfig) -> Result<Arc<Config>, anyhow::Error> {
    let raft_config = Config {
        heartbeat_interval: config.heartbeat_interval,
        election_timeout_min: config.election_timeout_min,
        election_timeout_max: config.election_timeout_max,
        ..Default::default()
    };
    Ok(Arc::new(raft_config.validate()?))
}

/// 构建 RaftApp，根据 RaftConfig.use_memory_log_store 决定使用内存或 RocksDB 日志存储。
/// 默认使用 RocksDB 持久化存储；可在配置文件中设置 `raft-use-memory-log-store yes` 切换为内存存储（适用于测试）。
pub async fn create_raft_node(
    config: RaftConfig,
    storage: Arc<Storage>,
) -> Result<Arc<RaftApp>, anyhow::Error> {
    let raft_config = build_raft_config(&config)?;
    let state_machine = KiwiStateMachine::new(config.node_id, storage.clone());
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
        storage,
    }))
}
