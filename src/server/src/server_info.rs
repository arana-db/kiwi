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

use std::sync::{Arc, RwLock};
use std::time::Instant;

use cmd::server_info::{ServerInfoProvider, ServerInfoSnapshot};
use conf::config::Config;
use raft::node::RaftApp;

/// The Redis compatibility baseline this server targets, advertised as
/// `redis_version` so clients select the correct capability branch.
const REDIS_COMPAT_VERSION: &str = "8.8.1";

/// A leader that has not acknowledged a quorum within this window is treated as
/// partitioned and the cluster is reported as failed. Two times the default max
/// election timeout (1500ms) bounds the stale-leader window.
const QUORUM_ACK_MAX_STALE_MS: u64 = 3000;

pub struct KiwiServerInfoProvider {
    version: String,
    kiwi_version: String,
    process_id: u64,
    start: Instant,
    port: u64,
    executable: String,
    config_file: String,
    raft_enabled: bool,
    raft: Arc<RwLock<Option<Arc<RaftApp>>>>,
}

impl KiwiServerInfoProvider {
    pub fn new(config: &Config, config_file: String) -> Self {
        Self {
            version: REDIS_COMPAT_VERSION.to_string(),
            kiwi_version: env!("CARGO_PKG_VERSION").to_string(),
            process_id: std::process::id() as u64,
            start: Instant::now(),
            port: config.port as u64,
            executable: std::env::current_exe()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            config_file,
            raft_enabled: config.raft.is_some(),
            raft: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_raft(&self, app: Arc<RaftApp>) {
        let mut slot = self.raft.write().unwrap_or_else(|err| err.into_inner());
        *slot = Some(app);
    }
}

impl ServerInfoProvider for KiwiServerInfoProvider {
    fn snapshot(&self) -> ServerInfoSnapshot {
        let uptime_seconds = self.start.elapsed().as_secs();
        let mut snapshot = ServerInfoSnapshot::empty();
        snapshot.version = self.version.clone();
        snapshot.kiwi_version = self.kiwi_version.clone();
        snapshot.redis_mode = if self.raft_enabled {
            "cluster"
        } else {
            "standalone"
        }
        .to_string();
        snapshot.os = std::env::consts::OS.to_string();
        snapshot.arch_bits = if usize::BITS == 64 { 64 } else { 32 };
        snapshot.multiplexing_api = "tokio".to_string();
        snapshot.process_id = self.process_id;
        snapshot.tcp_port = self.port;
        snapshot.uptime_seconds = uptime_seconds;
        snapshot.uptime_days = uptime_seconds / 86400;
        snapshot.executable = self.executable.clone();
        snapshot.config_file = self.config_file.clone();
        snapshot.cluster_enabled = self.raft_enabled;
        snapshot.cluster_state = if self.raft_enabled {
            "unavailable"
        } else {
            "disabled"
        }
        .to_string();

        let raft = self
            .raft
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .clone();
        if let Some(app) = raft {
            let metrics = app.raft.metrics();
            let guard = metrics.borrow();
            snapshot.raft_node_id = Some(guard.id);
            snapshot.raft_term = Some(guard.current_term);
            snapshot.raft_leader = guard.current_leader;
            snapshot.raft_last_applied = guard.last_applied.map(|log_id| log_id.index);
            snapshot.raft_last_log_index = guard.last_log_index;

            if guard.state.is_leader() {
                snapshot.raft_role = Some("leader".to_string());
            } else if guard.state.is_follower() {
                snapshot.raft_role = Some("follower".to_string());
            }

            let running = guard.running_state.is_ok();
            let campaigning = guard.state.is_candidate();
            let stale_quorum = guard.state.is_leader()
                && guard
                    .millis_since_quorum_ack
                    .is_some_and(|ms| ms > QUORUM_ACK_MAX_STALE_MS);
            snapshot.cluster_state =
                if running && !campaigning && guard.current_leader.is_some() && !stale_quorum {
                    "ok".to_string()
                } else {
                    "fail".to_string()
                };
        }

        snapshot
    }
}
