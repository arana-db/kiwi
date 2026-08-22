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

//! Read-only INFO snapshot. Missing sources are omitted or marked unavailable.

use std::sync::Arc;

#[derive(Clone)]
pub struct ServerInfoSnapshot {
    pub version: String,
    pub kiwi_version: String,
    pub git_sha1: String,
    pub redis_mode: String,
    pub os: String,
    pub arch_bits: u32,
    pub multiplexing_api: String,
    pub process_id: u64,
    pub tcp_port: u64,
    pub uptime_seconds: u64,
    pub uptime_days: u64,
    pub executable: String,
    pub config_file: String,
    pub cluster_enabled: bool,
    pub cluster_state: String,
    pub raft_node_id: Option<u64>,
    pub raft_role: Option<String>,
    pub raft_term: Option<u64>,
    pub raft_leader: Option<u64>,
    pub raft_last_applied: Option<u64>,
    pub raft_last_log_index: Option<u64>,
}

impl ServerInfoSnapshot {
    pub fn empty() -> Self {
        Self {
            version: String::new(),
            kiwi_version: String::new(),
            git_sha1: "0000000000000000".to_string(),
            redis_mode: "standalone".to_string(),
            os: String::new(),
            arch_bits: 0,
            multiplexing_api: String::new(),
            process_id: 0,
            tcp_port: 0,
            uptime_seconds: 0,
            uptime_days: 0,
            executable: String::new(),
            config_file: String::new(),
            cluster_enabled: false,
            cluster_state: "disabled".to_string(),
            raft_node_id: None,
            raft_role: None,
            raft_term: None,
            raft_leader: None,
            raft_last_applied: None,
            raft_last_log_index: None,
        }
    }
}

pub trait ServerInfoProvider: Send + Sync {
    fn snapshot(&self) -> ServerInfoSnapshot;
}

/// Empty snapshot for the network-runtime command table and tests.
#[derive(Clone, Default)]
pub struct NoopServerInfoProvider;

impl ServerInfoProvider for NoopServerInfoProvider {
    fn snapshot(&self) -> ServerInfoSnapshot {
        ServerInfoSnapshot::empty()
    }
}

pub type ServerInfoProviderRef = Arc<dyn ServerInfoProvider>;
