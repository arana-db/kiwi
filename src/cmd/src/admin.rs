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

//! Administrative commands

use std::sync::Arc;

use bytes::Bytes;
use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::server_info::{NoopServerInfoProvider, ServerInfoProviderRef, ServerInfoSnapshot};
use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

/// INFO VECTOR: FLAT index kind and query counters. Skip set/element counts to stay O(1).
fn vector_section(storage: &Storage) -> String {
    let metrics = storage.vector_metrics();
    format!(
        "# Vector\r\n\
         index-kind:flat\r\n\
         vector_flat_queries_total:{}\r\n\
         vector_flat_query_timeouts_total:{}\r\n\
         vector_flat_query_errors_total:{}\r\n\
         vector_search_capacity_rejected_total:{}\r\n\
         vector_flat_query_duration_micros_total:{}\r\n\
         vector_flat_query_duration_count:{}\r\n",
        metrics.flat_queries_total,
        metrics.flat_query_timeouts_total,
        metrics.flat_query_errors_total,
        metrics.capacity_rejected_total,
        metrics.flat_query_duration_micros_total,
        metrics.flat_query_duration_count,
    )
}

fn server_section(snapshot: &ServerInfoSnapshot) -> String {
    format!(
        "# Server\r\n\
         redis_version:{}\r\n\
         kiwi_version:{}\r\n\
         redis_git_sha1:{}\r\n\
         redis_mode:{}\r\n\
         os:{}\r\n\
         arch_bits:{}\r\n\
         multiplexing_api:{}\r\n\
         process_id:{}\r\n\
         tcp_port:{}\r\n\
         uptime_in_seconds:{}\r\n\
         uptime_in_days:{}\r\n\
         executable:{}\r\n\
         config_file:{}\r\n",
        snapshot.version,
        snapshot.kiwi_version,
        snapshot.git_sha1,
        snapshot.redis_mode,
        snapshot.os,
        snapshot.arch_bits,
        snapshot.multiplexing_api,
        snapshot.process_id,
        snapshot.tcp_port,
        snapshot.uptime_seconds,
        snapshot.uptime_days,
        snapshot.executable,
        snapshot.config_file,
    )
}

fn clients_section() -> String {
    String::from("# Clients\r\nkiwi_clients:unavailable\r\n")
}

fn stats_section() -> String {
    String::from("# Stats\r\nkiwi_stats:unavailable\r\n")
}

fn persistence_section() -> String {
    String::from("# Persistence\r\nkiwi_persistence:unavailable\r\n")
}

fn replication_section(snapshot: &ServerInfoSnapshot) -> String {
    let mut out = String::from("# Replication\r\n");
    match snapshot.raft_role.as_deref() {
        Some("leader") => out.push_str("role:master\r\n"),
        Some("follower") => out.push_str("role:slave\r\n"),
        Some(_) => out.push_str("kiwi_replication_role:unavailable\r\n"),
        None if !snapshot.cluster_enabled => out.push_str("role:master\r\n"),
        None => out.push_str("kiwi_replication_role:unavailable\r\n"),
    }
    out
}

fn cluster_section(snapshot: &ServerInfoSnapshot) -> String {
    let mut out = String::from("# Cluster\r\n");
    out.push_str(&format!(
        "cluster_enabled:{}\r\n",
        snapshot.cluster_enabled as u8
    ));
    out.push_str(&format!("cluster_state:{}\r\n", snapshot.cluster_state));
    if let Some(node_id) = snapshot.raft_node_id {
        out.push_str(&format!("kiwi_raft_node_id:{}\r\n", node_id));
    }
    if let Some(term) = snapshot.raft_term {
        out.push_str(&format!("kiwi_raft_current_term:{}\r\n", term));
    }
    if let Some(role) = snapshot.raft_role.as_deref() {
        out.push_str(&format!("kiwi_raft_role:{}\r\n", role));
    }
    if let Some(leader) = snapshot.raft_leader {
        out.push_str(&format!("kiwi_raft_current_leader:{}\r\n", leader));
    }
    if let Some(applied) = snapshot.raft_last_applied {
        out.push_str(&format!("kiwi_raft_last_applied:{}\r\n", applied));
    }
    if let Some(last_log) = snapshot.raft_last_log_index {
        out.push_str(&format!("kiwi_raft_last_log_index:{}\r\n", last_log));
    }
    out
}

/// O(1) RocksDB key estimate. This is a physical approximation, not the exact
/// Redis db0 contract, so it is reported under a kiwi_* name and marked
/// unavailable if any instance fails to report.
fn keyspace_section(storage: &Storage) -> String {
    let mut keys = 0u64;
    let mut estimate_failed = false;
    for instance in &storage.insts {
        match instance.get_property("rocksdb.estimate-num-keys") {
            Ok(count) => keys += count,
            Err(_) => estimate_failed = true,
        }
    }
    let mut out = String::from("# Keyspace\r\n");
    if estimate_failed {
        out.push_str("kiwi_keyspace_estimate:unavailable\r\n");
    } else {
        out.push_str(&format!("kiwi_keyspace_estimate:{}\r\n", keys));
    }
    out
}

/// INFO command.
#[derive(Clone)]
pub struct InfoCmd {
    meta: CmdMeta,
    info_provider: ServerInfoProviderRef,
}

impl Default for InfoCmd {
    fn default() -> Self {
        Self::new()
    }
}

impl InfoCmd {
    pub fn new() -> Self {
        Self::with_provider(Arc::new(NoopServerInfoProvider))
    }

    pub fn with_provider(info_provider: ServerInfoProviderRef) -> Self {
        Self {
            meta: CmdMeta {
                name: "info".to_string(),
                arity: -1,
                flags: CmdFlags::READONLY | CmdFlags::ADMIN,
                acl_category: AclCategory::ADMIN,
                ..Default::default()
            },
            info_provider,
        }
    }
}

impl Cmd for InfoCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let snapshot = self.info_provider.snapshot();

        // Unknown sections are ignored; the reply stays a bulk string.
        let sections: Vec<String> = if client.argv().len() > 1 {
            client.argv()[1..]
                .iter()
                .map(|arg| String::from_utf8_lossy(arg).to_lowercase())
                .collect()
        } else {
            vec!["default".to_string()]
        };

        let wants = |name: &str| sections.iter().any(|s| s == name);
        let all = wants("all") || wants("everything");
        let default = all || wants("default");

        let mut info = String::new();

        if default || wants("server") {
            info.push_str(&server_section(&snapshot));
        }
        if default || wants("clients") {
            info.push_str(&clients_section());
        }
        if default || wants("stats") {
            info.push_str(&stats_section());
        }
        if default || wants("persistence") {
            info.push_str(&persistence_section());
        }
        if default || wants("replication") {
            info.push_str(&replication_section(&snapshot));
        }
        if default || wants("cluster") {
            info.push_str(&cluster_section(&snapshot));
        }
        if default || wants("keyspace") {
            info.push_str(&keyspace_section(&storage));
        }
        if default || wants("vector") {
            info.push_str(&vector_section(&storage));
        }

        client.set_reply(RespData::BulkString(Some(Bytes::from(info))));
    }
}

/// CONFIG command - Get/Set configuration parameters
#[derive(Clone, Default)]
pub struct ConfigCmd {
    meta: CmdMeta,
}

impl ConfigCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "config".to_string(),
                arity: -2,
                flags: CmdFlags::ADMIN,
                acl_category: AclCategory::ADMIN,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ConfigCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        if client.argv().len() < 2 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'config' command".into(),
            ));
            return;
        }

        let subcommand = String::from_utf8_lossy(&client.argv()[1]).to_lowercase();

        match subcommand.as_str() {
            "get" => {
                if client.argv().len() < 3 {
                    client.set_reply(RespData::Error(
                        "ERR wrong number of arguments for 'config get' command".into(),
                    ));
                    return;
                }

                let parameter = String::from_utf8_lossy(&client.argv()[2]).to_lowercase();

                // Return configuration (cluster mode is removed; report disabled)
                match parameter.as_str() {
                    "cluster-enabled" => {
                        let result = vec![
                            RespData::BulkString(Some(Bytes::from("cluster-enabled"))),
                            RespData::BulkString(Some(Bytes::from("no"))),
                        ];
                        client.set_reply(RespData::Array(Some(result)));
                    }
                    "*" => {
                        let result = vec![
                            RespData::BulkString(Some(Bytes::from("cluster-enabled"))),
                            RespData::BulkString(Some(Bytes::from("no"))),
                            RespData::BulkString(Some(Bytes::from("port"))),
                            RespData::BulkString(Some(Bytes::from("7379"))),
                        ];
                        client.set_reply(RespData::Array(Some(result)));
                    }
                    _ => {
                        client.set_reply(RespData::Array(Some(vec![])));
                    }
                }
            }
            "set" => {
                // For now, don't allow runtime configuration changes
                client.set_reply(RespData::Error(
                    "ERR runtime configuration changes not supported".into(),
                ));
            }
            _ => {
                client.set_reply(RespData::Error(
                    format!("ERR unknown CONFIG subcommand '{}'", subcommand).into(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use client::{Client, StreamTrait};

    use super::*;
    use crate::server_info::ServerInfoProvider;

    struct TestStream {
        _marker: u8,
    }

    #[async_trait::async_trait]
    impl StreamTrait for TestStream {
        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }

        async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }
    }

    fn test_storage() -> Arc<Storage> {
        Arc::new(Storage::new(1, 0))
    }

    struct TestServerInfoProvider {
        snapshot: ServerInfoSnapshot,
    }

    impl TestServerInfoProvider {
        fn standalone() -> Self {
            let mut snapshot = ServerInfoSnapshot::empty();
            snapshot.version = "9.9.9".to_string();
            snapshot.kiwi_version = "0.0.0-kiwi".to_string();
            snapshot.process_id = 4242;
            snapshot.tcp_port = 7777;
            snapshot.os = "testos".to_string();
            snapshot.executable = "/opt/kiwi/kiwi".to_string();
            snapshot.config_file = "/etc/kiwi.conf".to_string();
            snapshot.cluster_enabled = false;
            snapshot.cluster_state = "disabled".to_string();
            Self { snapshot }
        }

        fn raft_leader() -> Self {
            let mut provider = Self::standalone();
            provider.snapshot.cluster_enabled = true;
            provider.snapshot.cluster_state = "ok".to_string();
            provider.snapshot.raft_node_id = Some(1);
            provider.snapshot.raft_term = Some(3);
            provider.snapshot.raft_leader = Some(1);
            provider.snapshot.raft_role = Some("leader".to_string());
            provider
        }

        fn raft_follower() -> Self {
            let mut provider = Self::raft_leader();
            provider.snapshot.raft_node_id = Some(2);
            provider.snapshot.raft_leader = Some(1);
            provider.snapshot.raft_role = Some("follower".to_string());
            provider
        }

        fn raft_no_leader() -> Self {
            let mut provider = Self::raft_leader();
            provider.snapshot.cluster_state = "fail".to_string();
            provider.snapshot.raft_leader = None;
            provider.snapshot.raft_role = None;
            provider
        }

        fn raft_unavailable() -> Self {
            let mut provider = Self::standalone();
            provider.snapshot.cluster_enabled = true;
            provider.snapshot.cluster_state = "unavailable".to_string();
            provider
        }
    }

    impl ServerInfoProvider for TestServerInfoProvider {
        fn snapshot(&self) -> ServerInfoSnapshot {
            self.snapshot.clone()
        }
    }

    fn run_info_reply(info_cmd: &InfoCmd, argv: &[&str]) -> RespData {
        let client = Client::new(Box::new(TestStream { _marker: 0 }));
        client.set_cmd_name(b"info");
        let argv: Vec<Vec<u8>> = argv.iter().map(|arg| arg.as_bytes().to_vec()).collect();
        client.set_argv(&argv);
        info_cmd.execute(&client, test_storage());
        client.take_reply()
    }

    fn run_info(info_cmd: &InfoCmd, argv: &[&str]) -> String {
        match run_info_reply(info_cmd, argv) {
            RespData::BulkString(Some(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
            other => panic!("expected bulk reply, got {other:?}"),
        }
    }

    #[test]
    fn server_section_uses_real_values() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::standalone()));
        let out = run_info(&cmd, &["info", "server"]);
        assert!(out.contains("redis_version:9.9.9\r\n"), "{out}");
        assert!(out.contains("kiwi_version:0.0.0-kiwi\r\n"), "{out}");
        assert!(out.contains("redis_git_sha1:0000000000000000\r\n"), "{out}");
        assert!(out.contains("process_id:4242\r\n"), "{out}");
        assert!(out.contains("tcp_port:7777\r\n"), "{out}");
        assert!(out.contains("os:testos\r\n"), "{out}");
        assert!(out.contains("executable:/opt/kiwi/kiwi\r\n"), "{out}");
        assert!(out.contains("config_file:/etc/kiwi.conf\r\n"), "{out}");
        assert!(!out.contains("7.0.0"), "no hardcoded redis version");
        assert!(!out.contains("Windows"), "no hardcoded os");
        assert!(!out.contains("process_id:1"), "no hardcoded pid");
        assert!(
            !out.contains("/path/to/kiwi-server"),
            "no placeholder executable"
        );
        assert!(!out.contains("tcp_port:7379"), "no hardcoded port");
    }

    #[test]
    fn cluster_section_reflects_live_leader() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::raft_leader()));
        let out = run_info(&cmd, &["info", "cluster"]);
        assert!(out.contains("cluster_enabled:1\r\n"), "{out}");
        assert!(out.contains("cluster_state:ok\r\n"), "{out}");
        assert!(out.contains("kiwi_raft_node_id:1\r\n"), "{out}");
        assert!(out.contains("kiwi_raft_current_term:3\r\n"), "{out}");
        assert!(out.contains("kiwi_raft_role:leader\r\n"), "{out}");
        assert!(out.contains("kiwi_raft_current_leader:1\r\n"), "{out}");
    }

    #[test]
    fn cluster_section_disabled_when_no_raft() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::standalone()));
        let out = run_info(&cmd, &["info", "cluster"]);
        assert!(out.contains("cluster_enabled:0\r\n"), "{out}");
        assert!(out.contains("cluster_state:disabled\r\n"), "{out}");
    }

    #[test]
    fn cluster_section_fail_when_leader_missing() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::raft_no_leader()));
        let out = run_info(&cmd, &["info", "cluster"]);
        assert!(out.contains("cluster_enabled:1\r\n"), "{out}");
        assert!(out.contains("cluster_state:fail\r\n"), "{out}");
        assert!(!out.contains("kiwi_raft_current_leader:"), "{out}");
        let repl = run_info(&cmd, &["info", "replication"]);
        assert!(
            repl.contains("kiwi_replication_role:unavailable\r\n"),
            "{repl}"
        );
        assert!(!repl.contains("role:master"), "{repl}");
    }

    #[test]
    fn cluster_section_unavailable_without_metrics() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::raft_unavailable()));
        let out = run_info(&cmd, &["info", "cluster"]);
        assert!(out.contains("cluster_enabled:1\r\n"), "{out}");
        assert!(out.contains("cluster_state:unavailable\r\n"), "{out}");
        assert!(!out.contains("cluster_state:ok\r\n"), "{out}");
    }

    #[test]
    fn replication_section_maps_follower_role() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::raft_follower()));
        let out = run_info(&cmd, &["info", "replication"]);
        assert!(out.contains("role:slave\r\n"), "{out}");
        assert!(!out.contains("connected_slaves:"), "{out}");
    }

    #[test]
    fn unwired_sections_do_not_emit_fake_zeros() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::standalone()));
        let out = run_info(&cmd, &["info", "clients", "stats", "persistence"]);
        assert!(out.contains("kiwi_clients:unavailable\r\n"), "{out}");
        assert!(out.contains("kiwi_stats:unavailable\r\n"), "{out}");
        assert!(out.contains("kiwi_persistence:unavailable\r\n"), "{out}");
        assert!(!out.contains("connected_clients:"), "{out}");
        assert!(!out.contains("total_commands_processed:"), "{out}");
        assert!(!out.contains("rdb_bgsave_in_progress:"), "{out}");
        assert!(!out.contains("loading:"), "{out}");
    }

    #[test]
    fn unknown_section_returns_empty_bulk() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::standalone()));
        let reply = run_info_reply(&cmd, &["info", "nosuch"]);
        match reply {
            RespData::BulkString(Some(bytes)) => {
                assert!(bytes.is_empty(), "unknown-only INFO must be empty bulk");
            }
            other => panic!("expected bulk reply, got {other:?}"),
        }
    }

    #[test]
    fn unknown_section_does_not_drop_known_sections() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::standalone()));
        let out = run_info(&cmd, &["info", "server", "nosuch", "cluster"]);
        assert!(out.contains("# Server\r\n"), "{out}");
        assert!(out.contains("# Cluster\r\n"), "{out}");
        assert!(!out.contains("# Clients\r\n"), "{out}");
    }

    #[test]
    fn default_section_includes_keyspace() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::standalone()));
        let out = run_info(&cmd, &["info"]);
        assert!(out.contains("# Server\r\n"), "{out}");
        assert!(out.contains("# Cluster\r\n"), "{out}");
        assert!(out.contains("# Keyspace\r\n"), "{out}");
        assert!(out.contains("kiwi_keyspace_estimate:"), "{out}");
        assert!(!out.contains("db0:"), "{out}");
        assert!(out.contains("# Clients\r\n"), "{out}");
    }

    #[test]
    fn all_section_includes_keyspace_and_vector() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::standalone()));
        let out = run_info(&cmd, &["info", "all"]);
        assert!(out.contains("# Keyspace\r\n"), "{out}");
        assert!(out.contains("# Vector\r\n"), "{out}");
    }
}
