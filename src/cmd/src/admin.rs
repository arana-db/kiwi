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

/// INFO VECTOR section: the Phase 1 index kind plus the FLAT query counters.
/// vector_sets / vector_elements are omitted on purpose: counting them needs
/// a full keyspace scan and INFO must stay O(1).
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

const INFO_SECTIONS: [&str; 8] = [
    "server",
    "clients",
    "stats",
    "persistence",
    "replication",
    "cluster",
    "keyspace",
    "vector",
];

fn server_section(snapshot: &ServerInfoSnapshot) -> String {
    format!(
        "# Server\r\n\
         redis_version:{}\r\n\
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

fn clients_section(snapshot: &ServerInfoSnapshot) -> String {
    format!(
        "# Clients\r\nconnected_clients:{}\r\n",
        snapshot.connected_clients
    )
}

fn stats_section(snapshot: &ServerInfoSnapshot) -> String {
    format!(
        "# Stats\r\n\
         total_connections_received:{}\r\n\
         total_commands_processed:{}\r\n",
        snapshot.total_connections_received, snapshot.total_commands_processed
    )
}

fn persistence_section(snapshot: &ServerInfoSnapshot) -> String {
    format!(
        "# Persistence\r\n\
         loading:0\r\n\
         rdb_bgsave_in_progress:{}\r\n\
         rdb_changes_since_last_save:{}\r\n",
        snapshot.rdb_bgsave_in_progress, snapshot.rdb_changes_since_last_save,
    )
}

fn replication_section(snapshot: &ServerInfoSnapshot) -> String {
    let role = match snapshot.raft_role.as_deref() {
        Some("leader") => "master",
        Some(_) => "slave",
        None => "master",
    };
    format!("# Replication\r\nrole:{}\r\nconnected_slaves:0\r\n", role)
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

/// note(guozhihao-224) Real key count would need a full keyspace scan, which INFO
/// must avoid; RocksDB's estimate-num-keys is O(1) and approximate.
fn keyspace_section(storage: &Storage) -> String {
    let keys: u64 = storage
        .insts
        .iter()
        .filter_map(|instance| instance.get_property("rocksdb.estimate-num-keys").ok())
        .sum();
    format!("# Keyspace\r\ndb0:keys={}\r\n", keys)
}

/// INFO command - Show real server information including cluster status.
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

        let sections: Vec<String> = if client.argv().len() > 1 {
            client.argv()[1..]
                .iter()
                .map(|arg| String::from_utf8_lossy(arg).to_lowercase())
                .collect()
        } else {
            vec!["default".to_string()]
        };

        for section in &sections {
            if section != "default"
                && section != "all"
                && section != "everything"
                && !INFO_SECTIONS.contains(&section.as_str())
            {
                client.set_reply(RespData::Error(
                    format!("ERR Unrecognized INFO section: '{}'", section).into(),
                ));
                return;
            }
        }

        let wants = |name: &str| sections.iter().any(|s| s == name);
        let all = wants("all") || wants("everything");
        let default = all || wants("default");

        let mut info = String::new();

        if default || all || wants("server") {
            info.push_str(&server_section(&snapshot));
        }
        if default || all || wants("clients") {
            info.push_str(&clients_section(&snapshot));
        }
        if default || all || wants("stats") {
            info.push_str(&stats_section(&snapshot));
        }
        if default || all || wants("persistence") {
            info.push_str(&persistence_section(&snapshot));
        }
        if default || all || wants("replication") {
            info.push_str(&replication_section(&snapshot));
        }
        if default || all || wants("cluster") {
            info.push_str(&cluster_section(&snapshot));
        }
        if all || wants("keyspace") {
            info.push_str(&keyspace_section(&storage));
        }
        if default || all || wants("vector") {
            info.push_str(&vector_section(&storage));
        }

        client.set_reply(RespData::BulkString(Some(Bytes::from(info))));
    }
}

#[cfg(test)]
mod tests {
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
        fn new(raft_enabled: bool) -> Self {
            let mut snapshot = ServerInfoSnapshot::empty();
            snapshot.version = "9.9.9".to_string();
            snapshot.process_id = 4242;
            snapshot.tcp_port = 7777;
            snapshot.os = "testos".to_string();
            snapshot.executable = "/opt/kiwi/kiwi".to_string();
            snapshot.config_file = "/etc/kiwi.conf".to_string();
            snapshot.cluster_enabled = raft_enabled;
            snapshot.cluster_state = if raft_enabled { "ok" } else { "disabled" }.to_string();
            if raft_enabled {
                snapshot.raft_node_id = Some(1);
                snapshot.raft_term = Some(3);
                snapshot.raft_role = Some("leader".to_string());
            }
            Self { snapshot }
        }
    }

    impl ServerInfoProvider for TestServerInfoProvider {
        fn snapshot(&self) -> ServerInfoSnapshot {
            self.snapshot.clone()
        }
    }

    fn run_info(info_cmd: &InfoCmd, argv: &[&str]) -> String {
        let client = Client::new(Box::new(TestStream { _marker: 0 }));
        client.set_cmd_name(b"info");
        let argv: Vec<Vec<u8>> = argv.iter().map(|arg| arg.as_bytes().to_vec()).collect();
        client.set_argv(&argv);
        info_cmd.execute(&client, test_storage());
        match client.take_reply() {
            RespData::BulkString(Some(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
            other => panic!("expected bulk reply, got {other:?}"),
        }
    }

    #[test]
    fn server_section_uses_real_values() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::new(false)));
        let out = run_info(&cmd, &["info", "server"]);
        assert!(out.contains("redis_version:9.9.9\r\n"), "{out}");
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
    fn cluster_section_reflects_raft_enabled() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::new(true)));
        let out = run_info(&cmd, &["info", "cluster"]);
        assert!(out.contains("cluster_enabled:1\r\n"), "{out}");
        assert!(out.contains("cluster_state:ok\r\n"), "{out}");
        assert!(out.contains("kiwi_raft_node_id:1\r\n"), "{out}");
        assert!(out.contains("kiwi_raft_current_term:3\r\n"), "{out}");
    }

    #[test]
    fn cluster_section_disabled_when_no_raft() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::new(false)));
        let out = run_info(&cmd, &["info", "cluster"]);
        assert!(out.contains("cluster_enabled:0\r\n"), "{out}");
        assert!(out.contains("cluster_state:disabled\r\n"), "{out}");
    }

    #[test]
    fn unknown_section_returns_error() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::new(false)));
        let client = Client::new(Box::new(TestStream { _marker: 0 }));
        client.set_cmd_name(b"info");
        client.set_argv(&[b"info".to_vec(), b"nosuch".to_vec()]);
        cmd.execute(&client, test_storage());
        assert!(matches!(client.take_reply(), RespData::Error(_)));
    }

    #[test]
    fn default_section_includes_server_and_cluster() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::new(false)));
        let out = run_info(&cmd, &["info"]);
        assert!(out.contains("# Server\r\n"), "{out}");
        assert!(out.contains("# Cluster\r\n"), "{out}");
    }

    #[test]
    fn all_section_includes_keyspace_and_vector() {
        let cmd = InfoCmd::with_provider(Arc::new(TestServerInfoProvider::new(false)));
        let out = run_info(&cmd, &["info", "all"]);
        assert!(out.contains("# Keyspace\r\n"), "{out}");
        assert!(out.contains("# Vector\r\n"), "{out}");
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
