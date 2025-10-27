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

//! Administrative commands for cluster management

use std::sync::Arc;

use client::Client;
use resp::RespData;
use storage::storage::Storage;
use bytes::Bytes;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

/// INFO command - Show server information including cluster status
#[derive(Clone, Default)]
pub struct InfoCmd {
    meta: CmdMeta,
}

impl InfoCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "info".to_string(),
                arity: -1,
                flags: CmdFlags::READONLY | CmdFlags::ADMIN,
                acl_category: AclCategory::ADMIN,
                ..Default::default()
            },
        }
    }
}

impl Cmd for InfoCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        let section = if client.argv().len() > 1 {
            String::from_utf8_lossy(&client.argv()[1]).to_lowercase()
        } else {
            "default".to_string()
        };

        let mut info = String::new();

        match section.as_str() {
            "cluster" => {
                info.push_str("# Cluster\r\n");
                info.push_str("cluster_enabled:1\r\n");
                info.push_str("cluster_state:ok\r\n");
                info.push_str("cluster_slots_assigned:16384\r\n");
                info.push_str("cluster_slots_ok:16384\r\n");
                info.push_str("cluster_slots_pfail:0\r\n");
                info.push_str("cluster_slots_fail:0\r\n");
                info.push_str("cluster_known_nodes:1\r\n");
                info.push_str("cluster_size:1\r\n");
                info.push_str("cluster_current_epoch:1\r\n");
                info.push_str("cluster_my_epoch:1\r\n");
            }
            "raft" => {
                info.push_str("# Raft\r\n");
                info.push_str("raft_state:follower\r\n");
                info.push_str("raft_term:1\r\n");
                info.push_str("raft_leader:none\r\n");
                info.push_str("raft_commit_index:0\r\n");
                info.push_str("raft_last_applied:0\r\n");
                info.push_str("raft_log_size:0\r\n");
            }
            "server" | "default" => {
                info.push_str("# Server\r\n");
                info.push_str("redis_version:7.0.0\r\n");
                info.push_str("redis_git_sha1:00000000\r\n");
                info.push_str("redis_git_dirty:0\r\n");
                info.push_str("redis_build_id:0\r\n");
                info.push_str("redis_mode:cluster\r\n");
                info.push_str("os:Windows\r\n");
                info.push_str("arch_bits:64\r\n");
                info.push_str("multiplexing_api:select\r\n");
                info.push_str("atomicvar_api:atomic-builtin\r\n");
                info.push_str("gcc_version:0.0.0\r\n");
                info.push_str("process_id:1\r\n");
                info.push_str("tcp_port:7379\r\n");
                info.push_str("uptime_in_seconds:1\r\n");
                info.push_str("uptime_in_days:0\r\n");
                info.push_str("hz:10\r\n");
                info.push_str("configured_hz:10\r\n");
                info.push_str("lru_clock:1\r\n");
                info.push_str("executable:/path/to/kiwi-server\r\n");
                info.push_str("config_file:\r\n");
                
                if section == "default" {
                    info.push_str("\r\n# Cluster\r\n");
                    info.push_str("cluster_enabled:1\r\n");
                    info.push_str("cluster_state:ok\r\n");
                    info.push_str("cluster_known_nodes:1\r\n");
                    info.push_str("cluster_size:1\r\n");
                }
            }
            _ => {
                // Default to server info for unknown sections
                info.push_str("# Server\r\n");
                info.push_str("redis_version:7.0.0\r\n");
                info.push_str("redis_mode:cluster\r\n");
            }
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
                
                // Return cluster-related configuration
                match parameter.as_str() {
                    "cluster-enabled" => {
                        let result = vec![
                            RespData::BulkString(Some(Bytes::from("cluster-enabled"))),
                            RespData::BulkString(Some(Bytes::from("yes"))),
                        ];
                        client.set_reply(RespData::Array(Some(result)));
                    }
                    "*" => {
                        // Return all configuration parameters
                        let result = vec![
                            RespData::BulkString(Some(Bytes::from("cluster-enabled"))),
                            RespData::BulkString(Some(Bytes::from("yes"))),
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