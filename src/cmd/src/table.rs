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

use std::collections::HashMap;
use std::sync::Arc;

use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::auth::RequirepassProvider;
use crate::{Cmd, CmdMeta};

pub type CmdTable = HashMap<String, Arc<dyn Cmd>>;

/// Feature gates captured when a command table is built.
///
/// The values come from startup configuration and do not change while a
/// command table is alive, so storing them by value avoids an unnecessary
/// callback/Arc allocation for every gate.
#[derive(Clone, Copy)]
pub struct CommandTableGates {
    /// Whether the Vector Set commands (VADD/VSIM/...) are enabled.
    pub vector_enabled: bool,
    /// Whether Vector Set commands are allowed given the cluster state:
    /// false in cluster mode until the Raft apply-correctness contract (PR0)
    /// lands.
    pub vector_cluster_allowed: bool,
    /// Whether FLUSHDB/FLUSHALL are allowed. Disabled in cluster mode unless
    /// `cluster-flush-enabled` is set.
    pub cluster_flush_allowed: bool,
}

impl Default for CommandTableGates {
    fn default() -> Self {
        Self {
            vector_enabled: true,
            vector_cluster_allowed: true,
            cluster_flush_allowed: true,
        }
    }
}

impl CommandTableGates {
    /// Build gates from static flags (the common case: values come from config).
    pub fn from_flags(
        vector_enabled: bool,
        vector_cluster_allowed: bool,
        cluster_flush_allowed: bool,
    ) -> Self {
        Self {
            vector_enabled,
            vector_cluster_allowed,
            cluster_flush_allowed,
        }
    }
}

/// Wraps a command with a deterministic pre-execution gate: when `allowed` is
/// false the command replies with `disabled_error` and the inner command never
/// runs.
#[derive(Clone)]
struct GatedCmd {
    inner: Arc<dyn Cmd>,
    allowed: bool,
    disabled_error: String,
}

impl Cmd for GatedCmd {
    fn meta(&self) -> &CmdMeta {
        self.inner.meta()
    }

    fn do_initial(&self, client: &Client) -> bool {
        if !self.allowed {
            client.set_reply(RespData::Error(self.disabled_error.clone().into()));
            return false;
        }
        self.inner.do_initial(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        self.inner.do_cmd(client, storage);
    }

    fn clone_box(&self) -> Box<dyn Cmd> {
        Box::new(self.clone())
    }
}

/// Register `cmds` wrapped in a gate that replies `disabled_error` when
/// `allowed` evaluates to false.
fn register_gated_cmds(
    cmd_table: &mut CmdTable,
    cmds: Vec<Arc<dyn Cmd>>,
    allowed: bool,
    disabled_error: impl Fn(&CmdMeta) -> String,
) {
    for cmd in cmds {
        let meta = cmd.meta().clone();
        let gated = GatedCmd {
            inner: cmd,
            allowed,
            disabled_error: disabled_error(&meta),
        };
        cmd_table.insert(meta.name, Arc::new(gated));
    }
}

/// Wrap each command in a gate without registering it, so another gate can be
/// layered on top before insertion into the table.
fn wrap_gated_cmds(
    cmds: Vec<Arc<dyn Cmd>>,
    allowed: bool,
    disabled_error: impl Fn(&CmdMeta) -> String,
) -> Vec<Arc<dyn Cmd>> {
    cmds.into_iter()
        .map(|cmd| {
            let meta = cmd.meta().clone();
            Arc::new(GatedCmd {
                inner: cmd,
                allowed,
                disabled_error: disabled_error(&meta),
            }) as Arc<dyn Cmd>
        })
        .collect()
}

#[macro_export]
macro_rules! register_cmd {
    ($cmd_table:expr_2021, $($cmd_struct:ty),+ $(,)?) => {
        $(
            {
                let cmd = <$cmd_struct>::new();
                let cmd_name = cmd.meta().name.clone();
                let arc_cmd = Arc::new(cmd);
                $cmd_table.insert(cmd_name, arc_cmd);
            }
        )+
    };
}

#[macro_export]
macro_rules! register_group_cmd {
    ($cmd_table:expr_2021, $($constructor:path),+ $(,)?) => {
        $(
            {
                let group_cmd = $constructor();
                let cmd_name = group_cmd.name().to_lowercase();
                $cmd_table.insert(cmd_name, Arc::new(group_cmd));
            }
        )+
    };
}

pub fn create_command_table(requirepass_provider: RequirepassProvider) -> CmdTable {
    create_command_table_with_gates(requirepass_provider, CommandTableGates::default())
}

pub fn create_command_table_with_gates(
    requirepass_provider: RequirepassProvider,
    gates: CommandTableGates,
) -> CmdTable {
    let mut cmd_table: CmdTable = HashMap::new();

    register_cmd!(
        cmd_table,
        // String commands
        crate::append::AppendCmd,
        crate::set::SetCmd,
        crate::get::GetCmd,
        crate::incr::IncrCmd,
        crate::incrby::IncrbyCmd,
        crate::incrbyfloat::IncrbyFloatCmd,
        crate::decr::DecrCmd,
        crate::decrby::DecrbyCmd,
        crate::strlen::StrlenCmd,
        crate::substr::SubstrCmd,
        crate::getrange::GetrangeCmd,
        crate::setrange::SetrangeCmd,
        crate::setex::SetexCmd,
        crate::psetex::PsetexCmd,
        crate::setnx::SetnxCmd,
        crate::getset::GetsetCmd,
        crate::mget::MgetCmd,
        crate::mset::MsetCmd,
        crate::msetnx::MsetnxCmd,
        crate::setbit::SetbitCmd,
        crate::getbit::GetbitCmd,
        crate::bitcount::BitcountCmd,
        crate::bitpos::BitposCmd,
        crate::bitop::BitopCmd,
        // Keyspace and TTL commands
        crate::del::DelCmd,
        crate::exists::ExistsCmd,
        crate::scan::ScanCmd,
        crate::expire::ExpireCmd,
        crate::expireat::ExpireatCmd,
        crate::pexpire::PexpireCmd,
        crate::pexpireat::PexpireatCmd,
        crate::ttl::TtlCmd,
        crate::pttl::PttlCmd,
        crate::persist::PersistCmd,
        crate::type_cmd::TypeCmd,
        crate::keys::KeysCmd,
        crate::randomkey::RandomkeyCmd,
        // Hash commands
        crate::hset::HSetCmd,
        crate::hget::HGetCmd,
        crate::hdel::HDelCmd,
        crate::hexists::HExistsCmd,
        crate::hlen::HLenCmd,
        crate::hkeys::HKeysCmd,
        crate::hmset::HMSetCmd,
        crate::hmget::HMGetCmd,
        crate::hgetall::HGetAllCmd,
        crate::hvals::HValsCmd,
        crate::hincrby::HIncrByCmd,
        crate::hincrbyfloat::HIncrByFloatCmd,
        crate::hsetnx::HSetNXCmd,
        crate::hstrlen::HStrLenCmd,
        crate::hscan::HScanCmd,
        // List commands
        crate::list::LPushCmd,
        crate::list::RPushCmd,
        crate::list::LPopCmd,
        crate::list::RPopCmd,
        crate::list::LLenCmd,
        crate::list::LIndexCmd,
        crate::list::LRangeCmd,
        crate::list::LSetCmd,
        crate::list::LTrimCmd,
        crate::list::LRemCmd,
        crate::list::LPushxCmd,
        crate::list::RPushxCmd,
        crate::list::LInsertCmd,
        crate::list::RPoplpushCmd,
        // Admin commands
        crate::admin::InfoCmd,
        crate::admin::ConfigCmd,
        // Set commands
        crate::sadd::SaddCmd,
        crate::scard::ScardCmd,
        crate::sdiff::SdiffCmd,
        crate::sdiffstore::SdiffstoreCmd,
        crate::sinter::SinterCmd,
        crate::sinterstore::SinterstoreCmd,
        crate::sismember::SismemberCmd,
        crate::smembers::SmembersCmd,
        crate::smismember::SmismemberCmd,
        crate::smove::SmoveCmd,
        crate::spop::SpopCmd,
        crate::srandmember::SrandmemberCmd,
        crate::srem::SremCmd,
        crate::sscan::SscanCmd,
        crate::sunion::SunionCmd,
        crate::sunionstore::SunionstoreCmd,
        // ZSet commands
        crate::zadd::ZaddCmd,
        crate::zcard::ZcardCmd,
        crate::zcount::ZcountCmd,
        crate::zincrby::ZincrbyCmd,
        crate::zinterstore::ZinterstoreCmd,
        crate::zlexcount::ZlexcountCmd,
        crate::zmscore::ZmscoreCmd,
        crate::zrange::ZrangeCmd,
        crate::zrangebylex::ZrangebylexCmd,
        crate::zrangebyscore::ZrangebyscoreCmd,
        crate::zrank::ZrankCmd,
        crate::zrem::ZremCmd,
        crate::zremrangebylex::ZremrangebylexCmd,
        crate::zremrangebyrank::ZremrangebyrankCmd,
        crate::zremrangebyscore::ZremrangebyscoreCmd,
        crate::zrevrange::ZrevrangeCmd,
        crate::zrevrangebyscore::ZrevrangebyscoreCmd,
        crate::zrevrank::ZrevrankCmd,
        crate::zscan::ZscanCmd,
        crate::zscore::ZscoreCmd,
        crate::zunionstore::ZunionstoreCmd,
        // connection commands
        crate::ping::PingCmd,
    );

    // FLUSHDB/FLUSHALL are gated: rejected in cluster mode unless
    // `cluster-flush-enabled` restores the legacy behavior.
    let flush_cmds: Vec<Arc<dyn Cmd>> = vec![
        Arc::new(crate::flushdb::FlushdbCmd::new()),
        Arc::new(crate::flushall::FlushallCmd::new()),
    ];
    register_gated_cmds(
        &mut cmd_table,
        flush_cmds,
        gates.cluster_flush_allowed,
        |meta| {
            format!(
                "ERR {} is not supported in cluster mode yet",
                meta.name.to_uppercase()
            )
        },
    );

    // Vector Set commands are gated behind `vector-enabled`, and additionally
    // rejected in cluster mode until the Raft apply-correctness contract
    // (PR0) lands: physical binlog replay cannot re-encode member keys with
    // the local storage incarnation, so cluster vector writes would be
    // unreadable after a leader failover. `vector-cluster-enabled` restores
    // the pre-gate behavior for development.
    let vector_cmds: Vec<Arc<dyn Cmd>> = vec![
        Arc::new(crate::vector::VAddCmd::new()),
        Arc::new(crate::vector::VSimCmd::new()),
        Arc::new(crate::vector::VRemCmd::new()),
        Arc::new(crate::vector::VCardCmd::new()),
        Arc::new(crate::vector::VDimCmd::new()),
        Arc::new(crate::vector::VEmbCmd::new()),
        Arc::new(crate::vector::VInfoCmd::new()),
        Arc::new(crate::vector::VIsMemberCmd::new()),
    ];
    let vector_cmds = wrap_gated_cmds(vector_cmds, gates.vector_cluster_allowed, |_| {
        "ERR vector commands are not supported in cluster mode yet".to_string()
    });
    register_gated_cmds(&mut cmd_table, vector_cmds, gates.vector_enabled, |_| {
        "ERR vector support is disabled (vector-enabled=false)".to_string()
    });

    // AuthCmd and HelloCmd require the requirepass provider for authentication.
    {
        let auth_cmd = crate::auth::AuthCmd::new(Arc::clone(&requirepass_provider));
        let cmd_name = auth_cmd.meta().name.clone();
        cmd_table.insert(cmd_name, Arc::new(auth_cmd));
    }
    {
        let hello_cmd = crate::hello::HelloCmd::new(requirepass_provider);
        cmd_table.insert(hello_cmd.meta().name.clone(), Arc::new(hello_cmd));
    }

    register_group_cmd!(
        cmd_table,
        crate::group_client::new_client_group_cmd,
        // TODO: add more group commands...
    );

    cmd_table
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use client::{Client, StreamTrait};
    use resp::RespData;
    use storage::storage::Storage;

    use crate::auth::{RequirepassProvider, no_requirepass_provider};

    use super::{
        CmdTable, CommandTableGates, create_command_table, create_command_table_with_gates,
    };

    #[test]
    fn registers_substr_but_not_touch_until_access_metadata_exists() {
        let table = create_command_table(no_requirepass_provider());

        assert!(table.contains_key("substr"));
        assert!(!table.contains_key("touch"));
    }

    struct TestStream;

    #[async_trait::async_trait]
    impl StreamTrait for TestStream {
        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }

        async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }
    }

    fn test_requirepass_provider(password: Option<&str>) -> RequirepassProvider {
        let password = password.map(str::to_owned);
        Arc::new(move || password.clone())
    }

    fn run_command(table: &CmdTable, name: &str, argv: &[Vec<u8>]) -> RespData {
        let command = table.get(name).expect("command should be registered");
        let client = Client::new(Box::new(TestStream));
        client.set_cmd_name(name.as_bytes());
        client.set_argv(argv);
        command.execute(&client, Arc::new(Storage::new(1, 0)));
        client.take_reply()
    }

    fn error_text(reply: &RespData) -> String {
        match reply {
            RespData::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("expected error reply, got {other:?}"),
        }
    }

    #[test]
    fn hello_command_returns_resp3_handshake() {
        let table = create_command_table(no_requirepass_provider());
        let command = table.get("hello").expect("HELLO should be registered");
        let client = Client::new(Box::new(TestStream));
        client.set_cmd_name(b"hello");
        client.set_argv(&[b"hello".to_vec(), b"3".to_vec()]);

        command.execute(&client, Arc::new(Storage::new(1, 0)));

        assert_eq!(
            client.take_reply(),
            RespData::Map(vec![
                (
                    RespData::BulkString(Some(Bytes::from("server"))),
                    RespData::BulkString(Some(Bytes::from("kiwi"))),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("version"))),
                    RespData::BulkString(Some(Bytes::from("1.0.0"))),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("proto"))),
                    RespData::Integer(3),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("id"))),
                    RespData::Integer(1),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("mode"))),
                    RespData::BulkString(Some(Bytes::from("standalone"))),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("role"))),
                    RespData::BulkString(Some(Bytes::from("master"))),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("modules"))),
                    RespData::Array(Some(vec![])),
                ),
            ])
        );
    }

    #[test]
    fn vector_commands_are_registered() {
        let table = create_command_table(no_requirepass_provider());
        for name in [
            "vadd",
            "vsim",
            "vrem",
            "vcard",
            "vdim",
            "vemb",
            "vinfo",
            "vismember",
        ] {
            assert!(table.contains_key(name), "{name} should be registered");
        }
    }

    #[test]
    fn vector_commands_are_rejected_when_disabled() {
        let table = create_command_table_with_gates(
            no_requirepass_provider(),
            CommandTableGates::from_flags(false, true, true),
        );
        let argvs: [(&str, Vec<Vec<u8>>); 8] = [
            (
                "vadd",
                vec![
                    b"vadd".to_vec(),
                    b"k".to_vec(),
                    b"FP32".to_vec(),
                    vec![0; 4],
                    b"e".to_vec(),
                ],
            ),
            (
                "vsim",
                vec![
                    b"vsim".to_vec(),
                    b"k".to_vec(),
                    b"FP32".to_vec(),
                    vec![0; 4],
                ],
            ),
            ("vrem", vec![b"vrem".to_vec(), b"k".to_vec(), b"e".to_vec()]),
            ("vcard", vec![b"vcard".to_vec(), b"k".to_vec()]),
            ("vdim", vec![b"vdim".to_vec(), b"k".to_vec()]),
            ("vemb", vec![b"vemb".to_vec(), b"k".to_vec(), b"e".to_vec()]),
            ("vinfo", vec![b"vinfo".to_vec(), b"k".to_vec()]),
            (
                "vismember",
                vec![b"vismember".to_vec(), b"k".to_vec(), b"e".to_vec()],
            ),
        ];
        for (name, argv) in argvs {
            let reply = run_command(&table, name, &argv);
            assert_eq!(
                error_text(&reply),
                "ERR vector support is disabled (vector-enabled=false)",
                "{name} should be rejected when vector-enabled=false"
            );
        }
    }

    #[test]
    fn vector_feature_gate_precedes_cluster_gate() {
        let table = create_command_table_with_gates(
            no_requirepass_provider(),
            CommandTableGates::from_flags(false, false, true),
        );
        let reply = run_command(&table, "vcard", &[b"vcard".to_vec(), b"k".to_vec()]);
        assert_eq!(
            error_text(&reply),
            "ERR vector support is disabled (vector-enabled=false)"
        );
    }

    #[test]
    fn vector_commands_pass_gate_when_enabled() {
        let table = create_command_table_with_gates(
            no_requirepass_provider(),
            CommandTableGates::from_flags(true, true, true),
        );
        // Malformed vector spec: parsing fails before storage is touched, so
        // reaching this error proves the command passed the gate.
        let reply = run_command(
            &table,
            "vadd",
            &[
                b"vadd".to_vec(),
                b"k".to_vec(),
                b"VALUES".to_vec(),
                b"2".to_vec(),
                b"1.0".to_vec(),
                b"e".to_vec(),
                b"NOQUANT".to_vec(),
            ],
        );
        assert_eq!(error_text(&reply), "ERR invalid vector specification");
    }

    #[test]
    fn vector_commands_are_rejected_when_cluster_gate_disallows() {
        let table = create_command_table_with_gates(
            no_requirepass_provider(),
            CommandTableGates::from_flags(true, false, true),
        );
        let reply = run_command(&table, "vcard", &[b"vcard".to_vec(), b"k".to_vec()]);
        assert_eq!(
            error_text(&reply),
            "ERR vector commands are not supported in cluster mode yet"
        );
        let reply = run_command(
            &table,
            "vadd",
            &[
                b"vadd".to_vec(),
                b"k".to_vec(),
                b"VALUES".to_vec(),
                b"1".to_vec(),
                b"1.0".to_vec(),
                b"e".to_vec(),
                b"NOQUANT".to_vec(),
            ],
        );
        assert_eq!(
            error_text(&reply),
            "ERR vector commands are not supported in cluster mode yet"
        );
    }

    #[test]
    fn info_vector_section_reports_flat_index_and_metrics() {
        let table = create_command_table(no_requirepass_provider());

        let reply = run_command(&table, "info", &[b"info".to_vec(), b"vector".to_vec()]);
        let RespData::BulkString(Some(body)) = reply else {
            panic!("INFO VECTOR must return a bulk string");
        };
        let body = String::from_utf8(body.to_vec()).expect("utf8 info");
        assert!(body.starts_with("# Vector\r\n"));
        assert!(body.contains("index-kind:flat\r\n"));
        assert!(body.contains("vector_flat_queries_total:0\r\n"));
        assert!(body.contains("vector_flat_query_timeouts_total:0\r\n"));
        assert!(body.contains("vector_flat_query_errors_total:0\r\n"));
        assert!(body.contains("vector_search_capacity_rejected_total:0\r\n"));
        assert!(body.contains("vector_flat_query_duration_micros_total:0\r\n"));
        assert!(body.contains("vector_flat_query_duration_count:0\r\n"));

        let reply = run_command(&table, "info", &[b"info".to_vec()]);
        let RespData::BulkString(Some(body)) = reply else {
            panic!("INFO must return a bulk string");
        };
        let body = String::from_utf8(body.to_vec()).expect("utf8 info");
        assert!(
            body.contains("# Vector\r\n"),
            "full INFO must include the Vector section"
        );
    }

    #[test]
    fn flush_commands_are_rejected_when_cluster_gate_disallows() {
        let table = create_command_table_with_gates(
            no_requirepass_provider(),
            CommandTableGates::from_flags(true, true, false),
        );
        let reply = run_command(&table, "flushdb", &[b"flushdb".to_vec()]);
        assert_eq!(
            error_text(&reply),
            "ERR FLUSHDB is not supported in cluster mode yet"
        );
        let reply = run_command(&table, "flushall", &[b"flushall".to_vec()]);
        assert_eq!(
            error_text(&reply),
            "ERR FLUSHALL is not supported in cluster mode yet"
        );
    }

    #[test]
    fn flush_commands_execute_when_gate_allows() {
        let table = create_command_table_with_gates(
            no_requirepass_provider(),
            CommandTableGates::from_flags(true, true, true),
        );
        let reply = run_command(&table, "flushdb", &[b"flushdb".to_vec()]);
        assert!(
            matches!(reply, RespData::SimpleString(ref s) if s.as_ref() == b"OK"),
            "flushdb should run when the gate allows it, got {reply:?}"
        );
    }

    #[test]
    fn flush_commands_execute_with_default_gates() {
        // Default gates model standalone mode: nothing is blocked.
        let table = create_command_table(no_requirepass_provider());
        let reply = run_command(&table, "flushdb", &[b"flushdb".to_vec()]);
        assert!(
            matches!(reply, RespData::SimpleString(ref s) if s.as_ref() == b"OK"),
            "standalone flushdb should be unaffected, got {reply:?}"
        );
    }

    #[test]
    fn hello_bare_with_requirepass_returns_noauth() {
        let table = create_command_table(test_requirepass_provider(Some("secret")));
        let command = table.get("hello").expect("HELLO should be registered");
        let client = Client::new(Box::new(TestStream));
        client.set_cmd_name(b"hello");
        client.set_argv(&[b"hello".to_vec(), b"3".to_vec()]);

        command.execute(&client, Arc::new(Storage::new(1, 0)));

        assert!(!client.is_authenticated());
        let reply = client.take_reply();
        assert!(
            matches!(reply, RespData::Error(ref e) if String::from_utf8_lossy(e).contains("NOAUTH")),
            "expected NOAUTH error, got {:?}",
            reply
        );
    }

    #[test]
    fn hello_setname_sets_client_name() {
        let table = create_command_table(no_requirepass_provider());
        let command = table.get("hello").expect("HELLO should be registered");
        let client = Client::new(Box::new(TestStream));
        client.set_cmd_name(b"hello");
        client.set_argv(&[
            b"hello".to_vec(),
            b"2".to_vec(),
            b"SETNAME".to_vec(),
            b"my-client".to_vec(),
        ]);

        command.execute(&client, Arc::new(Storage::new(1, 0)));

        assert_eq!(
            client.name().as_ref(),
            b"my-client",
            "expected SETNAME to set the client name"
        );
    }

    #[test]
    fn hello_auth_with_correct_password_authenticates() {
        let table = create_command_table(test_requirepass_provider(Some("secret")));
        let command = table.get("hello").expect("HELLO should be registered");
        let client = Client::new(Box::new(TestStream));
        client.set_cmd_name(b"hello");
        client.set_argv(&[
            b"hello".to_vec(),
            b"3".to_vec(),
            b"AUTH".to_vec(),
            b"default".to_vec(),
            b"secret".to_vec(),
        ]);

        command.execute(&client, Arc::new(Storage::new(1, 0)));

        assert!(client.is_authenticated());
        let reply = client.take_reply();
        assert!(
            matches!(reply, RespData::Map(_)),
            "expected HELLO handshake map, got {:?}",
            reply
        );
    }

    #[test]
    fn hello_auth_with_wrong_password_returns_wrongpass() {
        let table = create_command_table(test_requirepass_provider(Some("secret")));
        let command = table.get("hello").expect("HELLO should be registered");
        let client = Client::new(Box::new(TestStream));
        client.set_cmd_name(b"hello");
        client.set_argv(&[
            b"hello".to_vec(),
            b"3".to_vec(),
            b"AUTH".to_vec(),
            b"default".to_vec(),
            b"wrong".to_vec(),
        ]);

        command.execute(&client, Arc::new(Storage::new(1, 0)));

        assert!(!client.is_authenticated());
        let reply = client.take_reply();
        assert!(
            matches!(reply, RespData::Error(ref e) if String::from_utf8_lossy(e).contains("WRONGPASS")),
            "expected WRONGPASS error, got {:?}",
            reply
        );
    }

    #[test]
    fn hello_auth_without_requirepass_returns_error() {
        let table = create_command_table(no_requirepass_provider());
        let command = table.get("hello").expect("HELLO should be registered");
        let client = Client::new(Box::new(TestStream));
        client.set_cmd_name(b"hello");
        client.set_argv(&[
            b"hello".to_vec(),
            b"3".to_vec(),
            b"AUTH".to_vec(),
            b"default".to_vec(),
            b"anything".to_vec(),
        ]);

        command.execute(&client, Arc::new(Storage::new(1, 0)));

        assert!(!client.is_authenticated());
        let reply = client.take_reply();
        assert!(
            matches!(reply, RespData::Error(ref e) if String::from_utf8_lossy(e).contains("HELLO AUTH called without")),
            "expected no-password-configured error, got {:?}",
            reply
        );
    }
}
