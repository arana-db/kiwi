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

use std::sync::Arc;

use client::Client;
use resp::{RespData, RespVersion};
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct ZmscoreCmd {
    meta: CmdMeta,
}

impl ZmscoreCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "zmscore".to_string(),
                arity: -3, // ZMSCORE key member [member ...]
                flags: CmdFlags::READONLY | CmdFlags::FAST,
                acl_category: AclCategory::READ | AclCategory::SORTEDSET,
                ..Default::default()
            },
        }
    }
}

impl Cmd for ZmscoreCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        let key = argv[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();
        let argv = client.argv();
        let members = argv[2..].iter().map(Vec::as_slice).collect::<Vec<_>>();

        match storage.zmscore(&key, &members) {
            Ok(scores) => {
                let mut replies = Vec::with_capacity(scores.len());
                for score in scores {
                    let reply = match (score, client.resp_version()) {
                        (Some(score), RespVersion::RESP3) => {
                            let Some(score) = std::str::from_utf8(&score)
                                .ok()
                                .and_then(|score| score.parse::<f64>().ok())
                            else {
                                client.set_reply(RespData::Error(
                                    "ERR invalid score format".to_string().into(),
                                ));
                                return;
                            };
                            RespData::Double(score)
                        }
                        (Some(score), RespVersion::RESP1 | RespVersion::RESP2) => {
                            RespData::BulkString(Some(score.into()))
                        }
                        (None, _) => RespData::BulkString(None),
                    };
                    replies.push(reply);
                }
                client.set_reply(RespData::Array(Some(replies)));
            }
            Err(error) => {
                let message = error.to_string();
                let message = if message.starts_with("WRONGTYPE") {
                    message
                } else {
                    format!("ERR {message}")
                };
                client.set_reply(RespData::Error(message.into()));
            }
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use client::StreamTrait;
    use resp::{RespEncode, RespVersion, encode::RespEncoder};
    use storage::{StorageOptions, ZsetScoreMember, safe_cleanup_test_db, unique_test_db_path};

    use super::*;

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

    fn encode(reply: &RespData, version: RespVersion) -> Vec<u8> {
        let mut encoder = RespEncoder::new(version);
        encoder.encode_resp_data(reply);
        encoder.get_response().to_vec()
    }

    fn open_storage() -> (std::path::PathBuf, Arc<Storage>) {
        let db_path = unique_test_db_path();
        safe_cleanup_test_db(&db_path);
        let mut storage = Storage::new(1, 0);
        let _bg_task_rx = storage
            .open(Arc::new(StorageOptions::default()), &db_path)
            .unwrap();
        (db_path, Arc::new(storage))
    }

    fn run_zmscore(client: &Client, storage: &Arc<Storage>, members: &[Vec<u8>]) -> RespData {
        let mut argv = vec![b"zmscore".to_vec(), b"zmscore-key".to_vec()];
        argv.extend_from_slice(members);
        client.set_argv(&argv);
        let command = ZmscoreCmd::new();
        assert!(command.do_initial(client));
        command.do_cmd(client, Arc::clone(storage));
        client.take_reply()
    }

    #[test]
    fn test_zmscore_cmd_meta() {
        let cmd = ZmscoreCmd::new();
        assert_eq!(cmd.name(), "zmscore");
        assert_eq!(cmd.meta().arity, -3); // ZMSCORE key member [member ...]
        assert!(cmd.has_flag(CmdFlags::READONLY));
        assert!(cmd.has_flag(CmdFlags::FAST));
        assert!(!cmd.has_flag(CmdFlags::WRITE));
    }

    #[test]
    fn test_zmscore_cmd_clone() {
        let cmd = ZmscoreCmd::new();
        let cloned = cmd.clone_box();
        assert_eq!(cloned.name(), cmd.name());
        assert_eq!(cloned.meta().arity, cmd.meta().arity);
    }

    #[test]
    fn test_zmscore_acl_category() {
        let cmd = ZmscoreCmd::new();
        assert!(cmd.acl_category().contains(AclCategory::SORTEDSET));
        assert!(cmd.acl_category().contains(AclCategory::READ));
    }

    #[test]
    fn test_zmscore_argument_validation() {
        let cmd = ZmscoreCmd::new();

        // Valid: command + key + one or more members
        assert!(cmd.check_arg(3)); // ZMSCORE key member
        assert!(cmd.check_arg(4)); // ZMSCORE key member member
        assert!(cmd.check_arg(10));

        // Invalid: missing key and/or member
        assert!(!cmd.check_arg(2)); // Missing member
        assert!(!cmd.check_arg(1)); // Missing key and member
        assert!(!cmd.check_arg(0)); // No arguments
    }

    #[tokio::test]
    async fn zmscore_raw_reply_matches_resp2_and_resp3_types_and_order() {
        let (db_path, storage) = open_storage();
        storage
            .zadd(
                b"zmscore-key",
                &[
                    ZsetScoreMember::new(1.5, b"first".to_vec()),
                    ZsetScoreMember::new(-2.25, b"binary\x00\xff".to_vec()),
                ],
            )
            .unwrap();
        let members = vec![
            b"first".to_vec(),
            b"missing".to_vec(),
            b"binary\x00\xff".to_vec(),
            b"first".to_vec(),
        ];

        let resp2_client = Client::new(Box::new(TestStream { _marker: 0 }));
        let resp2_reply = run_zmscore(&resp2_client, &storage, &members);
        assert_eq!(
            encode(&resp2_reply, RespVersion::RESP2),
            b"*4\r\n$3\r\n1.5\r\n$-1\r\n$5\r\n-2.25\r\n$3\r\n1.5\r\n"
        );

        let resp3_client = Client::new(Box::new(TestStream { _marker: 0 }));
        resp3_client.set_argv(&[b"hello".to_vec(), b"3".to_vec()]);
        crate::hello::HelloCmd::default().do_cmd(&resp3_client, Arc::clone(&storage));
        let _hello_reply = resp3_client.take_reply();
        let resp3_reply = run_zmscore(&resp3_client, &storage, &members);
        assert_eq!(
            encode(&resp3_reply, RespVersion::RESP3),
            b"*4\r\n,1.5\r\n_\r\n,-2.25\r\n,1.5\r\n"
        );

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }

    #[tokio::test]
    async fn zmscore_returns_exact_wrongtype_error_and_expired_key_nils() {
        let (db_path, storage) = open_storage();
        let client = Client::new(Box::new(TestStream { _marker: 0 }));
        storage.set(b"zmscore-key", b"not-a-zset").unwrap();

        let wrongtype = run_zmscore(&client, &storage, &[b"member".to_vec()]);
        assert_eq!(
            encode(&wrongtype, RespVersion::RESP2),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );

        storage.del(&[b"zmscore-key".to_vec()]).unwrap();
        storage
            .zadd(
                b"zmscore-key",
                &[ZsetScoreMember::new(1.0, b"member".to_vec())],
            )
            .unwrap();
        assert!(storage.pexpire(b"zmscore-key", 1).unwrap());
        std::thread::sleep(std::time::Duration::from_millis(10));
        let expired = run_zmscore(
            &client,
            &storage,
            &[b"member".to_vec(), b"missing".to_vec()],
        );
        assert_eq!(
            encode(&expired, RespVersion::RESP2),
            b"*2\r\n$-1\r\n$-1\r\n"
        );

        drop(storage);
        safe_cleanup_test_db(&db_path);
    }
}
