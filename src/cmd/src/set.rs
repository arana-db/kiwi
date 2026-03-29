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
use resp::RespData;
use storage::storage::Storage;

use crate::{Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};
use conf::raft_type::{Binlog, BinlogEntry, ColumnFamilyIndex as RaftCfIndex, OperateType};
use storage::slot_indexer::key_to_slot_id;

#[derive(Clone, Default)]
pub struct SetCmd {
    meta: CmdMeta,
}

impl SetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "set".to_string(),
                arity: 3,
                flags: CmdFlags::WRITE | CmdFlags::RAFT,
                ..Default::default()
            },
        }
    }
}

impl Cmd for SetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    /// SET key value
    fn do_initial(&self, client: &Client) -> bool {
        // TODO: support xx, nx, ex, px
        let argv = client.argv();

        let key = argv[1].clone();
        client.set_key(&key);

        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();
        let value = &client.argv()[2];

        let result = storage.set(&key, value);

        match result {
            Ok(_) => {
                client.set_reply(RespData::SimpleString("OK".to_string().into()));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }

    fn to_binlog(&self, client: &Client) -> Option<Binlog> {
        let key = client.key();
        let value = &client.argv()[2];

        let slot_id = key_to_slot_id(&key) as u32;

        let entry = BinlogEntry {
            cf_idx: RaftCfIndex::MetaCF as u32,
            op_type: OperateType::Put,
            key: key.clone(),
            value: Some(value.clone()),
        };

        Some(Binlog {
            db_id: 0,
            slot_idx: slot_id,
            entries: vec![entry],
        })
    }
}
