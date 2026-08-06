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

use bytes::Bytes;
use client::Client;
use resp::RespData;
use storage::{VectorInfo, storage::Storage};

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

use super::{MissingError, integer_reply, storage_error_reply};

crate::define_vector_command!(
    VInfoCmd,
    "vinfo",
    2, // VINFO key
    CmdFlags::READONLY | CmdFlags::FAST,
    AclCategory::KEYSPACE | AclCategory::READ
);

/// Phase 1 FLAT reply: nine Redis-compatible fields, no Kiwi-private extras
/// (private diagnostics go to INFO VECTOR). HNSW fields carry their FLAT
/// sentinel values; the encoder degrades the map to a flat array on RESP2.
fn vinfo_reply(info: &VectorInfo) -> RespData {
    let field = |name: &'static str, value: RespData| {
        (
            RespData::BulkString(Some(Bytes::from_static(name.as_bytes()))),
            value,
        )
    };
    let integer = |value: u64| integer_reply(value);
    RespData::Map(vec![
        field(
            "quant-type",
            RespData::BulkString(Some(Bytes::from_static(b"fp32"))),
        ),
        field("hnsw-m", RespData::Integer(0)),
        field("vector-dim", integer(u64::from(info.dimension))),
        field("projection-input-dim", RespData::Integer(0)),
        field("size", integer(info.size)),
        field("max-level", RespData::Integer(0)),
        field("attributes-count", RespData::Integer(0)),
        field("vset-uid", integer(info.generation)),
        field("hnsw-max-node-uid", RespData::Integer(0)),
    ])
}

impl Cmd for VInfoCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        super::set_command_key(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let reply = match storage.vinfo(&client.key()) {
            // Redis null array for a missing key (RESP2 *-1, RESP3 _).
            Ok(Some(info)) => vinfo_reply(&info),
            Ok(None) => RespData::Array(None),
            Err(error) => storage_error_reply(error, MissingError::Key),
        };
        client.set_reply(reply);
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vinfo_reply_has_nine_phase1_fields() {
        let reply = vinfo_reply(&VectorInfo {
            dimension: 3,
            size: 42,
            generation: 7,
        });
        let RespData::Map(pairs) = reply else {
            panic!("vinfo reply must be a map");
        };
        let fields: Vec<(String, RespData)> = pairs
            .into_iter()
            .map(|(name, value)| {
                let RespData::BulkString(Some(name)) = name else {
                    panic!("field name must be a bulk string");
                };
                (String::from_utf8(name.to_vec()).expect("utf8 name"), value)
            })
            .collect();
        assert_eq!(
            fields,
            vec![
                (
                    "quant-type".to_string(),
                    RespData::BulkString(Some(Bytes::from_static(b"fp32")))
                ),
                ("hnsw-m".to_string(), RespData::Integer(0)),
                ("vector-dim".to_string(), RespData::Integer(3)),
                ("projection-input-dim".to_string(), RespData::Integer(0)),
                ("size".to_string(), RespData::Integer(42)),
                ("max-level".to_string(), RespData::Integer(0)),
                ("attributes-count".to_string(), RespData::Integer(0)),
                ("vset-uid".to_string(), RespData::Integer(7)),
                ("hnsw-max-node-uid".to_string(), RespData::Integer(0)),
            ]
        );
    }
}
