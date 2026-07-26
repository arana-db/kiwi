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
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

use super::{MissingError, error_reply, parse_vsim, storage_error_reply};

crate::define_vector_command!(
    VSimCmd,
    "vsim",
    -4,
    CmdFlags::READONLY,
    AclCategory::KEYSPACE | AclCategory::READ | AclCategory::SLOW
);

impl Cmd for VSimCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        super::set_command_key(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let parsed = match parse_vsim(&client.argv()) {
            Ok(parsed) => parsed,
            Err(message) => {
                client.set_reply(error_reply(message));
                return;
            }
        };
        let reply = match storage.vsim(&client.key(), parsed.query, parsed.options) {
            Ok(hits) if parsed.with_scores => RespData::Map(
                hits.into_iter()
                    .map(|hit| {
                        (
                            RespData::BulkString(Some(Bytes::from(hit.element))),
                            RespData::Double(hit.score),
                        )
                    })
                    .collect(),
            ),
            Ok(hits) => RespData::Array(Some(
                hits.into_iter()
                    .map(|hit| RespData::BulkString(Some(Bytes::from(hit.element))))
                    .collect(),
            )),
            Err(error) => storage_error_reply(error, MissingError::Element),
        };
        client.set_reply(reply);
    }
}
