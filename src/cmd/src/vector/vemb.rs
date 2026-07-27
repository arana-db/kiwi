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

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

use super::{ERR_INVALID_VECTOR, MissingError, ParseResult, error_reply, storage_error_reply};

const ERR_VEMB_RAW: &str = "ERR VEMB option RAW is not supported yet";

crate::define_vector_command!(
    VEmbCmd,
    "vemb",
    -3, // VEMB key element
    CmdFlags::READONLY | CmdFlags::FAST,
    AclCategory::KEYSPACE | AclCategory::READ
);

fn parse_vemb(argv: &[Vec<u8>]) -> ParseResult<Vec<u8>> {
    let element = argv.get(2).cloned().ok_or(ERR_INVALID_VECTOR)?;
    match &argv[3..] {
        [] => Ok(element),
        [option] if option.eq_ignore_ascii_case(b"RAW") => Err(ERR_VEMB_RAW),
        _ => Err(ERR_INVALID_VECTOR),
    }
}

impl Cmd for VEmbCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        super::set_command_key(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let element = match parse_vemb(&client.argv()) {
            Ok(element) => element,
            Err(message) => {
                client.set_reply(error_reply(message));
                return;
            }
        };
        let reply = match storage.vemb(&client.key(), &element) {
            Ok(Some(values)) => {
                RespData::Array(Some(values.into_iter().map(RespData::Double).collect()))
            }
            Ok(None) => RespData::BulkString(None),
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
    fn rejects_vemb_raw_and_unknown_trailing_options() {
        assert_eq!(
            parse_vemb(&[
                b"vemb".to_vec(),
                b"key".to_vec(),
                b"member".to_vec(),
                b"RAW".to_vec(),
            ])
            .unwrap_err(),
            ERR_VEMB_RAW
        );
        assert_eq!(
            parse_vemb(&[
                b"vemb".to_vec(),
                b"key".to_vec(),
                b"member".to_vec(),
                b"unknown".to_vec(),
            ])
            .unwrap_err(),
            ERR_INVALID_VECTOR
        );
    }
}
