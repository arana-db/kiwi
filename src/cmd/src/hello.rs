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
use resp::{CommandType, HelloAuthResult, RespCommand, RespData, RespError};
use storage::storage::Storage;
use subtle::ConstantTimeEq;

use crate::{
    AclCategory, Cmd, CmdFlags, CmdMeta, RequirepassProvider, impl_cmd_clone_box, impl_cmd_meta,
};

#[derive(Clone)]
pub struct HelloCmd {
    meta: CmdMeta,
    requirepass_provider: RequirepassProvider,
}

impl Default for HelloCmd {
    fn default() -> Self {
        Self {
            meta: CmdMeta {
                name: "hello".to_string(),
                arity: -1,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION | AclCategory::FAST,
                ..Default::default()
            },
            requirepass_provider: Arc::new(|| None),
        }
    }
}

impl HelloCmd {
    pub fn new(provider: RequirepassProvider) -> Self {
        Self {
            meta: CmdMeta {
                name: "hello".to_string(),
                arity: -1,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION | AclCategory::FAST,
                ..Default::default()
            },
            requirepass_provider: provider,
        }
    }
}

impl Cmd for HelloCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        let argv = client.argv();
        let command = RespCommand {
            command_type: CommandType::Hello,
            args: argv.into_iter().skip(1).map(Bytes::from).collect(),
            is_pipeline: false,
        };

        let mut auth_attempted = false;
        let provider = Arc::clone(&self.requirepass_provider);
        let mut authenticate = |password: &[u8]| -> HelloAuthResult {
            auth_attempted = true;
            match provider() {
                Some(requirepass) => {
                    let matches: bool = password.ct_eq(requirepass.as_bytes()).into();
                    if matches {
                        HelloAuthResult::Authenticated
                    } else {
                        HelloAuthResult::WrongPassword
                    }
                }
                None => HelloAuthResult::NoPasswordConfigured,
            }
        };

        match client.handle_hello(&command, &mut authenticate) {
            Ok(response) => {
                // Only mark the client as authenticated when the HELLO command
                // included an AUTH clause that succeeded. A bare HELLO must not
                // bypass the requirepass check.
                if auth_attempted {
                    client.set_authenticated(true);
                }
                client.set_reply(response);
            }
            Err(err) => client.set_reply(RespData::Error(format_hello_error(err).into())),
        }
    }
}

/// Format a RESP error from HELLO negotiation for the client.
///
/// `RespError::InvalidData` is used to carry the raw Redis error message, so
/// strip the Display prefix and any leading `-` that the encoder will add.
fn format_hello_error(err: RespError) -> String {
    let raw = match err {
        RespError::InvalidData(msg) => msg,
        other => other.to_string(),
    };
    raw.trim_start_matches('-').to_string()
}
