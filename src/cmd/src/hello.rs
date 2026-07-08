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

        let provider = Arc::clone(&self.requirepass_provider);
        let authentication_required = provider().is_some();
        let mut authenticate = |username: &[u8], password: &[u8]| -> HelloAuthResult {
            // ACL authentication is not supported yet; only the default user
            // is valid, matching Redis behavior when a single requirepass is
            // configured.
            if username != b"default" {
                return HelloAuthResult::WrongPassword;
            }
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

        match client.handle_hello(
            &command,
            client.is_authenticated(),
            authentication_required,
            &mut authenticate,
        ) {
            Ok((response, _)) => {
                // handle_hello only succeeds when the client is already
                // authenticated or the HELLO included an AUTH clause that
                // succeeded. In both cases the connection is authenticated.
                client.set_authenticated(true);
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

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use client::StreamTrait;

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

    fn make_client() -> Arc<Client> {
        Arc::new(Client::new(Box::new(TestStream)))
    }

    fn make_storage() -> Arc<Storage> {
        Arc::new(Storage::new(1, 0))
    }

    fn hello_cmd_with(password: Option<&str>) -> HelloCmd {
        let owned = password.map(|s| s.to_string());
        HelloCmd::new(Arc::new(move || owned.clone()))
    }

    fn reply_is_error(client: &Client) -> Option<String> {
        match client.take_reply() {
            RespData::Error(b) => Some(String::from_utf8_lossy(&b).to_string()),
            _ => None,
        }
    }

    #[test]
    fn hello_auth_default_user_succeeds() {
        let cmd = hello_cmd_with(Some("secret"));
        let client = make_client();
        client.set_argv(&[
            b"hello".to_vec(),
            b"3".to_vec(),
            b"AUTH".to_vec(),
            b"default".to_vec(),
            b"secret".to_vec(),
        ]);

        cmd.do_cmd(&client, make_storage());

        assert!(client.is_authenticated());
        assert!(
            reply_is_error(&client).is_none(),
            "expected a successful HELLO response"
        );
    }

    #[test]
    fn hello_auth_non_default_user_is_rejected() {
        let cmd = hello_cmd_with(Some("secret"));
        let client = make_client();
        client.set_argv(&[
            b"hello".to_vec(),
            b"3".to_vec(),
            b"AUTH".to_vec(),
            b"other".to_vec(),
            b"secret".to_vec(),
        ]);

        cmd.do_cmd(&client, make_storage());

        assert!(!client.is_authenticated());
        let err = reply_is_error(&client).expect("expected an error reply");
        assert!(err.starts_with("WRONGPASS"), "unexpected reply: {err}");
    }

    #[test]
    fn hello_setname_sets_client_name() {
        let cmd = hello_cmd_with(None);
        let client = make_client();
        client.set_argv(&[
            b"hello".to_vec(),
            b"2".to_vec(),
            b"SETNAME".to_vec(),
            b"my-client".to_vec(),
        ]);

        cmd.do_cmd(&client, make_storage());

        assert_eq!(client.name().as_slice(), b"my-client");
    }
}
