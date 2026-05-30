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
use subtle::ConstantTimeEq;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

pub type RequirepassProvider = Arc<dyn Fn() -> Option<String> + Send + Sync>;

#[derive(Clone)]
pub struct AuthCmd {
    meta: CmdMeta,
    requirepass_provider: RequirepassProvider,
}

impl Default for AuthCmd {
    fn default() -> Self {
        Self {
            meta: CmdMeta {
                name: "auth".to_string(),
                arity: -2,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION,
                ..Default::default()
            },
            requirepass_provider: Arc::new(|| None),
        }
    }
}

impl AuthCmd {
    pub fn new(provider: RequirepassProvider) -> Self {
        Self {
            meta: CmdMeta {
                name: "auth".to_string(),
                arity: -2,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION,
                ..Default::default()
            },
            requirepass_provider: provider,
        }
    }
}

impl Cmd for AuthCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        let argv = client.argv();
        match argv.len() {
            2 => {
                let password = &argv[1];
                if let Some(requirepass) = (self.requirepass_provider)() {
                    // Constant-time comparison to avoid leaking information
                    // about the configured password through timing side
                    // channels. Length mismatches still short-circuit, which
                    // only reveals the password length — acceptable here.
                    let matches: bool = password.ct_eq(requirepass.as_bytes()).into();
                    if matches {
                        client.set_authenticated(true);
                        client.set_reply(RespData::SimpleString("OK".into()));
                    } else {
                        client.set_reply(RespData::Error(
                            "WRONGPASS invalid username-password pair or user is disabled.".into(),
                        ));
                    }
                } else {
                    client.set_reply(RespData::Error(
                        "ERR AUTH called without any password configured".into(),
                    ));
                }
            }
            3 => {
                // AUTH <user> <pass> — ACL authentication reserved for future
                client.set_reply(RespData::Error(
                    "ERR ACL authentication is not supported".into(),
                ));
            }
            _ => {
                client.set_reply(RespData::Error(
                    "ERR wrong number of arguments for 'auth' command".into(),
                ));
            }
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;
    use client::StreamTrait;
    use storage::storage::Storage;

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
        // `Client::new` is fail-closed: a freshly built client is unauthenticated.
        Arc::new(Client::new(Box::new(TestStream)))
    }

    fn make_storage() -> Arc<Storage> {
        Arc::new(Storage::new(1, 0))
    }

    fn auth_cmd_with(password: Option<&str>) -> AuthCmd {
        let owned = password.map(|s| s.to_string());
        AuthCmd::new(Arc::new(move || owned.clone()))
    }

    fn reply_text(client: &Client) -> String {
        match client.take_reply() {
            RespData::SimpleString(b) => format!("+{}", String::from_utf8_lossy(&b)),
            RespData::Error(b) => format!("-{}", String::from_utf8_lossy(&b)),
            other => format!("{other:?}"),
        }
    }

    #[test]
    fn fresh_client_is_unauthenticated_by_default() {
        // Guards the fail-closed default in `Client::new`. The NOAUTH gate in
        // the network handler relies on this being false until AUTH succeeds.
        let client = make_client();
        assert!(!client.is_authenticated());
    }

    #[test]
    fn wrong_password_does_not_authenticate() {
        let cmd = auth_cmd_with(Some("secret"));
        let client = make_client();
        client.set_argv(&[b"auth".to_vec(), b"wrong".to_vec()]);

        cmd.do_cmd(&client, make_storage());

        assert!(!client.is_authenticated());
        let reply = reply_text(&client);
        assert!(reply.starts_with("-WRONGPASS"), "unexpected reply: {reply}");
    }

    #[test]
    fn correct_password_authenticates_and_replies_ok() {
        let cmd = auth_cmd_with(Some("secret"));
        let client = make_client();
        client.set_argv(&[b"auth".to_vec(), b"secret".to_vec()]);

        cmd.do_cmd(&client, make_storage());

        assert!(client.is_authenticated());
        assert_eq!(reply_text(&client), "+OK");
    }

    #[test]
    fn auth_without_requirepass_returns_error() {
        let cmd = auth_cmd_with(None);
        let client = make_client();
        client.set_argv(&[b"auth".to_vec(), b"anything".to_vec()]);

        cmd.do_cmd(&client, make_storage());

        assert!(!client.is_authenticated());
        let reply = reply_text(&client);
        assert!(
            reply.starts_with("-ERR AUTH called without"),
            "unexpected reply: {reply}"
        );
    }

    #[test]
    fn auth_with_user_and_password_is_rejected() {
        // ACL form (AUTH <user> <pass>) is reserved for future support.
        let cmd = auth_cmd_with(Some("secret"));
        let client = make_client();
        client.set_argv(&[b"auth".to_vec(), b"user".to_vec(), b"secret".to_vec()]);

        cmd.do_cmd(&client, make_storage());

        assert!(!client.is_authenticated());
        let reply = reply_text(&client);
        assert!(reply.starts_with("-ERR ACL"), "unexpected reply: {reply}");
    }

    #[test]
    fn auth_carries_no_auth_flag() {
        // AUTH itself must be exempt from the NOAUTH gate, otherwise an
        // unauthenticated client could never call it.
        let cmd = AuthCmd::default();
        assert!(cmd.has_flag(CmdFlags::NO_AUTH));
    }
}
