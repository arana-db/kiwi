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

use bytes::Bytes;

use crate::{
    command::RespCommand,
    error::{RespError, RespResult},
    types::{RespData, RespVersion},
};

/// Result of authenticating via the `HELLO ... AUTH username password` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelloAuthResult {
    /// Password matched the configured requirepass; client may be authenticated.
    Authenticated,
    /// Password did not match; client should be rejected with WRONGPASS.
    WrongPassword,
    /// No requirepass is configured; AUTH clause is not allowed.
    NoPasswordConfigured,
}

/// Protocol negotiation handler for RESP3
#[derive(Clone)]
pub struct ProtocolNegotiator {
    current_version: RespVersion,
    client_capabilities: HashMap<String, String>,
}

impl Default for ProtocolNegotiator {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolNegotiator {
    pub fn new() -> Self {
        Self {
            current_version: RespVersion::RESP2,
            client_capabilities: HashMap::new(),
        }
    }

    pub fn current_version(&self) -> RespVersion {
        self.current_version
    }

    pub fn client_capabilities(&self) -> &HashMap<String, String> {
        &self.client_capabilities
    }

    /// Handle HELLO command for protocol negotiation.
    ///
    /// `authentication_required` reflects whether the server is configured
    /// with a requirepass password. When true, a client must already be
    /// authenticated or authenticate via the HELLO ... AUTH clause to receive
    /// the HELLO handshake. When false, the NOAUTH check is skipped.
    ///
    /// `authenticate` is called with the `username` and `password` supplied via
    /// the AUTH clause. It must report whether the client should be
    /// authenticated, rejected for a wrong username/password pair, or rejected
    /// because no password is configured.
    pub fn handle_hello<F>(
        &mut self,
        command: &RespCommand,
        already_authenticated: bool,
        authentication_required: bool,
        mut authenticate: F,
    ) -> RespResult<(RespData, Option<String>)>
    where
        F: FnMut(&[u8], &[u8]) -> HelloAuthResult,
    {
        // HELLO [protover [AUTH username password] [SETNAME clientname]]
        let mut args_iter = command.args.iter().peekable();

        // Parse protocol version if the first arg is "2" or "3"
        let requested_version = if let Some(first) = args_iter.peek() {
            let s = std::str::from_utf8(first)
                .map_err(|_| RespError::InvalidData("Invalid protocol version".to_string()))?;
            match s {
                "2" => {
                    args_iter.next();
                    Some(RespVersion::RESP2)
                }
                "3" => {
                    args_iter.next();
                    Some(RespVersion::RESP3)
                }
                _ => None, // Not a version; proceed to parse AUTH/SETNAME
            }
        } else {
            None
        };

        // Determine the version that will be used for this response. The
        // connection's stored version is only updated after all checks pass.
        let response_version = requested_version.unwrap_or(self.current_version);

        let mut auth_attempted = false;

        let mut pending_set_name: Option<String> = None;

        // Parse additional arguments (AUTH, SETNAME, etc.)
        while let Some(arg) = args_iter.next() {
            let arg_str = std::str::from_utf8(arg)
                .map_err(|_| RespError::InvalidData("Invalid argument".to_string()))?
                .to_uppercase();

            match arg_str.as_str() {
                "AUTH" => {
                    // AUTH username password
                    let username = args_iter.next().ok_or_else(|| {
                        RespError::InvalidData("AUTH requires username".to_string())
                    })?;
                    let password = args_iter.next().ok_or_else(|| {
                        RespError::InvalidData("AUTH requires password".to_string())
                    })?;

                    auth_attempted = true;
                    match authenticate(username.as_ref(), password.as_ref()) {
                        HelloAuthResult::Authenticated => {}
                        HelloAuthResult::WrongPassword => {
                            return Err(RespError::InvalidData(
                                "WRONGPASS invalid username-password pair or user is disabled."
                                    .to_string(),
                            ));
                        }
                        HelloAuthResult::NoPasswordConfigured => {
                            return Err(RespError::InvalidData(
                                "ERR HELLO AUTH called without any password configured".to_string(),
                            ));
                        }
                    }
                }
                "SETNAME" => {
                    // SETNAME clientname
                    let client_name = args_iter.next().ok_or_else(|| {
                        RespError::InvalidData("SETNAME requires client name".to_string())
                    })?;
                    let client_name_str = std::str::from_utf8(client_name)
                        .map_err(|_| RespError::InvalidData("Invalid client name".to_string()))?;
                    pending_set_name = Some(client_name_str.to_string());
                }
                _ => {
                    return Err(RespError::InvalidData(format!(
                        "Unknown HELLO argument: {}",
                        arg_str
                    )));
                }
            }
        }

        // Redis requires the client to be authenticated before returning the
        // HELLO handshake when a password is configured. An AUTH clause that
        // succeeds satisfies this, as does a connection that was already
        // authenticated.
        if authentication_required && !auth_attempted && !already_authenticated {
            return Err(RespError::InvalidData(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time".to_string(),
            ));
        }

        // All checks passed: commit the negotiated version and persist any SETNAME
        // from this command.
        self.current_version = response_version;
        if let Some(name) = pending_set_name.as_ref() {
            self.client_capabilities
                .insert("client_name".to_string(), name.clone());
        }

        match self.current_version {
            RespVersion::RESP2 => self
                .build_resp2_hello_response()
                .map(|data| (data, pending_set_name)),
            RespVersion::RESP3 => self
                .build_resp3_hello_response()
                .map(|data| (data, pending_set_name)),
            _ => self
                .build_resp2_hello_response()
                .map(|data| (data, pending_set_name)), // Fallback to RESP2
        }
    }

    fn build_resp2_hello_response(&self) -> RespResult<RespData> {
        // RESP2 response format (array of key-value pairs)
        let response = vec![
            RespData::BulkString(Some(Bytes::from("server"))),
            RespData::BulkString(Some(Bytes::from("kiwi"))),
            RespData::BulkString(Some(Bytes::from("version"))),
            RespData::BulkString(Some(Bytes::from("1.0.0"))),
            RespData::BulkString(Some(Bytes::from("proto"))),
            RespData::Integer(2),
            RespData::BulkString(Some(Bytes::from("id"))),
            RespData::Integer(1),
            RespData::BulkString(Some(Bytes::from("mode"))),
            RespData::BulkString(Some(Bytes::from("standalone"))),
            RespData::BulkString(Some(Bytes::from("role"))),
            RespData::BulkString(Some(Bytes::from("master"))),
            RespData::BulkString(Some(Bytes::from("modules"))),
            RespData::Array(Some(vec![])),
        ];

        Ok(RespData::Array(Some(response)))
    }

    fn build_resp3_hello_response(&self) -> RespResult<RespData> {
        // RESP3 response format (map)
        let pairs = vec![
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
        ];

        Ok(RespData::Map(pairs))
    }

    /// Check if the current protocol supports a specific feature
    pub fn supports_feature(&self, feature: &str) -> bool {
        matches!(
            (self.current_version, feature),
            (RespVersion::RESP3, "maps")
                | (RespVersion::RESP3, "sets")
                | (RespVersion::RESP3, "booleans")
                | (RespVersion::RESP3, "doubles")
                | (RespVersion::RESP3, "big_numbers")
                | (RespVersion::RESP3, "verbatim_strings")
                | (RespVersion::RESP3, "push_messages")
        )
    }

    /// Gracefully fallback to RESP2 if needed
    pub fn fallback_to_resp2(&mut self) -> RespVersion {
        let old_version = self.current_version;
        self.current_version = RespVersion::RESP2;
        old_version
    }

    /// Convert RESP3 data to RESP2 compatible format for fallback
    pub fn convert_to_resp2(data: &RespData) -> RespData {
        match data {
            // RESP3 specific types that need conversion
            RespData::Null => RespData::BulkString(None),
            RespData::Boolean(b) => RespData::Integer(if *b { 1 } else { 0 }),
            RespData::Double(d) => RespData::BulkString(Some(Bytes::from(d.to_string()))),
            RespData::BigNumber(bytes) => RespData::BulkString(Some(bytes.clone())),
            RespData::BulkError(bytes) => RespData::Error(bytes.clone()),
            RespData::VerbatimString { data, .. } => RespData::BulkString(Some(data.clone())),
            RespData::Map(pairs) => {
                // Convert map to array of alternating key-value pairs
                let mut array = Vec::with_capacity(pairs.len() * 2);
                for (key, value) in pairs {
                    array.push(Self::convert_to_resp2(key));
                    array.push(Self::convert_to_resp2(value));
                }
                RespData::Array(Some(array))
            }
            RespData::Set(items) => {
                // Convert set to array
                let converted_items: Vec<_> = items.iter().map(Self::convert_to_resp2).collect();
                RespData::Array(Some(converted_items))
            }
            RespData::Push(items) => {
                // Convert push to array
                let converted_items: Vec<_> = items.iter().map(Self::convert_to_resp2).collect();
                RespData::Array(Some(converted_items))
            }
            // RESP2 compatible types - return as-is
            RespData::SimpleString(_)
            | RespData::Error(_)
            | RespData::Integer(_)
            | RespData::BulkString(_) => data.clone(),
            RespData::Array(Some(items)) => {
                // Recursively convert array items
                let converted_items: Vec<_> = items.iter().map(Self::convert_to_resp2).collect();
                RespData::Array(Some(converted_items))
            }
            RespData::Array(None) => data.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandType;

    #[test]
    fn test_hello_command_resp2() {
        let mut negotiator = ProtocolNegotiator::new();
        let command = RespCommand::new(CommandType::Hello, vec![Bytes::from("2")], false);

        let response = negotiator
            .handle_hello(&command, true, false, |_, _| HelloAuthResult::Authenticated)
            .expect("handle_hello should succeed")
            .0;
        assert_eq!(negotiator.current_version(), RespVersion::RESP2);

        if let RespData::Array(Some(items)) = response {
            assert!(items.len() >= 12); // At least the basic server info
        } else {
            panic!("Expected array response for RESP2");
        }
    }

    #[test]
    fn test_hello_command_resp3() {
        let mut negotiator = ProtocolNegotiator::new();
        let command = RespCommand::new(CommandType::Hello, vec![Bytes::from("3")], false);

        let response = negotiator
            .handle_hello(&command, true, false, |_, _| HelloAuthResult::Authenticated)
            .expect("handle_hello should succeed")
            .0;
        assert_eq!(negotiator.current_version(), RespVersion::RESP3);

        if let RespData::Map(pairs) = response {
            assert!(pairs.len() >= 6); // At least the basic server info
        } else {
            panic!("Expected map response for RESP3");
        }
    }

    #[test]
    fn test_hello_noauth_when_unauthenticated() {
        let mut negotiator = ProtocolNegotiator::new();
        let command = RespCommand::new(CommandType::Hello, vec![Bytes::from("3")], false);

        let result = negotiator.handle_hello(&command, false, true, |_, _| {
            panic!("authenticate should not be called without AUTH")
        });

        assert!(result.is_err());
        let err = result
            .expect_err("handle_hello should fail for unauthenticated client")
            .to_string();
        assert!(err.contains("NOAUTH"), "expected NOAUTH error, got {err}");
        // Version must not switch when the command is rejected.
        assert_eq!(negotiator.current_version(), RespVersion::RESP2);
    }

    #[test]
    fn test_feature_support() {
        let mut negotiator = ProtocolNegotiator::new();

        // RESP2 doesn't support RESP3 features
        assert!(!negotiator.supports_feature("maps"));

        // Switch to RESP3
        let command = RespCommand::new(CommandType::Hello, vec![Bytes::from("3")], false);
        negotiator
            .handle_hello(&command, true, false, |_, _| HelloAuthResult::Authenticated)
            .expect("handle_hello should succeed");

        // RESP3 supports new features
        assert!(negotiator.supports_feature("maps"));
        assert!(negotiator.supports_feature("sets"));
        assert!(negotiator.supports_feature("booleans"));
    }

    #[test]
    fn test_resp3_to_resp2_conversion() {
        let _negotiator = ProtocolNegotiator::new();

        // Test boolean conversion
        let bool_data = RespData::Boolean(true);
        let converted = ProtocolNegotiator::convert_to_resp2(&bool_data);
        assert_eq!(converted, RespData::Integer(1));

        // Test map conversion
        let map_data = RespData::Map(vec![
            (
                RespData::BulkString(Some(Bytes::from("key1"))),
                RespData::BulkString(Some(Bytes::from("value1"))),
            ),
            (
                RespData::BulkString(Some(Bytes::from("key2"))),
                RespData::Double(0.5),
            ),
        ]);
        let converted = ProtocolNegotiator::convert_to_resp2(&map_data);
        assert_eq!(
            converted,
            RespData::Array(Some(vec![
                RespData::BulkString(Some(Bytes::from("key1"))),
                RespData::BulkString(Some(Bytes::from("value1"))),
                RespData::BulkString(Some(Bytes::from("key2"))),
                RespData::BulkString(Some(Bytes::from("0.5"))),
            ]))
        );
    }
}
