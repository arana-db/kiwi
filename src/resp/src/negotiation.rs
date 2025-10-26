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

/// Protocol negotiation handler for RESP3
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

    /// Handle HELLO command for protocol negotiation
    pub fn handle_hello(&mut self, command: &RespCommand) -> RespResult<RespData> {
        // HELLO [protover [AUTH username password] [SETNAME clientname]]
        let mut args_iter = command.args.iter();
        
        // Parse protocol version if provided
        let requested_version = if let Some(version_arg) = args_iter.next() {
            let version_str = std::str::from_utf8(version_arg)
                .map_err(|_| RespError::InvalidData("Invalid protocol version".to_string()))?;
            
            match version_str {
                "2" => RespVersion::RESP2,
                "3" => RespVersion::RESP3,
                _ => return Err(RespError::InvalidData(
                    "Unsupported protocol version. Supported versions: 2, 3".to_string()
                )),
            }
        } else {
            // If no version specified, default to RESP2
            RespVersion::RESP2
        };

        // Update current version
        self.current_version = requested_version;

        // Parse additional arguments (AUTH, SETNAME, etc.)
        while let Some(arg) = args_iter.next() {
            let arg_str = std::str::from_utf8(arg)
                .map_err(|_| RespError::InvalidData("Invalid argument".to_string()))?
                .to_uppercase();

            match arg_str.as_str() {
                "AUTH" => {
                    // AUTH username password
                    let _username = args_iter.next().ok_or_else(|| {
                        RespError::InvalidData("AUTH requires username".to_string())
                    })?;
                    let _password = args_iter.next().ok_or_else(|| {
                        RespError::InvalidData("AUTH requires password".to_string())
                    })?;
                    // TODO: Implement authentication logic
                    self.client_capabilities.insert("auth".to_string(), "enabled".to_string());
                }
                "SETNAME" => {
                    // SETNAME clientname
                    let client_name = args_iter.next().ok_or_else(|| {
                        RespError::InvalidData("SETNAME requires client name".to_string())
                    })?;
                    let client_name_str = std::str::from_utf8(client_name)
                        .map_err(|_| RespError::InvalidData("Invalid client name".to_string()))?;
                    self.client_capabilities.insert("client_name".to_string(), client_name_str.to_string());
                }
                _ => {
                    return Err(RespError::InvalidData(format!("Unknown HELLO argument: {}", arg_str)));
                }
            }
        }

        // Build response based on protocol version
        match self.current_version {
            RespVersion::RESP2 => self.build_resp2_hello_response(),
            RespVersion::RESP3 => self.build_resp3_hello_response(),
            _ => self.build_resp2_hello_response(), // Fallback to RESP2
        }
    }

    fn build_resp2_hello_response(&self) -> RespResult<RespData> {
        // RESP2 response format (array of key-value pairs)
        let mut response = vec![
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
        ];

        // Add client capabilities if any
        for (key, value) in &self.client_capabilities {
            response.push(RespData::BulkString(Some(Bytes::from(key.clone()))));
            response.push(RespData::BulkString(Some(Bytes::from(value.clone()))));
        }

        Ok(RespData::Array(Some(response)))
    }

    fn build_resp3_hello_response(&self) -> RespResult<RespData> {
        // RESP3 response format (map)
        let mut pairs = vec![
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
        ];

        // Add client capabilities if any
        for (key, value) in &self.client_capabilities {
            pairs.push((
                RespData::BulkString(Some(Bytes::from(key.clone()))),
                RespData::BulkString(Some(Bytes::from(value.clone()))),
            ));
        }

        Ok(RespData::Map(pairs))
    }

    /// Check if the current protocol supports a specific feature
    pub fn supports_feature(&self, feature: &str) -> bool {
        match (self.current_version, feature) {
            (RespVersion::RESP3, "maps") => true,
            (RespVersion::RESP3, "sets") => true,
            (RespVersion::RESP3, "booleans") => true,
            (RespVersion::RESP3, "doubles") => true,
            (RespVersion::RESP3, "big_numbers") => true,
            (RespVersion::RESP3, "verbatim_strings") => true,
            (RespVersion::RESP3, "push_messages") => true,
            _ => false,
        }
    }

    /// Gracefully fallback to RESP2 if needed
    pub fn fallback_to_resp2(&mut self) -> RespVersion {
        let old_version = self.current_version;
        self.current_version = RespVersion::RESP2;
        old_version
    }

    /// Convert RESP3 data to RESP2 compatible format for fallback
    pub fn convert_to_resp2(&self, data: &RespData) -> RespData {
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
                    array.push(self.convert_to_resp2(key));
                    array.push(self.convert_to_resp2(value));
                }
                RespData::Array(Some(array))
            }
            RespData::Set(items) => {
                // Convert set to array
                let converted_items: Vec<_> = items.iter().map(|item| self.convert_to_resp2(item)).collect();
                RespData::Array(Some(converted_items))
            }
            RespData::Push(items) => {
                // Convert push to array
                let converted_items: Vec<_> = items.iter().map(|item| self.convert_to_resp2(item)).collect();
                RespData::Array(Some(converted_items))
            }
            // RESP2 compatible types - return as-is
            RespData::SimpleString(_) |
            RespData::Error(_) |
            RespData::Integer(_) |
            RespData::BulkString(_) |
            RespData::Inline(_) => data.clone(),
            RespData::Array(Some(items)) => {
                // Recursively convert array items
                let converted_items: Vec<_> = items.iter().map(|item| self.convert_to_resp2(item)).collect();
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
        let command = RespCommand::new(
            CommandType::Hello,
            vec![Bytes::from("2")],
            false,
        );

        let response = negotiator.handle_hello(&command).unwrap();
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
        let command = RespCommand::new(
            CommandType::Hello,
            vec![Bytes::from("3")],
            false,
        );

        let response = negotiator.handle_hello(&command).unwrap();
        assert_eq!(negotiator.current_version(), RespVersion::RESP3);
        
        if let RespData::Map(pairs) = response {
            assert!(pairs.len() >= 6); // At least the basic server info
        } else {
            panic!("Expected map response for RESP3");
        }
    }

    #[test]
    fn test_feature_support() {
        let mut negotiator = ProtocolNegotiator::new();
        
        // RESP2 doesn't support RESP3 features
        assert!(!negotiator.supports_feature("maps"));
        
        // Switch to RESP3
        let command = RespCommand::new(
            CommandType::Hello,
            vec![Bytes::from("3")],
            false,
        );
        negotiator.handle_hello(&command).unwrap();
        
        // RESP3 supports new features
        assert!(negotiator.supports_feature("maps"));
        assert!(negotiator.supports_feature("sets"));
        assert!(negotiator.supports_feature("booleans"));
    }

    #[test]
    fn test_resp3_to_resp2_conversion() {
        let negotiator = ProtocolNegotiator::new();
        
        // Test boolean conversion
        let bool_data = RespData::Boolean(true);
        let converted = negotiator.convert_to_resp2(&bool_data);
        assert_eq!(converted, RespData::Integer(1));
        
        // Test map conversion
        let map_data = RespData::Map(vec![
            (RespData::BulkString(Some(Bytes::from("key1"))), RespData::BulkString(Some(Bytes::from("value1")))),
            (RespData::BulkString(Some(Bytes::from("key2"))), RespData::Integer(42)),
        ]);
        let converted = negotiator.convert_to_resp2(&map_data);
        if let RespData::Array(Some(items)) = converted {
            assert_eq!(items.len(), 4); // 2 key-value pairs = 4 items
        } else {
            panic!("Expected array conversion for map");
        }
    }
}