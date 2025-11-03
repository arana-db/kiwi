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

//! Redis List commands implementation

use std::sync::Arc;

use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

/// LPUSH command - Insert all the specified values at the head of the list stored at key
#[derive(Clone, Default)]
pub struct LPushCmd {
    meta: CmdMeta,
}

impl LPushCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "lpush".to_string(),
                arity: -3, // LPUSH key value [value ...]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for LPushCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();
        let values: Vec<Vec<u8>> = client.argv()[2..].to_vec();

        match storage.lpush(&key, &values) {
            Ok(length) => {
                client.set_reply(RespData::Integer(length));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

/// RPUSH command - Insert all the specified values at the tail of the list stored at key
#[derive(Clone, Default)]
pub struct RPushCmd {
    meta: CmdMeta,
}

impl RPushCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "rpush".to_string(),
                arity: -3, // RPUSH key value [value ...]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for RPushCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();
        let values: Vec<Vec<u8>> = client.argv()[2..].to_vec();

        match storage.rpush(&key, &values) {
            Ok(length) => {
                client.set_reply(RespData::Integer(length));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

/// LPOP command - Removes and returns the first element of the list stored at key
#[derive(Clone, Default)]
pub struct LPopCmd {
    meta: CmdMeta,
}

impl LPopCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "lpop".to_string(),
                arity: -2, // LPOP key [count]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

const MAX_SAFE_POP_COUNT: usize = 1_000_000;

impl Cmd for LPopCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        // Parse optional count parameter
        let count = if client.argv().len() > 2 {
            match String::from_utf8_lossy(&client.argv()[2]).parse::<usize>() {
                Ok(c) if c > 0 && c < MAX_SAFE_POP_COUNT => Some(c),
                _ => {
                    client.set_reply(RespData::Error(
                        "ERR value is not an integer or out of range".into(),
                    ));
                    return;
                }
            }
        } else {
            None
        };

        match storage.lpop(&key, count) {
            Ok(Some(values)) => {
                if count.is_some() {
                    // Return array for multiple values
                    let resp_values: Vec<RespData> = values
                        .into_iter()
                        .map(|v| RespData::BulkString(Some(v.into())))
                        .collect();
                    client.set_reply(RespData::Array(Some(resp_values)));
                } else {
                    // Return single value for LPOP without count
                    client.set_reply(RespData::BulkString(Some(values[0].clone().into())));
                }
            }
            Ok(None) => {
                client.set_reply(RespData::BulkString(None));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

/// RPOP command - Removes and returns the last element of the list stored at key
#[derive(Clone, Default)]
pub struct RPopCmd {
    meta: CmdMeta,
}

impl RPopCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "rpop".to_string(),
                arity: -2, // RPOP key [count]
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for RPopCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        // Parse optional count parameter
        let count = if client.argv().len() > 2 {
            match String::from_utf8_lossy(&client.argv()[2]).parse::<usize>() {
                Ok(c) if c > 0 => Some(c),
                _ => {
                    client.set_reply(RespData::Error(
                        "ERR value is not an integer or out of range".into(),
                    ));
                    return;
                }
            }
        } else {
            None
        };

        match storage.rpop(&key, count) {
            Ok(Some(values)) => {
                if count.is_some() {
                    // Return array for multiple values
                    let resp_values: Vec<RespData> = values
                        .into_iter()
                        .map(|v| RespData::BulkString(Some(v.into())))
                        .collect();
                    client.set_reply(RespData::Array(Some(resp_values)));
                } else {
                    // Return single value for RPOP without count
                    if values.is_empty() {
                        client.set_reply(RespData::BulkString(None));
                        return;
                    } else {
                        client.set_reply(RespData::BulkString(Some(values[0].clone().into())));
                    }
                }
            }
            Ok(None) => {
                client.set_reply(RespData::BulkString(None));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

/// LLEN command - Returns the length of the list stored at key
#[derive(Clone, Default)]
pub struct LLenCmd {
    meta: CmdMeta,
}

impl LLenCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "llen".to_string(),
                arity: 2, // LLEN key
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::LIST | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for LLenCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        match storage.llen(&key) {
            Ok(length) => {
                client.set_reply(RespData::Integer(length));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

/// LINDEX command - Returns the element at index in the list stored at key
#[derive(Clone, Default)]
pub struct LIndexCmd {
    meta: CmdMeta,
}

impl LIndexCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "lindex".to_string(),
                arity: 3, // LINDEX key index
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::LIST | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for LIndexCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        // Parse index parameter
        let index = match String::from_utf8_lossy(&client.argv()[2]).parse::<i64>() {
            Ok(idx) => idx,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        match storage.lindex(&key, index) {
            Ok(Some(value)) => {
                client.set_reply(RespData::BulkString(Some(value.into())));
            }
            Ok(None) => {
                client.set_reply(RespData::BulkString(None));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

/// LRANGE command - Returns the specified elements of the list stored at key
#[derive(Clone, Default)]
pub struct LRangeCmd {
    meta: CmdMeta,
}

impl LRangeCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "lrange".to_string(),
                arity: 4, // LRANGE key start stop
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::LIST | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for LRangeCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        // Parse start and stop parameters
        let start = match String::from_utf8_lossy(&client.argv()[2]).parse::<i64>() {
            Ok(s) => s,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        let stop = match String::from_utf8_lossy(&client.argv()[3]).parse::<i64>() {
            Ok(s) => s,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        match storage.lrange(&key, start, stop) {
            Ok(values) => {
                let resp_values: Vec<RespData> = values
                    .into_iter()
                    .map(|v| RespData::BulkString(Some(v.into())))
                    .collect();
                client.set_reply(RespData::Array(Some(resp_values)));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

/// LSET command - Sets the list element at index to value
#[derive(Clone, Default)]
pub struct LSetCmd {
    meta: CmdMeta,
}

impl LSetCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "lset".to_string(),
                arity: 4, // LSET key index value
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for LSetCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        // Parse index parameter
        let index = match String::from_utf8_lossy(&client.argv()[2]).parse::<i64>() {
            Ok(idx) => idx,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        let value = client.argv()[3].clone();

        match storage.lset(&key, index, value) {
            Ok(()) => {
                client.set_reply(RespData::SimpleString("OK".into()));
            }
            Err(e) => {
                let error_msg = if e.to_string().contains("not found") {
                    "ERR no such key".to_string()
                } else if e.to_string().contains("out of range") {
                    "ERR index out of range".to_string()
                } else {
                    format!("ERR {e}")
                };
                client.set_reply(RespData::Error(error_msg.into()));
            }
        }
    }
}

/// LTRIM command - Trims the list to the specified range
#[derive(Clone, Default)]
pub struct LTrimCmd {
    meta: CmdMeta,
}

impl LTrimCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "ltrim".to_string(),
                arity: 4, // LTRIM key start stop
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for LTrimCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        // Parse start and stop parameters
        let start = match String::from_utf8_lossy(&client.argv()[2]).parse::<i64>() {
            Ok(s) => s,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        let stop = match String::from_utf8_lossy(&client.argv()[3]).parse::<i64>() {
            Ok(s) => s,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        match storage.ltrim(&key, start, stop) {
            Ok(()) => {
                client.set_reply(RespData::SimpleString("OK".into()));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

/// LREM command - Removes the first count occurrences of elements equal to value from the list
#[derive(Clone, Default)]
pub struct LRemCmd {
    meta: CmdMeta,
}

impl LRemCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "lrem".to_string(),
                arity: 4, // LREM key count value
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for LRemCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();

        // Parse count parameter
        let count = match String::from_utf8_lossy(&client.argv()[2]).parse::<i64>() {
            Ok(c) => c,
            Err(_) => {
                client.set_reply(RespData::Error(
                    "ERR value is not an integer or out of range".into(),
                ));
                return;
            }
        };

        let value = &client.argv()[3];

        match storage.lrem(&key, count, value) {
            Ok(removed_count) => {
                client.set_reply(RespData::Integer(removed_count));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct LPushxCmd {
    meta: CmdMeta,
}

impl LPushxCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "lpushx".to_string(),
                arity: 3, // LPUSHX key value
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for LPushxCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();
        let value = client.argv()[2].clone();

        // LPUSHX implemented - wrap single value in vector
        match storage.lpushx(&key, &[value]) {
            Ok(length) => {
                client.set_reply(RespData::Integer(length));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct RPushxCmd {
    meta: CmdMeta,
}

impl RPushxCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "rpushx".to_string(),
                arity: 3, // RPUSHX key value
                flags: CmdFlags::WRITE,
                acl_category: AclCategory::LIST | AclCategory::WRITE,
                ..Default::default()
            },
        }
    }
}

impl Cmd for RPushxCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let key = client.argv()[1].clone();
        client.set_key(&key);
        true
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let key = client.key();
        let value = client.argv()[2].clone();

        // RPUSHX implemented - wrap single value in vector
        match storage.rpushx(&key, &[value]) {
            Ok(length) => {
                client.set_reply(RespData::Integer(length));
            }
            Err(e) => {
                client.set_reply(RespData::Error(format!("ERR {e}").into()));
            }
        }
    }
}

// Tests are covered by the comprehensive storage layer tests in src/storage/tests/redis_list_test.rs
// The command layer is a thin wrapper around the storage operations
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_metadata() {
        let lpush_cmd = LPushCmd::new();
        assert_eq!(lpush_cmd.name(), "lpush");
        assert_eq!(lpush_cmd.meta().arity, -3);
        assert!(lpush_cmd.has_flag(CmdFlags::WRITE));

        let rpush_cmd = RPushCmd::new();
        assert_eq!(rpush_cmd.name(), "rpush");
        assert_eq!(rpush_cmd.meta().arity, -3);
        assert!(rpush_cmd.has_flag(CmdFlags::WRITE));

        let lpop_cmd = LPopCmd::new();
        assert_eq!(lpop_cmd.name(), "lpop");
        assert_eq!(lpop_cmd.meta().arity, -2);
        assert!(lpop_cmd.has_flag(CmdFlags::WRITE));

        let rpop_cmd = RPopCmd::new();
        assert_eq!(rpop_cmd.name(), "rpop");
        assert_eq!(rpop_cmd.meta().arity, -2);
        assert!(rpop_cmd.has_flag(CmdFlags::WRITE));

        let llen_cmd = LLenCmd::new();
        assert_eq!(llen_cmd.name(), "llen");
        assert_eq!(llen_cmd.meta().arity, 2);
        assert!(llen_cmd.has_flag(CmdFlags::READONLY));

        let lindex_cmd = LIndexCmd::new();
        assert_eq!(lindex_cmd.name(), "lindex");
        assert_eq!(lindex_cmd.meta().arity, 3);
        assert!(lindex_cmd.has_flag(CmdFlags::READONLY));

        let lrange_cmd = LRangeCmd::new();
        assert_eq!(lrange_cmd.name(), "lrange");
        assert_eq!(lrange_cmd.meta().arity, 4);
        assert!(lrange_cmd.has_flag(CmdFlags::READONLY));

        let lset_cmd = LSetCmd::new();
        assert_eq!(lset_cmd.name(), "lset");
        assert_eq!(lset_cmd.meta().arity, 4);
        assert!(lset_cmd.has_flag(CmdFlags::WRITE));

        let ltrim_cmd = LTrimCmd::new();
        assert_eq!(ltrim_cmd.name(), "ltrim");
        assert_eq!(ltrim_cmd.meta().arity, 4);
        assert!(ltrim_cmd.has_flag(CmdFlags::WRITE));

        let lrem_cmd = LRemCmd::new();
        assert_eq!(lrem_cmd.name(), "lrem");
        assert_eq!(lrem_cmd.meta().arity, 4);
        assert!(lrem_cmd.has_flag(CmdFlags::WRITE));
    }
}
