// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Cursor;

/// Binlog operation type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OperateType {
    NoOp = 0,
    Put = 1,
    Delete = 2,
}

/// Single Binlog entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogEntry {
    pub cf_idx: u32,            // Column Family index
    pub op_type: OperateType,   // Operation type
    pub key: Vec<u8>,           // Key
    pub value: Option<Vec<u8>>, // Value (None for Delete)
}

/// Binlog - Complete version, supports multi-CF operations
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Binlog {
    pub db_id: u32,                // Database ID
    pub slot_idx: u32,             // Slot index
    pub entries: Vec<BinlogEntry>, // Operation list
}

impl fmt::Display for Binlog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Binlog{{db_id:{}, slot_idx:{}, entries:{}}}",
            self.db_id,
            self.slot_idx,
            self.entries.len()
        )
    }
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct KiwiNode {
    pub raft_addr: String, // Raft RPC address
    pub resp_addr: String, // RESP service address (for client redirect)
}

impl fmt::Display for KiwiNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KiwiNode(raft={}, resp={})",
            self.raft_addr, self.resp_addr
        )
    }
}

/// Raft response type
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BinlogResponse {
    pub success: bool,
    pub message: Option<String>,
    /// 写入的日志位置（提交后的 log_id）
    /// 客户端可以使用此信息追踪写入状态
    pub log_id: Option<u64>,
}

impl BinlogResponse {
    pub fn ok() -> Self {
        Self {
            success: true,
            message: None,
            log_id: None,
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            message: Some(msg.into()),
            log_id: None,
        }
    }
}

/// Column Family index enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnFamilyIndex {
    MetaCF = 0, // default CF (String + Meta)
    HashesDataCF = 1,
    SetsDataCF = 2,
    ListsDataCF = 3,
    ZsetsDataCF = 4,
    ZsetsScoreCF = 5,
}

impl ColumnFamilyIndex {
    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            0 => Some(Self::MetaCF),
            1 => Some(Self::HashesDataCF),
            2 => Some(Self::SetsDataCF),
            3 => Some(Self::ListsDataCF),
            4 => Some(Self::ZsetsDataCF),
            5 => Some(Self::ZsetsScoreCF),
            _ => None,
        }
    }
}

declare_raft_types!(
    pub KiwiTypeConfig:
        D = Binlog,                   // 写请求类型
        R = BinlogResponse,           // 写响应类型
        NodeId = u64,                 // 节点 ID
        Node = KiwiNode,              // 节点信息
        Entry = openraft::Entry<KiwiTypeConfig>,
);
