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
// WITHOUT WARRANTIES OR ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Binlog entry representation

use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::RaftError;
use crate::types::{LogIndex, Term};

// Use generated protobuf types from parent module
use crate::binlog::kiwi::raft::binlog;

/// Operation type for binlog entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Unknown,
    Put,
    Delete,
    Expire,
    Clear,
}

impl From<i32> for OperationType {
    fn from(op: i32) -> Self {
        match binlog::OperationType::from_i32(op) {
            Some(binlog::OperationType::Unknown) => OperationType::Unknown,
            Some(binlog::OperationType::Put) => OperationType::Put,
            Some(binlog::OperationType::Delete) => OperationType::Delete,
            Some(binlog::OperationType::Expire) => OperationType::Expire,
            Some(binlog::OperationType::Clear) => OperationType::Clear,
            None => OperationType::Unknown,
        }
    }
}

impl From<OperationType> for i32 {
    fn from(op: OperationType) -> Self {
        match op {
            OperationType::Unknown => binlog::OperationType::Unknown as i32,
            OperationType::Put => binlog::OperationType::Put as i32,
            OperationType::Delete => binlog::OperationType::Delete as i32,
            OperationType::Expire => binlog::OperationType::Expire as i32,
            OperationType::Clear => binlog::OperationType::Clear as i32,
        }
    }
}

/// Binlog entry - 只保留操作类型和数据内容
#[derive(Debug, Clone)]
pub struct BinlogEntry {
    /// Operation type
    pub operation: OperationType,
    /// Command data (serialized Redis command)
    pub data: Bytes,

    /// Timestamp for ordering (nanoseconds since epoch)
    pub timestamp_ns: Option<i64>,

    /// Sequence number for mapping to RocksDB sequence
    pub sequence: Option<u64>,

    /// Raft log index (for Raft mode)
    pub log_index: Option<LogIndex>,

    /// Raft term (for Raft mode)
    pub term: Option<Term>,
}

impl BinlogEntry {
    /// Create a new binlog entry
    pub fn new(operation: OperationType, data: Bytes) -> Self {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_nanos() as i64);

        Self {
            operation,
            data,
            timestamp_ns,
            sequence: None,
            log_index: None,
            term: None,
        }
    }

    /// Set sequence number for RocksDB mapping
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    /// Set Raft log index and term (for Raft mode)
    pub fn with_raft_info(mut self, log_index: LogIndex, term: Term) -> Self {
        self.log_index = Some(log_index);
        self.term = Some(term);
        self
    }

    /// Serialize binlog entry to protobuf
    pub fn to_proto(&self) -> binlog::BinlogEntry {
        binlog::BinlogEntry {
            operation: self.operation.into(),
            data: self.data.to_vec(),
            timestamp_ns: self.timestamp_ns,
            sequence: self.sequence,
        }
    }

    /// Deserialize binlog entry from protobuf
    pub fn from_proto(proto: binlog::BinlogEntry) -> Result<Self, RaftError> {
        Ok(Self {
            operation: proto.operation.into(),
            data: Bytes::from(proto.data),
            timestamp_ns: proto.timestamp_ns,
            sequence: proto.sequence,
            log_index: None,
            term: None,
        })
    }

    /// Serialize binlog entry to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, RaftError> {
        let proto = self.to_proto();
        prost::Message::encode(&proto).map_err(|e| {
            RaftError::state_machine(format!("Failed to serialize binlog entry: {}", e))
        })
    }

    /// Deserialize binlog entry from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, RaftError> {
        let proto = binlog::BinlogEntry::decode(data).map_err(|e| {
            RaftError::state_machine(format!("Failed to deserialize binlog entry: {}", e))
        })?;
        Self::from_proto(proto)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_binlog_entry_serialization() {
        let entry = BinlogEntry::new(OperationType::Put, Bytes::from("SET key value"));
        let serialized = entry.serialize().unwrap();
        let deserialized = BinlogEntry::deserialize(&serialized).unwrap();

        assert_eq!(entry.operation, deserialized.operation);
        assert_eq!(entry.data, deserialized.data);
    }

    #[test]
    fn test_binlog_entry_with_sequence() {
        let entry =
            BinlogEntry::new(OperationType::Put, Bytes::from("SET key value")).with_sequence(12345);

        assert_eq!(entry.sequence, Some(12345));
    }

    #[test]
    fn test_binlog_entry_with_raft_info() {
        let entry = BinlogEntry::new(OperationType::Put, Bytes::from("SET key value"))
            .with_raft_info(100, 5);

        assert_eq!(entry.log_index, Some(100));
        assert_eq!(entry.term, Some(5));
    }
}
