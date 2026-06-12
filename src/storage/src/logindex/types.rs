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

//! LogIndex type definitions

use snafu::Snafu;

/// LogIndex type alias (i64 to match RocksDB conventions)
pub type LogIndex = i64;

/// Sequence number type alias
pub type SequenceNumber = u64;

/// Error type for LogIndex operations
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum LogIndexError {
    #[snafu(display("ColumnFamily {} not found in database", cf_name))]
    CfNotFound { cf_name: String },

    #[snafu(display("Invalid ColumnFamily index: {}", cf_id))]
    InvalidCfId { cf_id: usize },

    #[snafu(display("RocksDB error: {}", source))]
    RocksDb { source: rocksdb::Error },

    #[snafu(display("Unknown CF name: {:?}", name))]
    UnknownCfName { name: Option<Vec<u8>> },
}

/// Result type for LogIndex operations
pub type Result<T, E = LogIndexError> = std::result::Result<T, E>;

/// Column Family metadata - single source of truth for CF names
///
/// This module provides centralized CF name definitions to avoid
/// inconsistencies across db_access.rs, event_listener.rs, and cf_tracker.rs.
pub mod cf_metadata {
    /// Number of column families
    pub const COLUMN_FAMILY_COUNT: usize = 6;

    /// CF names as byte slices (for comparison with rocksdb CF handles)
    /// Note: Using &[u8] instead of &[u8; N] to allow variable-length names
    pub const CF_NAMES: &[&[u8]] = &[
        b"default",
        b"hash_data_cf",
        b"set_data_cf",
        b"list_data_cf",
        b"zset_data_cf",
        b"zset_score_cf",
    ];

    /// CF names as &str (for convenience in some contexts)
    pub const CF_NAMES_STR: &[&str] = &[
        "default",
        "hash_data_cf",
        "set_data_cf",
        "list_data_cf",
        "zset_data_cf",
        "zset_score_cf",
    ];

    // Compile-time guards: ensure arrays match COLUMN_FAMILY_COUNT.
    const _: () = assert!(CF_NAMES.len() == COLUMN_FAMILY_COUNT);
    const _: () = assert!(CF_NAMES_STR.len() == COLUMN_FAMILY_COUNT);

    /// Convert CF name (bytes) to index
    pub fn cf_name_to_index(name: &[u8]) -> Option<usize> {
        CF_NAMES.iter().position(|n| n == &name)
    }

    /// Convert CF name (&str) to index
    pub fn cf_name_str_to_index(name: &str) -> Option<usize> {
        CF_NAMES_STR.iter().position(|n| n == &name)
    }
}

/// Pair of (log_index, seqno) for tracking applied log state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogIndexSeqnoPair {
    log_index: LogIndex,
    seqno: SequenceNumber,
}

impl LogIndexSeqnoPair {
    pub fn new(log_index: LogIndex, seqno: SequenceNumber) -> Self {
        Self { log_index, seqno }
    }

    pub fn log_index(&self) -> LogIndex {
        self.log_index
    }

    pub fn seqno(&self) -> SequenceNumber {
        self.seqno
    }

    pub fn set(&mut self, log_index: LogIndex, seqno: SequenceNumber) {
        self.log_index = log_index;
        self.seqno = seqno;
    }

    /// Compare seqno with another pair
    pub fn ge_seqno(&self, other: &Self) -> bool {
        self.seqno >= other.seqno
    }
}

/// Pair of (log_index, seqno) for tracking applied log state (applied vs flushed)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogIndexAndSequencePair {
    applied_log_index: LogIndex,
    seqno: SequenceNumber,
}

impl LogIndexAndSequencePair {
    pub fn new(applied_log_index: LogIndex, seqno: SequenceNumber) -> Self {
        Self {
            applied_log_index,
            seqno,
        }
    }

    pub fn applied_log_index(&self) -> LogIndex {
        self.applied_log_index
    }

    pub fn seqno(&self) -> SequenceNumber {
        self.seqno
    }
}
