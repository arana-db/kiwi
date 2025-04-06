//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

//! Type definitions for storage engine

use std::cmp::Ordering;

/// Key-Value pair structure
#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KeyValue {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

impl Ord for KeyValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for KeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for KeyValue {}

/// Field-Value pair structure for hash type
#[derive(Debug, Clone, PartialEq)]
pub struct FieldValue {
    pub field: Vec<u8>,
    pub value: Vec<u8>,
}

impl FieldValue {
    pub fn new(field: Vec<u8>, value: Vec<u8>) -> Self {
        Self { field, value }
    }
}

/// Key with version information
#[derive(Debug, Clone, PartialEq)]
pub struct KeyVersion {
    pub key: Vec<u8>,
    pub version: u64,
}

impl KeyVersion {
    pub fn new(key: Vec<u8>, version: u64) -> Self {
        Self { key, version }
    }
}

/// Score-Member pair for sorted set
#[derive(Debug, Clone)]
pub struct ScoreMember {
    pub score: f64,
    pub member: Vec<u8>,
}

impl ScoreMember {
    pub fn new(score: f64, member: Vec<u8>) -> Self {
        Self { score, member }
    }
}

impl PartialEq for ScoreMember {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.member == other.member
    }
}

/// Value status including TTL
#[derive(Debug, Clone, PartialEq)]
pub struct ValueStatus {
    pub value: Vec<u8>,
    pub ttl: i64,
}

impl ValueStatus {
    pub fn new(value: Vec<u8>, ttl: i64) -> Self {
        Self { value, ttl }
    }
}

/// Key information statistics
#[derive(Debug, Clone, Default)]
pub struct KeyInfo {
    pub keys: u64,
    pub expires: u64,
    pub avg_ttl: u64,
    pub invalid_keys: u64,
}

impl KeyInfo {
    pub fn new(keys: u64, expires: u64, avg_ttl: u64, invalid_keys: u64) -> Self {
        Self {
            keys,
            expires,
            avg_ttl,
            invalid_keys,
        }
    }

    pub fn add(&self, other: &Self) -> Self {
        Self {
            keys: self.keys + other.keys,
            expires: self.expires + other.expires,
            avg_ttl: self.avg_ttl + other.avg_ttl,
            invalid_keys: self.invalid_keys + other.invalid_keys,
        }
    }
}

/// Data types supported by storage engine
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataType {
    String,
    Hash,
    List,
    Set,
    ZSet,
    All,
}

/// Operation types for background tasks
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operation {
    None,
    CleanAll,
    CompactRange,
}

/// Background task definition
#[derive(Debug, Clone)]
pub struct BGTask {
    pub type_: DataType,
    pub operation: Operation,
    pub args: Vec<Vec<u8>>,
}

impl BGTask {
    pub fn new(type_: DataType, operation: Operation, args: Vec<Vec<u8>>) -> Self {
        Self {
            type_,
            operation,
            args,
        }
    }
}