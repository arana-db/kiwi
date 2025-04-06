//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

//! Error types for the storage engine

use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Lock error: {0}")]
    Lock(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Batch operation error: {0}")]
    Batch(String),

    #[error("Compaction error: {0}")]
    Compaction(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("System error: {0}")]
    System(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

impl From<String> for StorageError {
    fn from(err: String) -> StorageError {
        StorageError::Unknown(err)
    }
}

impl From<&str> for StorageError {
    fn from(err: &str) -> StorageError {
        StorageError::Unknown(err.to_string())
    }
}