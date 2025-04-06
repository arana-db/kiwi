//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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