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

use snafu::{Location, Snafu};
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[allow(dead_code)]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("IO error"))]
    Io {
        #[snafu(source)]
        error: io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("RocksDB error"))]
    Rocks {
        #[snafu(source)]
        error: rocksdb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Encoding error: {}", message))]
    Encoding {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Key not found: {}", key))]
    KeyNotFound {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid format: {}", message))]
    InvalidFormat {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Lock error: {}", message))]
    Lock {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Transaction error: {}", message))]
    Transaction {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Batch operation error: {}", message))]
    Batch {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Compaction error: {}", message))]
    Compaction {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Configuration error: {}", message))]
    Config {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("System error: {}", message))]
    System {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unknown error: {}", message))]
    Unknown {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },
}
