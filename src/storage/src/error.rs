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

//! Error types for the storage engine

use std::io;

use common_macro::stack_trace_debug;
use snafu::{Location, ResultExt, Snafu};

use crate::storage::BgTask;

pub type Result<T> = std::result::Result<T, Error>;

#[allow(dead_code)]
#[derive(Snafu)]
#[stack_trace_debug]
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

    #[snafu(display("Mpsc error"))]
    Mpsc {
        #[snafu(source)]
        error: tokio::sync::mpsc::error::SendError<BgTask>,
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

    #[snafu(display("Option is none: {}", message))]
    OptionNone {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Option is not dynamically modifiable: {}", message))]
    OptionNotDynamicallyModifiable {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid argument: {}", message))]
    InvalidArgument {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    // all the redis error use this error type
    #[snafu(display("{}", message))]
    RedisErr {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },

    // FLAT vector query governance failures; kept as distinct variants so
    // metrics can tell timeout / cancellation / budget exhaustion apart.
    #[snafu(display("ERR vector flat query timeout"))]
    VectorFlatQueryTimeout {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ERR vector flat query cancelled"))]
    VectorFlatQueryCancelled {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("ERR vector flat query scan budget exceeded"))]
    VectorFlatScanBudgetExceeded {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("LogIndex error: {}", message))]
    LogIndex {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },
}

const STRICT_ROCKS_OPEN_CAUSE_PREFIX: &str = "\u{1f}kiwi-strict-rocks-open-invalid-argument\u{1f}";
const CURRENT_FIELD_LIMIT: usize = 512;
const ON_DISK_FIELD_LIMIT: usize = 1024;
const ACTION_FIELD_LIMIT: usize = 1024;
const CAUSE_FIELD_LIMIT: usize = 4096;
const TRUNCATION_MARKER: &str = "~truncated~";

pub(crate) fn map_existing_strict_rocks_open<T>(
    result: std::result::Result<T, rocksdb::Error>,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == rocksdb::ErrorKind::InvalidArgument => {
            Err(InvalidFormatSnafu {
                message: format!("{STRICT_ROCKS_OPEN_CAUSE_PREFIX}{}", error.into_string()),
            }
            .build())
        }
        Err(error) => Err(error).context(RocksSnafu),
    }
}

pub(crate) fn split_strict_rocks_open_cause(message: String) -> (String, bool) {
    match message.strip_prefix(STRICT_ROCKS_OPEN_CAUSE_PREFIX) {
        Some(cause) => (cause.to_string(), true),
        None => (message, false),
    }
}

pub(crate) fn format_storage_compatibility_refusal(
    current: &str,
    on_disk: &str,
    action: &str,
    cause: &str,
) -> String {
    format!(
        "storage compatibility refusal: current={}; on_disk={}; action={}; cause={}",
        encode_diagnostic_field(current, CURRENT_FIELD_LIMIT),
        encode_diagnostic_field(on_disk, ON_DISK_FIELD_LIMIT),
        encode_diagnostic_field(action, ACTION_FIELD_LIMIT),
        encode_diagnostic_field(cause, CAUSE_FIELD_LIMIT),
    )
}

fn encode_diagnostic_field(value: &str, limit: usize) -> String {
    let mut encoded = String::with_capacity(value.len().min(limit));
    for character in value.chars() {
        let mut utf8 = [0_u8; 4];
        let bytes = character.encode_utf8(&mut utf8).as_bytes();
        let must_encode =
            character == '%' || character == ';' || character == '=' || character.is_control();
        let encoded_len = if must_encode {
            bytes.len() * 3
        } else {
            bytes.len()
        };
        if encoded.len() + encoded_len + TRUNCATION_MARKER.len() > limit {
            encoded.push_str(TRUNCATION_MARKER);
            return encoded;
        }
        if must_encode {
            for byte in bytes {
                encode_diagnostic_byte(&mut encoded, *byte);
            }
        } else {
            encoded.push(character);
        }
    }
    encoded
}

fn encode_diagnostic_byte(encoded: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    encoded.push('%');
    encoded.push(HEX[(byte >> 4) as usize] as char);
    encoded.push(HEX[(byte & 0x0f) as usize] as char);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_formatter_escapes_field_injection_and_bounds_cause() {
        let formatted = format_storage_compatibility_refusal(
            "current; current=fake",
            "disk\non_disk=fake",
            "act\raction=fake",
            &"cause%=".repeat(2048),
        );

        for marker in ["current=", "on_disk=", "action=", "cause="] {
            assert_eq!(formatted.matches(marker).count(), 1, "{formatted}");
        }
        assert!(!formatted.contains('\r') && !formatted.contains('\n'));
        assert!(formatted.contains("current%3B current%3Dfake"));
        assert!(formatted.contains("disk%0Aon_disk%3Dfake"));
        assert!(formatted.contains("act%0Daction%3Dfake"));
        assert!(formatted.ends_with(TRUNCATION_MARKER));
        assert!(
            formatted.len() < 7_000,
            "bounded formatter grew unexpectedly"
        );
    }
}
