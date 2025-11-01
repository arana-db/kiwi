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

//! Binlog writer implementation

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use super::entry::BinlogEntry;
use crate::error::RaftError;

/// Binlog writer for writing binlog entries to storage
pub struct BinlogWriter {
    /// Write buffer
    buffer: Arc<RwLock<Vec<u8>>>,
}

impl BinlogWriter {
    /// Create a new binlog writer
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Write a binlog entry
    pub fn write_entry(&self, entry: &BinlogEntry) -> Result<usize, RaftError> {
        let serialized = entry.serialize()?;
        let length = serialized.len() as u32;

        // Encode length as varint
        let mut len_bytes = Vec::new();
        let mut v = length;
        loop {
            let byte = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                len_bytes.push(byte);
                break;
            } else {
                len_bytes.push(byte | 0x80);
            }
        }

        let mut buffer = self.buffer.write();
        let start_pos = buffer.len();

        // Write length-prefixed entry
        buffer.extend_from_slice(&len_bytes);
        buffer.extend_from_slice(&serialized);

        let written = buffer.len() - start_pos;
        Ok(written)
    }

    /// Flush buffer to storage
    pub fn flush(&self) -> Result<Vec<u8>, RaftError> {
        let buffer = self.buffer.read();
        Ok(buffer.clone())
    }

    /// Clear buffer
    pub fn clear(&self) {
        let mut buffer = self.buffer.write();
        buffer.clear();
    }

    /// Get current buffer size
    pub fn size(&self) -> usize {
        let buffer = self.buffer.read();
        buffer.len()
    }
}

impl Default for BinlogWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_binlog_writer() {
        let writer = BinlogWriter::new();

        let entry1 = BinlogEntry::new(OperationType::Put, Bytes::from("SET key1 value1"));
        let entry2 = BinlogEntry::new(OperationType::Delete, Bytes::from("DEL key2"));

        writer.write_entry(&entry1).unwrap();
        writer.write_entry(&entry2).unwrap();

        assert!(writer.size() > 0);

        let buffer = writer.flush().unwrap();
        assert!(!buffer.is_empty());
    }
}

// Import OperationType for tests
use super::entry::OperationType;
