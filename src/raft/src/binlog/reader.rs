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

//! Binlog reader implementation


use super::entry::BinlogEntry;
use crate::error::RaftError;

/// Binlog reader for reading binlog entries from storage
pub struct BinlogReader {
    /// Current read position
    position: usize,
}

impl BinlogReader {
    /// Create a new binlog reader
    pub fn new() -> Self {
        Self { position: 0 }
    }

    /// Read a binlog entry from bytes
    pub fn read_entry(&mut self, data: &[u8]) -> Result<BinlogEntry, RaftError> {
        // For protobuf, we need to handle length-prefixed encoding
        // First, read the length (varint)
        let (length, offset) = read_varint(data)?;

        if data.len() < offset + length as usize {
            return Err(RaftError::state_machine("Incomplete binlog entry"));
        }

        let entry_data = &data[offset..offset + length as usize];
        let entry = BinlogEntry::deserialize(entry_data)?;

        self.position += offset + length as usize;
        Ok(entry)
    }

    /// Get current read position
    pub fn position(&self) -> usize {
        self.position
    }

    /// Reset read position
    pub fn reset(&mut self) {
        self.position = 0;
    }
}

/// Read a varint (variable-length integer) from bytes
fn read_varint(data: &[u8]) -> Result<(u32, usize), RaftError> {
    let mut value = 0u32;
    let mut shift = 0;
    let mut offset = 0;

    for &byte in data.iter() {
        offset += 1;
        value |= ((byte & 0x7F) as u32) << shift;

        if (byte & 0x80) == 0 {
            break;
        }

        shift += 7;
        if shift >= 32 {
            return Err(RaftError::state_machine("Varint too large"));
        }
    }

    Ok((value, offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_binlog_reader() {
        let entry = BinlogEntry::new(crate::binlog::OperationType::Put, Bytes::from("SET key value"));
        let serialized = entry.serialize().unwrap();

        // For testing, we'll create a length-prefixed format
        let mut data = Vec::new();
        let len = serialized.len() as u32;

        // Encode length as varint
        let mut len_bytes = Vec::new();
        let mut v = len;
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

        data.extend_from_slice(&len_bytes);
        data.extend_from_slice(&serialized);

        let mut reader = BinlogReader::new();
        let read_entry = reader.read_entry(&data).unwrap();

        assert_eq!(entry.operation, read_entry.operation);
        assert_eq!(entry.data, read_entry.data);
    }
}

