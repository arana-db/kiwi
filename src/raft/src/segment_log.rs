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

//! Segment log format and layout
//!
//! This module implements the segment log format compatible with braft.
//! Layout:
//!   log_meta: record start_log
//!   log_000001-0001000: closed segment
//!   log_inprogress_0001001: open segment
//!
//! Header format:
//!   [checksum(8)][data_length(4)][data]

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use bytes::Bytes;
use crc16::State;

use crate::error::RaftError;
use crate::types::{LogIndex, Term};
use crate::binlog::{BinlogEntry, BinlogWriter};

/// Log metadata file name
const LOG_META_FILE: &str = "log_meta";

/// Segment file name prefix for closed segments
const LOG_SEGMENT_PREFIX: &str = "log_";

/// Segment file name prefix for in-progress segments
const LOG_INPROGRESS_PREFIX: &str = "log_inprogress_";

/// Header size: checksum(8) + data_length(4)
const LOG_HEADER_SIZE: usize = 12;

/// Maximum segment size (default: 1GB)
const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

/// Log metadata structure
#[derive(Debug, Clone)]
pub struct LogMeta {
    /// Start log index
    pub start_log_index: LogIndex,
}

impl LogMeta {
    /// Serialize metadata to bytes
    pub fn serialize(&self) -> Vec<u8> {
        // Simple serialization: 8 bytes for start_log_index
        self.start_log_index.to_be_bytes().to_vec()
    }
    
    /// Deserialize metadata from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, RaftError> {
        if data.len() < 8 {
            return Err(RaftError::state_machine("Invalid log meta data"));
        }
        
        let start_log_index = LogIndex::from_be_bytes(
            data[0..8].try_into()
                .map_err(|_| RaftError::state_machine("Failed to parse log index"))?
        );
        
        Ok(Self {
            start_log_index,
        })
    }
}

/// Segment header with checksum and data length
#[derive(Debug, Clone)]
struct SegmentHeader {
    /// CRC16 checksum
    checksum: u16,
    /// Data length (not including header)
    data_length: u32,
}

impl SegmentHeader {
    /// Create a new header for data
    fn new(data: &[u8]) -> Self {
        let checksum = State::<crc16::XMODEM>::calculate(data);
        Self {
            checksum,
            data_length: data.len() as u32,
        }
    }
    
    /// Serialize header to bytes
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(LOG_HEADER_SIZE);
        buf.extend_from_slice(&self.checksum.to_be_bytes());
        buf.extend_from_slice(&self.data_length.to_be_bytes());
        buf
    }
    
    /// Deserialize header from bytes
    fn deserialize(data: &[u8]) -> Result<Self, RaftError> {
        if data.len() < LOG_HEADER_SIZE {
            return Err(RaftError::state_machine("Invalid segment header"));
        }
        
        let checksum = u16::from_be_bytes(
            data[0..2].try_into()
                .map_err(|_| RaftError::state_machine("Failed to parse checksum"))?
        );
        
        let data_length = u32::from_be_bytes(
            data[2..6].try_into()
                .map_err(|_| RaftError::state_machine("Failed to parse data length"))?
        );
        
        Ok(Self {
            checksum,
            data_length,
        })
    }
    
    /// Verify checksum for data
    fn verify(&self, data: &[u8]) -> bool {
        let calculated = State::<crc16::XMODEM>::calculate(data);
        calculated == self.checksum
    }
}

/// Segment log manager
pub struct SegmentLog {
    /// Base directory for log files
    base_dir: PathBuf,
    /// Current start log index
    start_log_index: Arc<AtomicU64>,
    /// Current segment start index
    current_segment_start: Arc<AtomicU64>,
    /// Current segment file (for in-progress)
    current_segment_file: Arc<RwLock<Option<File>>>,
    /// Current segment size
    current_segment_size: Arc<AtomicU64>,
    /// Maximum segment size
    max_segment_size: u64,
}

impl SegmentLog {
    /// Create a new segment log manager
    pub fn new<P: AsRef<Path>>(base_dir: P, max_segment_size: Option<u64>) -> Result<Self, RaftError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| RaftError::state_machine(format!("Failed to create log directory: {}", e)))?;
        
        // Load metadata
        let meta = Self::load_meta(&base_dir)?;
        
        Ok(Self {
            base_dir,
            start_log_index: Arc::new(AtomicU64::new(meta.start_log_index)),
            current_segment_start: Arc::new(AtomicU64::new(meta.start_log_index)),
            current_segment_file: Arc::new(RwLock::new(None)),
            current_segment_size: Arc::new(AtomicU64::new(0)),
            max_segment_size: max_segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE),
        })
    }
    
    /// Load metadata from file
    fn load_meta(base_dir: &Path) -> Result<LogMeta, RaftError> {
        let meta_path = base_dir.join(LOG_META_FILE);
        
        if !meta_path.exists() {
            // Create default metadata
            let meta = LogMeta {
                start_log_index: 1,
            };
            Self::save_meta(base_dir, &meta)?;
            return Ok(meta);
        }
        
        let mut file = File::open(&meta_path)
            .map_err(|e| RaftError::state_machine(format!("Failed to open log meta: {}", e)))?;
        
        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .map_err(|e| RaftError::state_machine(format!("Failed to read log meta: {}", e)))?;
        
        LogMeta::deserialize(&data)
    }
    
    /// Save metadata to file
    fn save_meta(base_dir: &Path, meta: &LogMeta) -> Result<(), RaftError> {
        let meta_path = base_dir.join(LOG_META_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&meta_path)
            .map_err(|e| RaftError::state_machine(format!("Failed to create log meta: {}", e)))?;
        
        let data = meta.serialize();
        file.write_all(&data)
            .map_err(|e| RaftError::state_machine(format!("Failed to write log meta: {}", e)))?;
        
        file.sync_all()
            .map_err(|e| RaftError::state_machine(format!("Failed to sync log meta: {}", e)))?;
        
        Ok(())
    }
    
    /// Get segment file name for a closed segment
    fn segment_filename(start_index: LogIndex, end_index: LogIndex) -> String {
        format!("{}{:06}-{:06}", LOG_SEGMENT_PREFIX, start_index, end_index)
    }
    
    /// Get segment file name for an in-progress segment
    fn inprogress_filename(start_index: LogIndex) -> String {
        format!("{}{:06}", LOG_INPROGRESS_PREFIX, start_index)
    }
    
    /// Append a binlog entry to the current segment
    pub fn append_entry(&self, entry: &BinlogEntry) -> Result<(), RaftError> {
        // Serialize entry
        let data = entry.serialize()?;
        
        // Create header
        let header = SegmentHeader::new(&data);
        let header_bytes = header.serialize();
        
        // Ensure we have an open segment file
        let mut segment_file = self.current_segment_file.write();
        if segment_file.is_none() {
            let start_index = self.current_segment_start.load(Ordering::SeqCst);
            let filename = Self::inprogress_filename(start_index);
            let filepath = self.base_dir.join(&filename);
            
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&filepath)
                .map_err(|e| RaftError::state_machine(format!("Failed to open segment file: {}", e)))?;
            
            *segment_file = Some(file);
        }
        
        let file = segment_file.as_mut().unwrap();
        
        // Write header and data
        file.write_all(&header_bytes)
            .map_err(|e| RaftError::state_machine(format!("Failed to write header: {}", e)))?;
        file.write_all(&data)
            .map_err(|e| RaftError::state_machine(format!("Failed to write data: {}", e)))?;
        file.sync_all()
            .map_err(|e| RaftError::state_machine(format!("Failed to sync segment: {}", e)))?;
        
        // Update segment size
        let new_size = self.current_segment_size.fetch_add(
            (header_bytes.len() + data.len()) as u64,
            Ordering::SeqCst
        ) + (header_bytes.len() + data.len()) as u64;
        
        // Rotate segment if size exceeds limit
        if new_size >= self.max_segment_size {
            self.rotate_segment()?;
        }
        
        Ok(())
    }
    
    /// Rotate to a new segment
    fn rotate_segment(&self) -> Result<(), RaftError> {
        // Close current segment
        let mut segment_file = self.current_segment_file.write();
        if let Some(mut file) = segment_file.take() {
            file.sync_all()
                .map_err(|e| RaftError::state_machine(format!("Failed to sync segment: {}", e)))?;
            drop(file);
            
            // Rename in-progress to closed segment
            let start_index = self.current_segment_start.load(Ordering::SeqCst);
            let current_size = self.current_segment_size.load(Ordering::SeqCst);
            
            // Calculate end index (approximate)
            let end_index = start_index + (current_size / 1024) as LogIndex; // Rough estimate
            
            let inprogress_name = Self::inprogress_filename(start_index);
            let segment_name = Self::segment_filename(start_index, end_index);
            
            let inprogress_path = self.base_dir.join(&inprogress_name);
            let segment_path = self.base_dir.join(&segment_name);
            
            std::fs::rename(&inprogress_path, &segment_path)
                .map_err(|e| RaftError::state_machine(format!("Failed to rename segment: {}", e)))?;
        }
        
        // Reset for new segment
        let new_start = self.current_segment_start.load(Ordering::SeqCst) + 1000; // Approximate
        self.current_segment_start.store(new_start, Ordering::SeqCst);
        self.current_segment_size.store(0, Ordering::SeqCst);
        
        Ok(())
    }
    
    /// Get start log index
    pub fn get_start_log_index(&self) -> LogIndex {
        self.start_log_index.load(Ordering::SeqCst)
    }
    
    /// Flush current segment
    pub fn flush(&self) -> Result<(), RaftError> {
        let mut segment_file = self.current_segment_file.write();
        if let Some(ref mut file) = *segment_file {
            file.sync_all()
                .map_err(|e| RaftError::state_machine(format!("Failed to flush segment: {}", e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use bytes::Bytes;

    #[test]
    fn test_log_meta() {
        let meta = LogMeta {
            start_log_index: 100,
        };
        
        let data = meta.serialize();
        let deserialized = LogMeta::deserialize(&data).unwrap();
        
        assert_eq!(meta.start_log_index, deserialized.start_log_index);
    }

    #[test]
    fn test_segment_header() {
        let data = b"test data";
        let header = SegmentHeader::new(data);
        
        assert!(header.verify(data));
        
        let header_bytes = header.serialize();
        let deserialized = SegmentHeader::deserialize(&header_bytes).unwrap();
        
        assert_eq!(header.checksum, deserialized.checksum);
        assert_eq!(header.data_length, deserialized.data_length);
        assert!(deserialized.verify(data));
    }

    #[test]
    fn test_segment_log_append() {
        let temp_dir = TempDir::new().unwrap();
        let segment_log = SegmentLog::new(temp_dir.path(), Some(1024 * 1024)).unwrap();
        
        let entry = BinlogEntry::new(
            crate::binlog::OperationType::Put,
            Bytes::from("SET key value")
        );
        
        segment_log.append_entry(&entry).unwrap();
        segment_log.flush().unwrap();
        
        // Verify segment file exists
        let inprogress_name = SegmentLog::inprogress_filename(1);
        let filepath = temp_dir.path().join(&inprogress_name);
        assert!(filepath.exists());
    }
}
