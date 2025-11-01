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

//! Event listener for monitoring RocksDB memtable events
//!
//! This listener monitors:
//! 1. Memtable seal events - when memtable is sealed
//! 2. Memtable flush complete events - when flush is completed
//! These events are used to advance snapshot points and track log index progress.

use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::RaftError;
use crate::sequence_mapping::SequenceMappingQueue;
use crate::types::LogIndex;

/// Callback function type for memtable events
pub type MemtableEventCallback = Arc<dyn Fn(u64, Option<LogIndex>) + Send + Sync>;

/// Event listener for RocksDB memtable events
pub struct LogIndexEventListener {
    /// Sequence mapping queue (for reference)
    sequence_queue: Arc<SequenceMappingQueue>,
    /// Memtable seal count (for throttling snapshots)
    memtable_seal_count: Arc<AtomicU64>,
    /// Minimum log index for current memtable
    current_memtable_min_log_index: Arc<RwLock<Option<LogIndex>>>,
    /// Callback for memtable seal events
    on_memtable_seal: Option<MemtableEventCallback>,
    /// Callback for flush complete events
    on_flush_complete: Option<MemtableEventCallback>,
}

impl LogIndexEventListener {
    /// Create a new event listener
    pub fn new(sequence_queue: Arc<SequenceMappingQueue>) -> Self {
        Self {
            sequence_queue,
            memtable_seal_count: Arc::new(AtomicU64::new(0)),
            current_memtable_min_log_index: Arc::new(RwLock::new(None)),
            on_memtable_seal: None,
            on_flush_complete: None,
        }
    }

    /// Set callback for memtable seal events
    pub fn set_on_memtable_seal(&mut self, callback: MemtableEventCallback) {
        self.on_memtable_seal = Some(callback);
    }

    /// Set callback for flush complete events
    pub fn set_on_flush_complete(&mut self, callback: MemtableEventCallback) {
        self.on_flush_complete = Some(callback);
    }

    /// Handle memtable seal event
    pub fn on_memtable_seal(&self, sequence: u64) {
        let count = self.memtable_seal_count.fetch_add(1, Ordering::SeqCst);

        // Find log index for this sequence
        let log_index = self.sequence_queue.find_log_index(sequence);

        // Update minimum log index for current memtable
        if let Some(log_idx) = log_index {
            let mut min_log_index = self.current_memtable_min_log_index.write();
            match *min_log_index {
                None => *min_log_index = Some(log_idx),
                Some(current_min) => {
                    if log_idx < current_min {
                        *min_log_index = Some(log_idx);
                    }
                }
            }
        }

        // Call callback if set
        if let Some(ref callback) = self.on_memtable_seal {
            callback(sequence, log_index);
        }

        log::debug!(
            "Memtable sealed at sequence {}, seal count: {}",
            sequence,
            count + 1
        );
    }

    /// Handle flush complete event
    pub fn on_flush_complete(&self, sequence: u64, file_size: Option<u64>) {
        // Find log index for the largest sequence in the flushed memtable
        // Note: flush complete is async, so log index might be ahead
        let log_index = self.sequence_queue.find_log_index(sequence);

        // Get the minimum log index from the flushed memtable
        let min_log_index = *self.current_memtable_min_log_index.read();

        // Advance snapshot point based on flushed data
        // Use the minimum log index to ensure we don't skip any data
        let snapshot_log_index = min_log_index.or(log_index);

        // Call callback if set
        if let Some(ref callback) = self.on_flush_complete {
            callback(sequence, snapshot_log_index);
        }

        // Reset memtable tracking
        {
            let mut min_log_index = self.current_memtable_min_log_index.write();
            *min_log_index = None;
        }

        log::debug!(
            "Flush completed at sequence {}, file_size: {:?}, snapshot_log_index: {:?}",
            sequence,
            file_size,
            snapshot_log_index
        );
    }

    /// Get memtable seal count (for throttling snapshots)
    pub fn get_memtable_seal_count(&self) -> u64 {
        self.memtable_seal_count.load(Ordering::SeqCst)
    }

    /// Reset memtable seal count
    pub fn reset_memtable_seal_count(&self) {
        self.memtable_seal_count.store(0, Ordering::SeqCst);
    }
}

impl rocksdb::EventListener for LogIndexEventListener {
    fn on_flush_completed(&self, _db_name: &str, _cf_name: &str, file_path: &str, file_size: u64) {
        // Extract sequence number from flush info if available
        // For now, we'll use a placeholder - in real implementation,
        // we'd parse this from RocksDB's flush job info
        let sequence = self.sequence_queue.max_sequence().unwrap_or(0);
        self.on_flush_complete(sequence, Some(file_size));
    }

    fn on_memtable_sealed(&self, _db_name: &str, _cf_name: &str, _memtable_id: u64) {
        // Get sequence from the mapping queue
        // In a real implementation, we'd get this from RocksDB's memtable info
        if let Some(sequence) = self.sequence_queue.max_sequence() {
            self.on_memtable_seal(sequence);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_listener() {
        let queue = Arc::new(SequenceMappingQueue::new(100));

        // Add some mappings
        queue.add_mapping(100, 10).unwrap();
        queue.add_mapping(200, 20).unwrap();
        queue.add_mapping(300, 30).unwrap();

        let listener = LogIndexEventListener::new(queue.clone());

        // Test memtable seal
        listener.on_memtable_seal(250);

        // Should have tracked the log index
        let min_log_index = listener.current_memtable_min_log_index.read();
        assert!(min_log_index.is_some());

        // Test flush complete
        listener.on_flush_complete(350, Some(1024));

        // Memtable tracking should be reset
        let min_log_index = listener.current_memtable_min_log_index.read();
        assert!(min_log_index.is_none());
    }
}
