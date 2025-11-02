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

//! Sequence number to log index mapping
//!
//! This module provides mapping between RocksDB sequence numbers and Raft log indices.
//! It maintains an in-memory queue to track the relationship and uses RocksDB
//! table properties to persist this mapping during flush.

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::RaftError;
use crate::types::LogIndex;

/// Mapping entry: sequence number -> log index
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceMapping {
    /// RocksDB sequence number
    pub sequence: u64,
    /// Raft log index
    pub log_index: LogIndex,
}

/// In-memory queue for tracking sequence -> log index mappings
pub struct SequenceMappingQueue {
    /// Queue of mappings (oldest first)
    queue: Arc<RwLock<VecDeque<SequenceMapping>>>,
    /// Maximum queue size (to prevent unbounded growth)
    max_size: usize,
}

impl SequenceMappingQueue {
    /// Create a new mapping queue
    pub fn new(max_size: usize) -> Self {
        Self {
            queue: Arc::new(RwLock::new(VecDeque::new())),
            max_size,
        }
    }

    /// Add a mapping from sequence number to log index
    pub fn add_mapping(&self, sequence: u64, log_index: LogIndex) -> Result<(), RaftError> {
        let mut queue = self.queue.write();

        // Remove old mappings if queue is full
        while queue.len() >= self.max_size {
            queue.pop_front();
        }

        queue.push_back(SequenceMapping {
            sequence,
            log_index,
        });

        Ok(())
    }

    /// Find log index for a given sequence number
    /// Returns the log index if found, or None if not in queue
    pub fn find_log_index(&self, sequence: u64) -> Option<LogIndex> {
        let queue = self.queue.read();

        // Binary search for the sequence number
        // Since sequences are monotonic, we can optimize this
        for mapping in queue.iter().rev() {
            if mapping.sequence <= sequence {
                return Some(mapping.log_index);
            }
        }

        None
    }

    /// Find sequence number for a given log index
    /// Returns the sequence number if found, or None if not in queue
    pub fn find_sequence(&self, log_index: LogIndex) -> Option<u64> {
        let queue = self.queue.read();

        for mapping in queue.iter() {
            if mapping.log_index == log_index {
                return Some(mapping.sequence);
            }
        }

        None
    }

    /// Get the minimum sequence number in the queue
    pub fn min_sequence(&self) -> Option<u64> {
        let queue = self.queue.read();
        queue.front().map(|m| m.sequence)
    }

    /// Get the maximum sequence number in the queue
    pub fn max_sequence(&self) -> Option<u64> {
        let queue = self.queue.read();
        queue.back().map(|m| m.sequence)
    }

    /// Clear all mappings
    pub fn clear(&self) {
        let mut queue = self.queue.write();
        queue.clear();
    }

    /// Get current queue size
    pub fn len(&self) -> usize {
        let queue = self.queue.read();
        queue.len()
    }

    /// Remove mappings up to a given sequence number
    /// This is called after flush when data is persisted
    pub fn remove_up_to_sequence(&self, sequence: u64) {
        let mut queue = self.queue.write();

        // Remove all mappings with sequence <= given sequence
        while let Some(front) = queue.front() {
            if front.sequence <= sequence {
                queue.pop_front();
            } else {
                break;
            }
        }
    }

    /// Get all mappings (for persistence)
    pub fn get_all_mappings(&self) -> Vec<SequenceMapping> {
        let queue = self.queue.read();
        queue.iter().copied().collect()
    }
}

impl Default for SequenceMappingQueue {
    fn default() -> Self {
        Self::new(10000) // Default max size: 10000 entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_mapping_queue() {
        let queue = SequenceMappingQueue::new(100);

        // Add mappings
        queue.add_mapping(100, 1).unwrap();
        queue.add_mapping(200, 2).unwrap();
        queue.add_mapping(300, 3).unwrap();

        // Find log index by sequence
        assert_eq!(queue.find_log_index(150), Some(1));
        assert_eq!(queue.find_log_index(250), Some(2));
        assert_eq!(queue.find_log_index(350), Some(3));

        // Find sequence by log index
        assert_eq!(queue.find_sequence(1), Some(100));
        assert_eq!(queue.find_sequence(2), Some(200));
        assert_eq!(queue.find_sequence(3), Some(300));

        // Test min/max
        assert_eq!(queue.min_sequence(), Some(100));
        assert_eq!(queue.max_sequence(), Some(300));
    }

    #[test]
    fn test_sequence_mapping_queue_overflow() {
        let queue = SequenceMappingQueue::new(3);

        // Add more than max_size
        queue.add_mapping(100, 1).unwrap();
        queue.add_mapping(200, 2).unwrap();
        queue.add_mapping(300, 3).unwrap();
        queue.add_mapping(400, 4).unwrap();
        queue.add_mapping(500, 5).unwrap();

        // Oldest mappings should be removed
        assert_eq!(queue.find_sequence(1), None); // Removed
        assert_eq!(queue.find_sequence(2), None); // Removed
        assert_eq!(queue.find_sequence(3), Some(300));
        assert_eq!(queue.find_sequence(4), Some(400));
        assert_eq!(queue.find_sequence(5), Some(500));

        assert_eq!(queue.len(), 3);
    }

    #[test]
    fn test_remove_up_to_sequence() {
        let queue = SequenceMappingQueue::new(100);

        queue.add_mapping(100, 1).unwrap();
        queue.add_mapping(200, 2).unwrap();
        queue.add_mapping(300, 3).unwrap();
        queue.add_mapping(400, 4).unwrap();

        // Remove up to sequence 250
        queue.remove_up_to_sequence(250);

        // Sequences <= 250 should be removed
        assert_eq!(queue.find_sequence(1), None);
        assert_eq!(queue.find_sequence(2), None);
        assert_eq!(queue.find_sequence(3), Some(300));
        assert_eq!(queue.find_sequence(4), Some(400));

        assert_eq!(queue.len(), 2);
    }
}
