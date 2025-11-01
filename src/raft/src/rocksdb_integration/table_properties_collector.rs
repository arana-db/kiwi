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

//! Table properties collector for persisting log index information
//!
//! This collector stores the minimum log index for each SST file during flush,
//! which allows us to quickly locate the replay starting point after crash recovery.

use parking_lot::RwLock;
use std::sync::Arc;

use crate::error::RaftError;
use crate::sequence_mapping::SequenceMappingQueue;
use crate::types::LogIndex;

/// Custom table properties key for log index
const LOG_INDEX_PROPERTY_KEY: &str = "kiwi.raft.log_index.min";

/// Table properties collector that stores log index information in SST files
pub struct LogIndexTablePropertiesCollector {
    /// Sequence mapping queue (for reference)
    sequence_queue: Arc<SequenceMappingQueue>,
    /// Minimum log index seen in this SST file
    min_log_index: Arc<RwLock<Option<LogIndex>>>,
    /// Minimum sequence number seen in this SST file
    min_sequence: Arc<RwLock<Option<u64>>>,
}

impl LogIndexTablePropertiesCollector {
    /// Create a new table properties collector
    pub fn new(sequence_queue: Arc<SequenceMappingQueue>) -> Self {
        Self {
            sequence_queue,
            min_log_index: Arc::new(RwLock::new(None)),
            min_sequence: Arc::new(RwLock::new(None)),
        }
    }

    /// Add a key-value pair and update min log index
    pub fn add(&self, key: &[u8], _value: &[u8], sequence: u64) {
        // Try to find log index for this sequence
        if let Some(log_index) = self.sequence_queue.find_log_index(sequence) {
            let mut min_log_index = self.min_log_index.write();
            match *min_log_index {
                None => *min_log_index = Some(log_index),
                Some(current_min) => {
                    if log_index < current_min {
                        *min_log_index = Some(log_index);
                    }
                }
            }
        }

        // Track minimum sequence
        let mut min_seq = self.min_sequence.write();
        match *min_seq {
            None => *min_seq = Some(sequence),
            Some(current_min) => {
                if sequence < current_min {
                    *min_seq = Some(sequence);
                }
            }
        }
    }

    /// Finish collecting and return properties
    pub fn finish(&self) -> Result<Vec<(String, String)>, RaftError> {
        let mut properties = Vec::new();

        // Add min log index if available
        if let Some(min_log_index) = *self.min_log_index.read() {
            properties.push((
                LOG_INDEX_PROPERTY_KEY.to_string(),
                min_log_index.to_string(),
            ));
        }

        // Add min sequence if available
        if let Some(min_sequence) = *self.min_sequence.read() {
            properties.push((
                "kiwi.raft.sequence.min".to_string(),
                min_sequence.to_string(),
            ));
        }

        Ok(properties)
    }

    /// Get minimum log index for this SST file
    pub fn get_min_log_index(&self) -> Option<LogIndex> {
        *self.min_log_index.read()
    }

    /// Get minimum sequence number for this SST file
    pub fn get_min_sequence(&self) -> Option<u64> {
        *self.min_sequence.read()
    }
}

impl rocksdb::TablePropertiesCollector for LogIndexTablePropertiesCollector {
    fn name(&self) -> &str {
        "LogIndexTablePropertiesCollector"
    }

    fn add(
        &mut self,
        key: &[u8],
        value: &[u8],
        _entry_type: rocksdb::EntryType,
        sequence: u64,
        _file_size: u64,
    ) {
        // Use the add method to track sequence and log index
        self.add(key, value, sequence);
    }

    fn finish(&mut self) -> rocksdb::UserCollectedProperties {
        let properties = self.finish().unwrap_or_default();
        properties.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_properties_collector() {
        let queue = Arc::new(SequenceMappingQueue::new(100));

        // Add some mappings
        queue.add_mapping(100, 10).unwrap();
        queue.add_mapping(200, 20).unwrap();
        queue.add_mapping(300, 30).unwrap();

        let collector = LogIndexTablePropertiesCollector::new(queue.clone());

        // Simulate adding keys
        collector.add(b"key1", b"value1", 150);
        collector.add(b"key2", b"value2", 250);

        // Finish and get properties
        let properties = collector.finish().unwrap();

        // Should have min log index
        assert!(
            properties
                .iter()
                .any(|(k, v)| k == LOG_INDEX_PROPERTY_KEY && v == "10")
        );
    }
}
