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

//! Replication mode compatibility (Master-Slave and Raft)
//!
//! This module provides compatibility between master-slave mode and Raft mode.
//! It allows using the same binlog format for both modes, with dynamic switching.

use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::binlog::{BinlogEntry, OperationType};
use crate::error::RaftError;
use crate::segment_log::SegmentLog;
use crate::types::{LogIndex, Term};
use bytes::Bytes;

/// Replication mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Master-slave replication mode
    MasterSlave,
    /// Raft consensus mode
    Raft,
}

/// Replication mode manager
pub struct ReplicationModeManager {
    /// Current replication mode
    mode: Arc<RwLock<ReplicationMode>>,
    /// Segment log for storing binlog entries
    segment_log: Arc<SegmentLog>,
    /// Fake term for master-slave mode (to reuse Raft log format)
    fake_term: Term,
    /// Current log index counter (for master-slave mode)
    current_log_index: Arc<RwLock<LogIndex>>,
    /// Is this node the master/leader
    is_leader: Arc<AtomicBool>,
}

impl ReplicationModeManager {
    /// Create a new replication mode manager
    pub fn new<P: AsRef<std::path::Path>>(
        segment_log: Arc<SegmentLog>,
        initial_mode: ReplicationMode,
    ) -> Result<Self, RaftError> {
        let fake_term = 0; // Use term 0 for master-slave mode

        Ok(Self {
            mode: Arc::new(RwLock::new(initial_mode)),
            segment_log,
            fake_term,
            current_log_index: Arc::new(RwLock::new(1)),
            is_leader: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Get current replication mode
    pub fn get_mode(&self) -> ReplicationMode {
        *self.mode.read()
    }

    /// Switch replication mode
    pub fn switch_mode(&self, new_mode: ReplicationMode) -> Result<(), RaftError> {
        let mut mode = self.mode.write();
        let old_mode = *mode;

        if old_mode == new_mode {
            return Ok(());
        }

        // Flush current segment before switching
        self.segment_log.flush()?;

        *mode = new_mode;

        log::info!(
            "Replication mode switched from {:?} to {:?}",
            old_mode,
            new_mode
        );

        Ok(())
    }

    /// Write binlog entry (master-slave mode)
    /// Uses fake term to reuse Raft log format
    pub fn write_binlog_master_slave(
        &self,
        operation: OperationType,
        data: Bytes,
    ) -> Result<LogIndex, RaftError> {
        // Get next log index
        let log_index = {
            let mut current = self.current_log_index.write();
            let index = *current;
            *current += 1;
            index
        };

        // Create binlog entry with fake term
        let entry = BinlogEntry::new(operation, data).with_raft_info(log_index, self.fake_term);

        // Append to segment log
        self.segment_log.append_entry(&entry)?;

        Ok(log_index)
    }

    /// Write binlog entry (Raft mode)
    /// Uses actual Raft term and log index
    pub fn write_binlog_raft(
        &self,
        operation: OperationType,
        data: Bytes,
        log_index: LogIndex,
        term: Term,
    ) -> Result<(), RaftError> {
        // Create binlog entry with real Raft info
        let entry = BinlogEntry::new(operation, data).with_raft_info(log_index, term);

        // Append to segment log
        self.segment_log.append_entry(&entry)?;

        Ok(())
    }

    /// Write binlog entry (automatic mode detection)
    pub fn write_binlog(
        &self,
        operation: OperationType,
        data: Bytes,
        log_index: Option<LogIndex>,
        term: Option<Term>,
    ) -> Result<LogIndex, RaftError> {
        match self.get_mode() {
            ReplicationMode::MasterSlave => {
                // Master-slave mode: use internal counter
                self.write_binlog_master_slave(operation, data)
            }
            ReplicationMode::Raft => {
                // Raft mode: use provided log index and term
                let log_idx = log_index.ok_or_else(|| {
                    RaftError::invalid_request("Log index required for Raft mode")
                })?;
                let raft_term =
                    term.ok_or_else(|| RaftError::invalid_request("Term required for Raft mode"))?;

                self.write_binlog_raft(operation, data, log_idx, raft_term)?;
                Ok(log_idx)
            }
        }
    }

    /// Check if current mode is master-slave
    pub fn is_master_slave(&self) -> bool {
        self.get_mode() == ReplicationMode::MasterSlave
    }

    /// Check if current mode is Raft
    pub fn is_raft(&self) -> bool {
        self.get_mode() == ReplicationMode::Raft
    }

    /// Set leader status (for master-slave mode)
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader.store(is_leader, Ordering::SeqCst);
    }

    /// Check if this node is leader/master
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Get current log index (for master-slave mode)
    pub fn get_current_log_index(&self) -> LogIndex {
        *self.current_log_index.read()
    }

    /// Set current log index (for recovery)
    pub fn set_current_log_index(&self, log_index: LogIndex) {
        let mut current = self.current_log_index.write();
        *current = log_index;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_mode_switching() {
        let temp_dir = TempDir::new().unwrap();
        let segment_log = Arc::new(SegmentLog::new(temp_dir.path(), None).unwrap());

        let manager =
            ReplicationModeManager::new::<&std::path::Path>(segment_log, ReplicationMode::MasterSlave).unwrap();

        assert!(manager.is_master_slave());
        assert!(!manager.is_raft());

        // Switch to Raft mode
        manager.switch_mode(ReplicationMode::Raft).unwrap();
        assert!(manager.is_raft());
        assert!(!manager.is_master_slave());
    }

    #[test]
    fn test_write_binlog_master_slave() {
        let temp_dir = TempDir::new().unwrap();
        let segment_log = Arc::new(SegmentLog::new(temp_dir.path(), None).unwrap());

        let manager =
            ReplicationModeManager::new::<&std::path::Path>(segment_log, ReplicationMode::MasterSlave).unwrap();

        let log_index1 = manager
            .write_binlog(
                OperationType::Put,
                Bytes::from("SET key1 value1"),
                None,
                None,
            )
            .unwrap();

        assert_eq!(log_index1, 1);

        let log_index2 = manager
            .write_binlog(
                OperationType::Put,
                Bytes::from("SET key2 value2"),
                None,
                None,
            )
            .unwrap();

        assert_eq!(log_index2, 2);
    }

    #[test]
    fn test_write_binlog_raft() {
        let temp_dir = TempDir::new().unwrap();
        let segment_log = Arc::new(SegmentLog::new(temp_dir.path(), None).unwrap());

        let manager = ReplicationModeManager::new::<&std::path::Path>(segment_log, ReplicationMode::Raft).unwrap();

        manager
            .write_binlog(
                OperationType::Put,
                Bytes::from("SET key1 value1"),
                Some(100),
                Some(5),
            )
            .unwrap();

        // Verify log index was used
        assert_eq!(manager.get_current_log_index(), 1); // Still 1 (not used in Raft mode)
    }
}
