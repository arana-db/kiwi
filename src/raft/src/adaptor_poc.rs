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

// Proof of Concept for Openraft Adaptor Pattern
// This file demonstrates how to correctly use Openraft's Adaptor to work with sealed traits

use std::io::Cursor;
use std::ops::RangeBounds;

use openraft::storage::Adaptor;
use openraft::{
    Entry, EntryPayload, LogId, RaftLogId, RaftStorage, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError as OpenraftStorageError,
};

use crate::types::{BasicNode, ClientResponse, NodeId, TypeConfig};

/// Simple POC storage that implements RaftStorage trait for use with Adaptor
pub struct PocStorage {
    /// Log entries (in-memory for POC)
    logs: Vec<Entry<TypeConfig>>,
    /// Last applied log index
    last_applied: Option<LogId<NodeId>>,
    /// Current vote
    vote: Option<openraft::Vote<NodeId>>,
}

impl PocStorage {
    pub fn new() -> Self {
        Self {
            logs: Vec::new(),
            last_applied: None,
            vote: None,
        }
    }
}

/// Implement RaftStorage trait for POC
#[async_trait::async_trait]
impl RaftStorage<TypeConfig> for PocStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &openraft::Vote<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<NodeId>>, OpenraftStorageError<NodeId>> {
        Ok(self.vote.clone())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        PocStorage {
            logs: self.logs.clone(),
            last_applied: self.last_applied.clone(),
            vote: self.vote.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), OpenraftStorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        for entry in entries {
            self.logs.push(entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        // Find the position of the log_id and remove all logs after it
        if let Some(pos) = self.logs.iter().position(|entry| entry.get_log_id() >= &log_id) {
            self.logs.truncate(pos);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        // Remove all logs up to and including log_id
        if let Some(pos) = self.logs.iter().position(|entry| entry.get_log_id() > &log_id) {
            self.logs.drain(0..pos);
        } else {
            self.logs.clear();
        }
        Ok(())
    }

    async fn last_applied_state(&mut self) -> Result<(Option<LogId<NodeId>>, openraft::StoredMembership<NodeId, BasicNode>), OpenraftStorageError<NodeId>> {
        Ok((self.last_applied.clone(), Default::default()))
    }

    async fn apply_to_state_machine(&mut self, entries: &[Entry<TypeConfig>]) -> Result<Vec<ClientResponse>, OpenraftStorageError<NodeId>> {
        let mut responses = Vec::new();

        for entry in entries {
            self.last_applied = Some(entry.get_log_id().clone());

            match &entry.payload {
                EntryPayload::Blank => {
                    // Blank entries don't need processing
                }
                EntryPayload::Normal(_data) => {
                    // For POC, just create a success response
                    let response = ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("POC_OK")),
                        leader_id: None,
                    };
                    responses.push(response);
                }
                EntryPayload::Membership(_) => {
                    // Membership changes are handled by Openraft
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        PocStorage {
            logs: self.logs.clone(),
            last_applied: self.last_applied.clone(),
            vote: self.vote.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, OpenraftStorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        _snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), OpenraftStorageError<NodeId>> {
        self.last_applied = meta.last_log_id.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, OpenraftStorageError<NodeId>> {
        Ok(None)
    }

    async fn get_log_state(&mut self) -> Result<openraft::LogState<TypeConfig>, OpenraftStorageError<NodeId>> {
        let last_purged_log_id = None;
        let last_log_id = self.logs.last().map(|entry| entry.get_log_id().clone());
        
        Ok(openraft::LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
}

/// Implement RaftLogReader for POC storage
#[async_trait::async_trait]
impl openraft::storage::RaftLogReader<TypeConfig> for PocStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, OpenraftStorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Send + Sync,
    {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n as usize,
            std::ops::Bound::Excluded(&n) => (n + 1) as usize,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => (n + 1) as usize,
            std::ops::Bound::Excluded(&n) => n as usize,
            std::ops::Bound::Unbounded => self.logs.len(),
        };

        Ok(self.logs.get(start..end).unwrap_or(&[]).to_vec())
    }
}

/// Implement RaftSnapshotBuilder for POC storage
#[async_trait::async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for PocStorage {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, OpenraftStorageError<NodeId>>
    {
        let data = Vec::new();
        let snapshot_data = Cursor::new(data);

        let last_log_id = self.logs.last().map(|entry| entry.get_log_id().clone());

        let meta = SnapshotMeta {
            last_log_id,
            last_membership: Default::default(),
            snapshot_id: format!("poc_snapshot_{}", chrono::Utc::now().timestamp()),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(snapshot_data),
        })
    }
}

/// Test function to verify Adaptor usage
#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::{RaftLogReader, RaftStateMachine};

    #[tokio::test]
    async fn test_adaptor_pattern() {
        // Create POC storage
        let storage = PocStorage::new();

        // Use Adaptor to create log store and state machine
        let (mut log_store, mut sm) = Adaptor::new(storage);

        // Test that our implementations work with the traits through Adaptor
        let entries = log_store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(entries.len(), 0);

        let (last_applied, _membership) = sm.applied_state().await.unwrap();
        assert!(last_applied.is_none());

        println!("✅ Adaptor pattern working correctly!");
    }

    #[tokio::test]
    async fn test_raft_log_reader() {
        let storage = PocStorage::new();
        let (mut log_store, _sm) = Adaptor::new(storage);

        // Test try_get_log_entries
        let entries = log_store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_raft_state_machine() {
        let storage = PocStorage::new();
        let (_log_store, mut sm) = Adaptor::new(storage);

        // Test applied_state
        let (last_applied, _membership) = sm.applied_state().await.unwrap();
        assert!(last_applied.is_none());

        // Test apply with empty entries
        let responses = sm.apply(vec![]).await.unwrap();
        assert_eq!(responses.len(), 0);
    }

    #[tokio::test]
    async fn test_snapshot_builder() {
        let storage = PocStorage::new();
        let (_log_store, mut sm) = Adaptor::new(storage);

        // Test get_snapshot_builder and build_snapshot
        let mut builder = sm.get_snapshot_builder().await;
        use openraft::storage::RaftSnapshotBuilder;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert!(snapshot.meta.snapshot_id.starts_with("poc_snapshot_"));
    }
}
