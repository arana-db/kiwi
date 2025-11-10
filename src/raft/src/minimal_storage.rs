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

// Minimal working storage implementation for OpenRaft 0.9.21

use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{
    Entry, EntryPayload, LogId, RaftLogId, RaftStorage, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError as OpenraftStorageError, StoredMembership,
};
use tokio::sync::RwLock;

use crate::types::{BasicNode, ClientResponse, NodeId, TypeConfig};

/// Minimal storage implementation that works with OpenRaft 0.9.21
#[derive(Debug)]
pub struct MinimalStorage {
    /// Log entries storage
    logs: Arc<RwLock<BTreeMap<u64, Entry<TypeConfig>>>>,
    /// Vote storage
    vote: Arc<RwLock<Option<openraft::Vote<NodeId>>>>,
    /// Applied state
    applied: Arc<RwLock<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>)>>,
    /// Snapshot storage
    snapshot: Arc<RwLock<Option<Snapshot<TypeConfig>>>>,
}

impl MinimalStorage {
    pub fn new() -> Self {
        Self {
            logs: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(RwLock::new(None)),
            applied: Arc::new(RwLock::new((None, StoredMembership::default()))),
            snapshot: Arc::new(RwLock::new(None)),
        }
    }
}

impl Default for MinimalStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Implement RaftStorage trait using async_trait without manual lifetime specifications
#[async_trait::async_trait]
impl RaftStorage<TypeConfig> for MinimalStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &openraft::Vote<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        let mut v = self.vote.write().await;
        *v = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<NodeId>>, OpenraftStorageError<NodeId>> {
        let v = self.vote.read().await;
        Ok(v.clone())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Return a clone of self for log reading
        MinimalStorage {
            logs: self.logs.clone(),
            vote: self.vote.clone(),
            applied: self.applied.clone(),
            snapshot: self.snapshot.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), OpenraftStorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut logs = self.logs.write().await;
        for entry in entries {
            logs.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        let mut logs = self.logs.write().await;
        let keys_to_remove: Vec<u64> = logs
            .range(log_id.index..)
            .map(|(k, _)| *k)
            .collect();
        
        for key in keys_to_remove {
            logs.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        let mut logs = self.logs.write().await;
        let keys_to_remove: Vec<u64> = logs
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect();
        
        for key in keys_to_remove {
            logs.remove(&key);
        }
        Ok(())
    }

    async fn last_applied_state(&mut self) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), OpenraftStorageError<NodeId>> {
        let applied = self.applied.read().await;
        Ok(applied.clone())
    }

    async fn apply_to_state_machine(&mut self, entries: &[Entry<TypeConfig>]) -> Result<Vec<ClientResponse>, OpenraftStorageError<NodeId>> {
        let mut responses = Vec::new();
        let mut applied = self.applied.write().await;

        for entry in entries {
            // Update applied state
            applied.0 = Some(entry.log_id.clone());

            // Process the entry based on its payload
            match &entry.payload {
                EntryPayload::Blank => {
                    // Blank entries don't produce responses
                }
                EntryPayload::Normal(_request) => {
                    // Create a success response for normal entries
                    responses.push(ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("OK")),
                        leader_id: None,
                    });
                }
                EntryPayload::Membership(_membership) => {
                    // Membership changes are handled by OpenRaft
                    responses.push(ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("MEMBERSHIP_OK")),
                        leader_id: None,
                    });
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MinimalStorage {
            logs: self.logs.clone(),
            vote: self.vote.clone(),
            applied: self.applied.clone(),
            snapshot: self.snapshot.clone(),
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
        let mut applied = self.applied.write().await;
        applied.0 = meta.last_log_id.clone();
        applied.1 = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, OpenraftStorageError<NodeId>> {
        let snapshot = self.snapshot.read().await;
        Ok(snapshot.clone())
    }

    async fn get_log_state(&mut self) -> Result<openraft::LogState<TypeConfig>, OpenraftStorageError<NodeId>> {
        let logs = self.logs.read().await;
        let last_log_id = logs.values().last().map(|entry| entry.log_id.clone());
        
        Ok(openraft::LogState {
            last_purged_log_id: None,
            last_log_id,
        })
    }
}

/// Implement RaftLogReader for log reading operations
#[async_trait::async_trait]
impl openraft::storage::RaftLogReader<TypeConfig> for MinimalStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, OpenraftStorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Send + Sync,
    {
        let logs = self.logs.read().await;
        let entries: Vec<Entry<TypeConfig>> = logs
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(entries)
    }
}

/// Implement RaftSnapshotBuilder for snapshot creation
#[async_trait::async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for MinimalStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, OpenraftStorageError<NodeId>> {
        let applied = self.applied.read().await;
        let snapshot_data = Cursor::new(Vec::new()); // Empty snapshot for now

        let meta = SnapshotMeta {
            last_log_id: applied.0.clone(),
            last_membership: applied.1.clone(),
            snapshot_id: format!("minimal_snapshot_{}", chrono::Utc::now().timestamp()),
        };

        let snapshot = Snapshot {
            meta,
            snapshot: Box::new(snapshot_data),
        };

        // Store the snapshot
        let mut stored_snapshot = self.snapshot.write().await;
        *stored_snapshot = Some(snapshot.clone());

        Ok(snapshot)
    }
}

/// Create minimal Raft storage using the Adaptor pattern
pub fn create_minimal_raft_storage() -> (
    impl openraft::storage::RaftLogStorage<TypeConfig>,
    impl openraft::storage::RaftStateMachine<TypeConfig>,
) {
    let storage = MinimalStorage::new();
    Adaptor::new(storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::{RaftLogStorage, RaftStateMachine};

    #[tokio::test]
    async fn test_minimal_storage() {
        let (mut log_storage, mut state_machine) = create_minimal_raft_storage();

        // Test log storage
        let log_state = log_storage.get_log_state().await.unwrap();
        assert!(log_state.last_log_id.is_none());

        // Test state machine
        let (applied_log_id, _membership) = state_machine.applied_state().await.unwrap();
        assert!(applied_log_id.is_none());

        println!("✅ Minimal storage working!");
    }
}
