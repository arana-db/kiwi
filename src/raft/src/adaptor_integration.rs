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

//! Integration layer for OpenRaft using the Adaptor pattern
//! 
//! This module provides the correct way to integrate with OpenRaft 0.9.21
//! using the Adaptor pattern to work with sealed traits.

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{
    Entry, EntryPayload, LogId, RaftLogId, RaftStorage, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError as OpenraftStorageError, StoredMembership,
};
use tokio::sync::RwLock;

use crate::error::RaftError;
use crate::state_machine::core::{KiwiStateMachine, StateMachineSnapshot};
use crate::storage::core::RaftStorage as KiwiRaftStorage;
use crate::types::{BasicNode, ClientRequest, ClientResponse, NodeId, TypeConfig};

/// Unified storage that combines log storage and state machine for use with Adaptor
pub struct KiwiUnifiedStorage {
    /// Log storage component
    log_storage: Arc<KiwiRaftStorage>,
    /// State machine component
    state_machine: Arc<KiwiStateMachine>,
    /// In-memory cache for snapshots
    snapshots: Arc<RwLock<HashMap<String, StateMachineSnapshot>>>,
}

impl KiwiUnifiedStorage {
    /// Create a new unified storage instance
    pub fn new(node_id: NodeId) -> crate::error::RaftResult<Self> {
        let log_storage = Arc::new(KiwiRaftStorage::new()?);
        let state_machine = Arc::new(KiwiStateMachine::new(node_id));
        let snapshots = Arc::new(RwLock::new(HashMap::new()));

        Ok(Self {
            log_storage,
            state_machine,
            snapshots,
        })
    }

    /// Create a new unified storage instance with custom storage engine
    pub fn with_storage_engine(
        node_id: NodeId,
        storage_engine: Arc<dyn crate::state_machine::core::StorageEngine>,
    ) -> crate::error::RaftResult<Self> {
        let log_storage = Arc::new(KiwiRaftStorage::new()?);
        let state_machine = Arc::new(KiwiStateMachine::with_storage_engine(node_id, storage_engine));
        let snapshots = Arc::new(RwLock::new(HashMap::new()));

        Ok(Self {
            log_storage,
            state_machine,
            snapshots,
        })
    }

    /// Get the state machine component
    pub fn state_machine(&self) -> Arc<KiwiStateMachine> {
        Arc::clone(&self.state_machine)
    }

    /// Get the log storage component
    pub fn log_storage(&self) -> Arc<KiwiRaftStorage> {
        Arc::clone(&self.log_storage)
    }
}

/// Convert RaftError to OpenRaft StorageError
fn to_storage_error(err: RaftError) -> OpenraftStorageError<NodeId> {
    use openraft::{ErrorSubject, ErrorVerb, StorageIOError};

    OpenraftStorageError::IO {
        source: StorageIOError::new(
            ErrorSubject::Store,
            ErrorVerb::Write,
            openraft::AnyError::new(&err),
        ),
    }
}

/// Implement RaftStorage for KiwiUnifiedStorage to work with Adaptor
#[async_trait::async_trait]
impl<'a> RaftStorage<TypeConfig> for KiwiUnifiedStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &openraft::Vote<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        log::debug!("Saving vote: {:?}", vote);

        // Save the current term and voted_for
        self.log_storage.set_current_term(vote.leader_id().get_term())
            .map_err(to_storage_error)?;

        if let Some(voted_for) = vote.leader_id().voted_for() {
            self.log_storage.set_voted_for(Some(voted_for))
                .map_err(to_storage_error)?;
        }

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<NodeId>>, OpenraftStorageError<NodeId>> {
        log::debug!("Reading vote");

        let current_term = self.log_storage.get_current_term();
        let voted_for = self.log_storage.get_voted_for();

        if current_term == 0 {
            return Ok(None);
        }

        let vote = openraft::Vote::new(current_term, voted_for.unwrap_or(0));
        Ok(Some(vote))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Return a clone of self for log reading
        Self {
            log_storage: Arc::clone(&self.log_storage),
            state_machine: Arc::clone(&self.state_machine),
            snapshots: Arc::clone(&self.snapshots),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), OpenraftStorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        log::debug!("Appending log entries");

        for entry in entries {
            // Convert openraft Entry to our StoredLogEntry
            let payload = match &entry.payload {
                EntryPayload::Normal(client_request) => {
                    bincode::serialize(client_request).map_err(|e| {
                        to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency {
                            message: format!("Failed to serialize entry payload: {}", e),
                            context: String::new(),
                        }))
                    })?
                }
                EntryPayload::Blank => Vec::new(),
                EntryPayload::Membership(_) => {
                    bincode::serialize(&entry).map_err(|e| {
                        to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency {
                            message: format!("Failed to serialize membership entry: {}", e),
                            context: String::new(),
                        }))
                    })?
                }
            };

            let stored_entry = crate::storage::core::StoredLogEntry {
                index: entry.log_id.index,
                term: entry.log_id.leader_id.term,
                payload,
            };

            self.log_storage.append_log_entry(&stored_entry)
                .map_err(to_storage_error)?;
        }

        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        log::debug!("Deleting conflict logs since index {}", log_id.index);
        
        self.log_storage.delete_log_entries_from(log_id.index)
            .map_err(to_storage_error)?;
        
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        log::debug!("Purging logs up to index {}", log_id.index);
        
        // For now, we don't implement purging
        // This would typically delete logs that are older than the last snapshot
        Ok(())
    }

    async fn last_applied_state(&mut self) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), OpenraftStorageError<NodeId>> {
        let (log_id, _membership) = self.state_machine.get_applied_state().await;
        Ok((log_id, StoredMembership::default()))
    }

    async fn apply_to_state_machine(&mut self, entries: &[Entry<TypeConfig>]) -> Result<Vec<ClientResponse>, OpenraftStorageError<NodeId>> {
        log::debug!("Applying {} entries to state machine", entries.len());

        let mut responses = Vec::new();

        for entry in entries {
            match &entry.payload {
                EntryPayload::Normal(client_request) => {
                    let response = self.state_machine.apply_redis_command(client_request)
                        .await
                        .map_err(to_storage_error)?;
                    responses.push(response);
                }
                EntryPayload::Blank => {
                    responses.push(ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("OK")),
                        leader_id: Some(self.state_machine.node_id),
                    });
                }
                EntryPayload::Membership(_) => {
                    responses.push(ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("OK")),
                        leader_id: Some(self.state_machine.node_id),
                    });
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            log_storage: Arc::clone(&self.log_storage),
            state_machine: Arc::clone(&self.state_machine),
            snapshots: Arc::clone(&self.snapshots),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, OpenraftStorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), OpenraftStorageError<NodeId>> {
        log::info!("Installing snapshot with meta: {:?}", meta);

        let snapshot_data = snapshot.into_inner();
        let state_machine_snapshot: StateMachineSnapshot = bincode::deserialize(&snapshot_data)
            .map_err(|e| {
                to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency {
                    message: format!("Failed to deserialize snapshot: {}", e),
                    context: String::from("snapshot_operation"),
                }))
            })?;

        self.state_machine.restore_from_snapshot(&state_machine_snapshot)
            .await
            .map_err(to_storage_error)?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, OpenraftStorageError<NodeId>> {
        let snapshot_data_opt = self.state_machine.get_current_snapshot_data()
            .await
            .map_err(to_storage_error)?;

        if let Some(snapshot_data) = snapshot_data_opt {
            let serialized_data = bincode::serialize(&snapshot_data).map_err(|e| {
                to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency {
                    message: format!("Failed to serialize snapshot: {}", e),
                    context: String::from("snapshot_operation"),
                }))
            })?;

            let last_log_id = if snapshot_data.applied_index > 0 {
                Some(LogId::new(
                    openraft::CommittedLeaderId::new(1, self.state_machine.node_id),
                    snapshot_data.applied_index,
                ))
            } else {
                None
            };

            let meta = SnapshotMeta {
                last_log_id,
                last_membership: StoredMembership::default(),
                snapshot_id: format!("snapshot-{}-{}", self.state_machine.node_id, snapshot_data.applied_index),
            };

            let snapshot = Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(serialized_data)),
            };

            Ok(Some(snapshot))
        } else {
            Ok(None)
        }
    }

    async fn get_log_state(&mut self) -> Result<openraft::LogState<TypeConfig>, OpenraftStorageError<NodeId>> {
        let last_log = self.log_storage.get_last_log_entry().map_err(to_storage_error)?;

        let last_log_id = last_log.map(|entry| {
            LogId::new(
                openraft::CommittedLeaderId::new(entry.term, 0),
                entry.index,
            )
        });

        Ok(openraft::LogState {
            last_purged_log_id: None,
            last_log_id,
        })
    }
}

/// Implement RaftLogReader for KiwiUnifiedStorage
#[async_trait::async_trait]
impl openraft::storage::RaftLogReader<TypeConfig> for KiwiUnifiedStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, OpenraftStorageError<NodeId>>
    where
        RB: RangeBounds<u64> + Clone + Send + Sync,
    {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => n + 1,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let stored_entries = self.log_storage.get_log_entries_range(start, end)
            .map_err(to_storage_error)?;

        let mut entries = Vec::new();
        for stored_entry in stored_entries {
            let log_id = LogId::new(
                openraft::CommittedLeaderId::new(stored_entry.term, 0),
                stored_entry.index,
            );

            let payload = if stored_entry.payload.is_empty() {
                EntryPayload::Blank
            } else {
                // Try to deserialize as ClientRequest first
                match bincode::deserialize::<ClientRequest>(&stored_entry.payload) {
                    Ok(client_request) => EntryPayload::Normal(client_request),
                    Err(_) => {
                        // If that fails, try to deserialize as full Entry (for membership changes)
                        match bincode::deserialize::<Entry<TypeConfig>>(&stored_entry.payload) {
                            Ok(entry) => entry.payload,
                            Err(_) => EntryPayload::Blank, // Fallback to blank
                        }
                    }
                }
            };

            let entry = Entry { log_id, payload };
            entries.push(entry);
        }

        Ok(entries)
    }
}

/// Implement RaftSnapshotBuilder for KiwiUnifiedStorage
#[async_trait::async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for KiwiUnifiedStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, OpenraftStorageError<NodeId>> {
        log::info!("Building snapshot");

        let snapshot_data = self.state_machine.create_snapshot()
            .await
            .map_err(to_storage_error)?;

        let serialized_data = bincode::serialize(&snapshot_data).map_err(|e| {
            to_storage_error(RaftError::Storage(crate::error::StorageError::DataInconsistency {
                message: format!("Failed to serialize snapshot: {}", e),
                context: String::from("snapshot_operation"),
            }))
        })?;

        let last_log_id = if snapshot_data.applied_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(1, self.state_machine.node_id),
                snapshot_data.applied_index,
            ))
        } else {
            None
        };

        let snapshot_id = format!("snapshot-{}-{}", self.state_machine.node_id, snapshot_data.applied_index);

        let meta = SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::default(),
            snapshot_id,
        };

        let snapshot = Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(serialized_data)),
        };

        Ok(snapshot)
    }
}

/// Create an Adaptor-based Raft storage for use with OpenRaft
pub fn create_raft_storage(node_id: NodeId) -> crate::error::RaftResult<(
    impl openraft::storage::RaftLogStorage<TypeConfig>,
    impl openraft::storage::RaftStateMachine<TypeConfig>,
)> {
    let unified_storage = KiwiUnifiedStorage::new(node_id)?;
    let (log_storage, state_machine) = Adaptor::new(unified_storage);
    Ok((log_storage, state_machine))
}

/// Create an Adaptor-based Raft storage with custom storage engine
pub fn create_raft_storage_with_engine(
    node_id: NodeId,
    storage_engine: Arc<dyn crate::state_machine::core::StorageEngine>,
) -> crate::error::RaftResult<(
    impl openraft::storage::RaftLogStorage<TypeConfig>,
    impl openraft::storage::RaftStateMachine<TypeConfig>,
)> {
    let unified_storage = KiwiUnifiedStorage::with_storage_engine(node_id, storage_engine)?;
    let (log_storage, state_machine) = Adaptor::new(unified_storage);
    Ok((log_storage, state_machine))
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::{RaftLogStorage, RaftStateMachine};

    #[tokio::test]
    async fn test_adaptor_integration() {
        let node_id = 1;
        let (mut log_storage, mut state_machine) = create_raft_storage(node_id).unwrap();

        // Test log storage
        let log_state = log_storage.get_log_state().await.unwrap();
        assert!(log_state.last_log_id.is_none());

        // Test state machine
        let (applied_log_id, _membership) = state_machine.applied_state().await.unwrap();
        assert!(applied_log_id.is_none());

        println!("✅ Adaptor integration working correctly!");
    }
}