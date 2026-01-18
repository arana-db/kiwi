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

//! RaftStorage adaptor for OpenRaft integration
//!
//! This module provides an adaptor that wraps RaftStorage to implement
//! OpenRaft's RaftStorage trait using the Adaptor pattern.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder,
    RaftStorage, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};

use crate::error::RaftError;
use crate::state_machine::{KiwiStateMachine, StateMachineSnapshot};
use crate::storage::core::{RaftStorage as CoreRaftStorage, StoredLogEntry, StoredSnapshotMeta};
use crate::types::{ClientRequest, ClientResponse, NodeId, TypeConfig};

/// Wrapper around RaftStorage that implements OpenRaft's RaftStorage trait
///
/// This struct uses the Adaptor pattern to bridge our custom RaftStorage
/// implementation with OpenRaft's sealed traits.
#[derive(Clone)]
pub struct RaftStorageAdaptor {
    /// Inner RaftStorage instance
    storage: Arc<CoreRaftStorage>,
    /// State machine for applying log entries
    state_machine: Arc<KiwiStateMachine>,
}

impl Debug for RaftStorageAdaptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftStorageAdaptor")
            .field("node_id", &self.state_machine.node_id)
            .finish()
    }
}

impl RaftStorageAdaptor {
    /// Create a new RaftStorageAdaptor
    pub fn new(storage: Arc<CoreRaftStorage>, state_machine: Arc<KiwiStateMachine>) -> Self {
        Self {
            storage,
            state_machine,
        }
    }

    /// Convert RaftError to OpenRaft StorageError
    fn to_storage_error(err: RaftError) -> StorageError<NodeId> {
        use openraft::{ErrorSubject, ErrorVerb, StorageIOError};

        StorageError::IO {
            source: StorageIOError::new(
                ErrorSubject::Store,
                ErrorVerb::Write,
                openraft::AnyError::new(&err),
            ),
        }
    }

    /// Convert OpenRaft LogId to our internal format
    fn convert_log_id_to_entry(log_id: &LogId<NodeId>) -> (u64, u64) {
        (log_id.leader_id.term, log_id.index)
    }

    /// Convert our internal log entry to OpenRaft Entry
    async fn stored_entry_to_openraft_entry(
        &self,
        stored: &StoredLogEntry,
    ) -> Result<Entry<TypeConfig>, StorageError<NodeId>> {
        // Convert payload based on its variant
        let payload = match &stored.payload {
            crate::storage::core::StoredEntryPayload::Normal(bytes) => {
                let request: ClientRequest = bincode::deserialize(bytes).map_err(|e| {
                    Self::to_storage_error(RaftError::Storage(
                        crate::error::StorageError::DataInconsistency {
                            message: format!("Failed to deserialize log entry payload: {}", e),
                            context: String::from("stored_entry_to_openraft_entry"),
                        },
                    ))
                })?;
                EntryPayload::Normal(request)
            }
            crate::storage::core::StoredEntryPayload::Blank => EntryPayload::Blank,
            crate::storage::core::StoredEntryPayload::Membership(bytes) => {
                let membership: openraft::Membership<NodeId, BasicNode> =
                    bincode::deserialize(bytes).map_err(|e| {
                        Self::to_storage_error(RaftError::Storage(
                            crate::error::StorageError::DataInconsistency {
                                message: format!("Failed to deserialize membership: {}", e),
                                context: String::from("stored_entry_to_openraft_entry"),
                            },
                        ))
                    })?;
                EntryPayload::Membership(membership)
            }
        };

        Ok(Entry {
            log_id: LogId::new(
                openraft::CommittedLeaderId::new(stored.term, self.state_machine.node_id),
                stored.index,
            ),
            payload,
        })
    }

    pub async fn load_snapshot(&mut self) -> Result<(), StorageError<NodeId>> {
        log::info!("Loading snapshot from persistent storage");

        let meta = self
            .storage
            .get_snapshot_meta()
            .map_err(Self::to_storage_error)?;

        let meta = match meta {
            Some(m) => m,
            None => {
                log::info!("No snapshot found in storage");
                return Ok(());
            }
        };

        log::info!(
            "Found snapshot metadata: id = {}, last_log_index = {}, last_log_term = {}",
            meta.snapshot_id,
            meta.last_log_index,
            meta.last_log_term
        );

        let data = self
            .storage
            .get_snapshot_data(&meta.snapshot_id)
            .map_err(Self::to_storage_error)?;

        let data = match data {
            Some(d) => d,
            None => {
                return Err(Self::to_storage_error(RaftError::Storage(
                    crate::error::StorageError::SnapshotRestorationFailed {
                        message: "Snapshot metadata exists but data not found".to_string(),
                        snapshot_id: meta.snapshot_id.clone(),
                        context: String::from("load_snapshot"),
                    },
                )));
            }
        };

        log::info!(
            "Loaded snapshot data: {} bytes for id = {}",
            data.len(),
            meta.snapshot_id
        );

        let snapshot: StateMachineSnapshot = bincode::deserialize(&data).map_err(|e| {
            Self::to_storage_error(RaftError::Storage(
                crate::error::StorageError::SnapshotRestorationFailed {
                    message: format!("Failed to deserialize snapshot: {}", e),
                    snapshot_id: meta.snapshot_id.clone(),
                    context: String::from("load_snapshot"),
                },
            ))
        })?;

        log::info!(
            "Deserialized snapshot: {} keys, applied_index = {}",
            snapshot.data.len(),
            snapshot.applied_index
        );

        let storage_data = bincode::serialize(&snapshot.data).map_err(|e| {
            Self::to_storage_error(RaftError::Storage(
                crate::error::StorageError::SnapshotRestorationFailed {
                    message: format!(
                        "Failed to serialize snapshot data for storage engine: {}",
                        e
                    ),
                    snapshot_id: meta.snapshot_id.clone(),
                    context: String::from("load_snapshot"),
                },
            ))
        })?;

        self.state_machine
            .restore_storage_from_snapshot(&storage_data)
            .await
            .map_err(Self::to_storage_error)?;

        self.state_machine.set_applied_index(meta.last_log_index);
        log::info! {
            "Updated applied_index to {} from snapshot metadata",
            meta.last_log_index
        };

        self.storage
            .purge_logs_upto_async(meta.last_log_index)
            .await
            .map_err(Self::to_storage_error)?;

        log::info!(
            "Purged logs up to index {}, snapshot load complete",
            meta.last_log_index
        );

        Ok(())
    }
}

// Implement RaftStorage trait for RaftStorageAdaptor
impl RaftStorage<TypeConfig> for RaftStorageAdaptor {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Save term and voted_for from the vote
        self.storage
            .set_current_term_async(vote.leader_id().term)
            .await
            .map_err(Self::to_storage_error)?;

        self.storage
            .set_voted_for_async(Some(vote.leader_id().node_id))
            .await
            .map_err(Self::to_storage_error)?;

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let term = self.storage.get_current_term();
        let voted_for = self.storage.get_voted_for();

        if let Some(node_id) = voted_for {
            Ok(Some(Vote::new(term, node_id)))
        } else {
            Ok(None)
        }
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_entry = self
            .storage
            .get_last_log_entry()
            .map_err(Self::to_storage_error)?;

        let last_log_id = if let Some(entry) = last_entry {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(entry.term, self.state_machine.node_id),
                entry.index,
            ))
        } else {
            None
        };

        Ok(LogState {
            last_purged_log_id: None,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        // Collect entries into a Vec first to avoid Send issues with the iterator
        let entries_vec: Vec<_> = entries.into_iter().collect();

        for entry in entries_vec {
            let (term, index) = Self::convert_log_id_to_entry(&entry.log_id);

            // Serialize the payload based on its type
            let payload = match &entry.payload {
                EntryPayload::Normal(request) => {
                    let serialized = bincode::serialize(request).map_err(|e| {
                        Self::to_storage_error(RaftError::Storage(
                            crate::error::StorageError::DataInconsistency {
                                message: format!("Failed to serialize log entry: {}", e),
                                context: String::from("append_to_log"),
                            },
                        ))
                    })?;
                    crate::storage::core::StoredEntryPayload::Normal(serialized)
                }
                EntryPayload::Blank => crate::storage::core::StoredEntryPayload::Blank,
                EntryPayload::Membership(membership) => {
                    let serialized = bincode::serialize(membership).map_err(|e| {
                        Self::to_storage_error(RaftError::Storage(
                            crate::error::StorageError::DataInconsistency {
                                message: format!("Failed to serialize membership: {}", e),
                                context: String::from("append_to_log"),
                            },
                        ))
                    })?;
                    crate::storage::core::StoredEntryPayload::Membership(serialized)
                }
            };

            let stored_entry = StoredLogEntry {
                index,
                term,
                payload,
            };

            self.storage
                .append_log_entry_async(&stored_entry)
                .await
                .map_err(Self::to_storage_error)?;
        }

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        self.storage
            .delete_log_entries_from_async(log_id.index)
            .await
            .map_err(Self::to_storage_error)?;

        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Delete all logs up to (but not including) the specified log_id
        self.storage
            .purge_logs_upto_async(log_id.index)
            .await
            .map_err(Self::to_storage_error)?;

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let applied_index = self.state_machine.applied_index();

        let log_id = if applied_index > 0 {
            // Get the term from the log entry at applied_index
            let entry = self
                .storage
                .get_log_entry(applied_index)
                .map_err(Self::to_storage_error)?;

            if let Some(entry) = entry {
                Some(LogId::new(
                    openraft::CommittedLeaderId::new(entry.term, self.state_machine.node_id),
                    entry.index,
                ))
            } else {
                None
            }
        } else {
            None
        };

        // Return empty membership for now
        // TODO: Track membership changes properly
        let membership = StoredMembership::default();

        Ok((log_id, membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<ClientResponse>, StorageError<NodeId>> {
        let mut responses = Vec::new();

        for entry in entries {
            let response = match &entry.payload {
                EntryPayload::Blank => ClientResponse {
                    id: crate::types::RequestId::new(),
                    result: Ok(bytes::Bytes::from("OK")),
                    leader_id: None,
                },
                EntryPayload::Normal(request) => self
                    .state_machine
                    .apply_redis_command(request)
                    .await
                    .map_err(Self::to_storage_error)?,
                EntryPayload::Membership(_) => ClientResponse {
                    id: crate::types::RequestId::new(),
                    result: Ok(bytes::Bytes::from("OK")),
                    leader_id: None,
                },
            };

            responses.push(response);

            // Update applied index
            self.state_machine.set_applied_index(entry.log_id.index);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();

        // Deserialize the snapshot
        if !data.is_empty() {
            let snapshot_data: StateMachineSnapshot = bincode::deserialize(&data).map_err(|e| {
                Self::to_storage_error(RaftError::Storage(
                    crate::error::StorageError::SnapshotRestorationFailed {
                        message: format!("Failed to deserialize snapshot: {}", e),
                        snapshot_id: meta.snapshot_id.clone(),
                        context: String::from("install_snapshot"),
                    },
                ))
            })?;

            // Restore the state machine from snapshot
            self.state_machine
                .restore_from_snapshot(&snapshot_data)
                .await
                .map_err(Self::to_storage_error)?;
        }

        // Store snapshot metadata
        if let Some(last_log_id) = meta.last_log_id {
            let stored_meta = StoredSnapshotMeta {
                last_log_index: last_log_id.index,
                last_log_term: last_log_id.leader_id.term,
                snapshot_id: meta.snapshot_id.clone(),
                timestamp: chrono::Utc::now().timestamp(),
            };

            self.storage
                .store_snapshot_meta(&stored_meta)
                .map_err(Self::to_storage_error)?;
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let snapshot_meta = self
            .storage
            .get_snapshot_meta()
            .map_err(Self::to_storage_error)?;

        if let Some(meta) = snapshot_meta {
            // Get the snapshot data from state machine
            let snapshot_data = self
                .state_machine
                .get_current_snapshot_data()
                .await
                .map_err(Self::to_storage_error)?;

            if let Some(data) = snapshot_data {
                // Serialize the snapshot
                let serialized = bincode::serialize(&data).map_err(|e| {
                    Self::to_storage_error(RaftError::Storage(
                        crate::error::StorageError::SnapshotCreationFailed {
                            message: format!("Failed to serialize snapshot: {}", e),
                            snapshot_id: meta.snapshot_id.clone(),
                            context: String::from("get_current_snapshot"),
                        },
                    ))
                })?;

                let openraft_meta = SnapshotMeta {
                    last_log_id: Some(LogId::new(
                        openraft::CommittedLeaderId::new(
                            meta.last_log_term,
                            self.state_machine.node_id,
                        ),
                        meta.last_log_index,
                    )),
                    last_membership: StoredMembership::default(),
                    snapshot_id: meta.snapshot_id,
                };

                return Ok(Some(Snapshot {
                    meta: openraft_meta,
                    snapshot: Box::new(Cursor::new(serialized)),
                }));
            }
        }

        Ok(None)
    }
}

// Implement RaftLogReader
impl RaftLogReader<TypeConfig> for RaftStorageAdaptor {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        use std::ops::Bound;

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => u64::MAX,
        };

        let stored_entries = self
            .storage
            .get_log_entries_range_async(start, end)
            .await
            .map_err(Self::to_storage_error)?;

        let mut entries = Vec::new();
        for stored in stored_entries {
            let entry = self.stored_entry_to_openraft_entry(&stored).await?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

// Implement RaftSnapshotBuilder
impl RaftSnapshotBuilder<TypeConfig> for RaftStorageAdaptor {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Create a snapshot from the state machine
        let snapshot_data = self
            .state_machine
            .create_snapshot()
            .await
            .map_err(Self::to_storage_error)?;

        let applied_index = snapshot_data.applied_index;

        // Get the term from the log entry at applied_index
        let term = if applied_index > 0 {
            let entry = self
                .storage
                .get_log_entry(applied_index)
                .map_err(Self::to_storage_error)?;

            entry.map(|e| e.term).unwrap_or(0)
        } else {
            0
        };

        // Serialize the snapshot
        let serialized = bincode::serialize(&snapshot_data).map_err(|e| {
            Self::to_storage_error(RaftError::Storage(
                crate::error::StorageError::SnapshotCreationFailed {
                    message: format!("Failed to serialize snapshot: {}", e),
                    snapshot_id: format!("snapshot_{}", applied_index),
                    context: String::from("build_snapshot"),
                },
            ))
        })?;

        let snapshot_id = format!("snapshot_{}", applied_index);

        // Store snapshot metadata
        let stored_meta = StoredSnapshotMeta {
            last_log_index: applied_index,
            last_log_term: term,
            snapshot_id: snapshot_id.clone(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        self.storage
            .store_snapshot_meta(&stored_meta)
            .map_err(Self::to_storage_error)?;

        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(
                openraft::CommittedLeaderId::new(term, self.state_machine.node_id),
                applied_index,
            )),
            last_membership: StoredMembership::default(),
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(serialized)),
        })
    }
}

/// Create a RaftStorage adaptor using the Adaptor pattern
pub fn create_raft_storage_adaptor(
    storage: Arc<CoreRaftStorage>,
    state_machine: Arc<KiwiStateMachine>,
) -> (
    impl openraft::storage::RaftLogStorage<TypeConfig>,
    impl openraft::storage::RaftStateMachine<TypeConfig>,
) {
    let adaptor = RaftStorageAdaptor::new(storage, state_machine);
    Adaptor::new(adaptor)
}
