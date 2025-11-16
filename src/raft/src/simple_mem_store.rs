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

//! Simple memory store using OpenRaft's Adaptor pattern
//!
//! This implementation uses OpenRaft's `Adaptor` to wrap a simple storage
//! that implements the non-sealed helper traits.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::BasicNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::Vote;
use openraft::storage::Adaptor;
use tokio::sync::RwLock;

use crate::error::RaftResult;
use crate::types::{NodeId, TypeConfig};

/// Simple in-memory storage that implements RaftStorage
#[derive(Debug)]
pub struct SimpleMemStore {
    vote: Arc<RwLock<Option<Vote<NodeId>>>>,
    logs: Arc<RwLock<BTreeMap<u64, Entry<TypeConfig>>>>,
    committed: Arc<RwLock<Option<LogId<NodeId>>>>,
    applied: Arc<RwLock<Option<LogId<NodeId>>>>,
    snapshot: Arc<RwLock<Option<StoredSnapshot>>>,
    membership: Arc<RwLock<StoredMembership<NodeId, BasicNode>>>,
    /// In-memory key-value store for state machine
    kv_store: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    /// Optional data directory for persistence
    data_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotData {
    kv_store: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

impl SimpleMemStore {
    pub fn new() -> Self {
        Self::with_data_dir(None)
    }

    pub fn with_data_dir(data_dir: Option<PathBuf>) -> Self {
        let store = Self {
            vote: Arc::new(RwLock::new(None)),
            logs: Arc::new(RwLock::new(BTreeMap::new())),
            committed: Arc::new(RwLock::new(None)),
            applied: Arc::new(RwLock::new(None)),
            snapshot: Arc::new(RwLock::new(None)),
            membership: Arc::new(RwLock::new(StoredMembership::default())),
            kv_store: Arc::new(RwLock::new(HashMap::new())),
            data_dir: data_dir.clone(),
        };

        // Try to load persisted state if data_dir is provided
        if let Some(ref dir) = data_dir {
            if let Err(e) = std::fs::create_dir_all(dir) {
                log::warn!("Failed to create data directory: {}", e);
            } else {
                // Try to load vote
                let vote_path = dir.join("vote.bin");
                if vote_path.exists() {
                    if let Ok(data) = std::fs::read(&vote_path) {
                        if let Ok(vote) = bincode::deserialize::<Vote<NodeId>>(&data) {
                            if let Ok(mut v) = store.vote.try_write() {
                                *v = Some(vote);
                                log::info!("Loaded vote from disk");
                            }
                        }
                    }
                }

                // Try to load logs
                let logs_path = dir.join("logs.bin");
                if logs_path.exists() {
                    if let Ok(data) = std::fs::read(&logs_path) {
                        if let Ok(logs_vec) = bincode::deserialize::<Vec<Entry<TypeConfig>>>(&data)
                        {
                            if let Ok(mut logs) = store.logs.try_write() {
                                for entry in logs_vec {
                                    logs.insert(entry.log_id.index, entry);
                                }
                                log::info!("Loaded {} log entries from disk", logs.len());

                                // DON'T set applied index here - let OpenRaft replay the logs
                                // This ensures the state machine gets updated properly
                            }
                        }
                    }
                }

                // Try to load applied index
                let applied_path = dir.join("applied.bin");
                if applied_path.exists() {
                    if let Ok(data) = std::fs::read(&applied_path) {
                        if let Ok(applied_log_id) =
                            bincode::deserialize::<Option<LogId<NodeId>>>(&data)
                        {
                            if let Ok(mut applied) = store.applied.try_write() {
                                *applied = applied_log_id;
                                log::info!("Loaded applied index: {:?}", applied_log_id);
                            }
                        }
                    }
                }

                // Try to load membership
                let membership_path = dir.join("membership.bin");
                if membership_path.exists() {
                    if let Ok(data) = std::fs::read(&membership_path) {
                        if let Ok(stored_membership) =
                            bincode::deserialize::<StoredMembership<NodeId, BasicNode>>(&data)
                        {
                            if let Ok(mut membership) = store.membership.try_write() {
                                *membership = stored_membership;
                                log::info!("Loaded membership from disk");
                            }
                        }
                    }
                }

                // Try to load kv_store state
                let kv_path = dir.join("kv_store.bin");
                if kv_path.exists() {
                    if let Ok(data) = std::fs::read(&kv_path) {
                        if let Ok(snapshot_data) = bincode::deserialize::<SnapshotData>(&data) {
                            if let Ok(mut kv_store) = store.kv_store.try_write() {
                                *kv_store = snapshot_data.kv_store;
                                log::info!("Loaded {} keys from kv_store", kv_store.len());
                            }
                        }
                    }
                }

                // Try to load snapshot metadata to know the applied index (fallback)
                let snapshot_meta_path = dir.join("snapshot_meta.bin");
                if snapshot_meta_path.exists() {
                    if let Ok(data) = std::fs::read(&snapshot_meta_path) {
                        if let Ok(meta) =
                            bincode::deserialize::<SnapshotMeta<NodeId, BasicNode>>(&data)
                        {
                            if let Ok(mut membership) = store.membership.try_write() {
                                *membership = meta.last_membership.clone();
                            }
                        }
                    }
                }

                // Try to load snapshot (fallback for kv_store)
                let snapshot_path = dir.join("snapshot.bin");
                if snapshot_path.exists() && !kv_path.exists() {
                    if let Ok(data) = std::fs::read(&snapshot_path) {
                        if let Ok(snapshot_data) = bincode::deserialize::<SnapshotData>(&data) {
                            // Load the kv_store synchronously during initialization
                            if let Ok(mut kv_store) = store.kv_store.try_write() {
                                *kv_store = snapshot_data.kv_store;
                                log::info!("Loaded {} keys from snapshot", kv_store.len());
                            }
                        }
                    }
                }
            }
        }

        store
    }

    /// Execute a Redis command on the in-memory store
    async fn execute_command(
        &self,
        request: &crate::types::ClientRequest,
    ) -> RaftResult<bytes::Bytes> {
        use bytes::Bytes;

        let cmd = &request.command;
        match cmd.command.to_uppercase().as_str() {
            "SET" => {
                if cmd.args.len() < 2 {
                    return Err(crate::error::RaftError::invalid_request(
                        "SET requires key and value",
                    ));
                }
                let key = cmd.args[0].to_vec();
                let value = cmd.args[1].to_vec();

                let mut store = self.kv_store.write().await;
                store.insert(key, value);
                Ok(Bytes::from_static(b"OK"))
            }
            "GET" => {
                if cmd.args.is_empty() {
                    return Err(crate::error::RaftError::invalid_request("GET requires key"));
                }
                let key = &cmd.args[0];

                let store = self.kv_store.read().await;
                match store.get(key.as_ref()) {
                    Some(value) => Ok(Bytes::from(value.clone())),
                    None => Ok(Bytes::new()), // Return empty for non-existent key
                }
            }
            "DEL" => {
                if cmd.args.is_empty() {
                    return Err(crate::error::RaftError::invalid_request(
                        "DEL requires at least one key",
                    ));
                }

                let mut store = self.kv_store.write().await;
                let mut deleted_count = 0;
                for key in &cmd.args {
                    if store.remove(key.as_ref()).is_some() {
                        deleted_count += 1;
                    }
                }
                Ok(Bytes::from(deleted_count.to_string()))
            }
            "EXISTS" => {
                if cmd.args.is_empty() {
                    return Err(crate::error::RaftError::invalid_request(
                        "EXISTS requires at least one key",
                    ));
                }

                let store = self.kv_store.read().await;
                let mut exists_count = 0;
                for key in &cmd.args {
                    if store.contains_key(key.as_ref()) {
                        exists_count += 1;
                    }
                }
                Ok(Bytes::from(exists_count.to_string()))
            }
            "PING" => Ok(Bytes::from_static(b"PONG")),
            _ => Err(crate::error::RaftError::state_machine(format!(
                "Unsupported command: {}",
                cmd.command
            ))),
        }
    }
}

// Implement RaftStorage - this is the sealed trait that Adaptor requires
impl RaftStorage<TypeConfig> for SimpleMemStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut v = self.vote.write().await;
        *v = Some(*vote);

        // Persist vote to disk if data_dir is set
        if let Some(ref dir) = self.data_dir {
            let vote_path = dir.join("vote.bin");
            if let Ok(data) = bincode::serialize(vote) {
                if let Err(e) = std::fs::write(&vote_path, &data) {
                    log::warn!("Failed to persist vote to disk: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let v = self.vote.read().await;
        Ok(*v)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let logs = self.logs.read().await;
        let last_log_id = logs.values().last().map(|entry| entry.log_id);
        let last_purged_log_id = None;

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            vote: self.vote.clone(),
            logs: self.logs.clone(),
            committed: self.committed.clone(),
            applied: self.applied.clone(),
            snapshot: self.snapshot.clone(),
            membership: self.membership.clone(),
            kv_store: self.kv_store.clone(),
            data_dir: self.data_dir.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut logs = self.logs.write().await;
        for entry in entries {
            logs.insert(entry.log_id.index, entry);
        }

        // Persist logs to disk if data_dir is set
        if let Some(ref dir) = self.data_dir {
            let logs_path = dir.join("logs.bin");
            let logs_vec: Vec<_> = logs.values().cloned().collect();
            if let Ok(data) = bincode::serialize(&logs_vec) {
                if let Err(e) = std::fs::write(&logs_path, &data) {
                    log::warn!("Failed to persist logs to disk: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut logs = self.logs.write().await;
        logs.retain(|&index, _| index < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut logs = self.logs.write().await;
        logs.retain(|&index, _| index > log_id.index);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let applied = self.applied.read().await;
        let membership = self.membership.read().await;
        Ok((*applied, membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<crate::types::ClientResponse>, StorageError<NodeId>> {
        let mut applied = self.applied.write().await;
        let mut membership = self.membership.write().await;
        let mut responses = Vec::new();

        for entry in entries {
            *applied = Some(entry.log_id);

            // Create a response for each entry
            let response = match &entry.payload {
                EntryPayload::Blank => crate::types::ClientResponse {
                    id: crate::types::RequestId::new(),
                    result: Ok(bytes::Bytes::from("OK")),
                    leader_id: None,
                },
                EntryPayload::Normal(ref request) => {
                    // Execute the command and get the actual result
                    let result = self.execute_command(request).await;
                    crate::types::ClientResponse {
                        id: request.id,
                        result: result.map_err(|e| e.to_string()),
                        leader_id: None,
                    }
                }
                EntryPayload::Membership(ref m) => {
                    *membership = StoredMembership::new(Some(entry.log_id), m.clone());
                    crate::types::ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("OK")),
                        leader_id: None,
                    }
                }
            };
            responses.push(response);
        }

        // Persist the kv_store state after applying entries
        if let Some(ref dir) = self.data_dir {
            let kv_path = dir.join("kv_store.bin");
            let kv_store = self.kv_store.read().await;
            let snapshot_data = SnapshotData {
                kv_store: kv_store.clone(),
            };
            if let Ok(data) = bincode::serialize(&snapshot_data) {
                if let Err(e) = std::fs::write(&kv_path, &data) {
                    log::warn!("Failed to persist kv_store to disk: {}", e);
                }
            }

            // Also persist the applied index
            let applied_path = dir.join("applied.bin");
            if let Ok(data) = bincode::serialize(&*applied) {
                if let Err(e) = std::fs::write(&applied_path, &data) {
                    log::warn!("Failed to persist applied index to disk: {}", e);
                }
            }

            // Persist the membership
            let membership_path = dir.join("membership.bin");
            if let Ok(data) = bincode::serialize(&*membership) {
                if let Err(e) = std::fs::write(&membership_path, &data) {
                    log::warn!("Failed to persist membership to disk: {}", e);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            vote: self.vote.clone(),
            logs: self.logs.clone(),
            committed: self.committed.clone(),
            applied: self.applied.clone(),
            snapshot: self.snapshot.clone(),
            membership: self.membership.clone(),
            kv_store: self.kv_store.clone(),
            data_dir: self.data_dir.clone(),
        }
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

        // Deserialize and restore the kv_store
        if !data.is_empty() {
            if let Ok(snapshot_data) = bincode::deserialize::<SnapshotData>(&data) {
                let mut kv_store = self.kv_store.write().await;
                *kv_store = snapshot_data.kv_store;
            }
        }

        let mut snap = self.snapshot.write().await;
        *snap = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });

        let mut applied = self.applied.write().await;
        *applied = meta.last_log_id;

        let mut membership = self.membership.write().await;
        *membership = meta.last_membership.clone();

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let snapshot = self.snapshot.read().await;

        if let Some(snap) = snapshot.as_ref() {
            Ok(Some(Snapshot {
                meta: snap.meta.clone(),
                snapshot: Box::new(Cursor::new(snap.data.clone())),
            }))
        } else {
            Ok(None)
        }
    }
}

// Implement RaftLogReader
impl RaftLogReader<TypeConfig> for SimpleMemStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let logs = self.logs.read().await;
        let entries: Vec<_> = logs.range(range).map(|(_, entry)| entry.clone()).collect();
        Ok(entries)
    }
}

// Implement RaftSnapshotBuilder
impl RaftSnapshotBuilder<TypeConfig> for SimpleMemStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let applied = self.applied.read().await;
        let membership = self.membership.read().await;
        let kv_store = self.kv_store.read().await;

        // Serialize the current kv_store state
        let snapshot_data = SnapshotData {
            kv_store: kv_store.clone(),
        };

        let data = bincode::serialize(&snapshot_data).map_err(|e| {
            use openraft::{ErrorSubject, ErrorVerb, StorageIOError};
            StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                ),
            }
        })?;

        let meta = SnapshotMeta {
            last_log_id: *applied,
            last_membership: membership.clone(),
            snapshot_id: format!(
                "snapshot_{}",
                applied.as_ref().map(|l| l.index).unwrap_or(0)
            ),
        };

        // Store the snapshot in memory
        let mut snapshot = self.snapshot.write().await;
        *snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        // Persist to disk if data_dir is set
        if let Some(ref dir) = self.data_dir {
            let snapshot_path = dir.join("snapshot.bin");
            let snapshot_meta_path = dir.join("snapshot_meta.bin");

            if let Err(e) = std::fs::write(&snapshot_path, &data) {
                log::warn!("Failed to persist snapshot to disk: {}", e);
            } else {
                log::info!("Persisted snapshot with {} keys to disk", kv_store.len());
            }

            // Also persist the metadata
            if let Ok(meta_data) = bincode::serialize(&meta) {
                if let Err(e) = std::fs::write(&snapshot_meta_path, &meta_data) {
                    log::warn!("Failed to persist snapshot metadata to disk: {}", e);
                }
            }
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

/// Create a new simple memory store using the Adaptor pattern
pub fn create_mem_store() -> (
    impl openraft::storage::RaftLogStorage<TypeConfig>,
    impl openraft::storage::RaftStateMachine<TypeConfig>,
) {
    let store = SimpleMemStore::new();
    Adaptor::new(store)
}

/// Create a new simple memory store with persistence using the Adaptor pattern
pub fn create_mem_store_with_dir(
    data_dir: PathBuf,
) -> (
    impl openraft::storage::RaftLogStorage<TypeConfig>,
    impl openraft::storage::RaftStateMachine<TypeConfig>,
) {
    let store = SimpleMemStore::with_data_dir(Some(data_dir));
    Adaptor::new(store)
}
