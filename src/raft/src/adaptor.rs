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

//! Combined RaftStorage implementation

use std::path::Path;

use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::StorageError;
use async_trait::async_trait;

use crate::error::RaftError;
use crate::state_machine::KiwiStateMachine;
use crate::storage::RaftStorage;
use crate::types::{NodeId, TypeConfig};

/// Combined Raft storage that implements both RaftLogStorage and RaftStateMachine
pub struct KiwiRaftStorage {
    log_storage: RaftStorage,
    state_machine: KiwiStateMachine,
}

impl KiwiRaftStorage {
    /// Create a new Kiwi Raft storage instance
    pub fn new<P: AsRef<Path>>(node_id: NodeId, db_path: P) -> Result<Self, RaftError> {
        let log_storage = RaftStorage::new(db_path)?;
        let state_machine = KiwiStateMachine::new(node_id);
        
        Ok(Self {
            log_storage,
            state_machine,
        })
    }
    
    /// Create a new Kiwi Raft storage instance with storage engine
    pub fn with_storage_engine<P: AsRef<Path>>(
        node_id: NodeId,
        db_path: P,
        storage_engine: std::sync::Arc<dyn crate::state_machine::StorageEngine>,
    ) -> Result<Self, RaftError> {
        let log_storage = RaftStorage::new(db_path)?;
        let state_machine = KiwiStateMachine::with_storage_engine(node_id, storage_engine);
        
        Ok(Self {
            log_storage,
            state_machine,
        })
    }
}

// Forward RaftLogStorage implementation to the inner log_storage
#[async_trait]
impl RaftLogStorage<TypeConfig> for KiwiRaftStorage {
    type LogReader = RaftStorage;

    async fn get_log_state(&mut self) -> Result<openraft::storage::LogState<TypeConfig>, StorageError<NodeId>> {
        self.log_storage.get_log_state().await
    }

    async fn save_committed(&mut self, committed: Option<openraft::LogId<NodeId>>) -> Result<(), StorageError<NodeId>> {
        self.log_storage.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<openraft::LogId<NodeId>>, StorageError<NodeId>> {
        self.log_storage.read_committed().await
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log_storage.save_vote(vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<NodeId>>, StorageError<NodeId>> {
        self.log_storage.read_vote().await
    }

    async fn append<I>(&mut self, entries: I, callback: openraft::storage::LogFlushed<TypeConfig>) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        self.log_storage.append(entries, callback).await
    }

    async fn truncate(&mut self, log_id: openraft::LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log_storage.truncate(log_id).await
    }

    async fn purge(&mut self, log_id: openraft::LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log_storage.purge(log_id).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.log_storage.get_log_reader().await
    }
}

// Forward RaftStateMachine implementation to the inner state_machine
#[async_trait]
impl RaftStateMachine<TypeConfig> for KiwiRaftStorage {
    type SnapshotBuilder = <KiwiStateMachine as RaftStateMachine<TypeConfig>>::SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<openraft::LogId<NodeId>>, openraft::EffectiveMembership<NodeId, openraft::BasicNode>), StorageError<NodeId>> {
        self.state_machine.applied_state().await
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<crate::types::ClientResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        self.state_machine.apply(entries).await
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Self::SnapshotBuilder>, StorageError<NodeId>> {
        self.state_machine.begin_receiving_snapshot().await
    }

    async fn install_snapshot(&mut self, meta: &openraft::SnapshotMeta<NodeId, openraft::BasicNode>, snapshot: Box<Self::SnapshotBuilder>) -> Result<(), StorageError<NodeId>> {
        self.state_machine.install_snapshot(meta, snapshot).await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<openraft::storage::Snapshot<TypeConfig>>, StorageError<NodeId>> {
        self.state_machine.get_current_snapshot().await
    }
}

/// Create a new Kiwi Raft storage instance
pub fn create_raft_storage<P: AsRef<Path>>(
    node_id: NodeId,
    db_path: P,
) -> Result<KiwiRaftStorage, RaftError> {
    KiwiRaftStorage::new(node_id, db_path)
}

/// Create a new Kiwi Raft storage instance with storage engine
pub fn create_raft_storage_with_engine<P: AsRef<Path>>(
    node_id: NodeId,
    db_path: P,
    storage_engine: std::sync::Arc<dyn crate::state_machine::StorageEngine>,
) -> Result<KiwiRaftStorage, RaftError> {
    KiwiRaftStorage::with_storage_engine(node_id, db_path, storage_engine)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_raft_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = create_raft_storage(1, temp_dir.path()).unwrap();
        
        // Basic smoke test - just ensure we can create the storage
        drop(storage);
    }

    #[tokio::test]
    async fn test_raft_storage_basic_operations() {
        use openraft::storage::{RaftLogStorage, RaftStateMachine};
        
        let temp_dir = TempDir::new().unwrap();
        let mut storage = create_raft_storage(1, temp_dir.path()).unwrap();
        
        // Test getting initial log state
        let log_state = storage.get_log_state().await.unwrap();
        assert!(log_state.last_log_id.is_none());
        assert!(log_state.last_purged_log_id.is_none());
        
        // Test getting applied state
        let (applied_log_id, _membership) = storage.applied_state().await.unwrap();
        assert!(applied_log_id.is_none());
    }
}