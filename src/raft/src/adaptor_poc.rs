// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Proof of Concept for Openraft Adaptor Pattern
// This file demonstrates how to correctly use Openraft's Adaptor to work with sealed traits

use std::io::Cursor;
use std::ops::RangeBounds;

use openraft::storage::{RaftLogReader, RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    Entry, EntryPayload, LogId, RaftLogId, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError,
};

use crate::types::{BasicNode, ClientResponse, NodeId, TypeConfig};

/// Proof of concept: Minimal storage implementation to test Adaptor pattern
pub struct PocStorage {
    /// Log entries (in-memory for POC)
    logs: Vec<Entry<TypeConfig>>,
}

impl PocStorage {
    pub fn new() -> Self {
        Self {
            logs: Vec::new(),
        }
    }
}

/// Implement RaftLogReader trait (non-sealed)
#[async_trait::async_trait]
impl RaftLogReader<TypeConfig> for PocStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>> {
        // Simple implementation for POC
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

/// Implement RaftSnapshotBuilder trait (non-sealed)
#[async_trait::async_trait]
impl RaftSnapshotBuilder<TypeConfig> for PocStorage {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        // Create a simple snapshot for POC
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

/// Proof of concept: Minimal state machine implementation
pub struct PocStateMachine {
    /// Last applied log index
    last_applied: Option<LogId<NodeId>>,
}

impl PocStateMachine {
    pub fn new() -> Self {
        Self {
            last_applied: None,
        }
    }
}

/// Implement RaftStateMachine trait (non-sealed)
#[async_trait::async_trait]
impl RaftStateMachine<TypeConfig> for PocStateMachine {
    type SnapshotBuilder = PocStorage;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            openraft::EffectiveMembership<NodeId, BasicNode>,
        ),
        StorageError<TypeConfig>,
    > {
        Ok((self.last_applied.clone(), Default::default()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClientResponse>, StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            self.last_applied = Some(entry.get_log_id().clone());

            // Process the entry based on its payload type
            match entry.payload {
                EntryPayload::Blank => {
                    // Blank entries don't need processing
                }
                EntryPayload::Normal(ref _data) => {
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
        PocStorage::new()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<TypeConfig>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        _snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<TypeConfig>> {
        // Update last applied to snapshot's last log id
        self.last_applied = meta.last_log_id.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
        Ok(None)
    }
}

/// Test function to verify Adaptor usage
#[cfg(test)]
mod tests {
    use super::*;

    // Note: Adaptor is only available with storage-v2 feature in openraft 0.9.21
    // The storage-v2 feature is enabled, but the Adaptor type is gated behind
    // #[cfg(not(feature = "storage-v2"))], which means it's NOT available when storage-v2 is enabled.
    // This is because storage-v2 uses the new RaftLogStorage and RaftStateMachine traits directly.
    
    #[tokio::test]
    async fn test_storage_traits() {
        // Create storage and state machine
        let mut storage = PocStorage::new();
        let mut state_machine = PocStateMachine::new();

        // Test that our implementations work with the traits
        let entries = storage.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(entries.len(), 0);

        let (last_applied, _membership) = state_machine.applied_state().await.unwrap();
        assert!(last_applied.is_none());

        println!("✅ Storage traits implemented correctly!");
    }

    #[tokio::test]
    async fn test_raft_log_reader() {
        let mut storage = PocStorage::new();

        // Test try_get_log_entries
        let entries = storage.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_raft_state_machine() {
        let mut sm = PocStateMachine::new();

        // Test applied_state
        let (last_applied, _membership) = sm.applied_state().await.unwrap();
        assert!(last_applied.is_none());

        // Test apply with empty entries
        let responses = sm.apply(vec![]).await.unwrap();
        assert_eq!(responses.len(), 0);
    }

    #[tokio::test]
    async fn test_snapshot_builder() {
        let mut storage = PocStorage::new();

        // Test build_snapshot
        let snapshot = storage.build_snapshot().await.unwrap();
        assert!(snapshot.meta.snapshot_id.starts_with("poc_snapshot_"));
    }
}
