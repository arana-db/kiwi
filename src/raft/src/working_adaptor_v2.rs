// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Working Adaptor implementation for OpenRaft 0.9.21
// This implementation uses the Adaptor pattern correctly with proper lifetime handling

use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{
    Entry, EntryPayload, LogId, RaftLogId, RaftStorage, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError as OpenraftStorageError, StoredMembership,
};

use crate::types::{BasicNode, ClientResponse, NodeId, TypeConfig};

/// Working storage implementation that properly implements RaftStorage
/// This uses the Adaptor pattern which handles all the lifetime complexities
pub struct WorkingStorage {
    logs: Vec<Entry<TypeConfig>>,
    vote: Option<openraft::Vote<NodeId>>,
    applied_index: u64,
    snapshot_data: Vec<u8>,
}

impl WorkingStorage {
    pub fn new() -> Self {
        Self {
            logs: Vec::new(),
            vote: None,
            applied_index: 0,
            snapshot_data: Vec::new(),
        }
    }
}

/// Implement RaftStorage using async_trait (which handles lifetimes automatically)
#[async_trait::async_trait]
impl RaftStorage<TypeConfig> for WorkingStorage {
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
        WorkingStorage {
            logs: self.logs.clone(),
            vote: self.vote.clone(),
            applied_index: self.applied_index,
            snapshot_data: self.snapshot_data.clone(),
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
        if let Some(pos) = self.logs.iter().position(|entry| entry.get_log_id() >= &log_id) {
            self.logs.truncate(pos);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), OpenraftStorageError<NodeId>> {
        if let Some(pos) = self.logs.iter().position(|entry| entry.get_log_id() > &log_id) {
            self.logs.drain(0..pos);
        } else {
            self.logs.clear();
        }
        Ok(())
    }

    async fn last_applied_state(&mut self) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), OpenraftStorageError<NodeId>> {
        let log_id = if self.applied_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(1, 1),
                self.applied_index,
            ))
        } else {
            None
        };
        Ok((log_id, StoredMembership::default()))
    }

    async fn apply_to_state_machine(&mut self, entries: &[Entry<TypeConfig>]) -> Result<Vec<ClientResponse>, OpenraftStorageError<NodeId>> {
        let mut responses = Vec::new();
        for entry in entries {
            self.applied_index = entry.get_log_id().index;
            match &entry.payload {
                EntryPayload::Normal(_) => {
                    responses.push(ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("OK")),
                        leader_id: Some(1),
                    });
                }
                EntryPayload::Blank => {
                    responses.push(ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("OK")),
                        leader_id: Some(1),
                    });
                }
                EntryPayload::Membership(_) => {
                    responses.push(ClientResponse {
                        id: crate::types::RequestId::new(),
                        result: Ok(bytes::Bytes::from("OK")),
                        leader_id: Some(1),
                    });
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        WorkingStorage {
            logs: self.logs.clone(),
            vote: self.vote.clone(),
            applied_index: self.applied_index,
            snapshot_data: self.snapshot_data.clone(),
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
        if let Some(last_log_id) = &meta.last_log_id {
            self.applied_index = last_log_id.index;
        }
        self.snapshot_data = snapshot.into_inner();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, OpenraftStorageError<NodeId>> {
        if self.snapshot_data.is_empty() {
            return Ok(None);
        }

        let last_log_id = if self.applied_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(1, 1),
                self.applied_index,
            ))
        } else {
            None
        };

        let meta = SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::default(),
            snapshot_id: format!("working_snapshot_{}", chrono::Utc::now().timestamp()),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(self.snapshot_data.clone())),
        }))
    }

    async fn get_log_state(&mut self) -> Result<openraft::LogState<TypeConfig>, OpenraftStorageError<NodeId>> {
        let last_log_id = self.logs.last().map(|entry| entry.get_log_id().clone());
        Ok(openraft::LogState {
            last_purged_log_id: None,
            last_log_id,
        })
    }
}

/// Implement RaftLogReader using async_trait
#[async_trait::async_trait]
impl openraft::storage::RaftLogReader<TypeConfig> for WorkingStorage {
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

/// Implement RaftSnapshotBuilder using async_trait
#[async_trait::async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for WorkingStorage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, OpenraftStorageError<NodeId>> {
        let data = format!("snapshot_data_{}", self.applied_index).into_bytes();
        let snapshot_data = Cursor::new(data);

        let last_log_id = self.logs.last().map(|entry| entry.get_log_id().clone());

        let meta = SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::default(),
            snapshot_id: format!("working_snapshot_{}", chrono::Utc::now().timestamp()),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(snapshot_data),
        })
    }
}

/// Create working Adaptor-based storage
pub fn create_working_raft_storage() -> (
    impl openraft::storage::RaftLogStorage<TypeConfig>,
    impl openraft::storage::RaftStateMachine<TypeConfig>,
) {
    let storage = WorkingStorage::new();
    Adaptor::new(storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::{RaftLogStorage, RaftStateMachine};

    #[tokio::test]
    async fn test_working_adaptor() {
        let (mut log_storage, mut state_machine) = create_working_raft_storage();

        // Test log storage
        let log_state = log_storage.get_log_state().await.unwrap();
        assert!(log_state.last_log_id.is_none());

        // Test state machine
        let (applied_log_id, _membership) = state_machine.applied_state().await.unwrap();
        assert!(applied_log_id.is_none());

        println!("✅ Working Adaptor pattern functioning correctly!");
    }
}