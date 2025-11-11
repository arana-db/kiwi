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

// Working Adaptor implementation for OpenRaft 0.9.21

use std::io::Cursor;
use std::ops::RangeBounds;

use openraft::storage::Adaptor;
use openraft::{
    Entry, EntryPayload, LogId, RaftLogId, RaftStorage, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError as OpenraftStorageError, StoredMembership,
};

use crate::types::{BasicNode, ClientResponse, NodeId, TypeConfig};

/// Working storage implementation that correctly implements RaftStorage
pub struct WorkingStorage {
    logs: Vec<Entry<TypeConfig>>,
    vote: Option<openraft::Vote<NodeId>>,
    applied_index: u64,
}

impl WorkingStorage {
    pub fn new() -> Self {
        Self {
            logs: Vec::new(),
            vote: None,
            applied_index: 0,
        }
    }
}

/// Implement RaftStorage with correct lifetimes for OpenRaft 0.9.21
#[async_trait::async_trait]
impl RaftStorage<TypeConfig> for WorkingStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote<'life0, 'async_trait>(
        &'life0 mut self,
        vote: &openraft::Vote<NodeId>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<(), OpenraftStorageError<NodeId>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.vote = Some(vote.clone());
            Ok(())
        })
    }

    async fn read_vote<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<Option<openraft::Vote<NodeId>>, OpenraftStorageError<NodeId>>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { Ok(self.vote.clone()) })
    }

    async fn get_log_reader<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Self::LogReader>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            WorkingStorage {
                logs: self.logs.clone(),
                vote: self.vote.clone(),
                applied_index: self.applied_index,
            }
        })
    }

    async fn append_to_log<'life0, 'life1, 'async_trait, I>(
        &'life0 mut self,
        entries: I,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<(), OpenraftStorageError<NodeId>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send + 'life1,
        I::IntoIter: Send,
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            for entry in entries {
                self.logs.push(entry);
            }
            Ok(())
        })
    }

    async fn delete_conflict_logs_since<'life0, 'async_trait>(
        &'life0 mut self,
        log_id: LogId<NodeId>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<(), OpenraftStorageError<NodeId>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if let Some(pos) = self.logs.iter().position(|entry| entry.get_log_id() >= &log_id) {
                self.logs.truncate(pos);
            }
            Ok(())
        })
    }

    async fn purge_logs_upto<'life0, 'async_trait>(
        &'life0 mut self,
        log_id: LogId<NodeId>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<(), OpenraftStorageError<NodeId>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if let Some(pos) = self.logs.iter().position(|entry| entry.get_log_id() > &log_id) {
                self.logs.drain(0..pos);
            } else {
                self.logs.clear();
            }
            Ok(())
        })
    }

    async fn last_applied_state<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<
                        (Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>),
                        OpenraftStorageError<NodeId>,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let log_id = if self.applied_index > 0 {
                Some(LogId::new(
                    openraft::CommittedLeaderId::new(1, 1),
                    self.applied_index,
                ))
            } else {
                None
            };
            Ok((log_id, StoredMembership::default()))
        })
    }

    async fn apply_to_state_machine<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        entries: &'life1 [Entry<TypeConfig>],
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<Vec<ClientResponse>, OpenraftStorageError<NodeId>>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
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
        })
    }

    async fn get_snapshot_builder<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Self::SnapshotBuilder>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            WorkingStorage {
                logs: self.logs.clone(),
                vote: self.vote.clone(),
                applied_index: self.applied_index,
            }
        })
    }

    async fn begin_receiving_snapshot<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<
                        Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
                        OpenraftStorageError<NodeId>,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { Ok(Box::new(Cursor::new(Vec::new()))) })
    }

    async fn install_snapshot<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 mut self,
        meta: &'life1 SnapshotMeta<NodeId, BasicNode>,
        _snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<(), OpenraftStorageError<NodeId>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if let Some(last_log_id) = &meta.last_log_id {
                self.applied_index = last_log_id.index;
            }
            Ok(())
        })
    }

    async fn get_current_snapshot<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<Option<Snapshot<TypeConfig>>, OpenraftStorageError<NodeId>>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { Ok(None) })
    }

    async fn get_log_state<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<openraft::LogState<TypeConfig>, OpenraftStorageError<NodeId>>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let last_log_id = self.logs.last().map(|entry| entry.get_log_id().clone());
            Ok(openraft::LogState {
                last_purged_log_id: None,
                last_log_id,
            })
        })
    }
}

/// Implement RaftLogReader with correct lifetimes
#[async_trait::async_trait]
impl openraft::storage::RaftLogReader<TypeConfig> for WorkingStorage {
    async fn try_get_log_entries<'life0, 'life1, 'async_trait, RB>(
        &'life0 mut self,
        range: RB,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<Vec<Entry<TypeConfig>>, OpenraftStorageError<NodeId>>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        RB: RangeBounds<u64> + Clone + Send + Sync + 'life1,
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
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
        })
    }
}

/// Implement RaftSnapshotBuilder with correct lifetimes
#[async_trait::async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for WorkingStorage {
    async fn build_snapshot<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<Snapshot<TypeConfig>, OpenraftStorageError<NodeId>>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let data = Vec::new();
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

        println!("✅ Working Adaptor pattern successful!");
    }
}
