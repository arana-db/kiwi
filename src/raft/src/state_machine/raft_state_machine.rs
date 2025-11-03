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

//! RaftStateMachine implementation for openraft integration

use std::io::Cursor;

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{EffectiveMembership, Entry, LogId, RaftTypeConfig, SnapshotMeta, StorageError};
use async_trait::async_trait;

use crate::error::RaftError;
use crate::state_machine::{KiwiStateMachine, StateMachineSnapshot};
use crate::types::{ClientRequest, ClientResponse, NodeId, TypeConfig};

/// Implementation of RaftStateMachine trait for KiwiStateMachine
#[async_trait::async_trait]
impl RaftStateMachine<TypeConfig> for KiwiStateMachine {
    type SnapshotBuilder = KiwiSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            EffectiveMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        let (log_id, membership) = self.get_applied_state().await;
        Ok((log_id, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClientResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        self.apply_entries(entries)
            .await
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotBuilder>, StorageError<NodeId>> {
        let snapshot_builder = self
            .begin_receiving_snapshot()
            .await
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })?;

        Ok(Box::new(KiwiSnapshotBuilder::new(snapshot_builder)))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Self::SnapshotBuilder>,
    ) -> Result<(), StorageError<NodeId>> {
        let snapshot_data = snapshot.into_state_machine();
        self.install_snapshot_data(meta, &snapshot_data)
            .await
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // Get the current snapshot data
        let snapshot_data = self
            .get_current_snapshot_data()
            .await
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })?;

        match snapshot_data {
            Some(snap) => {
                // Create snapshot metadata
                let last_log_id = if snap.applied_index > 0 {
                    Some(LogId::new(
                        openraft::CommittedLeaderId::new(1, self.node_id), // TODO: Get actual term
                        snap.applied_index,
                    ))
                } else {
                    None
                };

                let membership = EffectiveMembership::new(None, openraft::Membership::new(vec![], None));

                let meta = SnapshotMeta {
                    last_log_id,
                    last_membership: membership,
                    snapshot_id: format!("snapshot_{}", snap.applied_index),
                };

                // Serialize the snapshot data
                let serialized_data = bincode::serialize(&snap).map_err(|e| StorageError::IO {
                    source: Box::new(RaftError::state_machine(format!(
                        "Failed to serialize snapshot: {}",
                        e
                    ))),
                })?;

                let snapshot = Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(serialized_data)),
                };

                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }
}

/// Snapshot builder for KiwiStateMachine
pub struct KiwiSnapshotBuilder {
    state_machine: KiwiStateMachine,
}

impl KiwiSnapshotBuilder {
    pub fn new(state_machine: KiwiStateMachine) -> Self {
        Self { state_machine }
    }

    pub fn into_state_machine(self) -> KiwiStateMachine {
        self.state_machine
    }
}

#[async_trait::async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for KiwiSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Create a snapshot of the current state
        let snapshot_data = self
            .state_machine
            .create_snapshot()
            .await
            .map_err(|e| StorageError::IO {
                source: Box::new(e),
            })?;

        // Create snapshot metadata
        let last_log_id = if snapshot_data.applied_index > 0 {
            Some(LogId::new(
                openraft::CommittedLeaderId::new(1, self.state_machine.node_id), // TODO: Get actual term
                snapshot_data.applied_index,
            ))
        } else {
            None
        };

        let membership = EffectiveMembership::new(None, openraft::Membership::new(vec![], None));

        let meta = SnapshotMeta {
            last_log_id,
            last_membership: membership,
            snapshot_id: format!("snapshot_{}", snapshot_data.applied_index),
        };

        // Serialize the snapshot data
        let serialized_data = bincode::serialize(&snapshot_data).map_err(|e| StorageError::IO {
            source: Box::new(RaftError::state_machine(format!(
                "Failed to serialize snapshot: {}",
                e
            ))),
        })?;

        let snapshot = Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(serialized_data)),
        };

        Ok(snapshot)
    }
}