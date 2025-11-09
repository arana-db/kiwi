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

//! State machine tests

use super::*;
use bytes::Bytes;
use std::collections::{HashMap, BTreeSet};
use openraft::storage::{RaftStateMachine as OpenraftStateMachine, RaftSnapshotBuilder};
use openraft::{Entry, EntryPayload, LogId, CommittedLeaderId};
use crate::types::{ClientRequest, ConsistencyLevel, RedisCommand, RequestId};

#[tokio::test]
async fn test_applied_state_initial() {
    let mut state_machine = KiwiStateMachine::new(1);

    let (log_id, membership) = state_machine.applied_state().await.unwrap();

    // Initially, no entries have been applied
    assert!(log_id.is_none());
    assert_eq!(state_machine.applied_index(), 0);
}

#[tokio::test]
async fn test_applied_state_after_apply() {
    let mut state_machine = KiwiStateMachine::new(1);

    // Create and apply a log entry
    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("key1"), Bytes::from("value1")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };

    let entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 5),
        payload: EntryPayload::Normal(request),
    };

    let _responses = state_machine.apply(vec![entry]).await.unwrap();

    // Check applied state
    let (log_id, _membership) = state_machine.applied_state().await.unwrap();
    assert!(log_id.is_some());
    let log_id = log_id.unwrap();
    assert_eq!(log_id.index, 5);
    assert_eq!(state_machine.applied_index(), 5);
}

#[tokio::test]
async fn test_apply_single_entry() {
    let mut state_machine = KiwiStateMachine::new(1);

    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("test_key"), Bytes::from("test_value")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };

    let entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Normal(request.clone()),
    };

    let responses = state_machine.apply(vec![entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].id, request.id);
    assert!(responses[0].result.is_ok());
    assert_eq!(state_machine.applied_index(), 1);
}

#[tokio::test]
async fn test_apply_multiple_entries() {
    let mut state_machine = KiwiStateMachine::new(1);

    let mut entries = Vec::new();
    for i in 0..5 {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![
                    Bytes::from(format!("key{}", i)),
                    Bytes::from(format!("value{}", i)),
                ],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), i),
            payload: EntryPayload::Normal(request),
        };

        entries.push(entry);
    }

    let responses = state_machine.apply(entries).await.unwrap();

    assert_eq!(responses.len(), 5);
    assert_eq!(state_machine.applied_index(), 4); // Last index is 4 (0-4)
}

#[tokio::test]
async fn test_apply_blank_entry() {
    let mut state_machine = KiwiStateMachine::new(1);

    let entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Blank,
    };

    let responses = state_machine.apply(vec![entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(responses[0].result.is_ok());
    assert_eq!(state_machine.applied_index(), 1);
}

#[tokio::test]
async fn test_apply_membership_entry() {
    let mut state_machine = KiwiStateMachine::new(1);

    let mut nodes = BTreeSet::new();
    nodes.insert(1);
    nodes.insert(2);
    nodes.insert(3);
    let membership = openraft::Membership::new(vec![nodes], None);
    let entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Membership(membership),
    };

    let responses = state_machine.apply(vec![entry]).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert!(responses[0].result.is_ok());
    assert_eq!(state_machine.applied_index(), 1);
}

#[tokio::test]
async fn test_get_snapshot_builder() {
    let mut state_machine = KiwiStateMachine::new(1);

    // Apply some entries first
    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("key1"), Bytes::from("value1")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };

    let entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Normal(request),
    };

    let _responses = state_machine.apply(vec![entry]).await.unwrap();

    // Get snapshot builder
    let mut builder = state_machine.get_snapshot_builder().await;

    // Verify the builder has the same applied index
    assert_eq!(builder.applied_index(), 1);
}

#[tokio::test]
async fn test_build_snapshot() {
    let mut state_machine = KiwiStateMachine::new(1);

    // Apply some entries
    for i in 0..3 {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![
                    Bytes::from(format!("key{}", i)),
                    Bytes::from(format!("value{}", i)),
                ],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), i),
            payload: EntryPayload::Normal(request),
        };

        let _responses = state_machine.apply(vec![entry]).await.unwrap();
    }

    // Build snapshot
    let mut builder = state_machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();

    // Verify snapshot metadata
    assert!(snapshot.meta.last_log_id.is_some());
    let last_log_id = snapshot.meta.last_log_id.unwrap();
    assert_eq!(last_log_id.index, 2); // Last index is 2 (0-2)
}

#[tokio::test]
async fn test_begin_receiving_snapshot() {
    let mut state_machine = KiwiStateMachine::new(1);

    let cursor = state_machine.begin_receiving_snapshot().await.unwrap();

    // Verify we got an empty cursor
    assert_eq!(cursor.get_ref().len(), 0);
}

#[tokio::test]
async fn test_install_snapshot() {
    let mut state_machine = KiwiStateMachine::new(1);

    // Create a snapshot to install
    let snapshot_data = StateMachineSnapshot {
        data: HashMap::new(),
        applied_index: 10,
    };

    let serialized = bincode::serialize(&snapshot_data).unwrap();
    let cursor = Box::new(std::io::Cursor::new(serialized));

    let meta = openraft::SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 10)),
        last_membership: openraft::StoredMembership::default(),
        snapshot_id: "test-snapshot".to_string(),
    };

    // Install the snapshot
    state_machine.install_snapshot(&meta, cursor).await.unwrap();

    // Verify applied index was updated
    assert_eq!(state_machine.applied_index(), 10);
}

#[tokio::test]
async fn test_get_current_snapshot_none() {
    let mut state_machine = KiwiStateMachine::new(1);

    let snapshot = state_machine.get_current_snapshot().await.unwrap();

    // Initially, no snapshot exists
    assert!(snapshot.is_none());
}

#[tokio::test]
async fn test_get_current_snapshot_after_build() {
    let mut state_machine = KiwiStateMachine::new(1);

    // Apply some entries
    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("key1"), Bytes::from("value1")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };

    let entry = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 5),
        payload: EntryPayload::Normal(request),
    };

    let _responses = state_machine.apply(vec![entry]).await.unwrap();

    // Build a snapshot
    let mut builder = state_machine.get_snapshot_builder().await;
    let _snapshot = builder.build_snapshot().await.unwrap();

    // Get current snapshot
    let snapshot = state_machine.get_current_snapshot().await.unwrap();

    assert!(snapshot.is_some());
    let snapshot = snapshot.unwrap();
    assert!(snapshot.meta.last_log_id.is_some());
    let last_log_id = snapshot.meta.last_log_id.unwrap();
    assert_eq!(last_log_id.index, 5);
}

#[tokio::test]
async fn test_snapshot_restore_updates_applied_index() {
    let mut state_machine = KiwiStateMachine::new(1);

    // Create a snapshot with applied_index = 20
    let snapshot_data = StateMachineSnapshot {
        data: HashMap::new(),
        applied_index: 20,
    };

    let serialized = bincode::serialize(&snapshot_data).unwrap();
    let cursor = Box::new(std::io::Cursor::new(serialized));

    let meta = openraft::SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(2, 1), 20)),
        last_membership: openraft::StoredMembership::default(),
        snapshot_id: "test-snapshot-20".to_string(),
    };

    // Install the snapshot
    state_machine.install_snapshot(&meta, cursor).await.unwrap();

    // Verify applied index
    assert_eq!(state_machine.applied_index(), 20);

    // Verify applied_state returns correct log_id
    let (log_id, _) = state_machine.applied_state().await.unwrap();
    assert!(log_id.is_some());
    assert_eq!(log_id.unwrap().index, 20);
}

#[tokio::test]
async fn test_apply_sequential_ordering() {
    let mut state_machine = KiwiStateMachine::new(1);

    // Apply entries in order
    for i in 1..=5 {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![Bytes::from("counter"), Bytes::from(i.to_string())],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), i),
            payload: EntryPayload::Normal(request),
        };

        let _responses = state_machine.apply(vec![entry]).await.unwrap();

        // Verify applied index increases sequentially
        assert_eq!(state_machine.applied_index(), i);
    }
}

#[tokio::test]
async fn test_snapshot_serialization_roundtrip() {
    let mut state_machine = KiwiStateMachine::new(1);

    // Apply some entries
    for i in 0..3 {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![
                    Bytes::from(format!("key{}", i)),
                    Bytes::from(format!("value{}", i)),
                ],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), i),
            payload: EntryPayload::Normal(request),
        };

        let _responses = state_machine.apply(vec![entry]).await.unwrap();
    }

    // Build snapshot
    let mut builder = state_machine.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();

    // Extract snapshot data
    let snapshot_data = snapshot.snapshot.into_inner();

    // Create a new state machine and install the snapshot
    let mut new_state_machine = KiwiStateMachine::new(2);
    let cursor = Box::new(std::io::Cursor::new(snapshot_data));

    let meta = openraft::SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 2)),
        last_membership: openraft::StoredMembership::default(),
        snapshot_id: "test-snapshot".to_string(),
    };

    new_state_machine.install_snapshot(&meta, cursor).await.unwrap();

    // Verify the new state machine has the same applied index
    assert_eq!(new_state_machine.applied_index(), 2);
}
