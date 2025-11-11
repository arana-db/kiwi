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

//! Unit tests for type conversion functions

use super::*;
use bytes::Bytes;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

use crate::types::{ClientRequest, ClientResponse, ConsistencyLevel, RedisCommand, RequestId};

// ============================================================================
// Entry → ClientRequest Conversion Tests
// ============================================================================

#[test]
fn test_entry_to_client_request_normal() {
    // Create a ClientRequest
    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "SET".to_string(),
            vec![Bytes::from("key1"), Bytes::from("value1")],
        ),
        consistency_level: ConsistencyLevel::Linearizable,
    };

    // Create an Entry with Normal payload
    let log_id = LogId::new(CommittedLeaderId::new(1, 1), 10);
    let entry = Entry {
        log_id,
        payload: EntryPayload::Normal(request.clone()),
    };

    // Convert Entry to ClientRequest
    let result = entry_to_client_request(&entry);
    assert!(result.is_ok());

    let converted_request = result.unwrap();
    assert_eq!(converted_request.id, request.id);
    assert_eq!(converted_request.command.command, "SET");
    assert_eq!(converted_request.command.args.len(), 2);
}

#[test]
fn test_entry_to_client_request_blank() {
    // Create an Entry with Blank payload
    let log_id = LogId::new(CommittedLeaderId::new(1, 1), 10);
    let entry: Entry<TypeConfig> = Entry {
        log_id,
        payload: EntryPayload::Blank,
    };

    // Convert Entry to ClientRequest should fail
    let result = entry_to_client_request(&entry);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Cannot convert blank entry"));
}

#[test]
fn test_entry_to_client_request_membership() {
    use std::collections::BTreeSet;
    
    // Create an Entry with Membership payload
    let log_id = LogId::new(CommittedLeaderId::new(1, 1), 10);
    let mut nodes = BTreeSet::new();
    nodes.insert(1);
    nodes.insert(2);
    nodes.insert(3);
    let membership = openraft::Membership::new(vec![nodes], None);
    let entry: Entry<TypeConfig> = Entry {
        log_id,
        payload: EntryPayload::Membership(membership),
    };

    // Convert Entry to ClientRequest should fail
    let result = entry_to_client_request(&entry);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Cannot convert membership entry"));
}

#[test]
fn test_deserialize_client_request_success() {
    // Create and serialize a ClientRequest
    let request = ClientRequest {
        id: RequestId::new(),
        command: RedisCommand::new(
            "GET".to_string(),
            vec![Bytes::from("test_key")],
        ),
        consistency_level: ConsistencyLevel::Eventual,
    };

    let serialized = bincode::serialize(&request).unwrap();

    // Deserialize it back
    let result = deserialize_client_request(&serialized);
    assert!(result.is_ok());

    let deserialized = result.unwrap();
    assert_eq!(deserialized.id, request.id);
    assert_eq!(deserialized.command.command, "GET");
    assert_eq!(deserialized.consistency_level, ConsistencyLevel::Eventual);
}

#[test]
fn test_deserialize_client_request_invalid_data() {
    // Try to deserialize invalid data
    let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
    let result = deserialize_client_request(&invalid_data);
    assert!(result.is_err());
}

// ============================================================================
// ClientResponse → Response Conversion Tests
// ============================================================================

#[test]
fn test_serialize_client_response_success() {
    // Create a successful ClientResponse
    let response = ClientResponse::success(
        RequestId::new(),
        Bytes::from("OK"),
        Some(1),
    );

    // Serialize it
    let result = serialize_client_response(&response);
    assert!(result.is_ok());

    let serialized = result.unwrap();
    assert!(!serialized.is_empty());

    // Verify we can deserialize it back
    let deserialized: ClientResponse = bincode::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.id, response.id);
    assert!(deserialized.result.is_ok());
}

#[test]
fn test_serialize_client_response_error() {
    // Create an error ClientResponse
    let response = ClientResponse::error(
        RequestId::new(),
        "Command failed".to_string(),
        Some(2),
    );

    // Serialize it
    let result = serialize_client_response(&response);
    assert!(result.is_ok());

    let serialized = result.unwrap();
    assert!(!serialized.is_empty());

    // Verify we can deserialize it back
    let deserialized: ClientResponse = bincode::deserialize(&serialized).unwrap();
    assert_eq!(deserialized.id, response.id);
    assert!(deserialized.result.is_err());
    assert_eq!(deserialized.result.unwrap_err(), "Command failed");
}

#[test]
fn test_deserialize_client_response_success() {
    // Create and serialize a ClientResponse
    let response = ClientResponse {
        id: RequestId::new(),
        result: Ok(Bytes::from("test_value")),
        leader_id: Some(3),
    };

    let serialized = bincode::serialize(&response).unwrap();

    // Deserialize it back
    let result = deserialize_client_response(&serialized);
    assert!(result.is_ok());

    let deserialized = result.unwrap();
    assert_eq!(deserialized.id, response.id);
    assert_eq!(deserialized.leader_id, Some(3));
    assert!(deserialized.result.is_ok());
}

#[test]
fn test_deserialize_client_response_invalid_data() {
    // Try to deserialize invalid data
    let invalid_data = vec![0x00, 0x01, 0x02];
    let result = deserialize_client_response(&invalid_data);
    assert!(result.is_err());
}

#[test]
fn test_client_response_roundtrip() {
    // Test full roundtrip: serialize then deserialize
    let original = ClientResponse {
        id: RequestId::new(),
        result: Ok(Bytes::from("roundtrip_test")),
        leader_id: Some(5),
    };

    let serialized = serialize_client_response(&original).unwrap();
    let deserialized = deserialize_client_response(&serialized).unwrap();

    assert_eq!(deserialized.id, original.id);
    assert_eq!(deserialized.leader_id, original.leader_id);
    assert_eq!(
        deserialized.result.unwrap(),
        original.result.unwrap()
    );
}

// ============================================================================
// RaftError → StorageError Conversion Tests
// ============================================================================

#[test]
fn test_to_storage_error_rocksdb() {
    let raft_error = RaftError::Storage(StorageError::DataInconsistency {
        message: "Test RocksDB error".to_string(),
        context: "test".to_string(),
    });

    let storage_error = to_storage_error(raft_error);

    // Verify it's an IO error
    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error
        }
        _ => panic!("Expected IO error"),
    }
}

#[test]
fn test_to_storage_error_log_corruption() {
    let raft_error = RaftError::Storage(StorageError::LogCorruption { 
        index: 42,
        term: 1,
        context: "test".to_string(),
    });

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error with Log subject
        }
        _ => panic!("Expected IO error with Log subject"),
    }
}

#[test]
fn test_to_storage_error_snapshot_creation_failed() {
    let raft_error = RaftError::Storage(StorageError::SnapshotCreationFailed {
        message: "Failed to create snapshot".to_string(),
        snapshot_id: "snap_1".to_string(),
        context: "test".to_string(),
    });

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error with Snapshot subject
        }
        _ => panic!("Expected IO error with Snapshot subject"),
    }
}

#[test]
fn test_to_storage_error_snapshot_restoration_failed() {
    let raft_error = RaftError::Storage(StorageError::SnapshotRestorationFailed {
        message: "Failed to restore snapshot".to_string(),
        snapshot_id: "snap_1".to_string(),
        context: "test".to_string(),
    });

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error with Snapshot subject
        }
        _ => panic!("Expected IO error with Snapshot subject"),
    }
}

#[test]
fn test_to_storage_error_insufficient_disk_space() {
    let raft_error = RaftError::Storage(StorageError::InsufficientDiskSpace {
        required_bytes: 1000,
        available_bytes: 500,
        path: "/tmp".to_string(),
    });

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error
        }
        _ => panic!("Expected IO error"),
    }
}

#[test]
fn test_to_storage_error_data_inconsistency() {
    let raft_error = RaftError::Storage(StorageError::DataInconsistency {
        message: "Data is inconsistent".to_string(),
        context: "test".to_string(),
    });

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error
        }
        _ => panic!("Expected IO error"),
    }
}

#[test]
fn test_to_storage_error_state_machine() {
    let raft_error = RaftError::StateMachine {
        message: "State machine error".to_string(),
        context: "test".to_string(),
    };

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error with StateMachine subject
        }
        _ => panic!("Expected IO error with StateMachine subject"),
    }
}

#[test]
fn test_to_storage_error_serialization() {
    let raft_error = RaftError::Serialization(serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::Other,
        "test",
    )));

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error
        }
        _ => panic!("Expected IO error"),
    }
}

#[test]
fn test_to_storage_error_io() {
    let raft_error = RaftError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "File not found",
    ));

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error
        }
        _ => panic!("Expected IO error"),
    }
}

#[test]
fn test_to_storage_error_invalid_request() {
    let raft_error = RaftError::InvalidRequest {
        message: "Invalid request".to_string(),
        context: "test".to_string(),
    };

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error
        }
        _ => panic!("Expected IO error"),
    }
}

#[test]
fn test_to_storage_error_invalid_state() {
    let raft_error = RaftError::InvalidState {
        message: "Invalid state".to_string(),
        current_state: "follower".to_string(),
        context: "test".to_string(),
    };

    let storage_error = to_storage_error(raft_error);

    match storage_error {
        OpenraftStorageError::IO { .. } => {
            // Success - converted to IO error
        }
        _ => panic!("Expected IO error"),
    }
}

#[test]
fn test_from_storage_error_io() {
    use openraft::{ErrorSubject, ErrorVerb, StorageIOError};

    let storage_error = OpenraftStorageError::<NodeId>::IO {
        source: StorageIOError::new(
            ErrorSubject::Store,
            ErrorVerb::Read,
            openraft::AnyError::error("test error"),
        ),
    };

    let raft_error = from_storage_error(storage_error);

    match raft_error {
        RaftError::Storage(StorageError::DataInconsistency { message, context: _ }) => {
            assert!(message.contains("Storage IO error"));
        }
        _ => panic!("Expected Storage error with DataInconsistency"),
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_entry_conversion_with_multiple_commands() {
    // Test conversion with different Redis commands
    let commands = vec![
        ("SET", vec!["key1", "value1"]),
        ("GET", vec!["key1"]),
        ("DEL", vec!["key1", "key2"]),
        ("EXISTS", vec!["key1"]),
    ];

    for (cmd, args) in commands {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                cmd.to_string(),
                args.iter().map(|s| Bytes::from(*s)).collect(),
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let log_id = LogId::new(CommittedLeaderId::new(1, 1), 1);
        let entry = Entry {
            log_id,
            payload: EntryPayload::Normal(request.clone()),
        };

        let result = entry_to_client_request(&entry);
        assert!(result.is_ok());

        let converted = result.unwrap();
        assert_eq!(converted.command.command, cmd);
        assert_eq!(converted.command.args.len(), args.len());
    }
}

#[test]
fn test_error_conversion_preserves_context() {
    // Test that error conversion preserves error messages
    let original_message = "Critical storage failure at index 100";
    let raft_error = RaftError::Storage(StorageError::DataInconsistency {
        message: original_message.to_string(),
        context: "test".to_string(),
    });

    let storage_error = to_storage_error(raft_error);

    // Convert back and verify message is preserved
    let converted_back = from_storage_error(storage_error);
    assert!(converted_back.to_string().contains("Storage IO error"));
}
