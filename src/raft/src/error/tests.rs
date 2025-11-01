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

//! Unit tests for error types

#[cfg(test)]
mod tests {
    use super::super::*;
    use std::io;

    #[test]
    fn test_raft_error_creation() {
        let config_err = RaftError::configuration("Invalid configuration");
        assert!(matches!(config_err, RaftError::Configuration { .. }));
        
        let consistency_err = RaftError::consistency("Consistency violation");
        assert!(matches!(consistency_err, RaftError::Consistency { .. }));
        
        let state_machine_err = RaftError::state_machine("State machine error");
        assert!(matches!(state_machine_err, RaftError::StateMachine { .. }));
        
        let timeout_err = RaftError::timeout("Operation timeout");
        assert!(matches!(timeout_err, RaftError::Timeout { .. }));
        
        let invalid_request_err = RaftError::invalid_request("Invalid request");
        assert!(matches!(invalid_request_err, RaftError::InvalidRequest { .. }));
        
        let invalid_state_err = RaftError::invalid_state("Invalid state");
        assert!(matches!(invalid_state_err, RaftError::InvalidState { .. }));
        
        let resource_exhausted_err = RaftError::resource_exhausted("Out of memory");
        assert!(matches!(resource_exhausted_err, RaftError::ResourceExhausted { .. }));
        
        let not_found_err = RaftError::not_found("Key not found");
        assert!(matches!(not_found_err, RaftError::NotFound { .. }));
    }

    #[test]
    fn test_raft_error_display() {
        let config_err = RaftError::configuration("Test config error");
        let error_string = format!("{}", config_err);
        assert!(error_string.contains("Configuration error"));
        assert!(error_string.contains("Test config error"));
        
        let timeout_err = RaftError::timeout("test_operation");
        let timeout_string = format!("{}", timeout_err);
        assert!(timeout_string.contains("Timeout error"));
        assert!(timeout_string.contains("test_operation"));
    }

    #[test]
    fn test_raft_error_classification() {
        let not_leader_err = RaftError::NotLeader { leader_id: Some(1) };
        assert!(not_leader_err.is_not_leader());
        assert!(not_leader_err.is_retryable());
        
        let timeout_err = RaftError::timeout("test");
        assert!(!timeout_err.is_not_leader());
        assert!(timeout_err.is_retryable());
        
        let config_err = RaftError::configuration("test");
        assert!(!config_err.is_not_leader());
        assert!(!config_err.is_retryable());
        
        let invalid_request_err = RaftError::invalid_request("test");
        assert!(!invalid_request_err.is_not_leader());
        assert!(!invalid_request_err.is_retryable());
    }

    #[test]
    fn test_network_error_creation() {
        let connection_err = NetworkError::ConnectionFailed {
            node_id: 1,
            source: io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused"),
        };
        assert!(matches!(connection_err, NetworkError::ConnectionFailed { .. }));
        
        let timeout_err = NetworkError::RequestTimeout { node_id: 2 };
        assert!(matches!(timeout_err, NetworkError::RequestTimeout { .. }));
        
        let invalid_response_err = NetworkError::InvalidResponse {
            node_id: 3,
            message: "Invalid format".to_string(),
        };
        assert!(matches!(invalid_response_err, NetworkError::InvalidResponse { .. }));
        
        let partition_err = NetworkError::NetworkPartition;
        assert!(matches!(partition_err, NetworkError::NetworkPartition));
    }

    #[test]
    fn test_network_error_display() {
        let connection_err = NetworkError::ConnectionFailed {
            node_id: 1,
            source: io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused"),
        };
        let error_string = format!("{}", connection_err);
        assert!(error_string.contains("Connection failed to node 1"));
        
        let timeout_err = NetworkError::RequestTimeout { node_id: 2 };
        let timeout_string = format!("{}", timeout_err);
        assert!(timeout_string.contains("Request timeout for node 2"));
        
        let partition_err = NetworkError::NetworkPartition;
        let partition_string = format!("{}", partition_err);
        assert!(partition_string.contains("Network partition detected"));
    }

    #[test]
    fn test_storage_error_creation() {
        let corruption_err = StorageError::LogCorruption { index: 100 };
        assert!(matches!(corruption_err, StorageError::LogCorruption { .. }));
        
        let snapshot_err = StorageError::SnapshotCreationFailed {
            message: "Disk full".to_string(),
        };
        assert!(matches!(snapshot_err, StorageError::SnapshotCreationFailed { .. }));
        
        let restore_err = StorageError::SnapshotRestorationFailed {
            message: "Corrupted snapshot".to_string(),
        };
        assert!(matches!(restore_err, StorageError::SnapshotRestorationFailed { .. }));
        
        let disk_err = StorageError::InsufficientDiskSpace;
        assert!(matches!(disk_err, StorageError::InsufficientDiskSpace));
        
        let consistency_err = StorageError::DataInconsistency {
            message: "Checksum mismatch".to_string(),
        };
        assert!(matches!(consistency_err, StorageError::DataInconsistency { .. }));
    }

    #[test]
    fn test_storage_error_display() {
        let corruption_err = StorageError::LogCorruption { index: 42 };
        let error_string = format!("{}", corruption_err);
        assert!(error_string.contains("Log corruption detected at index 42"));
        
        let disk_err = StorageError::InsufficientDiskSpace;
        let disk_string = format!("{}", disk_err);
        assert!(disk_string.contains("Disk space insufficient"));
    }

    #[test]
    fn test_error_conversion() {
        // Test conversion from NetworkError to RaftError
        let network_err = NetworkError::RequestTimeout { node_id: 1 };
        let raft_err: RaftError = network_err.into();
        assert!(matches!(raft_err, RaftError::Network(_)));
        
        // Test conversion from StorageError to RaftError
        let storage_err = StorageError::InsufficientDiskSpace;
        let raft_err: RaftError = storage_err.into();
        assert!(matches!(raft_err, RaftError::Storage(_)));
        
        // Test conversion from io::Error to RaftError
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "Permission denied");
        let raft_err: RaftError = io_err.into();
        assert!(matches!(raft_err, RaftError::Io(_)));
        
        // Test conversion from serde_json::Error to RaftError
        let json_err = serde_json::from_str::<i32>("invalid json").unwrap_err();
        let raft_err: RaftError = json_err.into();
        assert!(matches!(raft_err, RaftError::Serialization(_)));
    }

    #[test]
    fn test_raft_result_type() {
        // Test successful result
        let success: RaftResult<i32> = Ok(42);
        assert_eq!(success.unwrap(), 42);
        
        // Test error result
        let error: RaftResult<i32> = Err(RaftError::configuration("test error"));
        assert!(error.is_err());
        assert!(matches!(error.unwrap_err(), RaftError::Configuration { .. }));
    }

    #[test]
    fn test_error_chaining() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused");
        let network_err = NetworkError::ConnectionFailed {
            node_id: 1,
            source: io_err,
        };
        let raft_err: RaftError = network_err.into();
        
        // Test that the error chain is preserved
        let error_string = format!("{}", raft_err);
        assert!(error_string.contains("Network error"));
        assert!(error_string.contains("Connection failed to node 1"));
    }

    #[test]
    fn test_error_debug_formatting() {
        let config_err = RaftError::configuration("debug test");
        let debug_string = format!("{:?}", config_err);
        assert!(debug_string.contains("Configuration"));
        assert!(debug_string.contains("debug test"));
        
        let network_err = NetworkError::NetworkPartition;
        let debug_string = format!("{:?}", network_err);
        assert!(debug_string.contains("NetworkPartition"));
    }

    #[test]
    fn test_not_leader_error() {
        let not_leader_with_id = RaftError::NotLeader { leader_id: Some(3) };
        let error_string = format!("{}", not_leader_with_id);
        assert!(error_string.contains("Not leader"));
        assert!(error_string.contains("current leader is Some(3)"));
        
        let not_leader_without_id = RaftError::NotLeader { leader_id: None };
        let error_string = format!("{}", not_leader_without_id);
        assert!(error_string.contains("Not leader"));
        assert!(error_string.contains("current leader is None"));
    }

    #[test]
    fn test_serialization_error_helper() {
        let err = RaftError::serialization("JSON parse error");
        assert!(matches!(err, RaftError::InvalidRequest { .. }));
        
        let error_string = format!("{}", err);
        assert!(error_string.contains("Serialization error"));
        assert!(error_string.contains("JSON parse error"));
    }

    #[test]
    fn test_consensus_error_helper() {
        let err = RaftError::consensus("Leader election failed");
        assert!(matches!(err, RaftError::InvalidRequest { .. }));
        
        let error_string = format!("{}", err);
        assert!(error_string.contains("Consensus error"));
        assert!(error_string.contains("Leader election failed"));
    }
}