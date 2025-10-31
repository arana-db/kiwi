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

//! Raft consensus implementation for Kiwi database
//!
//! This module provides strong consistency guarantees for distributed Kiwi deployments
//! using the Raft consensus algorithm via the openraft library.

use openraft::Config;

pub mod cluster_config;
pub mod config_change;
pub mod consistency;
pub mod consistency_handler;
pub mod discovery;
pub mod error;
pub mod logging;
pub mod metrics;
pub mod network;
pub mod node;
pub mod performance;
pub mod placeholder_types;
pub mod protocol_compatibility;
pub mod redis_integration;
pub mod serialization;
pub mod state_machine;
pub mod storage;
pub mod types;

// Re-export commonly used types
pub use error::RaftError;
pub use node::{RaftNode, RaftNodeInterface};
pub use state_machine::KiwiStateMachine;
pub use storage::RaftStorage;
pub use types::*;

/// Create default Raft configuration
pub fn default_raft_config() -> Config {
    Config {
        heartbeat_interval: 150,
        election_timeout_min: 300,
        election_timeout_max: 600,
        max_payload_entries: 300,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(3000),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_config_consistency() {
        // Ensure our type configuration is properly set up
        let _config: Config = default_raft_config();
        assert_eq!(ConsistencyLevel::default(), ConsistencyLevel::Linearizable);
    }

    #[test]
    fn test_client_request_serialization() {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::from_bytes("SET".to_string(), vec![b"key".to_vec(), b"value".to_vec()]),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let serialized = serde_json::to_string(&request).unwrap();
        let deserialized: ClientRequest = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(request.id, deserialized.id);
        assert_eq!(request.command.command, deserialized.command.command);
        assert_eq!(request.consistency_level, deserialized.consistency_level);
    }
}

#[cfg(test)]
pub mod integration_tests;