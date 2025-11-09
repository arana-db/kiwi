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

#![allow(clippy::result_large_err)]
#![allow(clippy::new_without_default)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::for_kv_map)]
#![allow(clippy::useless_format)]
#![allow(clippy::redundant_pattern_matching)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::type_complexity)]
#![allow(clippy::unnecessary_cast)]
#![allow(clippy::io_other_error)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::needless_return)]
#![allow(clippy::needless_borrow)]
#![allow(clippy::should_implement_trait)]
#![allow(clippy::needless_question_mark)]
#![allow(clippy::single_match)]
#![allow(clippy::unnecessary_lazy_evaluations)]
#![allow(clippy::useless_conversion)]
#![allow(clippy::needless_borrows_for_generic_args)]
#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::identity_op)]
#![allow(clippy::wildcard_in_or_patterns)]
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::len_without_is_empty)]

use openraft::Config;

// pub mod adaptor; // TODO: Re-enable when adaptor is properly implemented
#[cfg(test)]
pub mod adaptor_poc; // Proof of concept for Adaptor pattern
pub mod binlog;
pub mod cluster_config;
#[cfg(test)]
pub mod cluster_tests;
pub mod config_change;
pub mod consistency;
pub mod conversion;
pub mod consistency_handler;
pub mod discovery;
pub mod error;
pub mod health_monitor;
pub mod logging;
pub mod metrics;
pub mod monitoring_api;
pub mod network;
pub mod node;
pub mod performance;
pub mod placeholder_types;
pub mod protocol_compatibility;
pub mod redis_integration;
pub mod replication_mode;
pub mod rocksdb_integration;
pub mod segment_log;
pub mod sequence_mapping;
pub mod serialization;
pub mod simple_storage;
pub mod snapshot;
pub mod state_machine;
pub mod storage;
pub mod types;

// Re-export commonly used types
pub use error::RaftError;
pub use node::{RaftNode, RaftNodeInterface};
pub use state_machine::KiwiStateMachine;
pub use storage::RaftStorage;
pub use types::*;

// Re-export simple storage functions
pub use simple_storage::{create_simple_raft_storage, create_simple_raft_storage_with_engine};

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
mod unit_tests {
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
            command: RedisCommand::from_bytes(
                "SET".to_string(),
                vec![b"key".to_vec(), b"value".to_vec()],
            ),
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

#[cfg(test)]
mod tests;
