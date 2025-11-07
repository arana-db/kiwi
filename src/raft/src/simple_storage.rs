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

//! Simple storage implementation using openraft's built-in storage

use std::path::Path;

use crate::error::RaftError;
use crate::state_machine::KiwiStateMachine;
use crate::types::NodeId;

/// Placeholder storage type for compilation
pub struct PlaceholderStorage;

/// Create simple Raft storage components using memory storage
/// This is a temporary implementation until we can properly integrate with openraft's storage traits
pub fn create_simple_raft_storage<P: AsRef<Path>>(
    node_id: NodeId,
    _db_path: P,
) -> Result<(PlaceholderStorage, KiwiStateMachine), RaftError> {
    // Create a basic state machine for testing
    let state_machine = KiwiStateMachine::new(node_id);
    let storage = PlaceholderStorage;
    
    Ok((storage, state_machine))
}

/// Create simple Raft storage components with storage engine
pub fn create_simple_raft_storage_with_engine<P: AsRef<Path>>(
    _node_id: NodeId,
    _db_path: P,
    _storage_engine: std::sync::Arc<dyn crate::state_machine::StorageEngine>,
) -> Result<(PlaceholderStorage, KiwiStateMachine), RaftError> {
    // For now, return an error indicating this needs proper implementation
    // TODO: Implement proper storage integration
    Err(RaftError::Configuration {
        message: "Simple storage implementation with engine is not yet complete. Need to research openraft's correct storage API.".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_simple_storage() {
        let temp_dir = TempDir::new().unwrap();
        let (log_storage, state_machine) = create_simple_raft_storage(1, temp_dir.path()).unwrap();

        // Basic smoke test
        assert_eq!(state_machine.node_id, 1);
        drop(log_storage);
    }
}
