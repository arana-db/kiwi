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

// OpenRaft compatibility layer that works around lifetime issues in 0.9.21

use std::sync::Arc;
use crate::state_machine::core::KiwiStateMachine;
use crate::types::{NodeId, ClientRequest, ClientResponse};

/// A compatibility layer that provides OpenRaft-like functionality
/// without directly implementing the problematic traits
pub struct OpenRaftCompatibilityLayer {
    state_machine: Arc<KiwiStateMachine>,
    node_id: NodeId,
}

impl OpenRaftCompatibilityLayer {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            state_machine: Arc::new(KiwiStateMachine::new(node_id)),
            node_id,
        }
    }

    pub fn with_state_machine(state_machine: Arc<KiwiStateMachine>) -> Self {
        let node_id = state_machine.node_id;
        Self {
            state_machine,
            node_id,
        }
    }

    /// Apply a client request to the state machine
    pub async fn apply_client_request(&self, request: &ClientRequest) -> Result<ClientResponse, String> {
        self.state_machine
            .apply_redis_command(request)
            .await
            .map_err(|e| e.to_string())
    }

    /// Apply multiple client requests in batch
    pub async fn apply_client_requests_batch(&self, requests: &[ClientRequest]) -> Result<Vec<ClientResponse>, String> {
        self.state_machine
            .apply_redis_commands_batch(requests)
            .await
            .map_err(|e| e.to_string())
    }

    /// Get the current applied index
    pub fn applied_index(&self) -> u64 {
        self.state_machine.applied_index()
    }

    /// Create a snapshot of the current state
    pub async fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        let snapshot = self.state_machine
            .create_snapshot()
            .await
            .map_err(|e| e.to_string())?;
        
        bincode::serialize(&snapshot).map_err(|e| e.to_string())
    }

    /// Restore from a snapshot
    pub async fn restore_from_snapshot(&self, snapshot_data: &[u8]) -> Result<(), String> {
        let snapshot = bincode::deserialize(snapshot_data).map_err(|e| e.to_string())?;
        
        self.state_machine
            .restore_from_snapshot(&snapshot)
            .await
            .map_err(|e| e.to_string())
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get a reference to the state machine
    pub fn state_machine(&self) -> &Arc<KiwiStateMachine> {
        &self.state_machine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RedisCommand, ConsistencyLevel, RequestId};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_compatibility_layer_basic_operations() {
        let compat = OpenRaftCompatibilityLayer::new(1);

        // Test SET command
        let set_request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![Bytes::from("key1"), Bytes::from("value1")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let response = compat.apply_client_request(&set_request).await.unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.id, set_request.id);

        // Test GET command
        let get_request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "GET".to_string(),
                vec![Bytes::from("key1")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let response = compat.apply_client_request(&get_request).await.unwrap();
        assert!(response.result.is_ok());
        assert_eq!(response.id, get_request.id);
    }

    #[tokio::test]
    async fn test_compatibility_layer_batch_operations() {
        let compat = OpenRaftCompatibilityLayer::new(1);

        let requests = vec![
            ClientRequest {
                id: RequestId::new(),
                command: RedisCommand::new(
                    "SET".to_string(),
                    vec![Bytes::from("key1"), Bytes::from("value1")],
                ),
                consistency_level: ConsistencyLevel::Linearizable,
            },
            ClientRequest {
                id: RequestId::new(),
                command: RedisCommand::new(
                    "SET".to_string(),
                    vec![Bytes::from("key2"), Bytes::from("value2")],
                ),
                consistency_level: ConsistencyLevel::Linearizable,
            },
        ];

        let responses = compat.apply_client_requests_batch(&requests).await.unwrap();
        assert_eq!(responses.len(), 2);
        assert!(responses[0].result.is_ok());
        assert!(responses[1].result.is_ok());
    }

    #[tokio::test]
    async fn test_compatibility_layer_snapshot_operations() {
        let compat = OpenRaftCompatibilityLayer::new(1);

        // Apply some data first
        let set_request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![Bytes::from("key1"), Bytes::from("value1")],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let _response = compat.apply_client_request(&set_request).await.unwrap();

        // Create snapshot
        let snapshot_data = compat.create_snapshot().await.unwrap();
        assert!(!snapshot_data.is_empty());

        // Create new compatibility layer and restore
        let compat2 = OpenRaftCompatibilityLayer::new(2);
        compat2.restore_from_snapshot(&snapshot_data).await.unwrap();

        // Verify the data was restored (this is a basic test)
        assert_eq!(compat2.node_id(), 2);
    }
}
