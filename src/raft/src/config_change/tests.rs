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

//! Tests for configuration change safety mechanisms

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::cluster_config::{ClusterConfiguration, NodeEndpoint};

    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;

    fn create_test_config() -> Arc<RwLock<ClusterConfiguration>> {
        use std::collections::BTreeMap;
        use std::path::PathBuf;

        let mut endpoints = BTreeMap::new();
        endpoints.insert(1, NodeEndpoint::new(1, "127.0.0.1".to_string(), 7379));
        endpoints.insert(2, NodeEndpoint::new(2, "127.0.0.1".to_string(), 7380));
        endpoints.insert(3, NodeEndpoint::new(3, "127.0.0.1".to_string(), 7381));

        Arc::new(RwLock::new(ClusterConfiguration {
            node_id: 1,
            enabled: true,
            data_dir: PathBuf::from("/tmp/raft"),
            endpoints,
            bootstrap: crate::cluster_config::BootstrapConfig::default(),
            raft_config: crate::cluster_config::RaftConfiguration::default(),
        }))
    }

    fn create_test_manager() -> ConfigChangeManager {
        let config = create_test_config();
        ConfigChangeManager::new(1, config, None)
    }

    #[tokio::test]
    async fn test_safety_checkpoint_creation() {
        let manager = create_test_manager();

        let checkpoint = manager.create_safety_checkpoint().await.unwrap();

        assert!(!checkpoint.id.is_empty());
        assert_eq!(checkpoint.membership_snapshot.len(), 3);
        assert!(checkpoint.membership_snapshot.contains(&1));
        assert!(checkpoint.membership_snapshot.contains(&2));
        assert!(checkpoint.membership_snapshot.contains(&3));
    }

    #[tokio::test]
    async fn test_validate_change_preconditions() {
        let manager = create_test_manager();

        let request = ConfigChangeRequest {
            change_type: ConfigChangeType::AddLearner {
                node_id: 4,
                endpoint: NodeEndpoint::new(4, "127.0.0.1".to_string(), 7382),
            },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test add learner".to_string(),
        };

        // This should pass basic validation (though it may fail on Raft-specific checks)
        let result = manager.validate_change_preconditions(&request).await;

        // We expect this to fail because we don't have a Raft node configured
        // but it should fail at the cluster stability check, not earlier
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_timing_constraints_validation() {
        let manager = create_test_manager();

        let request = ConfigChangeRequest {
            change_type: ConfigChangeType::AddLearner {
                node_id: 4,
                endpoint: NodeEndpoint::new(4, "127.0.0.1".to_string(), 7382),
            },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test timing constraints".to_string(),
        };

        // First call should pass timing validation (no previous operations)
        let result = manager.validate_timing_constraints(&request).await;
        assert!(result.is_ok());

        // Add a completed operation to the history
        {
            let mut operations = manager.active_operations.write().await;
            operations.push(ConfigChangeOperation {
                id: "test_op".to_string(),
                request: request.clone(),
                status: ConfigChangeStatus::Completed,
                started_at: chrono::Utc::now() - chrono::Duration::seconds(10),
                completed_at: Some(chrono::Utc::now() - chrono::Duration::seconds(5)),
                result: None,
            });
        }

        // Second call should fail due to timing constraints (unless forced)
        let result = manager.validate_timing_constraints(&request).await;
        assert!(result.is_err());

        // But should pass if forced
        let forced_request = ConfigChangeRequest {
            force: true,
            ..request
        };
        let result = manager.validate_timing_constraints(&forced_request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pause_and_resume_operations() {
        let manager = create_test_manager();

        let request = ConfigChangeRequest {
            change_type: ConfigChangeType::RemoveNode { node_id: 2 },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test pause operations".to_string(),
        };

        let paused_ops = manager
            .pause_conflicting_operations(&request)
            .await
            .unwrap();

        // Resume should not fail
        let result = manager.resume_operations(paused_ops).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_simulate_membership_change() {
        let manager = create_test_manager();
        let current_membership = manager.get_current_membership().await.unwrap();

        // Test adding a node
        let add_request = ConfigChangeRequest {
            change_type: ConfigChangeType::AddLearner {
                node_id: 4,
                endpoint: NodeEndpoint::new(4, "127.0.0.1".to_string(), 7382),
            },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test simulation".to_string(),
        };

        let new_membership = manager
            .simulate_membership_change(&add_request, &current_membership)
            .await
            .unwrap();
        assert_eq!(new_membership.len(), 4);
        assert!(new_membership.contains(&4));

        // Test removing a node
        let remove_request = ConfigChangeRequest {
            change_type: ConfigChangeType::RemoveNode { node_id: 3 },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test simulation".to_string(),
        };

        let new_membership = manager
            .simulate_membership_change(&remove_request, &current_membership)
            .await
            .unwrap();
        assert_eq!(new_membership.len(), 2);
        assert!(!new_membership.contains(&3));
    }

    #[tokio::test]
    async fn test_rollback_request_creation() {
        let manager = create_test_manager();

        // Test rollback for add learner
        let add_request = ConfigChangeRequest {
            change_type: ConfigChangeType::AddLearner {
                node_id: 4,
                endpoint: NodeEndpoint::new(4, "127.0.0.1".to_string(), 7382),
            },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test rollback".to_string(),
        };

        let rollback = manager.create_rollback_request(&add_request).unwrap();
        match rollback.change_type {
            ConfigChangeType::RemoveNode { node_id } => {
                assert_eq!(node_id, 4);
            }
            _ => panic!("Expected RemoveNode rollback for AddLearner"),
        }

        // Test rollback for remove node (should fail)
        let remove_request = ConfigChangeRequest {
            change_type: ConfigChangeType::RemoveNode { node_id: 2 },
            force: false,
            timeout: Duration::from_secs(30),
            reason: "Test rollback".to_string(),
        };

        let rollback_result = manager.create_rollback_request(&remove_request);
        assert!(rollback_result.is_err());
    }

    #[tokio::test]
    async fn test_operation_tracking() {
        let manager = create_test_manager();

        // Initially no operations
        let ops = manager.get_active_operations().await;
        assert_eq!(ops.len(), 0);

        // Add an operation
        {
            let mut operations = manager.active_operations.write().await;
            operations.push(ConfigChangeOperation {
                id: "test_op_1".to_string(),
                request: ConfigChangeRequest {
                    change_type: ConfigChangeType::AddLearner {
                        node_id: 4,
                        endpoint: NodeEndpoint::new(4, "127.0.0.1".to_string(), 7382),
                    },
                    force: false,
                    timeout: Duration::from_secs(30),
                    reason: "Test operation".to_string(),
                },
                status: ConfigChangeStatus::InProgress,
                started_at: chrono::Utc::now(),
                completed_at: None,
                result: None,
            });
        }

        // Should have one operation
        let ops = manager.get_active_operations().await;
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].status, ConfigChangeStatus::InProgress);

        // Cancel the operation
        let result = manager.cancel_operation("test_op_1").await;
        assert!(result.is_ok());

        // Operation should be marked as rolled back
        let ops = manager.get_active_operations().await;
        assert_eq!(ops[0].status, ConfigChangeStatus::RolledBack);
    }

    #[tokio::test]
    async fn test_cleanup_operations() {
        let manager = create_test_manager();

        // Add some old completed operations
        {
            let mut operations = manager.active_operations.write().await;
            operations.push(ConfigChangeOperation {
                id: "old_op".to_string(),
                request: ConfigChangeRequest {
                    change_type: ConfigChangeType::AddLearner {
                        node_id: 4,
                        endpoint: NodeEndpoint::new(4, "127.0.0.1".to_string(), 7382),
                    },
                    force: false,
                    timeout: Duration::from_secs(30),
                    reason: "Old operation".to_string(),
                },
                status: ConfigChangeStatus::Completed,
                started_at: chrono::Utc::now() - chrono::Duration::hours(2),
                completed_at: Some(chrono::Utc::now() - chrono::Duration::hours(2)),
                result: None,
            });

            operations.push(ConfigChangeOperation {
                id: "recent_op".to_string(),
                request: ConfigChangeRequest {
                    change_type: ConfigChangeType::AddLearner {
                        node_id: 5,
                        endpoint: NodeEndpoint::new(5, "127.0.0.1".to_string(), 7383),
                    },
                    force: false,
                    timeout: Duration::from_secs(30),
                    reason: "Recent operation".to_string(),
                },
                status: ConfigChangeStatus::Completed,
                started_at: chrono::Utc::now() - chrono::Duration::minutes(5),
                completed_at: Some(chrono::Utc::now() - chrono::Duration::minutes(5)),
                result: None,
            });
        }

        // Should have 2 operations
        let ops = manager.get_active_operations().await;
        assert_eq!(ops.len(), 2);

        // Clean up operations older than 1 hour
        manager.cleanup_operations(Duration::from_secs(3600)).await;

        // Should have 1 operation left (the recent one)
        let ops = manager.get_active_operations().await;
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].id, "recent_op");
    }
}
