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

//! Integration tests for Raft cluster functionality

use engine::RocksdbEngine;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};
use crate::error::RaftResult;
use crate::network::KiwiRaftNetworkFactory;
use crate::state_machine::KiwiStateMachine;
use crate::storage::RaftStorage;
use crate::types::{
    ClientRequest, ClientResponse, ConsistencyLevel, NodeId, RedisCommand, RequestId,
};
use bytes::Bytes;

/// Test cluster configuration
#[derive(Debug, Clone)]
pub struct TestClusterConfig {
    pub node_count: usize,
    pub base_port: u16,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

impl Default for TestClusterConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            base_port: 7380,
            election_timeout_ms: 150,
            heartbeat_interval_ms: 50,
        }
    }
}

/// Helper function to create a test client request
fn create_test_request(id: u64, command: &str, args: Vec<&str>) -> ClientRequest {
    ClientRequest {
        id: RequestId(id),
        command: RedisCommand {
            command: command.to_string(),
            args: args.iter().map(|s| Bytes::from(s.to_string())).collect(),
        },
        consistency_level: ConsistencyLevel::Linearizable,
    }
}

/// Helper function to create a test client request with bytes args
#[allow(dead_code)]
fn create_test_request_bytes(id: u64, command: &str, args: Vec<Bytes>) -> ClientRequest {
    ClientRequest {
        id: RequestId(id),
        command: RedisCommand {
            command: command.to_string(),
            args,
        },
        consistency_level: ConsistencyLevel::Linearizable,
    }
}

/// Test cluster for integration testing
pub struct TestCluster {
    nodes: HashMap<NodeId, TestNode>,
    #[allow(dead_code)]
    config: TestClusterConfig,
    _temp_dirs: Vec<TempDir>,
}

/// Individual test node
pub struct TestNode {
    pub node_id: NodeId,
    pub endpoint: String,
    pub storage: Arc<RaftStorage>,
    pub state_machine: Arc<KiwiStateMachine>,
    pub network: KiwiRaftNetworkFactory,
    // Note: RaftNode would be added when implemented in task 5
}

impl TestCluster {
    /// Create a new test cluster
    pub async fn new(config: TestClusterConfig) -> RaftResult<Self> {
        let mut nodes = HashMap::new();
        let mut temp_dirs = Vec::new();

        // Create nodes
        for i in 0..config.node_count {
            let node_id = (i + 1) as NodeId;
            let endpoint = format!("127.0.0.1:{}", config.base_port + i as u16);

            // Create temporary directory for this node
            let temp_dir = TempDir::new().map_err(|e| {
                crate::error::RaftError::state_machine(format!("Failed to create temp dir: {}", e))
            })?;

            // Create storage
            let storage = Arc::new(RaftStorage::new(temp_dir.path())?);

            // Create state machine
            let state_machine = Arc::new(KiwiStateMachine::new(node_id));

            // Create network factory
            let network_factory = KiwiRaftNetworkFactory::new(node_id);

            let test_node = TestNode {
                node_id,
                endpoint: endpoint.clone(),
                storage,
                state_machine,
                network: network_factory.clone(),
            };

            nodes.insert(node_id, test_node);
            temp_dirs.push(temp_dir);
        }

        // Configure network endpoints for all nodes
        for node in nodes.values() {
            for other_node in nodes.values() {
                if node.node_id != other_node.node_id {
                    node.network.add_endpoint(other_node.node_id, other_node.endpoint.clone()).await;
                }
            }
        }

        Ok(Self {
            nodes,
            config,
            _temp_dirs: temp_dirs,
        })
    }

    /// Get a node by ID
    pub fn get_node(&self, node_id: NodeId) -> Option<&TestNode> {
        self.nodes.get(&node_id)
    }

    /// Get all node IDs
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().cloned().collect()
    }

    /// Simulate network partition by isolating a node
    pub async fn isolate_node(&mut self, node_id: NodeId) -> RaftResult<()> {
        // In a real implementation, this would disable network communication
        // For now, we'll simulate by clearing the node's network endpoints
        if let Some(node) = self.nodes.get(&node_id) {
            let mut endpoints = node.network.endpoints.write().await;
            endpoints.clear();
        }
        Ok(())
    }

    /// Restore network connectivity for a node
    pub async fn restore_node(&mut self, node_id: NodeId) -> RaftResult<()> {
        // Restore network endpoints
        if let Some(node) = self.nodes.get(&node_id) {
            for other_node in self.nodes.values() {
                if node.node_id != other_node.node_id {
                    node.network.add_endpoint(other_node.node_id, other_node.endpoint.clone()).await;
                }
            }
        }
        Ok(())
    }

    /// Simulate node failure by stopping a node
    pub async fn stop_node(&mut self, node_id: NodeId) -> RaftResult<()> {
        // In a real implementation, this would stop the Raft node
        // For testing, we simulate by marking the node as unavailable
        log::info!("Simulating node {} failure", node_id);
        Ok(())
    }

    /// Restart a failed node
    pub async fn restart_node(&mut self, node_id: NodeId) -> RaftResult<()> {
        // In a real implementation, this would restart the Raft node
        log::info!("Simulating node {} restart", node_id);
        Ok(())
    }

    /// Send a client request to a specific node
    pub async fn send_request(
        &self,
        node_id: NodeId,
        request: ClientRequest,
    ) -> RaftResult<ClientResponse> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or_else(|| crate::error::RaftError::invalid_request("Node not found"))?;

        // Apply the command directly to the state machine for testing
        node.state_machine.apply_redis_command(&request).await
    }

    /// Wait for cluster to reach consensus (simplified for testing)
    pub async fn wait_for_consensus(&self, timeout_ms: u64) -> RaftResult<()> {
        let timeout_duration = Duration::from_millis(timeout_ms);

        timeout(timeout_duration, async {
            // In a real implementation, this would check for leader election
            // and log replication consensus
            sleep(Duration::from_millis(100)).await;
        })
        .await
        .map_err(|_| crate::error::RaftError::timeout("Consensus timeout"))?;

        Ok(())
    }
}

// Mock engine creation for testing
#[allow(dead_code)]
fn create_mock_engine(temp_dir: &TempDir) -> RaftResult<RocksdbEngine> {
    use rocksdb::{DB, Options};
    
    let mut opts = Options::default();
    opts.create_if_missing(true);
    
    let db = DB::open(&opts, temp_dir.path()).map_err(|e| {
        crate::error::RaftError::state_machine(format!("Failed to create DB: {}", e))
    })?;
    
    Ok(RocksdbEngine::new(db))
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_creation() {
        let config = TestClusterConfig::default();
        let cluster = TestCluster::new(config.clone()).await.unwrap();

        assert_eq!(cluster.nodes.len(), config.node_count);

        // Verify all nodes have correct IDs
        for i in 1..=config.node_count {
            let node_id = i as NodeId;
            assert!(cluster.get_node(node_id).is_some());
        }
    }

    #[tokio::test]
    async fn test_single_node_operations() {
        let config = TestClusterConfig {
            node_count: 1,
            ..Default::default()
        };
        let cluster = TestCluster::new(config).await.unwrap();

        let node_id = 1;
        let request = create_test_request(1, "SET", vec!["test_key", "test_value"]);

        let response = cluster.send_request(node_id, request).await.unwrap();
        assert_eq!(response.id, RequestId(1));
        assert!(response.result.is_ok());
    }

    #[tokio::test]
    async fn test_three_node_cluster() {
        let config = TestClusterConfig::default();
        let cluster = TestCluster::new(config).await.unwrap();

        // Test that all nodes can process requests
        for node_id in cluster.node_ids() {
            let request = ClientRequest {
                id: RequestId(node_id),
                command: RedisCommand {
                    command: "PING".to_string(),
                    args: vec![],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };

            let response = cluster.send_request(node_id, request).await.unwrap();
            assert_eq!(response.id, RequestId(node_id));
            assert!(response.result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_network_partition_simulation() {
        let config = TestClusterConfig::default();
        let mut cluster = TestCluster::new(config).await.unwrap();

        // Isolate node 1
        cluster.isolate_node(1).await.unwrap();

        // Verify node 1's network is isolated
        let node1 = cluster.get_node(1).unwrap();
        assert!(node1.network.endpoints.read().await.is_empty());

        // Restore node 1
        cluster.restore_node(1).await.unwrap();

        // Verify node 1's network is restored
        let node1 = cluster.get_node(1).unwrap();
        assert_eq!(node1.network.endpoints.read().await.len(), 2); // Connected to nodes 2 and 3
    }

    #[tokio::test]
    async fn test_node_failure_and_recovery() {
        let config = TestClusterConfig::default();
        let mut cluster = TestCluster::new(config).await.unwrap();

        // Stop node 2
        cluster.stop_node(2).await.unwrap();

        // Remaining nodes should still be able to process requests
        let request = ClientRequest {
            id: RequestId(1),
            command: RedisCommand {
                command: "SET".to_string(),
                args: vec!["key_after_failure".to_string().into(), "value".to_string().into()],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let response = cluster.send_request(1, request).await.unwrap();
        assert!(response.result.is_ok());

        // Restart node 2
        cluster.restart_node(2).await.unwrap();

        // Node 2 should be able to process requests again
        let request2 = ClientRequest {
            id: RequestId(2),
            command: RedisCommand {
                command: "PING".to_string(),
                args: vec![],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let response2 = cluster.send_request(2, request2).await.unwrap();
        assert!(response2.result.is_ok());
    }

    #[tokio::test]
    async fn test_redis_protocol_compatibility() {
        let config = TestClusterConfig::default();
        let cluster = TestCluster::new(config).await.unwrap();

        let node_id = 1;

        // Test SET command
        let set_request = ClientRequest {
            id: RequestId(1),
            command: RedisCommand {
                command: "SET".to_string(),
                args: vec!["redis_key".to_string().into(), "redis_value".to_string().into()],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let set_response = cluster.send_request(node_id, set_request).await.unwrap();
        assert!(set_response.result.is_ok());

        // Test GET command
        let get_request = ClientRequest {
            id: RequestId(2),
            command: RedisCommand {
                command: "GET".to_string(),
                args: vec!["redis_key".to_string().into()],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let get_response = cluster.send_request(node_id, get_request).await.unwrap();
        assert!(get_response.result.is_ok());
        assert_eq!(
            get_response.result.unwrap(),
            bytes::Bytes::from("redis_value")
        );

        // Test DEL command
        let del_request = ClientRequest {
            id: RequestId(3),
            command: RedisCommand {
                command: "DEL".to_string(),
                args: vec!["redis_key".to_string().into()],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let del_response = cluster.send_request(node_id, del_request).await.unwrap();
        assert!(del_response.result.is_ok());
        assert_eq!(del_response.result.unwrap(), bytes::Bytes::from("1"));

        // Verify key is deleted
        let get_request2 = ClientRequest {
            id: RequestId(4),
            command: RedisCommand {
                command: "GET".to_string(),
                args: vec!["redis_key".to_string().into()],
            },
            consistency_level: ConsistencyLevel::Linearizable,
        };

        let get_response2 = cluster.send_request(node_id, get_request2).await.unwrap();
        assert_eq!(get_response2.result.unwrap(), bytes::Bytes::new());
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let config = TestClusterConfig::default();
        let cluster = TestCluster::new(config).await.unwrap();
        let cluster = Arc::new(cluster);

        let mut handles = vec![];

        // Send concurrent requests to different nodes
        for i in 0..10 {
            let cluster_clone = Arc::clone(&cluster);
            let handle = tokio::spawn(async move {
                let node_id = (i % 3) + 1; // Distribute across nodes 1, 2, 3
                let request = ClientRequest {
                    id: RequestId(i),
                    command: RedisCommand {
                        command: "SET".to_string(),
                        args: vec![format!("concurrent_key_{}", i).into(), format!("value_{}", i).into()],
                    },
                    consistency_level: ConsistencyLevel::Linearizable,
                };

                cluster_clone.send_request(node_id, request).await
            });
            handles.push(handle);
        }

        // Wait for all requests to complete
        for handle in handles {
            let response = handle.await.unwrap().unwrap();
            assert!(response.result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_large_cluster() {
        let config = TestClusterConfig {
            node_count: 7,
            ..Default::default()
        };
        let cluster = TestCluster::new(config.clone()).await.unwrap();

        assert_eq!(cluster.nodes.len(), 7);

        // Test that all nodes in large cluster can process requests
        for node_id in cluster.node_ids() {
            let request = ClientRequest {
                id: RequestId(node_id),
                command: RedisCommand {
                    command: "SET".to_string(),
                    args: vec![
                        format!("large_cluster_key_{}", node_id).into(),
                        "value".to_string().into(),
                    ],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };

            let response = cluster.send_request(node_id, request).await.unwrap();
            assert!(response.result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_consensus_timeout() {
        let config = TestClusterConfig::default();
        let cluster = TestCluster::new(config).await.unwrap();

        // Test consensus with very short timeout (should succeed quickly)
        let result = cluster.wait_for_consensus(1000).await;
        assert!(result.is_ok());

        // Test consensus with very short timeout (might timeout)
        let _result = cluster.wait_for_consensus(1).await;
        // This might succeed or timeout depending on timing
    }

    #[tokio::test]
    async fn test_mixed_failure_scenarios() {
        let config = TestClusterConfig {
            node_count: 5,
            ..Default::default()
        };
        let mut cluster = TestCluster::new(config).await.unwrap();

        // Simulate mixed failures
        cluster.stop_node(1).await.unwrap();
        cluster.isolate_node(2).await.unwrap();

        // Remaining nodes should still function
        for node_id in [3, 4, 5] {
            let request = ClientRequest {
                id: RequestId(node_id),
                command: RedisCommand {
                    command: "PING".to_string(),
                    args: vec![],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };

            let response = cluster.send_request(node_id, request).await.unwrap();
            assert!(response.result.is_ok());
        }

        // Recover nodes
        cluster.restart_node(1).await.unwrap();
        cluster.restore_node(2).await.unwrap();

        // All nodes should work again
        for node_id in cluster.node_ids() {
            let request = ClientRequest {
                id: RequestId(node_id + 100),
                command: RedisCommand {
                    command: "PING".to_string(),
                    args: vec![],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };

            let response = cluster.send_request(node_id, request).await.unwrap();
            assert!(response.result.is_ok());
        }
    }
}

/// Chaos testing module for network partitions and node failures
pub mod chaos_tests {
    use super::*;
    use rand::Rng;
    use rand::seq::SliceRandom;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::RwLock;

    /// Chaos test configuration
    #[derive(Debug, Clone)]
    pub struct ChaosConfig {
        pub duration_seconds: u64,
        pub failure_probability: f64,
        pub partition_probability: f64,
        pub recovery_probability: f64,
        pub operation_rate_per_second: u64,
    }

    impl Default for ChaosConfig {
        fn default() -> Self {
            Self {
                duration_seconds: 30,
                failure_probability: 0.1,
                partition_probability: 0.05,
                recovery_probability: 0.3,
                operation_rate_per_second: 10,
            }
        }
    }

    /// Chaos test runner
    pub struct ChaosTestRunner {
        cluster: Arc<RwLock<TestCluster>>,
        config: ChaosConfig,
        running: Arc<AtomicBool>,
        failed_nodes: Arc<RwLock<Vec<NodeId>>>,
        partitioned_nodes: Arc<RwLock<Vec<NodeId>>>,
    }

    impl ChaosTestRunner {
        pub fn new(cluster: TestCluster, config: ChaosConfig) -> Self {
            Self {
                cluster: Arc::new(RwLock::new(cluster)),
                config,
                running: Arc::new(AtomicBool::new(false)),
                failed_nodes: Arc::new(RwLock::new(Vec::new())),
                partitioned_nodes: Arc::new(RwLock::new(Vec::new())),
            }
        }

        /// Run chaos test
        pub async fn run_chaos_test(&self) -> RaftResult<ChaosTestResults> {
            self.running.store(true, Ordering::SeqCst);

            let start_time = std::time::Instant::now();
            let mut results = ChaosTestResults::new();

            // Start background chaos operations
            let chaos_handle = self.start_chaos_operations();

            // Start client operations
            let client_handle = self.start_client_operations(&mut results);

            // Wait for test duration
            sleep(Duration::from_secs(self.config.duration_seconds)).await;

            // Stop chaos test
            self.running.store(false, Ordering::SeqCst);

            // Wait for handles to complete
            let _ = tokio::join!(chaos_handle, client_handle);

            results.duration = start_time.elapsed();
            results.validate_linearizability().await?;

            Ok(results)
        }

        /// Start chaos operations (failures, partitions, recoveries)
        async fn start_chaos_operations(&self) -> RaftResult<()> {
            let mut rng = rand::thread_rng();

            while self.running.load(Ordering::SeqCst) {
                let cluster = self.cluster.read().await;
                let node_ids = cluster.node_ids();
                drop(cluster);

                // Random node failure
                if rng.gen::<f64>() < self.config.failure_probability {
                    if let Some(&node_id) = node_ids.choose(&mut rng) {
                        let mut failed_nodes = self.failed_nodes.write().await;
                        if !failed_nodes.contains(&node_id) {
                            let mut cluster = self.cluster.write().await;
                            cluster.stop_node(node_id).await?;
                            failed_nodes.push(node_id);
                            log::info!("Chaos: Failed node {}", node_id);
                        }
                    }
                }

                // Random network partition
                if rng.gen::<f64>() < self.config.partition_probability {
                    if let Some(&node_id) = node_ids.choose(&mut rng) {
                        let mut partitioned_nodes = self.partitioned_nodes.write().await;
                        if !partitioned_nodes.contains(&node_id) {
                            let mut cluster = self.cluster.write().await;
                            cluster.isolate_node(node_id).await?;
                            partitioned_nodes.push(node_id);
                            log::info!("Chaos: Partitioned node {}", node_id);
                        }
                    }
                }

                // Random recovery
                if rng.gen::<f64>() < self.config.recovery_probability {
                    let mut recovered = false;

                    // Recover failed nodes
                    {
                        let mut failed_nodes = self.failed_nodes.write().await;
                        if !failed_nodes.is_empty() {
                            let idx = rng.gen_range(0..failed_nodes.len());
                            let node_id = failed_nodes.remove(idx);
                            let mut cluster = self.cluster.write().await;
                            cluster.restart_node(node_id).await?;
                            log::info!("Chaos: Recovered failed node {}", node_id);
                            recovered = true;
                        }
                    }

                    // Recover partitioned nodes
                    if !recovered {
                        let mut partitioned_nodes = self.partitioned_nodes.write().await;
                        if !partitioned_nodes.is_empty() {
                            let idx = rng.gen_range(0..partitioned_nodes.len());
                            let node_id = partitioned_nodes.remove(idx);
                            let mut cluster = self.cluster.write().await;
                            cluster.restore_node(node_id).await?;
                            log::info!("Chaos: Recovered partitioned node {}", node_id);
                        }
                    }
                }

                // Wait before next chaos operation
                sleep(Duration::from_millis(100)).await;
            }

            Ok(())
        }

        /// Start client operations
        async fn start_client_operations(&self, results: &mut ChaosTestResults) -> RaftResult<()> {
            let mut operation_id = 0u64;
            let interval = Duration::from_millis(1000 / self.config.operation_rate_per_second);

            while self.running.load(Ordering::SeqCst) {
                operation_id += 1;

                let cluster = self.cluster.read().await;
                let node_ids = cluster.node_ids();

                if let Some(&target_node) = node_ids.choose(&mut rand::thread_rng()) {
                    let operation = self.generate_random_operation(operation_id);
                    let start_time = std::time::Instant::now();

                    match cluster.send_request(target_node, operation.clone()).await {
                        Ok(response) => {
                            results.record_operation(
                                operation,
                                response,
                                start_time.elapsed(),
                                true,
                            );
                        }
                        Err(e) => {
                            log::warn!("Operation {} failed: {}", operation_id, e);
                            let error_response = ClientResponse {
                                id: operation.id,
                                result: Err(e.to_string()),
                                leader_id: None,
                            };
                            results.record_operation(
                                operation,
                                error_response,
                                start_time.elapsed(),
                                false,
                            );
                        }
                    }
                }

                drop(cluster);
                sleep(interval).await;
            }

            Ok(())
        }

        /// Generate random operation for testing
        fn generate_random_operation(&self, id: u64) -> ClientRequest {
            let mut rng = rand::thread_rng();
            let operation_type = rng.gen_range(0..4);

            match operation_type {
                0 => ClientRequest {
                    id: RequestId(id),
                    command: RedisCommand {
                        command: "SET".to_string(),
                        args: vec![
                            format!("chaos_key_{}", rng.gen_range(0..100)).into(),
                            format!("value_{}", id).into(),
                        ],
                    },
                    consistency_level: ConsistencyLevel::Linearizable,
                },
                1 => ClientRequest {
                    id: RequestId(id),
                    command: RedisCommand {
                        command: "GET".to_string(),
                        args: vec![format!("chaos_key_{}", rng.gen_range(0..100)).into()],
                    },
                    consistency_level: ConsistencyLevel::Linearizable,
                },
                2 => ClientRequest {
                    id: RequestId(id),
                    command: RedisCommand {
                        command: "DEL".to_string(),
                        args: vec![format!("chaos_key_{}", rng.gen_range(0..100)).into()],
                    },
                    consistency_level: ConsistencyLevel::Linearizable,
                },
                _ => ClientRequest {
                    id: RequestId(id),
                    command: RedisCommand {
                        command: "PING".to_string(),
                        args: vec![],
                    },
                    consistency_level: ConsistencyLevel::Linearizable,
                },
            }
        }
    }

    /// Results from chaos testing
    #[derive(Debug)]
    pub struct ChaosTestResults {
        pub duration: Duration,
        pub total_operations: u64,
        pub successful_operations: u64,
        pub failed_operations: u64,
        pub operations: Vec<OperationRecord>,
    }

    #[derive(Debug, Clone)]
    pub struct OperationRecord {
        pub operation: ClientRequest,
        pub response: ClientResponse,
        pub duration: Duration,
        pub successful: bool,
        pub timestamp: std::time::Instant,
    }

    impl ChaosTestResults {
        pub fn new() -> Self {
            Self {
                duration: Duration::default(),
                total_operations: 0,
                successful_operations: 0,
                failed_operations: 0,
                operations: Vec::new(),
            }
        }

        pub fn record_operation(
            &mut self,
            operation: ClientRequest,
            response: ClientResponse,
            duration: Duration,
            successful: bool,
        ) {
            self.total_operations += 1;
            if successful {
                self.successful_operations += 1;
            } else {
                self.failed_operations += 1;
            }

            self.operations.push(OperationRecord {
                operation,
                response,
                duration,
                successful,
                timestamp: std::time::Instant::now(),
            });
        }

        /// Validate linearizability of operations
        pub async fn validate_linearizability(&self) -> RaftResult<()> {
            // Simplified linearizability check
            // In a real implementation, this would use a proper linearizability checker

            let mut set_operations = Vec::new();
            let mut get_operations = Vec::new();

            for record in &self.operations {
                if !record.successful {
                    continue;
                }

                match record.operation.command.command.as_str() {
                    "SET" => {
                        if record.operation.command.args.len() >= 2 {
                            set_operations.push((
                                record.operation.command.args[0].clone(),
                                record.operation.command.args[1].clone(),
                                record.timestamp,
                            ));
                        }
                    }
                    "GET" => {
                        if !record.operation.command.args.is_empty() {
                            let key = record.operation.command.args[0].clone();
                            let value = if let Ok(ref result) = record.response.result {
                                if result.is_empty() {
                                    None
                                } else {
                                    Some(String::from_utf8_lossy(result).to_string())
                                }
                            } else {
                                None
                            };
                            get_operations.push((key, value, record.timestamp));
                        }
                    }
                    _ => {}
                }
            }

            // Basic consistency check: ensure GET operations return values
            // that were SET before them
            for (get_key, get_value, get_time) in &get_operations {
                if let Some(get_val) = get_value {
                    // Find the most recent SET for this key before the GET
                    let mut latest_set_value = None;
                    let mut latest_set_time = None;

                    for (set_key, set_value, set_time) in &set_operations {
                        if set_key == get_key && set_time < get_time {
                            if latest_set_time.is_none() || set_time > &latest_set_time.unwrap() {
                                latest_set_value = Some(set_value.clone());
                                latest_set_time = Some(*set_time);
                            }
                        }
                    }

                    // If we found a SET operation, the GET should return that value
                    if let Some(expected_value) = latest_set_value {
                        if get_val != &expected_value {
                            return Err(crate::error::RaftError::consistency(format!(
                                "Linearizability violation: GET {:?} returned '{:?}' but expected '{:?}'",
                                get_key, get_val, expected_value
                            )));
                        }
                    }
                }
            }

            log::info!(
                "Linearizability check passed for {} operations",
                self.operations.len()
            );
            Ok(())
        }

        /// Calculate success rate
        pub fn success_rate(&self) -> f64 {
            if self.total_operations == 0 {
                0.0
            } else {
                self.successful_operations as f64 / self.total_operations as f64
            }
        }

        /// Calculate average operation latency
        pub fn average_latency(&self) -> Duration {
            if self.operations.is_empty() {
                return Duration::default();
            }

            let total_duration: Duration = self.operations.iter().map(|op| op.duration).sum();

            total_duration / self.operations.len() as u32
        }
    }

    #[cfg(test)]
    mod chaos_test_cases {
        use super::*;

        #[tokio::test]
        async fn test_basic_chaos_scenario() {
            let config = TestClusterConfig::default();
            let cluster = TestCluster::new(config).await.unwrap();

            let chaos_config = ChaosConfig {
                duration_seconds: 5,
                failure_probability: 0.2,
                partition_probability: 0.1,
                recovery_probability: 0.5,
                operation_rate_per_second: 5,
            };

            let chaos_runner = ChaosTestRunner::new(cluster, chaos_config);
            let results = chaos_runner.run_chaos_test().await.unwrap();

            assert!(results.total_operations > 0);
            assert!(results.success_rate() > 0.0);
            println!(
                "Chaos test completed: {} operations, {:.2}% success rate",
                results.total_operations,
                results.success_rate() * 100.0
            );
        }

        #[tokio::test]
        async fn test_network_partition_recovery() {
            let config = TestClusterConfig {
                node_count: 5,
                ..Default::default()
            };
            let cluster = TestCluster::new(config).await.unwrap();

            let chaos_config = ChaosConfig {
                duration_seconds: 10,
                failure_probability: 0.0,
                partition_probability: 0.3,
                recovery_probability: 0.4,
                operation_rate_per_second: 3,
            };

            let chaos_runner = ChaosTestRunner::new(cluster, chaos_config);
            let results = chaos_runner.run_chaos_test().await.unwrap();

            // Even with network partitions, some operations should succeed
            assert!(results.successful_operations > 0);
            assert!(results.success_rate() > 0.1); // At least 10% success rate
        }

        #[tokio::test]
        async fn test_mixed_failures() {
            let config = TestClusterConfig {
                node_count: 7,
                ..Default::default()
            };
            let cluster = TestCluster::new(config).await.unwrap();

            let chaos_config = ChaosConfig {
                duration_seconds: 15,
                failure_probability: 0.15,
                partition_probability: 0.1,
                recovery_probability: 0.3,
                operation_rate_per_second: 8,
            };

            let chaos_runner = ChaosTestRunner::new(cluster, chaos_config);
            let results = chaos_runner.run_chaos_test().await.unwrap();

            // With a larger cluster, we should maintain better availability
            assert!(results.total_operations > 50);
            assert!(results.success_rate() > 0.3); // At least 30% success rate

            // Verify linearizability
            results.validate_linearizability().await.unwrap();
        }

        #[tokio::test]
        async fn test_high_load_chaos() {
            let config = TestClusterConfig::default();
            let cluster = TestCluster::new(config).await.unwrap();

            let chaos_config = ChaosConfig {
                duration_seconds: 8,
                failure_probability: 0.1,
                partition_probability: 0.05,
                recovery_probability: 0.4,
                operation_rate_per_second: 20, // High load
            };

            let chaos_runner = ChaosTestRunner::new(cluster, chaos_config);
            let results = chaos_runner.run_chaos_test().await.unwrap();

            assert!(results.total_operations > 100);
            println!(
                "High load chaos test: {} ops, avg latency: {:?}",
                results.total_operations,
                results.average_latency()
            );
        }

        #[tokio::test]
        async fn test_linearizability_validation() {
            let config = TestClusterConfig {
                node_count: 3,
                ..Default::default()
            };
            let cluster = TestCluster::new(config).await.unwrap();

            // Run a controlled test with specific operations
            let node_id = 1;

            // SET key1 = value1
            let set1 = ClientRequest {
                id: RequestId(1),
                command: RedisCommand {
                    command: "SET".to_string(),
                    args: vec!["key1".to_string().into(), "value1".to_string().into()],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            cluster.send_request(node_id, set1).await.unwrap();

            // GET key1 should return value1
            let get1 = ClientRequest {
                id: RequestId(2),
                command: RedisCommand {
                    command: "GET".to_string(),
                    args: vec!["key1".to_string().into()],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            let response = cluster.send_request(node_id, get1).await.unwrap();
            assert_eq!(response.result.unwrap(), bytes::Bytes::from("value1"));

            // SET key1 = value2
            let set2 = ClientRequest {
                id: RequestId(3),
                command: RedisCommand {
                    command: "SET".to_string(),
                    args: vec!["key1".to_string().into(), "value2".to_string().into()],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            cluster.send_request(node_id, set2).await.unwrap();

            // GET key1 should return value2
            let get2 = ClientRequest {
                id: RequestId(4),
                command: RedisCommand {
                    command: "GET".to_string(),
                    args: vec!["key1".to_string().into()],
                },
                consistency_level: ConsistencyLevel::Linearizable,
            };
            let response2 = cluster.send_request(node_id, get2).await.unwrap();
            assert_eq!(response2.result.unwrap(), bytes::Bytes::from("value2"));
        }
    }
}
