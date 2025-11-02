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

//! Cluster configuration system for Raft consensus

use crate::error::{RaftError, RaftResult};
use crate::types::{ClusterConfig, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

/// Node endpoint information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeEndpoint {
    pub node_id: NodeId,
    pub host: String,
    pub port: u16,
    pub is_secure: bool,
}

impl NodeEndpoint {
    pub fn new(node_id: NodeId, host: String, port: u16) -> Self {
        Self {
            node_id,
            host,
            port,
            is_secure: false,
        }
    }

    pub fn with_tls(node_id: NodeId, host: String, port: u16) -> Self {
        Self {
            node_id,
            host,
            port,
            is_secure: true,
        }
    }

    pub fn address(&self) -> String {
        // Bracket IPv6 addresses for proper parsing
        if self.host.contains(':') && !(self.host.starts_with('[') && self.host.ends_with(']')) {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    pub fn socket_addr(&self) -> RaftResult<SocketAddr> {
        let addr = self.address();
        addr.parse().map_err(|e| {
            RaftError::configuration(format!("Invalid socket address {}: {}", addr, e))
        })
    }
}

impl FromStr for NodeEndpoint {
    type Err = RaftError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Find first colon for node_id
        let (node_id_str, rest) = s.split_once(':').ok_or_else(|| {
            RaftError::configuration(format!(
                "Invalid node endpoint format '{}', expected 'node_id:host:port'",
                s
            ))
        })?;

        let node_id = node_id_str.parse::<NodeId>().map_err(|e| {
            RaftError::configuration(format!("Invalid node ID '{}': {}", node_id_str, e))
        })?;

        // Find last colon for port (handles IPv6)
        let (host, port_str) = rest.rsplit_once(':').ok_or_else(|| {
            RaftError::configuration(format!("Invalid host:port format '{}'", rest))
        })?;

        let port = port_str
            .parse::<u16>()
            .map_err(|e| RaftError::configuration(format!("Invalid port '{}': {}", port_str, e)))?;

        Ok(NodeEndpoint::new(node_id, host.to_string(), port))
    }
}

impl std::fmt::Display for NodeEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.node_id, self.host, self.port)
    }
}

/// Bootstrap configuration for initial cluster setup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapConfig {
    pub initial_members: Vec<NodeEndpoint>,
    pub bootstrap_expect: usize,
    pub join_timeout: Duration,
    pub retry_interval: Duration,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            initial_members: Vec::new(),
            bootstrap_expect: 1,
            join_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_secs(5),
        }
    }
}

/// Comprehensive cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfiguration {
    pub node_id: NodeId,
    pub enabled: bool,
    pub data_dir: PathBuf,
    pub endpoints: BTreeMap<NodeId, NodeEndpoint>,
    pub bootstrap: BootstrapConfig,
    pub raft_config: RaftConfiguration,
}

/// Raft-specific configuration parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfiguration {
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub snapshot_threshold: u64,
    pub max_payload_entries: u64,
    pub compaction_threshold: u64,
    pub install_snapshot_timeout: Duration,
    pub send_append_entries_timeout: Duration,
}

impl Default for RaftConfiguration {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(150),
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(600),
            snapshot_threshold: 3000,
            max_payload_entries: 300,
            compaction_threshold: 1000,
            install_snapshot_timeout: Duration::from_secs(30),
            send_append_entries_timeout: Duration::from_secs(5),
        }
    }
}

impl Default for ClusterConfiguration {
    fn default() -> Self {
        Self {
            node_id: 1,
            enabled: false,
            data_dir: PathBuf::from("./raft_data"),
            endpoints: BTreeMap::new(),
            bootstrap: BootstrapConfig::default(),
            raft_config: RaftConfiguration::default(),
        }
    }
}

/// Cluster configuration manager
pub struct ClusterConfigManager {
    config: ClusterConfiguration,
    config_path: PathBuf,
}

impl ClusterConfigManager {
    /// Create a new cluster configuration manager
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            config: ClusterConfiguration::default(),
            config_path,
        }
    }

    /// Load configuration from file
    pub fn load(&mut self) -> RaftResult<()> {
        if !self.config_path.exists() {
            log::info!(
                "Configuration file {:?} does not exist, using defaults",
                self.config_path
            );
            return Ok(());
        }

        let content = fs::read_to_string(&self.config_path).map_err(|e| {
            RaftError::configuration(format!(
                "Failed to read config file {:?}: {}",
                self.config_path, e
            ))
        })?;

        self.config = serde_json::from_str(&content).map_err(|e| {
            RaftError::configuration(format!(
                "Failed to parse config file {:?}: {}",
                self.config_path, e
            ))
        })?;

        self.validate_config()?;
        log::info!("Loaded cluster configuration from {:?}", self.config_path);
        Ok(())
    }

    /// Save configuration to file
    pub fn save(&self) -> RaftResult<()> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = self.config_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                RaftError::configuration(format!(
                    "Failed to create config directory {:?}: {}",
                    parent, e
                ))
            })?;
        }

        let content = serde_json::to_string_pretty(&self.config)
            .map_err(|e| RaftError::configuration(format!("Failed to serialize config: {}", e)))?;

        fs::write(&self.config_path, content).map_err(|e| {
            RaftError::configuration(format!(
                "Failed to write config file {:?}: {}",
                self.config_path, e
            ))
        })?;

        log::info!("Saved cluster configuration to {:?}", self.config_path);
        Ok(())
    }

    /// Validate the configuration
    pub fn validate_config(&self) -> RaftResult<()> {
        // Validate node ID
        if self.config.node_id == 0 {
            return Err(RaftError::configuration("Node ID cannot be zero"));
        }

        // Validate data directory
        if self.config.data_dir.as_os_str().is_empty() {
            return Err(RaftError::configuration("Data directory cannot be empty"));
        }

        // Validate endpoints
        for (node_id, endpoint) in &self.config.endpoints {
            if *node_id != endpoint.node_id {
                return Err(RaftError::configuration(format!(
                    "Node ID mismatch: key {} != endpoint.node_id {}",
                    node_id, endpoint.node_id
                )));
            }

            // Validate socket address
            endpoint.socket_addr()?;
        }

        // Validate Raft configuration
        if self.config.raft_config.election_timeout_min
            >= self.config.raft_config.election_timeout_max
        {
            return Err(RaftError::configuration(
                "Election timeout min must be less than max",
            ));
        }

        if self.config.raft_config.heartbeat_interval
            >= self.config.raft_config.election_timeout_min
        {
            return Err(RaftError::configuration(
                "Heartbeat interval must be less than election timeout min",
            ));
        }

        // Validate bootstrap configuration
        if self.config.bootstrap.bootstrap_expect == 0 {
            return Err(RaftError::configuration("Bootstrap expect cannot be zero"));
        }

        Ok(())
    }

    /// Get the current configuration
    pub fn config(&self) -> &ClusterConfiguration {
        &self.config
    }

    /// Get mutable reference to configuration
    pub fn config_mut(&mut self) -> &mut ClusterConfiguration {
        &mut self.config
    }

    /// Set node ID
    pub fn set_node_id(&mut self, node_id: NodeId) -> RaftResult<()> {
        if node_id == 0 {
            return Err(RaftError::configuration("Node ID cannot be zero"));
        }
        self.config.node_id = node_id;
        Ok(())
    }

    /// Enable or disable cluster mode
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }

    /// Set data directory
    pub fn set_data_dir<P: AsRef<Path>>(&mut self, path: P) {
        self.config.data_dir = path.as_ref().to_path_buf();
    }

    /// Add a node endpoint
    pub fn add_endpoint(&mut self, endpoint: NodeEndpoint) -> RaftResult<()> {
        // Validate the endpoint
        endpoint.socket_addr()?;

        self.config.endpoints.insert(endpoint.node_id, endpoint);
        Ok(())
    }

    /// Remove a node endpoint
    pub fn remove_endpoint(&mut self, node_id: NodeId) -> Option<NodeEndpoint> {
        self.config.endpoints.remove(&node_id)
    }

    /// Get endpoint for a node
    pub fn get_endpoint(&self, node_id: NodeId) -> Option<&NodeEndpoint> {
        self.config.endpoints.get(&node_id)
    }

    /// Get all endpoints
    pub fn get_endpoints(&self) -> &BTreeMap<NodeId, NodeEndpoint> {
        &self.config.endpoints
    }

    /// Get all node IDs
    pub fn get_node_ids(&self) -> BTreeSet<NodeId> {
        self.config.endpoints.keys().copied().collect()
    }

    /// Check if a node is configured
    pub fn has_node(&self, node_id: NodeId) -> bool {
        self.config.endpoints.contains_key(&node_id)
    }

    /// Set bootstrap configuration
    pub fn set_bootstrap_config(&mut self, bootstrap: BootstrapConfig) -> RaftResult<()> {
        if bootstrap.bootstrap_expect == 0 {
            return Err(RaftError::configuration("Bootstrap expect cannot be zero"));
        }
        self.config.bootstrap = bootstrap;
        Ok(())
    }

    /// Add initial member for bootstrap
    pub fn add_initial_member(&mut self, endpoint: NodeEndpoint) -> RaftResult<()> {
        endpoint.socket_addr()?;
        self.config.bootstrap.initial_members.push(endpoint);
        Ok(())
    }

    /// Set Raft configuration
    pub fn set_raft_config(&mut self, raft_config: RaftConfiguration) -> RaftResult<()> {
        if raft_config.election_timeout_min >= raft_config.election_timeout_max {
            return Err(RaftError::configuration(
                "Election timeout min must be less than max",
            ));
        }

        if raft_config.heartbeat_interval >= raft_config.election_timeout_min {
            return Err(RaftError::configuration(
                "Heartbeat interval must be less than election timeout min",
            ));
        }

        self.config.raft_config = raft_config;
        Ok(())
    }

    /// Convert to legacy ClusterConfig format for compatibility
    pub fn to_legacy_config(&self) -> ClusterConfig {
        let cluster_members: BTreeSet<String> = self
            .config
            .endpoints
            .values()
            .map(|endpoint| endpoint.to_string())
            .collect();

        ClusterConfig {
            enabled: self.config.enabled,
            node_id: self.config.node_id,
            cluster_members,
            data_dir: self.config.data_dir.to_string_lossy().to_string(),
            heartbeat_interval_ms: self.config.raft_config.heartbeat_interval.as_millis() as u64,
            election_timeout_min_ms: self.config.raft_config.election_timeout_min.as_millis()
                as u64,
            election_timeout_max_ms: self.config.raft_config.election_timeout_max.as_millis()
                as u64,
            snapshot_threshold: self.config.raft_config.snapshot_threshold,
            max_payload_entries: self.config.raft_config.max_payload_entries,
        }
    }

    /// Create from legacy ClusterConfig format
    pub fn from_legacy_config(legacy: &ClusterConfig) -> RaftResult<Self> {
        let mut config = ClusterConfiguration {
            node_id: legacy.node_id,
            enabled: legacy.enabled,
            data_dir: PathBuf::from(&legacy.data_dir),
            endpoints: BTreeMap::new(),
            bootstrap: BootstrapConfig::default(),
            raft_config: RaftConfiguration {
                heartbeat_interval: Duration::from_millis(legacy.heartbeat_interval_ms),
                election_timeout_min: Duration::from_millis(legacy.election_timeout_min_ms),
                election_timeout_max: Duration::from_millis(legacy.election_timeout_max_ms),
                snapshot_threshold: legacy.snapshot_threshold,
                max_payload_entries: legacy.max_payload_entries,
                ..Default::default()
            },
        };

        // Parse cluster members
        for member_str in &legacy.cluster_members {
            let endpoint = NodeEndpoint::from_str(member_str)?;
            config.endpoints.insert(endpoint.node_id, endpoint);
        }

        let manager = Self {
            config,
            config_path: PathBuf::from("cluster.json"),
        };

        manager.validate_config()?;
        Ok(manager)
    }

    /// Initialize cluster configuration for bootstrap
    pub fn init_bootstrap(
        &mut self,
        node_id: NodeId,
        initial_members: Vec<NodeEndpoint>,
    ) -> RaftResult<()> {
        self.set_node_id(node_id)?;
        self.set_enabled(true);

        // Clear existing endpoints for fresh initialization
        self.config.endpoints.clear();

        // Add all initial members as endpoints
        for endpoint in &initial_members {
            self.add_endpoint(endpoint.clone())?;
        }

        // Set bootstrap configuration
        let bootstrap = BootstrapConfig {
            initial_members,
            bootstrap_expect: self.config.endpoints.len(),
            ..Default::default()
        };
        self.set_bootstrap_config(bootstrap)?;

        Ok(())
    }

    /// Check if this node should bootstrap the cluster
    pub fn should_bootstrap(&self) -> bool {
        self.config.enabled
            && self.config.bootstrap.initial_members.len() >= self.config.bootstrap.bootstrap_expect
            && self
                .config
                .bootstrap
                .initial_members
                .iter()
                .any(|ep| ep.node_id == self.config.node_id)
    }

    /// Get bootstrap peers (excluding self)
    pub fn get_bootstrap_peers(&self) -> Vec<NodeEndpoint> {
        self.config
            .bootstrap
            .initial_members
            .iter()
            .filter(|ep| ep.node_id != self.config.node_id)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_node_endpoint_parsing() {
        let endpoint_str = "1:127.0.0.1:8080";
        let endpoint = NodeEndpoint::from_str(endpoint_str).unwrap();

        assert_eq!(endpoint.node_id, 1);
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 8080);
        assert_eq!(endpoint.to_string(), endpoint_str);
    }

    #[test]
    fn test_node_endpoint_invalid_format() {
        let result = NodeEndpoint::from_str("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_cluster_config_validation() {
        let mut manager = ClusterConfigManager::new(PathBuf::from("test.json"));

        // Valid configuration should pass
        manager.config.node_id = 1;
        manager.config.enabled = true;
        assert!(manager.validate_config().is_ok());

        // Invalid node ID should fail
        manager.config.node_id = 0;
        assert!(manager.validate_config().is_err());
    }

    #[test]
    fn test_config_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("cluster.json");

        let mut manager = ClusterConfigManager::new(config_path.clone());
        manager.config.node_id = 42;
        manager.config.enabled = true;

        let endpoint = NodeEndpoint::new(1, "127.0.0.1".to_string(), 8080);
        manager.add_endpoint(endpoint.clone()).unwrap();

        // Save configuration
        manager.save().unwrap();

        // Load configuration
        let mut manager2 = ClusterConfigManager::new(config_path);
        manager2.load().unwrap();

        assert_eq!(manager2.config.node_id, 42);
        assert_eq!(manager2.config.enabled, true);
        assert_eq!(manager2.get_endpoint(1), Some(&endpoint));
    }

    #[test]
    fn test_legacy_config_conversion() {
        let legacy = ClusterConfig {
            enabled: true,
            node_id: 1,
            cluster_members: vec![
                "1:127.0.0.1:8080".to_string(),
                "2:127.0.0.1:8081".to_string(),
            ]
            .into_iter()
            .collect(),
            data_dir: "/tmp/raft".to_string(),
            heartbeat_interval_ms: 1000,
            election_timeout_min_ms: 3000,
            election_timeout_max_ms: 6000,
            snapshot_threshold: 1000,
            max_payload_entries: 100,
        };

        let manager = ClusterConfigManager::from_legacy_config(&legacy).unwrap();
        assert_eq!(manager.config.node_id, 1);
        assert_eq!(manager.config.enabled, true);
        assert_eq!(manager.config.endpoints.len(), 2);

        let converted_back = manager.to_legacy_config();
        assert_eq!(converted_back.node_id, legacy.node_id);
        assert_eq!(converted_back.enabled, legacy.enabled);
    }

    #[test]
    fn test_bootstrap_configuration() {
        let mut manager = ClusterConfigManager::new(PathBuf::from("test.json"));

        let endpoints = vec![
            NodeEndpoint::new(1, "127.0.0.1".to_string(), 8080),
            NodeEndpoint::new(2, "127.0.0.1".to_string(), 8081),
            NodeEndpoint::new(3, "127.0.0.1".to_string(), 8082),
        ];

        manager.init_bootstrap(1, endpoints.clone()).unwrap();

        assert_eq!(manager.config.node_id, 1);
        assert!(manager.config.enabled);
        assert_eq!(manager.config.endpoints.len(), 3);
        assert_eq!(manager.config.bootstrap.initial_members.len(), 3);
        assert!(manager.should_bootstrap());

        let peers = manager.get_bootstrap_peers();
        assert_eq!(peers.len(), 2);
        assert!(!peers.iter().any(|ep| ep.node_id == 1));
    }
}
