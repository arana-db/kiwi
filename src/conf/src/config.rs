// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use snafu::ResultExt;
use std::collections::BTreeSet;
use validator::Validate;

use crate::de_func::{parse_bool_from_string, parse_memory, parse_redis_config};
use crate::error::Error;

/// Information about cluster startup configuration
#[derive(Debug, Clone)]
pub struct ClusterStartupInfo {
    pub enabled: bool,
    pub node_id: u64,
    pub self_endpoint: Option<String>,
    pub peer_endpoints: Vec<String>,
    pub data_dir: String,
    pub is_valid_cluster: bool,
}

const DEFAULT_BINDING: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7379; // Redis-compatible port (7xxx variant of 6379)

// Cluster configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: u64,
    pub cluster_members: BTreeSet<String>, // Format: "node_id:host:port"
    pub data_dir: String,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub snapshot_threshold: u64,
    pub max_payload_entries: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 1,
            cluster_members: BTreeSet::new(),
            data_dir: "./raft_data".to_string(),
            heartbeat_interval_ms: 1000,
            election_timeout_min_ms: 3000,
            election_timeout_max_ms: 6000,
            snapshot_threshold: 1000,
            max_payload_entries: 100,
        }
    }
}

impl ClusterConfig {
    /// Validate cluster configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled {
            // Validate node ID
            if self.node_id == 0 {
                return Err("cluster-node-id must be >= 1 (0 is reserved)".to_string());
            }

            // Validate timing constraints for Raft
            if self.election_timeout_min_ms == 0 {
                return Err("cluster-election-timeout-min must be > 0".to_string());
            }
            if self.election_timeout_min_ms >= self.election_timeout_max_ms {
                return Err(
                    "cluster-election-timeout-min must be < cluster-election-timeout-max"
                        .to_string(),
                );
            }
            if self.heartbeat_interval_ms >= self.election_timeout_min_ms {
                return Err("cluster-heartbeat-interval must be < cluster-election-timeout-min for proper Raft operation".to_string());
            }

            // Validate cluster members format
            for member in &self.cluster_members {
                if let Err(e) = self.validate_member_format(member) {
                    return Err(format!("Invalid cluster member '{}': {}", member, e));
                }
            }

            // Validate data directory
            if self.data_dir.is_empty() {
                return Err("cluster-data-dir cannot be empty".to_string());
            }

            // Validate thresholds
            if self.snapshot_threshold == 0 {
                return Err("cluster-snapshot-threshold must be > 0".to_string());
            }
            if self.max_payload_entries == 0 {
                return Err("cluster-max-payload-entries must be > 0".to_string());
            }
        }
        Ok(())
    }

    /// Validate cluster member format: "node_id:host:port"
    fn validate_member_format(&self, member: &str) -> Result<(), String> {
        let parts: Vec<&str> = member.split(':').collect();
        if parts.len() != 3 {
            return Err("format must be 'node_id:host:port'".to_string());
        }

        // Validate node ID
        let node_id: u64 = parts[0]
            .parse()
            .map_err(|_| "node_id must be a valid number")?;
        if node_id == 0 {
            return Err("node_id must be >= 1".to_string());
        }

        // Validate host (basic check)
        let host = parts[1];
        if host.is_empty() {
            return Err("host cannot be empty".to_string());
        }

        // Validate port
        let port: u16 = parts[2]
            .parse()
            .map_err(|_| "port must be a valid number between 1-65535")?;
        if port == 0 {
            return Err("port must be > 0".to_string());
        }

        Ok(())
    }

    /// Check if cluster mode is properly configured
    pub fn is_cluster_mode(&self) -> bool {
        self.enabled && !self.cluster_members.is_empty()
    }

    /// Get this node's endpoint from cluster members
    pub fn get_self_endpoint(&self) -> Option<String> {
        self.cluster_members
            .iter()
            .find(|member| {
                if let Ok(parts) = self.parse_member(member) {
                    parts.0 == self.node_id
                } else {
                    false
                }
            })
            .cloned()
    }

    /// Parse cluster member string into (node_id, host, port)
    pub fn parse_member(&self, member: &str) -> Result<(u64, String, u16), String> {
        let parts: Vec<&str> = member.split(':').collect();
        if parts.len() != 3 {
            return Err("Invalid format, expected 'node_id:host:port'".to_string());
        }

        let node_id = parts[0].parse::<u64>().map_err(|_| "Invalid node_id")?;
        let host = parts[1].to_string();
        let port = parts[2].parse::<u16>().map_err(|_| "Invalid port")?;

        Ok((node_id, host, port))
    }

    /// Get all peer endpoints (excluding self)
    pub fn get_peer_endpoints(&self) -> Vec<String> {
        self.cluster_members
            .iter()
            .filter(|member| {
                if let Ok(parts) = self.parse_member(member) {
                    parts.0 != self.node_id
                } else {
                    false
                }
            })
            .cloned()
            .collect()
    }

    /// Check if this node is part of the cluster
    pub fn is_node_in_cluster(&self) -> bool {
        self.get_self_endpoint().is_some()
    }
}

// config struct define - keeping original config items but using Redis-style format
#[derive(Debug, Validate)]
pub struct Config {
    // Original config items from config.ini
    #[validate(range(min = 1024, max = 65535))]
    pub port: u16,
    pub memory: u64,
    pub small_compaction_threshold: usize,
    pub small_compaction_duration_threshold: usize,
    pub rocksdb_max_subcompactions: u32,
    pub rocksdb_max_background_jobs: i32,
    pub rocksdb_max_write_buffer_number: i32,
    pub rocksdb_min_write_buffer_number_to_merge: i32,
    pub rocksdb_write_buffer_size: usize,
    pub rocksdb_level0_file_num_compaction_trigger: i32,
    pub rocksdb_num_levels: i32,
    pub rocksdb_enable_pipelined_write: bool,
    pub rocksdb_level0_slowdown_writes_trigger: i32,
    pub rocksdb_level0_stop_writes_trigger: i32,
    pub rocksdb_ttl_second: u64,
    pub rocksdb_periodic_second: u64,
    pub rocksdb_level_compaction_dynamic_level_bytes: bool,
    pub rocksdb_max_open_files: i32,
    pub rocksdb_target_file_size_base: u64,

    // Additional fields from original config
    pub binding: String,
    pub timeout: u32,
    pub log_dir: String,
    pub redis_compatible_mode: bool,
    pub db_instance_num: usize,

    // Cluster configuration
    pub cluster: ClusterConfig,
}

// set default value for config
impl Default for Config {
    fn default() -> Self {
        Self {
            binding: DEFAULT_BINDING.to_string(),
            port: DEFAULT_PORT,
            timeout: 50,
            memory: 1024 * 1024 * 1024, // 1GB
            log_dir: "/data/kiwi_rs/logs".to_string(),
            redis_compatible_mode: false,

            rocksdb_max_subcompactions: 0,
            rocksdb_max_background_jobs: 4,
            rocksdb_max_write_buffer_number: 2,
            rocksdb_min_write_buffer_number_to_merge: 2,
            rocksdb_write_buffer_size: 64 << 20, // 64MB
            rocksdb_level0_file_num_compaction_trigger: 4,
            rocksdb_num_levels: 7,
            rocksdb_enable_pipelined_write: false,
            rocksdb_level0_slowdown_writes_trigger: 20,
            rocksdb_level0_stop_writes_trigger: 36,
            rocksdb_ttl_second: 30 * 24 * 60 * 60, // 30 days
            rocksdb_periodic_second: 30 * 24 * 60 * 60, // 30 days
            rocksdb_level_compaction_dynamic_level_bytes: true,
            rocksdb_max_open_files: 10000,
            rocksdb_target_file_size_base: 64 << 20, // 64MB

            db_instance_num: 3,
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,

            cluster: ClusterConfig::default(),
        }
    }
}

impl Config {
    /// Determine if the server should run in cluster mode
    pub fn should_run_cluster_mode(&self, force_single_node: bool) -> bool {
        !force_single_node && self.cluster.is_cluster_mode()
    }

    /// Validate cluster configuration for startup
    pub fn validate_cluster_startup(&self, init_cluster: bool) -> Result<(), String> {
        if !self.cluster.enabled {
            return Ok(());
        }

        // Check if this node is configured in the cluster
        if !self.cluster.is_node_in_cluster() {
            return Err(format!(
                "Node ID {} is not found in cluster members. Current members: {:?}",
                self.cluster.node_id, self.cluster.cluster_members
            ));
        }

        // For non-bootstrap scenarios, require cluster members
        if !init_cluster && self.cluster.cluster_members.is_empty() {
            return Err(
                "Cluster mode enabled but no cluster members specified. Use --init-cluster for the first node.".to_string()
            );
        }

        // Validate that we have at least one other member for a proper cluster
        if !init_cluster && self.cluster.cluster_members.len() < 2 {
            return Err("Cluster requires at least 2 members. Current members: {}".to_string());
        }

        Ok(())
    }

    /// Get cluster startup information
    pub fn get_cluster_info(&self) -> ClusterStartupInfo {
        ClusterStartupInfo {
            enabled: self.cluster.enabled,
            node_id: self.cluster.node_id,
            self_endpoint: self.cluster.get_self_endpoint(),
            peer_endpoints: self.cluster.get_peer_endpoints(),
            data_dir: self.cluster.data_dir.clone(),
            is_valid_cluster: self.cluster.is_cluster_mode() && self.cluster.is_node_in_cluster(),
        }
    }

    // load config from file - supports Redis-style format with comments
    pub fn load(path: &str) -> Result<Self, Error> {
        let content =
            std::fs::read_to_string(path).context(crate::error::ConfigFileSnafu { path })?;

        // Parse Redis-style configuration
        let config_map = parse_redis_config(&content).map_err(|e| Error::InvalidConfig {
            source: serde_ini::de::Error::Custom(e),
        })?;

        // Create config from parsed values
        let mut config = Config::default();
        // Parse each configuration value
        for (key, value) in config_map {
            match key.as_str() {
                "port" => {
                    config.port = value.parse().map_err(|e| Error::InvalidConfig {
                        source: serde_ini::de::Error::Custom(format!("Invalid port: {}", e)),
                    })?;
                }
                "binding" => {
                    config.binding = value;
                }
                "timeout" => {
                    config.timeout = value.parse().map_err(|e| Error::InvalidConfig {
                        source: serde_ini::de::Error::Custom(format!("Invalid timeout: {}", e)),
                    })?;
                }
                "log-dir" => {
                    config.log_dir = value;
                }
                "redis-compatible-mode" => {
                    config.redis_compatible_mode =
                        parse_bool_from_string(&value).map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid redis-compatible-mode: {}",
                                e
                            )),
                        })?;
                }
                "db-instance-num" => {
                    config.db_instance_num = value.parse().map_err(|e| Error::InvalidConfig {
                        source: serde_ini::de::Error::Custom(format!(
                            "Invalid db-instance-num: {}",
                            e
                        )),
                    })?;
                }
                "memory" => {
                    config.memory =
                        parse_memory(&value).map_err(|e| Error::MemoryParse { source: e })?;
                }
                "small-compaction-threshold" => {
                    config.small_compaction_threshold =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid small-compaction-threshold: {}",
                                e
                            )),
                        })?;
                }
                "small-compaction-duration-threshold" => {
                    config.small_compaction_duration_threshold =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid small-compaction-duration-threshold: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-max-subcompactions" => {
                    config.rocksdb_max_subcompactions =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-max-subcompactions: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-max-background-jobs" => {
                    config.rocksdb_max_background_jobs =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-max-background-jobs: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-max-write-buffer-number" => {
                    config.rocksdb_max_write_buffer_number =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-max-write-buffer-number: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-min-write-buffer-number-to-merge" => {
                    config.rocksdb_min_write_buffer_number_to_merge =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-min-write-buffer-number-to-merge: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-write-buffer-size" => {
                    config.rocksdb_write_buffer_size =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-write-buffer-size: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-level0-file-num-compaction-trigger" => {
                    config.rocksdb_level0_file_num_compaction_trigger =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-level0-file-num-compaction-trigger: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-num-levels" => {
                    config.rocksdb_num_levels =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-num-levels: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-enable-pipelined-write" => {
                    config.rocksdb_enable_pipelined_write = parse_bool_from_string(&value)
                        .map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-enable-pipelined-write: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-level0-slowdown-writes-trigger" => {
                    config.rocksdb_level0_slowdown_writes_trigger =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-level0-slowdown-writes-trigger: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-level0-stop-writes-trigger" => {
                    config.rocksdb_level0_stop_writes_trigger =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-level0-stop-writes-trigger: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-ttl-second" => {
                    config.rocksdb_ttl_second =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-ttl-second: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-periodic-second" => {
                    config.rocksdb_periodic_second =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-periodic-second: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-level-compaction-dynamic-level-bytes" => {
                    config.rocksdb_level_compaction_dynamic_level_bytes =
                        parse_bool_from_string(&value).map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-level-compaction-dynamic-level-bytes: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-max-open-files" => {
                    config.rocksdb_max_open_files =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-max-open-files: {}",
                                e
                            )),
                        })?;
                }
                "rocksdb-target-file-size-base" => {
                    config.rocksdb_target_file_size_base =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-target-file-size-base: {}",
                                e
                            )),
                        })?;
                }
                // Cluster configuration
                "cluster-enabled" => {
                    config.cluster.enabled =
                        parse_bool_from_string(&value).map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid cluster-enabled: {}",
                                e
                            )),
                        })?;
                }
                "cluster-node-id" => {
                    config.cluster.node_id = value.parse().map_err(|e| Error::InvalidConfig {
                        source: serde_ini::de::Error::Custom(format!(
                            "Invalid cluster-node-id: {}",
                            e
                        )),
                    })?;
                }
                "cluster-members" => {
                    // Parse comma-separated list of cluster members
                    config.cluster.cluster_members = value
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                }
                "cluster-data-dir" => {
                    config.cluster.data_dir = value;
                }
                "cluster-heartbeat-interval" => {
                    config.cluster.heartbeat_interval_ms =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid cluster-heartbeat-interval: {}",
                                e
                            )),
                        })?;
                }
                "cluster-election-timeout-min" => {
                    config.cluster.election_timeout_min_ms =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid cluster-election-timeout-min: {}",
                                e
                            )),
                        })?;
                }
                "cluster-election-timeout-max" => {
                    config.cluster.election_timeout_max_ms =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid cluster-election-timeout-max: {}",
                                e
                            )),
                        })?;
                }
                "cluster-snapshot-threshold" => {
                    config.cluster.snapshot_threshold =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid cluster-snapshot-threshold: {}",
                                e
                            )),
                        })?;
                }
                "cluster-max-payload-entries" => {
                    config.cluster.max_payload_entries =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid cluster-max-payload-entries: {}",
                                e
                            )),
                        })?;
                }
                _ => {
                    // Unknown configuration key, skip it
                    continue;
                }
            }
        }

        // Validate cluster configuration using the new validation method
        if let Err(e) = config.cluster.validate() {
            return Err(Error::InvalidConfig {
                source: serde_ini::de::Error::Custom(e),
            });
        }

        config
            .validate()
            .map_err(|_e| Error::ValidConfigFail { source: _e })?;

        Ok(config)
    }

    // TODO: Due to API issues, the rocksdb_ttl_second parameter is temporarily missing
    pub fn get_rocksdb_options(&self) -> rocksdb::Options {
        let mut options = rocksdb::Options::default();

        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_subcompactions(self.rocksdb_max_subcompactions);
        options.set_max_background_jobs(self.rocksdb_max_background_jobs);
        options.set_max_write_buffer_number(self.rocksdb_max_write_buffer_number);
        options.set_min_write_buffer_number_to_merge(self.rocksdb_min_write_buffer_number_to_merge);
        options.set_write_buffer_size(self.rocksdb_write_buffer_size);
        options.set_level_zero_file_num_compaction_trigger(
            self.rocksdb_level0_file_num_compaction_trigger,
        );
        options.set_num_levels(self.rocksdb_num_levels);
        options.set_enable_pipelined_write(self.rocksdb_enable_pipelined_write);
        options.set_level_zero_slowdown_writes_trigger(self.rocksdb_level0_slowdown_writes_trigger);
        options.set_level_zero_stop_writes_trigger(self.rocksdb_level0_stop_writes_trigger);
        options.set_level_compaction_dynamic_level_bytes(
            self.rocksdb_level_compaction_dynamic_level_bytes,
        );
        options.set_max_open_files(self.rocksdb_max_open_files);
        options.set_target_file_size_base(self.rocksdb_target_file_size_base);

        if self.rocksdb_periodic_second > 0 {
            options.set_periodic_compaction_seconds(self.rocksdb_periodic_second);
        }

        options
    }

    pub fn get_rocksdb_block_based_table_options(&self) -> rocksdb::BlockBasedOptions {
        rocksdb::BlockBasedOptions::default()
    }
}
