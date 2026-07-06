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
use rocksdb::DBCompressionType;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::fmt::Formatter;
use validator::Validate;

use crate::de_func::{parse_bool_from_string, parse_memory, parse_redis_config};
use crate::error::Error;
use crate::runtime_config::RuntimeConfig;

/// Compression algorithm for RocksDB column families.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Snappy,
    Lz4,
    Zstd,
    Zlib,
    Bz2,
}

impl CompressionType {
    pub fn to_rocksdb(&self) -> rocksdb::DBCompressionType {
        match self {
            CompressionType::None => rocksdb::DBCompressionType::None,
            CompressionType::Snappy => rocksdb::DBCompressionType::Snappy,
            CompressionType::Lz4 => rocksdb::DBCompressionType::Lz4,
            CompressionType::Zstd => rocksdb::DBCompressionType::Zstd,
            CompressionType::Zlib => rocksdb::DBCompressionType::Zlib,
            CompressionType::Bz2 => rocksdb::DBCompressionType::Bz2,
        }
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            CompressionType::None => "none",
            CompressionType::Snappy => "snappy",
            CompressionType::Lz4 => "lz4",
            CompressionType::Zstd => "zstd",
            CompressionType::Zlib => "zlib",
            CompressionType::Bz2 => "bz2",
        };
        write!(f, "{}", s)
    }
}

impl std::str::FromStr for CompressionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" | "no" => Ok(CompressionType::None),
            "snappy" => Ok(CompressionType::Snappy),
            "lz4" => Ok(CompressionType::Lz4),
            "zstd" => Ok(CompressionType::Zstd),
            "zlib" => Ok(CompressionType::Zlib),
            "bz2" => Ok(CompressionType::Bz2),
            _ => Err(format!(
                "unknown compression type '{}', valid values: none, snappy, lz4, zstd, zlib, bz2",
                s
            )),
        }
    }
}

const DEFAULT_BINDING: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7379; // Redis-compatible port (7xxx variant of 6379)
// config struct define - keeping original config items but using Redis-style format
#[derive(Validate, Serialize, Deserialize)]
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
    /// Compression algorithm applied to all column families.
    /// Supported values: none, lz4, snappy, zstd, zlib, bz2. Default: lz4.
    pub rocksdb_compression_type: CompressionType,

    // Additional fields from original config
    pub binding: String,
    pub timeout: u32,
    pub log_dir: String,
    pub data_dir: String,
    pub redis_compatible_mode: bool,
    pub db_instance_num: usize,
    /// Authentication password. When set, clients must authenticate via AUTH command.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requirepass: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raft: Option<RaftClusterConfig>,

    /// Dual-runtime configuration.
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("port", &self.port)
            .field("memory", &self.memory)
            .field(
                "small_compaction_threshold",
                &self.small_compaction_threshold,
            )
            .field(
                "small_compaction_duration_threshold",
                &self.small_compaction_duration_threshold,
            )
            .field(
                "rocksdb_max_subcompactions",
                &self.rocksdb_max_subcompactions,
            )
            .field(
                "rocksdb_max_background_jobs",
                &self.rocksdb_max_background_jobs,
            )
            .field(
                "rocksdb_max_write_buffer_number",
                &self.rocksdb_max_write_buffer_number,
            )
            .field(
                "rocksdb_min_write_buffer_number_to_merge",
                &self.rocksdb_min_write_buffer_number_to_merge,
            )
            .field("rocksdb_write_buffer_size", &self.rocksdb_write_buffer_size)
            .field(
                "rocksdb_level0_file_num_compaction_trigger",
                &self.rocksdb_level0_file_num_compaction_trigger,
            )
            .field("rocksdb_num_levels", &self.rocksdb_num_levels)
            .field(
                "rocksdb_enable_pipelined_write",
                &self.rocksdb_enable_pipelined_write,
            )
            .field(
                "rocksdb_level0_slowdown_writes_trigger",
                &self.rocksdb_level0_slowdown_writes_trigger,
            )
            .field(
                "rocksdb_level0_stop_writes_trigger",
                &self.rocksdb_level0_stop_writes_trigger,
            )
            .field("rocksdb_ttl_second", &self.rocksdb_ttl_second)
            .field("rocksdb_periodic_second", &self.rocksdb_periodic_second)
            .field(
                "rocksdb_level_compaction_dynamic_level_bytes",
                &self.rocksdb_level_compaction_dynamic_level_bytes,
            )
            .field("rocksdb_max_open_files", &self.rocksdb_max_open_files)
            .field(
                "rocksdb_target_file_size_base",
                &self.rocksdb_target_file_size_base,
            )
            .field("rocksdb_compression_type", &self.rocksdb_compression_type)
            .field("binding", &self.binding)
            .field("timeout", &self.timeout)
            .field("log_dir", &self.log_dir)
            .field("data_dir", &self.data_dir)
            .field("redis_compatible_mode", &self.redis_compatible_mode)
            .field("db_instance_num", &self.db_instance_num)
            .field(
                "requirepass",
                if self.requirepass.is_some() {
                    &"<REDACTED>"
                } else {
                    &"<NONE>"
                },
            )
            .field("raft", &self.raft)
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Debug, Validate, Clone, Serialize, Deserialize)]
pub struct RaftClusterConfig {
    #[validate(range(min = 1))]
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub data_dir: String,
    pub heartbeat_interval_ms: Option<u64>,
    pub election_timeout_min_ms: Option<u64>,
    pub election_timeout_max_ms: Option<u64>,
    /// 是否使用内存日志存储，默认 false（使用 RocksDB 持久化存储）
    pub use_memory_log_store: bool,
}

impl Default for RaftClusterConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            raft_addr: "127.0.0.1:8081".to_string(),
            resp_addr: "127.0.0.1:7379".to_string(),
            data_dir: "./kiwi_data/raft".to_string(),
            heartbeat_interval_ms: Some(200),
            election_timeout_min_ms: Some(500),
            election_timeout_max_ms: Some(1500),
            use_memory_log_store: false,
        }
    }
}

// set default value for config
impl Default for Config {
    fn default() -> Self {
        Self {
            binding: DEFAULT_BINDING.to_string(),
            port: DEFAULT_PORT,
            timeout: 50,
            memory: 1024 * 1024 * 1024, // 1GB
            log_dir: "./kiwi_data/logs".to_string(),
            data_dir: "./kiwi_data/db".to_string(),
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
            rocksdb_compression_type: CompressionType::Lz4,

            db_instance_num: 3,
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,
            requirepass: None,
            raft: None,
            runtime: RuntimeConfig::default(),
        }
    }
}

fn invalid_config(message: String) -> Error {
    Error::InvalidConfig {
        source: serde_ini::de::Error::Custom(message),
    }
}

fn parse_usize_value(key: &str, value: &str) -> Result<usize, Error> {
    value
        .parse()
        .map_err(|e| invalid_config(format!("Invalid {}: {}", key, e)))
}

fn parse_bool_value(key: &str, value: &str) -> Result<bool, Error> {
    parse_bool_from_string(value).map_err(|e| invalid_config(format!("Invalid {}: {}", key, e)))
}

fn validate_loaded_config(config: &Config) -> Result<(), Error> {
    config
        .validate()
        .map_err(|e| Error::ValidConfigFail { source: e })?;
    config.runtime.validate().map_err(invalid_config)?;
    if let Some(raft) = config.raft.as_ref() {
        raft.validate()
            .map_err(|e| Error::ValidConfigFail { source: e })?;
    }
    Ok(())
}

impl Config {
    // load config from file - supports Redis-style key-value format
    pub fn load(path: &str) -> Result<Self, Error> {
        let content =
            std::fs::read_to_string(path).context(crate::error::ConfigFileSnafu { path })?;

        let config_map = parse_redis_config(&content).map_err(|e| Error::InvalidConfig {
            source: serde_ini::de::Error::Custom(e),
        })?;

        // Create config from parsed values
        let mut config = Config::default();

        let mut raft_config = RaftClusterConfig::default();
        let mut raft_node_id: Option<u64> = None;
        let mut raft_fields_seen = false;

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
                "data-dir" => {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        return Err(Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(
                                "data-dir must not be empty".to_string(),
                            ),
                        });
                    }
                    config.data_dir = trimmed.to_string();
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
                "rocksdb-compression-type" => {
                    config.rocksdb_compression_type =
                        value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid rocksdb-compression-type: {}",
                                e
                            )),
                        })?;
                }
                "raft-node-id" | "cluster-node-id" => {
                    raft_fields_seen = true;
                    raft_node_id = Some(value.parse().map_err(|e| Error::InvalidConfig {
                        source: serde_ini::de::Error::Custom(format!(
                            "Invalid raft-node-id: {}",
                            e
                        )),
                    })?);
                }
                "raft-addr" | "cluster-addr" => {
                    raft_fields_seen = true;
                    raft_config.raft_addr = value;
                }
                "raft-resp-addr" | "cluster-resp-addr" => {
                    raft_fields_seen = true;
                    raft_config.resp_addr = value;
                }
                "raft-data-dir" | "cluster-data-dir" => {
                    raft_fields_seen = true;
                    raft_config.data_dir = value;
                }
                "raft-heartbeat-interval"
                | "raft-heartbeat-interval-ms"
                | "cluster-heartbeat-interval"
                | "cluster-heartbeat-interval-ms" => {
                    raft_fields_seen = true;
                    raft_config.heartbeat_interval_ms =
                        Some(value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid raft-heartbeat-interval: {}",
                                e
                            )),
                        })?);
                }
                "raft-election-timeout-min"
                | "raft-election-timeout-min-ms"
                | "cluster-election-timeout-min"
                | "cluster-election-timeout-min-ms" => {
                    raft_fields_seen = true;
                    raft_config.election_timeout_min_ms =
                        Some(value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid raft-election-timeout-min: {}",
                                e
                            )),
                        })?);
                }
                "raft-election-timeout-max"
                | "raft-election-timeout-max-ms"
                | "cluster-election-timeout-max"
                | "cluster-election-timeout-max-ms" => {
                    raft_fields_seen = true;
                    raft_config.election_timeout_max_ms =
                        Some(value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid raft-election-timeout-max: {}",
                                e
                            )),
                        })?);
                }
                "raft-use-memory-log-store" => {
                    raft_fields_seen = true;
                    raft_config.use_memory_log_store =
                        parse_bool_from_string(&value).map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid raft-use-memory-log-store: {}",
                                e
                            )),
                        })?;
                }
                "requirepass" => {
                    config.requirepass = Some(value);
                }
                "runtime-network-threads" | "runtime-network_threads" => {
                    config.runtime.network_threads = parse_usize_value(&key, &value)?;
                }
                "runtime-storage-threads" | "runtime-storage_threads" => {
                    config.runtime.storage_threads = parse_usize_value(&key, &value)?;
                }
                "runtime-channel-buffer-size" | "runtime-channel_buffer_size" => {
                    config.runtime.channel_buffer_size = parse_usize_value(&key, &value)?;
                }
                "runtime-batch-size" | "runtime-batch_size" => {
                    config.runtime.batch_size = parse_usize_value(&key, &value)?;
                }
                "runtime-scaling-enabled" => {
                    config.runtime.scaling.enabled = parse_bool_value(&key, &value)?;
                }
                "runtime-scaling-min-network-threads" | "runtime-scaling-min_network_threads" => {
                    config.runtime.scaling.min_network_threads = parse_usize_value(&key, &value)?;
                }
                "runtime-scaling-max-network-threads" | "runtime-scaling-max_network_threads" => {
                    config.runtime.scaling.max_network_threads = parse_usize_value(&key, &value)?;
                }
                "runtime-scaling-min-storage-threads" | "runtime-scaling-min_storage_threads" => {
                    config.runtime.scaling.min_storage_threads = parse_usize_value(&key, &value)?;
                }
                "runtime-scaling-max-storage-threads" | "runtime-scaling-max_storage_threads" => {
                    config.runtime.scaling.max_storage_threads = parse_usize_value(&key, &value)?;
                }
                "runtime-scaling-scale-up-threshold" | "runtime-scaling-scale_up_threshold" => {
                    config.runtime.scaling.scale_up_threshold = parse_usize_value(&key, &value)?;
                }
                "runtime-scaling-scale-down-threshold" | "runtime-scaling-scale_down_threshold" => {
                    config.runtime.scaling.scale_down_threshold = parse_usize_value(&key, &value)?;
                }
                "runtime-scaling-scale-increment" | "runtime-scaling-scale_increment" => {
                    config.runtime.scaling.scale_increment = parse_usize_value(&key, &value)?;
                }
                "runtime-priority-enabled" => {
                    config.runtime.priority.enabled = parse_bool_value(&key, &value)?;
                }
                "runtime-priority-high-priority-weight"
                | "runtime-priority-high_priority_weight" => {
                    config.runtime.priority.high_priority_weight = parse_usize_value(&key, &value)?;
                }
                "runtime-priority-normal-priority-weight"
                | "runtime-priority-normal_priority_weight" => {
                    config.runtime.priority.normal_priority_weight =
                        parse_usize_value(&key, &value)?;
                }
                "runtime-priority-low-priority-weight" | "runtime-priority-low_priority_weight" => {
                    config.runtime.priority.low_priority_weight = parse_usize_value(&key, &value)?;
                }
                "runtime-priority-max-queue-size-per-priority"
                | "runtime-priority-max_queue_size_per_priority" => {
                    config.runtime.priority.max_queue_size_per_priority =
                        parse_usize_value(&key, &value)?;
                }
                "runtime-raft-metrics-enabled" | "runtime-raft_metrics-enabled" => {
                    config.runtime.raft_metrics.enabled = parse_bool_value(&key, &value)?;
                }
                "runtime-raft-metrics-track-replication-latency"
                | "runtime-raft_metrics-track_replication_latency" => {
                    config.runtime.raft_metrics.track_replication_latency =
                        parse_bool_value(&key, &value)?;
                }
                "runtime-raft-metrics-track-election-events"
                | "runtime-raft_metrics-track_election_events" => {
                    config.runtime.raft_metrics.track_election_events =
                        parse_bool_value(&key, &value)?;
                }
                "runtime-fault-injection-enabled" | "runtime-fault_injection-enabled" => {
                    config.runtime.fault_injection.enabled = parse_bool_value(&key, &value)?;
                }
                "runtime-fault-injection-log-events" | "runtime-fault_injection-log_events" => {
                    config.runtime.fault_injection.log_events = parse_bool_value(&key, &value)?;
                }
                _ => {
                    log::warn!("unknown config key: {}", key);
                    continue;
                }
            }
        }

        if raft_fields_seen && raft_node_id.is_none() {
            return Err(Error::InvalidConfig {
                source: serde_ini::de::Error::Custom(
                    "raft-node-id is required when any raft-* option is set".to_string(),
                ),
            });
        }

        if let Some(node_id) = raft_node_id {
            raft_config.node_id = node_id;
            config.raft = Some(raft_config);
        }

        validate_loaded_config(&config)?;

        Ok(config)
    }

    /// Generate a sample configuration file from the default Config (Redis-style).
    pub fn sample_config() -> String {
        format!(
            "# Kiwi Configuration File\n\
             # Generated by `kiwi --sample-config`\n\n{}",
            Config::default().to_redis_style()
        )
    }

    /// Generate a complete sample configuration with all available keys, including
    /// Raft cluster settings and authentication.
    pub fn full_sample_config() -> String {
        let c = Config {
            raft: Some(RaftClusterConfig::default()),
            requirepass: Some("your-password".to_string()),
            ..Default::default()
        };

        format!(
            "# Kiwi Configuration File (all keys)\n\
             # Generated by `kiwi --full-sample-config`\n\n{}",
            c.to_redis_style()
        )
    }
}

impl Config {
    /// Serialize to Redis-style `key value` format via `toml::Value` flattening.
    /// Nested tables are flattened with `-` separators (e.g. `raft.node_id` → `raft-node-id`).
    /// Field name underscores become hyphens.
    fn to_redis_style(&self) -> String {
        let value = toml::Value::try_from(self).expect("Config should serialize to toml::Value");
        let mut lines: Vec<(String, String)> = Vec::new();
        flatten_table(
            value.as_table().expect("top-level should be a table"),
            "",
            &mut lines,
        );
        lines.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out = String::new();
        for (key, val) in lines {
            out.push_str(&format!("{} {}\n", key, val));
        }
        out
    }
}

fn flatten_table(table: &toml::Table, prefix: &str, lines: &mut Vec<(String, String)>) {
    for (key, value) in table {
        let flat_key = key.replace('_', "-");
        let full_key = if prefix.is_empty() {
            flat_key
        } else if flat_key.starts_with(&format!("{}-", prefix)) {
            // e.g. raft.raft_addr → raft-addr, not raft-raft-addr
            flat_key
        } else {
            format!("{}-{}", prefix, flat_key)
        };
        match value {
            toml::Value::Table(inner) => flatten_table(inner, &full_key, lines),
            toml::Value::String(s) => lines.push((full_key, s.clone())),
            toml::Value::Integer(i) => lines.push((full_key, i.to_string())),
            toml::Value::Float(f) => lines.push((full_key, f.to_string())),
            toml::Value::Boolean(b) => {
                lines.push((full_key, if *b { "yes".into() } else { "no".into() }))
            }
            _ => {} // skip arrays, datetimes — not used in Config
        }
    }
}

impl Config {
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

        // Apply compression to all levels (level 0 and 1 use no compression, levels 2+ use configured type)
        let compression = self.rocksdb_compression_type.to_rocksdb();
        let mut compressions = vec![DBCompressionType::None; 3];
        for _ in 3..self.rocksdb_num_levels {
            compressions.push(compression);
        }
        options.set_compression_per_level(&compressions);

        if self.rocksdb_periodic_second > 0 {
            options.set_periodic_compaction_seconds(self.rocksdb_periodic_second);
        }

        options
    }

    pub fn get_rocksdb_block_based_table_options(&self) -> rocksdb::BlockBasedOptions {
        rocksdb::BlockBasedOptions::default()
    }
}
