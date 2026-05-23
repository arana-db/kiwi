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
#[derive(Validate)]
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
    pub db_dir: String,
    pub redis_compatible_mode: bool,
    pub db_instance_num: usize,
    pub db_path: String,
    /// Authentication password. When set, clients must authenticate via AUTH command.
    pub requirepass: Option<String>,
    pub raft: Option<RaftClusterConfig>,
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
            .field("redis_compatible_mode", &self.redis_compatible_mode)
            .field("db_instance_num", &self.db_instance_num)
            .field("db_path", &self.db_path)
            .field(
                "requirepass",
                if self.requirepass.is_some() {
                    &"<REDACTED>"
                } else {
                    &"<NONE>"
                },
            )
            .field("raft", &self.raft)
            .finish()
    }
}

#[derive(Debug, Validate, Clone)]
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

// set default value for config
impl Default for Config {
    fn default() -> Self {
        Self {
            binding: DEFAULT_BINDING.to_string(),
            port: DEFAULT_PORT,
            timeout: 50,
            memory: 1024 * 1024 * 1024, // 1GB
            log_dir: "/data/kiwi_rs/logs".to_string(),
            db_dir: "./db".to_string(),
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
            db_path: "./db".to_string(),
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,
            requirepass: None,
            raft: None,
        }
    }
}
impl Config {
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

        let mut raft_node_id: Option<u64> = None;
        let mut raft_addr: Option<String> = None;
        let mut raft_resp_addr: Option<String> = None;
        let mut raft_data_dir: Option<String> = None;
        let mut raft_heartbeat_interval: Option<u64> = None;
        let mut raft_election_timeout_min: Option<u64> = None;
        let mut raft_election_timeout_max: Option<u64> = None;
        let mut raft_use_memory_log_store: bool = false;

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
                "db-dir" => {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        return Err(Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(
                                "db-dir must not be empty".to_string(),
                            ),
                        });
                    }
                    config.db_dir = trimmed.to_string();
                    config.db_path = trimmed.to_string();
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
                    raft_node_id = Some(value.parse().map_err(|e| Error::InvalidConfig {
                        source: serde_ini::de::Error::Custom(format!(
                            "Invalid raft-node-id: {}",
                            e
                        )),
                    })?);
                }
                "raft-addr" | "cluster-addr" => {
                    raft_addr = Some(value);
                }
                "raft-resp-addr" | "cluster-resp-addr" => {
                    raft_resp_addr = Some(value);
                }
                "raft-data-dir" | "cluster-data-dir" => {
                    raft_data_dir = Some(value);
                }
                "raft-heartbeat-interval" | "cluster-heartbeat-interval" => {
                    raft_heartbeat_interval =
                        Some(value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid raft-heartbeat-interval: {}",
                                e
                            )),
                        })?);
                }
                "raft-election-timeout-min" | "cluster-election-timeout-min" => {
                    raft_election_timeout_min =
                        Some(value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid raft-election-timeout-min: {}",
                                e
                            )),
                        })?);
                }
                "raft-election-timeout-max" | "cluster-election-timeout-max" => {
                    raft_election_timeout_max =
                        Some(value.parse().map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid raft-election-timeout-max: {}",
                                e
                            )),
                        })?);
                }
                "raft-use-memory-log-store" => {
                    raft_use_memory_log_store =
                        parse_bool_from_string(&value).map_err(|e| Error::InvalidConfig {
                            source: serde_ini::de::Error::Custom(format!(
                                "Invalid raft-use-memory-log-store: {}",
                                e
                            )),
                        })?;
                }
                "db-path" => {
                    config.db_path = value.clone();
                    config.db_dir = value;
                }
                "requirepass" => {
                    config.requirepass = Some(value);
                }
                _ => {
                    // Unknown configuration key, skip it
                    continue;
                }
            }
        }

        if let (Some(node_id), Some(addr), Some(resp_addr), Some(data_dir)) =
            (raft_node_id, raft_addr, raft_resp_addr, raft_data_dir)
        {
            config.raft = Some(RaftClusterConfig {
                node_id,
                raft_addr: addr,
                resp_addr,
                data_dir,
                heartbeat_interval_ms: raft_heartbeat_interval,
                election_timeout_min_ms: raft_election_timeout_min,
                election_timeout_max_ms: raft_election_timeout_max,
                use_memory_log_store: raft_use_memory_log_store,
            });
        }

        config
            .validate()
            .map_err(|_e| Error::ValidConfigFail { source: _e })?;

        Ok(config)
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
