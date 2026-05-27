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
use serde::{Deserialize, Deserializer, Serialize, de};
use snafu::ResultExt;
use std::fmt::Formatter;
use validator::Validate;

use crate::de_func::deserialize_memory;
use crate::error::Error;

/// Compression algorithm for RocksDB column families.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum CompressionType {
    None,
    Snappy,
    Lz4,
    Zstd,
    Zlib,
    Bz2,
}

impl<'de> Deserialize<'de> for CompressionType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "none" | "no" => Ok(CompressionType::None),
            "snappy" => Ok(CompressionType::Snappy),
            "lz4" => Ok(CompressionType::Lz4),
            "zstd" => Ok(CompressionType::Zstd),
            "zlib" => Ok(CompressionType::Zlib),
            "bz2" => Ok(CompressionType::Bz2),
            _ => Err(de::Error::custom(format!(
                "unknown compression type '{}', valid values: none, snappy, lz4, zstd, zlib, bz2",
                s
            ))),
        }
    }
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

/// Default functions for serde
mod defaults {
    pub fn binding() -> String {
        super::DEFAULT_BINDING.to_string()
    }
    pub fn port() -> u16 {
        super::DEFAULT_PORT
    }
    pub fn timeout() -> u32 {
        50
    }
    pub fn memory() -> u64 {
        1024 * 1024 * 1024
    } // 1GB
    pub fn log_dir() -> String {
        "/data/kiwi_rs/logs".to_string()
    }
    pub fn db_dir() -> String {
        "./db".to_string()
    }
    pub fn db_path() -> String {
        "./db".to_string()
    }
    pub fn redis_compatible_mode() -> bool {
        false
    }
    pub fn db_instance_num() -> usize {
        3
    }
    pub fn rocksdb_max_subcompactions() -> u32 {
        0
    }
    pub fn rocksdb_max_background_jobs() -> i32 {
        4
    }
    pub fn rocksdb_max_write_buffer_number() -> i32 {
        2
    }
    pub fn rocksdb_min_write_buffer_number_to_merge() -> i32 {
        2
    }
    pub fn rocksdb_write_buffer_size() -> usize {
        64 << 20
    } // 64MB
    pub fn rocksdb_level0_file_num_compaction_trigger() -> i32 {
        4
    }
    pub fn rocksdb_num_levels() -> i32 {
        7
    }
    pub fn rocksdb_enable_pipelined_write() -> bool {
        false
    }
    pub fn rocksdb_level0_slowdown_writes_trigger() -> i32 {
        20
    }
    pub fn rocksdb_level0_stop_writes_trigger() -> i32 {
        36
    }
    pub fn rocksdb_ttl_second() -> u64 {
        30 * 24 * 60 * 60
    } // 30 days
    pub fn rocksdb_periodic_second() -> u64 {
        30 * 24 * 60 * 60
    } // 30 days
    pub fn rocksdb_level_compaction_dynamic_level_bytes() -> bool {
        true
    }
    pub fn rocksdb_max_open_files() -> i32 {
        10000
    }
    pub fn rocksdb_target_file_size_base() -> u64 {
        64 << 20
    } // 64MB
    pub fn rocksdb_compression_type() -> super::CompressionType {
        super::CompressionType::Lz4
    }
    pub fn small_compaction_threshold() -> usize {
        5000
    }
    pub fn small_compaction_duration_threshold() -> usize {
        10000
    }
}

// config struct define
#[derive(Deserialize, Validate)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(default = "defaults::binding")]
    pub binding: String,

    #[validate(range(min = 1024, max = 65535))]
    #[serde(default = "defaults::port")]
    pub port: u16,

    #[serde(default = "defaults::timeout")]
    pub timeout: u32,

    /// Memory limit for the server. Accepts human-readable sizes like "1GB", "256MB"
    /// or raw byte counts as integers.
    #[serde(default = "defaults::memory", deserialize_with = "deserialize_memory")]
    pub memory: u64,

    #[serde(default = "defaults::log_dir")]
    pub log_dir: String,

    #[serde(default = "defaults::db_dir")]
    pub db_dir: String,

    #[serde(default = "defaults::db_path")]
    pub db_path: String,

    #[serde(default = "defaults::redis_compatible_mode")]
    pub redis_compatible_mode: bool,

    #[serde(default = "defaults::db_instance_num")]
    pub db_instance_num: usize,

    #[serde(default = "defaults::small_compaction_threshold")]
    pub small_compaction_threshold: usize,

    #[serde(default = "defaults::small_compaction_duration_threshold")]
    pub small_compaction_duration_threshold: usize,

    #[serde(default = "defaults::rocksdb_max_subcompactions")]
    pub rocksdb_max_subcompactions: u32,

    #[serde(default = "defaults::rocksdb_max_background_jobs")]
    pub rocksdb_max_background_jobs: i32,

    #[serde(default = "defaults::rocksdb_max_write_buffer_number")]
    pub rocksdb_max_write_buffer_number: i32,

    #[serde(default = "defaults::rocksdb_min_write_buffer_number_to_merge")]
    pub rocksdb_min_write_buffer_number_to_merge: i32,

    #[serde(default = "defaults::rocksdb_write_buffer_size")]
    pub rocksdb_write_buffer_size: usize,

    #[serde(default = "defaults::rocksdb_level0_file_num_compaction_trigger")]
    pub rocksdb_level0_file_num_compaction_trigger: i32,

    #[serde(default = "defaults::rocksdb_num_levels")]
    pub rocksdb_num_levels: i32,

    #[serde(default = "defaults::rocksdb_enable_pipelined_write")]
    pub rocksdb_enable_pipelined_write: bool,

    #[serde(default = "defaults::rocksdb_level0_slowdown_writes_trigger")]
    pub rocksdb_level0_slowdown_writes_trigger: i32,

    #[serde(default = "defaults::rocksdb_level0_stop_writes_trigger")]
    pub rocksdb_level0_stop_writes_trigger: i32,

    #[serde(default = "defaults::rocksdb_ttl_second")]
    pub rocksdb_ttl_second: u64,

    #[serde(default = "defaults::rocksdb_periodic_second")]
    pub rocksdb_periodic_second: u64,

    #[serde(default = "defaults::rocksdb_level_compaction_dynamic_level_bytes")]
    pub rocksdb_level_compaction_dynamic_level_bytes: bool,

    #[serde(default = "defaults::rocksdb_max_open_files")]
    pub rocksdb_max_open_files: i32,

    #[serde(default = "defaults::rocksdb_target_file_size_base")]
    pub rocksdb_target_file_size_base: u64,

    /// Compression algorithm applied to all column families.
    /// Supported values: none, lz4, snappy, zstd, zlib, bz2. Default: lz4.
    #[serde(default = "defaults::rocksdb_compression_type")]
    pub rocksdb_compression_type: CompressionType,

    /// Authentication password. When set, clients must authenticate via AUTH command.
    #[serde(default)]
    pub requirepass: Option<String>,

    #[serde(default)]
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

#[derive(Debug, Deserialize, Validate, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RaftClusterConfig {
    #[validate(range(min = 1))]
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub data_dir: String,
    #[serde(default)]
    pub heartbeat_interval_ms: Option<u64>,
    #[serde(default)]
    pub election_timeout_min_ms: Option<u64>,
    #[serde(default)]
    pub election_timeout_max_ms: Option<u64>,
    /// 是否使用内存日志存储，默认 false（使用 RocksDB 持久化存储）
    #[serde(default)]
    pub use_memory_log_store: bool,
}

// set default value for config
impl Default for Config {
    fn default() -> Self {
        Self {
            binding: defaults::binding(),
            port: defaults::port(),
            timeout: defaults::timeout(),
            memory: defaults::memory(),
            log_dir: defaults::log_dir(),
            db_dir: defaults::db_dir(),
            db_path: defaults::db_path(),
            redis_compatible_mode: defaults::redis_compatible_mode(),
            rocksdb_max_subcompactions: defaults::rocksdb_max_subcompactions(),
            rocksdb_max_background_jobs: defaults::rocksdb_max_background_jobs(),
            rocksdb_max_write_buffer_number: defaults::rocksdb_max_write_buffer_number(),
            rocksdb_min_write_buffer_number_to_merge:
                defaults::rocksdb_min_write_buffer_number_to_merge(),
            rocksdb_write_buffer_size: defaults::rocksdb_write_buffer_size(),
            rocksdb_level0_file_num_compaction_trigger:
                defaults::rocksdb_level0_file_num_compaction_trigger(),
            rocksdb_num_levels: defaults::rocksdb_num_levels(),
            rocksdb_enable_pipelined_write: defaults::rocksdb_enable_pipelined_write(),
            rocksdb_level0_slowdown_writes_trigger:
                defaults::rocksdb_level0_slowdown_writes_trigger(),
            rocksdb_level0_stop_writes_trigger: defaults::rocksdb_level0_stop_writes_trigger(),
            rocksdb_ttl_second: defaults::rocksdb_ttl_second(),
            rocksdb_periodic_second: defaults::rocksdb_periodic_second(),
            rocksdb_level_compaction_dynamic_level_bytes:
                defaults::rocksdb_level_compaction_dynamic_level_bytes(),
            rocksdb_max_open_files: defaults::rocksdb_max_open_files(),
            rocksdb_target_file_size_base: defaults::rocksdb_target_file_size_base(),
            rocksdb_compression_type: defaults::rocksdb_compression_type(),
            db_instance_num: defaults::db_instance_num(),
            small_compaction_threshold: defaults::small_compaction_threshold(),
            small_compaction_duration_threshold: defaults::small_compaction_duration_threshold(),
            requirepass: None,
            raft: None,
        }
    }
}

impl Config {
    /// Load config from a TOML file.
    ///
    /// Missing fields use their default values. The `[raft]` section is optional.
    ///
    /// # Errors
    ///
    /// Returns `Error::ConfigFile` if the file cannot be read,
    /// `Error::InvalidConfig` if TOML parsing fails,
    /// or `Error::ValidConfigFail` if validation constraints are violated.
    pub fn load(path: &str) -> Result<Self, Error> {
        let content =
            std::fs::read_to_string(path).context(crate::error::ConfigFileSnafu { path })?;

        let mut config: Config =
            toml::from_str(&content).context(crate::error::InvalidConfigSnafu)?;

        // If db-dir is set but db-path is not explicitly set, sync them
        if config.db_dir != defaults::db_dir() && config.db_path == defaults::db_path() {
            config.db_path = config.db_dir.clone();
        }

        config
            .validate()
            .map_err(|e| Error::ValidConfigFail { source: e })?;

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

        // Apply compression to all column families (level 0 and 1 use no compression, levels 2+ use configured type)
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
