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
use validator::Validate;

use crate::de_func::{parse_bool_from_string, parse_memory, parse_redis_config};
use crate::error::Error;

const DEFAULT_BINDING: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 7379; // Redis-compatible port (7xxx variant of 6379)
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
    pub raft: Option<RaftClusterConfig>,
}

#[derive(Debug, Validate, Clone)]
pub struct RaftClusterConfig {
    #[validate(range(min = 1))]
    pub node_id: u64,
    pub raft_addr: String,
    pub resp_addr: String,
    pub data_dir: String,
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
                "raft-node-id" => {
                    raft_node_id = Some(value.parse().map_err(|e| Error::InvalidConfig {
                        source: serde_ini::de::Error::Custom(format!(
                            "Invalid raft-node-id: {}",
                            e
                        )),
                    })?);
                }
                "raft-addr" => {
                    raft_addr = Some(value);
                }
                "raft-resp-addr" => {
                    raft_resp_addr = Some(value);
                }
                "raft-data-dir" => {
                    raft_data_dir = Some(value);
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
