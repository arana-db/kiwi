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

//! Storage engine options and configurations

use std::sync::Arc;

use rocksdb::{BlockBasedOptions, Cache, Options};

use crate::error::{OptionNotDynamicallyModifiableSnafu, Result};

/// Dynamic database options that can be modified at runtime
const DYNAMIC_DB_OPTIONS: &[&str] = &[
    "max_background_jobs",
    "max_background_compactions",
    "max_open_files",
    "bytes_per_sync",
    "delayed_write_rate",
    "max_total_wal_size",
    "wal_bytes_per_sync",
    "stats_dump_period_sec",
];

/// Dynamic column family options that can be modified at runtime
const DYNAMIC_CF_OPTIONS: &[&str] = &[
    "max_write_buffer_number",
    "write_buffer_size",
    "target_file_size_base",
    "target_file_size_multiplier",
    "arena_block_size",
    "level0_file_num_compaction_trigger",
    "level0_slowdown_writes_trigger",
    "level0_stop_writes_trigger",
    "max_compaction_bytes",
    "soft_pending_compaction_bytes_limit",
    "hard_pending_compaction_bytes_limit",
];

/// Storage engine options
pub struct StorageOptions {
    /// RocksDB options
    pub options: Options,
    /// BlockBasedTable options
    pub table_options: BlockBasedOptions,
    /// Block cache size in bytes
    pub block_cache_size: usize,
    /// Whether to share block cache across column families
    pub share_block_cache: bool,
    /// Shared block cache created by the default/configuration path and reused
    /// by every `Redis` instance through the shared `Arc<StorageOptions>`.
    /// `None` means no explicit shared cache. When sharing is disabled,
    /// each `Redis` instance builds one cache from `block_cache_size`.
    pub block_cache: Option<Arc<Cache>>,
    /// Maximum size for statistics
    pub statistics_max_size: usize,
    /// Threshold for small value compaction
    pub small_compaction_threshold: usize,
    /// Duration threshold for small value compaction (in milliseconds)
    pub small_compaction_duration_threshold: usize,
    /// Number of database instances
    pub db_instance_num: usize,
    /// Database ID
    pub db_id: usize,
    /// Raft timeout in seconds
    pub raft_timeout_s: u32,
    /// Maximum gap between log indices
    pub max_gap: i64,
    /// Memory manager size
    pub mem_manager_size: usize,
    /// Vector Set feature configuration
    pub vector: conf::vector_config::VectorConfig,
}

impl Default for StorageOptions {
    fn default() -> Self {
        let block_cache_size = 8 << 30; // 8GB
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(false);
        options.set_max_open_files(10000);
        options.set_write_buffer_size(64 << 20); // 64MB
        options.set_max_write_buffer_number(3);
        options.set_target_file_size_base(64 << 20); // 64MB
        options.set_level_compaction_dynamic_level_bytes(true);

        Self {
            options,
            table_options: BlockBasedOptions::default(),
            block_cache_size,
            share_block_cache: true,
            block_cache: Some(Arc::new(Cache::new_lru_cache(block_cache_size))),
            statistics_max_size: 0,
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,
            db_instance_num: 3,
            db_id: 0,
            raft_timeout_s: u32::MAX,
            max_gap: 1000,
            mem_manager_size: 100_000_000,
            vector: conf::vector_config::VectorConfig::default(),
        }
    }
}

impl StorageOptions {
    /// Create a new StorageOptions with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Build StorageOptions from a loaded [`conf::config::Config`].
    pub fn from_config(config: &conf::config::Config) -> Self {
        let mut rocksdb_opts = config.get_rocksdb_options();
        rocksdb_opts.create_missing_column_families(false);
        // Build the shared block cache once when sharing is enabled and a
        // memory budget is configured. Every `Redis` instance receives the
        // same `Arc<StorageOptions>` and therefore reuses this single cache.
        let block_cache = if config.share_block_cache && config.memory > 0 {
            Some(Arc::new(rocksdb::Cache::new_lru_cache(
                config.memory as usize,
            )))
        } else {
            None
        };
        Self {
            options: rocksdb_opts,
            block_cache_size: config.memory as usize,
            share_block_cache: config.share_block_cache,
            block_cache,
            small_compaction_threshold: config.small_compaction_threshold,
            small_compaction_duration_threshold: config.small_compaction_duration_threshold,
            db_instance_num: config.db_instance_num,
            vector: config.vector.clone(),
            ..Self::default()
        }
    }

    /// Set block cache size
    pub fn set_block_cache_size(&mut self, size: usize) -> &mut Self {
        self.block_cache_size = size;
        self.rebuild_shared_block_cache();
        self
    }

    /// Set whether to share block cache
    pub fn set_share_block_cache(&mut self, share: bool) -> &mut Self {
        self.share_block_cache = share;
        self.rebuild_shared_block_cache();
        self
    }

    fn rebuild_shared_block_cache(&mut self) {
        self.block_cache = if self.share_block_cache && self.block_cache_size > 0 {
            Some(Arc::new(Cache::new_lru_cache(self.block_cache_size)))
        } else {
            None
        };
    }

    /// Set statistics maximum size
    pub fn set_statistics_max_size(&mut self, size: usize) -> &mut Self {
        self.statistics_max_size = size;
        self
    }

    /// Set small compaction threshold
    pub fn set_small_compaction_threshold(&mut self, threshold: usize) -> &mut Self {
        self.small_compaction_threshold = threshold;
        self
    }

    /// Set small compaction duration threshold
    pub fn set_small_compaction_duration_threshold(&mut self, threshold: usize) -> &mut Self {
        self.small_compaction_duration_threshold = threshold;
        self
    }

    /// Set database instance number
    pub fn set_db_instance_num(&mut self, num: usize) {
        self.db_instance_num = num;
    }

    /// Set database ID
    pub fn set_db_id(&mut self, id: usize) {
        self.db_id = id;
    }

    /// Set Raft timeout
    pub fn set_raft_timeout(&mut self, timeout: u32) -> &mut Self {
        self.raft_timeout_s = timeout;
        self
    }

    /// Set maximum gap
    pub fn set_max_gap(&mut self, gap: i64) -> &mut Self {
        self.max_gap = gap;
        self
    }

    /// Set memory manager size
    pub fn set_mem_manager_size(&mut self, size: usize) -> &mut Self {
        self.mem_manager_size = size;
        self
    }

    pub fn validate_dynamic_option(option_type: OptionType, key: &str) -> Result<()> {
        match option_type {
            OptionType::DB if Self::is_dynamic_db_option(key) => Ok(()),
            OptionType::ColumnFamily if Self::is_dynamic_cf_option(key) => Ok(()),
            _ => OptionNotDynamicallyModifiableSnafu {
                message: format!("option '{key}' is not dynamically modifiable"),
            }
            .fail(),
        }
    }

    fn is_dynamic_db_option(key: &str) -> bool {
        DYNAMIC_DB_OPTIONS.contains(&key)
    }

    fn is_dynamic_cf_option(key: &str) -> bool {
        DYNAMIC_CF_OPTIONS.contains(&key)
    }

    pub fn get_supported_dynamic_options() -> (Vec<String>, Vec<String>) {
        let db_options = DYNAMIC_DB_OPTIONS.iter().map(|s| s.to_string()).collect();

        let cf_options = DYNAMIC_CF_OPTIONS.iter().map(|s| s.to_string()).collect();

        (db_options, cf_options)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionType {
    DB,
    ColumnFamily,
}

#[cfg(test)]
mod tests {
    use super::StorageOptions;

    #[test]
    fn from_config_preserves_disabled_share_block_cache() {
        let config = conf::config::Config {
            share_block_cache: false,
            memory: 64 * 1024 * 1024,
            ..conf::config::Config::default()
        };

        let options = StorageOptions::from_config(&config);

        assert!(!options.share_block_cache);
        assert!(options.block_cache.is_none());
        assert_eq!(options.block_cache_size, 64 * 1024 * 1024);
    }

    #[test]
    fn block_cache_setters_keep_the_derived_cache_consistent() {
        let config = conf::config::Config {
            memory: 4 * 1024 * 1024,
            ..conf::config::Config::default()
        };
        let mut options = StorageOptions::from_config(&config);
        let original_cache = options
            .block_cache
            .as_ref()
            .expect("sharing should create a block cache")
            .clone();

        options.set_block_cache_size(8 * 1024 * 1024);
        let resized_cache = options
            .block_cache
            .as_ref()
            .expect("resizing a shared cache should rebuild it");
        assert!(!std::sync::Arc::ptr_eq(&original_cache, resized_cache));

        options.set_share_block_cache(false);
        assert!(options.block_cache.is_none());

        options.set_share_block_cache(true);
        assert!(options.block_cache.is_some());
    }
}
