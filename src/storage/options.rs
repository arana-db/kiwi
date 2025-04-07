//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Storage engine options and configurations

use rocksdb::{BlockBasedOptions, Cache, Env, Options};
use std::collections::HashMap;

/// Column family types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnFamilyType {
    /// For metadata storage
    Meta,
    /// For actual data storage
    Data,
    /// For both metadata and data
    MetaAndData,
}

/// Storage engine options
pub struct StorageOptions {
    /// RocksDB options
    pub options: Options,
    /// Table options for RocksDB
    pub table_options: BlockBasedOptions,
    /// Block cache size in bytes
    pub block_cache_size: usize,
    /// Whether to share block cache across column families
    pub share_block_cache: bool,
    /// Maximum size for statistics
    pub statistics_max_size: usize,
    /// Threshold for small value compaction
    pub small_compaction_threshold: usize,
    /// Duration threshold for small value compaction (in milliseconds)
    pub small_compaction_duration_threshold: usize,
    /// Number of database instances
    pub db_instance_num: usize,
    /// Database ID
    pub db_id: i32,
    /// Raft timeout in seconds
    pub raft_timeout_s: u32,
    /// Maximum gap between log indices
    pub max_gap: i64,
    /// Memory manager size
    pub mem_manager_size: u64,
}

impl Default for StorageOptions {
    fn default() -> Self {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_open_files(10000);
        options.set_write_buffer_size(64 << 20); // 64MB
        options.set_max_write_buffer_number(3);
        options.set_target_file_size_base(64 << 20); // 64MB
        options.set_level_compaction_dynamic_level_bytes(true);

        let mut table_options = BlockBasedOptions::default();
        table_options.set_block_size(4 * 1024); // 4KB
        table_options.set_cache_index_and_filter_blocks(true);
        table_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

        Self {
            options,
            table_options,
            block_cache_size: 8 << 30, // 8GB
            share_block_cache: true,
            statistics_max_size: 0,
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,
            db_instance_num: 3,
            db_id: 0,
            raft_timeout_s: u32::MAX,
            max_gap: 1000,
            mem_manager_size: 100_000_000,
        }
    }
}

impl StorageOptions {
    /// Create a new StorageOptions with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set block cache size
    pub fn set_block_cache_size(&mut self, size: usize) -> &mut Self {
        self.block_cache_size = size;
        self
    }

    /// Set whether to share block cache
    pub fn set_share_block_cache(&mut self, share: bool) -> &mut Self {
        self.share_block_cache = share;
        self
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
    pub fn set_db_id(&mut self, id: i32) {
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
    pub fn set_mem_manager_size(&mut self, size: u64) -> &mut Self {
        self.mem_manager_size = size;
        self
    }
}
