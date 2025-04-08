// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Redis implementation for KiwiDB storage engine
//! This module provides Redis-compatible storage operations using RocksDB

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::collections::HashMap;
use rocksdb::{DB, ColumnFamilyDescriptor, Options, BlockBasedOptions, ReadOptions, WriteOptions, Env};
use rocksdb::{DBCompactionStyle, SliceTransform};

use crate::{Result, StorageError};
use crate::base_data_value_format::{DataType, data_type_to_tag};
use crate::lock_mgr::LockMgr;
use crate::types::{KeyInfo, KeyValue, FieldValue, ScoreMember, ValueStatus};
use crate::engine::Engine;
use lru::LruCache;

/// Column family indices
pub enum ColumnFamilyIndex {
    MetaCF = 0,
    HashesDataCF = 1,
    SetsDataCF = 2,
    ListsDataCF = 3,
    ZsetsDataCF = 4,
    ZsetsScoreCF = 5,
}

/// Redis storage options
pub struct RedisOptions {
    pub options: Options,
    pub table_options: BlockBasedOptions,
    pub block_cache_size: usize,
    pub share_block_cache: bool,
    pub statistics_max_size: usize,
    pub small_compaction_threshold: usize,
    pub small_compaction_duration_threshold: usize,
    pub raft_timeout_s: u32,
}

impl Clone for RedisOptions {
    fn clone(&self) -> Self {
        let mut table_options = BlockBasedOptions::default();
        // Copy block size from original table options
        let block_size = 16 * 1024; // Default 16KB block size
        
        Self {
            options: self.options.clone(),
            table_options,
            block_cache_size: self.block_cache_size,
            share_block_cache: self.share_block_cache,
            statistics_max_size: self.statistics_max_size,
            small_compaction_threshold: self.small_compaction_threshold,
            small_compaction_duration_threshold: self.small_compaction_duration_threshold,
            raft_timeout_s: self.raft_timeout_s,
        }
    }
}

impl Default for RedisOptions {
    fn default() -> Self {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_open_files(10000);
        options.set_write_buffer_size(64 << 20); // 64MB
        options.set_max_write_buffer_number(3);
        options.set_target_file_size_base(64 << 20); // 64MB
        options.set_level_compaction_dynamic_level_bytes(true);
        
        let mut table_options = BlockBasedOptions::default();
        table_options.set_block_size(16 * 1024); // 16KB

        RedisOptions {
            options,
            table_options,
            block_cache_size: 8 << 30, // 8GB
            share_block_cache: true,
            statistics_max_size: 1000,
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 60,
            raft_timeout_s: 5,
        }
    }
}

/// Key statistics for tracking modifications and durations
#[derive(Clone)]
pub struct KeyStatistics {
    window_size: usize,
    durations: Vec<u64>,
    modify_count: u64,
}

impl KeyStatistics {
    pub fn new(size: usize) -> Self {
        Self {
            window_size: size + 2,
            durations: Vec::with_capacity(size + 2),
            modify_count: 0,
        }
    }
    
    pub fn add_duration(&mut self, duration: u64) {
        self.durations.push(duration);
        if self.durations.len() > self.window_size {
            self.durations.remove(0);
        }
    }
    
    pub fn avg_duration(&self) -> u64 {
        if self.durations.len() < self.window_size {
            return 0;
        }
        
        let mut min = self.durations[0];
        let mut max = self.durations[0];
        let mut sum = 0;
        
        for &duration in &self.durations {
            if duration < min {
                min = duration;
            }
            if duration > max {
                max = duration;
            }
            sum += duration;
        }
        
        (sum - max - min) / (self.durations.len() as u64 - 2)
    }
    
    pub fn add_modify_count(&mut self, count: u64) {
        self.modify_count += count;
    }
    
    pub fn modify_count(&self) -> u64 {
        self.modify_count
    }
}

// Redis implementation
pub struct Redis {
    pub db: Option<DB>,
    pub handles: Vec<rocksdb::ColumnFamily>,
    pub options: RedisOptions,
    pub lock_mgr: Arc<LockMgr>,
    pub storage: Arc<crate::Storage>,
    pub index: i32,
    pub statistics_store: LruCache<String, KeyStatistics>,
    pub scan_cursors_store: LruCache<String, u64>,
    pub spop_counts_store: LruCache<String, u64>,
    pub small_compaction_threshold: u64,
    pub small_compaction_duration_threshold: u64,
    pub default_write_options: WriteOptions,
    pub need_close: AtomicBool,
    pub is_starting: bool,
}

// Remove BlockBasedOptions Clone implementation since it violates orphan rules
// Instead clone the options manually where needed

impl Redis {
    /// Create a new Redis instance
    pub fn new(storage: Arc<crate::Storage>, index: i32, lock_mgr: Arc<LockMgr>) -> Self {
        let mut redis = Self {
            options: RedisOptions::default(),
            db: None,
            storage,
            index,
            lock_mgr,
            handles: Vec::new(),
            statistics_store: LruCache::new(std::num::NonZeroUsize::new(10000).unwrap()),
            scan_cursors_store: LruCache::new(std::num::NonZeroUsize::new(5000).unwrap()),
            spop_counts_store: LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()),
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,
            default_write_options: WriteOptions::default(),
            need_close: AtomicBool::new(false),
            is_starting: true,
        };
        
        redis
    }
    
    /// Open the Redis database
    pub fn open(&mut self, options: RedisOptions, db_path: &str) -> Result<()> {
        // Set up options
        self.small_compaction_threshold = options.small_compaction_threshold as u64;
        self.statistics_store = LruCache::new(std::num::NonZeroUsize::new(options.statistics_max_size).unwrap());
        
        // Create column family descriptors
        let mut column_families = Vec::new();
        
        // Meta & string column family
        let mut meta_cf_opts = options.options.clone();
        let mut meta_table_opts = BlockBasedOptions::default();
        meta_table_opts.set_block_size(16 * 1024); // Use same block size as default
        if !options.share_block_cache && options.block_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(options.block_cache_size);
            meta_table_opts.set_block_cache(&cache);
        }
        meta_cf_opts.set_block_based_table_factory(&meta_table_opts);
        column_families.push(ColumnFamilyDescriptor::new("default", meta_cf_opts));
        
        // Hash column family
        let mut hash_data_cf_opts = options.options.clone();
        let mut hash_data_cf_table_opts = BlockBasedOptions::default();
        hash_data_cf_table_opts.set_block_size(16 * 1024); // Use same block size as default
        if !options.share_block_cache && options.block_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(options.block_cache_size);
            hash_data_cf_table_opts.set_block_cache(&cache);
        }
        hash_data_cf_opts.set_block_based_table_factory(&hash_data_cf_table_opts);
        column_families.push(ColumnFamilyDescriptor::new("hash_data_cf", hash_data_cf_opts));
        
        // Set column family
        let mut set_data_cf_opts = options.options.clone();
        let mut set_data_cf_table_opts = BlockBasedOptions::default();
        set_data_cf_table_opts.set_block_size(16 * 1024); // Use same block size as default
        if !options.share_block_cache && options.block_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(options.block_cache_size);
            set_data_cf_table_opts.set_block_cache(&cache);
        }
        set_data_cf_opts.set_block_based_table_factory(&set_data_cf_table_opts);
        column_families.push(ColumnFamilyDescriptor::new("set_data_cf", set_data_cf_opts));
        
        // List column family
        let mut list_data_cf_opts = options.options.clone();
        let mut list_data_cf_table_opts = BlockBasedOptions::default();
        list_data_cf_table_opts.set_block_size(16 * 1024); // Use 16KB as default block size
        if !options.share_block_cache && options.block_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(options.block_cache_size);
            list_data_cf_table_opts.set_block_cache(&cache);
        }
        list_data_cf_opts.set_block_based_table_factory(&list_data_cf_table_opts);
        column_families.push(ColumnFamilyDescriptor::new("list_data_cf", list_data_cf_opts));
        
        // ZSet column families
        let mut zset_data_cf_opts = options.options.clone();
        let mut zset_data_cf_table_opts = BlockBasedOptions::default();
        zset_data_cf_table_opts.set_block_size(16 * 1024); // Use 16KB as default block size
        if !options.share_block_cache && options.block_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(options.block_cache_size);
            zset_data_cf_table_opts.set_block_cache(&cache);
        }
        zset_data_cf_opts.set_block_based_table_factory(&zset_data_cf_table_opts);
        column_families.push(ColumnFamilyDescriptor::new("zset_data_cf", zset_data_cf_opts));
        
        let mut zset_score_cf_opts = options.options.clone();
        let mut zset_score_cf_table_opts = BlockBasedOptions::default();
        zset_score_cf_table_opts.set_block_size(16 * 1024); // Use 16KB as default block size
        if !options.share_block_cache && options.block_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(options.block_cache_size);
            zset_score_cf_table_opts.set_block_cache(&cache);
        }
        zset_score_cf_opts.set_block_based_table_factory(&zset_score_cf_table_opts);
        column_families.push(ColumnFamilyDescriptor::new("zset_score_cf", zset_score_cf_opts));
        
        // Open DB with column families
        let db = DB::open_cf_descriptors(&options.options, db_path, column_families)
            .map_err(|e| StorageError::Rocks(e))?;
        
        self.db = Some(db);
        // Initialize handles from column families
        self.handles = Vec::new(); // TODO: properly initialize handles
        self.is_starting = false;
        
        Ok(())
    }
    
    /// Get the database index
    pub fn get_index(&self) -> i32 {
        self.index
    }
    
    /// Set whether to close the database on drop
    pub fn set_need_close(&self, need_close: bool) {
        self.need_close.store(need_close, Ordering::SeqCst);
    }
    
    /// Compact the database range
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        if let Some(db) = &self.db {
            // Compact default column family
            db.compact_range(begin, end);
            
            // Compact other column families
            for (i, handle) in self.handles.iter().enumerate() {
                if i > 0 { // Skip default CF which we already compacted
                    db.compact_range_cf(handle, begin, end);
                }
            }
        }
        
        Ok(())
    }
    
    /// Get a property from the database
    pub fn get_property(&self, property: &str, out: &mut u64) -> Result<()> {
        if let Some(db) = &self.db {
            if let Some(value) = db.property_value(property)? {
                *out = value.parse().unwrap_or(0);
            }
        }
        
        Ok(())
    }
    
    /// Set write WAL options
    pub fn set_write_wal_options(&mut self, is_wal_disable: bool) {
        self.default_write_options.disable_wal(is_wal_disable);
    }
    
    /// Update statistics for a specific key
    pub fn update_specific_key_statistics(&mut self, dtype: DataType, key: &str, count: u64) -> Result<()> {
        if self.statistics_store.cap().get() == 0 || count == 0 || self.small_compaction_threshold == 0 {
            return Ok(());
        }
        
        let lkp_key = format!("{}{}", data_type_to_tag(dtype), key);
        
        let stats = if let Some(stats) = self.statistics_store.get(&lkp_key) {
            let mut stats_clone = KeyStatistics::new(stats.window_size - 2);
            stats_clone.modify_count = stats.modify_count;
            stats_clone.durations = stats.durations.clone();
            stats_clone.add_modify_count(count);
            stats_clone
        } else {
            let mut stats = KeyStatistics::new(10);
            stats.add_modify_count(count);
            stats
        };
        
        self.statistics_store.put(lkp_key.clone(), stats.clone());
        self.add_compact_key_task_if_needed(dtype, key, stats.modify_count(), stats.avg_duration())?;
        
        Ok(())
    }
    
    /// Update duration statistics for a specific key
    pub fn update_specific_key_duration(&mut self, dtype: DataType, key: &str, duration: u64) -> Result<()> {
        if self.statistics_store.cap().get() == 0 || duration == 0 || self.small_compaction_duration_threshold == 0 {
            return Ok(());
        }
        
        let lkp_key = format!("{}{}", data_type_to_tag(dtype), key);
        
        let stats = if let Some(stats) = self.statistics_store.get(&lkp_key) {
            let mut stats_clone = KeyStatistics::new(stats.window_size - 2);
            stats_clone.modify_count = stats.modify_count;
            stats_clone.durations = stats.durations.clone();
            stats_clone.add_duration(duration);
            stats_clone
        } else {
            let mut stats = KeyStatistics::new(10);
            stats.add_duration(duration);
            stats
        };
        
        self.statistics_store.put(lkp_key.clone(), stats.clone());
        self.add_compact_key_task_if_needed(dtype, key, stats.modify_count(), stats.avg_duration())?;
        
        Ok(())
    }
    
    /// Add a compact key task if needed
    fn add_compact_key_task_if_needed(&mut self, dtype: DataType, key: &str, total: u64, duration: u64) -> Result<()> {
        if total < self.small_compaction_threshold || duration < self.small_compaction_duration_threshold {
            return Ok(());
        }
        
        let lkp_key = format!("{}{}", data_type_to_tag(dtype), key);
        // TODO: Add background task for compaction
        self.statistics_store.pop(&lkp_key);
        
        Ok(())
    }
    
    /// Get scan start point
    pub fn get_scan_start_point(&mut self, dtype: DataType, key: &[u8], pattern: &[u8], cursor: i64, start_point: &mut String) -> Result<()> {
        let index_key = format!("{}_{}_{}_{}\0", data_type_to_tag(dtype), String::from_utf8_lossy(key), 
                               String::from_utf8_lossy(pattern), cursor);
        
        if let Some(point) = self.scan_cursors_store.get(&index_key) {
            *start_point = point.to_string();
            Ok(())
        } else {
            Err(StorageError::KeyNotFound(index_key))
        }
    }
    
    /// Store scan next point
    pub fn store_scan_next_point(&mut self, dtype: DataType, key: &[u8], pattern: &[u8], cursor: i64, next_point: &str) -> Result<()> {
        let index_key = format!("{}_{}_{}_{}\0", data_type_to_tag(dtype), String::from_utf8_lossy(key), 
                               String::from_utf8_lossy(pattern), cursor);
        
        self.scan_cursors_store.put(index_key, next_point.parse::<u64>().unwrap_or(0));
        Ok(())
    }
    
    /// Set maximum cache statistic keys
    pub fn set_max_cache_statistic_keys(&mut self, max_cache_statistic_keys: usize) -> Result<()> {
        self.statistics_store = LruCache::new(std::num::NonZeroUsize::new(max_cache_statistic_keys).unwrap());
        Ok(())
    }
    
    /// Set small compaction threshold
    pub fn set_small_compaction_threshold(&mut self, small_compaction_threshold: u64) -> Result<()> {
        self.small_compaction_threshold = small_compaction_threshold;
        Ok(())
    }
    
    /// Set small compaction duration threshold
    pub fn set_small_compaction_duration_threshold(&mut self, small_compaction_duration_threshold: u64) -> Result<()> {
        self.small_compaction_duration_threshold = small_compaction_duration_threshold;
        Ok(())
    }
    
    /// Get RocksDB information
    pub fn get_rocksdb_info(&self, info: &mut String, prefix: &str) {
        if let Some(db) = &self.db {
            info.push_str(&format!("#{}RocksDB\r\n", prefix));
            
            // Helper closure to get property and append to info
            let mut write_property = |property: &str, metric: &str| {
                if let Ok(Some(value)) = db.property_value(property) {
                    info.push_str(&format!("{}{}: {}\r\n", prefix, metric, value));
                }
            };
            
            // Memtables num
            write_property("rocksdb.num-immutable-mem-table", "num_immutable_mem_table");
            write_property("rocksdb.num-immutable-mem-table-flushed", "num_immutable_mem_table_flushed");
            write_property("rocksdb.mem-table-flush-pending", "mem_table_flush_pending");
            write_property("rocksdb.num-running-flushes", "num_running_flushes");
            
            // Compaction
            write_property("rocksdb.compaction-pending", "compaction_pending");
            write_property("rocksdb.num-running-compactions", "num_running_compactions");
            
            // Background errors
            write_property("rocksdb.background-errors", "background_errors");
            
            // Memtables size
            write_property("rocksdb.cur-size-active-mem-table", "cur_size_active_mem_table");
            write_property("rocksdb.cur-size-all-mem-tables", "cur_size_all_mem_tables");
            write_property("rocksdb.size-all-mem-tables", "size_all_mem_tables");
            
            // Keys
            write_property("rocksdb.estimate-num-keys", "estimate_num_keys");
            
            // Table readers mem
            write_property("rocksdb.estimate-table-readers-mem", "estimate_table_readers_mem");
            
            // Snapshot
            write_property("rocksdb.num-snapshots", "num_snapshots");
            
            // Version
            write_property("rocksdb.num-live-versions", "num_live_versions");
            write_property("rocksdb.current-super-version-number", "current_super_version_number");
            
            // Live data size
            write_property("rocksdb.estimate-live-data-size", "estimate_live_data_size");
            
            // SST files
            write_property("rocksdb.total-sst-files-size", "total_sst_files_size");
            write_property("rocksdb.live-sst-files-size", "live_sst_files_size");
            
            // Pending compaction bytes
            write_property("rocksdb.estimate-pending-compaction-bytes", "estimate_pending_compaction_bytes");
            
            // Block cache
            write_property("rocksdb.block-cache-capacity", "block_cache_capacity");
            write_property("rocksdb.block-cache-usage", "block_cache_usage");
            write_property("rocksdb.block-cache-pinned-usage", "block_cache_pinned_usage");
            
            // Blob files
            write_property("rocksdb.num-blob-files", "num_blob_files");
            write_property("rocksdb.blob-stats", "blob_stats");
            write_property("rocksdb.total-blob-file-size", "total_blob_file_size");
            write_property("rocksdb.live-blob-file-size", "live_blob_file_size");
        }
    }
    
    /// Scan key numbers
    pub fn scan_key_num(&self) -> Result<Vec<KeyInfo>> {
        let mut key_infos = vec![KeyInfo::default(); 5];
        
        // TODO: Implement scan methods for each data type
        
        Ok(key_infos)
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        if self.need_close.load(Ordering::SeqCst) {
            if let Some(db) = &self.db {
                // Cancel background work
                db.cancel_all_background_work(true);
            }
            
            // Clear handles
            self.handles.clear();
            
            // Close DB
            self.db = None;
        }
    }
}