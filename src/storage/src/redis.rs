//  Copyright 2024 The Kiwi-rs Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::error::{Result, RocksSnafu, UnknownSnafu};
use crate::object_pool::{BufferPool, WriteBatchPool};
use crate::options::StorageOptions;
use rocksdb::{
    BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, CompactOptions, ReadOptions,
    WriteOptions, DB,
};
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnFamilyIndex {
    MetaCF = 0,       // meta & string
    HashesDataCF = 1, // hash data
    SetsDataCF = 2,   // set data
    ListsDataCF = 3,  // list data
    ZsetsDataCF = 4,  // zset data
    ZsetsScoreCF = 5, // zset score
}

impl ColumnFamilyIndex {
    pub fn name(&self) -> &'static str {
        match self {
            ColumnFamilyIndex::MetaCF => "default",
            ColumnFamilyIndex::HashesDataCF => "hash_data_cf",
            ColumnFamilyIndex::SetsDataCF => "set_data_cf",
            ColumnFamilyIndex::ListsDataCF => "list_data_cf",
            ColumnFamilyIndex::ZsetsDataCF => "zset_data_cf",
            ColumnFamilyIndex::ZsetsScoreCF => "zset_score_cf",
        }
    }
}

/// 简化的键统计结构
#[derive(Debug, Clone)]
pub struct KeyStatistics {
    pub modify_count: u64,
    pub avg_duration: u64,
}

impl Default for KeyStatistics {
    fn default() -> Self {
        Self {
            modify_count: 0,
            avg_duration: 0,
        }
    }
}

#[repr(C, align(64))]
pub struct Redis {
    pub index: i32,
    pub need_close: std::sync::atomic::AtomicBool,
    pub handles: Vec<ColumnFamily>,
    pub write_options: WriteOptions,
    pub read_options: ReadOptions,
    pub compact_options: CompactOptions,
    pub storage: Arc<StorageOptions>,
    pub db: Option<DB>,

    // For statistics
    pub statistics_store: crate::lru_cache::LRUCache<String, KeyStatistics>,
    pub small_compaction_threshold: u64,
    pub small_compaction_duration_threshold: u64,

    // For performance optimization
    pub buffer_pool: Arc<BufferPool>,
    pub batch_pool: Arc<WriteBatchPool>,

    // For Scan
    pub scan_cursors_store: crate::lru_cache::LRUCache<String, u64>,
    pub spop_counts_store: crate::lru_cache::LRUCache<String, u64>,

    // For raft
    pub is_starting: bool,
}

impl Redis {
    pub fn new(storage: Arc<StorageOptions>, index: i32) -> Self {
        let mut compact_options = CompactOptions::default();
        compact_options.set_change_level(true);
        compact_options.set_exclusive_manual_compaction(false);

        let redis = Self {
            index,
            need_close: std::sync::atomic::AtomicBool::new(false),
            is_starting: true,

            storage,
            db: None,

            handles: Vec::new(),
            write_options: WriteOptions::default(),
            read_options: ReadOptions::default(),
            compact_options,

            buffer_pool: Arc::new(BufferPool::new()),
            batch_pool: Arc::new(WriteBatchPool::new(50)),

            statistics_store: crate::lru_cache::LRUCache::with_capacity(10000),
            scan_cursors_store: crate::lru_cache::LRUCache::with_capacity(5000),
            spop_counts_store: crate::lru_cache::LRUCache::with_capacity(1000),

            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,
        };

        redis
    }

    // TODO: add raft support
    pub fn open(&mut self, db_path: &str) -> Result<()> {
        self.small_compaction_threshold = self.storage.small_compaction_threshold as u64;
        self.statistics_store
            .set_capacity(self.storage.statistics_max_size);

        const CF_CONFIGS: &[(&str, bool, Option<usize>)] = &[
            ("default", true, None),                   // meta & string: bloom filter
            ("hash_data_cf", true, None),              // hash: bloom filter
            ("list_data_cf", true, None),              // list: bloom filter
            ("set_data_cf", false, None),              // set: no bloom filter
            ("zset_data_cf", false, Some(16 * 1024)),  // zset data: 16KB block size
            ("zset_score_cf", false, Some(16 * 1024)), // zset score: 16KB block size
        ];

        let column_families: Vec<ColumnFamilyDescriptor> = CF_CONFIGS
            .iter()
            .map(|(name, use_bloom, block_size)| {
                Self::create_cf_options(&self.storage, name, *use_bloom, *block_size)
            })
            .collect();

        self.db = Some(
            DB::open_cf_descriptors(&self.storage.options, db_path, column_families)
                .context(RocksSnafu)?,
        );

        if let Some(db) = &self.db {
            let mut handles = Vec::new();
            for (name, _, _) in CF_CONFIGS {
                if let Some(cf) = db.cf_handle(name) {
                    // Use unsafe to obtain ownership of ColumnFamily
                    // This is safe because the lifetime of ColumnFamily is consistent with that of DB
                    unsafe {
                        handles.push(std::mem::transmute_copy(&cf));
                    }
                }
            }
            self.handles = handles;
        }

        self.is_starting = false;

        Ok(())
    }

    // Helper function: create column-family options
    fn create_cf_options(
        storage_options: &StorageOptions,
        cf_name: &str,
        use_bloom_filter: bool,
        block_size: Option<usize>,
    ) -> ColumnFamilyDescriptor {
        let mut cf_opts = storage_options.options.clone();
        let mut table_opts = BlockBasedOptions::default();

        // Set bloom filter
        if use_bloom_filter {
            table_opts.set_bloom_filter(10.0, true);
        }

        // Set block size
        if let Some(size) = block_size {
            table_opts.set_block_size(size);
        }

        // Set block cache
        if !storage_options.share_block_cache && storage_options.block_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(storage_options.block_cache_size);
            table_opts.set_block_cache(&cache);
        }

        cf_opts.set_block_based_table_factory(&table_opts);
        ColumnFamilyDescriptor::new(cf_name, cf_opts)
    }

    /// Get database index
    pub fn get_index(&self) -> i32 {
        self.index
    }

    /// Set whether to close the database
    pub fn set_need_close(&self, need_close: bool) {
        self.need_close
            .store(need_close, std::sync::atomic::Ordering::SeqCst);
    }

    /// Compact database range
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        if let Some(db) = &self.db {
            // Compact default column-family
            db.compact_range(begin, end);

            // Compact other column-families
            for (i, handle) in self.handles.iter().enumerate() {
                if i > 0 {
                    // Skip already compacted default CF
                    db.compact_range_cf(handle, begin, end);
                }
            }
        }

        Ok(())
    }

    pub fn get_property(&self, property: &str) -> Result<u64> {
        if let Some(db) = &self.db {
            if let Some(value) = db.property_int_value(property).context(RocksSnafu)? {
                return Ok(value);
            }
        }

        UnknownSnafu {
            message: format!("Property {} not found", property),
        }
        .fail()
    }

    // /// 零拷贝字符串操作 - 返回字节切片
    // pub fn get_bytes(&self, key: &str) -> Result<Option<&[u8]>> {
    //     let cf = self.get_cf_handle(ColumnFamilyIndex::MetaCF)?;

    //     let value = self
    //         .db
    //         .as_ref()
    //         .ok_or(Error::InvalidFormat {
    //             message: "Database not open".to_string(),
    //         })?
    //         .get_cf_opt(cf, key.as_bytes(), &self.read_options)?;

    //     match value {
    //         Some(data) => {
    //             let (value_bytes, ttl, _dtype) = ValueUtils::parse_value_bytes(&data)?;

    //             // 检查是否过期
    //             if ValueUtils::is_expired(ttl) {
    //                 return Ok(None);
    //             }

    //             Ok(Some(value_bytes))
    //         }
    //         None => Ok(None),
    //     }
    // }

    // /// 零拷贝字符串设置
    // pub fn set_bytes(&self, key: &str, value: &[u8], ttl: Option<u64>) -> Result<()> {
    //     let cf = self.get_cf_handle(ColumnFamilyIndex::MetaCF)?;
    //     let encoded_value = ValueUtils::format_value_bytes(value, ttl, DataType::String)?;

    //     self.db
    //         .as_ref()
    //         .ok_or(Error::InvalidFormat {
    //             message: "Database not open".to_string(),
    //         })?
    //         .put_cf_opt(cf, key.as_bytes(), &encoded_value, &self.write_options)?;

    //     Ok(())
    // }

    /// Get column-family handle
    fn get_cf_handle(&self, cf_index: ColumnFamilyIndex) -> Option<&ColumnFamily> {
        self.handles.get(cf_index as usize)
    }

    /// Acquire buffer from object pool
    pub fn acquire_buffer(&self, size: usize) -> Vec<u8> {
        self.buffer_pool.acquire_buffer(size)
    }

    /// Release buffer to object pool
    pub fn release_buffer(&self, buffer: Vec<u8>) {
        self.buffer_pool.release_buffer(buffer);
    }

    /// Acquire WriteBatch from object pool
    pub fn acquire_batch(&self) -> rocksdb::WriteBatch {
        self.batch_pool.acquire()
    }

    /// Release WriteBatch to object pool
    pub fn release_batch(&self, batch: rocksdb::WriteBatch) {
        self.batch_pool.release(batch);
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        if self.need_close.load(std::sync::atomic::Ordering::SeqCst) {
            if let Some(db) = &self.db {
                // Cancel background work
                db.cancel_all_background_work(true);
            }

            // Clear handles
            self.handles.clear();

            // Close database
            self.db = None;
        }
    }
}
