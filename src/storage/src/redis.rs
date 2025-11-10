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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use chrono::Utc;
use once_cell::sync::OnceCell;

use engine::{Engine, RocksdbEngine};
use foyer::{Cache, CacheBuilder};
use kstd::lock_mgr::LockMgr;
use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, CompactOptions, DB, ReadOptions, WriteOptions,
};
use snafu::{OptionExt, ResultExt};

use crate::base_value_format::{DATA_TYPE_TAG, DataType};
use crate::custom_comparator::{
    lists_data_key_comparator_name, lists_data_key_compare, zsets_score_key_comparator_name,
    zsets_score_key_compare,
};
use crate::data_compaction_filter::DataCompactionFilterFactory;
use crate::error::Error::RedisErr;
use crate::error::InvalidFormatSnafu;
use crate::error::{OptionNoneSnafu, Result, RocksSnafu};
use crate::meta_compaction_filter::MetaCompactionFilterFactory;
use crate::options::{OptionType, StorageOptions};
use crate::statistics::KeyStatistics;
use crate::storage::BgTaskHandler;
use crate::storage_define::TYPE_LENGTH;

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

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            ColumnFamilyIndex::HashesDataCF => Some(DataType::Hash),
            ColumnFamilyIndex::SetsDataCF => Some(DataType::Set),
            ColumnFamilyIndex::ListsDataCF => Some(DataType::List),
            ColumnFamilyIndex::ZsetsDataCF | ColumnFamilyIndex::ZsetsScoreCF => {
                Some(DataType::ZSet)
            }
            ColumnFamilyIndex::MetaCF => None,
        }
    }
}

#[repr(C, align(64))]
pub struct Redis {
    pub index: i32,
    pub need_close: std::sync::atomic::AtomicBool,
    pub lock_mgr: Arc<LockMgr>,

    // For RocksDB
    pub handles: Vec<String>,
    pub write_options: WriteOptions,
    pub read_options: ReadOptions,
    pub compact_options: CompactOptions,
    pub db: Option<Box<dyn Engine>>,

    // For background task
    pub storage: Arc<StorageOptions>,
    pub bg_task_handler: Arc<BgTaskHandler>,

    // For statistics
    pub statistics_store: Arc<Cache<String, KeyStatistics>>,
    pub small_compaction_threshold: AtomicU64,
    pub small_compaction_duration_threshold: AtomicU64,

    // For Scan
    pub scan_cursors_store: Mutex<Cache<String, u64>>,
    pub spop_counts_store: Mutex<Cache<String, u64>>,

    // For raft
    pub is_starting: AtomicBool,
}

impl Redis {
    pub fn new(
        storage: Arc<StorageOptions>,
        index: i32,
        bg_task_handler: Arc<BgTaskHandler>,
        lock_mgr: Arc<LockMgr>,
    ) -> Self {
        let mut compact_options = CompactOptions::default();
        compact_options.set_change_level(true);
        compact_options.set_exclusive_manual_compaction(false);

        let statistics_store: Cache<String, KeyStatistics> =
            CacheBuilder::new(storage.statistics_max_size).build();

        Self {
            index,
            need_close: std::sync::atomic::AtomicBool::new(false),
            is_starting: AtomicBool::new(true),

            storage,
            db: None,
            bg_task_handler,
            lock_mgr,
            handles: Vec::new(),
            write_options: WriteOptions::default(),
            read_options: ReadOptions::default(),
            compact_options,

            statistics_store: Arc::new(statistics_store),
            scan_cursors_store: Mutex::new(CacheBuilder::new(5000).build()),
            spop_counts_store: Mutex::new(CacheBuilder::new(1000).build()),

            small_compaction_threshold: std::sync::atomic::AtomicU64::new(5000),
            small_compaction_duration_threshold: std::sync::atomic::AtomicU64::new(10000),
        }
    }

    // TODO: add raft support
    pub fn open(&mut self, db_path: &str) -> Result<()> {
        self.small_compaction_threshold.store(
            self.storage.small_compaction_threshold as u64,
            std::sync::atomic::Ordering::SeqCst,
        );

        const CF_CONFIGS: &[(&str, bool, Option<usize>)] = &[
            ("default", true, None),                   // meta & string: bloom filter
            ("hash_data_cf", true, None),              // hash: bloom filter
            ("set_data_cf", false, None),              // set: no bloom filter
            ("list_data_cf", true, None),              // list: bloom filter
            ("zset_data_cf", false, Some(16 * 1024)),  // zset data: 16KB block size
            ("zset_score_cf", false, Some(16 * 1024)), // zset score: 16KB block size
        ];
        let db_once_cell = Arc::new(OnceCell::new());
        let column_families: Vec<ColumnFamilyDescriptor> = CF_CONFIGS
            .iter()
            .map(|(name, use_bloom, block_size)| {
                Self::create_cf_options(
                    &self.storage,
                    name,
                    *use_bloom,
                    *block_size,
                    Some(&db_once_cell),
                )
            })
            .collect();

        let db = DB::open_cf_descriptors(&self.storage.options, db_path, column_families)
            .context(RocksSnafu)?;
        let engine = RocksdbEngine::new(db);
        let db_arc = engine.shared_db();
        let _ = db_once_cell.set(Arc::clone(&db_arc));

        self.handles = CF_CONFIGS
            .iter()
            .filter(|(name, _, _)| engine.cf_handle(name).is_some())
            .map(|(name, _, _)| name.to_string())
            .collect();
        self.db = Some(Box::new(engine));
        self.is_starting.store(false, Ordering::SeqCst);

        Ok(())
    }

    // Helper function: create column-family options
    fn create_cf_options(
        storage_options: &StorageOptions,
        cf_name: &str,
        use_bloom_filter: bool,
        block_size: Option<usize>,
        db_once_cell: Option<&Arc<OnceCell<Arc<DB>>>>,
    ) -> ColumnFamilyDescriptor {
        let mut cf_opts = storage_options.options.clone();
        let mut table_opts = BlockBasedOptions::default();

        // Set comparator
        if cf_name == ColumnFamilyIndex::ListsDataCF.name() {
            cf_opts.set_comparator(
                lists_data_key_comparator_name(),
                Box::new(lists_data_key_compare),
            );
        } else if cf_name == ColumnFamilyIndex::ZsetsScoreCF.name() {
            cf_opts.set_comparator(
                zsets_score_key_comparator_name(),
                Box::new(zsets_score_key_compare),
            );
        }

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

        // Set table factory
        cf_opts.set_block_based_table_factory(&table_opts);

        // Set compaction filter factory
        if cf_name == ColumnFamilyIndex::MetaCF.name() {
            cf_opts.set_compaction_filter_factory(MetaCompactionFilterFactory);
        } else if let Some(db_once_cell) = db_once_cell {
            if let Some(data_type) = [
                ColumnFamilyIndex::HashesDataCF,
                ColumnFamilyIndex::SetsDataCF,
                ColumnFamilyIndex::ListsDataCF,
                ColumnFamilyIndex::ZsetsDataCF,
                ColumnFamilyIndex::ZsetsScoreCF,
            ]
            .iter()
            .find(|cf| cf.name() == cf_name)
            .and_then(|cf| cf.data_type())
            {
                let factory = DataCompactionFilterFactory::new(Arc::clone(db_once_cell), data_type);
                cf_opts.set_compaction_filter_factory(factory);
            }
        }

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
            for (i, cf_name) in self.handles.iter().enumerate() {
                if i > 0 {
                    // Skip already compacted default CF
                    if let Some(cf) = db.cf_handle(cf_name) {
                        db.compact_range_cf(&cf, begin, end);
                    }
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

        OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        }
        .fail()
    }

    /// Get column-family handle
    pub fn get_cf_handle(
        &self,
        cf_index: ColumnFamilyIndex,
    ) -> Option<Arc<rocksdb::BoundColumnFamily<'_>>> {
        if let Some(db) = &self.db {
            if let Some(cf_name) = self.handles.get(cf_index as usize) {
                return db.cf_handle(cf_name);
            }
        }
        None
    }

    pub fn update_specific_key_duration(
        &self,
        dtype: DataType,
        key: &str,
        duration: u64,
    ) -> Result<()> {
        let threshold = self
            .small_compaction_duration_threshold
            .load(Ordering::SeqCst);

        if duration != 0 && threshold != 0 {
            let mut lookup_key = String::new();
            lookup_key.push(DATA_TYPE_TAG[dtype as usize]);
            lookup_key.push_str(key);

            let mut data = self
                .statistics_store
                .get(&lookup_key)
                .map(|entry| entry.value().clone())
                .unwrap_or_else(|| KeyStatistics::new(10));
            data.add_duration(duration);

            let modify_count = data.modify_count();
            let avg_duration = data.avg_duration();

            self.statistics_store.insert(lookup_key.clone(), data);
            self.add_compact_key_task_if_needed(dtype, key, modify_count, avg_duration)?;
        }

        Ok(())
    }

    pub fn update_specific_key_statistics(
        &self,
        dtype: DataType,
        key: &str,
        count: u64,
    ) -> Result<()> {
        let threshold = self.small_compaction_threshold.load(Ordering::SeqCst);

        if count != 0 && threshold != 0 {
            let mut lookup_key = String::new();
            lookup_key.push(DATA_TYPE_TAG[dtype as usize]);
            lookup_key.push_str(key);

            let mut data = self
                .statistics_store
                .get(&lookup_key)
                .map(|entry| entry.value().clone())
                .unwrap_or_else(|| KeyStatistics::new(10));
            data.add_modify_count(count);

            let modify_count = data.modify_count();
            let avg_duration = data.avg_duration();

            self.statistics_store.insert(lookup_key.clone(), data);
            self.add_compact_key_task_if_needed(dtype, key, modify_count, avg_duration)?;
        }

        Ok(())
    }

    pub fn add_compact_key_task_if_needed(
        &self,
        dtype: DataType,
        key: &str,
        total: u64,
        duration: u64,
    ) -> Result<()> {
        let threshold = self.small_compaction_threshold.load(Ordering::SeqCst);
        let duration_threshold = self
            .small_compaction_duration_threshold
            .load(Ordering::SeqCst);

        if total < threshold || duration < duration_threshold {
            return Ok(());
        }

        let mut lookup_key = String::new();
        lookup_key.push(DATA_TYPE_TAG[dtype as usize]);
        lookup_key.push_str(key);

        self.statistics_store.remove(&lookup_key);

        // send background compact task
        let key = key.to_string();
        let bg_task_handler = self.bg_task_handler.clone();
        tokio::spawn(async move {
            let _ = bg_task_handler
                .send(crate::storage::BgTask::CompactSpecificKey { dtype, key })
                .await;
        });

        Ok(())
    }

    pub fn set_option(
        &self,
        option_type: OptionType,
        options: &HashMap<String, String>,
    ) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let opts_vec: Vec<_> = options
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        match option_type {
            OptionType::DB => {
                db.set_options(&opts_vec).context(RocksSnafu)?;
            }
            OptionType::ColumnFamily => {
                if self.handles.is_empty() {
                    let cf = db.cf_handle("default").context(OptionNoneSnafu {
                        message: "Column family not init".to_string(),
                    })?;
                    db.set_options_cf(&cf, &opts_vec).context(RocksSnafu)?;
                } else {
                    for cf_name in &self.handles {
                        let cf = db.cf_handle(cf_name).context(OptionNoneSnafu {
                            message: format!("Column family {cf_name} not found"),
                        })?;
                        db.set_options_cf(&cf, &opts_vec).context(RocksSnafu)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn check_type(&self, key: &[u8], key_type: DataType) -> Result<()> {
        if key.is_empty() {
            return Ok(());
        }

        if key.first().copied() == Some(key_type as u8) {
            return Ok(());
        }

        Err(RedisErr {
            message: "WRONGTYPE Operation against a key holding the wrong kind of value"
                .to_string(),
            location: Default::default(),
        })
    }

    /// check if the encoded value of any type is expired (type-agnostic)
    ///
    /// This function can check the expired status without parsing the value.
    ///
    /// The etime field is in the last 8 bytes of the value format of all types:
    /// - String: | type(1B) | value | reserve(16B) | ctime(8B) | etime(8B) |
    /// - Hash/Set/ZSet: | type(1B) | count(8B) | version(8B) | reserve(16B) | ctime(8B) | etime(8B) |
    /// - List: | type(1B) | count(8B) | version(8B) | left(8B) | right(8B) | reserve(16B) | ctime(8B) | etime(8B) |
    ///
    /// # Arguments
    /// * `val_raw` - the raw value bytes
    ///
    /// # Returns
    /// * `Ok(true)` - the value is expired or the count is 0
    /// * `Ok(false)` - the value is not expired and is valid
    /// * `Err(_)` - parsing error
    pub fn is_stale(&self, val_raw: &[u8]) -> Result<bool> {
        if val_raw.is_empty() {
            return Ok(false);
        }

        let data_type = DataType::try_from(val_raw[0])?;
        if val_raw.len() < data_type.min_meta_raw_len()? {
            return InvalidFormatSnafu {
                message: format!("Invalid value length for data type: {data_type:?}"),
            }
            .fail();
        }

        let now = Utc::now().timestamp_micros() as u64;
        match data_type {
            DataType::String => {
                // | type(1B) | value | reserve(16B) | ctime(8B) | etime(8B) |
                let etime_offset = val_raw.len() - 8;
                let etime_bytes = &val_raw[etime_offset..etime_offset + 8];
                let etime = u64::from_le_bytes(etime_bytes.try_into().map_err(|_| RedisErr {
                    message: "Failed to read etime".to_string(),
                    location: Default::default(),
                })?);

                if etime == 0 {
                    return Ok(false);
                }
                Ok(etime < now)
            }
            DataType::Hash | DataType::Set | DataType::ZSet => {
                // | type(1B) | count(8B) | version(8B) | reserve(16B) | ctime(8B) | etime(8B) |
                let count_offset = TYPE_LENGTH;
                let count_bytes = &val_raw[count_offset..count_offset + 8];
                let count = u64::from_le_bytes(count_bytes.try_into().map_err(|_| RedisErr {
                    message: "Failed to read count".to_string(),
                    location: Default::default(),
                })?);

                if count == 0 {
                    return Ok(true);
                }

                let etime_offset = val_raw.len() - 8;
                let etime_bytes = &val_raw[etime_offset..etime_offset + 8];
                let etime = u64::from_le_bytes(etime_bytes.try_into().map_err(|_| RedisErr {
                    message: "Failed to read etime".to_string(),
                    location: Default::default(),
                })?);

                if etime == 0 {
                    return Ok(false);
                }
                Ok(etime < now)
            }
            DataType::List => {
                // | type(1B) | count(8B) | version(8B) | left(8B) | right(8B) | reserve(16B) | ctime(8B) | etime(8B) |
                let count_offset = TYPE_LENGTH;
                let count_bytes = &val_raw[count_offset..count_offset + 8];
                let count = u64::from_le_bytes(count_bytes.try_into().map_err(|_| RedisErr {
                    message: "Failed to read count".to_string(),
                    location: Default::default(),
                })?);

                if count == 0 {
                    return Ok(true);
                }

                let etime_offset = val_raw.len() - 8;
                let etime_bytes = &val_raw[etime_offset..etime_offset + 8];
                let etime = u64::from_le_bytes(etime_bytes.try_into().map_err(|_| RedisErr {
                    message: "Failed to read etime".to_string(),
                    location: Default::default(),
                })?);

                if etime == 0 {
                    return Ok(false);
                }
                Ok(etime < now)
            }
            _ => InvalidFormatSnafu {
                message: format!(
                    "data type: {data_type:?} should not be used as meta value: {val_raw:?}"
                ),
            }
            .fail(),
        }
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        if self.need_close.load(std::sync::atomic::Ordering::SeqCst) {
            // Clear handles
            self.handles.clear();

            // Close database
            self.db = None;
        }
    }
}

/// Retrieves the database reference and the specified column family handles.
///
/// # Parameters
/// - `$self`: The receiver, typically a reference to a `Redis` instance.
/// - `$cf`: One or more column family identifiers to retrieve handles for.
///
/// # Returns
/// Returns a tuple containing:
/// - A reference to the database (`db`).
/// - A vector of column family handles corresponding to the provided identifiers.
///
/// # Errors
/// Returns an error if the database is not initialized or if any column family handle is not found.
#[macro_export]
macro_rules! get_db_and_cfs {
    ($self:expr $(, $cf:expr)*) => {{
        let db = $self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cfs = vec![
            $(
                $self
                    .get_cf_handle($cf)
                    .context(OptionNoneSnafu {
                        message: format!("{:?} cf handle not found", $cf),
                    })?
            ),*
        ];

        (db, cfs)
    }};
}
