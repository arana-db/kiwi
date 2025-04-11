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

use std::collections::VecDeque;
use std::f64;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicI32},
};

use crate::storage::base_data_value_format::DataType;
use crate::storage::lru_cache::LRUCache;
use crate::storage::options::StorageOptions;
use crate::storage::redis::Redis;
use crate::storage::slot_indexer::SlotIndexer;
use crate::storage::util;

// Constant definitions
pub const ZSET_SCORE_MAX: f64 = f64::MAX;
pub const ZSET_SCORE_MIN: f64 = f64::MIN;

pub const PROPERTY_TYPE_ROCKSDB_CUR_SIZE_ALL_MEM_TABLES: &str = "rocksdb.cur-size-all-mem-tables";
pub const PROPERTY_TYPE_ROCKSDB_ESTIMATE_TABLE_READER_MEM: &str =
    "rocksdb.estimate-table-readers-mem";
pub const PROPERTY_TYPE_ROCKSDB_BACKGROUND_ERRORS: &str = "rocksdb.background-errors";

pub const BATCH_DELETE_LIMIT: usize = 100;
pub const COMPACT_THRESHOLD_COUNT: usize = 2000;

pub const NO_FLUSH: u64 = u64::MAX;
pub const FLUSH: u64 = 0;

// Type aliases
pub type Status = Result<(), String>;
pub type LogIndex = i64;
pub type AppendLogFunction =
    Arc<dyn Fn(Binlog, Pin<Box<dyn Future<Output = Status> + Send>>) + Send + Sync>;
pub type DoSnapshotFunction = Arc<dyn Fn(LogIndex, bool) + Send + Sync>;

// Struct definitions
pub struct Binlog {
    // Implementation of Binlog struct
    // This needs to be completed based on the specific implementation of kiwi::Binlog
}

pub struct KeyValue {
    pub key: String,
    pub value: String,
}

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl PartialOrd for KeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

pub struct KeyInfo {
    pub keys: u64,
    pub expires: u64,
    pub avg_ttl: u64,
    pub invalid_keys: u64,
}

impl Default for KeyInfo {
    fn default() -> Self {
        Self {
            keys: 0,
            expires: 0,
            avg_ttl: 0,
            invalid_keys: 0,
        }
    }
}

impl KeyInfo {
    pub fn new(k: u64, e: u64, a: u64, i: u64) -> Self {
        Self {
            keys: k,
            expires: e,
            avg_ttl: a,
            invalid_keys: i,
        }
    }

    pub fn add(&self, info: &KeyInfo) -> KeyInfo {
        KeyInfo {
            keys: self.keys + info.keys,
            expires: self.expires + info.expires,
            avg_ttl: self.avg_ttl + info.avg_ttl,
            invalid_keys: self.invalid_keys + info.invalid_keys,
        }
    }
}

pub struct ValueStatus {
    pub value: String,
    pub status: Status,
    pub ttl: i64,
}

impl PartialEq for ValueStatus {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.status == other.status && self.ttl == other.ttl
    }
}

pub struct FieldValue {
    pub field: String,
    pub value: String,
}

impl Default for FieldValue {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldValue {
    pub fn new() -> Self {
        Self {
            field: String::new(),
            value: String::new(),
        }
    }

    pub fn with_values(field: String, value: String) -> Self {
        Self { field, value }
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        self.field == other.field && self.value == other.value
    }
}

pub struct KeyVersion {
    pub key: String,
    pub version: u64,
}

impl PartialEq for KeyVersion {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.version == other.version
    }
}

pub struct ScoreMember {
    pub score: f64,
    pub member: String,
}

impl Default for ScoreMember {
    fn default() -> Self {
        Self::new()
    }
}

impl ScoreMember {
    pub fn new() -> Self {
        Self {
            score: 0.0,
            member: String::new(),
        }
    }

    pub fn with_values(score: f64, member: String) -> Self {
        Self { score, member }
    }
}

impl PartialEq for ScoreMember {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.member == other.member
    }
}

// Enum definitions
pub enum BeforeOrAfter {
    Before,
    After,
}

pub enum OptionType {
    DB,
    ColumnFamily,
}

pub enum ColumnFamilyType {
    Meta,
    Data,
    MetaAndData,
}

pub enum AGGREGATE {
    SUM,
    MIN,
    MAX,
}

pub enum BitOpType {
    BitOpAnd = 1,
    BitOpOr,
    BitOpXor,
    BitOpNot,
    BitOpDefault,
}

pub enum Operation {
    None = 0,
    CleanAll,
    CompactRange,
}

pub struct BGTask {
    pub type_: DataType,
    pub operation: Operation,
    pub argv: Vec<String>,
}

impl Default for BGTask {
    fn default() -> Self {
        Self::new()
    }
}

impl BGTask {
    pub fn new() -> Self {
        Self {
            type_: DataType::All,
            operation: Operation::None,
            argv: Vec::new(),
        }
    }

    pub fn with_values(type_: DataType, operation: Operation, argv: Vec<String>) -> Self {
        Self {
            type_,
            operation,
            argv,
        }
    }
}

// Rust implementation of Storage class
pub struct Storage {
    insts: Vec<Box<Redis>>,
    slot_indexer: SlotIndexer,
    is_opened: AtomicBool,

    cursors_store: LRUCache<String, String>,

    // Background task related
    bg_tasks_mutex: Mutex<()>,
    bg_tasks_queue: Mutex<VecDeque<BGTask>>,
    bg_tasks_should_exit: AtomicBool,

    current_task_type: AtomicI32,

    // Key scanning related
    scan_keynum_exit: AtomicBool,
    db_instance_num: usize,
    db_id: i32,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    pub fn new() -> Self {
        Self {
            insts: Vec::new(),
            slot_indexer: SlotIndexer::default(),
            is_opened: AtomicBool::new(false),

            cursors_store: LRUCache::with_capacity(5000),

            bg_tasks_mutex: Mutex::new(()),
            bg_tasks_queue: Mutex::new(VecDeque::new()),
            bg_tasks_should_exit: AtomicBool::new(false),

            current_task_type: AtomicI32::new(0), // kNone

            scan_keynum_exit: AtomicBool::new(false),
            db_instance_num: 3,
            db_id: 0,
        }
    }

    pub fn open(&mut self, storage_options: &mut StorageOptions, db_path: &str) -> Status {
        // Implementation of storage open logic
        if let Err(err) = util::mkdir_with_path(db_path, 0o755) {
            return Err(format!("Failed to create db directory: {}", err));
        }

        self.db_instance_num = storage_options.db_instance_num;

        // TODO: allow_stall should configure.
        let write_buffer_manager = rocksdb::WriteBufferManager::new_write_buffer_manager(
            storage_options.mem_manager_size,
            true,
        );

        storage_options
            .options
            .set_write_buffer_manager(&write_buffer_manager);

        for index in 0..self.db_instance_num {
            // insts.add(Redis::new(&self, index as i32))
            // status s = insts.open(storage_options)
            // if !s.ok() {
            //     ERROR(err message);
            //     return Err(err);
            // }
            // INFO("open Rocksdb{} success", index);
        }

        self.slot_indexer = SlotIndexer::new(self.db_instance_num);
        self.db_id = self.db_id;
        self.is_opened.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn close(&mut self) -> Status {
        // Implementation of storage close logic
        Ok(())
    }

    // Below are the Rust implementations of various methods in the Storage class
    // Due to the large number of methods, only some examples are listed here, and the rest can be implemented following the same pattern

    // Strings Commands

    // Set key to hold the string value. if key
    // already holds a value, it is overwritten
    pub fn set(&self, key: &[u8], value: &[u8]) -> Status {
        // Implementation of key-value set logic
        Ok(())
    }

    // Set key to hold the string value. if key exist
    pub fn setxx(&self, key: &[u8], value: &[u8], ret: &mut i32, ttl: i64) -> Status {
        // Implementation of setxx logic
        Ok(())
    }

    // Get the value of key. If the key does not exist
    // the special value nil is returned
    pub fn get(&self, key: &[u8], value: &mut String) -> Status {
        // Implementation of key-value get logic
        Ok(())
    }

    // Get the value and ttl of key. If the key does not exist
    // the special value nil is returned. If the key has no ttl, ttl is -1
    pub fn get_with_ttl(&self, key: &[u8], value: &mut String, ttl: &mut i64) -> Status {
        // Implementation of key-value get with TTL logic
        Ok(())
    }

    // Other methods can be implemented following the same pattern

    // Background task related methods
    pub fn start_bg_thread(&self) -> Status {
        // Implementation of background thread start logic
        Ok(())
    }

    pub fn run_bg_task(&self) -> Status {
        // Implementation of background task execution logic
        Ok(())
    }

    pub fn add_bg_task(&self, bg_task: BGTask) -> Status {
        // Implementation of background task addition logic
        Ok(())
    }

    // Get current task type
    pub fn get_current_task_type(&self) -> String {
        // Implementation of current task type retrieval logic
        String::new()
    }

    // Get minimum flushed log index
    pub fn get_smallest_flushed_log_index(&self) -> LogIndex {
        // Implementation of minimum flushed log index retrieval logic
        0
    }
}
