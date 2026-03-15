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

//! RocksDB 实现的 Raft 日志存储
//!
//! 此模块提供了基于 RocksDB 的持久化 Raft 日志存储实现，用于替代内存中的 BTreeMap 实现。
//! 它实现了 openraft 的 `RaftLogStorage` 和 `RaftLogReader` trait，提供生产级的日志管理能力。
//!
//! 使用三个 RocksDB 列族（Column Family）来组织不同类型的数据：
//!
//! - **logs_cf**: 存储日志条目（Entry）
//!   - Key: 日志索引（u64，big-endian 编码）
//!   - Value: 序列化的 Entry<KiwiTypeConfig>
//!
//! - **meta_cf**: 存储元数据
//!   - Key: "last_purged_log_id"
//!   - Value: 序列化的 Option<LogId<u64>>
//!
//! - **state_cf**: 存储 Raft 状态
//!   - Key: "vote" 或 "committed"
//!   - Value: 序列化的 Vote<u64> 或 LogId<u64>

#![allow(clippy::result_large_err)]

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use openraft::StorageError;

use serde::Serialize;
use serde::de::DeserializeOwned;

use conf::raft_type::KiwiTypeConfig;
use engine::{Engine, RocksdbEngine};

use rocksdb::{ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch};

const LOGS_CF: &str = "logs";
const META_CF: &str = "meta";
const STATE_CF: &str = "state";

const LAST_PURGED_KEY: &[u8] = b"last_purged_log_id";
const VOTE_KEY: &[u8] = b"vote";
const COMMITTED_KEY: &[u8] = b"committed";

#[derive(Clone)]
pub struct RocksdbLogStore {
    engine: Arc<dyn Engine>,
}

impl RocksdbLogStore {
    /// 获取列族句柄，若不存在则返回写操作错误
    fn cf_write(
        &self,
        name: &str,
    ) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, StorageError<u64>> {
        self.engine
            .cf_handle(name)
            .ok_or_else(|| cf_not_found_write(name))
    }

    /// 获取列族句柄，若不存在则返回读操作错误
    fn cf_read(
        &self,
        name: &str,
    ) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, StorageError<u64>> {
        self.engine
            .cf_handle(name)
            .ok_or_else(|| cf_not_found_read(name))
    }

    /// 将日志条目序列化为 WriteBatch
    fn build_append_batch<I>(&self, entries: I) -> Result<WriteBatch, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<KiwiTypeConfig>>,
    {
        // 获取 logs_cf 列族句柄
        let logs_cf = self.cf_write(LOGS_CF)?;

        // 创建 WriteBatch 并序列化每个日志条目
        let mut batch = WriteBatch::default();
        for entry in entries {
            let key = encode_log_key(entry.log_id.index);
            let value = serialize(&entry)?;
            batch.put_cf(&logs_cf, key, &value);
        }

        Ok(batch)
    }

    /// 追加日志条目到存储
    async fn do_append<I>(
        &self,
        entries: I,
        callback: openraft::storage::LogFlushed<KiwiTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<KiwiTypeConfig>>,
    {
        // 构建批量写入，序列化逻辑集中在 build_append_batch 中
        let batch = self.build_append_batch(entries)?;

        // 执行批量写入，先捕获结果再通知回调，确保 callback 无论成功失败都被调用
        let write_result = self.engine.write(batch).map_err(io_write_err);

        // 通知回调写入结果（openraft 要求 callback 必须被调用，否则内部状态会挂住）
        callback.log_io_completed(
            write_result
                .as_ref()
                .map(|_| ())
                .map_err(|e| std::io::Error::other(e.to_string())),
        );

        write_result?;
        Ok(())
    }

    /// 读取指定范围的日志条目
    async fn do_try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug>(
        &self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<KiwiTypeConfig>>, StorageError<u64>> {
        // 获取 logs_cf 列族句柄
        let logs_cf = self.cf_read(LOGS_CF)?;

        // 确定起始键
        let start_key = match range.start_bound() {
            Bound::Included(&idx) => encode_log_key(idx),
            Bound::Excluded(&idx) => match idx.checked_add(1) {
                Some(next_idx) => encode_log_key(next_idx),
                None => {
                    // Excluded(u64::MAX) 表示空范围，直接返回空结果
                    return Ok(Vec::new());
                }
            },
            Bound::Unbounded => encode_log_key(0),
        };

        // 创建迭代器从起始键开始
        let iter = self
            .engine
            .iterator_cf(&logs_cf, IteratorMode::From(&start_key, Direction::Forward));

        let mut entries = Vec::new();

        // 遍历迭代器并收集符合范围的条目
        for item in iter {
            let (key, value) = item.map_err(io_read_err)?;

            // 解码键获取索引
            let index = decode_log_key(&key)?;

            // 检查是否在范围内
            if !range.contains(&index) {
                break;
            }

            // 反序列化日志条目
            let entry: openraft::Entry<KiwiTypeConfig> = deserialize(&value)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// 保存投票信息到持久化存储
    async fn do_save_vote(&self, vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        // 获取 state_cf 列族句柄
        let state_cf = self.cf_write(STATE_CF)?;

        // 序列化投票信息
        let value = serialize(vote)?;

        // 写入到 RocksDB
        self.engine
            .put_cf(&state_cf, VOTE_KEY, &value)
            .map_err(io_write_err)?;

        Ok(())
    }

    /// 从持久化存储读取投票信息
    async fn do_read_vote(&self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        // 获取 state_cf 列族句柄
        let state_cf = self.cf_read(STATE_CF)?;

        // 从 RocksDB 读取
        let value = self
            .engine
            .get_cf(&state_cf, VOTE_KEY)
            .map_err(io_read_err)?;

        // 如果不存在，返回 None
        let Some(bytes) = value else {
            return Ok(None);
        };

        // 反序列化投票信息
        let vote: openraft::Vote<u64> = deserialize(&bytes)?;
        Ok(Some(vote))
    }

    /// 保存已提交日志 ID 到持久化存储
    async fn do_save_committed(
        &self,
        committed: Option<openraft::LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        // 获取 state_cf 列族句柄
        let state_cf = self.cf_write(STATE_CF)?;

        // 序列化已提交日志 ID
        let value = serialize(&committed)?;

        // 写入到 RocksDB
        self.engine
            .put_cf(&state_cf, COMMITTED_KEY, &value)
            .map_err(io_write_err)?;

        Ok(())
    }

    /// 从持久化存储读取已提交日志 ID
    async fn do_read_committed(&self) -> Result<Option<openraft::LogId<u64>>, StorageError<u64>> {
        // 获取 state_cf 列族句柄
        let state_cf = self.cf_read(STATE_CF)?;

        // 从 RocksDB 读取
        let value = self
            .engine
            .get_cf(&state_cf, COMMITTED_KEY)
            .map_err(io_read_err)?;

        // 如果不存在，返回 None
        let Some(bytes) = value else {
            return Ok(None);
        };

        // 反序列化已提交日志 ID
        let committed: Option<openraft::LogId<u64>> = deserialize(&bytes)?;
        Ok(committed)
    }

    /// 截断指定索引及之后的所有日志条目
    async fn do_truncate(&self, log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        // 获取 logs_cf 列族句柄
        let logs_cf = self.cf_write(LOGS_CF)?;

        // 删除从 log_id.index 开始的所有日志条目
        let start_key = encode_log_key(log_id.index);
        let end_key = encode_log_key(u64::MAX);

        // 创建 WriteBatch 用于批量删除
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&logs_cf, &start_key, &end_key);
        batch.delete_cf(&logs_cf, end_key);
        self.engine.write(batch).map_err(io_write_err)?;

        Ok(())
    }

    /// 清理指定索引及之前的所有日志条目
    async fn do_purge(&self, log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        let logs_cf = self.cf_write(LOGS_CF)?;
        let meta_cf = self.cf_write(META_CF)?;

        let start_key = encode_log_key(0);
        // delete_range_cf 是左闭右开区间，所以结束键用 index + 1
        let end_key = encode_log_key(log_id.index.saturating_add(1));

        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&logs_cf, &start_key, &end_key);

        // 更新最后清理的日志 ID
        let value = serialize(&Some(log_id))?;
        batch.put_cf(&meta_cf, LAST_PURGED_KEY, &value);

        self.engine.write(batch).map_err(io_write_err)?;

        Ok(())
    }

    /// 获取日志存储的当前状态
    async fn do_get_log_state(
        &self,
    ) -> Result<openraft::LogState<KiwiTypeConfig>, StorageError<u64>> {
        // 获取 logs_cf 列族句柄
        let logs_cf = self.cf_read(LOGS_CF)?;

        // 使用反向迭代器获取最后一条日志
        let iter = self.engine.iterator_cf(&logs_cf, IteratorMode::End);
        let last_log_id = iter
            .take(1)
            .next()
            .transpose()
            .map_err(io_read_err)?
            .map(|res| {
                let (_, value) = res;
                // 反序列化日志条目以获取 log_id
                deserialize::<openraft::Entry<KiwiTypeConfig>>(&value).map(|entry| entry.log_id)
            })
            .transpose()?;

        // 从 meta_cf 读取最后清理的日志 ID
        let meta_cf = self.cf_read(META_CF)?;

        let last_purged_log_id = self
            .engine
            .get_cf(&meta_cf, LAST_PURGED_KEY)
            .map_err(io_read_err)?
            .map(|bytes| deserialize::<Option<openraft::LogId<u64>>>(&bytes))
            .transpose()?
            .flatten();

        // 如果没有日志条目，last_log_id 应该等于 last_purged_log_id
        let last_log_id = match last_log_id {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };

        Ok(openraft::LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
}

impl RocksdbLogStore {
    /// 从已有的 RocksDB 引擎创建日志存储实例
    pub fn new(engine: Arc<dyn Engine>) -> Result<Self, StorageError<u64>> {
        for cf_name in [LOGS_CF, META_CF, STATE_CF] {
            if engine.cf_handle(cf_name).is_none() {
                return Err(cf_not_found_read(cf_name));
            }
        }
        Ok(Self { engine })
    }

    /// 打开或创建指定路径的 RocksDB 日志存储
    pub fn open(path: impl AsRef<Path>) -> Result<Self, anyhow::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(LOGS_CF, Options::default()),
            ColumnFamilyDescriptor::new(META_CF, Options::default()),
            ColumnFamilyDescriptor::new(STATE_CF, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cfs)?;
        let engine = Arc::new(RocksdbEngine::new(db));
        Ok(Self::new(engine)?)
    }
}

// Trait implementations

impl openraft::RaftLogReader<KiwiTypeConfig> for RocksdbLogStore {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<KiwiTypeConfig>>, StorageError<u64>> {
        self.do_try_get_log_entries(range).await
    }
}

impl openraft::storage::RaftLogStorage<KiwiTypeConfig> for RocksdbLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::LogState<KiwiTypeConfig>, StorageError<u64>> {
        self.do_get_log_state().await
    }

    async fn save_committed(
        &mut self,
        committed: Option<openraft::LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.do_save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<openraft::LogId<u64>>, StorageError<u64>> {
        self.do_read_committed().await
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        self.do_save_vote(vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        self.do_read_vote().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<KiwiTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<KiwiTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        self.do_append(entries, callback).await
    }

    async fn truncate(&mut self, log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        self.do_truncate(log_id).await
    }

    async fn purge(&mut self, log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        self.do_purge(log_id).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

// 辅助函数

/// 将任意错误包装为写操作的 StorageError::IO
fn io_write_err(e: impl std::fmt::Display) -> StorageError<u64> {
    let io_err = std::io::Error::other(e.to_string());
    StorageError::IO {
        source: openraft::StorageIOError::write(&io_err),
    }
}

/// 将任意错误包装为读操作的 StorageError::IO
fn io_read_err(e: impl std::fmt::Display) -> StorageError<u64> {
    let io_err = std::io::Error::other(e.to_string());
    StorageError::IO {
        source: openraft::StorageIOError::read(&io_err),
    }
}

/// 将 CF 不存在包装为写操作的 StorageError::IO
fn cf_not_found_write(name: &str) -> StorageError<u64> {
    let io_err = std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("Column family '{}' not found", name),
    );
    StorageError::IO {
        source: openraft::StorageIOError::write(&io_err),
    }
}

/// 将 CF 不存在包装为读操作的 StorageError::IO
fn cf_not_found_read(name: &str) -> StorageError<u64> {
    let io_err = std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("Column family '{}' not found", name),
    );
    StorageError::IO {
        source: openraft::StorageIOError::read(&io_err),
    }
}

/// 序列化数据结构为字节序列
fn serialize<T: Serialize>(data: &T) -> Result<Vec<u8>, StorageError<u64>> {
    serde_json::to_vec(data).map_err(|e| {
        let io_err = std::io::Error::new(std::io::ErrorKind::InvalidData, e);
        StorageError::IO {
            source: openraft::StorageIOError::write(&io_err),
        }
    })
}

/// 反序列化字节序列为数据结构
fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError<u64>> {
    serde_json::from_slice(bytes).map_err(|e| {
        let io_err = std::io::Error::new(std::io::ErrorKind::InvalidData, e);
        StorageError::IO {
            source: openraft::StorageIOError::read(&io_err),
        }
    })
}

/// 编码日志索引为 RocksDB 键
fn encode_log_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

/// 解码 RocksDB 键为日志索引
fn decode_log_key(key: &[u8]) -> Result<u64, StorageError<u64>> {
    if key.len() != 8 {
        let err = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid key length: expected 8 bytes, got {}", key.len()),
        );
        return Err(StorageError::IO {
            source: openraft::StorageIOError::read(&err),
        });
    }

    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(key);
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use conf::raft_type::{Binlog, BinlogEntry, OperateType};
    use engine::RocksdbEngine;
    use openraft::storage::RaftLogStorageExt;
    use openraft::{Entry, EntryPayload, LeaderId, LogId, Vote};
    use proptest::prelude::*;
    use tempfile::TempDir;

    // 单元测试：编码解码

    #[test]
    fn test_encode_decode_log_key_zero() {
        let index = 0u64;
        let encoded = encode_log_key(index);
        let decoded = decode_log_key(&encoded).expect("Decode should succeed");
        assert_eq!(index, decoded);
    }

    #[test]
    fn test_encode_decode_log_key_max() {
        let index = u64::MAX;
        let encoded = encode_log_key(index);
        let decoded = decode_log_key(&encoded).expect("Decode should succeed");
        assert_eq!(index, decoded);
    }

    #[test]
    fn test_encode_decode_log_key_various() {
        let test_cases = vec![1, 100, 1000, 10000, 100000, u64::MAX / 2];
        for index in test_cases {
            let encoded = encode_log_key(index);
            let decoded = decode_log_key(&encoded).expect("Decode should succeed");
            assert_eq!(index, decoded);
        }
    }

    #[test]
    fn test_log_key_lexicographic_order() {
        // 验证编码后的键保持字典序
        let indices = vec![1u64, 10, 100, 1000, 10000];
        let mut encoded_keys: Vec<[u8; 8]> = indices.iter().map(|&i| encode_log_key(i)).collect();

        // 字典序排序
        encoded_keys.sort();

        // 解码并验证顺序
        let decoded: Vec<u64> = encoded_keys
            .iter()
            .map(|k| decode_log_key(k).unwrap())
            .collect();

        assert_eq!(decoded, indices);
    }

    #[test]
    fn test_decode_log_key_invalid_length() {
        let invalid_key = vec![1, 2, 3]; // 长度不是 8
        let result = decode_log_key(&invalid_key);
        assert!(result.is_err());
    }

    // 生成随机 LogId 的策略
    fn arb_log_id() -> impl Strategy<Value = LogId<u64>> {
        (1u64..1000, 1u64..10000)
            .prop_map(|(term, index)| LogId::new(LeaderId::new(term, 1), index))
    }

    // 生成随机 Vote 的策略
    fn arb_vote() -> impl Strategy<Value = Vote<u64>> {
        (1u64..1000, 1u64..10).prop_map(|(term, node_id)| Vote::new(term, node_id))
    }

    // 生成随机 OperateType 的策略
    fn arb_operate_type() -> impl Strategy<Value = OperateType> {
        prop_oneof![
            Just(OperateType::NoOp),
            Just(OperateType::Put),
            Just(OperateType::Delete),
        ]
    }

    // 生成随机 BinlogEntry 的策略
    fn arb_binlog_entry() -> impl Strategy<Value = BinlogEntry> {
        (
            0u32..6,
            arb_operate_type(),
            prop::collection::vec(prop::num::u8::ANY, 1..20),
            prop::option::of(prop::collection::vec(prop::num::u8::ANY, 0..50)),
        )
            .prop_map(|(cf_idx, op_type, key, value)| BinlogEntry {
                cf_idx,
                op_type,
                key,
                value,
            })
    }

    // 生成随机 Binlog 的策略
    fn arb_binlog() -> impl Strategy<Value = Binlog> {
        (
            0u32..100,
            0u32..16384,
            prop::collection::vec(arb_binlog_entry(), 0..5),
        )
            .prop_map(|(db_id, slot_idx, entries)| Binlog {
                db_id,
                slot_idx,
                entries,
            })
    }

    // 生成随机 Entry 的策略
    fn arb_entry() -> impl Strategy<Value = Entry<conf::raft_type::KiwiTypeConfig>> {
        (arb_log_id(), arb_binlog()).prop_map(|(log_id, payload)| Entry {
            log_id,
            payload: EntryPayload::Normal(payload),
        })
    }

    // Feature: rocksdb-raft-log-store, Property 10: Serialization Round-Trip Consistency
    // Validates: Requirements 9.4
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_serialize_deserialize_log_id(log_id in arb_log_id()) {
            let serialized = serialize(&log_id).expect("Serialization should succeed");
            let deserialized: LogId<u64> = deserialize(&serialized).expect("Deserialization should succeed");
            prop_assert_eq!(log_id, deserialized);
        }

        #[test]
        fn prop_serialize_deserialize_vote(vote in arb_vote()) {
            let serialized = serialize(&vote).expect("Serialization should succeed");
            let deserialized: Vote<u64> = deserialize(&serialized).expect("Deserialization should succeed");
            prop_assert_eq!(vote, deserialized);
        }

        #[test]
        fn prop_serialize_deserialize_entry(entry in arb_entry()) {
            let serialized = serialize(&entry).expect("Serialization should succeed");
            let deserialized: Entry<conf::raft_type::KiwiTypeConfig> = deserialize(&serialized).expect("Deserialization should succeed");
            prop_assert_eq!(entry.log_id, deserialized.log_id);
            // Note: We compare log_id and payload structure
            match (&entry.payload, &deserialized.payload) {
                (EntryPayload::Normal(b1), EntryPayload::Normal(b2)) => {
                    prop_assert_eq!(b1.db_id, b2.db_id);
                    prop_assert_eq!(b1.slot_idx, b2.slot_idx);
                    prop_assert_eq!(b1.entries.len(), b2.entries.len());
                }
                _ => {}
            }
        }
    }

    // Unit tests for initialization

    /// Helper function to create a test database with required column families
    fn create_test_db_with_cfs() -> (TempDir, Arc<dyn Engine>) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Create column family descriptors for all required CFs
        let cfs = vec![
            ColumnFamilyDescriptor::new(LOGS_CF, Options::default()),
            ColumnFamilyDescriptor::new(META_CF, Options::default()),
            ColumnFamilyDescriptor::new(STATE_CF, Options::default()),
        ];

        let db =
            DB::open_cf_descriptors(&opts, temp_dir.path(), cfs).expect("Failed to open database");

        let engine: Arc<dyn Engine> = Arc::new(RocksdbEngine::new(db));
        (temp_dir, engine)
    }

    /// Helper function to create a test database without column families
    fn create_test_db_without_cfs() -> (TempDir, Arc<dyn Engine>) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, temp_dir.path()).expect("Failed to open database");
        let engine: Arc<dyn Engine> = Arc::new(RocksdbEngine::new(db));
        (temp_dir, engine)
    }

    #[test]
    fn test_initialization_success() {
        // Test normal initialization with all required column families
        let (_temp_dir, engine) = create_test_db_with_cfs();

        let result = RocksdbLogStore::new(engine);
        assert!(
            result.is_ok(),
            "Initialization should succeed with all column families present"
        );

        let log_store = result.unwrap();
        // Verify the log store is usable by checking it can be cloned
        let _cloned = log_store.clone();
    }

    #[test]
    fn test_initialization_missing_logs_cf() {
        // Test initialization fails when logs_cf is missing
        let (_temp_dir, engine) = create_test_db_without_cfs();

        let result = RocksdbLogStore::new(engine);
        assert!(
            result.is_err(),
            "Initialization should fail when logs_cf is missing"
        );

        if let Err(StorageError::IO { source }) = result {
            let error_msg = format!("{:?}", source);
            assert!(
                error_msg.contains("logs") || error_msg.contains("not found"),
                "Error should mention missing column family"
            );
        } else {
            panic!("Expected StorageError::IO");
        }
    }

    #[test]
    fn test_initialization_with_partial_cfs() {
        // Test initialization fails when only some column families exist
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Create only logs_cf, missing meta_cf and state_cf
        let cfs = vec![ColumnFamilyDescriptor::new(LOGS_CF, Options::default())];

        let db =
            DB::open_cf_descriptors(&opts, temp_dir.path(), cfs).expect("Failed to open database");

        let engine: Arc<dyn Engine> = Arc::new(RocksdbEngine::new(db));

        let result = RocksdbLogStore::new(engine);
        assert!(
            result.is_err(),
            "Initialization should fail when not all column families are present"
        );
    }

    #[test]
    fn test_initialization_allows_basic_operations() {
        // Test that after successful initialization, basic operations can be performed
        let (_temp_dir, engine) = create_test_db_with_cfs();

        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Verify we can clone the log store (required for multi-threaded usage)
        let cloned_store = log_store.clone();

        // Both stores should be independent but share the same underlying data
        // This is a basic sanity check that the Arc<Mutex<>> structure works
        drop(cloned_store);
        drop(log_store);
    }

    #[test]
    fn test_column_family_handles_accessible() {
        // Test that all column family handles are accessible after initialization
        let (_temp_dir, engine) = create_test_db_with_cfs();

        // Verify all column families are accessible through the engine
        assert!(
            engine.cf_handle(LOGS_CF).is_some(),
            "logs_cf should be accessible"
        );
        assert!(
            engine.cf_handle(META_CF).is_some(),
            "meta_cf should be accessible"
        );
        assert!(
            engine.cf_handle(STATE_CF).is_some(),
            "state_cf should be accessible"
        );

        // Now create the log store
        let _log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");
    }

    // Tests for build_append_batch / do_append serialization logic
    // These cover the core write path (Requirements 2.1, 2.3, 3.1) without needing LogFlushed.

    #[tokio::test]
    async fn test_build_append_batch_round_trip() {
        // build_append_batch serializes entries; writing the batch and reading back must match.
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine.clone()).expect("init ok");

        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
        ];

        let batch = log_store
            .build_append_batch(entries.clone())
            .expect("build_append_batch should succeed");

        engine.write(batch).expect("write should succeed");

        let read = log_store
            .do_try_get_log_entries(1..=2)
            .await
            .expect("read should succeed");

        assert_eq!(read.len(), entries.len());
        for (expected, actual) in entries.iter().zip(read.iter()) {
            assert_eq!(expected.log_id, actual.log_id);
            match (&expected.payload, &actual.payload) {
                (EntryPayload::Normal(b1), EntryPayload::Normal(b2)) => {
                    assert_eq!(b1.db_id, b2.db_id);
                    assert_eq!(b1.slot_idx, b2.slot_idx);
                }
                _ => panic!("unexpected payload variant"),
            }
        }
    }

    #[test]
    fn test_build_append_batch_empty() {
        // An empty iterator must produce a valid (empty) batch without error.
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine.clone()).expect("init ok");

        let batch = log_store
            .build_append_batch(std::iter::empty::<Entry<KiwiTypeConfig>>())
            .expect("empty batch should succeed");

        // Writing an empty batch is a no-op but must not fail.
        engine
            .write(batch)
            .expect("write empty batch should succeed");
    }

    #[test]
    fn test_build_append_batch_missing_cf_returns_error() {
        // If the logs CF is absent, build_append_batch must return an error.
        let (_temp_dir, engine) = create_test_db_without_cfs();
        // Bypass new() validation by constructing directly.
        let log_store = RocksdbLogStore { engine };

        let entry = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: EntryPayload::Normal(Binlog {
                db_id: 0,
                slot_idx: 0,
                entries: vec![],
            }),
        };

        let result = log_store.build_append_batch(std::iter::once(entry));
        assert!(result.is_err(), "missing CF should produce an error");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_build_append_batch_round_trip(entries in prop::collection::vec(arb_entry(), 1..20)) {
            // Property: every entry written via build_append_batch is readable back with correct log_id.
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let (_temp_dir, engine) = create_test_db_with_cfs();
                let log_store = RocksdbLogStore::new(engine.clone()).expect("init ok");

                // Assign unique sequential indices so there are no duplicates.
                let mut unique_entries: Vec<Entry<KiwiTypeConfig>> = entries
                    .into_iter()
                    .enumerate()
                    .map(|(i, mut e)| { e.log_id.index = (i + 1) as u64; e })
                    .collect();
                unique_entries.sort_by_key(|e| e.log_id.index);

                let max_index = unique_entries.len() as u64;

                let batch = log_store
                    .build_append_batch(unique_entries.clone())
                    .expect("build_append_batch should succeed");
                engine.write(batch).expect("write should succeed");

                let read = log_store
                    .do_try_get_log_entries(1..=max_index)
                    .await
                    .expect("read should succeed");

                prop_assert_eq!(unique_entries.len(), read.len());
                for (expected, actual) in unique_entries.iter().zip(read.iter()) {
                    prop_assert_eq!(expected.log_id.index, actual.log_id.index);
                    prop_assert_eq!(expected.log_id.leader_id.term, actual.log_id.leader_id.term);
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            }).unwrap();
        }
    }

    // Feature: rocksdb-raft-log-store, Property 3: Range Query Completeness
    // Validates: Requirements 3.1
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_range_query_completeness(
            entries in prop::collection::vec(arb_entry(), 1..20)
        ) {
            // Run async test in tokio runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Create test database
                let (_temp_dir, engine) = create_test_db_with_cfs();
                let engine_clone = engine.clone();
                let log_store = RocksdbLogStore::new(engine)
                    .expect("Initialization should succeed");

                // Sort entries by index and assign sequential indices
                let mut sorted_entries = entries.clone();
                sorted_entries.sort_by_key(|e| e.log_id.index);

                // Reassign sequential indices starting from 1
                let mut sequential_entries: Vec<Entry<KiwiTypeConfig>> = Vec::new();
                for (i, entry) in sorted_entries.iter().enumerate() {
                    let new_index = (i + 1) as u64;
                    let mut new_entry = entry.clone();
                    new_entry.log_id.index = new_index;
                    sequential_entries.push(new_entry);
                }

                // Write entries to database
                {
                    let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
                    let mut batch = WriteBatch::default();

                    for entry in &sequential_entries {
                        let key = encode_log_key(entry.log_id.index);
                        let value = serialize(&entry).unwrap();
                        batch.put_cf(&logs_cf, &key, &value);
                    }

                    engine_clone.write(batch).unwrap();
                }

                // Generate random range within the valid indices
                let max_index = sequential_entries.len() as u64;
                if max_index == 0 {
                    return Ok(());
                }

                // Test various ranges
                let test_ranges: Vec<(u64, u64)> = vec![
                    (1, max_index),  // Full range
                    (1, max_index / 2),  // First half
                    (max_index / 2 + 1, max_index),  // Second half
                    (max_index / 3, max_index * 2 / 3),  // Middle portion
                ];

                for (start, end) in test_ranges {
                    if start > max_index || end > max_index || start > end {
                        continue;
                    }

                    // Query the range
                    let read_entries = {

                        log_store.do_try_get_log_entries(start..=end).await
                            .expect("Read should succeed")
                    };

                    // Expected entries in this range
                    let expected_entries: Vec<_> = sequential_entries.iter()
                        .filter(|e| e.log_id.index >= start && e.log_id.index <= end)
                        .cloned()
                        .collect();

                    // Verify count matches
                    prop_assert_eq!(
                        expected_entries.len(),
                        read_entries.len(),
                        "Range {}..={} should return {} entries, got {}",
                        start, end, expected_entries.len(), read_entries.len()
                    );

                    // Verify all entries are present and in order
                    for (expected, actual) in expected_entries.iter().zip(read_entries.iter()) {
                        prop_assert_eq!(
                            expected.log_id.index,
                            actual.log_id.index,
                            "Entry indices should match"
                        );
                        prop_assert_eq!(
                            expected.log_id.leader_id.term,
                            actual.log_id.leader_id.term,
                            "Entry terms should match"
                        );
                    }

                    // Verify entries are in ascending order
                    for i in 1..read_entries.len() {
                        prop_assert!(
                            read_entries[i-1].log_id.index < read_entries[i].log_id.index,
                            "Entries should be in ascending order by index"
                        );
                    }
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            }).unwrap();
        }
    }

    // Unit tests for boundary cases - Requirements 3.2, 3.3

    #[tokio::test]
    async fn test_empty_range_query() {
        // Test querying an empty range returns empty list
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Query empty database
        let entries = {
            log_store
                .do_try_get_log_entries(1..=10)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(entries.len(), 0, "Empty database should return empty list");
    }

    #[tokio::test]
    async fn test_nonexistent_index_range() {
        // Test querying indices that don't exist
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 2, 3
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Query range that doesn't exist (100..=200)
        let read_entries = {
            log_store
                .do_try_get_log_entries(100..=200)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            read_entries.len(),
            0,
            "Nonexistent range should return empty list"
        );

        // Query range partially overlapping (2..=100)
        let read_entries = {
            log_store
                .do_try_get_log_entries(2..=100)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            read_entries.len(),
            2,
            "Should return only existing entries in range"
        );
        assert_eq!(read_entries[0].log_id.index, 2);
        assert_eq!(read_entries[1].log_id.index, 3);
    }

    #[tokio::test]
    async fn test_deserialization_error_handling() {
        // Test that corrupted data returns an error
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write corrupted data directly to database
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            let key = encode_log_key(1);
            let corrupted_value = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Invalid JSON
            batch.put_cf(&logs_cf, &key, &corrupted_value);

            engine_clone.write(batch).unwrap();
        }

        // Try to read the corrupted entry
        let result = { log_store.do_try_get_log_entries(1..=1).await };

        assert!(
            result.is_err(),
            "Reading corrupted data should return error"
        );

        if let Err(StorageError::IO { source }) = result {
            let error_msg = format!("{:?}", source);
            assert!(
                error_msg.contains("InvalidData") || error_msg.contains("expected"),
                "Error should indicate deserialization failure"
            );
        } else {
            panic!("Expected StorageError::IO for deserialization error");
        }
    }

    #[tokio::test]
    async fn test_range_query_with_gaps() {
        // Test querying when there are gaps in the indices
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 3, 5, 7 (with gaps)
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 5),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 7),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 3,
                    slot_idx: 3,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Query range 1..=7 should return all 4 entries
        let read_entries = {
            log_store
                .do_try_get_log_entries(1..=7)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            read_entries.len(),
            4,
            "Should return all entries in range despite gaps"
        );
        assert_eq!(read_entries[0].log_id.index, 1);
        assert_eq!(read_entries[1].log_id.index, 3);
        assert_eq!(read_entries[2].log_id.index, 5);
        assert_eq!(read_entries[3].log_id.index, 7);

        // Query range 2..=6 should return entries 3 and 5
        let read_entries = {
            log_store
                .do_try_get_log_entries(2..=6)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            read_entries.len(),
            2,
            "Should return only entries within range"
        );
        assert_eq!(read_entries[0].log_id.index, 3);
        assert_eq!(read_entries[1].log_id.index, 5);
    }

    #[tokio::test]
    async fn test_single_entry_range() {
        // Test querying a single entry
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write a single entry
        let entry = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 5),
            payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                db_id: 0,
                slot_idx: 0,
                entries: vec![],
            }),
        };

        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            let key = encode_log_key(entry.log_id.index);
            let value = serialize(&entry).unwrap();
            batch.put_cf(&logs_cf, &key, &value);

            engine_clone.write(batch).unwrap();
        }

        // Query exactly that entry
        let read_entries = {
            log_store
                .do_try_get_log_entries(5..=5)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(read_entries.len(), 1, "Should return exactly one entry");
        assert_eq!(read_entries[0].log_id.index, 5);
    }

    // Feature: rocksdb-raft-log-store, Property 4: Vote Information Round-Trip Consistency
    // Validates: Requirements 4.1, 4.2
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_vote_round_trip(vote in arb_vote()) {
            // Run async test in tokio runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Create test database
                let (_temp_dir, engine) = create_test_db_with_cfs();
                let log_store = RocksdbLogStore::new(engine)
                    .expect("Initialization should succeed");

                // Save the vote
                {

                    log_store.do_save_vote(&vote).await
                        .expect("Save vote should succeed");
                }

                // Read back the vote
                let read_vote = {

                    log_store.do_read_vote().await
                        .expect("Read vote should succeed")
                };

                // Verify the vote was read back correctly
                prop_assert!(read_vote.is_some(), "Vote should exist after saving");
                let read_vote = read_vote.unwrap();

                prop_assert_eq!(vote.leader_id().term, read_vote.leader_id().term, "Vote term should match");
                prop_assert_eq!(vote.leader_id().node_id, read_vote.leader_id().node_id, "Vote node_id should match");
                prop_assert_eq!(vote.committed, read_vote.committed, "Vote committed should match");

                Ok::<(), proptest::test_runner::TestCaseError>(())
            }).unwrap();
        }
    }

    // Feature: rocksdb-raft-log-store, Property 5: Committed Log ID Round-Trip Consistency
    // Validates: Requirements 5.1, 5.2
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_committed_log_id_round_trip(log_id in prop::option::of(arb_log_id())) {
            // Run async test in tokio runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Create test database
                let (_temp_dir, engine) = create_test_db_with_cfs();
                let log_store = RocksdbLogStore::new(engine)
                    .expect("Initialization should succeed");

                // Save the committed log ID
                {

                    log_store.do_save_committed(log_id).await
                        .expect("Save committed should succeed");
                }

                // Read back the committed log ID
                let read_committed = {

                    log_store.do_read_committed().await
                        .expect("Read committed should succeed")
                };

                // Verify the committed log ID was read back correctly
                match (log_id, read_committed) {
                    (Some(saved), Some(read)) => {
                        prop_assert_eq!(saved.leader_id.term, read.leader_id.term, "Committed term should match");
                        prop_assert_eq!(saved.leader_id.node_id, read.leader_id.node_id, "Committed node_id should match");
                        prop_assert_eq!(saved.index, read.index, "Committed index should match");
                    }
                    (None, None) => {
                        // Both None is valid
                    }
                    _ => {
                        prop_assert!(false, "Saved and read committed should both be Some or both be None");
                    }
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            }).unwrap();
        }
    }

    // Unit tests for vote boundary cases - Requirements 4.3

    #[tokio::test]
    async fn test_read_nonexistent_vote() {
        // Test reading vote when none has been saved
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Read vote from empty database
        let vote = { log_store.do_read_vote().await.expect("Read should succeed") };

        assert!(
            vote.is_none(),
            "Reading nonexistent vote should return None"
        );
    }

    #[tokio::test]
    async fn test_vote_overwrite() {
        // Test that saving a new vote overwrites the old one
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Save first vote
        let vote1 = Vote::new(1, 1);
        {
            log_store
                .do_save_vote(&vote1)
                .await
                .expect("Save vote should succeed");
        }

        // Save second vote (different term and node)
        let vote2 = Vote::new(2, 2);
        {
            log_store
                .do_save_vote(&vote2)
                .await
                .expect("Save vote should succeed");
        }

        // Read back and verify it's the second vote
        let read_vote = { log_store.do_read_vote().await.expect("Read should succeed") };

        assert!(read_vote.is_some(), "Vote should exist");
        let read_vote = read_vote.unwrap();
        assert_eq!(
            read_vote.leader_id().term,
            2,
            "Should have term from second vote"
        );
        assert_eq!(
            read_vote.leader_id().node_id,
            2,
            "Should have node_id from second vote"
        );
    }

    #[tokio::test]
    async fn test_vote_persistence_across_instances() {
        // Test that vote persists when creating a new log store instance
        let (temp_dir, engine) = create_test_db_with_cfs();

        // Save vote with first instance
        let vote = Vote::new(5, 3);
        {
            let log_store =
                RocksdbLogStore::new(engine.clone()).expect("Initialization should succeed");

            log_store
                .do_save_vote(&vote)
                .await
                .expect("Save vote should succeed");
        }

        // Create new instance and read vote
        {
            let log_store =
                RocksdbLogStore::new(engine.clone()).expect("Initialization should succeed");

            let read_vote = { log_store.do_read_vote().await.expect("Read should succeed") };

            assert!(read_vote.is_some(), "Vote should persist across instances");
            let read_vote = read_vote.unwrap();
            assert_eq!(read_vote.leader_id().term, 5, "Term should match");
            assert_eq!(read_vote.leader_id().node_id, 3, "Node ID should match");
        }

        // Clean up
        drop(temp_dir);
    }

    // Unit tests for committed log ID boundary cases - Requirements 5.3

    #[tokio::test]
    async fn test_read_nonexistent_committed() {
        // Test reading committed log ID when none has been saved
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Read committed from empty database
        let committed = {
            log_store
                .do_read_committed()
                .await
                .expect("Read should succeed")
        };

        assert!(
            committed.is_none(),
            "Reading nonexistent committed log ID should return None"
        );
    }

    #[tokio::test]
    async fn test_committed_overwrite() {
        // Test that saving a new committed log ID overwrites the old one
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Save first committed log ID
        let committed1 = Some(LogId::new(LeaderId::new(1, 1), 10));
        {
            log_store
                .do_save_committed(committed1)
                .await
                .expect("Save committed should succeed");
        }

        // Save second committed log ID (different term and index)
        let committed2 = Some(LogId::new(LeaderId::new(2, 2), 20));
        {
            log_store
                .do_save_committed(committed2)
                .await
                .expect("Save committed should succeed");
        }

        // Read back and verify it's the second committed log ID
        let read_committed = {
            log_store
                .do_read_committed()
                .await
                .expect("Read should succeed")
        };

        assert!(read_committed.is_some(), "Committed log ID should exist");
        let read_committed = read_committed.unwrap();
        assert_eq!(
            read_committed.leader_id.term, 2,
            "Should have term from second committed"
        );
        assert_eq!(
            read_committed.leader_id.node_id, 2,
            "Should have node_id from second committed"
        );
        assert_eq!(
            read_committed.index, 20,
            "Should have index from second committed"
        );
    }

    #[tokio::test]
    async fn test_committed_none_value() {
        // Test that saving None as committed log ID works correctly
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // First save a committed log ID
        let committed1 = Some(LogId::new(LeaderId::new(1, 1), 10));
        {
            log_store
                .do_save_committed(committed1)
                .await
                .expect("Save committed should succeed");
        }

        // Verify it was saved
        let read_committed = {
            log_store
                .do_read_committed()
                .await
                .expect("Read should succeed")
        };
        assert!(read_committed.is_some(), "Committed log ID should exist");

        // Now save None
        {
            log_store
                .do_save_committed(None)
                .await
                .expect("Save committed None should succeed");
        }

        // Read back and verify it's None
        let read_committed = {
            log_store
                .do_read_committed()
                .await
                .expect("Read should succeed")
        };

        assert!(
            read_committed.is_none(),
            "Committed log ID should be None after saving None"
        );
    }

    #[tokio::test]
    async fn test_committed_persistence_across_instances() {
        // Test that committed log ID persists when creating a new log store instance
        let (temp_dir, engine) = create_test_db_with_cfs();

        // Save committed with first instance
        let committed = Some(LogId::new(LeaderId::new(5, 3), 100));
        {
            let log_store =
                RocksdbLogStore::new(engine.clone()).expect("Initialization should succeed");

            log_store
                .do_save_committed(committed)
                .await
                .expect("Save committed should succeed");
        }

        // Create new instance and read committed
        {
            let log_store =
                RocksdbLogStore::new(engine.clone()).expect("Initialization should succeed");

            let read_committed = {
                log_store
                    .do_read_committed()
                    .await
                    .expect("Read should succeed")
            };

            assert!(
                read_committed.is_some(),
                "Committed log ID should persist across instances"
            );
            let read_committed = read_committed.unwrap();
            assert_eq!(read_committed.leader_id.term, 5, "Term should match");
            assert_eq!(read_committed.leader_id.node_id, 3, "Node ID should match");
            assert_eq!(read_committed.index, 100, "Index should match");
        }

        // Clean up
        drop(temp_dir);
    }

    // Feature: rocksdb-raft-log-store, Property 6: Log State Reflects Last Entry
    // Validates: Requirements 6.1
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_log_state_reflects_last_entry(
            entries in prop::collection::vec(arb_entry(), 1..20)
        ) {
            // Run async test in tokio runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Create test database
                let (_temp_dir, engine) = create_test_db_with_cfs();
                let engine_clone = engine.clone();
                let log_store = RocksdbLogStore::new(engine)
                    .expect("Initialization should succeed");

                // Sort entries by index and assign sequential indices
                let mut sorted_entries = entries.clone();
                sorted_entries.sort_by_key(|e| e.log_id.index);

                // Reassign sequential indices starting from 1
                let mut sequential_entries: Vec<Entry<KiwiTypeConfig>> = Vec::new();
                for (i, entry) in sorted_entries.iter().enumerate() {
                    let new_index = (i + 1) as u64;
                    let mut new_entry = entry.clone();
                    new_entry.log_id.index = new_index;
                    sequential_entries.push(new_entry);
                }

                if sequential_entries.is_empty() {
                    return Ok(());
                }

                // Write entries to database
                {
                    let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
                    let mut batch = WriteBatch::default();

                    for entry in &sequential_entries {
                        let key = encode_log_key(entry.log_id.index);
                        let value = serialize(&entry).unwrap();
                        batch.put_cf(&logs_cf, &key, &value);
                    }

                    engine_clone.write(batch).unwrap();
                }

                // Get the log state
                let log_state = {

                    log_store.do_get_log_state().await
                        .expect("Get log state should succeed")
                };

                // The last entry should be the one with the highest index
                let expected_last = sequential_entries.last().unwrap();

                // Verify last_log_id matches the last entry
                prop_assert!(log_state.last_log_id.is_some(), "last_log_id should be Some when entries exist");
                let last_log_id = log_state.last_log_id.unwrap();

                prop_assert_eq!(
                    last_log_id.index,
                    expected_last.log_id.index,
                    "last_log_id index should match the last entry's index"
                );
                prop_assert_eq!(
                    last_log_id.leader_id.term,
                    expected_last.log_id.leader_id.term,
                    "last_log_id term should match the last entry's term"
                );

                Ok::<(), proptest::test_runner::TestCaseError>(())
            }).unwrap();
        }
    }

    // Feature: rocksdb-raft-log-store, Property 7: Log State Reflects Last Purged
    // Validates: Requirements 6.2
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_log_state_reflects_last_purged(
            entries in prop::collection::vec(arb_entry(), 5..20),
            purge_index in 1u64..10
        ) {
            // Run async test in tokio runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Create test database
                let (_temp_dir, engine) = create_test_db_with_cfs();
                let engine_clone = engine.clone();
                let log_store = RocksdbLogStore::new(engine)
                    .expect("Initialization should succeed");

                // Sort entries by index and assign sequential indices
                let mut sorted_entries = entries.clone();
                sorted_entries.sort_by_key(|e| e.log_id.index);

                // Reassign sequential indices starting from 1
                let mut sequential_entries: Vec<Entry<KiwiTypeConfig>> = Vec::new();
                for (i, entry) in sorted_entries.iter().enumerate() {
                    let new_index = (i + 1) as u64;
                    let mut new_entry = entry.clone();
                    new_entry.log_id.index = new_index;
                    sequential_entries.push(new_entry);
                }

                if sequential_entries.is_empty() {
                    return Ok(());
                }

                // Write entries to database
                {
                    let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
                    let mut batch = WriteBatch::default();

                    for entry in &sequential_entries {
                        let key = encode_log_key(entry.log_id.index);
                        let value = serialize(&entry).unwrap();
                        batch.put_cf(&logs_cf, &key, &value);
                    }

                    engine_clone.write(batch).unwrap();
                }

                // Determine which entry to purge up to (must be within valid range)
                let max_index = sequential_entries.len() as u64;
                let purge_up_to_index = std::cmp::min(purge_index, max_index);

                if purge_up_to_index == 0 {
                    return Ok(());
                }

                let purge_log_id = sequential_entries[(purge_up_to_index - 1) as usize].log_id;

                // Purge logs up to the selected index
                {

                    log_store.do_purge(purge_log_id).await
                        .expect("Purge should succeed");
                }

                // Get the log state after purge
                let log_state = {

                    log_store.do_get_log_state().await
                        .expect("Get log state should succeed")
                };

                // Verify last_purged_log_id matches the purged LogId
                prop_assert!(log_state.last_purged_log_id.is_some(), "last_purged_log_id should be Some after purge");
                let last_purged = log_state.last_purged_log_id.unwrap();

                prop_assert_eq!(
                    last_purged.index,
                    purge_log_id.index,
                    "last_purged_log_id index should match the purged index"
                );
                prop_assert_eq!(
                    last_purged.leader_id.term,
                    purge_log_id.leader_id.term,
                    "last_purged_log_id term should match the purged term"
                );

                Ok::<(), proptest::test_runner::TestCaseError>(())
            }).unwrap();
        }
    }

    // Feature: rocksdb-raft-log-store, Property 9: Purge Removes Previous Entries
    // Validates: Requirements 8.1, 8.2
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_purge_removes_previous_entries(
            entries in prop::collection::vec(arb_entry(), 5..20),
            purge_index_offset in 1u64..10
        ) {
            // Run async test in tokio runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Create test database
                let (_temp_dir, engine) = create_test_db_with_cfs();
                let engine_clone = engine.clone();
                let log_store = RocksdbLogStore::new(engine)
                    .expect("Initialization should succeed");

                // Sort entries by index and assign sequential indices
                let mut sorted_entries = entries.clone();
                sorted_entries.sort_by_key(|e| e.log_id.index);

                // Reassign sequential indices starting from 1
                let mut sequential_entries: Vec<Entry<KiwiTypeConfig>> = Vec::new();
                for (i, entry) in sorted_entries.iter().enumerate() {
                    let new_index = (i + 1) as u64;
                    let mut new_entry = entry.clone();
                    new_entry.log_id.index = new_index;
                    sequential_entries.push(new_entry);
                }

                if sequential_entries.is_empty() {
                    return Ok(());
                }

                // Write entries to database
                {
                    let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
                    let mut batch = WriteBatch::default();

                    for entry in &sequential_entries {
                        let key = encode_log_key(entry.log_id.index);
                        let value = serialize(&entry).unwrap();
                        batch.put_cf(&logs_cf, &key, &value);
                    }

                    engine_clone.write(batch).unwrap();
                }

                // Determine which entry to purge up to (must be within valid range)
                let max_index = sequential_entries.len() as u64;
                let purge_up_to_index = std::cmp::min(purge_index_offset, max_index);

                if purge_up_to_index == 0 {
                    return Ok(());
                }

                let purge_log_id = sequential_entries[(purge_up_to_index - 1) as usize].log_id;

                // Purge logs up to the selected index
                {

                    log_store.do_purge(purge_log_id).await
                        .expect("Purge should succeed");
                }

                // Read all remaining entries
                let remaining_entries = {

                    log_store.do_try_get_log_entries(1..=max_index).await
                        .expect("Read should succeed")
                };

                // Expected entries are those with index > purge_up_to_index
                let expected_count = (max_index - purge_up_to_index) as usize;

                prop_assert_eq!(
                    remaining_entries.len(),
                    expected_count,
                    "After purging up to index {}, should have {} entries remaining, got {}",
                    purge_up_to_index,
                    expected_count,
                    remaining_entries.len()
                );

                // Verify all remaining entries have index > purge_up_to_index
                for entry in &remaining_entries {
                    prop_assert!(
                        entry.log_id.index > purge_up_to_index,
                        "Remaining entry index {} should be greater than purge index {}",
                        entry.log_id.index,
                        purge_up_to_index
                    );
                }

                // Verify entries are still in order
                for i in 1..remaining_entries.len() {
                    prop_assert!(
                        remaining_entries[i-1].log_id.index < remaining_entries[i].log_id.index,
                        "Remaining entries should still be in ascending order"
                    );
                }

                // Verify that trying to read purged entries returns nothing
                if purge_up_to_index > 0 {
                    let purged_entries = {

                        log_store.do_try_get_log_entries(1..=purge_up_to_index).await
                            .expect("Read should succeed")
                    };

                    prop_assert_eq!(
                        purged_entries.len(),
                        0,
                        "Purged entries should not be readable"
                    );
                }

                // Verify metadata was updated
                let log_state = {

                    log_store.do_get_log_state().await
                        .expect("Get log state should succeed")
                };

                prop_assert!(log_state.last_purged_log_id.is_some(), "last_purged_log_id should be Some after purge");
                let last_purged = log_state.last_purged_log_id.unwrap();

                prop_assert_eq!(
                    last_purged.index,
                    purge_log_id.index,
                    "last_purged_log_id index should match the purged index"
                );

                Ok::<(), proptest::test_runner::TestCaseError>(())
            }).unwrap();
        }
    }

    // Feature: rocksdb-raft-log-store, Property 8: Truncate Removes Subsequent Entries
    // Validates: Requirements 7.1, 7.2
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_truncate_removes_subsequent_entries(
            entries in prop::collection::vec(arb_entry(), 5..20),
            truncate_index_offset in 1u64..10
        ) {
            // Run async test in tokio runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Create test database
                let (_temp_dir, engine) = create_test_db_with_cfs();
                let engine_clone = engine.clone();
                let log_store = RocksdbLogStore::new(engine)
                    .expect("Initialization should succeed");

                // Sort entries by index and assign sequential indices
                let mut sorted_entries = entries.clone();
                sorted_entries.sort_by_key(|e| e.log_id.index);

                // Reassign sequential indices starting from 1
                let mut sequential_entries: Vec<Entry<KiwiTypeConfig>> = Vec::new();
                for (i, entry) in sorted_entries.iter().enumerate() {
                    let new_index = (i + 1) as u64;
                    let mut new_entry = entry.clone();
                    new_entry.log_id.index = new_index;
                    sequential_entries.push(new_entry);
                }

                if sequential_entries.is_empty() {
                    return Ok(());
                }

                // Write entries to database
                {
                    let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
                    let mut batch = WriteBatch::default();

                    for entry in &sequential_entries {
                        let key = encode_log_key(entry.log_id.index);
                        let value = serialize(&entry).unwrap();
                        batch.put_cf(&logs_cf, &key, &value);
                    }

                    engine_clone.write(batch).unwrap();
                }

                // Determine which entry to truncate at (must be within valid range)
                let max_index = sequential_entries.len() as u64;
                let truncate_at_index = std::cmp::min(truncate_index_offset, max_index);

                if truncate_at_index == 0 {
                    return Ok(());
                }

                let truncate_log_id = sequential_entries[(truncate_at_index - 1) as usize].log_id;

                // Truncate logs at the selected index
                {

                    log_store.do_truncate(truncate_log_id).await
                        .expect("Truncate should succeed");
                }

                // Read all remaining entries
                let remaining_entries = {

                    log_store.do_try_get_log_entries(1..=max_index).await
                        .expect("Read should succeed")
                };

                // Expected entries are those with index < truncate_at_index
                let expected_count = (truncate_at_index - 1) as usize;

                prop_assert_eq!(
                    remaining_entries.len(),
                    expected_count,
                    "After truncating at index {}, should have {} entries remaining, got {}",
                    truncate_at_index,
                    expected_count,
                    remaining_entries.len()
                );

                // Verify all remaining entries have index < truncate_at_index
                for entry in &remaining_entries {
                    prop_assert!(
                        entry.log_id.index < truncate_at_index,
                        "Remaining entry index {} should be less than truncate index {}",
                        entry.log_id.index,
                        truncate_at_index
                    );
                }

                // Verify entries are still in order
                for i in 1..remaining_entries.len() {
                    prop_assert!(
                        remaining_entries[i-1].log_id.index < remaining_entries[i].log_id.index,
                        "Remaining entries should still be in ascending order"
                    );
                }

                // Verify that trying to read truncated entries returns nothing
                if truncate_at_index < max_index {
                    let truncated_entries = {

                        log_store.do_try_get_log_entries(truncate_at_index..=max_index).await
                            .expect("Read should succeed")
                    };

                    prop_assert_eq!(
                        truncated_entries.len(),
                        0,
                        "Truncated entries should not be readable"
                    );
                }

                Ok::<(), proptest::test_runner::TestCaseError>(())
            }).unwrap();
        }
    }

    // Unit tests for truncate boundary cases - Requirements 7.3

    #[tokio::test]
    async fn test_truncate_nonexistent_index() {
        // Test truncating at an index that doesn't exist should succeed
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 2, 3
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Truncate at index 100 (doesn't exist, beyond all entries)
        let truncate_log_id = LogId::new(LeaderId::new(1, 1), 100);
        let result = { log_store.do_truncate(truncate_log_id).await };

        // Should succeed even though index doesn't exist
        assert!(
            result.is_ok(),
            "Truncating nonexistent index should succeed"
        );

        // Since we're truncating at index 100 which is >= 100, all entries with index >= 100 should be removed
        // But since all our entries (1, 2, 3) are < 100, they should remain
        // However, the iterator starts at 100, so it won't find any entries to delete
        let remaining_entries = {
            log_store
                .do_try_get_log_entries(1..=3)
                .await
                .expect("Read should succeed")
        };

        // All entries should remain because they are all < 100
        assert_eq!(
            remaining_entries.len(),
            3,
            "All entries should remain when truncating beyond last index"
        );
    }

    #[tokio::test]
    async fn test_truncate_at_gap() {
        // Test truncating at an index in a gap (between existing entries)
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 3, 5 (with gaps at 2, 4)
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 5),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Truncate at index 4 (doesn't exist, in a gap)
        let truncate_log_id = LogId::new(LeaderId::new(1, 1), 4);
        {
            log_store
                .do_truncate(truncate_log_id)
                .await
                .expect("Truncate should succeed");
        }

        // Should remove entry at index 5 (since 5 >= 4)
        // Should keep entries at indices 1 and 3 (since 1 < 4 and 3 < 4)
        let remaining_entries = {
            log_store
                .do_try_get_log_entries(1..=5)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            remaining_entries.len(),
            2,
            "Should have 2 entries remaining after truncating at gap"
        );
        assert_eq!(remaining_entries[0].log_id.index, 1);
        assert_eq!(remaining_entries[1].log_id.index, 3);
    }

    #[tokio::test]
    async fn test_truncate_at_first_entry() {
        // Test truncating at the first entry removes all entries
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 2, 3
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Truncate at index 1 (first entry)
        let truncate_log_id = LogId::new(LeaderId::new(1, 1), 1);
        {
            log_store
                .do_truncate(truncate_log_id)
                .await
                .expect("Truncate should succeed");
        }

        // All entries should be removed
        let remaining_entries = {
            log_store
                .do_try_get_log_entries(1..=3)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            remaining_entries.len(),
            0,
            "All entries should be removed when truncating at first entry"
        );
    }

    #[tokio::test]
    async fn test_truncate_at_middle_entry() {
        // Test truncating at a middle entry keeps earlier entries
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 2, 3, 4, 5
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 4),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 3,
                    slot_idx: 3,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 5),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 4,
                    slot_idx: 4,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Truncate at index 3
        let truncate_log_id = LogId::new(LeaderId::new(1, 1), 3);
        {
            log_store
                .do_truncate(truncate_log_id)
                .await
                .expect("Truncate should succeed");
        }

        // Should have entries 1 and 2 remaining
        let remaining_entries = {
            log_store
                .do_try_get_log_entries(1..=5)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            remaining_entries.len(),
            2,
            "Should have 2 entries remaining after truncating at index 3"
        );
        assert_eq!(remaining_entries[0].log_id.index, 1);
        assert_eq!(remaining_entries[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn test_truncate_empty_database() {
        // Test truncating an empty database succeeds
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Truncate at index 1 on empty database
        let truncate_log_id = LogId::new(LeaderId::new(1, 1), 1);
        let result = { log_store.do_truncate(truncate_log_id).await };

        // Should succeed
        assert!(result.is_ok(), "Truncating empty database should succeed");
    }

    // Unit tests for log state boundary cases - Requirements 6.3, 6.4

    #[tokio::test]
    async fn test_empty_database_log_state() {
        // Test that querying log state on an empty database returns None for both fields
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Get log state from empty database
        let log_state = {
            log_store
                .do_get_log_state()
                .await
                .expect("Get log state should succeed")
        };

        // Both last_log_id and last_purged_log_id should be None
        assert!(
            log_state.last_log_id.is_none(),
            "last_log_id should be None for empty database"
        );
        assert!(
            log_state.last_purged_log_id.is_none(),
            "last_purged_log_id should be None when no purge has been performed"
        );
    }

    #[tokio::test]
    async fn test_log_state_with_entries_no_purge() {
        // Test that log state correctly reflects entries when no purge has been performed
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write some entries
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
        ];

        // Write entries to database
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Get log state
        let log_state = {
            log_store
                .do_get_log_state()
                .await
                .expect("Get log state should succeed")
        };

        // last_log_id should be the last entry (index 3)
        assert!(
            log_state.last_log_id.is_some(),
            "last_log_id should be Some when entries exist"
        );
        let last_log_id = log_state.last_log_id.unwrap();
        assert_eq!(last_log_id.index, 3, "last_log_id should be the last entry");
        assert_eq!(
            last_log_id.leader_id.term, 1,
            "last_log_id term should match"
        );

        // last_purged_log_id should still be None
        assert!(
            log_state.last_purged_log_id.is_none(),
            "last_purged_log_id should be None when no purge has been performed"
        );
    }

    #[tokio::test]
    async fn test_log_state_single_entry() {
        // Test log state with exactly one entry
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write a single entry
        let entry = Entry {
            log_id: LogId::new(LeaderId::new(5, 2), 100),
            payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                db_id: 0,
                slot_idx: 0,
                entries: vec![],
            }),
        };

        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            let key = encode_log_key(entry.log_id.index);
            let value = serialize(&entry).unwrap();
            batch.put_cf(&logs_cf, &key, &value);

            engine_clone.write(batch).unwrap();
        }

        // Get log state
        let log_state = {
            log_store
                .do_get_log_state()
                .await
                .expect("Get log state should succeed")
        };

        // Verify the single entry is reflected correctly
        assert!(
            log_state.last_log_id.is_some(),
            "last_log_id should be Some"
        );
        let last_log_id = log_state.last_log_id.unwrap();
        assert_eq!(last_log_id.index, 100, "last_log_id index should match");
        assert_eq!(
            last_log_id.leader_id.term, 5,
            "last_log_id term should match"
        );
        assert_eq!(
            last_log_id.leader_id.node_id, 2,
            "last_log_id node_id should match"
        );
    }

    #[tokio::test]
    async fn test_log_state_persistence_across_instances() {
        // Test that log state persists when creating a new log store instance
        let (temp_dir, engine) = create_test_db_with_cfs();

        // Write entries with first instance
        {
            let engine_clone = engine.clone();
            let _log_store =
                RocksdbLogStore::new(engine.clone()).expect("Initialization should succeed");

            let entries: Vec<Entry<KiwiTypeConfig>> = vec![
                Entry {
                    log_id: LogId::new(LeaderId::new(1, 1), 1),
                    payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                        db_id: 0,
                        slot_idx: 0,
                        entries: vec![],
                    }),
                },
                Entry {
                    log_id: LogId::new(LeaderId::new(1, 1), 2),
                    payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                        db_id: 1,
                        slot_idx: 1,
                        entries: vec![],
                    }),
                },
            ];

            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Create new instance and check log state
        {
            let log_store =
                RocksdbLogStore::new(engine.clone()).expect("Initialization should succeed");

            let log_state = {
                log_store
                    .do_get_log_state()
                    .await
                    .expect("Get log state should succeed")
            };

            // Verify log state persists
            assert!(
                log_state.last_log_id.is_some(),
                "last_log_id should persist across instances"
            );
            let last_log_id = log_state.last_log_id.unwrap();
            assert_eq!(last_log_id.index, 2, "last_log_id should be the last entry");
        }

        // Clean up
        drop(temp_dir);
    }

    // Unit tests for purge error conditions - Requirements 8.3

    #[tokio::test]
    async fn test_purge_with_smaller_log_id() {
        // Test that purging with a LogId smaller than the current last_purged_log_id panics
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 2, 3, 4, 5
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 4),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 3,
                    slot_idx: 3,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 5),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 4,
                    slot_idx: 4,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // First purge up to index 3
        let purge_log_id_1 = LogId::new(LeaderId::new(1, 1), 3);
        {
            log_store
                .do_purge(purge_log_id_1)
                .await
                .expect("First purge should succeed");
        }

        // Try to purge with a smaller LogId (index 2)
        let purge_log_id_2 = LogId::new(LeaderId::new(1, 1), 2);
        {
            log_store
                .do_purge(purge_log_id_2)
                .await
                .expect("Second purge should succeed");
        }
    }

    #[tokio::test]
    async fn test_purge_with_equal_log_id() {
        // Test that purging with a LogId equal to the current last_purged_log_id panics
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 2, 3
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // First purge up to index 2
        let purge_log_id = LogId::new(LeaderId::new(1, 1), 2);
        {
            log_store
                .do_purge(purge_log_id)
                .await
                .expect("First purge should succeed");
        }

        // Try to purge with the same LogId again - this should panic
        {
            log_store
                .do_purge(purge_log_id)
                .await
                .expect("Second purge should succeed");
        }
    }

    #[tokio::test]
    async fn test_purge_first_entry() {
        // Test purging just the first entry
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 2, 3
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Purge up to index 1 (first entry only)
        let purge_log_id = LogId::new(LeaderId::new(1, 1), 1);
        {
            log_store
                .do_purge(purge_log_id)
                .await
                .expect("Purge should succeed");
        }

        // Should have entries 2 and 3 remaining
        let remaining_entries = {
            log_store
                .do_try_get_log_entries(1..=3)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            remaining_entries.len(),
            2,
            "Should have 2 entries remaining after purging first entry"
        );
        assert_eq!(remaining_entries[0].log_id.index, 2);
        assert_eq!(remaining_entries[1].log_id.index, 3);

        // Verify metadata was updated
        let log_state = {
            log_store
                .do_get_log_state()
                .await
                .expect("Get log state should succeed")
        };

        assert!(
            log_state.last_purged_log_id.is_some(),
            "last_purged_log_id should be set"
        );
        let last_purged = log_state.last_purged_log_id.unwrap();
        assert_eq!(last_purged.index, 1, "last_purged_log_id should be 1");
    }

    #[tokio::test]
    async fn test_purge_all_entries() {
        // Test purging all entries
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 2, 3
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Purge all entries (up to index 3)
        let purge_log_id = LogId::new(LeaderId::new(1, 1), 3);
        {
            log_store
                .do_purge(purge_log_id)
                .await
                .expect("Purge should succeed");
        }

        // Should have no entries remaining
        let remaining_entries = {
            log_store
                .do_try_get_log_entries(1..=3)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            remaining_entries.len(),
            0,
            "Should have no entries remaining after purging all"
        );

        // Verify metadata was updated
        let log_state = {
            log_store
                .do_get_log_state()
                .await
                .expect("Get log state should succeed")
        };

        assert!(
            log_state.last_purged_log_id.is_some(),
            "last_purged_log_id should be set"
        );
        let last_purged = log_state.last_purged_log_id.unwrap();
        assert_eq!(last_purged.index, 3, "last_purged_log_id should be 3");

        // last_log_id should equal last_purged_log_id when all entries are purged
        assert_eq!(
            log_state.last_log_id, log_state.last_purged_log_id,
            "last_log_id should equal last_purged_log_id when all entries are purged"
        );
    }

    #[tokio::test]
    async fn test_purge_with_gaps() {
        // Test purging when there are gaps in the indices
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write entries at indices 1, 3, 5, 7 (with gaps)
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 3),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 5),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 2,
                    slot_idx: 2,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 7),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 3,
                    slot_idx: 3,
                    entries: vec![],
                }),
            },
        ];

        // Write entries
        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Purge up to index 4 (which doesn't exist, but should purge 1 and 3)
        let purge_log_id = LogId::new(LeaderId::new(1, 1), 4);
        {
            log_store
                .do_purge(purge_log_id)
                .await
                .expect("Purge should succeed");
        }

        // Should have entries 5 and 7 remaining
        let remaining_entries = {
            log_store
                .do_try_get_log_entries(1..=7)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            remaining_entries.len(),
            2,
            "Should have 2 entries remaining after purging up to gap"
        );
        assert_eq!(remaining_entries[0].log_id.index, 5);
        assert_eq!(remaining_entries[1].log_id.index, 7);
    }

    #[tokio::test]
    async fn test_purge_persistence_across_instances() {
        // Test that purge metadata persists when creating a new log store instance
        let (temp_dir, engine) = create_test_db_with_cfs();

        // Write and purge with first instance
        {
            let engine_clone = engine.clone();
            let log_store =
                RocksdbLogStore::new(engine.clone()).expect("Initialization should succeed");

            let entries: Vec<Entry<KiwiTypeConfig>> = vec![
                Entry {
                    log_id: LogId::new(LeaderId::new(1, 1), 1),
                    payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                        db_id: 0,
                        slot_idx: 0,
                        entries: vec![],
                    }),
                },
                Entry {
                    log_id: LogId::new(LeaderId::new(1, 1), 2),
                    payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                        db_id: 1,
                        slot_idx: 1,
                        entries: vec![],
                    }),
                },
                Entry {
                    log_id: LogId::new(LeaderId::new(1, 1), 3),
                    payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                        db_id: 2,
                        slot_idx: 2,
                        entries: vec![],
                    }),
                },
            ];

            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();

            // Purge up to index 2
            let purge_log_id = LogId::new(LeaderId::new(1, 1), 2);

            log_store
                .do_purge(purge_log_id)
                .await
                .expect("Purge should succeed");
        }

        // Create new instance and verify purge persisted
        {
            let log_store =
                RocksdbLogStore::new(engine.clone()).expect("Initialization should succeed");

            let log_state = {
                log_store
                    .do_get_log_state()
                    .await
                    .expect("Get log state should succeed")
            };

            // Verify purge metadata persists
            assert!(
                log_state.last_purged_log_id.is_some(),
                "last_purged_log_id should persist across instances"
            );
            let last_purged = log_state.last_purged_log_id.unwrap();
            assert_eq!(last_purged.index, 2, "last_purged_log_id should be 2");

            // Verify purged entries are still gone
            let remaining_entries = {
                log_store
                    .do_try_get_log_entries(1..=3)
                    .await
                    .expect("Read should succeed")
            };

            assert_eq!(remaining_entries.len(), 1, "Only entry 3 should remain");
            assert_eq!(remaining_entries[0].log_id.index, 3);
        }

        // Clean up
        drop(temp_dir);
    }

    // Unit tests for Clone functionality - Requirements 10.4

    #[tokio::test]
    async fn test_clone_basic() {
        // Test that cloning creates an independent handle to the same data
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Clone the log store
        let cloned_store = log_store.clone();

        // Both stores should be usable
        drop(log_store);
        drop(cloned_store);
    }

    #[tokio::test]
    async fn test_clone_shares_data() {
        // Test that cloned instances share the same underlying data
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write an entry using the original store
        let entry = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                db_id: 0,
                slot_idx: 0,
                entries: vec![],
            }),
        };

        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            let key = encode_log_key(entry.log_id.index);
            let value = serialize(&entry).unwrap();
            batch.put_cf(&logs_cf, &key, &value);

            engine_clone.write(batch).unwrap();
        }

        // Clone the store
        let cloned_store = log_store.clone();

        // Read from the cloned store - should see the same data
        let read_entries = {
            cloned_store
                .do_try_get_log_entries(1..=1)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            read_entries.len(),
            1,
            "Cloned store should see the same data"
        );
        assert_eq!(read_entries[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_clone_in_different_tasks() {
        // Test using cloned stores in different async tasks
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write some initial entries
        let entries: Vec<Entry<KiwiTypeConfig>> = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
        ];

        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            for entry in &entries {
                let key = encode_log_key(entry.log_id.index);
                let value = serialize(&entry).unwrap();
                batch.put_cf(&logs_cf, &key, &value);
            }

            engine_clone.write(batch).unwrap();
        }

        // Clone the store for use in another task
        let cloned_store = log_store.clone();

        // Spawn a task that uses the cloned store
        let task_handle = tokio::spawn(async move {
            let read_entries = {
                cloned_store
                    .do_try_get_log_entries(1..=2)
                    .await
                    .expect("Read should succeed")
            };
            read_entries.len()
        });

        // Use the original store in this task
        let read_entries = {
            log_store
                .do_try_get_log_entries(1..=2)
                .await
                .expect("Read should succeed")
        };

        // Wait for the spawned task to complete
        let task_result = task_handle
            .await
            .expect("Task should complete successfully");

        // Both should see the same data
        assert_eq!(read_entries.len(), 2, "Original store should see 2 entries");
        assert_eq!(task_result, 2, "Cloned store in task should see 2 entries");
    }

    #[tokio::test]
    async fn test_clone_data_consistency() {
        // Test that modifications through one clone are visible through another
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Clone the store
        let cloned_store = log_store.clone();

        // Write an entry through the original store
        let entry1 = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                db_id: 0,
                slot_idx: 0,
                entries: vec![],
            }),
        };

        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            let key = encode_log_key(entry1.log_id.index);
            let value = serialize(&entry1).unwrap();
            batch.put_cf(&logs_cf, &key, &value);

            engine_clone.write(batch).unwrap();
        }

        // Read from the cloned store - should see the new entry
        let read_entries = {
            cloned_store
                .do_try_get_log_entries(1..=1)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            read_entries.len(),
            1,
            "Cloned store should see entry written by original"
        );
        assert_eq!(read_entries[0].log_id.index, 1);

        // Write another entry through the cloned store
        let entry2 = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 2),
            payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                db_id: 1,
                slot_idx: 1,
                entries: vec![],
            }),
        };

        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            let key = encode_log_key(entry2.log_id.index);
            let value = serialize(&entry2).unwrap();
            batch.put_cf(&logs_cf, &key, &value);

            engine_clone.write(batch).unwrap();
        }

        // Read from the original store - should see both entries
        let read_entries = {
            log_store
                .do_try_get_log_entries(1..=2)
                .await
                .expect("Read should succeed")
        };

        assert_eq!(
            read_entries.len(),
            2,
            "Original store should see entry written by clone"
        );
        assert_eq!(read_entries[0].log_id.index, 1);
        assert_eq!(read_entries[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn test_clone_multiple_times() {
        // Test that cloning multiple times works correctly
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let engine_clone = engine.clone();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Write an entry
        let entry = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                db_id: 0,
                slot_idx: 0,
                entries: vec![],
            }),
        };

        {
            let logs_cf = engine_clone.cf_handle(LOGS_CF).unwrap();
            let mut batch = WriteBatch::default();

            let key = encode_log_key(entry.log_id.index);
            let value = serialize(&entry).unwrap();
            batch.put_cf(&logs_cf, &key, &value);

            engine_clone.write(batch).unwrap();
        }

        // Create multiple clones
        let clone1 = log_store.clone();
        let clone2 = log_store.clone();
        let clone3 = clone1.clone();

        // All clones should see the same data
        for (i, store) in [&log_store, &clone1, &clone2, &clone3].iter().enumerate() {
            let read_entries = {
                store
                    .do_try_get_log_entries(1..=1)
                    .await
                    .expect("Read should succeed")
            };

            assert_eq!(read_entries.len(), 1, "Clone {} should see the entry", i);
            assert_eq!(read_entries[0].log_id.index, 1);
        }
    }

    // Tests for append callback invocation (the fix: callback must always be called)

    /// Verifies that on a successful append, the callback is invoked and the entries are persisted.
    /// Uses `RaftLogStorageExt::blocking_append` which internally creates a `LogFlushed` callback
    /// and awaits it — if the callback is never called the future would hang forever.
    #[tokio::test]
    async fn test_append_callback_called_on_success() {
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let mut log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        let entries = vec![
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 1),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 0,
                    slot_idx: 0,
                    entries: vec![],
                }),
            },
            Entry {
                log_id: LogId::new(LeaderId::new(1, 1), 2),
                payload: EntryPayload::<KiwiTypeConfig>::Normal(Binlog {
                    db_id: 1,
                    slot_idx: 1,
                    entries: vec![],
                }),
            },
        ];

        // blocking_append constructs LogFlushed internally and awaits the callback.
        // If our fix is wrong and callback is skipped on the success path, this would
        // either hang or return an error.
        let result = log_store.blocking_append(entries).await;
        assert!(
            result.is_ok(),
            "blocking_append should succeed: {:?}",
            result
        );

        // Verify entries were actually persisted
        let read_entries = {
            log_store
                .do_try_get_log_entries(1..=2)
                .await
                .expect("Read should succeed")
        };
        assert_eq!(read_entries.len(), 2, "Both entries should be persisted");
        assert_eq!(read_entries[0].log_id.index, 1);
        assert_eq!(read_entries[1].log_id.index, 2);
    }

    /// Verifies that appending an empty list still invokes the callback (no hang).
    #[tokio::test]
    async fn test_append_callback_called_on_empty_entries() {
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let mut log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        let result = log_store
            .blocking_append(Vec::<Entry<KiwiTypeConfig>>::new())
            .await;
        assert!(
            result.is_ok(),
            "blocking_append with empty entries should succeed: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_clone_with_vote_and_committed() {
        // Test that clones share vote and committed state
        let (_temp_dir, engine) = create_test_db_with_cfs();
        let log_store = RocksdbLogStore::new(engine).expect("Initialization should succeed");

        // Save vote using original store
        let vote = Vote::new(5, 3);
        {
            log_store
                .do_save_vote(&vote)
                .await
                .expect("Save vote should succeed");
        }

        // Save committed using original store
        let committed = Some(LogId::new(LeaderId::new(5, 3), 100));
        {
            log_store
                .do_save_committed(committed)
                .await
                .expect("Save committed should succeed");
        }

        // Clone the store
        let cloned_store = log_store.clone();

        // Read vote from cloned store
        let read_vote = {
            cloned_store
                .do_read_vote()
                .await
                .expect("Read vote should succeed")
        };

        assert!(read_vote.is_some(), "Cloned store should see the vote");
        let read_vote = read_vote.unwrap();
        assert_eq!(read_vote.leader_id().term, 5);
        assert_eq!(read_vote.leader_id().node_id, 3);

        // Read committed from cloned store
        let read_committed = {
            cloned_store
                .do_read_committed()
                .await
                .expect("Read committed should succeed")
        };

        assert!(
            read_committed.is_some(),
            "Cloned store should see the committed log ID"
        );
        let read_committed = read_committed.unwrap();
        assert_eq!(read_committed.index, 100);
    }
}
