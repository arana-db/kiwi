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

use bytes::BytesMut;
use chrono::Utc;
use once_cell::sync::OnceCell;
use std::{mem::ManuallyDrop, sync::Arc};

use rocksdb::{
    CompactionDecision, DB, DEFAULT_COLUMN_FAMILY_NAME, ReadOptions, Snapshot,
    compaction_filter::CompactionFilter, compaction_filter_factory::CompactionFilterFactory,
};

use crate::{
    DataType,
    base_meta_value_format::ParsedBaseMetaValue,
    coding::decode_fixed,
    list_meta_value_format::ParsedListsMetaValue,
    storage_define::{
        ENCODED_KEY_DELIM_SIZE, NEED_TRANSFORM_CHARACTER, PREFIX_RESERVE_LENGTH,
        SUFFIX_RESERVE_LENGTH, VERSION_LENGTH, seek_userkey_delim,
    },
};

const DATA_FILTER_NAME: &std::ffi::CStr = c"DataCompactionFilter";
const DATA_FILTER_FACTORY_NAME: &std::ffi::CStr = c"DataCompactionFilterFactory";

#[derive(Debug)]
enum MetaLookup {
    Valid,
    NotFound,
    Unavailable,
}

pub struct DataCompactionFilter {
    db: Option<Arc<DB>>,
    /// Snapshot and read options captured at filter creation time to provide
    /// a consistent view of metadata during compaction.
    snapshot_ctx: Option<SnapshotContext>,
    data_type: DataType,
    cur_key: BytesMut,
    meta_not_found: bool,
    cur_meta_version: u64,
    cur_meta_etime: u64,
}

impl DataCompactionFilter {
    pub fn new(db: Option<Arc<DB>>, data_type: DataType) -> Self {
        let snapshot_ctx = db.as_ref().map(|db| SnapshotContext::new(Arc::clone(db)));

        Self {
            db,
            snapshot_ctx,
            data_type,
            cur_key: BytesMut::new(),
            meta_not_found: false,
            cur_meta_version: 0,
            cur_meta_etime: 0,
        }
    }

    /// build meta key from data key
    ///
    /// ## data_key format
    /// ```text
    /// | reserve1 | encoded_key | version | Data / index | reserve2 |   
    /// |    8B    |     N B     |   8B    |       8B     |   16B    |
    /// ```
    ///
    /// The 'encoded_key' is the encoded user key with delimiter:
    /// - Original user key is encoded by `encode_user_key()` function
    /// - Special characters: `\x00` in user key is escaped to `\x00\x01`
    /// - Delimiter: `\x00\x00` is appended at the end to mark the end of encoded key
    /// - Format: `[encoded_user_key_content]\x00\x00`
    ///
    /// Example:
    /// - User key: `"mykey"` → Encoded: `"mykey\x00\x00"`
    /// - User key: `"test\x00key"` → Encoded: `"test\x00\x01key\x00\x00"`
    /// ## meta_key format
    /// ```text
    /// | reserve1 | encoded_key | reserve2 |
    /// |    8B    |     N B     |   16B    |
    /// ```
    fn build_meta_key(key: &[u8]) -> Option<Vec<u8>> {
        if key.len() <= PREFIX_RESERVE_LENGTH + ENCODED_KEY_DELIM_SIZE {
            return None;
        }

        let encoded = &key[PREFIX_RESERVE_LENGTH..];

        // seek userkey delim
        // it will return the index of the first 0x00 after the encoded key
        // if not found, return the length of the encoded key
        let encoded_len = seek_userkey_delim(encoded);
        if encoded_len == encoded.len() {
            return None;
        }

        let prefix_end = PREFIX_RESERVE_LENGTH + encoded_len;
        let mut meta_key = Vec::with_capacity(prefix_end + SUFFIX_RESERVE_LENGTH);
        meta_key.extend_from_slice(&key[..prefix_end]);
        meta_key.resize(
            prefix_end + SUFFIX_RESERVE_LENGTH,
            NEED_TRANSFORM_CHARACTER as u8,
        );

        Some(meta_key)
    }

    fn extract_data_version(key: &[u8]) -> Option<u64> {
        if key.len() <= PREFIX_RESERVE_LENGTH + VERSION_LENGTH {
            return None;
        }
        let encoded = &key[PREFIX_RESERVE_LENGTH..];
        let encoded_len = seek_userkey_delim(encoded);
        let version_offset = PREFIX_RESERVE_LENGTH + encoded_len;
        Some(decode_fixed::<u64>(
            &key[version_offset..version_offset + VERSION_LENGTH],
        ))
    }

    fn parse_meta_value(&self, value: &[u8]) -> Option<(u64, u64)> {
        if value.is_empty() {
            return None;
        }

        let meta_type = DataType::try_from(value[0]).ok()?;
        if meta_type != self.data_type {
            return None;
        }

        let raw = BytesMut::from(value);
        match meta_type {
            DataType::List => ParsedListsMetaValue::new(raw)
                .ok()
                .map(|m| (m.version(), m.etime())),
            DataType::Hash | DataType::Set | DataType::ZSet => ParsedBaseMetaValue::new(raw)
                .ok()
                .map(|m| (m.version(), m.etime())),
            _ => None,
        }
    }

    fn ensure_meta_state(&mut self, meta_key: &[u8]) -> MetaLookup {
        if self.cur_key.as_ref() != meta_key {
            self.cur_key = BytesMut::from(meta_key);
            self.meta_not_found = false;
            self.cur_meta_version = 0;
            self.cur_meta_etime = 0;

            let (Some(db), Some(snapshot_ctx)) = (self.db.as_ref(), self.snapshot_ctx.as_ref())
            else {
                return MetaLookup::Unavailable;
            };
            let Some(cf) = db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME) else {
                return MetaLookup::Unavailable;
            };

            match db.get_cf_opt(&cf, meta_key, snapshot_ctx.read_opts()) {
                Ok(Some(v)) => {
                    if let Some((ver, etime)) = self.parse_meta_value(&v) {
                        self.cur_meta_version = ver;
                        self.cur_meta_etime = etime;
                        self.meta_not_found = false;
                    } else {
                        log::debug!(
                            "DataCompactionFilter: meta parse/type mismatch for key {meta_key:?}"
                        );
                        self.meta_not_found = true;
                    }
                }
                Ok(None) => self.meta_not_found = true,
                Err(e) => {
                    log::warn!("DataCompactionFilter: failed to read meta key {meta_key:?}: {e}");
                    return MetaLookup::Unavailable;
                }
            }
        }

        if self.meta_not_found {
            MetaLookup::NotFound
        } else {
            MetaLookup::Valid
        }
    }
}

impl CompactionFilter for DataCompactionFilter {
    fn name(&self) -> &std::ffi::CStr {
        DATA_FILTER_NAME
    }

    fn filter(&mut self, _level: u32, key: &[u8], _value: &[u8]) -> CompactionDecision {
        let Some(meta_key) = Self::build_meta_key(key) else {
            return CompactionDecision::Keep;
        };

        match self.ensure_meta_state(&meta_key) {
            MetaLookup::Unavailable => CompactionDecision::Keep,
            MetaLookup::NotFound => CompactionDecision::Remove,
            MetaLookup::Valid => {
                let cur_time = Utc::now().timestamp_micros() as u64;
                if self.cur_meta_etime != 0 && self.cur_meta_etime < cur_time {
                    return CompactionDecision::Remove;
                }

                match Self::extract_data_version(key) {
                    Some(ver) if self.cur_meta_version > ver => CompactionDecision::Remove,
                    _ => CompactionDecision::Keep,
                }
            }
        }
    }
}

struct SnapshotContext {
    /// Keeps the underlying DB alive for the lifetime of the snapshot.
    _db: Arc<DB>,
    snapshot: ManuallyDrop<Snapshot<'static>>,
    read_opts: ReadOptions,
}

impl SnapshotContext {
    fn new(db: Arc<DB>) -> Self {
        let snapshot = db.snapshot();
        // SAFETY: `snapshot` is tied to the lifetime of `db`. We hold an `Arc<DB>` inside the
        // context to guarantee `db` stays alive until after the snapshot is explicitly dropped.
        let snapshot = unsafe { std::mem::transmute::<Snapshot<'_>, Snapshot<'static>>(snapshot) };
        let mut read_opts = ReadOptions::default();
        read_opts.set_snapshot(&snapshot);
        Self {
            _db: db,
            snapshot: ManuallyDrop::new(snapshot),
            read_opts,
        }
    }

    fn read_opts(&self) -> &ReadOptions {
        &self.read_opts
    }
}

impl Drop for SnapshotContext {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.snapshot);
        }
        // `Arc<DB>` and `ReadOptions` are dropped automatically afterwards.
    }
}

pub struct DataCompactionFilterFactory {
    db: Arc<OnceCell<Arc<DB>>>,
    data_type: DataType,
}

impl DataCompactionFilterFactory {
    pub fn new(db: Arc<OnceCell<Arc<DB>>>, data_type: DataType) -> Self {
        Self { db, data_type }
    }
}

impl CompactionFilterFactory for DataCompactionFilterFactory {
    type Filter = DataCompactionFilter;

    fn create(
        &mut self,
        _context: rocksdb::compaction_filter_factory::CompactionFilterContext,
    ) -> Self::Filter {
        let db = self.db.get().cloned();
        DataCompactionFilter::new(db, self.data_type)
    }

    fn name(&self) -> &std::ffi::CStr {
        DATA_FILTER_FACTORY_NAME
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base_key_format::BaseKey;
    use crate::base_meta_value_format::BaseMetaValue;
    use crate::list_meta_value_format::ListsMetaValue;
    use crate::storage_define::SUFFIX_RESERVE_LENGTH;
    use crate::unique_test_db_path;
    use bytes::BufMut;
    use rocksdb::{
        ColumnFamilyDescriptor, Options, compaction_filter_factory::CompactionFilterContext,
    };

    fn setup_db_for_filter_test(path: &std::path::Path) -> (Arc<OnceCell<Arc<DB>>>, Arc<DB>) {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let meta_cf = ColumnFamilyDescriptor::new(DEFAULT_COLUMN_FAMILY_NAME, Options::default());
        let data_cf = ColumnFamilyDescriptor::new("hash_data_cf", Options::default());

        let db = DB::open_cf_descriptors(&db_opts, path, vec![meta_cf, data_cf]).unwrap();
        let db = Arc::new(db);

        let db_cell = Arc::new(OnceCell::new());
        db_cell.set(Arc::clone(&db)).unwrap();

        (db_cell, db)
    }

    /// Helper to create a generic data key for testing purposes.
    fn encode_data_key(key: &[u8], version: u64) -> Vec<u8> {
        let base_key = BaseKey::new(key);
        let mut encoded = base_key.encode().unwrap();
        // remove suffix
        encoded.truncate(encoded.len() - SUFFIX_RESERVE_LENGTH);
        // append version
        encoded.put_u64_le(version);
        // re-append suffix
        encoded.resize(encoded.len() + SUFFIX_RESERVE_LENGTH, 0);
        encoded.to_vec()
    }

    fn put_meta(db: &Arc<DB>, user_key: &[u8], data_type: DataType, version: u64, etime: u64) {
        let meta_key = BaseKey::new(user_key).encode().unwrap();
        match data_type {
            DataType::List => {
                let mut meta_value =
                    ListsMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
                meta_value.set_version(version);
                meta_value.set_etime(etime);
                db.put(&meta_key, &meta_value.encode()).unwrap();
            }
            DataType::Hash | DataType::Set | DataType::ZSet => {
                let mut meta_value =
                    BaseMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
                meta_value.inner.data_type = data_type;
                meta_value.set_version(version);
                meta_value.set_etime(etime);
                db.put(&meta_key, &meta_value.encode()).unwrap();
            }
            _ => panic!("unsupported data type for meta: {data_type:?}"),
        }
    }

    #[test]
    fn test_removes_data_if_meta_is_missing() {
        let path = unique_test_db_path();
        let (db_cell, _db) = setup_db_for_filter_test(&path);

        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::Hash);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        let data_key = encode_data_key(b"mykey", 1);

        let decision = filter.filter(0, &data_key, b"");
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_keeps_data_if_meta_is_valid_for_hash() {
        let path = unique_test_db_path();
        let (db_cell, db) = setup_db_for_filter_test(&path);

        put_meta(&db, b"mykey", DataType::Hash, 1, 0);

        // Allow some time for the snapshot to see the write
        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::Hash);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        let data_key = encode_data_key(b"mykey", 1); // data version matches meta version

        let decision = filter.filter(0, &data_key, b"");
        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_keeps_data_if_meta_is_valid_for_set() {
        let path = unique_test_db_path();
        let (db_cell, db) = setup_db_for_filter_test(&path);

        put_meta(&db, b"set_key", DataType::Set, 1, 0);

        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::Set);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        let data_key = encode_data_key(b"set_key", 1);
        let decision = filter.filter(0, &data_key, b"");

        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_keeps_data_if_meta_is_valid_for_zset() {
        let path = unique_test_db_path();
        let (db_cell, db) = setup_db_for_filter_test(&path);

        put_meta(&db, b"zset_key", DataType::ZSet, 1, 0);

        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::ZSet);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        let data_key = encode_data_key(b"zset_key", 1);
        let decision = filter.filter(0, &data_key, b"");

        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_keeps_data_if_meta_is_valid_for_list() {
        let path = unique_test_db_path();
        let (db_cell, db) = setup_db_for_filter_test(&path);

        put_meta(&db, b"list_key", DataType::List, 1, 0);

        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::List);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        let data_key = encode_data_key(b"list_key", 1);
        let decision = filter.filter(0, &data_key, b"");

        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_removes_data_if_meta_is_expired() {
        let path = unique_test_db_path();
        let (db_cell, db) = setup_db_for_filter_test(&path);

        let past_time = Utc::now().timestamp_micros() as u64 - 1000;
        let meta_key = BaseKey::new(b"mykey").encode().unwrap();
        let mut meta_value = BaseMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
        meta_value.inner.data_type = DataType::Hash;
        meta_value.set_version(1);
        meta_value.set_etime(past_time); // expired
        db.put(&meta_key, &meta_value.encode()).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::Hash);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        let data_key = encode_data_key(b"mykey", 1);

        let decision = filter.filter(0, &data_key, b"");
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_removes_data_if_version_is_older() {
        let path = unique_test_db_path();
        let (db_cell, db) = setup_db_for_filter_test(&path);

        let meta_key = BaseKey::new(b"mykey").encode().unwrap();
        let mut meta_value = BaseMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
        meta_value.inner.data_type = DataType::Hash;
        meta_value.set_version(2); // meta version is 2
        meta_value.set_etime(0);
        db.put(&meta_key, &meta_value.encode()).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));

        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::Hash);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        let data_key = encode_data_key(b"mykey", 1); // data version is 1 (older)

        let decision = filter.filter(0, &data_key, b"");
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_removes_data_if_type_mismatches() {
        let path = unique_test_db_path();
        let (db_cell, db) = setup_db_for_filter_test(&path);

        put_meta(&db, b"mykey", DataType::List, 1, 0);

        std::thread::sleep(std::time::Duration::from_millis(10));

        // Filter is for Hashes
        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::Hash);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        let data_key = encode_data_key(b"mykey", 1);

        let decision = filter.filter(0, &data_key, b"");
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_keeps_data_if_key_is_malformed() {
        let path = unique_test_db_path();
        let (db_cell, _) = setup_db_for_filter_test(&path);

        let mut factory = DataCompactionFilterFactory::new(db_cell, DataType::Hash);
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let mut filter = factory.create(context);

        // This key is too short to have a valid meta key derived from it
        let malformed_key = b"short";

        let decision = filter.filter(0, malformed_key, b"");
        assert!(matches!(decision, CompactionDecision::Keep));
    }
}
