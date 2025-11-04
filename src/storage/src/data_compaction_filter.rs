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
use std::sync::Arc;

use rocksdb::{
    CompactionDecision, DB, DEFAULT_COLUMN_FAMILY_NAME, ReadOptions,
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
    read_opts: ReadOptions,
    data_type: DataType,
    cur_key: BytesMut,
    meta_not_found: bool,
    cur_meta_version: u64,
    cur_meta_etime: u64,
}

impl DataCompactionFilter {
    pub fn new(db: Option<Arc<DB>>, data_type: DataType) -> Self {
        let read_opts = db
            .as_ref()
            .map(|db| {
                let snapshot = db.snapshot();
                let mut opts = ReadOptions::default();
                opts.set_snapshot(&snapshot);
                opts
            })
            .unwrap_or_default();

        Self {
            db,
            read_opts,
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
    /// ```
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
    /// ```
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

            let Some(db) = &self.db else {
                return MetaLookup::Unavailable;
            };
            let Some(cf) = db.cf_handle(DEFAULT_COLUMN_FAMILY_NAME) else {
                return MetaLookup::Unavailable;
            };

            match db.get_cf_opt(&cf, meta_key, &self.read_opts) {
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
        // TODO : check logic here
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
