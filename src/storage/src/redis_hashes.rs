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

//! Redis hashes operations implementation
//! This module provides hash operations for Redis storage

use bytes::Bytes;
use kstd::lock_mgr::ScopeRecordLock;
use rocksdb::{ReadOptions, WriteBatch};
use snafu::{OptionExt, ResultExt};

use crate::base_data_value_format::{BaseDataValue, ParsedBaseDataValue};
use crate::base_meta_value_format::{HashesMetaValue, ParsedHashesMetaValue};
use crate::error::{OptionNoneSnafu, RedisErrSnafu, RocksSnafu};
use crate::get_db_and_cfs;
use crate::member_data_key_format::MemberDataKey;
use crate::redis_sets::glob_match;
use crate::util::is_tail_wildcard;
use crate::{BaseMetaKey, ColumnFamilyIndex, DataType, Redis, Result};

impl Redis {
    /// Delete one or more hash fields
    pub fn hdel(&self, key: &[u8], fields: &[Vec<u8>]) -> Result<i32> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        // Remove duplicate fields
        let mut field_set = std::collections::HashSet::new();
        let filtered_fields: Vec<&[u8]> = fields
            .iter()
            .filter(|field| field_set.insert(field.as_slice()))
            .map(|f| f.as_slice())
            .collect();

        let mut batch = WriteBatch::default();
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let base_meta_key = BaseMetaKey::new(key).encode()?;
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        match db
            .get_cf_opt(meta_cf, &base_meta_key, &read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let mut meta_val = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;
                if meta_val.is_stale() {
                    return Ok(0);
                }
                if meta_val.inner.data_type != DataType::Hash {
                    return RedisErrSnafu {
                        message: format!(
                            "Wrong type of value, expected: {:?}, got: {:?}",
                            DataType::Hash,
                            meta_val.inner.data_type
                        ),
                    }
                    .fail();
                }

                let version = meta_val.version();
                let mut del_cnt = 0i32;

                for field in filtered_fields {
                    let data_key = MemberDataKey::new(key, version, field);
                    if db
                        .get_cf_opt(data_cf, &data_key.encode()?, &read_options)
                        .context(RocksSnafu)?
                        .is_some()
                    {
                        del_cnt += 1;
                        batch.delete_cf(data_cf, data_key.encode()?);
                    }
                }

                if del_cnt > 0 {
                    let current = meta_val.count();
                    let to_del = del_cnt as u64;
                    if to_del > current {
                        return RedisErrSnafu {
                            message: "hash size underflow".to_string(),
                        }
                        .fail();
                    }
                    meta_val.set_count(current - to_del);
                    batch.put_cf(meta_cf, &base_meta_key, meta_val.encoded());
                    db.write_opt(batch, &self.write_options)
                        .context(RocksSnafu)?;
                }

                Ok(del_cnt)
            }
            None => Ok(0),
        }
    }

    /// Determine if a hash field exists
    pub fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool> {
        let ret = self.hget(key, field)?;
        Ok(ret.is_some())
    }

    /// Get the value of a hash field
    pub fn hget(&self, _key: &[u8], _field: &[u8]) -> Result<Option<String>> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let base_meta_key = BaseMetaKey::new(_key).encode()?;
        if let Some(meta_val_bytes) = db
            .get_cf_opt(meta_cf, &base_meta_key, &read_options)
            .context(RocksSnafu)?
        {
            let meta_val = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;
            if meta_val.is_stale() {
                return Ok(None);
            }
            if meta_val.inner.data_type != DataType::Hash {
                return RedisErrSnafu {
                    message: format!(
                        "Wrong type of value, expected: {:?}, got: {:?}",
                        DataType::Hash,
                        meta_val.inner.data_type
                    ),
                }
                .fail();
            }
            let version = meta_val.version();
            let data_key = MemberDataKey::new(_key, version, _field);
            if let Some(data_val_bytes) = db
                .get_cf_opt(data_cf, &data_key.encode()?, &read_options)
                .context(RocksSnafu)?
            {
                let mut data_val = ParsedBaseDataValue::new(&data_val_bytes[..])?;
                data_val.strip_suffix();
                return Ok(Some(
                    String::from_utf8_lossy(&data_val.user_value()).to_string(),
                ));
            }
        }

        Ok(None)
    }

    /// Get all the fields in a hash
    pub fn hkeys(&self, key: &[u8]) -> Result<Vec<String>> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let base_meta_key = BaseMetaKey::new(key).encode()?;
        match db
            .get_cf_opt(meta_cf, &base_meta_key, &read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let meta_val = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;
                if meta_val.is_stale() {
                    return Ok(Vec::new());
                }
                if meta_val.inner.data_type != DataType::Hash {
                    return RedisErrSnafu {
                        message: format!(
                            "Wrong type of value, expected: {:?}, got: {:?}",
                            DataType::Hash,
                            meta_val.inner.data_type
                        ),
                    }
                    .fail();
                }

                let version = meta_val.version();
                let data_key = MemberDataKey::new(key, version, &[]);
                let prefix = data_key.encode_seek_key()?;

                let iter = db.iterator_cf_opt(
                    data_cf,
                    ReadOptions::default(),
                    rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                );
                let mut fields = Vec::new();
                for item in iter {
                    let (k, _) = item.context(RocksSnafu)?;
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    let parsed_key = crate::member_data_key_format::ParsedMemberDataKey::new(&k)?;
                    fields.push(String::from_utf8_lossy(parsed_key.data()).to_string());
                }
                Ok(fields)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Get the number of fields in a hash
    pub fn hlen(&self, key: &[u8]) -> Result<i32> {
        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::MetaCF);
        debug_assert_eq!(cfs.len(), 1);
        let meta_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key).encode()?;
        match db
            .get_cf_opt(meta_cf, &base_meta_key, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let meta_val = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;
                if meta_val.is_stale() {
                    return Ok(0);
                }
                if meta_val.inner.data_type != DataType::Hash {
                    return RedisErrSnafu {
                        message: format!(
                            "Wrong type of value, expected: {:?}, got: {:?}",
                            DataType::Hash,
                            meta_val.inner.data_type
                        ),
                    }
                    .fail();
                }
                Ok(meta_val.count() as i32)
            }
            None => Ok(0),
        }
    }

    pub fn hset(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<i32> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let mut batch = WriteBatch::default();
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);
        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let create_new_hash =
            |batch: &mut WriteBatch, key: &[u8], field: &[u8], value: &[u8]| -> Result<()> {
                let mut hashes_meta =
                    HashesMetaValue::new(Bytes::copy_from_slice(&1u64.to_le_bytes()));
                hashes_meta.inner.data_type = DataType::Hash;
                let version = hashes_meta.update_version();

                batch.put_cf(meta_cf, &base_meta_key, hashes_meta.encode());

                let data_key = MemberDataKey::new(key, version, field);
                let data_value = BaseDataValue::new(value.to_vec());
                batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());

                Ok(())
            };

        match db
            .get_cf_opt(meta_cf, &base_meta_key, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let mut parsed_meta = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;

                if parsed_meta.data_type() != DataType::Hash {
                    if parsed_meta.is_stale() {
                        create_new_hash(&mut batch, key, field, value)?;
                        db.write_opt(batch, &self.write_options)
                            .context(RocksSnafu)?;
                        return Ok(1);
                    } else {
                        return RedisErrSnafu {
                            message: format!(
                                "Wrong type of value, expected: {:?}, got: {:?}",
                                DataType::Hash,
                                parsed_meta.inner.data_type
                            ),
                        }
                        .fail();
                    }
                }

                if parsed_meta.is_stale() || parsed_meta.count() == 0 {
                    let version = parsed_meta.initial_meta_value();
                    parsed_meta.set_count(1);
                    batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                    let data_key = MemberDataKey::new(key, version, field);
                    let data_value = BaseDataValue::new(value.to_vec());
                    batch.put_cf(data_cf, data_key.encode()?, data_value.encode());

                    db.write_opt(batch, &self.write_options)
                        .context(RocksSnafu)?;
                    Ok(1)
                } else {
                    let version = parsed_meta.version();
                    let data_key = MemberDataKey::new(key, version, field);

                    match db
                        .get_cf_opt(data_cf, &data_key.encode()?, &self.read_options)
                        .context(RocksSnafu)?
                    {
                        Some(existing_data_bytes) => {
                            let mut existing_data =
                                ParsedBaseDataValue::new(&existing_data_bytes[..])?;
                            existing_data.strip_suffix();

                            if existing_data.user_value() == value {
                                Ok(0)
                            } else {
                                let data_value = BaseDataValue::new(value.to_vec());
                                batch.put_cf(data_cf, data_key.encode()?, data_value.encode());

                                db.write_opt(batch, &self.write_options)
                                    .context(RocksSnafu)?;
                                Ok(0)
                            }
                        }
                        None => {
                            if !parsed_meta.check_modify_count(1) {
                                return RedisErrSnafu {
                                    message: "hash size overflow".to_string(),
                                }
                                .fail();
                            }
                            parsed_meta.modify_count(1);
                            batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                            let data_value = BaseDataValue::new(value.to_vec());
                            batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());

                            db.write_opt(batch, &self.write_options)
                                .context(RocksSnafu)?;
                            Ok(1)
                        }
                    }
                }
            }
            None => {
                create_new_hash(&mut batch, key, field, value)?;
                db.write_opt(batch, &self.write_options)
                    .context(RocksSnafu)?;
                Ok(1)
            }
        }
    }

    /// Get all fields and values in a hash
    pub fn hgetall(&self, key: &[u8]) -> Result<Vec<(String, String)>> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let base_meta_key = BaseMetaKey::new(key).encode()?;
        match db
            .get_cf_opt(meta_cf, &base_meta_key, &read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let meta_val = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;
                if meta_val.is_stale() {
                    return Ok(Vec::new());
                }
                if meta_val.inner.data_type != DataType::Hash {
                    return RedisErrSnafu {
                        message: format!(
                            "Wrong type of value, expected: {:?}, got: {:?}",
                            DataType::Hash,
                            meta_val.inner.data_type
                        ),
                    }
                    .fail();
                }

                let version = meta_val.version();
                let data_key = MemberDataKey::new(key, version, &[]);
                let prefix = data_key.encode_seek_key()?;

                let iter = db.iterator_cf_opt(
                    data_cf,
                    ReadOptions::default(),
                    rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                );
                let mut result = Vec::new();
                for item in iter {
                    let (k, v) = item.context(RocksSnafu)?;
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    let parsed_key = crate::member_data_key_format::ParsedMemberDataKey::new(&k)?;
                    let mut parsed_val = ParsedBaseDataValue::new(&*v)?;
                    parsed_val.strip_suffix();
                    result.push((
                        String::from_utf8_lossy(parsed_key.data()).to_string(),
                        String::from_utf8_lossy(&parsed_val.user_value()).to_string(),
                    ));
                }
                Ok(result)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Get all values in a hash
    pub fn hvals(&self, key: &[u8]) -> Result<Vec<String>> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let base_meta_key = BaseMetaKey::new(key).encode()?;
        match db
            .get_cf_opt(meta_cf, &base_meta_key, &read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let meta_val = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;
                if meta_val.is_stale() {
                    return Ok(Vec::new());
                }
                if meta_val.inner.data_type != DataType::Hash {
                    return RedisErrSnafu {
                        message: format!(
                            "Wrong type of value, expected: {:?}, got: {:?}",
                            DataType::Hash,
                            meta_val.inner.data_type
                        ),
                    }
                    .fail();
                }

                let version = meta_val.version();
                let data_key = MemberDataKey::new(key, version, &[]);
                let prefix = data_key.encode_seek_key()?;

                let iter = db.iterator_cf_opt(
                    data_cf,
                    ReadOptions::default(),
                    rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                );
                let mut values = Vec::new();
                for item in iter {
                    let (k, v) = item.context(RocksSnafu)?;
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    let mut parsed_val = ParsedBaseDataValue::new(&*v)?;
                    parsed_val.strip_suffix();
                    values.push(String::from_utf8_lossy(&parsed_val.user_value()).to_string());
                }
                Ok(values)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Get multiple hash field values
    pub fn hmget(&self, key: &[u8], fields: &[Vec<u8>]) -> Result<Vec<Option<String>>> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let base_meta_key = BaseMetaKey::new(key).encode()?;
        let mut results = Vec::new();

        match db
            .get_cf_opt(meta_cf, &base_meta_key, &read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let meta_val = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;
                if meta_val.is_stale() {
                    for _ in fields {
                        results.push(None);
                    }
                    return Ok(results);
                }
                if meta_val.inner.data_type != DataType::Hash {
                    return RedisErrSnafu {
                        message: format!(
                            "Wrong type of value, expected: {:?}, got: {:?}",
                            DataType::Hash,
                            meta_val.inner.data_type
                        ),
                    }
                    .fail();
                }

                let version = meta_val.version();
                for field in fields {
                    let data_key = MemberDataKey::new(key, version, field);
                    match db
                        .get_cf_opt(data_cf, &data_key.encode()?, &read_options)
                        .context(RocksSnafu)?
                    {
                        Some(data_val_bytes) => {
                            let mut data_val = ParsedBaseDataValue::new(&data_val_bytes[..])?;
                            data_val.strip_suffix();
                            results.push(Some(
                                String::from_utf8_lossy(&data_val.user_value()).to_string(),
                            ));
                        }
                        None => {
                            results.push(None);
                        }
                    }
                }
            }
            None => {
                for _ in fields {
                    results.push(None);
                }
            }
        }
        Ok(results)
    }

    /// Set multiple hash fields
    pub fn hmset(&self, key: &[u8], field_values: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        // Remove duplicates by keeping the last value
        let mut field_map = std::collections::HashMap::new();
        for (field, value) in field_values {
            field_map.insert(field.clone(), value.clone());
        }

        let mut batch = WriteBatch::default();
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);
        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let create_new_hash = |batch: &mut WriteBatch,
                               key: &[u8],
                               field_map: &std::collections::HashMap<Vec<u8>, Vec<u8>>|
         -> Result<()> {
            let count = field_map.len() as u64;
            let mut hashes_meta =
                HashesMetaValue::new(Bytes::copy_from_slice(&count.to_le_bytes()));
            hashes_meta.inner.data_type = DataType::Hash;
            let version = hashes_meta.update_version();

            batch.put_cf(meta_cf, &base_meta_key, hashes_meta.encode());

            for (field, value) in field_map {
                let data_key = MemberDataKey::new(key, version, field);
                let data_value = BaseDataValue::new(value.clone());
                batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());
            }
            Ok(())
        };

        match db
            .get_cf_opt(meta_cf, &base_meta_key, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let mut parsed_meta = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;

                if parsed_meta.data_type() != DataType::Hash {
                    if parsed_meta.is_stale() {
                        create_new_hash(&mut batch, key, &field_map)?;
                        db.write_opt(batch, &self.write_options)
                            .context(RocksSnafu)?;
                        return Ok(());
                    } else {
                        return RedisErrSnafu {
                            message: format!(
                                "Wrong type of value, expected: {:?}, got: {:?}",
                                DataType::Hash,
                                parsed_meta.inner.data_type
                            ),
                        }
                        .fail();
                    }
                }

                if parsed_meta.is_stale() || parsed_meta.count() == 0 {
                    let version = parsed_meta.initial_meta_value();
                    parsed_meta.set_count(field_map.len() as u64);
                    batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                    for (field, value) in &field_map {
                        let data_key = MemberDataKey::new(key, version, field);
                        let data_value = BaseDataValue::new(value.clone());
                        batch.put_cf(data_cf, data_key.encode()?, data_value.encode());
                    }
                } else {
                    let version = parsed_meta.version();
                    let mut new_fields_count = 0i32;

                    for (field, value) in &field_map {
                        let data_key = MemberDataKey::new(key, version, field);
                        let exists = db
                            .get_cf_opt(data_cf, &data_key.encode()?, &self.read_options)
                            .context(RocksSnafu)?
                            .is_some();

                        if !exists {
                            new_fields_count += 1;
                        }

                        let data_value = BaseDataValue::new(value.clone());
                        batch.put_cf(data_cf, data_key.encode()?, data_value.encode());
                    }

                    if new_fields_count > 0 {
                        if !parsed_meta.check_modify_count(new_fields_count as u64) {
                            return RedisErrSnafu {
                                message: "hash size overflow".to_string(),
                            }
                            .fail();
                        }
                        parsed_meta.modify_count(new_fields_count as u64);
                        batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());
                    }
                }

                db.write_opt(batch, &self.write_options)
                    .context(RocksSnafu)?;
            }
            None => {
                create_new_hash(&mut batch, key, &field_map)?;
                db.write_opt(batch, &self.write_options)
                    .context(RocksSnafu)?;
            }
        }
        Ok(())
    }

    /// Set field only if it doesn't exist
    pub fn hsetnx(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<i32> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let mut batch = WriteBatch::default();
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);
        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let create_new_hash =
            |batch: &mut WriteBatch, key: &[u8], field: &[u8], value: &[u8]| -> Result<()> {
                let mut hashes_meta =
                    HashesMetaValue::new(Bytes::copy_from_slice(&1u64.to_le_bytes()));
                hashes_meta.inner.data_type = DataType::Hash;
                let version = hashes_meta.update_version();

                batch.put_cf(meta_cf, &base_meta_key, hashes_meta.encode());

                let data_key = MemberDataKey::new(key, version, field);
                let data_value = BaseDataValue::new(value.to_vec());
                batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());
                Ok(())
            };

        match db
            .get_cf_opt(meta_cf, &base_meta_key, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let mut parsed_meta = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;

                if parsed_meta.data_type() != DataType::Hash {
                    if parsed_meta.is_stale() {
                        create_new_hash(&mut batch, key, field, value)?;
                        db.write_opt(batch, &self.write_options)
                            .context(RocksSnafu)?;
                        return Ok(1);
                    } else {
                        return RedisErrSnafu {
                            message: format!(
                                "Wrong type of value, expected: {:?}, got: {:?}",
                                DataType::Hash,
                                parsed_meta.inner.data_type
                            ),
                        }
                        .fail();
                    }
                }

                if parsed_meta.is_stale() || parsed_meta.count() == 0 {
                    let version = parsed_meta.initial_meta_value();
                    parsed_meta.set_count(1);
                    batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                    let data_key = MemberDataKey::new(key, version, field);
                    let data_value = BaseDataValue::new(value.to_vec());
                    batch.put_cf(data_cf, data_key.encode()?, data_value.encode());

                    db.write_opt(batch, &self.write_options)
                        .context(RocksSnafu)?;
                    Ok(1)
                } else {
                    let version = parsed_meta.version();
                    let data_key = MemberDataKey::new(key, version, field);

                    match db
                        .get_cf_opt(data_cf, &data_key.encode()?, &self.read_options)
                        .context(RocksSnafu)?
                    {
                        Some(_) => Ok(0), // Field already exists
                        None => {
                            if !parsed_meta.check_modify_count(1) {
                                return RedisErrSnafu {
                                    message: "hash size overflow".to_string(),
                                }
                                .fail();
                            }
                            parsed_meta.modify_count(1);
                            batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                            let data_value = BaseDataValue::new(value.to_vec());
                            batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());

                            db.write_opt(batch, &self.write_options)
                                .context(RocksSnafu)?;
                            Ok(1)
                        }
                    }
                }
            }
            None => {
                create_new_hash(&mut batch, key, field, value)?;
                db.write_opt(batch, &self.write_options)
                    .context(RocksSnafu)?;
                Ok(1)
            }
        }
    }

    /// Increment the integer value of a hash field
    pub fn hincrby(&self, key: &[u8], field: &[u8], increment: i64) -> Result<i64> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let mut batch = WriteBatch::default();
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);
        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let create_new_hash =
            |batch: &mut WriteBatch, key: &[u8], field: &[u8], value: i64| -> Result<()> {
                let mut hashes_meta =
                    HashesMetaValue::new(Bytes::copy_from_slice(&1u64.to_le_bytes()));
                hashes_meta.inner.data_type = DataType::Hash;
                let version = hashes_meta.update_version();

                batch.put_cf(meta_cf, &base_meta_key, hashes_meta.encode());

                let data_key = MemberDataKey::new(key, version, field);
                let value_str = value.to_string();
                let data_value = BaseDataValue::new(value_str.into_bytes());
                batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());
                Ok(())
            };

        match db
            .get_cf_opt(meta_cf, &base_meta_key, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let mut parsed_meta = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;

                if parsed_meta.data_type() != DataType::Hash {
                    if parsed_meta.is_stale() {
                        create_new_hash(&mut batch, key, field, increment)?;
                        db.write_opt(batch, &self.write_options)
                            .context(RocksSnafu)?;
                        return Ok(increment);
                    } else {
                        return RedisErrSnafu {
                            message: format!(
                                "Wrong type of value, expected: {:?}, got: {:?}",
                                DataType::Hash,
                                parsed_meta.inner.data_type
                            ),
                        }
                        .fail();
                    }
                }

                if parsed_meta.is_stale() || parsed_meta.count() == 0 {
                    let version = parsed_meta.initial_meta_value();
                    parsed_meta.set_count(1);
                    batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                    let data_key = MemberDataKey::new(key, version, field);
                    let value_str = increment.to_string();
                    let data_value = BaseDataValue::new(value_str.into_bytes());
                    batch.put_cf(data_cf, data_key.encode()?, data_value.encode());

                    db.write_opt(batch, &self.write_options)
                        .context(RocksSnafu)?;
                    Ok(increment)
                } else {
                    let version = parsed_meta.version();
                    let data_key = MemberDataKey::new(key, version, field);

                    match db
                        .get_cf_opt(data_cf, &data_key.encode()?, &self.read_options)
                        .context(RocksSnafu)?
                    {
                        Some(old_val_bytes) => {
                            let mut old_val = ParsedBaseDataValue::new(&old_val_bytes[..])?;
                            old_val.strip_suffix();
                            let user_val = old_val.user_value();
                            let old_str = String::from_utf8_lossy(&user_val);
                            let old_int: i64 = old_str.parse().map_err(|_| {
                                RedisErrSnafu {
                                    message: "hash value is not an integer".to_string(),
                                }
                                .build()
                            })?;

                            let new_val =
                                old_int.checked_add(increment).context(OptionNoneSnafu {
                                    message: "integer overflow",
                                })?;

                            let new_val_str = new_val.to_string();
                            let data_value = BaseDataValue::new(new_val_str.into_bytes());
                            batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());

                            db.write_opt(batch, &self.write_options)
                                .context(RocksSnafu)?;
                            Ok(new_val)
                        }
                        None => {
                            if !parsed_meta.check_modify_count(1) {
                                return RedisErrSnafu {
                                    message: "hash size overflow".to_string(),
                                }
                                .fail();
                            }
                            parsed_meta.modify_count(1);
                            batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                            let value_str = increment.to_string();
                            let data_value = BaseDataValue::new(value_str.into_bytes());
                            batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());

                            db.write_opt(batch, &self.write_options)
                                .context(RocksSnafu)?;
                            Ok(increment)
                        }
                    }
                }
            }
            None => {
                create_new_hash(&mut batch, key, field, increment)?;
                db.write_opt(batch, &self.write_options)
                    .context(RocksSnafu)?;
                Ok(increment)
            }
        }
    }

    /// Increment the float value of a hash field
    pub fn hincrbyfloat(&self, key: &[u8], field: &[u8], increment: f64) -> Result<f64> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let mut batch = WriteBatch::default();
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);
        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let create_new_hash =
            |batch: &mut WriteBatch, key: &[u8], field: &[u8], value: f64| -> Result<()> {
                let mut hashes_meta =
                    HashesMetaValue::new(Bytes::copy_from_slice(&1u64.to_le_bytes()));
                hashes_meta.inner.data_type = DataType::Hash;
                let version = hashes_meta.update_version();

                batch.put_cf(meta_cf, &base_meta_key, hashes_meta.encode());

                let data_key = MemberDataKey::new(key, version, field);
                let value_str = value.to_string();
                let data_value = BaseDataValue::new(value_str.into_bytes());
                batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());
                Ok(())
            };

        match db
            .get_cf_opt(meta_cf, &base_meta_key, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let mut parsed_meta = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;

                if parsed_meta.data_type() != DataType::Hash {
                    if parsed_meta.is_stale() {
                        create_new_hash(&mut batch, key, field, increment)?;
                        db.write_opt(batch, &self.write_options)
                            .context(RocksSnafu)?;
                        return Ok(increment);
                    } else {
                        return RedisErrSnafu {
                            message: format!(
                                "Wrong type of value, expected: {:?}, got: {:?}",
                                DataType::Hash,
                                parsed_meta.inner.data_type
                            ),
                        }
                        .fail();
                    }
                }

                if parsed_meta.is_stale() || parsed_meta.count() == 0 {
                    let version = parsed_meta.initial_meta_value();
                    parsed_meta.set_count(1);
                    batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                    let data_key = MemberDataKey::new(key, version, field);
                    let value_str = increment.to_string();
                    let data_value = BaseDataValue::new(value_str.into_bytes());
                    batch.put_cf(data_cf, data_key.encode()?, data_value.encode());

                    db.write_opt(batch, &self.write_options)
                        .context(RocksSnafu)?;
                    Ok(increment)
                } else {
                    let version = parsed_meta.version();
                    let data_key = MemberDataKey::new(key, version, field);

                    match db
                        .get_cf_opt(data_cf, &data_key.encode()?, &self.read_options)
                        .context(RocksSnafu)?
                    {
                        Some(old_val_bytes) => {
                            let mut old_val = ParsedBaseDataValue::new(&old_val_bytes[..])?;
                            old_val.strip_suffix();
                            let user_val = old_val.user_value();
                            let old_str = String::from_utf8_lossy(&user_val);
                            let old_float: f64 = old_str.parse().map_err(|_| {
                                RedisErrSnafu {
                                    message: "hash value is not a valid float".to_string(),
                                }
                                .build()
                            })?;

                            let new_val = old_float + increment;
                            if !new_val.is_finite() {
                                return RedisErrSnafu {
                                    message: "increment would produce NaN or Infinity".to_string(),
                                }
                                .fail();
                            }

                            let new_val_str = new_val.to_string();
                            let data_value = BaseDataValue::new(new_val_str.into_bytes());
                            batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());

                            db.write_opt(batch, &self.write_options)
                                .context(RocksSnafu)?;
                            Ok(new_val)
                        }
                        None => {
                            if !parsed_meta.check_modify_count(1) {
                                return RedisErrSnafu {
                                    message: "hash size overflow".to_string(),
                                }
                                .fail();
                            }
                            parsed_meta.modify_count(1);
                            batch.put_cf(meta_cf, &base_meta_key, parsed_meta.encoded());

                            let value_str = increment.to_string();
                            let data_value = BaseDataValue::new(value_str.into_bytes());
                            batch.put_cf(data_cf, &data_key.encode()?, data_value.encode());

                            db.write_opt(batch, &self.write_options)
                                .context(RocksSnafu)?;
                            Ok(increment)
                        }
                    }
                }
            }
            None => {
                create_new_hash(&mut batch, key, field, increment)?;
                db.write_opt(batch, &self.write_options)
                    .context(RocksSnafu)?;
                Ok(increment)
            }
        }
    }

    /// Get the length of a hash field value
    pub fn hstrlen(&self, key: &[u8], field: &[u8]) -> Result<i32> {
        match self.hget(key, field)? {
            Some(value) => Ok(value.len() as i32),
            None => Ok(0),
        }
    }

    /// Scan hash fields with cursor-based iteration
    /// Returns (next_cursor, vec_of_(field, value)_tuples)
    pub fn hscan(
        &self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<(u64, Vec<(String, String)>)> {
        let (db, cfs) = get_db_and_cfs!(
            self,
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF
        );
        debug_assert_eq!(cfs.len(), 2);
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let base_meta_key = BaseMetaKey::new(key).encode()?;
        let default_count = 10usize;
        let step_length = count.unwrap_or(default_count);
        let mut rest = step_length as i64; 

        // Read meta
        match db
            .get_cf_opt(meta_cf, &base_meta_key, &read_options)
            .context(RocksSnafu)?
        {
            Some(meta_val_bytes) => {
                let meta_val = ParsedHashesMetaValue::new(&meta_val_bytes[..])?;

                if meta_val.is_stale() {
                    return Ok((0, Vec::new()));
                }
                if meta_val.data_type() != DataType::Hash {
                    return RedisErrSnafu {
                        message: format!(
                            "Wrong type of value, expected: {:?}, got: {:?}",
                            DataType::Hash,
                            meta_val.data_type()
                        ),
                    }
                    .fail();
                }

                let mut start_point = String::new();
                let version = meta_val.version();
                let mut next_cursor = cursor;

                if cursor > 0 {
                    if let Some(pat) = pattern {
                        if let Some(sp) = self.get_scan_start_point(DataType::Hash, key, pat.as_bytes(), cursor)? {
                            start_point = sp;
                        }
                    } else if let Some(sp) = self.get_scan_start_point(DataType::Hash, key, &[], cursor)? {
                            start_point = sp;
                    }
                } else if let Some(pat) = pattern {
                    if is_tail_wildcard(pat) {
                        start_point = pat[..pat.len() - 1].to_string();
                    }
                }
                let sub_field: Option<&str> = match pattern {
                    Some(pat) if is_tail_wildcard(pat) => Some(&pat[..pat.len() - 1]),
                    _ => None,
                };

                let hashes_data_prefix = MemberDataKey::new(key, version, sub_field.map(|s| s.as_bytes()).unwrap_or(&[]));
                let prefix = hashes_data_prefix.encode_seek_key()?;
                
                let start_key = if !start_point.is_empty() {
                    let hashes_start_key = MemberDataKey::new(key, version, start_point.as_bytes());
                    hashes_start_key.encode_seek_key()?
                } else {
                    prefix.clone()
                };

                let mut field_values = Vec::new();

                let mut iter = db.iterator_cf_opt(
                    data_cf,
                    read_options,
                    rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
                );

                while rest > 0 {
                    if let Some(item) = iter.next() {
                        let (k, v) = item.context(RocksSnafu)?;
                        
                        if !k.starts_with(&prefix) {
                            break;
                        }

                        let parsed_hashes_data_key = crate::member_data_key_format::ParsedMemberDataKey::new(&k)?;
                        let field = String::from_utf8_lossy(parsed_hashes_data_key.data()).to_string();

                        let matches_pattern = if let Some(pat) = pattern {
                            glob_match(pat, field.as_str())
                        } else {
                            true
                        };

                        if matches_pattern && rest > 0 {
                            let parsed_internal_value = ParsedBaseDataValue::new(&*v)?;
                            let value = String::from_utf8_lossy(&parsed_internal_value.user_value()).to_string();
                            field_values.push((field.clone(), value));
                        }
                        rest -= 1;
                    } else {
                        break;
                    }
                }

                next_cursor = if let Some(item) = iter.next() {
                    let (k, _v) = item.context(RocksSnafu)?;
                    
                    if k.starts_with(&prefix) {
                        let new_cursor = next_cursor + step_length as u64;

                        let parsed_key = crate::member_data_key_format::ParsedMemberDataKey::new(&k)?;
                        let next_field = String::from_utf8_lossy(parsed_key.data()).to_string();

                        self.store_scan_next_point(DataType::Hash, key, pattern.unwrap_or("").as_bytes(), new_cursor, next_field.as_bytes())?;
                        new_cursor
                    } else {
                        0
                    }
                } else {
                    0
                };
                drop(iter);
                Ok((next_cursor, field_values))
            }
            None => {
                Ok((0, Vec::new()))
            }
        }
    }
}
