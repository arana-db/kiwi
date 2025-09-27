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
use crate::{BaseMetaKey, ColumnFamilyIndex, DataType, Redis, Result};

impl Redis {
    /// Delete one or more hash fields
    pub fn hdel(&self, _key: &[u8], _fields: &[Vec<u8>]) -> Result<i32> {
        unimplemented!("hdel is not implemented");
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
    pub fn hkeys(&self, _key: &[u8]) -> Result<Vec<String>> {
        unimplemented!("hkeys is not implemented");
    }

    /// Get the number of fields in a hash
    pub fn hlen(&self, _key: &[u8]) -> Result<i32> {
        unimplemented!("hlen is not implemented");
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
}
