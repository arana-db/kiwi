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

use rocksdb::ReadOptions;
use snafu::OptionExt;
use snafu::ResultExt;

use crate::base_data_value_format::ParsedBaseDataValue;
use crate::base_meta_value_format::ParsedHashesMetaValue;
use crate::error::{InvalidFormatSnafu, OptionNoneSnafu, RocksSnafu};
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
        let meta_cf = &cfs[0];
        let data_cf = &cfs[1];

        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&db.snapshot());

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
                return InvalidFormatSnafu {
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

    pub fn hset(&self, _key: &[u8], _field: &[u8], _value: &[u8]) -> Result<i32> {
        unimplemented!("hset is not implemented");
    }
}
