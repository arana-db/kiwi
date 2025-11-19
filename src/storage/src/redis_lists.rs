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

//! Redis lists operations implementation
//! This module provides list operations for Redis storage

use bytes::BytesMut;
use rocksdb::WriteBatch;
use snafu::{OptionExt, ResultExt};

use crate::{
    Result,
    base_data_value_format::{BaseDataValue, ParsedBaseDataValue},
    base_key_format::BaseMetaKey,
    base_value_format::DataType,
    error::{InvalidFormatSnafu, KeyNotFoundSnafu, OptionNoneSnafu, RocksSnafu},
    get_db_and_cfs,
    list_meta_value_format::{ListsMetaValue, ParsedListsMetaValue},
    lists_data_key_format::ListsDataKey,
    redis::{ColumnFamilyIndex, Redis},
    storage_impl::BeforeOrAfter,
};
use kstd::lock_mgr::ScopeRecordLock;

impl Redis {
    /// Core implementation for pushing values to a list
    /// position: true for left (head), false for right (tail)
    /// allow_create: if true, creates new list when key doesn't exist
    /// acquire_lock: if true, acquires key lock (should be false when called from rpoplpush)
    fn push_core(
        &self,
        key: &[u8],
        values: &[Vec<u8>],
        position: bool,
        allow_create: bool,
        acquire_lock: bool,
    ) -> Result<Option<i64>> {
        // Acquire key lock to prevent concurrent modifications
        let _lock = if acquire_lock {
            let key_str = String::from_utf8_lossy(key);
            Some(ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str))
        } else {
            None
        };

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        if values.is_empty() {
            // For empty values array, check if key exists
            // If key exists, return current count; if not, return 0
            return match db.get(&meta_key).context(RocksSnafu)? {
                Some(meta_value) => {
                    let parsed_meta =
                        ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                    if parsed_meta.is_valid() {
                        Ok(Some(parsed_meta.count() as i64))
                    } else {
                        Ok(Some(0))
                    }
                }
                None => Ok(Some(0)),
            };
        }

        // Get existing metadata
        let mut parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let mut parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    // Initialize if invalid/expired
                    parsed.initial_meta_value();
                }
                parsed
            }
            None => {
                if !allow_create {
                    return Ok(None); // Key doesn't exist and creation not allowed
                }
                // Create new metadata
                let meta_value = ListsMetaValue::new(0u64.to_le_bytes().to_vec());
                let encoded = meta_value.encode();
                let mut parsed = ParsedListsMetaValue::new(encoded)?;
                parsed.initial_meta_value();
                parsed
            }
        };

        let mut batch = WriteBatch::default();
        let current_version = parsed_meta.version();
        let current_count = parsed_meta.count();

        if position {
            // Left (head) push
            if current_count == 0 {
                // Empty list - store elements starting from left_index + 1
                for (i, value) in values.iter().rev().enumerate() {
                    let storage_index =
                        parsed_meta.left_index() - values.len() as u64 + i as u64 + 1;
                    let data_key = ListsDataKey::new(key, current_version, storage_index);
                    let encoded_data_key = data_key.encode()?;

                    let data_value = BaseDataValue::new(value.clone());
                    let encoded_data_value = data_value.encode();

                    batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
                }

                // Update metadata
                parsed_meta.modify_left_index(values.len() as u64);
                parsed_meta.modify_count(values.len() as u64);
            } else {
                // Non-empty list - read existing elements first, then rewrite everything
                let mut existing_elements = Vec::new();

                // Read existing elements directly from storage using the old version
                for i in 0..current_count {
                    let storage_index = parsed_meta.left_index() + i + 1;
                    let data_key = ListsDataKey::new(key, current_version, storage_index);
                    let encoded_data_key = data_key.encode()?;

                    if let Some(data_value) = db
                        .get_cf(lists_data_cf, &encoded_data_key)
                        .context(RocksSnafu)?
                    {
                        let parsed_data =
                            ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                        existing_elements.push(parsed_data.user_value().to_vec());
                    }
                }

                // Clear existing elements using the old version
                for i in 0..current_count {
                    let storage_index = parsed_meta.left_index() + i + 1;
                    let data_key = ListsDataKey::new(key, current_version, storage_index);
                    let encoded_data_key = data_key.encode()?;
                    batch.delete_cf(lists_data_cf, encoded_data_key);
                }

                // Update version only when reorganizing the entire list
                let version = parsed_meta.update_version();

                // Store new elements at the head (in correct order for lpush)
                for (i, value) in values.iter().rev().enumerate() {
                    let storage_index =
                        parsed_meta.left_index() - values.len() as u64 + i as u64 + 1;
                    let data_key = ListsDataKey::new(key, version, storage_index);
                    let encoded_data_key = data_key.encode()?;

                    let data_value = BaseDataValue::new(value.clone());
                    let encoded_data_value = data_value.encode();

                    batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
                }

                // Store existing elements after the new ones
                for (i, value) in existing_elements.iter().enumerate() {
                    let storage_index = parsed_meta.left_index() + i as u64 + 1;
                    let data_key = ListsDataKey::new(key, version, storage_index);
                    let encoded_data_key = data_key.encode()?;

                    let data_value = BaseDataValue::new(value.clone());
                    let encoded_data_value = data_value.encode();

                    batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
                }

                // Update metadata - adjust left_index to accommodate new elements at head
                parsed_meta.set_left_index(parsed_meta.left_index() - values.len() as u64);
                parsed_meta.set_count(current_count + values.len() as u64);
                // Update right_index to reflect the new end position
                parsed_meta.set_right_index(parsed_meta.left_index() + parsed_meta.count());
            }
        } else {
            // Right (tail) push
            let is_empty_list = current_count == 0;

            if is_empty_list {
                // For empty list, store elements starting from left_index + 1
                // so they're accessible via standard lindex calculation
                for (i, value) in values.iter().enumerate() {
                    let storage_index = parsed_meta.left_index() + i as u64 + 1;
                    let data_key = ListsDataKey::new(key, current_version, storage_index);
                    let encoded_data_key = data_key.encode()?;

                    let data_value = BaseDataValue::new(value.clone());
                    let encoded_data_value = data_value.encode();

                    batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
                }

                // Update right_index to point to the last element
                parsed_meta.set_right_index(parsed_meta.left_index() + values.len() as u64);
            } else {
                // For non-empty list, append to the right contiguously
                // Elements should be stored at left_index + count + 1, left_index + count + 2, etc.
                for (i, value) in values.iter().enumerate() {
                    let storage_index = parsed_meta.left_index() + current_count + i as u64 + 1;
                    let data_key = ListsDataKey::new(key, current_version, storage_index);
                    let encoded_data_key = data_key.encode()?;

                    let data_value = BaseDataValue::new(value.clone());
                    let encoded_data_value = data_value.encode();

                    batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
                }

                // Update right_index to point to the last element
                parsed_meta.set_right_index(
                    parsed_meta.left_index() + current_count + values.len() as u64,
                );
            }

            parsed_meta.modify_count(values.len() as u64);
        }

        batch.put(&meta_key, parsed_meta.value());
        db.write(batch).context(RocksSnafu)?;

        // Update statistics
        self.update_specific_key_statistics(
            DataType::List,
            &String::from_utf8_lossy(key),
            values.len() as u64,
        )?;

        Ok(Some(parsed_meta.count() as i64))
    }

    /// Insert all the specified values at the head of the list stored at key
    pub fn lpush(&self, key: &[u8], values: &[Vec<u8>]) -> Result<i64> {
        match self.push_core(key, values, true, true, true)? {
            Some(count) => Ok(count),
            None => unreachable!("lpush always creates list when key doesn't exist"),
        }
    }

    /// Insert all the specified values at the tail of the list stored at key
    pub fn rpush(&self, key: &[u8], values: &[Vec<u8>]) -> Result<i64> {
        match self.push_core(key, values, false, true, true)? {
            Some(count) => Ok(count),
            None => unreachable!("rpush always creates list when key doesn't exist"),
        }
    }

    /// Insert all the specified values at the head of the list stored at key only if key exists
    pub fn lpushx(&self, key: &[u8], values: &[Vec<u8>]) -> Result<i64> {
        match self.push_core(key, values, true, false, true)? {
            Some(count) => Ok(count),
            None => Ok(0), // Key doesn't exist, return 0 per Redis protocol
        }
    }

    /// Insert all the specified values at the tail of the list stored at key only if key exists
    pub fn rpushx(&self, key: &[u8], values: &[Vec<u8>]) -> Result<i64> {
        match self.push_core(key, values, false, false, true)? {
            Some(count) => Ok(count),
            None => Ok(0), // Key doesn't exist, return 0 per Redis protocol
        }
    }

    /// Removes and returns the first element of the list stored at key
    pub fn lpop(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>> {
        // Acquire key lock to prevent concurrent modifications
        let key_str = String::from_utf8_lossy(key);
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let mut parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return Ok(None);
                }
                parsed
            }
            None => return Ok(None),
        };

        if parsed_meta.count() == 0 {
            return Ok(None);
        }

        let pop_count = count.unwrap_or(1).min(parsed_meta.count() as usize);
        let mut result = Vec::with_capacity(pop_count);
        let mut batch = WriteBatch::default();

        // Pop elements from the left (head)
        for i in 0..pop_count {
            let current_left_index = parsed_meta.left_index() + i as u64 + 1;
            let data_key = ListsDataKey::new(key, parsed_meta.version(), current_left_index);
            let encoded_data_key = data_key.encode()?;

            if let Some(data_value) = db
                .get_cf(lists_data_cf, &encoded_data_key)
                .context(RocksSnafu)?
            {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                result.push(parsed_data.user_value().to_vec());
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }
        }

        // Update metadata - increment left_index to move the head forward
        parsed_meta.set_left_index(parsed_meta.left_index() + pop_count as u64);
        parsed_meta.set_count(parsed_meta.count() - pop_count as u64);

        // Always preserve the metadata for empty lists to allow lpushx/rpushx operations
        batch.put(&meta_key, parsed_meta.value());

        db.write(batch).context(RocksSnafu)?;

        // Update statistics
        self.update_specific_key_statistics(
            DataType::List,
            &String::from_utf8_lossy(key),
            pop_count as u64,
        )?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    /// Internal version of rpop without locking (used by rpoplpush)
    fn rpop_internal(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>> {
        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let mut parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return Ok(None);
                }
                parsed
            }
            None => return Ok(None),
        };

        if parsed_meta.count() == 0 {
            return Ok(None);
        }

        let pop_count = count.unwrap_or(1).min(parsed_meta.count() as usize);
        let mut result = Vec::with_capacity(pop_count);
        let mut batch = WriteBatch::default();

        // Pop elements from the right (tail)
        for i in 0..pop_count {
            let current_right_index = parsed_meta.right_index() - i as u64;
            let data_key = ListsDataKey::new(key, parsed_meta.version(), current_right_index);
            let encoded_data_key = data_key.encode()?;

            if let Some(data_value) = db
                .get_cf(lists_data_cf, &encoded_data_key)
                .context(RocksSnafu)?
            {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                result.push(parsed_data.user_value().to_vec());
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }
        }

        // Update metadata
        parsed_meta.set_right_index(parsed_meta.right_index() - pop_count as u64);
        parsed_meta.set_count(parsed_meta.count() - pop_count as u64);

        // Always preserve the metadata for empty lists to allow lpushx/rpushx operations
        batch.put(&meta_key, parsed_meta.value());

        db.write(batch).context(RocksSnafu)?;

        // Update statistics
        self.update_specific_key_statistics(
            DataType::List,
            &String::from_utf8_lossy(key),
            pop_count as u64,
        )?;

        Ok(Some(result))
    }

    /// Removes and returns the last element of the list stored at key
    pub fn rpop(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>> {
        // Acquire key lock to prevent concurrent modifications
        let key_str = String::from_utf8_lossy(key);
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let mut parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return Ok(None);
                }
                parsed
            }
            None => return Ok(None),
        };

        if parsed_meta.count() == 0 {
            return Ok(None);
        }

        let pop_count = count.unwrap_or(1).min(parsed_meta.count() as usize);
        let mut result = Vec::with_capacity(pop_count);
        let mut batch = WriteBatch::default();

        // Pop elements from the right (tail)
        for i in 0..pop_count {
            let current_right_index = parsed_meta.right_index() - i as u64;
            let data_key = ListsDataKey::new(key, parsed_meta.version(), current_right_index);
            let encoded_data_key = data_key.encode()?;

            if let Some(data_value) = db
                .get_cf(lists_data_cf, &encoded_data_key)
                .context(RocksSnafu)?
            {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                result.push(parsed_data.user_value().to_vec());
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }
        }

        // Update metadata - decrement right_index to move the tail backward
        parsed_meta.set_right_index(parsed_meta.right_index() - pop_count as u64);
        parsed_meta.set_count(parsed_meta.count() - pop_count as u64);

        // Always preserve the metadata for empty lists to allow lpushx/rpushx operations
        batch.put(&meta_key, parsed_meta.value());

        db.write(batch).context(RocksSnafu)?;

        // Update statistics
        self.update_specific_key_statistics(
            DataType::List,
            &String::from_utf8_lossy(key),
            pop_count as u64,
        )?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    /// Returns the length of the list stored at key
    pub fn llen(&self, key: &[u8]) -> Result<i64> {
        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        let (db, _cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::MetaCF);

        match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed_meta = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if parsed_meta.is_valid() {
                    Ok(parsed_meta.count() as i64)
                } else {
                    Ok(0)
                }
            }
            None => Ok(0),
        }
    }

    /// Returns the element at index in the list stored at key
    pub fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Vec<u8>>> {
        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return Ok(None);
                }
                parsed
            }
            None => return Ok(None),
        };

        if parsed_meta.count() == 0 {
            return Ok(None);
        }

        // Convert negative index to positive
        let real_index = if index < 0 {
            parsed_meta.count() as i64 + index
        } else {
            index
        };

        // Check bounds
        if real_index < 0 || real_index >= parsed_meta.count() as i64 {
            return Ok(None);
        }

        // Calculate the actual storage index
        let storage_index = parsed_meta.left_index() + real_index as u64 + 1;
        let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
        let encoded_data_key = data_key.encode()?;

        match db
            .get_cf(lists_data_cf, &encoded_data_key)
            .context(RocksSnafu)?
        {
            Some(data_value) => {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                Ok(Some(parsed_data.user_value().to_vec()))
            }
            None => Ok(None),
        }
    }

    /// Returns the specified elements of the list stored at key
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return Ok(Vec::new());
                }
                parsed
            }
            None => return Ok(Vec::new()),
        };

        if parsed_meta.count() == 0 {
            return Ok(Vec::new());
        }

        let list_len = parsed_meta.count() as i64;

        // Normalize indices
        let real_start = if start < 0 {
            (list_len + start).max(0)
        } else {
            start.min(list_len - 1)
        };

        let real_stop = if stop < 0 {
            (list_len + stop).max(-1)
        } else {
            stop.min(list_len - 1)
        };

        if real_start > real_stop || real_start >= list_len {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();

        // Fetch elements in the range
        for i in real_start..=real_stop {
            let storage_index = parsed_meta.left_index() + i as u64 + 1;
            let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
            let encoded_data_key = data_key.encode()?;

            if let Some(data_value) = db
                .get_cf(lists_data_cf, &encoded_data_key)
                .context(RocksSnafu)?
            {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                result.push(parsed_data.user_value().to_vec());
            }
        }

        Ok(result)
    }

    /// Sets the list element at index to value
    pub fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> Result<()> {
        // Acquire key lock to prevent concurrent modifications
        let key_str = String::from_utf8_lossy(key);
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return KeyNotFoundSnafu {
                        key: String::from_utf8_lossy(key).to_string(),
                    }
                    .fail();
                }
                parsed
            }
            None => {
                return KeyNotFoundSnafu {
                    key: String::from_utf8_lossy(key).to_string(),
                }
                .fail();
            }
        };

        if parsed_meta.count() == 0 {
            return KeyNotFoundSnafu {
                key: String::from_utf8_lossy(key).to_string(),
            }
            .fail();
        }

        // Convert negative index to positive
        let real_index = if index < 0 {
            parsed_meta.count() as i64 + index
        } else {
            index
        };

        // Check bounds
        if real_index < 0 || real_index >= parsed_meta.count() as i64 {
            return InvalidFormatSnafu {
                message: "index out of range".to_string(),
            }
            .fail();
        }

        // Calculate the actual storage index
        let storage_index = parsed_meta.left_index() + real_index as u64 + 1;
        let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
        let encoded_data_key = data_key.encode()?;

        // Create new data value
        let data_value = BaseDataValue::new(value);
        let encoded_data_value = data_value.encode();

        // Update the element
        db.put_cf(lists_data_cf, &encoded_data_key, &encoded_data_value)
            .context(RocksSnafu)?;

        Ok(())
    }

    /// Trims the list to the specified range
    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> Result<()> {
        // Acquire key lock to prevent concurrent modifications
        let key_str = String::from_utf8_lossy(key);
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let mut parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return Ok(());
                }
                parsed
            }
            None => return Ok(()),
        };

        if parsed_meta.count() == 0 {
            return Ok(());
        }

        let list_len = parsed_meta.count() as i64;

        // Normalize indices
        let real_start = if start < 0 {
            (list_len + start).max(0)
        } else {
            start.min(list_len - 1)
        };

        let real_stop = if stop < 0 {
            (list_len + stop).max(-1)
        } else {
            stop.min(list_len - 1)
        };

        let mut batch = WriteBatch::default();

        if real_start > real_stop || real_start >= list_len {
            // Delete all elements
            for i in 0..parsed_meta.count() {
                let storage_index = parsed_meta.left_index() + i + 1;
                let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
                let encoded_data_key = data_key.encode()?;
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }
            batch.delete(&meta_key);
        } else {
            // Delete elements before start
            for i in 0..real_start {
                let storage_index = parsed_meta.left_index() + i as u64 + 1;
                let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
                let encoded_data_key = data_key.encode()?;
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }

            // Delete elements after stop
            for i in (real_stop + 1)..list_len {
                let storage_index = parsed_meta.left_index() + i as u64 + 1;
                let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
                let encoded_data_key = data_key.encode()?;
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }

            // Update metadata
            let new_count = (real_stop - real_start + 1) as u64;
            parsed_meta.set_count(new_count);
            let new_left_index = parsed_meta.left_index() + real_start as u64;
            parsed_meta.set_left_index(new_left_index);
            parsed_meta.set_right_index(new_left_index + new_count);

            batch.put(&meta_key, parsed_meta.value());
        }

        db.write(batch).context(RocksSnafu)?;

        Ok(())
    }

    /// Removes the first count occurrences of elements equal to value from the list
    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> Result<i64> {
        // Acquire key lock to prevent concurrent modifications
        let key_str = String::from_utf8_lossy(key);
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let mut parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return Ok(0);
                }
                parsed
            }
            None => return Ok(0),
        };

        if parsed_meta.count() == 0 {
            return Ok(0);
        }

        let mut removed_count = 0i64;
        let mut batch = WriteBatch::default();

        // Determine removal direction

        let max_removals = if count == 0 { i64::MAX } else { count.abs() };

        // Read all current elements
        let mut all_elements = Vec::new();
        for i in 0..parsed_meta.count() {
            let storage_index = parsed_meta.left_index() + i + 1;
            let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
            let encoded_data_key = data_key.encode()?;

            if let Some(data_value) = db
                .get_cf(lists_data_cf, &encoded_data_key)
                .context(RocksSnafu)?
            {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                all_elements.push(parsed_data.user_value().to_vec());
            }
        }

        // Filter out the elements to remove
        let mut remaining_elements = Vec::new();
        let mut removals_left = max_removals;

        if count >= 0 {
            // Remove from head to tail
            for element in all_elements {
                if removals_left > 0 && element == value {
                    removals_left -= 1;
                    removed_count += 1;
                } else {
                    remaining_elements.push(element);
                }
            }
        } else {
            // Remove from tail to head
            for element in all_elements.into_iter().rev() {
                if removals_left > 0 && element == value {
                    removals_left -= 1;
                    removed_count += 1;
                } else {
                    remaining_elements.push(element);
                }
            }
            remaining_elements.reverse();
        }

        if removed_count > 0 {
            // Clear all existing elements
            for i in 0..parsed_meta.count() {
                let storage_index = parsed_meta.left_index() + i + 1;
                let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
                let encoded_data_key = data_key.encode()?;
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }

            if remaining_elements.is_empty() {
                // List is now empty
                batch.delete(&meta_key);
            } else {
                // Write remaining elements contiguously
                for (i, element) in remaining_elements.iter().enumerate() {
                    let storage_index = parsed_meta.left_index() + i as u64 + 1;
                    let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
                    let encoded_data_key = data_key.encode()?;

                    let data_value = BaseDataValue::new(element.clone());
                    let encoded_data_value = data_value.encode();

                    batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
                }

                // Update count and right_index
                parsed_meta.set_count(remaining_elements.len() as u64);
                parsed_meta
                    .set_right_index(parsed_meta.left_index() + remaining_elements.len() as u64);
                batch.put(&meta_key, parsed_meta.value());
            }

            db.write(batch).context(RocksSnafu)?;

            // Update statistics
            self.update_specific_key_statistics(
                DataType::List,
                &String::from_utf8_lossy(key),
                removed_count as u64,
            )?;
        }

        Ok(removed_count)
    }

    /// Insert value in the list stored at key either before or after the reference value pivot
    pub fn linsert(
        &self,
        key: &[u8],
        before_or_after: BeforeOrAfter,
        pivot: &[u8],
        value: &[u8],
    ) -> Result<i64> {
        // Acquire key lock to prevent concurrent modifications
        let key_str = String::from_utf8_lossy(key);
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

        // Get existing metadata
        let mut parsed_meta = match db.get(&meta_key).context(RocksSnafu)? {
            Some(meta_value) => {
                let parsed = ParsedListsMetaValue::new(BytesMut::from(meta_value.as_slice()))?;
                if !parsed.is_valid() {
                    return KeyNotFoundSnafu {
                        key: String::from_utf8_lossy(key).to_string(),
                    }
                    .fail();
                }
                parsed
            }
            None => {
                return KeyNotFoundSnafu {
                    key: String::from_utf8_lossy(key).to_string(),
                }
                .fail();
            }
        };

        if parsed_meta.count() == 0 {
            return KeyNotFoundSnafu {
                key: String::from_utf8_lossy(key).to_string(),
            }
            .fail();
        }

        // Read all elements to find the pivot
        let mut all_elements = Vec::new();
        let mut pivot_index = None;

        for i in 0..parsed_meta.count() {
            let storage_index = parsed_meta.left_index() + i + 1;
            let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
            let encoded_data_key = data_key.encode()?;

            if let Some(data_value) = db
                .get_cf(lists_data_cf, &encoded_data_key)
                .context(RocksSnafu)?
            {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                let element = parsed_data.user_value().to_vec();

                // Check if this element matches the pivot
                if element == pivot {
                    pivot_index = Some(i as usize);
                }

                all_elements.push(element);
            }
        }

        // If pivot not found, return -1 per Redis protocol
        if pivot_index.is_none() {
            return Ok(-1);
        }

        let insert_position = match before_or_after {
            BeforeOrAfter::Before => pivot_index.unwrap(),
            BeforeOrAfter::After => pivot_index.unwrap() + 1,
        };

        // Insert the new value at the calculated position
        all_elements.insert(insert_position, value.to_vec());

        // Clear existing elements
        let mut batch = WriteBatch::default();
        for i in 0..parsed_meta.count() {
            let storage_index = parsed_meta.left_index() + i + 1;
            let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
            let encoded_data_key = data_key.encode()?;
            batch.delete_cf(lists_data_cf, encoded_data_key);
        }

        // Update version for consistency
        let new_version = parsed_meta.update_version();

        // Write all elements back
        for (i, element) in all_elements.iter().enumerate() {
            let storage_index = parsed_meta.left_index() + i as u64 + 1;
            let data_key = ListsDataKey::new(key, new_version, storage_index);
            let encoded_data_key = data_key.encode()?;

            let data_value = BaseDataValue::new(element.clone());
            let encoded_data_value = data_value.encode();

            batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
        }

        // Update metadata
        parsed_meta.set_count(all_elements.len() as u64);
        parsed_meta.set_right_index(parsed_meta.left_index() + all_elements.len() as u64);
        batch.put(&meta_key, parsed_meta.value());

        db.write(batch).context(RocksSnafu)?;

        // Update statistics
        self.update_specific_key_statistics(DataType::List, &String::from_utf8_lossy(key), 1)?;

        Ok(all_elements.len() as i64)
    }

    /// Atomically returns and removes the last element (tail) of the list stored at source,
    /// and pushes it to the first element (head) of the list stored at destination
    pub fn rpoplpush(&self, source_key: &[u8], destination_key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Acquire locks for both keys to ensure atomicity
        // Sort keys to prevent deadlocks
        let source_str = String::from_utf8_lossy(source_key);
        let dest_str = String::from_utf8_lossy(destination_key);

        // Acquire locks for both keys to ensure atomicity
        // For same key, only acquire one lock to avoid deadlock
        if source_str == dest_str {
            let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &source_str);

            // First, pop from source list using internal logic
            let popped_value = match self.rpop_internal(source_key, None)? {
                Some(values) => {
                    if values.is_empty() {
                        return Ok(None);
                    }
                    values[0].clone()
                }
                None => return Ok(None),
            };

            // Then, push to destination list using internal method to avoid double locking
            match self.push_core(
                destination_key,
                std::slice::from_ref(&popped_value),
                true,
                true,
                false,
            )? {
                Some(_) => {}
                None => unreachable!("Destination list creation should always succeed"),
            }

            return Ok(Some(popped_value));
        }

        // For different keys, acquire both locks in sorted order
        let _lock1;
        let _lock2;
        if source_str < dest_str {
            _lock1 = ScopeRecordLock::new(self.lock_mgr.as_ref(), &source_str);
            _lock2 = ScopeRecordLock::new(self.lock_mgr.as_ref(), &dest_str);
        } else {
            _lock1 = ScopeRecordLock::new(self.lock_mgr.as_ref(), &dest_str);
            _lock2 = ScopeRecordLock::new(self.lock_mgr.as_ref(), &source_str);
        }

        // First, pop from source list using internal logic
        let popped_value = match self.rpop_internal(source_key, None)? {
            Some(values) => {
                if values.is_empty() {
                    return Ok(None);
                }
                values[0].clone()
            }
            None => return Ok(None),
        };

        // Then, push to destination list using internal method to avoid double locking
        match self.push_core(
            destination_key,
            std::slice::from_ref(&popped_value),
            true,
            true,
            false,
        )? {
            Some(_) => {}
            None => unreachable!("Destination list creation should always succeed"),
        }

        Ok(Some(popped_value))
    }
}
