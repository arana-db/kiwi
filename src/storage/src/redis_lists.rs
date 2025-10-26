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
};

impl Redis {
    /// Insert all the specified values at the head of the list stored at key
    pub fn lpush(&self, key: &[u8], values: &[Vec<u8>]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

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
                // Create new metadata
                let meta_value = ListsMetaValue::new(0u64.to_le_bytes().to_vec());
                let encoded = meta_value.encode();
                let mut parsed = ParsedListsMetaValue::new(encoded)?;
                parsed.initial_meta_value();
                parsed
            }
        };

        let mut batch = WriteBatch::default();
        let version = parsed_meta.update_version();

        // Insert values at the head (left side)
        for (i, value) in values.iter().enumerate() {
            let new_left_index = parsed_meta.left_index() - (i as u64 + 1);
            let data_key = ListsDataKey::new(key, version, new_left_index);
            let encoded_data_key = data_key.encode()?;

            let data_value = BaseDataValue::new(value.clone());
            let encoded_data_value = data_value.encode();

            batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
        }

        // Update metadata
        parsed_meta.modify_left_index(values.len() as u64);
        parsed_meta.modify_count(values.len() as u64);

        batch.put(&meta_key, parsed_meta.value());
        db.write(batch).context(RocksSnafu)?;

        // Update statistics
        self.update_specific_key_statistics(
            DataType::List,
            &String::from_utf8_lossy(key),
            values.len() as u64,
        )?;

        Ok(parsed_meta.count() as i64)
    }

    /// Insert all the specified values at the tail of the list stored at key
    pub fn rpush(&self, key: &[u8], values: &[Vec<u8>]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }

        let (db, cfs) = get_db_and_cfs!(self, ColumnFamilyIndex::ListsDataCF);
        let lists_data_cf = &cfs[0];

        let base_meta_key = BaseMetaKey::new(key);
        let meta_key = base_meta_key.encode()?;

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
                // Create new metadata
                let meta_value = ListsMetaValue::new(0u64.to_le_bytes().to_vec());
                let encoded = meta_value.encode();
                let mut parsed = ParsedListsMetaValue::new(encoded)?;
                parsed.initial_meta_value();
                parsed
            }
        };

        let mut batch = WriteBatch::default();
        let version = parsed_meta.update_version();

        // Insert values at the tail (right side)
        for (i, value) in values.iter().enumerate() {
            let new_right_index = parsed_meta.right_index() + i as u64 + 1;
            let data_key = ListsDataKey::new(key, version, new_right_index);
            let encoded_data_key = data_key.encode()?;

            let data_value = BaseDataValue::new(value.clone());
            let encoded_data_value = data_value.encode();

            batch.put_cf(lists_data_cf, encoded_data_key, encoded_data_value);
        }

        // Update metadata
        parsed_meta.modify_right_index(values.len() as u64);
        parsed_meta.modify_count(values.len() as u64);

        batch.put(&meta_key, parsed_meta.value());
        db.write(batch).context(RocksSnafu)?;

        // Update statistics
        self.update_specific_key_statistics(
            DataType::List,
            &String::from_utf8_lossy(key),
            values.len() as u64,
        )?;

        Ok(parsed_meta.count() as i64)
    }

    /// Removes and returns the first element of the list stored at key
    pub fn lpop(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>> {
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

            if let Some(data_value) = db.get_cf(lists_data_cf, &encoded_data_key).context(RocksSnafu)? {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                result.push(parsed_data.user_value().to_vec());
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }
        }

        // Update metadata
        parsed_meta.modify_left_index(pop_count as u64);
        parsed_meta.set_count(parsed_meta.count() - pop_count as u64);

        if parsed_meta.count() == 0 {
            // Delete the key if list is empty
            batch.delete(&meta_key);
        } else {
            batch.put(&meta_key, parsed_meta.value());
        }

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

    /// Removes and returns the last element of the list stored at key
    pub fn rpop(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>> {
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

            if let Some(data_value) = db.get_cf(lists_data_cf, &encoded_data_key).context(RocksSnafu)? {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                result.push(parsed_data.user_value().to_vec());
                batch.delete_cf(lists_data_cf, encoded_data_key);
            }
        }

        // Update metadata
        parsed_meta.modify_right_index(pop_count as u64);
        parsed_meta.set_count(parsed_meta.count() - pop_count as u64);

        if parsed_meta.count() == 0 {
            // Delete the key if list is empty
            batch.delete(&meta_key);
        } else {
            batch.put(&meta_key, parsed_meta.value());
        }

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

        match db.get_cf(lists_data_cf, &encoded_data_key).context(RocksSnafu)? {
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

            if let Some(data_value) = db.get_cf(lists_data_cf, &encoded_data_key).context(RocksSnafu)? {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                result.push(parsed_data.user_value().to_vec());
            }
        }

        Ok(result)
    }

    /// Sets the list element at index to value
    pub fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> Result<()> {
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
                .fail()
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
            parsed_meta.set_left_index(parsed_meta.left_index() + real_start as u64);
            parsed_meta.set_right_index(parsed_meta.left_index() + new_count);

            batch.put(&meta_key, parsed_meta.value());
        }

        db.write(batch).context(RocksSnafu)?;

        Ok(())
    }

    /// Removes the first count occurrences of elements equal to value from the list
    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> Result<i64> {
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
        let mut indices_to_remove = Vec::new();

        // Determine search direction and limit
        let (start_idx, end_idx, step): (i64, i64, i64) = if count > 0 {
            // Remove from head to tail
            (0, parsed_meta.count() as i64, 1)
        } else if count < 0 {
            // Remove from tail to head
            (parsed_meta.count() as i64 - 1, -1, -1)
        } else {
            // Remove all occurrences
            (0, parsed_meta.count() as i64, 1)
        };

        let max_removals = if count == 0 { i64::MAX } else { count.abs() };

        // Find elements to remove
        let mut current_idx = start_idx;
        while (step > 0 && current_idx < end_idx) || (step < 0 && current_idx > end_idx) {
            if removed_count >= max_removals {
                break;
            }

            let storage_index = parsed_meta.left_index() + current_idx as u64 + 1;
            let data_key = ListsDataKey::new(key, parsed_meta.version(), storage_index);
            let encoded_data_key = data_key.encode()?;

            if let Some(data_value) = db.get_cf(lists_data_cf, &encoded_data_key).context(RocksSnafu)? {
                let parsed_data = ParsedBaseDataValue::new(BytesMut::from(data_value.as_slice()))?;
                if parsed_data.user_value() == value {
                    indices_to_remove.push(current_idx);
                    batch.delete_cf(lists_data_cf, encoded_data_key);
                    removed_count += 1;
                }
            }

            current_idx += step;
        }

        if removed_count > 0 {
            // Update count
            parsed_meta.set_count(parsed_meta.count() - removed_count as u64);

            if parsed_meta.count() == 0 {
                batch.delete(&meta_key);
            } else {
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
}
