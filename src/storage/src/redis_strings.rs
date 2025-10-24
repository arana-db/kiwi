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

//! Redis strings operations implementation
//! This module provides string operations for Redis storage

// use std::time::{SystemTime, UNIX_EPOCH};
// use rocksdb::{WriteBatch, WriteOptions, ReadOptions};

// use crate::{Result, StorageError};
// use crate::base_data_value_format::{DataType, InternalValue, ParsedInternalValue};

// use crate::types::KeyValue;

use kstd::lock_mgr::ScopeRecordLock;
use snafu::{OptionExt, ResultExt};

use crate::{
    ColumnFamilyIndex, Redis, Result,
    base_key_format::BaseKey,
    error::{Error, InvalidArgumentSnafu, KeyNotFoundSnafu, OptionNoneSnafu, RocksSnafu},
    strings_value_format::{ParsedStringsValue, StringValue},
};

pub enum BitOpType {
    And,
    Or,
    Xor,
    Not,
}

impl Redis {
    // /// Append a value to the string stored at key
    // pub fn append(&self, key: &[u8], value: &[u8], ret: &mut i32) -> Result<()> {
    //     let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

    //     let mut string_value = String::new();
    //     let mut version = 0;
    //     let mut timestamp = 0;

    //     // Try to get the existing value
    //     let read_options = ReadOptions::default();
    //     match db.get_opt(key, &read_options)? {
    //         Some(existing) => {
    //             // Parse the existing value
    //             let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
    //             if parsed_value.data_type() != DataType::String {
    //                 return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
    //             }

    //             // Check if expired
    //             let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    //             if parsed_value.is_expired(now) {
    //                 // Create a new value
    //                 string_value = String::from_utf8_lossy(value).to_string();
    //                 version = 0;
    //             } else {
    //                 // Append to existing value
    //                 string_value = format!("{}{}",
    //                     String::from_utf8_lossy(parsed_value.user_value()),
    //                     String::from_utf8_lossy(value));
    //                 version = parsed_value.version();
    //                 timestamp = parsed_value.etime();
    //             }
    //         },
    //         None => {
    //             // Create a new value
    //             string_value = String::from_utf8_lossy(value).to_string();
    //         }
    //     }

    //     // Create the new value
    //     let mut internal_value = InternalValue::new(DataType::String, string_value.as_bytes());
    //     internal_value.set_version(version);
    //     if timestamp > 0 {
    //         internal_value.set_etime(timestamp);
    //     }

    //     // Write to DB
    //     let encoded_value = internal_value.encode();
    //     db.put_opt(key, &encoded_value, &self.default_write_options)?;

    //     // Set the return value to the new string length
    //     *ret = string_value.len() as i32;

    //     // Update statistics
    //     self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;

    //     Ok(())
    // }

    // Get the value of a key
    pub fn get(&self, key: &[u8]) -> Result<String> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let string_key = BaseKey::new(key);

        match db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(val) => {
                let string_value = ParsedStringsValue::new(&val[..])?;
                let user_value = string_value.user_value();
                Ok(String::from_utf8_lossy(&user_value).to_string())
            }
            None => KeyNotFoundSnafu {
                key: String::from_utf8_lossy(key).to_string(),
            }
            .fail(),
        }
    }

    // /// Get the value and TTL of a key
    // pub fn get_with_ttl(&self, key: &[u8], value: &mut String, ttl: &mut i64) -> Result<()> {
    //     let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

    //     // Try to get the value
    //     let read_options = ReadOptions::default();
    //     match db.get_opt(key, &read_options)? {
    //         Some(existing) => {
    //             // Parse the existing value
    //             let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
    //             if parsed_value.data_type() != DataType::String {
    //                 return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
    //             }

    //             // Check if expired
    //             let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    //             if parsed_value.is_expired(now) {
    //                 return Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()));
    //             }

    //             // Calculate TTL
    //             let etime = parsed_value.etime();
    //             if etime > 0 {
    //                 *ttl = etime as i64 - now as i64;
    //                 if *ttl < 0 {
    //                     *ttl = 0;
    //                 }
    //             } else {
    //                 *ttl = -1; // No expiration
    //             }

    //             // Return the value
    //             *value = String::from_utf8_lossy(parsed_value.user_value()).to_string();
    //             Ok(())
    //         },
    //         None => {
    //             Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()))
    //         }
    //     }
    // }

    /// Set key to hold the string value
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let string_key = BaseKey::new(key);
        let string_value = StringValue::new(value.to_owned());

        // Get lock for the key
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, string_key.encode()?, string_value.encode());

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(())
    }

    // /// Set key to hold string value and expiration time
    // pub fn setex(&self, key: &[u8], value: &[u8], ttl: i64) -> Result<()> {
    //     let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

    //     if ttl <= 0 {
    //         return Err(StorageError::InvalidFormat("Invalid expire time".to_string()));
    //     }

    //     // Create the new value with expiration
    //     let mut internal_value = InternalValue::new(DataType::String, value);
    //     internal_value.set_relative_timestamp(ttl as u64)?;
    //     let encoded_value = internal_value.encode();

    //     // Write to DB
    //     db.put_opt(key, &encoded_value, &self.default_write_options)?;

    //     // Update statistics
    //     self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;

    //     Ok(())
    // }

    // /// Set key to hold string value if key does not exist
    // pub fn setnx(&self, key: &[u8], value: &[u8], ret: &mut i32, ttl: i64) -> Result<()> {
    //     let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

    //     // Check if key exists
    //     let read_options = ReadOptions::default();
    //     match db.get_opt(key, &read_options)? {
    //         Some(existing) => {
    //             // Parse the existing value
    //             let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());

    //             // Check if expired
    //             let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    //             if !parsed_value.is_expired(now) {
    //                 *ret = 0; // Key exists, not set
    //                 return Ok(());
    //             }
    //         },
    //         None => {}
    //     }

    //     // Create the new value
    //     let mut internal_value = InternalValue::new(DataType::String, value);
    //     if ttl > 0 {
    //         internal_value.set_relative_timestamp(ttl as u64)?;
    //     }
    //     let encoded_value = internal_value.encode();

    //     // Write to DB
    //     db.put_opt(key, &encoded_value, &self.default_write_options)?;

    //     // Update statistics
    //     self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;

    //     *ret = 1; // Key set
    //     Ok(())
    // }

    // /// Set the string value and return the old value
    // pub fn getset(&self, key: &[u8], value: &[u8], old_value: &mut String) -> Result<()> {
    //     let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

    //     // Try to get the existing value
    //     let read_options = ReadOptions::default();
    //     match db.get_opt(key, &read_options)? {
    //         Some(existing) => {
    //             // Parse the existing value
    //             let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
    //             if parsed_value.data_type() != DataType::String {
    //                 return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
    //             }

    //             // Check if expired
    //             let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    //             if !parsed_value.is_expired(now) {
    //                 // Return the old value
    //                 *old_value = String::from_utf8_lossy(parsed_value.user_value()).to_string();
    //             }
    //         },
    //         None => {}
    //     }

    //     // Create the new value
    //     let internal_value = InternalValue::new(DataType::String, value);
    //     let encoded_value = internal_value.encode();

    //     // Write to DB
    //     db.put_opt(key, &encoded_value, &self.default_write_options)?;

    //     // Update statistics
    //     self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;

    //     Ok(())
    // }

    // /// Set multiple keys to multiple values
    // pub fn mset(&self, kvs: &[KeyValue]) -> Result<()> {
    //     let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

    //     let mut batch = WriteBatch::default();

    //     for kv in kvs {
    //         // Create the new value
    //         let internal_value = InternalValue::new(DataType::String, &kv.value);
    //         let encoded_value = internal_value.encode();

    //         // Add to batch
    //         batch.put(&kv.key, &encoded_value);

    //         // Update statistics
    //         self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(&kv.key).to_string(), 1)?;
    //     }

    //     // Write batch to DB
    //     db.write_opt(batch, &self.default_write_options)?;

    //     Ok(())
    // }

    // /// Set multiple keys to multiple values, only if none of the keys exist
    // pub fn msetnx(&self, kvs: &[KeyValue], ret: &mut i32) -> Result<()> {
    //     let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

    //     // Check if any key exists
    //     let read_options = ReadOptions::default();
    //     let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    //     for kv in kvs {
    //         match db.get_opt(&kv.key, &read_options)? {
    //             Some(existing) => {
    //                 // Parse the existing value
    //                 let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());

    //                 // Check if expired
    //                 if !parsed_value.is_expired(now) {
    //                     *ret = 0; // At least one key exists, not set
    //                     return Ok(());
    //                 }
    //             },
    //             None => {}
    //         }
    //     }

    //     // All keys don't exist, set them
    //     let mut batch = WriteBatch::default();

    //     for kv in kvs {
    //         // Create the new value
    //         let internal_value = InternalValue::new(DataType::String, &kv.value);
    //         let encoded_value = internal_value.encode();

    //         // Add to batch
    //         batch.put(&kv.key, &encoded_value);

    //         // Update statistics
    //         self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(&kv.key).to_string(), 1)?;
    //     }

    //     // Write batch to DB
    //     db.write_opt(batch, &self.default_write_options)?;

    //     *ret = 1; // All keys set
    //     Ok(())
    // }

    // /// Get the length of the string value stored at key
    // pub fn strlen(&self, key: &[u8], len: &mut i32) -> Result<()> {
    //     let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

    //     // Try to get the value
    //     let read_options = ReadOptions::default();
    //     match db.get_opt(key, &read_options)? {
    //         Some(existing) => {
    //             // Parse the existing value
    //             let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
    //             if parsed_value.data_type() != DataType::String {
    //                 return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
    //             }

    //             // Check if expired
    //             let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    //             if parsed_value.is_expired(now) {
    //                 *len = 0;
    //                 return Ok(());
    //             }

    //             // Return the length
    //             *len = parsed_value.user_value().len() as i32;
    //             Ok(())
    //         },
    //         None => {
    //             *len = 0;
    //             Ok(())
    //         }
    //     }
    // }
    pub fn setbit(&self, key: &[u8], offset: i64, on: i32) -> Result<i32> {
        // Validate offset
        if offset < 0 || offset >= (1_i64 << 32) {
            InvalidArgumentSnafu {
                message: "offset must be 0 <= offset < 2^32".to_string(),
            }
            .fail()?;
        }

        // Validate value
        if on != 0 && on != 1 {
            return Err(Error::InvalidArgument {
                message: "value must be 0 or 1".to_string(),
                location: snafu::location!(),
            });
        }

        let base_key = BaseKey::new(key);
        let l = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &l);
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        // try to get existing value
        let mut data_value = Vec::new();
        let mut timestamp = 0u64;
        match db
            .get_opt(&base_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(val) => {
                let parsed_value = ParsedStringsValue::new(&val[..])?;
                if parsed_value.is_stale() {
                    // Treat as non-existent. Do not preserve TTL.
                    data_value.clear();
                    timestamp = 0;
                } else {
                    data_value = parsed_value.user_value().to_vec();
                    timestamp = parsed_value.etime();
                }
            }
            None => {
                // Key doesn't exist, data_value remains empty
            }
        }
        // calculate byte and bit position
        let byte = (offset >> 3) as usize;
        // MSB-first: bit 0 is the most significant bit
        let bit = (7 - (offset & 0x7)) as usize;
        let value_length = data_value.len();
        let mut byte_val: u8;
        let ret: i32;

        // get current bit value
        if byte + 1 > value_length {
            ret = 0;
            byte_val = 0;
        } else {
            ret = ((data_value[byte] & (1 << bit)) >> bit) as i32;
            byte_val = data_value[byte];
        }

        // set or clear the bit
        byte_val = byte_val & (!(1 << bit));
        byte_val = byte_val | ((on as u8 & 0x1) << bit);

        // update or extend the data_value
        if byte < value_length {
            data_value[byte] = byte_val;
        } else {
            // extend with zeros and write the target byte
            data_value.resize(byte + 1, 0);
            data_value[byte] = byte_val;
        }

        // create new base value with updated data
        let mut base_value = StringValue::new(data_value);
        base_value.set_etime(timestamp);

        // write to db
        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;

        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, base_key.encode()?, base_value.encode());

        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(ret)
    }
    pub fn getbit(&self, key: &[u8], offset: i64) -> Result<i32> {
        if offset < 0 {
            return Err(Error::InvalidArgument {
                message: "offset < 0".to_string(),
                location: snafu::location!(),
            });
        }
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let base_key = BaseKey::new(key);
        match db
            .get_opt(&base_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(val) => {
                let parsed_value =
                    ParsedStringsValue::new(&val[..]).map_err(|e| Error::InvalidArgument {
                        message: e.to_string(),
                        location: snafu::location!(),
                    })?;

                // Check if value is stale (expired)
                if parsed_value.is_stale() {
                    return Ok(0);
                }

                let data_value = parsed_value.user_value();

                // Calculate byte and bit position (MSB-first)
                let byte = (offset >> 3) as usize;
                let bit = (7 - (offset & 0x7)) as usize;

                // Check if offset is within bounds
                if byte >= data_value.len() {
                    Ok(0)
                } else {
                    Ok(((data_value[byte] & (1 << bit)) >> bit) as i32)
                }
            }
            None => Ok(0),
        }
    }

    pub fn bitcount(
        &self,
        key: &[u8],
        start_offset: Option<i64>,
        end_offset: Option<i64>,
    ) -> Result<i32> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let base_key = BaseKey::new(key);

        match db
            .get_opt(&base_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(val) => {
                let parsed_value =
                    ParsedStringsValue::new(&val[..]).map_err(|e| Error::InvalidArgument {
                        message: e.to_string(),
                        location: snafu::location!(),
                    })?;

                // Expired => treat as empty
                if parsed_value.is_stale() {
                    return Ok(0);
                }

                let data_value = parsed_value.user_value();
                let value_length = data_value.len() as i64;

                // calculate start and end byte positions
                let (start_byte, end_byte) =
                    if let (Some(mut start), Some(mut end)) = (start_offset, end_offset) {
                        // handle negative offsets
                        if start < 0 {
                            start = start + value_length;
                        }
                        if end < 0 {
                            end = end + value_length;
                        }

                        // clamp to valid range
                        if start < 0 {
                            start = 0;
                        }
                        if end < 0 {
                            end = 0;
                        }

                        if end >= value_length {
                            end = value_length - 1;
                        }
                        // if start > end return 0
                        if start > end {
                            return Ok(0);
                        }
                        (start as usize, (end + 1) as usize)
                    } else {
                        // no range specified, count entire value
                        (0, value_length.max(0) as usize)
                    };

                // get the slice and count bits
                let slice = &data_value[start_byte..end_byte];
                Ok(Self::get_bit_count(slice, slice.len() as i64))
            }
            None => Ok(0),
        }
    }

    pub fn get_bit_count(value: &[u8], bytes: i64) -> i32 {
        static BITS_IN_BYTE: [u8; 256] = [
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3,
            4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4,
            4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4,
            5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5,
            4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2,
            3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5,
            5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
            5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6,
            4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
        ];

        let mut bit_num = 0i32;
        let byte_count = bytes.min(value.len() as i64) as usize;

        for i in 0..byte_count {
            bit_num += BITS_IN_BYTE[value[i] as usize] as i32;
        }
        bit_num
    }
    pub fn bitop(&self, op: BitOpType, dest_key: &[u8], src_keys: &[&[u8]]) -> Result<i64> {
        // Validate arguments
        if matches!(op, BitOpType::Not) && src_keys.len() != 1 {
            return Err(Error::InvalidArgument {
                message: "BITOP NOT requires exactly one source key".to_string(),
                location: snafu::location!(),
            });
        }
        if src_keys.is_empty() {
            return Err(Error::InvalidArgument {
                message: "BITOP requires at least one source key".to_string(),
                location: snafu::location!(),
            });
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Collect source values
        let mut max_len = 0usize;
        let mut src_values = Vec::with_capacity(src_keys.len());

        for &src_key in src_keys {
            let string_key = BaseKey::new(src_key);

            match db
                .get_opt(&string_key.encode()?, &self.read_options)
                .context(RocksSnafu)?
            {
                Some(val) => {
                    let parsed = ParsedStringsValue::new(&val[..])?;

                    // Check if value is stale (expired)
                    if parsed.is_stale() {
                        src_values.push(Vec::new());
                    } else {
                        let user_value = parsed.user_value().to_vec();
                        max_len = max_len.max(user_value.len());
                        src_values.push(user_value);
                    }
                }
                None => {
                    src_values.push(Vec::new());
                }
            }
        }

        // Perform bitwise operation
        let dest_value = self.bitop_operate(op, &src_values, max_len);
        let result_len = dest_value.len() as i64;

        // Store result
        let dest_string_key = BaseKey::new(dest_key);
        let dest_string_value = StringValue::new(dest_value);

        let key_str = String::from_utf8_lossy(dest_key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;

        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, dest_string_key.encode()?, dest_string_value.encode());

        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(result_len)
    }

    fn bitop_operate(&self, op: BitOpType, src_values: &[Vec<u8>], max_len: usize) -> Vec<u8> {
        let mut result = vec![0u8; max_len];

        match op {
            BitOpType::And => {
                // Initialize with all 1s
                result.fill(0xFF);
                for src in src_values {
                    for (i, &byte) in src.iter().enumerate() {
                        result[i] &= byte;
                    }
                    // AND with 0 for remaining bytes
                    for i in src.len()..max_len {
                        result[i] = 0;
                    }
                }
            }
            BitOpType::Or => {
                for src in src_values {
                    for (i, &byte) in src.iter().enumerate() {
                        result[i] |= byte;
                    }
                }
            }
            BitOpType::Xor => {
                for src in src_values {
                    for (i, &byte) in src.iter().enumerate() {
                        result[i] ^= byte;
                    }
                }
            }
            BitOpType::Not => {
                // NOT operation on single source
                if let Some(src) = src_values.first() {
                    for (i, &byte) in src.iter().enumerate() {
                        result[i] = !byte;
                    }
                    for i in src.len()..max_len {
                        result[i] = 0xFF;
                    }
                }
            }
        }

        result
    }

    /// Find the first occurrence of a specified bit value in a byte array  
    /// Returns the bit position (in bits), or 8 * bytes if not found  
    /// MSB-first to match Redis (bit 0 = MSB of first byte)
    fn get_bit_pos(data: &[u8], bit: i32) -> i64 {
        let bytes = data.len();

        if bit == 1 {
            for (byte_idx, &byte) in data.iter().enumerate() {
                if byte != 0 {
                    let lz = (byte as u8).leading_zeros() as usize;
                    return (byte_idx * 8 + lz) as i64;
                }
            }
        } else {
            for (byte_idx, &byte) in data.iter().enumerate() {
                if byte != 0xFF {
                    let lz = ((!byte) as u8).leading_zeros() as usize;
                    return (byte_idx * 8 + lz) as i64;
                }
            }
        }

        // Not found, return total number of bits
        (bytes * 8) as i64
    }

    /// BITPOS key bit - Search for bit in the entire string  
    pub fn bitpos(&self, key: &[u8], bit: i32) -> Result<i64> {
        // Validate parameters
        if bit != 0 && bit != 1 {
            return Err(Error::InvalidArgument {
                message: "bit must be 0 or 1".to_string(),
                location: snafu::location!(),
            });
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let string_key = BaseKey::new(key);

        match db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(val) => {
                let parsed = ParsedStringsValue::new(&val[..])?;

                // Check if expired
                if parsed.is_stale() {
                    return Ok(if bit == 1 { -1 } else { 0 });
                }

                // Get user value
                let user_value = parsed.user_value();
                let value_length = user_value.len() as i64;

                if value_length == 0 {
                    return Ok(if bit == 1 { -1 } else { 0 });
                }

                // Find bit position
                let start_offset = 0i64;
                let end_offset = value_length - 1;
                let bytes = (end_offset - start_offset + 1) as usize;

                let pos = Self::get_bit_pos(&user_value[start_offset as usize..], bit);

                if pos == (bytes * 8) as i64 {
                    // Not found
                    return Ok(if bit == 1 {
                        -1
                    } else {
                        (value_length * 8) as i64
                    });
                }

                // Adjust position (add start offset)
                if pos != -1 {
                    Ok(pos + 8 * start_offset)
                } else {
                    Ok(-1)
                }
            }
            None => {
                // Key does not exist
                Ok(if bit == 1 { -1 } else { 0 })
            }
        }
    }

    /// BITPOS key bit start - Search starting from specified byte offset  
    pub fn bitpos_with_start(&self, key: &[u8], bit: i32, start_offset: i64) -> Result<i64> {
        // Validate parameters
        if bit != 0 && bit != 1 {
            return Err(Error::InvalidArgument {
                message: "bit must be 0 or 1".to_string(),
                location: snafu::location!(),
            });
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let string_key = BaseKey::new(key);

        match db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(val) => {
                let parsed = ParsedStringsValue::new(&val[..])?;

                // Check if expired
                if parsed.is_stale() {
                    return Ok(if bit == 1 { -1 } else { 0 });
                }

                // Get user value
                let user_value = parsed.user_value();
                let value_length = user_value.len() as i64;

                if value_length == 0 {
                    return Ok(if bit == 1 { -1 } else { 0 });
                }

                // Handle negative offset
                let mut start = start_offset;
                if start < 0 {
                    start = start + value_length;
                }
                if start < 0 {
                    start = 0;
                }

                let end_offset = value_length - 1;

                // Check boundary conditions
                if start > end_offset || start > value_length - 1 {
                    return Ok(if bit == 1 { -1 } else { value_length * 8 });
                }

                // Find bit position
                let bytes = (end_offset - start + 1) as usize;
                let pos = Self::get_bit_pos(&user_value[start as usize..], bit);

                if pos == (bytes * 8) as i64 {
                    return Ok(if bit == 1 { -1 } else { value_length * 8 });
                }
                // Adjust position (add start offset)
                if pos != -1 {
                    Ok(pos + 8 * start)
                } else {
                    Ok(-1)
                }
            }
            None => {
                // Key does not exist
                Ok(if bit == 1 { -1 } else { 0 })
            }
        }
    }

    /// BITPOS key bit start end - Search within specified byte range  
    pub fn bitpos_with_range(
        &self,
        key: &[u8],
        bit: i32,
        start_offset: i64,
        end_offset: i64,
    ) -> Result<i64> {
        // Validate parameters
        if bit != 0 && bit != 1 {
            return Err(Error::InvalidArgument {
                message: "bit must be 0 or 1".to_string(),
                location: snafu::location!(),
            });
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let string_key = BaseKey::new(key);

        match db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
        {
            Some(val) => {
                let parsed = ParsedStringsValue::new(&val[..])?;

                // Check if expired
                if parsed.is_stale() {
                    return Ok(if bit == 1 { -1 } else { 0 });
                }

                // Get user value
                let user_value = parsed.user_value();
                let value_length = user_value.len() as i64;

                if value_length == 0 {
                    return Ok(if bit == 1 { -1 } else { 0 });
                }

                // Handle negative offsets
                let mut start = start_offset;
                if start < 0 {
                    start = start + value_length;
                }
                if start < 0 {
                    start = 0;
                }

                let mut end = end_offset;
                if end < 0 {
                    end = end + value_length;
                }
                if end > value_length - 1 {
                    end = value_length - 1;
                }
                if end < 0 {
                    end = 0;
                }

                // Check boundary conditions
                if start > end || start > value_length - 1 {
                    return Ok(-1);
                }

                // Find bit position
                let bytes = (end - start + 1) as usize;
                let search_slice = &user_value[start as usize..=end as usize];
                let pos = Self::get_bit_pos(search_slice, bit);

                // If searching for 1 and not found, also return -1
                if pos == (bytes * 8) as i64 {
                    return Ok(-1);
                }

                // Adjust position (add start offset)
                if pos != -1 {
                    Ok(pos + 8 * start)
                } else {
                    Ok(-1)
                }
            }
            None => {
                // Key does not exist
                Ok(if bit == 1 { -1 } else { 0 })
            }
        }
    }
}
