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

use chrono::Utc;
use kstd::lock_mgr::ScopeRecordLock;
use snafu::{OptionExt, ResultExt};

use crate::error::Error::*;
use crate::{
    ColumnFamilyIndex, DataType, Redis, Result,
    base_key_format::BaseKey,
    error::{KeyNotFoundSnafu, OptionNoneSnafu, RocksSnafu},
    strings_value_format::{ParsedStringsValue, StringValue},
};

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

    /// Returns the length of the string value stored at key in bytes.
    ///
    /// This command is compatible with Redis STRLEN, which returns the length
    /// of the string value stored at key. If the key does not exist or has expired,
    /// the command returns 0.
    ///
    /// # Arguments
    /// * `key` - The key to get the length of
    ///
    /// # Returns
    /// * `Ok(0)` - if the key does not exist or is expired
    /// * `Ok(length)` - the byte length of the string value (not character count for UTF-8)
    /// * `Err(RedisErr)` - if the key holds a value that is not a string (WRONGTYPE error)
    ///
    /// # Performance
    /// This operation is O(1) as it only reads metadata without accessing the full value.
    pub fn strlen(&self, key: &[u8]) -> Result<i32> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let string_key = BaseKey::new(key);
        let encode_value = db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        // If key doesn't exist, return 0
        if encode_value.is_empty() {
            return Ok(0);
        }

        // Check type first to match Redis compatibility
        // Redis returns WRONGTYPE regardless of expiration status for non-string keys
        self.check_type(encode_value.as_slice(), DataType::String)?;

        let decode_value = ParsedStringsValue::new(&encode_value[..])?;

        // Then check expiration
        if decode_value.is_stale() {
            return Ok(0);
        }

        // Return the length of the string value
        let user_value = decode_value.user_value();
        Ok(user_value.len() as i32)
    }

    /// Append a value to a key
    /// Returns the length of the string after the append operation
    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<i32> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Get lock for the key
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let string_key = BaseKey::new(key);
        let encode_value = db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        let new_value: Vec<u8>;
        let mut ctime: u64 = Utc::now().timestamp_micros() as u64;
        let mut etime: u64 = 0;

        if !encode_value.is_empty() {
            // Check if key exists and is a string type
            self.check_type(encode_value.as_slice(), DataType::String)?;

            let decode_value = ParsedStringsValue::new(&encode_value[..])?;

            // If key is stale (expired), treat as new key
            if decode_value.is_stale() {
                new_value = value.to_vec();
                // ctime and etime remain at their initialized values (current time and 0)
                // This is correct: expired keys don't preserve old metadata
            } else {
                // Append to existing value
                let user_value = decode_value.user_value();
                // Efficiently concatenate the old value and new value
                new_value = [&user_value[..], value].concat();
                ctime = decode_value.ctime();
                etime = decode_value.etime();
            }
        } else {
            // Key doesn't exist, create new
            new_value = value.to_vec();
        }

        // Check for string length overflow (Redis compatible)
        let new_len = new_value.len();
        if new_len > i32::MAX as usize {
            return Err(RedisErr {
                message: "string exceeds maximum allowed size".to_string(),
                location: Default::default(),
            });
        }

        // Set new value with metadata
        let mut string_value = StringValue::new(new_value);
        string_value.set_ctime(ctime);
        string_value.set_etime(etime);

        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, string_key.encode()?, string_value.encode());
        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(new_len as i32)
    }

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

    pub fn incr_decr(&self, key: &[u8], incr: i64) -> Result<i64> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Get lock for the key
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        // get value by key
        let string_key = BaseKey::new(key);
        let encode_value = db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        // check key type
        self.check_type(encode_value.as_slice(), DataType::String)?;

        let mut value: i64 = 0;
        let mut ctime: u64 = Utc::now().timestamp_micros() as u64;
        let mut etime: u64 = 0;

        // convert user_value to i64
        if !encode_value.is_empty() {
            let decode_value = ParsedStringsValue::new(&encode_value[..])?;
            // check ttl
            if !decode_value.is_stale() {
                let user_value = decode_value.user_value();
                value = match String::from_utf8_lossy(&user_value).to_string().parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return Err(RedisErr {
                            message: "value is not an integer or out of range".to_string(),
                            location: Default::default(),
                        });
                    }
                };
                ctime = decode_value.ctime();
                etime = decode_value.etime();
            }
        }

        // check overflow
        value = value.checked_add(incr).ok_or_else(|| RedisErr {
            message: "increment or decrement would overflow".to_string(),
            location: Default::default(),
        })?;

        // set new value
        {
            let mut string_value = StringValue::new(format!("{}", value).to_owned());
            string_value.set_ctime(ctime);
            string_value.set_etime(etime);
            let cf = self
                .get_cf_handle(ColumnFamilyIndex::MetaCF)
                .context(OptionNoneSnafu {
                    message: "cf is not initialized".to_string(),
                })?;
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(&cf, string_key.encode()?, string_value.encode());
            db.write_opt(batch, &self.write_options)
                .context(RocksSnafu)?;
        }

        Ok(value)
    }
}
