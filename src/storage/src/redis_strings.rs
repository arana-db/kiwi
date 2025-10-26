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

use chrono::Utc;
use kstd::lock_mgr::ScopeRecordLock;
use snafu::{OptionExt, ResultExt};

use crate::error::Error::*;
use crate::{
    ColumnFamilyIndex, DataType, Redis, Result,
    base_key_format::BaseKey,
    error::{InvalidFormatSnafu, KeyNotFoundSnafu, OptionNoneSnafu, RocksSnafu},
    strings_value_format::{ParsedStringsValue, StringValue},
};

impl Redis {
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

        let decode_value = ParsedStringsValue::new(&encode_value[..])?;

        // Check if key is expired first - expired keys are treated as non-existent
        if decode_value.is_stale() {
            return Ok(0);
        }

        // Now check type on live key to match Redis compatibility
        // Redis returns WRONGTYPE only for live non-string keys
        self.check_type(encode_value.as_slice(), DataType::String)?;

        // Return the length of the string value
        let user_value = decode_value.user_value();
        Ok(user_value.len() as i32)
    }

    /// Returns a substring of the string value stored at key.
    ///
    /// This command is compatible with Redis GETRANGE. The substring is determined
    /// by the start and end offsets (both inclusive). Negative offsets can be used
    /// to provide an offset starting from the end of the string.
    ///
    /// # Arguments
    /// * `key` - The key to get the substring from
    /// * `start` - The starting offset (inclusive, can be negative)
    /// * `end` - The ending offset (inclusive, can be negative)
    ///
    /// # Returns
    /// * `Ok(String)` - the substring, or empty string if key doesn't exist or is expired
    /// * `Err(RedisErr)` - if the key holds a value that is not a string (WRONGTYPE error)
    ///
    /// # Examples
    /// - GETRANGE key 0 3 returns first 4 characters
    /// - GETRANGE key -3 -1 returns last 3 characters
    /// - GETRANGE key 0 -1 returns the entire string
    /// - GETRANGE key 10 5 returns empty string (start > end after normalization)
    ///
    /// # Performance
    /// This operation is O(N) where N is the length of the returned substring.
    pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Result<Vec<u8>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let string_key = BaseKey::new(key);
        let encode_value = db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        // If key doesn't exist, return empty string
        if encode_value.is_empty() {
            return Ok(Vec::new());
        }

        let decode_value = ParsedStringsValue::new(&encode_value[..])?;

        // Check if key is expired first - expired keys are treated as non-existent
        if decode_value.is_stale() {
            return Ok(Vec::new());
        }

        // Now check type on live key to match Redis compatibility
        // Redis returns WRONGTYPE only for live non-string keys
        self.check_type(encode_value.as_slice(), DataType::String)?;

        let user_value = decode_value.user_value();
        let len = user_value.len() as i64;

        // Handle empty string
        if len == 0 {
            return Ok(Vec::new());
        }

        // Normalize negative indices
        let start_idx = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        };

        let end_idx = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        // If start > end after normalization, return empty string
        if start_idx > end_idx {
            return Ok(Vec::new());
        }

        // Extract substring (end is inclusive, so +1)
        let start_pos = start_idx as usize;
        let end_pos = (end_idx + 1) as usize;
        let substring = user_value[start_pos..end_pos].to_vec();

        Ok(substring)
    }

    /// Overwrites part of a string at key starting at the specified offset.
    ///
    /// This command is compatible with Redis SETRANGE. If the offset is larger than
    /// the current string length, the string is padded with zero-bytes (\x00) to make
    /// the offset fit. If the key doesn't exist, it's created as an empty string before
    /// performing the operation.
    ///
    /// # Arguments
    /// * `key` - The key to modify
    /// * `offset` - The starting position to overwrite (must be >= 0)
    /// * `value` - The value to write at the offset
    ///
    /// # Returns
    /// * `Ok(length)` - the length of the string after modification
    /// * `Err(RedisErr)` - if the key holds a value that is not a string (WRONGTYPE error)
    /// * `Err(RedisErr)` - if offset is negative or out of range
    /// * `Err(RedisErr)` - if the resulting string would exceed maximum size
    ///
    /// # Examples
    /// - SETRANGE key 6 "Redis" on "Hello World" results in "Hello Redis"
    /// - SETRANGE key 6 "Redis" on "Hello" results in "Hello\x00Redis"
    /// - SETRANGE nonexistent 5 "Redis" creates "\x00\x00\x00\x00\x00Redis"
    ///
    /// # Time Complexity
    /// O(1) for small strings when offset is within current length,
    /// O(M) where M is the length of the value argument for other cases.
    pub fn setrange(&self, key: &[u8], offset: i64, value: &[u8]) -> Result<i32> {
        // Validate offset early to avoid unnecessary database operations
        if offset < 0 {
            return Err(RedisErr {
                message: "ERR offset is out of range".to_string(),
                location: Default::default(),
            });
        }

        // Check for offset upper bound to prevent potential overflow
        if offset > i32::MAX as i64 {
            return Err(RedisErr {
                message: "ERR offset is out of range".to_string(),
                location: Default::default(),
            });
        }

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

        let mut existing_value = Vec::new();
        let mut ctime: u64 = Utc::now().timestamp_micros() as u64;
        let mut etime: u64 = 0;

        if !encode_value.is_empty() {
            let decode_value = ParsedStringsValue::new(&encode_value[..])?;

            // Check if key is expired first - expired keys are treated as non-existent
            if !decode_value.is_stale() {
                // Now check type on live key to match Redis compatibility
                // Redis returns WRONGTYPE only for live non-string keys
                self.check_type(encode_value.as_slice(), DataType::String)?;

                existing_value = decode_value.user_value().to_vec();
                ctime = decode_value.ctime();
                etime = decode_value.etime();
            }
            // If expired, treat as empty string (existing_value remains empty)
        }

        // Early return optimization: if value is empty and offset is within bounds
        if value.is_empty() {
            let current_len = existing_value.len() as i32;
            // If offset is within current string, no modification needed
            if offset <= current_len as i64 {
                return Ok(current_len);
            }
            // If offset is beyond current string, we need to pad
        }

        let offset_usize = offset as usize;
        let current_len = existing_value.len();

        // Calculate required length
        let required_len = offset_usize + value.len();

        // Check for string length overflow
        if required_len > i32::MAX as usize {
            return Err(RedisErr {
                message: "ERR string exceeds maximum allowed size".to_string(),
                location: Default::default(),
            });
        }

        let mut new_value = existing_value;

        // If offset is beyond current length, pad with zero bytes
        if offset_usize > current_len {
            new_value.resize(offset_usize, 0);
        }

        // If new value extends beyond current length, extend the vector
        if required_len > new_value.len() {
            new_value.resize(required_len, 0);
        }

        // Overwrite the range with the new value
        new_value[offset_usize..offset_usize + value.len()].copy_from_slice(value);

        let new_len = new_value.len();

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
            let decode_value = ParsedStringsValue::new(&encode_value[..])?;

            // Check if key is expired first - expired keys are treated as non-existent
            if decode_value.is_stale() {
                new_value = value.to_vec();
                // ctime and etime remain at their initialized values (current time and 0)
                // This is correct: expired keys don't preserve old metadata
            } else {
                // Now check type on live key to match Redis compatibility
                // Redis returns WRONGTYPE only for live non-string keys
                self.check_type(encode_value.as_slice(), DataType::String)?;

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

                // Check if key is expired
                if string_value.is_stale() {
                    return KeyNotFoundSnafu {
                        key: String::from_utf8_lossy(key).to_string(),
                    }
                    .fail();
                }

                let user_value = string_value.user_value();
                Ok(String::from_utf8_lossy(&user_value).to_string())
            }
            None => KeyNotFoundSnafu {
                key: String::from_utf8_lossy(key).to_string(),
            }
            .fail(),
        }
    }

    /// Get the value of a key as bytes, preserving binary data
    pub fn get_binary(&self, key: &[u8]) -> Result<Vec<u8>> {
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

                // Check if key is expired
                if string_value.is_stale() {
                    return KeyNotFoundSnafu {
                        key: String::from_utf8_lossy(key).to_string(),
                    }
                    .fail();
                }

                let user_value = string_value.user_value();
                Ok(user_value.to_vec())
            }
            None => KeyNotFoundSnafu {
                key: String::from_utf8_lossy(key).to_string(),
            }
            .fail(),
        }
    }

    /// MGET key [key ...]
    ///
    /// Returns the values of all specified keys. For every key that does not hold
    /// a string value or does not exist, the special value nil is returned.
    /// Because a non-existing keys are treated as empty strings, running MGET
    /// against a non-existing key will return nil.
    ///
    /// # Time Complexity
    /// O(N) where N is the number of keys to retrieve
    ///
    /// # Returns
    /// Array reply: list of values at the specified keys
    ///
    /// # Behavior
    /// - Non-existing keys return nil
    /// - Expired keys return nil
    /// - Keys with wrong data type return nil (no error)
    /// - Empty key list returns empty array
    ///
    /// # Examples
    /// ```text
    /// MGET key1 key2 key3  // Returns array of values or nil for each key
    /// MGET nonexistent     // Returns [nil]
    /// ```
    pub fn mget(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<String>>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let mut results = Vec::with_capacity(keys.len());

        // Note: RocksDB multi_get would be more efficient for batch reads,
        // but it's not currently exposed through the Engine trait.
        // Future optimization: add multi_get to Engine trait.
        for key in keys {
            let string_key = BaseKey::new(key);

            match db
                .get_opt(&string_key.encode()?, &self.read_options)
                .context(RocksSnafu)?
            {
                Some(val) => {
                    // Check type - if not string type, return None (like Redis does)
                    if self.check_type(val.as_slice(), DataType::String).is_err() {
                        results.push(None);
                        continue;
                    }

                    let string_value = ParsedStringsValue::new(&val[..])?;

                    // Check if key is expired
                    if string_value.is_stale() {
                        results.push(None);
                    } else {
                        let user_value = string_value.user_value();
                        results.push(Some(String::from_utf8_lossy(&user_value).to_string()));
                    }
                }
                None => {
                    results.push(None);
                }
            }
        }

        Ok(results)
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

    /// Set key to hold string value with TTL in seconds.
    ///
    /// This command is compatible with Redis SETEX, which sets a key to hold a string value
    /// and sets an expiration time in seconds. This is an atomic operation.
    ///
    /// # Arguments
    /// * `key` - The key to set
    /// * `seconds` - TTL in seconds (must be positive)
    /// * `value` - The value to set
    ///
    /// # Returns
    /// * `Ok(())` - if the operation succeeded
    /// * `Err(RedisErr)` - if TTL is invalid (not positive)
    ///
    /// # Examples
    /// - SETEX mykey 10 "Hello" - Sets mykey to "Hello" with 10 seconds expiration
    ///
    /// # Performance
    /// This operation is O(1) as it only performs a single database write.
    pub fn setex(&self, key: &[u8], seconds: i64, value: &[u8]) -> Result<()> {
        // Validate TTL - must be positive
        if seconds <= 0 {
            return Err(RedisErr {
                message: "ERR invalid expire time in setex".to_string(),
                location: Default::default(),
            });
        }

        let string_key = BaseKey::new(key);
        let mut string_value = StringValue::new(value.to_owned());

        // Set TTL in microseconds (seconds * 1_000_000)
        let ttl_micros = (seconds as u64)
            .checked_mul(1_000_000)
            .context(InvalidFormatSnafu {
                message: "TTL overflow when converting to microseconds".to_string(),
            })?;
        string_value.set_relative_etime(ttl_micros)?;

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

    /// Set key to hold string value with TTL in milliseconds.
    ///
    /// This command is compatible with Redis PSETEX, which sets a key to hold a string value
    /// and sets an expiration time in milliseconds. This is an atomic operation.
    /// It is exactly equivalent to executing the following commands:
    /// SET key value
    /// PEXPIRE key milliseconds
    ///
    /// # Arguments
    /// * `key` - The key to set
    /// * `milliseconds` - TTL in milliseconds (must be positive)
    /// * `value` - The value to set
    ///
    /// # Returns
    /// * `Ok(())` - if the operation succeeded
    /// * `Err(RedisErr)` - if TTL is invalid (not positive or causes overflow)
    ///
    /// # Examples
    /// - PSETEX mykey 10000 "Hello" - Sets mykey to "Hello" with 10000 milliseconds (10 seconds) expiration
    ///
    /// # Time Complexity
    /// O(1)
    ///
    /// # Performance
    /// This operation is O(1) as it only performs a single database write.
    pub fn psetex(&self, key: &[u8], milliseconds: i64, value: &[u8]) -> Result<()> {
        // Validate TTL first - must be positive
        if milliseconds <= 0 {
            return Err(RedisErr {
                message: "ERR invalid expire time in psetex".to_string(),
                location: Default::default(),
            });
        }

        // Check overflow and convert to microseconds before acquiring lock
        let ttl_micros_opt = (milliseconds as u64).checked_mul(1_000);

        let string_key = BaseKey::new(key);
        let mut string_value = StringValue::new(value.to_owned());

        // If overflow occurs, set etime to u64::MAX (approximately 292 million years)
        // Otherwise, use set_relative_etime for normal TTL values
        if let Some(ttl_micros) = ttl_micros_opt {
            string_value.set_relative_etime(ttl_micros)?;
        } else {
            string_value.set_etime(u64::MAX);
        }

        // Get lock for the key after validation
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

    /// SETNX key value
    ///
    /// Set key to hold string value if key does not exist.
    /// When key already holds a value, no operation is performed.
    /// SETNX is short for "SET if Not eXists".
    ///
    /// # Time Complexity
    /// O(1)
    ///
    /// # Returns
    /// Integer reply, specifically:
    /// - 1 if the key was set
    /// - 0 if the key was not set
    ///
    /// # Errors
    /// Returns WRONGTYPE error if key exists but holds a non-string value
    pub fn setnx(&self, key: &[u8], value: &[u8]) -> Result<i32> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let string_key = BaseKey::new(key);

        // Get lock for the key to ensure atomicity
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        // Check if key exists and is not expired
        let encode_value = db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?;

        if let Some(val) = encode_value {
            let string_value = ParsedStringsValue::new(&val[..])?;

            // Check if key is expired first - expired keys are treated as non-existent
            if !string_value.is_stale() {
                // Now check type on live key to match Redis compatibility
                // Redis returns WRONGTYPE only for live non-string keys
                self.check_type(val.as_slice(), DataType::String)?;

                // Key exists and is not expired, do not set
                return Ok(0);
            }
            // Key is expired, treat as non-existent and continue to set
        }

        // Key doesn't exist or is expired, set the value
        let string_value = StringValue::new(value.to_owned());

        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, string_key.encode()?, string_value.encode());
        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(1)
    }

    /// GETSET key value
    ///
    /// Atomically sets key to value and returns the old value stored at key.
    /// Returns an error when key exists but does not hold a string value.
    ///
    /// This command is useful for implementing atomic counters and similar patterns.
    ///
    /// # Time Complexity
    /// O(1)
    ///
    /// # Returns
    /// Bulk string reply: the old value stored at key, or nil when key did not exist
    ///
    /// # Errors
    /// Returns WRONGTYPE error if key exists but holds a non-string value
    ///
    /// # Examples
    /// ```text
    /// GETSET mykey "new value"  // Returns old value and sets new value
    /// GETSET counter "0"        // Atomic counter reset pattern
    /// ```
    pub fn getset(&self, key: &[u8], value: &[u8]) -> Result<Option<String>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let string_key = BaseKey::new(key);

        // Get lock for the key to ensure atomicity
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        // Try to get the old value
        let encode_value = db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?;

        let old_value = if let Some(val) = encode_value {
            let string_value = ParsedStringsValue::new(&val[..])?;

            // Check if key is expired first - expired keys are treated as non-existent
            if !string_value.is_stale() {
                // Now check type on live key to match Redis compatibility
                // Redis returns WRONGTYPE only for live non-string keys
                self.check_type(val.as_slice(), DataType::String)?;

                // Key exists and is not expired, return old value
                let user_value = string_value.user_value();
                Some(String::from_utf8_lossy(&user_value).to_string())
            } else {
                // Key is expired, treat as non-existent
                None
            }
        } else {
            // Key doesn't exist
            None
        };

        // Set the new value
        let string_value = StringValue::new(value.to_owned());

        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, string_key.encode()?, string_value.encode());
        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(old_value)
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
    //     self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(&kv.key).to_string(), 1)?;

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

    /// MSET key value [key value ...]
    ///
    /// Sets the given keys to their respective values.
    /// MSET replaces existing values with new values, just like regular SET.
    /// MSET is atomic, so all given keys are set at once. It is not possible
    /// for clients to see that some of the keys were updated while others are unchanged.
    ///
    /// # Arguments
    /// * `kvs` - A slice of (key, value) tuples to set
    ///
    /// # Returns
    /// * `Ok(())` - if the operation succeeded
    /// * `Err(_)` - if the operation failed
    ///
    /// # Time Complexity
    /// O(N) where N is the number of keys to set
    ///
    /// # Examples
    /// ```text
    /// MSET key1 "Hello" key2 "World"  // Sets both keys atomically
    /// ```
    pub fn mset(&self, kvs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;

        // Use WriteBatch for atomic operation
        let mut batch = rocksdb::WriteBatch::default();

        // Process all key-value pairs
        for (key, value) in kvs {
            let string_key = BaseKey::new(key);
            let string_value = StringValue::new(value.to_owned());
            batch.put_cf(&cf, string_key.encode()?, string_value.encode());
        }

        // Atomic write of all key-value pairs
        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(())
    }

    /// MSETNX key value [key value ...]
    ///
    /// Sets the given keys to their respective values, only if all keys don't exist.
    /// MSETNX is atomic, so either all keys are set or no keys are set.
    ///
    /// # Arguments
    /// * `kvs` - A slice of (key, value) tuples to set
    ///
    /// # Returns
    /// * `Ok(true)` - if all keys were set
    /// * `Ok(false)` - if no keys were set because at least one key already exists
    /// * `Err(_)` - if the operation failed
    ///
    /// # Time Complexity
    /// O(N) where N is the number of keys to set
    ///
    /// # Examples
    /// ```text
    /// MSETNX key1 "Hello" key2 "World"  // Sets both keys if neither exists
    /// ```
    pub fn msetnx(&self, kvs: &[(Vec<u8>, Vec<u8>)]) -> Result<bool> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;

        // Check if any key exists and is not expired
        for (key, _) in kvs {
            let string_key = BaseKey::new(key);

            match db
                .get_opt(&string_key.encode()?, &self.read_options)
                .context(RocksSnafu)?
            {
                Some(val) => {
                    let string_value = ParsedStringsValue::new(&val[..])?;

                    // MSETNX returns false if any live key exists (regardless of type)
                    // Redis never returns WRONGTYPE for MSETNX - it only checks existence
                    if !string_value.is_stale() {
                        // Any live key (any type) blocks the batch
                        return Ok(false);
                    }
                    // Key is expired, treat as non-existent and continue checking
                }
                None => {
                    // Key doesn't exist, continue checking
                }
            }
        }

        // All keys don't exist or are expired, set them all
        let mut batch = rocksdb::WriteBatch::default();

        // Process all key-value pairs
        for (key, value) in kvs {
            let string_key = BaseKey::new(key);
            let string_value = StringValue::new(value.to_owned());
            batch.put_cf(&cf, string_key.encode()?, string_value.encode());
        }

        // Atomic write of all key-value pairs
        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(true)
    }

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

        let mut value: i64 = 0;
        let mut ctime: u64 = Utc::now().timestamp_micros() as u64;
        let mut etime: u64 = 0;

        // convert user_value to i64
        if !encode_value.is_empty() {
            let decode_value = ParsedStringsValue::new(&encode_value[..])?;
            // Check if key is expired first - expired keys are treated as non-existent
            if !decode_value.is_stale() {
                // Now check type on live key to match Redis compatibility
                // Redis returns WRONGTYPE only for live non-string keys
                self.check_type(encode_value.as_slice(), DataType::String)?;

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

    pub fn incr_decr_float(&self, key: &[u8], incr: f64) -> Result<f64> {
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

        let mut value: f64 = 0.0;
        let mut ctime: u64 = Utc::now().timestamp_micros() as u64;
        let mut etime: u64 = 0;

        // convert user_value to f64
        if !encode_value.is_empty() {
            let decode_value = ParsedStringsValue::new(&encode_value[..])?;
            // Check if key is expired first - expired keys are treated as non-existent
            if !decode_value.is_stale() {
                // Now check type on live key to match Redis compatibility
                // Redis returns WRONGTYPE only for live non-string keys
                self.check_type(encode_value.as_slice(), DataType::String)?;

                let user_value = decode_value.user_value();
                value = match String::from_utf8_lossy(&user_value).to_string().parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return Err(RedisErr {
                            message: "value is not a valid float".to_string(),
                            location: Default::default(),
                        });
                    }
                };
                ctime = decode_value.ctime();
                etime = decode_value.etime();
            }
        }

        // perform increment
        value += incr;

        // check for NaN or infinity
        if value.is_nan() || value.is_infinite() {
            return Err(RedisErr {
                message: "increment would produce NaN or Infinity".to_string(),
                location: Default::default(),
            });
        }

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

    /// Check if a key exists and is not expired (type-agnostic)
    ///
    /// This helper is used for MSETNX to check key existence without type validation,
    /// preventing WRONGTYPE errors on expired non-string keys.
    ///
    /// # Arguments
    /// * key - The key to check
    ///
    /// # Returns
    /// * Ok(true) - if the key exists and is not expired
    /// * Ok(false) - if the key doesn't exist or is expired
    /// * Err(RedisErr) - if there's a database error
    pub fn key_exists_live(&self, key: &[u8]) -> Result<bool> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let string_key = BaseKey::new(key);
        if let Some(val) = db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
        {
            let meta = ParsedStringsValue::new(&val[..])?;
            Ok(!meta.is_stale())
        } else {
            Ok(false)
        }
    }

    /// SETBIT key offset value
    ///
    /// Sets or clears the bit at offset in the string value stored at key.
    /// The bit is either set or cleared depending on value, which can be either 0 or 1.
    /// When key does not exist, a new string value is created. The string is grown to make sure
    /// it can hold a bit at offset. When the string at key is grown, added bits are set to 0.
    ///
    /// # Arguments
    /// * `key` - The key to modify
    /// * `offset` - The bit offset to set (must be >= 0)
    /// * `value` - The bit value to set (0 or 1)
    ///
    /// # Returns
    /// * `Ok(old_bit)` - the original bit value stored at offset
    /// * `Err(RedisErr)` - if the key holds a value that is not a string (WRONGTYPE error)
    /// * `Err(RedisErr)` - if offset is negative or out of range
    ///
    /// # Time Complexity
    /// O(1) for small strings, O(N) for larger strings where N is the byte length of the string
    pub fn setbit(&self, key: &[u8], offset: i64, value: i64) -> Result<i64> {
        // Validate offset early to avoid unnecessary database operations
        if offset < 0 {
            return Err(RedisErr {
                message: "ERR bit offset is not an integer or out of range".to_string(),
                location: Default::default(),
            });
        }

        // Validate bit value
        if value != 0 && value != 1 {
            return Err(RedisErr {
                message: "ERR bit is not an integer or out of range".to_string(),
                location: Default::default(),
            });
        }

        // Check for offset upper bound to prevent potential overflow
        // Redis has a limit of 2^32-1 for bit offsets
        if offset > (1i64 << 32) - 1 {
            return Err(RedisErr {
                message: "ERR bit offset is not an integer or out of range".to_string(),
                location: Default::default(),
            });
        }

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

        let mut existing_value = Vec::new();
        let mut ctime: u64 = Utc::now().timestamp_micros() as u64;
        let mut etime: u64 = 0;

        if !encode_value.is_empty() {
            let decode_value = ParsedStringsValue::new(&encode_value[..])?;

            // Check if key is expired first - expired keys are treated as non-existent
            if !decode_value.is_stale() {
                // Now check type on live key to match Redis compatibility
                // Redis returns WRONGTYPE only for live non-string keys
                self.check_type(encode_value.as_slice(), DataType::String)?;

                existing_value = decode_value.user_value().to_vec();
                ctime = decode_value.ctime();
                etime = decode_value.etime();
            }
            // If expired, treat as empty string (existing_value remains empty)
        }

        // Calculate byte and bit positions
        let byte_offset = (offset / 8) as usize;
        let bit_offset = (offset % 8) as usize;
        let bit_mask = 1u8 << (7 - bit_offset); // Redis uses big-endian bit ordering

        // Ensure the string is large enough to accommodate the bit at offset
        if byte_offset >= existing_value.len() {
            existing_value.resize(byte_offset + 1, 0);
        }

        // Get the original bit value
        let old_byte = existing_value[byte_offset];
        let old_bit = if (old_byte & bit_mask) != 0 { 1 } else { 0 };

        // Set or clear the bit
        if value == 1 {
            existing_value[byte_offset] |= bit_mask; // Set bit
        } else {
            existing_value[byte_offset] &= !bit_mask; // Clear bit
        }

        // Set new value with metadata
        let mut string_value = StringValue::new(existing_value);
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

        Ok(old_bit)
    }

    /// GETBIT key offset
    ///
    /// Returns the bit value at offset in the string value stored at key.
    /// When key does not exist, or the bit at offset does not exist, the default value is 0.
    ///
    /// # Arguments
    /// * `key` - The key to get the bit from
    /// * `offset` - The bit offset to get (must be >= 0)
    ///
    /// # Returns
    /// * `Ok(bit_value)` - the bit value (0 or 1) stored at offset
    /// * `Err(RedisErr)` - if the key holds a value that is not a string (WRONGTYPE error)
    /// * `Err(RedisErr)` - if offset is negative or out of range
    ///
    /// # Time Complexity
    /// O(1)
    pub fn getbit(&self, key: &[u8], offset: i64) -> Result<i64> {
        // Validate offset early to avoid unnecessary database operations
        if offset < 0 {
            return Err(RedisErr {
                message: "ERR bit offset is not an integer or out of range".to_string(),
                location: Default::default(),
            });
        }

        // Check for offset upper bound to prevent potential overflow
        // Redis has a limit of 2^32-1 for bit offsets
        if offset > (1i64 << 32) - 1 {
            return Err(RedisErr {
                message: "ERR bit offset is not an integer or out of range".to_string(),
                location: Default::default(),
            });
        }

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

        let decode_value = ParsedStringsValue::new(&encode_value[..])?;

        // Check if key is expired first - expired keys are treated as non-existent
        if decode_value.is_stale() {
            return Ok(0);
        }

        // Now check type on live key to match Redis compatibility
        // Redis returns WRONGTYPE only for live non-string keys
        self.check_type(encode_value.as_slice(), DataType::String)?;

        let user_value = decode_value.user_value();

        // Calculate byte and bit positions
        let byte_offset = (offset / 8) as usize;
        let bit_offset = (offset % 8) as usize;
        let bit_mask = 1u8 << (7 - bit_offset); // Redis uses big-endian bit ordering

        // If the offset is beyond the string length, return 0
        if byte_offset >= user_value.len() {
            return Ok(0);
        }

        // Get the bit value
        let byte = user_value[byte_offset];
        let bit_value = if (byte & bit_mask) != 0 { 1 } else { 0 };

        Ok(bit_value)
    }

    /// BITCOUNT key [start end]
    ///
    /// Count the number of set bits (population counting) in a string.
    /// By default all the bytes contained in the string are examined.
    /// It is possible to specify the counting operation only in an interval passing the additional arguments start and end.
    ///
    /// # Arguments
    /// * `key` - The key to count bits in
    /// * `start` - Optional start byte index (inclusive, can be negative)
    /// * `end` - Optional end byte index (inclusive, can be negative)
    ///
    /// # Returns
    /// * `Ok(count)` - the number of bits set to 1
    /// * `Err(RedisErr)` - if the key holds a value that is not a string (WRONGTYPE error)
    ///
    /// # Time Complexity
    /// O(N) where N is the length of the string
    pub fn bitcount(&self, key: &[u8], start: Option<i64>, end: Option<i64>) -> Result<i64> {
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

        let decode_value = ParsedStringsValue::new(&encode_value[..])?;

        // Check if key is expired first - expired keys are treated as non-existent
        if decode_value.is_stale() {
            return Ok(0);
        }

        // Now check type on live key to match Redis compatibility
        // Redis returns WRONGTYPE only for live non-string keys
        self.check_type(encode_value.as_slice(), DataType::String)?;

        let user_value = decode_value.user_value();

        // If no range specified, count all bits
        let (start_idx, end_idx) = if start.is_none() && end.is_none() {
            (0, user_value.len() as i64 - 1)
        } else {
            // Handle range parameters
            let start_val = start.unwrap_or(0);
            let end_val = end.unwrap_or(user_value.len() as i64 - 1);

            // Normalize negative indices
            let start_idx = if start_val < 0 {
                (user_value.len() as i64 + start_val).max(0)
            } else {
                start_val
            };

            let end_idx = if end_val < 0 {
                (user_value.len() as i64 + end_val).max(-1)
            } else {
                end_val
            };

            // If start > end after normalization, return 0
            if start_idx > end_idx {
                return Ok(0);
            }

            (start_idx, end_idx)
        };

        // Clamp indices to valid range
        let start_pos = (start_idx.max(0) as usize).min(user_value.len());
        let end_pos = (end_idx.max(-1) as usize).min(user_value.len().saturating_sub(1));

        // If start position is beyond the string length, return 0
        if start_pos >= user_value.len() {
            return Ok(0);
        }

        // Count bits in the specified range
        let mut count = 0i64;
        for i in start_pos..=end_pos {
            let byte = user_value[i];
            // Count bits set to 1 in the byte
            count += byte.count_ones() as i64;
        }

        Ok(count)
    }

    /// BITPOS key bit [start] [end] [BYTE | BIT]
    ///
    /// Return the position of the first bit set to 1 or 0 in a string.
    /// The position is returned, thinking of the string as an array of bits from left to right,
    /// where the first byte's most significant bit is at position 0, the second byte's most significant bit is at position 8, and so forth.
    ///
    /// # Arguments
    /// * `key` - The key to search bits in
    /// * `bit` - The bit value to search for (0 or 1)
    /// * `start` - Optional start position (inclusive, can be negative)
    /// * `end` - Optional end position (inclusive, can be negative)
    /// * `is_bit_mode` - Whether to interpret start/end as bit positions (true) or byte positions (false)
    ///
    /// # Returns
    /// * `Ok(position)` - the position of the first bit matching the given value, or -1 if not found
    /// * `Err(RedisErr)` - if the key holds a value that is not a string (WRONGTYPE error) or invalid arguments
    ///
    /// # Time Complexity
    /// O(N) where N is the length of the string
    pub fn bitpos(
        &self,
        key: &[u8],
        bit: i64,
        start: Option<i64>,
        end: Option<i64>,
        is_bit_mode: bool,
    ) -> Result<i64> {
        // Validate bit argument
        if bit != 0 && bit != 1 {
            return Err(RedisErr {
                message: "ERR The bit argument must be 1 or 0".to_string(),
                location: Default::default(),
            });
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let string_key = BaseKey::new(key);
        let encode_value = db
            .get_opt(&string_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        // If key doesn't exist, return -1 for bit=1, 0 for bit=0 (Redis behavior)
        if encode_value.is_empty() {
            return Ok(if bit == 1 { -1 } else { 0 });
        }

        let decode_value = ParsedStringsValue::new(&encode_value[..])?;

        // Check if key is expired first - expired keys are treated as non-existent
        if decode_value.is_stale() {
            return Ok(if bit == 1 { -1 } else { 0 });
        }

        // Now check type on live key to match Redis compatibility
        // Redis returns WRONGTYPE only for live non-string keys
        self.check_type(encode_value.as_slice(), DataType::String)?;

        let user_value = decode_value.user_value();

        // If user value is empty, return -1 for bit=1, 0 for bit=0 (Redis behavior)
        if user_value.is_empty() {
            return Ok(if bit == 1 { -1 } else { 0 });
        }

        let (start_pos, end_pos) = if is_bit_mode {
            // Bit mode: start and end are bit positions
            let start_bit = start.unwrap_or(0);
            let end_bit = end.unwrap_or((user_value.len() as i64 * 8) - 1);

            // Normalize negative indices
            let start_bit = if start_bit < 0 {
                (user_value.len() as i64 * 8 + start_bit).max(0)
            } else {
                start_bit
            };

            let end_bit = if end_bit < 0 {
                (user_value.len() as i64 * 8 + end_bit).max(-1)
            } else {
                end_bit
            };

            // Clamp to valid range
            let start_bit = start_bit.max(0).min(user_value.len() as i64 * 8 - 1);
            let end_bit = end_bit.max(-1).min(user_value.len() as i64 * 8 - 1);

            // Convert bit positions to byte positions for iteration
            (start_bit, end_bit)
        } else {
            // Byte mode: start and end are byte positions
            let start_byte = start.unwrap_or(0);
            let end_byte = end.unwrap_or(user_value.len() as i64 - 1);

            // Normalize negative indices
            let start_byte = if start_byte < 0 {
                (user_value.len() as i64 + start_byte).max(0)
            } else {
                start_byte
            };

            let end_byte = if end_byte < 0 {
                (user_value.len() as i64 + end_byte).max(-1)
            } else {
                end_byte
            };

            // Clamp to valid range
            let start_byte = start_byte.max(0).min(user_value.len() as i64 - 1);
            let end_byte = end_byte.max(-1).min(user_value.len() as i64 - 1);

            // If start > end after normalization, return -1 for bit=1, 0 for bit=0
            if start_byte > end_byte {
                return Ok(if bit == 1 { -1 } else { 0 });
            }

            // Convert to bit positions for consistent processing
            (start_byte * 8, end_byte * 8 + 7)
        };

        // Search for the bit
        let target_bit = bit as u8;

        // Iterate through the bit range
        for bit_pos in start_pos..=end_pos {
            let byte_index = (bit_pos / 8) as usize;

            // Check bounds
            if byte_index >= user_value.len() {
                break;
            }

            let bit_index = (bit_pos % 8) as u8;
            let byte = user_value[byte_index];

            // Extract the bit (bit 0 is the most significant bit)
            let current_bit = (byte >> (7 - bit_index)) & 1;

            if current_bit == target_bit {
                return Ok(bit_pos);
            }
        }

        // Bit not found
        Ok(if bit == 1 { -1 } else { 0 })
    }

    /// BITOP operation destkey key [key ...]
    ///
    /// Perform bitwise operations between strings.
    ///
    /// # Arguments
    /// * `operation` - The bitwise operation to perform (AND, OR, XOR, NOT)
    /// * `dest_key` - The key where the result will be stored
    /// * `src_keys` - The source keys to perform the operation on
    ///
    /// # Returns
    /// * `Ok(size)` - the size of the string stored in the destination key
    /// * `Err(RedisErr)` - if any key holds a value that is not a string (WRONGTYPE error) or invalid arguments
    ///
    /// # Time Complexity
    /// O(N) where N is the size of the longest string
    pub fn bitop(&self, operation: &str, dest_key: &[u8], src_keys: &[&[u8]]) -> Result<i64> {
        // Validate operation
        let op_func = match operation.to_uppercase().as_str() {
            "AND" => |a: u8, b: u8| a & b,
            "OR" => |a: u8, b: u8| a | b,
            "XOR" => |a: u8, b: u8| a ^ b,
            "NOT" => {
                // NOT operation only takes one source key
                if src_keys.len() != 1 {
                    return Err(RedisErr {
                        message: "ERR BITOP NOT must be called with a single source key"
                            .to_string(),
                        location: Default::default(),
                    });
                }
                |a: u8, _b: u8| !a
            }
            _ => {
                return Err(RedisErr {
                    message: "ERR syntax error".to_string(),
                    location: Default::default(),
                });
            }
        };

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Get lock for the destination key
        let dest_key_str = String::from_utf8_lossy(dest_key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &dest_key_str);

        // For NOT operation, we only need one source key
        if operation.to_uppercase() == "NOT" {
            if src_keys.len() != 1 {
                return Err(RedisErr {
                    message: "ERR BITOP NOT must be called with a single source key".to_string(),
                    location: Default::default(),
                });
            }

            let src_key = src_keys[0];
            let string_key = BaseKey::new(src_key);
            let encode_value = db
                .get_opt(&string_key.encode()?, &self.read_options)
                .context(RocksSnafu)?
                .unwrap_or_else(Vec::new);

            // If source key doesn't exist or is empty, result is an empty string
            if encode_value.is_empty() {
                // Store an empty string as the result
                let mut string_value = StringValue::new(vec![]);
                string_value.set_ctime(Utc::now().timestamp_micros() as u64);
                string_value.set_etime(0); // No expiration by default

                let dest_string_key = BaseKey::new(dest_key);
                let cf =
                    self.get_cf_handle(ColumnFamilyIndex::MetaCF)
                        .context(OptionNoneSnafu {
                            message: "cf is not initialized".to_string(),
                        })?;
                let mut batch = rocksdb::WriteBatch::default();
                batch.put_cf(&cf, dest_string_key.encode()?, string_value.encode());
                db.write_opt(batch, &self.write_options)
                    .context(RocksSnafu)?;
                return Ok(0);
            }

            let decode_value = ParsedStringsValue::new(&encode_value[..])?;

            // Check if key is expired first - expired keys are treated as non-existent
            if decode_value.is_stale() {
                // Store an empty string as the result
                let mut string_value = StringValue::new(vec![]);
                string_value.set_ctime(Utc::now().timestamp_micros() as u64);
                string_value.set_etime(0); // No expiration by default

                let dest_string_key = BaseKey::new(dest_key);
                let cf =
                    self.get_cf_handle(ColumnFamilyIndex::MetaCF)
                        .context(OptionNoneSnafu {
                            message: "cf is not initialized".to_string(),
                        })?;
                let mut batch = rocksdb::WriteBatch::default();
                batch.put_cf(&cf, dest_string_key.encode()?, string_value.encode());
                db.write_opt(batch, &self.write_options)
                    .context(RocksSnafu)?;
                return Ok(0);
            }

            let user_value = decode_value.user_value();

            // Apply NOT operation to each byte
            let result: Vec<u8> = user_value.iter().map(|&byte| !byte).collect();

            // Store result
            let mut string_value = StringValue::new(result);
            string_value.set_ctime(Utc::now().timestamp_micros() as u64);
            string_value.set_etime(0); // No expiration by default

            let dest_string_key = BaseKey::new(dest_key);
            let cf = self
                .get_cf_handle(ColumnFamilyIndex::MetaCF)
                .context(OptionNoneSnafu {
                    message: "cf is not initialized".to_string(),
                })?;
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(&cf, dest_string_key.encode()?, string_value.encode());
            db.write_opt(batch, &self.write_options)
                .context(RocksSnafu)?;

            return Ok(string_value.user_value_len() as i64);
        }

        // For AND, OR, XOR operations
        if src_keys.is_empty() {
            return Err(RedisErr {
                message: "ERR wrong number of arguments for 'bitop' command".to_string(),
                location: Default::default(),
            });
        }

        // Get all source values
        let mut src_values: Vec<Vec<u8>> = Vec::new();
        let mut max_len = 0;

        for src_key in src_keys {
            let string_key = BaseKey::new(src_key);
            let encode_value = db
                .get_opt(&string_key.encode()?, &self.read_options)
                .context(RocksSnafu)?
                .unwrap_or_else(Vec::new);

            // If any source key doesn't exist or is empty, treat as empty string
            if encode_value.is_empty() {
                src_values.push(Vec::new());
                continue;
            }

            let decode_value = ParsedStringsValue::new(&encode_value[..])?;

            // Check if key is expired first - expired keys are treated as non-existent
            if decode_value.is_stale() {
                src_values.push(Vec::new());
                continue;
            }

            // Now check type on live key to match Redis compatibility
            // Redis returns WRONGTYPE only for live non-string keys
            self.check_type(encode_value.as_slice(), DataType::String)?;

            let user_value = decode_value.user_value();
            max_len = max_len.max(user_value.len());
            src_values.push(user_value.to_vec());
        }

        // If all source keys are empty, result is an empty string
        if max_len == 0 {
            // Store an empty string as the result
            let mut string_value = StringValue::new(vec![]);
            string_value.set_ctime(Utc::now().timestamp_micros() as u64);
            string_value.set_etime(0); // No expiration by default

            let dest_string_key = BaseKey::new(dest_key);
            let cf = self
                .get_cf_handle(ColumnFamilyIndex::MetaCF)
                .context(OptionNoneSnafu {
                    message: "cf is not initialized".to_string(),
                })?;
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(&cf, dest_string_key.encode()?, string_value.encode());
            db.write_opt(batch, &self.write_options)
                .context(RocksSnafu)?;
            return Ok(0);
        }

        // Apply the operation
        let mut result = vec![0u8; max_len];

        // For each byte position
        for i in 0..max_len {
            let mut current_byte = if operation.to_uppercase() == "AND" {
                0xFF // For AND, start with all bits set
            } else {
                0x00 // For OR and XOR, start with all bits unset
            };

            // For each source value
            for src_value in &src_values {
                let byte = if i < src_value.len() {
                    src_value[i]
                } else {
                    0x00 // Pad with zeros for shorter strings
                };

                current_byte = op_func(current_byte, byte);
            }

            result[i] = current_byte;
        }

        // Store result
        let mut string_value = StringValue::new(result);
        string_value.set_ctime(Utc::now().timestamp_micros() as u64);
        string_value.set_etime(0); // No expiration by default

        let dest_string_key = BaseKey::new(dest_key);
        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, dest_string_key.encode()?, string_value.encode());
        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(string_value.user_value_len() as i64)
    }
}
