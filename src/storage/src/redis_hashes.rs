/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


//! Redis hashes operations implementation
//! This module provides hash operations for Redis storage

use std::time::{SystemTime, UNIX_EPOCH};
use rocksdb::{WriteBatch, WriteOptions, ReadOptions, Direction, IteratorMode};

use crate::{Result, StorageError};
use crate::base_data_value_format::{DataType, InternalValue, ParsedInternalValue};
use crate::redis::Redis;
use crate::types::{FieldValue, ValueStatus};

impl Redis {
    /// Delete one or more hash fields
    pub fn hdel(&self, key: &[u8], fields: &[Vec<u8>], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let mut parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    *ret = 0;
                    return Ok(());
                }
                
                // Get hash size
                let hash_size = parsed_meta.size();
                if hash_size == 0 {
                    *ret = 0;
                    return Ok(());
                }
                
                // Create batch
                let mut batch = WriteBatch::default();
                let mut deleted = 0;
                
                // Delete fields
                for field in fields {
                    // Create data key
                    let data_key = self.encode_hash_data_key(key, field, parsed_meta.version());
                    
                    // Check if field exists
                    match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &data_key, &read_options)? {
                        Some(_) => {
                            // Delete field
                            batch.delete_cf(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &data_key);
                            deleted += 1;
                        },
                        None => {}
                    }
                }
                
                if deleted > 0 {
                    // Update metadata
                    let new_size = hash_size - deleted as u64;
                    
                    // If hash is empty, delete the key
                    if new_size == 0 {
                        batch.delete(key);
                    } else {
                        // Update metadata
                        parsed_meta.set_size(new_size);
                        let new_meta_value = parsed_meta.encode();
                        batch.put(key, &new_meta_value);
                    }
                    
                    // Write batch to DB
                    db.write_opt(batch, &self.default_write_options)?;
                    
                    // Update statistics
                    self.update_specific_key_statistics(DataType::Hash, &String::from_utf8_lossy(key).to_string(), deleted as u64)?;
                }
                
                *ret = deleted;
                Ok(())
            },
            None => {
                *ret = 0;
                Ok(())
            }
        }
    }
    
    /// Determine if a hash field exists
    pub fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Ok(false);
                }
                
                // Get hash size
                let hash_size = parsed_meta.size();
                if hash_size == 0 {
                    return Ok(false);
                }
                
                // Create data key
                let data_key = self.encode_hash_data_key(key, field, parsed_meta.version());
                
                // Check if field exists
                match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &data_key, &read_options)? {
                    Some(_) => Ok(true),
                    None => Ok(false)
                }
            },
            None => Ok(false)
        }
    }
    
    /// Get the value of a hash field
    pub fn hget(&self, key: &[u8], field: &[u8], value: &mut String) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()));
                }
                
                // Get hash size
                let hash_size = parsed_meta.size();
                if hash_size == 0 {
                    return Err(StorageError::KeyNotFound("Empty hash".to_string()));
                }
                
                // Create data key
                let data_key = self.encode_hash_data_key(key, field, parsed_meta.version());
                
                // Get the value
                match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &data_key, &read_options)? {
                    Some(data_value) => {
                        // Parse the data value
                        let parsed_data = ParsedInternalValue::new(&data_value);
                        *value = String::from_utf8_lossy(parsed_data.user_value()).to_string();
                        Ok(())
                    },
                    None => {
                        Err(StorageError::KeyNotFound("Hash field not found".to_string()))
                    }
                }
            },
            None => {
                Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()))
            }
        }
    }
    
    /// Get all the fields and values in a hash
    pub fn hgetall(&self, key: &[u8], fvs: &mut Vec<FieldValue>) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Ok(());
                }
                
                // Get hash size
                let hash_size = parsed_meta.size();
                if hash_size == 0 {
                    return Ok(());
                }
                
                // Create prefix for data keys
                let prefix = self.encode_hash_prefix(key, parsed_meta.version());
                
                // Iterate over all fields
                let iter_opt = ReadOptions::default();
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF),
                    iter_opt,
                    IteratorMode::From(&prefix, Direction::Forward)
                );
                
                for result in iter {
                    let (k, v) = result?;
                    // Check if key has the correct prefix
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    
                    // Extract field from key
                    let field = self.decode_hash_field(&k, &prefix);
                    
                    // Parse the data value
                    let parsed_data = ParsedInternalValue::new(&v);
                    let value = String::from_utf8_lossy(parsed_data.user_value()).to_string();
                    
                    // Add to result
                    fvs.push(FieldValue {
                        field: field.to_vec(),
                        value: value.into_bytes(),
                    });
                }
                
                Ok(())
            },
            None => Ok(())
        }
    }
    
    /// Get all the fields and values in a hash with TTL
    pub fn hgetall_with_ttl(&self, key: &[u8], fvs: &mut Vec<FieldValue>, ttl: &mut i64) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    *ttl = -2; // Expired
                    return Ok(());
                }
                
                // Calculate TTL
                let etime = parsed_meta.etime();
                if etime > 0 {
                    *ttl = etime as i64 - now as i64;
                    if *ttl < 0 {
                        *ttl = 0;
                    }
                } else {
                    *ttl = -1; // No expiration
                }
                
                // Get hash size
                let hash_size = parsed_meta.size();
                if hash_size == 0 {
                    return Ok(());
                }
                
                // Create prefix for data keys
                let prefix = self.encode_hash_prefix(key, parsed_meta.version());
                
                // Iterate over all fields
                let iter_opt = ReadOptions::default();
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF),
                    iter_opt,
                    IteratorMode::From(&prefix, Direction::Forward)
                );
                
                for result in iter {
                    let (k, v) = result?;
                    // Check if key has the correct prefix
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    
                    // Extract field from key
                    let field = self.decode_hash_field(&k, &prefix);
                    
                    // Parse the data value
                    let parsed_data = ParsedInternalValue::new(&v);
                    let value = String::from_utf8_lossy(parsed_data.user_value()).to_string();
                    
                    // Add to result
                    fvs.push(FieldValue {
                        field: field.to_vec(),
                        value: value.into_bytes(),
                    });
                }
                
                Ok(())
            },
            None => {
                *ttl = -2; // Key does not exist
                Ok(())
            }
        }
    }
    
    /// Get all the fields in a hash
    pub fn hkeys(&self, key: &[u8], fields: &mut Vec<String>) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Ok(());
                }
                
                // Get hash size
                let hash_size = parsed_meta.size();
                if hash_size == 0 {
                    return Ok(());
                }
                
                // Create prefix for data keys
                let prefix = self.encode_hash_prefix(key, parsed_meta.version());
                
                // Iterate over all fields
                let iter_opt = ReadOptions::default();
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF),
                    iter_opt,
                    IteratorMode::From(&prefix, Direction::Forward)
                );
                
                for result in iter {
                    let (k, _) = result?;
                    // Check if key has the correct prefix
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    
                    // Extract field from key
                    let field = self.decode_hash_field(&k, &prefix);
                    
                    // Add to result
                    fields.push(String::from_utf8_lossy(field).to_string());
                }
                
                Ok(())
            },
            None => Ok(())
        }
    }
    
    /// Get the number of fields in a hash
    pub fn hlen(&self, key: &[u8], len: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    *len = 0;
                    return Ok(());
                }
                
                // Return the length
                *len = parsed_meta.size() as i32;
                Ok(())
            },
            None => {
                *len = 0;
                Ok(())
            }
        }
    }
    
    /// Set the string value of a hash field
    pub fn hset(&self, key: &[u8], field: &[u8], value: &[u8], res: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        let mut version = 0;
        let mut hash_size = 0;
        let mut timestamp = 0;
        let mut exists = false;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if !parsed_meta.is_expired(now) {
                    version = parsed_meta.version();
                    hash_size = parsed_meta.size();
                    timestamp = parsed_meta.etime();
                    
                    // Check if field exists
                    let data_key = self.encode_hash_data_key(key, field, version);
                    match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &data_key, &read_options)? {
                        Some(_) => {
                            exists = true;
                        },
                        None => {}
                    }
                }
            },
            None => {}
        }
        
        // Create batch
        let mut batch = WriteBatch::default();
        
        // Create metadata
        let mut internal_meta = InternalValue::new(DataType::Hashes, &[]);
        internal_meta.set_version(version);
        if !exists {
            internal_meta.set_size(hash_size + 1);
        } else {
            internal_meta.set_size(hash_size);
        }
        if timestamp > 0 {
            internal_meta.set_etime(timestamp);
        }
        let meta_value = internal_meta.encode();
        
        // Add metadata to batch
        batch.put(key, &meta_value);
        
        // Create data key
        let data_key = self.encode_hash_data_key(key, field, version);
        
        // Create data value
        let internal_data = InternalValue::new(DataType::Hashes, value);
        let data_value = internal_data.encode();
        
        // Add to batch
        batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &data_key, &data_value);
        
        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::Hashes, &String::from_utf8_lossy(key).to_string(), 1)?;
        
        // Set the return value
        *res = if exists { 0 } else { 1 };
        
        Ok(())
    }
    
    /// Set the value of a hash field, only if the field does not exist
    pub fn hsetnx(&self, key: &[u8], field: &[u8], value: &[u8], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        let mut version = 0;
        let mut hash_size = 0;
        let mut timestamp = 0;
        let mut exists = false;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if !parsed_meta.is_expired(now) {
                    version = parsed_meta.version();
                    hash_size = parsed_meta.size();
                    timestamp = parsed_meta.etime();
                    
                    // Check if field exists
                    let data_key = self.encode_hash_data_key(key, field, version);
                    match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &data_key, &read_options)? {
                        Some(_) => {
                            exists = true;
                            *ret = 0; // Field exists, not set
                            return Ok(());
                        },
                        None => {}
                    }
                }
            },
            None => {}
        }
        
        // Create batch
        let mut batch = WriteBatch::default();
        
        // Create metadata
        let mut internal_meta = InternalValue::new(DataType::Hashes, &[]);
        internal_meta.set_version(version);
        internal_meta.set_size(hash_size + 1);
        if timestamp > 0 {
            internal_meta.set_etime(timestamp);
        }
        let meta_value = internal_meta.encode();
        
        // Add metadata to batch
        batch.put(key, &meta_value);
        
        // Create data key
        let data_key = self.encode_hash_data_key(key, field, version);
        
        // Create data value
        let internal_data = InternalValue::new(DataType::Hashes, value);
        let data_value = internal_data.encode();
        
        // Add to batch
        batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &data_key, &data_value);
        
        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::Hashes, &String::from_utf8_lossy(key).to_string(), 1)?;
        
        // Set the return value
        *ret = 1; // Field set
        
        Ok(())
    }
    
    /// Get all the values in a hash
    pub fn hvals(&self, key: &[u8], values: &mut Vec<String>) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the hash metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                if parsed_meta.data_type() != DataType::Hash {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Ok(());
                }
                
                // Get hash size
                let hash_size = parsed_meta.size();
                if hash_size == 0 {
                    return Ok(());
                }
                
                // Create prefix for data keys
                let prefix = self.encode_hash_prefix(key, parsed_meta.version());
                
                // Iterate over all fields
                let iter_opt = ReadOptions::default();
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF),
                    iter_opt,
                    IteratorMode::From(&prefix, Direction::Forward)
                );
                
                for result in iter {
                    let (k, v) = result?;
                    // Check if key has the correct prefix
                    if !k.starts_with(&prefix) {
                        break;
                    }
                    
                    // Parse the data value
                    let parsed_data = ParsedInternalValue::new(&v);
                    let value = String::from_utf8_lossy(parsed_data.user_value()).to_string();
                    
                    // Add to result
                    values.push(value);
                }
                
                Ok(())
            },
            None => Ok(())
        }
    }
    
    /// Helper method to encode hash data key
    fn encode_hash_data_key(&self, key: &[u8], field: &[u8], version: u64) -> Vec<u8> {
        // In a real implementation, this would encode the key properly
        // For now, we'll use a simple format: key + "_" + field + "_" + version
        let mut data_key = Vec::with_capacity(key.len() + field.len() + 20);
        data_key.extend_from_slice(key);
        data_key.push(b'_');
        data_key.extend_from_slice(field);
        data_key.push(b'_');
        data_key.extend_from_slice(version.to_string().as_bytes());
        data_key
    }
    
    /// Helper method to encode hash prefix
    fn encode_hash_prefix(&self, key: &[u8], version: u64) -> Vec<u8> {
        // In a real implementation, this would encode the prefix properly
        // For now, we'll use a simple format: key + "_"
        let mut prefix = Vec::with_capacity(key.len() + 1);
        prefix.extend_from_slice(key);
        prefix.push(b'_');
        prefix
    }
    
    /// Helper method to decode hash field from data key
    fn decode_hash_field<'a>(&self, data_key: &'a [u8], prefix: &[u8]) -> &'a [u8] {
        // In a real implementation, this would decode the field properly
        // For now, we'll extract the field from the data key
        let field_start = prefix.len();
        let field_end = data_key.iter().rposition(|&b| b == b'_').unwrap_or(data_key.len());
        &data_key[field_start..field_end]
    }
}