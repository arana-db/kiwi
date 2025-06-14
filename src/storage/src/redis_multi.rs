//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Redis multi-key operations implementation
//! This module provides multi-key operations for Redis storage

use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashSet;
use rocksdb::{WriteBatch, WriteOptions, ReadOptions};

use crate::{Result, StorageError};
use crate::format::{DataType, InternalValue, ParsedInternalValue};
use crate::redis::Redis;
use crate::lock::ScopeRecordLock;
use crate::types::KeyValue;

impl Redis {
    /// Delete a key
    pub fn del(&self, key: &[u8]) -> Result<i32> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Create lock for the key
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), key);
        
        // Try to get the meta value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Ok(0);
                }
                
                // Get the data type
                let data_type = parsed_meta.data_type();
                let version = parsed_meta.version();
                
                // Create batch for deletion
                let mut batch = WriteBatch::default();
                
                // Delete the meta key
                batch.delete(key);
                
                // Delete data based on type
                match data_type {
                    DataType::Strings => {
                        // No additional data to delete for strings
                    },
                    DataType::Hashes => {
                        // Delete all hash fields
                        let prefix = self.encode_hash_data_prefix(key, version);
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::HashesDataCF, &prefix)?;
                    },
                    DataType::Lists => {
                        // Delete all list elements
                        let prefix = self.encode_list_data_prefix(key, version);
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ListsDataCF, &prefix)?;
                    },
                    DataType::Sets => {
                        // Delete all set members
                        let prefix = self.encode_sets_member_prefix(key, version);
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::SetsDataCF, &prefix)?;
                    },
                    DataType::ZSets => {
                        // Delete all sorted set members and scores
                        let member_prefix = self.encode_zsets_member_prefix(key, version);
                        let score_prefix = self.encode_zsets_score_prefix(key, version);
                        
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ZsetsDataCF, &member_prefix)?;
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ZsetsScoreCF, &score_prefix)?;
                    },
                    _ => {
                        // Other types not supported yet
                    }
                }
                
                // Write batch to DB
                db.write_opt(batch, &self.default_write_options)?;
                
                // Update statistics
                self.update_specific_key_statistics(data_type, &String::from_utf8_lossy(key).to_string(), 1)?;
                
                Ok(1)
            },
            None => {
                Ok(0)
            }
        }
    }
    
    /// Delete multiple keys
    pub fn multi_del(&self, keys: &[&[u8]]) -> Result<i32> {
        if keys.is_empty() {
            return Ok(0);
        }
        
        // Create locks for all keys
        let _locks = self.lock_mgr.as_ref().multi_lock(keys);
        
        let mut deleted = 0;
        for &key in keys {
            deleted += self.del(key)?;
        }
        
        Ok(deleted)
    }
    
    /// Check if a key exists
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Try to get the meta value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Ok(false);
                }
                
                Ok(true)
            },
            None => {
                Ok(false)
            }
        }
    }
    
    /// Set a key's time to live in seconds
    pub fn expire(&self, key: &[u8], ttl: i64) -> Result<i32> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        if ttl <= 0 {
            // Delete the key if ttl is negative or zero
            return self.del(key);
        }
        
        // Create lock for the key
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), key);
        
        // Try to get the meta value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let mut parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Ok(0);
                }
                
                // Set expiration time
                parsed_meta.set_etime(now + ttl as u64);
                
                // Write updated meta value
                db.put_opt(key, &parsed_meta.encode(), &self.default_write_options)?;
                
                Ok(1)
            },
            None => {
                Ok(0)
            }
        }
    }
    
    /// Get the time to live for a key in seconds
    pub fn ttl(&self, key: &[u8]) -> Result<i64> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Try to get the meta value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Get expiration time
                let etime = parsed_meta.etime();
                if etime == 0 {
                    // No expiration
                    return Ok(-1);
                }
                
                // Calculate TTL
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if etime <= now {
                    // Key has expired
                    return Ok(-2);
                }
                
                Ok((etime - now) as i64)
            },
            None => {
                // Key does not exist
                Ok(-2)
            }
        }
    }
    
    /// Rename a key
    pub fn rename(&self, key: &[u8], new_key: &[u8]) -> Result<()> {
        if key == new_key {
            return Ok(());
        }
        
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Create locks for both keys
        let _lock1 = ScopeRecordLock::new(self.lock_mgr.as_ref(), key);
        let _lock2 = ScopeRecordLock::new(self.lock_mgr.as_ref(), new_key);
        
        // Try to get the source key
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()));
                }
                
                // Get the data type and version
                let data_type = parsed_meta.data_type();
                let version = parsed_meta.version();
                
                // Create batch for operations
                let mut batch = WriteBatch::default();
                
                // Delete destination key if it exists
                match db.get_opt(new_key, &read_options)? {
                    Some(dest_meta_value) => {
                        let dest_parsed_meta = ParsedInternalValue::new(&dest_meta_value);
                        if !dest_parsed_meta.is_expired(now) {
                            // Delete destination key data
                            let dest_data_type = dest_parsed_meta.data_type();
                            let dest_version = dest_parsed_meta.version();
                            
                            match dest_data_type {
                                DataType::Hashes => {
                                    let prefix = self.encode_hash_data_prefix(new_key, dest_version);
                                    self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::HashesDataCF, &prefix)?;
                                },
                                DataType::Lists => {
                                    let prefix = self.encode_list_data_prefix(new_key, dest_version);
                                    self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ListsDataCF, &prefix)?;
                                },
                                DataType::Sets => {
                                    let prefix = self.encode_sets_member_prefix(new_key, dest_version);
                                    self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::SetsDataCF, &prefix)?;
                                },
                                DataType::ZSets => {
                                    let member_prefix = self.encode_zsets_member_prefix(new_key, dest_version);
                                    let score_prefix = self.encode_zsets_score_prefix(new_key, dest_version);
                                    
                                    self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ZsetsDataCF, &member_prefix)?;
                                    self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ZsetsScoreCF, &score_prefix)?;
                                },
                                _ => {}
                            }
                        }
                    },
                    None => {}
                }
                
                // Copy data based on type
                match data_type {
                    DataType::Strings => {
                        // Just copy the meta value
                        batch.put(new_key, &meta_value);
                    },
                    DataType::Hashes => {
                        // Copy meta value
                        batch.put(new_key, &meta_value);
                        
                        // Copy all hash fields
                        let prefix = self.encode_hash_data_prefix(key, version);
                        let iter = db.iterator_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF),
                            read_options.clone(),
                            IteratorMode::From(&prefix, Direction::Forward)
                        );
                        
                        for (old_key, value) in iter {
                            if !old_key.starts_with(&prefix) {
                                break;
                            }
                            
                            // Create new key with same suffix but different prefix
                            let suffix = &old_key[prefix.len()..];
                            let new_data_key = self.encode_hash_data_key(new_key, suffix, version);
                            
                            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::HashesDataCF), &new_data_key, &value);
                        }
                    },
                    DataType::Lists => {
                        // Copy meta value
                        batch.put(new_key, &meta_value);
                        
                        // Copy all list elements
                        let prefix = self.encode_list_data_prefix(key, version);
                        let iter = db.iterator_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF),
                            read_options.clone(),
                            IteratorMode::From(&prefix, Direction::Forward)
                        );
                        
                        for (old_key, value) in iter {
                            if !old_key.starts_with(&prefix) {
                                break;
                            }
                            
                            // Create new key with same suffix but different prefix
                            let suffix = &old_key[prefix.len()..];
                            let new_data_key = self.encode_list_data_key(new_key, suffix[0] as u64, version);
                            
                            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF), &new_data_key, &value);
                        }
                    },
                    DataType::Sets => {
                        // Copy meta value
                        batch.put(new_key, &meta_value);
                        
                        // Copy all set members
                        let prefix = self.encode_sets_member_prefix(key, version);
                        let iter = db.iterator_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            read_options.clone(),
                            IteratorMode::From(&prefix, Direction::Forward)
                        );
                        
                        for (old_key, value) in iter {
                            if !old_key.starts_with(&prefix) {
                                break;
                            }
                            
                            // Extract member from key
                            let member = self.decode_sets_member_from_key(&old_key);
                            let new_data_key = self.encode_sets_member_key(new_key, version, &member);
                            
                            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF), &new_data_key, &value);
                        }
                    },
                    DataType::ZSets => {
                        // Copy meta value
                        batch.put(new_key, &meta_value);
                        
                        // Copy all sorted set members
                        let member_prefix = self.encode_zsets_member_prefix(key, version);
                        let iter = db.iterator_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsDataCF),
                            read_options.clone(),
                            IteratorMode::From(&member_prefix, Direction::Forward)
                        );
                        
                        for (old_key, value) in iter {
                            if !old_key.starts_with(&member_prefix) {
                                break;
                            }
                            
                            // Extract member from key
                            let member = &old_key[member_prefix.len()..];
                            let new_member_key = self.encode_zsets_member_key(new_key, version, member);
                            
                            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsDataCF), &new_member_key, &value);
                            
                            // Get score from value
                            let parsed_value = ParsedInternalValue::new(&value);
                            let score_bytes = parsed_value.user_value();
                            let score = f64::from_be_bytes(score_bytes[0..8].try_into().unwrap());
                            
                            // Create score key
                            let new_score_key = self.encode_zsets_score_key(new_key, version, score, member);
                            let score_value = InternalValue::new(DataType::None, &[]);
                            
                            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsScoreCF), &new_score_key, &score_value.encode());
                        }
                    },
                    _ => {
                        return Err(StorageError::InvalidFormat(format!("Unsupported data type: {:?}", data_type)));
                    }
                }
                
                // Delete the source key
                batch.delete(key);
                
                // Delete source data based on type
                match data_type {
                    DataType::Hashes => {
                        let prefix = self.encode_hash_data_prefix(key, version);
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::HashesDataCF, &prefix)?;
                    },
                    DataType::Lists => {
                        let prefix = self.encode_list_data_prefix(key, version);
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ListsDataCF, &prefix)?;
                    },
                    DataType::Sets => {
                        let prefix = self.encode_sets_member_prefix(key, version);
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::SetsDataCF, &prefix)?;
                    },
                    DataType::ZSets => {
                        let member_prefix = self.encode_zsets_member_prefix(key, version);
                        let score_prefix = self.encode_zsets_score_prefix(key, version);
                        
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ZsetsDataCF, &member_prefix)?;
                        self.delete_keys_with_prefix(&mut batch, crate::redis::ColumnFamilyIndex::ZsetsScoreCF, &score_prefix)?;
                    },
                    _ => {}
                }
                
                // Write batch to DB
                db.write_opt(batch, &self.default_write_options)?;
                
                Ok(())
            },
            None => {
                Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()))
            }
        }
    }
    
    /// Helper method to delete keys with a specific prefix
    fn delete_keys_with_prefix(&self, batch: &mut WriteBatch, cf_index: crate::redis::ColumnFamilyIndex, prefix: &[u8]) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        let read_options = ReadOptions::default();
        let iter = db.iterator_cf_opt(
            self.get_handle(cf_index),
            read_options,
            IteratorMode::From(prefix, Direction::Forward)
        );
        
        for (key, _) in iter {
            if !key.starts_with(prefix) {
                break;
            }
            
            batch.delete_cf(self.get_handle(cf_index), &key);
        }
        
        Ok(())
    }
    
    /// Helper method to encode hash data prefix
    fn encode_hash_data_prefix(&self, key: &[u8], version: u64) -> Vec<u8> {
        let mut result = Vec::with_capacity(key.len() + 9);
        result.extend_from_slice(key);
        result.push(0); // separator
        
        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());
        
        result
    }
    
    /// Helper method to encode hash data key
    fn encode_hash_data_key(&self, key: &[u8], field: &[u8], version: u64) -> Vec<u8> {
        let mut result = Vec::with_capacity(key.len() + 8 + field.len() + 2);
        result.extend_from_slice(key);
        result.push(0); // separator
        
        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());
        
        result.push(0); // separator
        result.extend_from_slice(field);
        
        result
    }
    
    /// Helper method to encode list data prefix
    fn encode_list_data_prefix(&self, key: &[u8], version: u64) -> Vec<u8> {
        let mut result = Vec::with_capacity(key.len() + 9);
        result.extend_from_slice(key);
        result.push(0); // separator
        
        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());
        
        result
    }
    
    /// Helper method to encode list data key
    fn encode_list_data_key(&self, key: &[u8], index: u64, version: u64) -> Vec<u8> {
        let mut result = Vec::with_capacity(key.len() + 8 + 8 + 2);
        result.extend_from_slice(key);
        result.push(0); // separator
        
        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());
        
        result.push(0); // separator
        result.extend_from_slice(&index.to_be_bytes());
        
        result
    }
    
    /// Helper method to encode zsets member prefix
    fn encode_zsets_member_prefix(&self, key: &[u8], version: u64) -> Vec<u8> {
        let mut result = Vec::with_capacity(key.len() + 9);
        result.extend_from_slice(key);
        result.push(0); // separator
        
        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());
        
        result
    }
    
    /// Helper method to encode zsets score prefix
    fn encode_zsets_score_prefix(&self, key: &[u8], version: u64) -> Vec<u8> {
        let mut result = Vec::with_capacity(key.len() + 9);
        result.extend_from_slice(key);
        result.push(0); // separator
        
        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());
        
        result
    }
}