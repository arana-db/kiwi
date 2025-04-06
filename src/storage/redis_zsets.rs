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

//! Redis sorted sets operations implementation
//! This module provides sorted set operations for Redis storage

use std::time::{SystemTime, UNIX_EPOCH};
use std::cmp::{min, max};
use std::collections::HashSet;
use rocksdb::{WriteBatch, WriteOptions, ReadOptions, Direction, IteratorMode};

use crate::{Result, StorageError};
use crate::format::{DataType, InternalValue, ParsedInternalValue};
use crate::redis::Redis;
use crate::lock::ScopeRecordLock;
use crate::types::{ScoreMember, KeyVersion};

impl Redis {
    /// Scan sorted sets key number
    pub fn scan_zsets_key_num(&self, key_info: &mut crate::types::KeyInfo) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        let mut keys = 0;
        let mut expires = 0;
        let mut ttl_sum = 0;
        let mut invalid_keys = 0;
        
        // Get current time
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        // Create iterator options
        let mut iterator_options = ReadOptions::default();
        iterator_options.fill_cache(false);
        
        // Create snapshot
        let snapshot = db.snapshot();
        iterator_options.set_snapshot(&snapshot);
        
        // Iterate through all keys in meta column family
        let iter = db.iterator_cf_opt(
            self.get_handle(crate::redis::ColumnFamilyIndex::MetaCF),
            iterator_options,
            IteratorMode::Start
        );
        
        for (_, value) in iter {
            // Parse the meta value
            let parsed_value = ParsedInternalValue::new(&value);
            
            // Check if it's a zsets type
            if parsed_value.data_type() != DataType::ZSets {
                continue;
            }
            
            // Check if it's stale or empty
            if parsed_value.is_expired(now) || parsed_value.size() == 0 {
                invalid_keys += 1;
            } else {
                keys += 1;
                
                // Check if it has expiration
                let etime = parsed_value.etime();
                if etime > 0 {
                    expires += 1;
                    ttl_sum += etime - now;
                }
            }
        }
        
        // Set key info
        key_info.keys = keys;
        key_info.expires = expires;
        key_info.avg_ttl = if expires > 0 { ttl_sum / expires } else { 0 };
        key_info.invalid_keys = invalid_keys;
        
        Ok(())
    }
    
    /// Add one or more members to a sorted set, or update its score if it already exists
    pub fn zadd(&self, key: &[u8], score_members: &[ScoreMember], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Filter duplicate members
        let mut unique = HashSet::new();
        let mut filtered_score_members = Vec::new();
        
        for sm in score_members {
            if !unique.contains(&sm.member) {
                unique.insert(sm.member.clone());
                filtered_score_members.push(sm.clone());
            }
        }
        
        if filtered_score_members.is_empty() {
            *ret = 0;
            return Ok(());
        }
        
        // Create lock for the key
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), key);
        
        let mut version = 0;
        let mut batch = WriteBatch::default();
        
        // Try to get the existing meta value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let mut parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Check if it's the right type
                if parsed_meta.data_type() != DataType::ZSets {
                    if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) {
                        // Treat as not found if expired
                        let mut zsets_meta = InternalValue::new(DataType::ZSets, &[]);
                        zsets_meta.set_size(filtered_score_members.len() as u64);
                        version = zsets_meta.update_version();
                        
                        batch.put(key, &zsets_meta.encode());
                        
                        // Add all members
                        for sm in &filtered_score_members {
                            let member_key = self.encode_zsets_member_key(key, version, sm.member.as_bytes());
                            let score_key = self.encode_zsets_score_key(key, version, sm.score, sm.member.as_bytes());
                            
                            let score_value = InternalValue::new(DataType::None, &[]);
                            let member_value = InternalValue::new(DataType::None, &sm.score.to_be_bytes());
                            
                            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsDataCF), &member_key, &member_value.encode());
                            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsScoreCF), &score_key, &score_value.encode());
                        }
                        
                        *ret = filtered_score_members.len() as i32;
                    } else {
                        return Err(StorageError::InvalidFormat(format!("Wrong type for key: {}", String::from_utf8_lossy(key))));
                    }
                } else if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) || parsed_meta.size() == 0 {
                    // Initialize meta value if expired or empty
                    version = parsed_meta.update_version();
                    parsed_meta.set_size(filtered_score_members.len() as u64);
                    
                    batch.put(key, &parsed_meta.encode());
                    
                    // Add all members
                    for sm in &filtered_score_members {
                        let member_key = self.encode_zsets_member_key(key, version, sm.member.as_bytes());
                        let score_key = self.encode_zsets_score_key(key, version, sm.score, sm.member.as_bytes());
                        
                        let score_value = InternalValue::new(DataType::None, &[]);
                        let member_value = InternalValue::new(DataType::None, &sm.score.to_be_bytes());
                        
                        batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsDataCF), &member_key, &member_value.encode());
                        batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsScoreCF), &score_key, &score_value.encode());
                    }
                    
                    *ret = filtered_score_members.len() as i32;
                } else {
                    // Add new members or update existing ones
                    let mut count = 0;
                    version = parsed_meta.version();
                    
                    for sm in &filtered_score_members {
                        let member_key = self.encode_zsets_member_key(key, version, sm.member.as_bytes());
                        
                        // Check if member already exists
                        match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsDataCF), &member_key, &read_options)? {
                            Some(existing_value) => {
                                // Member exists, check if score is different
                                let parsed_value = ParsedInternalValue::new(&existing_value);
                                let existing_score = f64::from_be_bytes(parsed_value.user_value()[0..8].try_into().unwrap());
                                
                                if existing_score != sm.score {
                                    // Update score
                                    let old_score_key = self.encode_zsets_score_key(key, version, existing_score, sm.member.as_bytes());
                                    let new_score_key = self.encode_zsets_score_key(key, version, sm.score, sm.member.as_bytes());
                                    
                                    let score_value = InternalValue::new(DataType::None, &[]);
                                    let member_value = InternalValue::new(DataType::None, &sm.score.to_be_bytes());
                                    
                                    batch.delete_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsScoreCF), &old_score_key);
                                    batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsScoreCF), &new_score_key, &score_value.encode());
                                    batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsDataCF), &member_key, &member_value.encode());
                                }
                            },
                            None => {
                                // Add new member
                                count += 1;
                                let score_key = self.encode_zsets_score_key(key, version, sm.score, sm.member.as_bytes());
                                
                                let score_value = InternalValue::new(DataType::None, &[]);
                                let member_value = InternalValue::new(DataType::None, &sm.score.to_be_bytes());
                                
                                batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsDataCF), &member_key, &member_value.encode());
                                batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsScoreCF), &score_key, &score_value.encode());
                            }
                        }
                    }
                    
                    *ret = count;
                    
                    if count > 0 {
                        // Update meta value with new count
                        parsed_meta.set_size(parsed_meta.size() + count as u64);
                        batch.put(key, &parsed_meta.encode());
                    }
                }
            },
            None => {
                // Create new sorted set
                let mut zsets_meta = InternalValue::new(DataType::ZSets, &[]);
                zsets_meta.set_size(filtered_score_members.len() as u64);
                version = zsets_meta.update_version();
                
                batch.put(key, &zsets_meta.encode());
                
                // Add all members
                for sm in &filtered_score_members {
                    let member_key = self.encode_zsets_member_key(key, version, sm.member.as_bytes());
                    let score_key = self.encode_zsets_score_key(key, version, sm.score, sm.member.as_bytes());
                    
                    let score_value = InternalValue::new(DataType::None, &[]);
                    let member_value = InternalValue::new(DataType::None, &sm.score.to_be_bytes());
                    
                    batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsDataCF), &member_key, &member_value.encode());
                    batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsScoreCF), &score_key, &score_value.encode());
                }
                
                *ret = filtered_score_members.len() as i32;
            }
        }
        
        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::ZSets, &String::from_utf8_lossy(key).to_string(), *ret as u64)?;
        
        Ok(())
    }
    
    /// Get the number of members in a sorted set
    pub fn zcard(&self, key: &[u8], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        *ret = 0;
        
        // Try to get the meta value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Check if it's the right type
                if parsed_meta.data_type() != DataType::ZSets {
                    return Err(StorageError::InvalidFormat(format!("Wrong type for key: {}", String::from_utf8_lossy(key))));
                }
                
                // Check if expired
                if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) {
                    return Ok(());
                }
                
                // Get the count
                *ret = parsed_meta.size() as i32;
            },
            None => {
                // Key not found
            }
        }
        
        Ok(())
    }
    
    /// Count the number of members in a sorted set with scores within the given values
    pub fn zcount(&self, key: &[u8], min_score: f64, max_score: f64, ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        *ret = 0;
        
        // Try to get the meta value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let parsed_meta = ParsedInternalValue::new(&meta_value);
                
                // Check if it's the right type
                if parsed_meta.data_type() != DataType::ZSets {
                    return Err(StorageError::InvalidFormat(format!("Wrong type for key: {}", String::from_utf8_lossy(key))));
                }
                
                // Check if expired
                if parsed_meta.is_expired(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()) {
                    return Ok(());
                }
                
                // Get the version
                let version = parsed_meta.version();
                
                // Create prefix for iteration
                let min_score_key = self.encode_zsets_score_key(key, version, min_score, &[]);
                
                // Iterate through scores
                let mut count = 0;
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::ZsetsScoreCF),
                    read_options.clone(),
                    IteratorMode::From(&min_score_key, Direction::Forward)
                );
                
                for (key_bytes, _) in iter {
                    // Extract score from key
                    let (score, member_key) = self.decode_score_from_zsets_score_key(&key_bytes);
                    
                    // Check if we're still in the same zset
                    if !member_key.starts_with(key) {
                        break;
                    }
                    
                    // Check if score is within range
                    if score > max_score {
                        break;
                    }
                    
                    count += 1;
                }
                
                *ret = count;
            },
            None => {
                // Key not found
            }
        }
        
        Ok(())
    }
    
    /// Helper method to encode zsets member key
    fn encode_zsets_member_key(&self, key: &[u8], version: u64, member: &[u8]) -> Vec<u8> {
        // Format: key + separator + version + separator + member
        let mut result = Vec::with_capacity(key.len() + 8 + member.len() + 2);
        result.extend_from_slice(key);
        result.push(0); // separator
        
        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());
        
        result.push(0); // separator
        result.extend_from_slice(member);
        
        result
    }
    
    /// Helper method to encode zsets score key
    fn encode_zsets_score_key(&self, key: &[u8], version: u64, score: f64, member: &[u8]) -> Vec<u8> {
        // Format: key + separator + version + separator + score + separator + member
        let mut result = Vec::with_capacity(key.len() + 8 + 8 + member.len() + 3);
        result.extend_from_slice(key);
        result.push(0); // separator
        
        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());
        
        result.push(0); // separator
        
        // Add score (8 bytes)
        result.extend_from_slice(&score.to_be_bytes());
        
        result.push(0); // separator
        result.extend_from_slice(member);
        
        result
    }
    
    /// Helper method to decode score from zsets score key
    fn decode_score_from_zsets_score_key(&self, key: &[u8]) -> (f64, Vec<u8>) {
        // Find the second and third separators
        let mut separator_count = 0;
        let mut score_pos = 0;
        let mut member_pos = 0;
        
        for (i, &byte) in key.iter().enumerate() {
            if byte == 0 {
                separator_count += 1;
                if separator_count == 2 {
                    score_pos = i + 1;
                } else if separator_count == 3 {
                    member_pos = i + 1;
                    break;
                }
            }
        }
        
        // Extract score
        let score_bytes = &key[score_pos..score_pos+8];
        let score = f64::from_be_bytes(score_bytes.try_into().unwrap());
        
        // Extract member key (the part before the score)
        let member_key = key[0..score_pos-1].to_vec();
        
        (score, member_key)
    }
}