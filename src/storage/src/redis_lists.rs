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

//! Redis lists operations implementation
//! This module provides list operations for Redis storage

use std::time::{SystemTime, UNIX_EPOCH};
use rocksdb::{WriteBatch, WriteOptions, ReadOptions, Direction};

use crate::{Result, StorageError};
use crate::base_data_value_format::{DataType, InternalValue, ParsedInternalValue};
use crate::redis::Redis;

impl Redis {
    /// Returns the element at index in the list stored at key
    pub fn lindex(&self, key: &[u8], index: i64, value: &mut String) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the list metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&meta_value).to_string());
                if parsed_meta.data_type() != DataType::List {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()));
                }
                
                // Get list size
                let list_size = parsed_meta.get_size().unwrap_or(0);
                if list_size == 0 {
                    return Err(StorageError::KeyNotFound("Empty list".to_string()));
                }
                
                // Convert negative index
                let real_index = if index < 0 {
                    list_size as i64 + index
                } else {
                    index
                };
                
                // Check if index is out of range
                if real_index < 0 || real_index >= list_size as i64 {
                    return Err(StorageError::InvalidFormat("Index out of range".to_string()));
                }
                
                // Create data key
                let data_key = self.encode_list_data_key(key, real_index as u64, parsed_meta.version());
                
                // Get the value
                match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF), &data_key, &read_options)? {
                    Some(data_value) => {
                        // Parse the data value
                        let parsed_data = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&data_value).to_string());
                        *value = String::from_utf8_lossy(parsed_data.user_value()).to_string();
                        Ok(())
                    },
                    None => {
                        Err(StorageError::KeyNotFound("List element not found".to_string()))
                    }
                }
            },
            None => {
                Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()))
            }
        }
    }
    
    /// Returns the length of the list stored at key
    pub fn llen(&self, key: &[u8], len: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the list metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&meta_value).to_string());
                if parsed_meta.data_type() != DataType::List {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    *len = 0;
                    return Ok(());
                }
                
                // Return the length
                *len = parsed_meta.get_size().unwrap_or(0) as i32;
                Ok(())
            },
            None => {
                *len = 0;
                Ok(())
            }
        }
    }
    
    /// Removes and returns the first element of the list stored at key
    pub fn lpop(&self, key: &[u8], value: &mut String) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the list metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let mut parsed_meta = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&meta_value).to_string());
                if parsed_meta.data_type() != DataType::List {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()));
                }
                
                // Get list size
                let list_size = parsed_meta.get_size().unwrap_or(0);
                if list_size == 0 {
                    return Err(StorageError::KeyNotFound("Empty list".to_string()));
                }
                
                // Create data key for the first element
                let data_key = self.encode_list_data_key(key, 0, parsed_meta.version());
                
                // Get the value
                match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF), &data_key, &read_options)? {
                    Some(data_value) => {
                        // Parse the data value
                        let parsed_data = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&data_value).to_string());
                        *value = String::from_utf8_lossy(parsed_data.user_value()).to_string();
                        
                        // Update metadata
                        let mut batch = WriteBatch::default();
                        
                        // Delete the first element
                        batch.delete_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF), &data_key);
                        
                        // Update list size
                        parsed_meta.set_size(list_size - 1);
                        
                        // If list is empty, delete the key
                        if parsed_meta.size() == 0 {
                            batch.delete(key);
                        } else {
                            // Shift all elements
                            // In a real implementation, we would need to shift all elements
                            // or use a more efficient data structure
                            
                            // Update metadata
                            let new_meta_value = parsed_meta.encode();
                            batch.put(key, &new_meta_value);
                        }
                        
                        // Write batch to DB
                        db.write_opt(batch, &self.default_write_options)?;
                        
                        // Update statistics
                        self.update_specific_key_statistics(DataType::List, &String::from_utf8_lossy(key).to_string(), 1)?;
                        
                        Ok(())
                    },
                    None => {
                        Err(StorageError::KeyNotFound("List element not found".to_string()))
                    }
                }
            },
            None => {
                Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()))
            }
        }
    }
    
    /// Insert all the specified values at the head of the list stored at key
    pub fn lpush(&self, key: &[u8], values: &[&[u8]], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        if values.is_empty() {
            return Ok(());
        }
        
        let mut version = 0;
        let mut list_size = 0;
        let mut timestamp = 0;
        
        // Get the list metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&meta_value).to_string());
                if parsed_meta.data_type() != DataType::List {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if !parsed_meta.is_expired(now) {
                    version = parsed_meta.version();
                    list_size = parsed_meta.size();
                    timestamp = parsed_meta.etime();
                }
            },
            None => {}
        }
        
        // Create batch
        let mut batch = WriteBatch::default();
        
        // Create metadata
        let mut internal_meta = InternalValue::new(DataType::List, &[]);
        internal_meta.set_version(version);
        internal_meta.set_size(list_size + values.len() as u64);
        if timestamp > 0 {
            internal_meta.set_etime(timestamp);
        }
        let meta_value = internal_meta.encode();
        
        // Add metadata to batch
        batch.put(key, &meta_value);
        
        // Add values to batch
        for (i, value) in values.iter().enumerate() {
            // In a real implementation, we would need to shift existing elements
            // or use a more efficient data structure
            
            // Create data key
            let data_key = self.encode_list_data_key(key, i as u64, version);
            
            // Create data value
            let internal_data = InternalValue::new(DataType::List, value);
            let data_value = internal_data.encode();
            
            // Add to batch
            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF), &data_key, &data_value);
        }
        
        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::List, &String::from_utf8_lossy(key).to_string(), values.len() as u64)?;
        
        // Set the return value to the new list length
        *ret = (list_size + values.len() as u64) as i32;
        
        Ok(())
    }
    
    /// Insert all the specified values at the tail of the list stored at key
    pub fn rpush(&self, key: &[u8], values: &[&[u8]], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        if values.is_empty() {
            return Ok(());
        }
        
        let mut version = 0;
        let mut list_size = 0;
        let mut timestamp = 0;
        
        // Get the list metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let parsed_meta = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&meta_value).to_string());
                if parsed_meta.data_type() != DataType::List {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if !parsed_meta.is_expired(now) {
                    version = parsed_meta.version();
                    list_size = parsed_meta.size();
                    timestamp = parsed_meta.etime();
                }
            },
            None => {}
        }
        
        // Create batch
        let mut batch = WriteBatch::default();
        
        // Create metadata
        let mut internal_meta = InternalValue::new(DataType::List, &[]);
        internal_meta.set_version(version);
        internal_meta.set_size(list_size + values.len() as u64);
        if timestamp > 0 {
            internal_meta.set_etime(timestamp);
        }
        let meta_value = internal_meta.encode();
        
        // Add metadata to batch
        batch.put(key, &meta_value);
        
        // Add values to batch
        for (i, value) in values.iter().enumerate() {
            // Create data key
            let data_key = self.encode_list_data_key(key, list_size + i as u64, version);
            
            // Create data value
            let internal_data = InternalValue::new(DataType::List, value);
            let data_value = internal_data.encode();
            
            // Add to batch
            batch.put_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF), &data_key, &data_value);
        }
        
        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::List, &String::from_utf8_lossy(key).to_string(), values.len() as u64)?;
        
        // Set the return value to the new list length
        *ret = (list_size + values.len() as u64) as i32;
        
        Ok(())
    }
    
    /// Removes and returns the last element of the list stored at key
    pub fn rpop(&self, key: &[u8], value: &mut String) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Get the list metadata
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the metadata
                let mut parsed_meta = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&meta_value).to_string());
                if parsed_meta.data_type() != DataType::List {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_meta.is_expired(now) {
                    return Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()));
                }
                
                // Get list size
                let list_size = parsed_meta.get_size().unwrap_or(0);
                if list_size == 0 {
                    return Err(StorageError::KeyNotFound("Empty list".to_string()));
                }
                
                // Create data key for the last element
                let data_key = self.encode_list_data_key(key, list_size - 1, parsed_meta.version());
                
                // Get the value
                match db.get_cf_opt(self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF), &data_key, &read_options)? {
                    Some(data_value) => {
                        // Parse the data value
                        let parsed_data = ParsedInternalValue::new(DataType::List, String::from_utf8_lossy(&data_value).to_string());
                        *value = String::from_utf8_lossy(parsed_data.user_value()).to_string();
                        
                        // Update metadata
                        let mut batch = WriteBatch::default();
                        
                        // Delete the last element
                        batch.delete_cf(self.get_handle(crate::redis::ColumnFamilyIndex::ListsDataCF), &data_key);
                        
                        // Update list size
                        parsed_meta.set_size(list_size - 1);
                        
                        // If list is empty, delete the key
                        if parsed_meta.size() == 0 {
                            batch.delete(key);
                        } else {
                            // Update metadata
                            let new_meta_value = parsed_meta.encode();
                            batch.put(key, &new_meta_value);
                        }
                        
                        // Write batch to DB
                        db.write_opt(batch, &self.default_write_options)?;
                        
                        // Update statistics
                        self.update_specific_key_statistics(DataType::List, &String::from_utf8_lossy(key).to_string(), 1)?;
                        
                        Ok(())
                    },
                    None => {
                        Err(StorageError::KeyNotFound("List element not found".to_string()))
                    }
                }
            },
            None => {
                Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()))
            }
        }
    }
    
    /// Helper method to encode list data key
    fn encode_list_data_key(&self, key: &[u8], index: u64, version: u64) -> Vec<u8> {
        // In a real implementation, this would encode the key properly
        // For now, we'll use a simple format: key + "_" + index + "_" + version
        let mut data_key = Vec::with_capacity(key.len() + 20);
        data_key.extend_from_slice(key);
        data_key.push(b'_');
        data_key.extend_from_slice(index.to_string().as_bytes());
        data_key.push(b'_');
        data_key.extend_from_slice(version.to_string().as_bytes());
        data_key
    }
    
    /// Helper method to get column family handle
    pub fn get_handle(&self, cf_index: crate::redis::ColumnFamilyIndex) -> &rocksdb::ColumnFamily {
        &self.handles[cf_index as usize]
    }
}