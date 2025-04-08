// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//  of patent rights can be found in the PATENTS file in the same directory.

//! Redis strings operations implementation
//! This module provides string operations for Redis storage

use std::time::{SystemTime, UNIX_EPOCH};
use rocksdb::{WriteBatch, WriteOptions, ReadOptions};

use crate::{Result, StorageError};
use crate::base_data_value_format::{DataType, InternalValue, ParsedInternalValue};
use crate::redis::Redis;
use crate::types::KeyValue;

impl Redis {
    /// Append a value to the string stored at key
    pub fn append(&self, key: &[u8], value: &[u8], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        let mut string_value = String::new();
        let mut version = 0;
        let mut timestamp = 0;
        
        // Try to get the existing value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(existing) => {
                // Parse the existing value
                let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
                if parsed_value.data_type() != DataType::String {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_value.is_expired(now) {
                    // Create a new value
                    string_value = String::from_utf8_lossy(value).to_string();
                    version = 0;
                } else {
                    // Append to existing value
                    string_value = format!("{}{}", 
                        String::from_utf8_lossy(parsed_value.user_value()), 
                        String::from_utf8_lossy(value));
                    version = parsed_value.version();
                    timestamp = parsed_value.etime();
                }
            },
            None => {
                // Create a new value
                string_value = String::from_utf8_lossy(value).to_string();
            }
        }
        
        // Create the new value
        let mut internal_value = InternalValue::new(DataType::String, string_value.as_bytes());
        internal_value.set_version(version);
        if timestamp > 0 {
            internal_value.set_etime(timestamp);
        }
        
        // Write to DB
        let encoded_value = internal_value.encode();
        db.put_opt(key, &encoded_value, &self.default_write_options)?;
        
        // Set the return value to the new string length
        *ret = string_value.len() as i32;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;
        
        Ok(())
    }
    
    /// Get the value of a key
    pub fn get(&self, key: &[u8], value: &mut String) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Try to get the value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(existing) => {
                // Parse the existing value
                let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
                if parsed_value.data_type() != DataType::String {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_value.is_expired(now) {
                    return Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()));
                }
                
                // Return the value
                *value = String::from_utf8_lossy(parsed_value.user_value()).to_string();
                Ok(())
            },
            None => {
                Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()))
            }
        }
    }
    
    /// Get the value and TTL of a key
    pub fn get_with_ttl(&self, key: &[u8], value: &mut String, ttl: &mut i64) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Try to get the value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(existing) => {
                // Parse the existing value
                let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
                if parsed_value.data_type() != DataType::String {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_value.is_expired(now) {
                    return Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()));
                }
                
                // Calculate TTL
                let etime = parsed_value.etime();
                if etime > 0 {
                    *ttl = etime as i64 - now as i64;
                    if *ttl < 0 {
                        *ttl = 0;
                    }
                } else {
                    *ttl = -1; // No expiration
                }
                
                // Return the value
                *value = String::from_utf8_lossy(parsed_value.user_value()).to_string();
                Ok(())
            },
            None => {
                Err(StorageError::KeyNotFound(String::from_utf8_lossy(key).to_string()))
            }
        }
    }
    
    /// Set key to hold the string value
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Create the new value
        let internal_value = InternalValue::new(DataType::String, value);
        let encoded_value = internal_value.encode();
        
        // Write to DB
        db.put_opt(key, &encoded_value, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;
        
        Ok(())
    }
    
    /// Set key to hold string value and expiration time
    pub fn setex(&self, key: &[u8], value: &[u8], ttl: i64) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        if ttl <= 0 {
            return Err(StorageError::InvalidFormat("Invalid expire time".to_string()));
        }
        
        // Create the new value with expiration
        let mut internal_value = InternalValue::new(DataType::String, value);
        internal_value.set_relative_timestamp(ttl as u64)?;
        let encoded_value = internal_value.encode();
        
        // Write to DB
        db.put_opt(key, &encoded_value, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;
        
        Ok(())
    }
    
    /// Set key to hold string value if key does not exist
    pub fn setnx(&self, key: &[u8], value: &[u8], ret: &mut i32, ttl: i64) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Check if key exists
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(existing) => {
                // Parse the existing value
                let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if !parsed_value.is_expired(now) {
                    *ret = 0; // Key exists, not set
                    return Ok(());
                }
            },
            None => {}
        }
        
        // Create the new value
        let mut internal_value = InternalValue::new(DataType::String, value);
        if ttl > 0 {
            internal_value.set_relative_timestamp(ttl as u64)?;
        }
        let encoded_value = internal_value.encode();
        
        // Write to DB
        db.put_opt(key, &encoded_value, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;
        
        *ret = 1; // Key set
        Ok(())
    }
    
    /// Set the string value and return the old value
    pub fn getset(&self, key: &[u8], value: &[u8], old_value: &mut String) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Try to get the existing value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(existing) => {
                // Parse the existing value
                let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
                if parsed_value.data_type() != DataType::String {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if !parsed_value.is_expired(now) {
                    // Return the old value
                    *old_value = String::from_utf8_lossy(parsed_value.user_value()).to_string();
                }
            },
            None => {}
        }
        
        // Create the new value
        let internal_value = InternalValue::new(DataType::String, value);
        let encoded_value = internal_value.encode();
        
        // Write to DB
        db.put_opt(key, &encoded_value, &self.default_write_options)?;
        
        // Update statistics
        self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(key).to_string(), 1)?;
        
        Ok(())
    }
    
    /// Set multiple keys to multiple values
    pub fn mset(&self, kvs: &[KeyValue]) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        let mut batch = WriteBatch::default();
        
        for kv in kvs {
            // Create the new value
            let internal_value = InternalValue::new(DataType::String, &kv.value);
            let encoded_value = internal_value.encode();
            
            // Add to batch
            batch.put(&kv.key, &encoded_value);
            
            // Update statistics
            self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(&kv.key).to_string(), 1)?;
        }
        
        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;
        
        Ok(())
    }
    
    /// Set multiple keys to multiple values, only if none of the keys exist
    pub fn msetnx(&self, kvs: &[KeyValue], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Check if any key exists
        let read_options = ReadOptions::default();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        for kv in kvs {
            match db.get_opt(&kv.key, &read_options)? {
                Some(existing) => {
                    // Parse the existing value
                    let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
                    
                    // Check if expired
                    if !parsed_value.is_expired(now) {
                        *ret = 0; // At least one key exists, not set
                        return Ok(());
                    }
                },
                None => {}
            }
        }
        
        // All keys don't exist, set them
        let mut batch = WriteBatch::default();
        
        for kv in kvs {
            // Create the new value
            let internal_value = InternalValue::new(DataType::String, &kv.value);
            let encoded_value = internal_value.encode();
            
            // Add to batch
            batch.put(&kv.key, &encoded_value);
            
            // Update statistics
            self.update_specific_key_statistics(DataType::String, &String::from_utf8_lossy(&kv.key).to_string(), 1)?;
        }
        
        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;
        
        *ret = 1; // All keys set
        Ok(())
    }
    
    /// Get the length of the string value stored at key
    pub fn strlen(&self, key: &[u8], len: &mut i32) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
        
        // Try to get the value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(existing) => {
                // Parse the existing value
                let parsed_value = ParsedInternalValue::new(DataType::String, String::from_utf8_lossy(&existing).to_string());
                if parsed_value.data_type() != DataType::String {
                    return Err(StorageError::InvalidFormat("Wrong type of value".to_string()));
                }
                
                // Check if expired
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if parsed_value.is_expired(now) {
                    *len = 0;
                    return Ok(());
                }
                
                // Return the length
                *len = parsed_value.user_value().len() as i32;
                Ok(())
            },
            None => {
                *len = 0;
                Ok(())
            }
        }
    }
}