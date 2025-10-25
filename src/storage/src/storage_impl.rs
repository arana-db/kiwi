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

use crate::error::{Error, InvalidArgumentSnafu, Result};
use crate::slot_indexer::key_to_slot_id;
use crate::storage::Storage;

// Implementation of Storage struct methods
impl Storage {
    // Strings Commands Implementation

    // Set key to hold the string value. if key
    // already holds a value, it is overwritten
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].set(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<String> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].get(key)
    }

    pub fn mget(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<String>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // If single instance, process directly for better performance
        if self.insts.len() == 1 {
            return self.insts[0].mget(keys);
        }

        // Multi-instance: group keys by instance and process
        let mut instance_keys: std::collections::HashMap<usize, Vec<(usize, &Vec<u8>)>> =
            std::collections::HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            let slot_id = key_to_slot_id(key);
            let instance_id = self.slot_indexer.get_instance_id(slot_id);
            instance_keys
                .entry(instance_id)
                .or_default()
                .push((idx, key));
        }

        let mut results = vec![None; keys.len()];

        for (instance_id, key_indices) in instance_keys {
            let instance_keys: Vec<Vec<u8>> =
                key_indices.iter().map(|(_, key)| (*key).clone()).collect();
            let instance_results = self.insts[instance_id].mget(&instance_keys)?;

            for ((original_idx, _), result) in key_indices.iter().zip(instance_results) {
                results[*original_idx] = result;
            }
        }

        Ok(results)
    }

    pub fn mset(&self, kvs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        // If single instance, process directly for better performance
        if self.insts.len() == 1 {
            return self.insts[0].mset(kvs);
        }

        // Define type alias for complex HashMap type to satisfy clippy::type_complexity
        type InstanceKvs = std::collections::HashMap<usize, Vec<(Vec<u8>, Vec<u8>)>>;

        // Multi-instance: group key-value pairs by instance and process
        let mut instance_kvs: InstanceKvs = std::collections::HashMap::new();

        for (key, value) in kvs {
            let slot_id = key_to_slot_id(key);
            let instance_id = self.slot_indexer.get_instance_id(slot_id);
            instance_kvs
                .entry(instance_id)
                .or_default()
                .push((key.clone(), value.clone()));
        }

        // Execute mset on each instance
        for (instance_id, instance_kvs) in instance_kvs {
            self.insts[instance_id].mset(&instance_kvs)?;
        }

        Ok(())
    }

    pub fn msetnx(&self, kvs: &[(Vec<u8>, Vec<u8>)]) -> Result<bool> {
        if kvs.is_empty() {
            return Ok(true);
        }

        if self.insts.len() == 1 {
            return self.insts[0].msetnx(kvs);
        }

        type InstanceKvs = std::collections::HashMap<usize, Vec<(Vec<u8>, Vec<u8>)>>;
        let mut instance_kvs: InstanceKvs = std::collections::HashMap::new();

        // Group key-value pairs by instance
        for (key, value) in kvs {
            let slot_id = key_to_slot_id(key);
            let instance_id = self.slot_indexer.get_instance_id(slot_id);
            instance_kvs
                .entry(instance_id)
                .or_default()
                .push((key.clone(), value.clone()));
        }

        // Sort keys to prevent deadlock, then dedup to avoid re-locking the same key
        let mut sorted_keys: Vec<&Vec<u8>> = kvs.iter().map(|(key, _)| key).collect();
        sorted_keys.sort();
        sorted_keys.dedup();

        // Acquire locks on all keys to ensure atomicity
        // Use hex encoding to avoid lock aliasing from binary keys
        let _locks: Vec<_> = sorted_keys
            .iter()
            .map(|key| {
                let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                self.lock_mgr.lock(&key_hex)
            })
            .collect();

        // Check if any key already exists
        for (instance_id, kv_pairs) in &instance_kvs {
            let instance = &self.insts[*instance_id];
            for (key, _) in kv_pairs {
                match instance.key_exists_live(key) {
                    Ok(true) => return Ok(false), // any live key blocks
                    Ok(false) => continue,        // missing/expired
                    Err(err) => return Err(err),
                }
            }
        }

        // Set all key-value pairs since none exist
        for (instance_id, kv_pairs) in instance_kvs {
            self.insts[instance_id].mset(&kv_pairs)?;
        }

        Ok(true)
    }

    pub fn incr_decr(&self, key: &[u8], incr: i64) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].incr_decr(key, incr)
    }

    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].append(key, value)
    }

    pub fn strlen(&self, key: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].strlen(key)
    }

    pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Result<Vec<u8>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].getrange(key, start, end)
    }

    pub fn setrange(&self, key: &[u8], offset: i64, value: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].setrange(key, offset, value)
    }

    pub fn setex(&self, key: &[u8], seconds: i64, value: &[u8]) -> Result<()> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].setex(key, seconds, value)
    }

    pub fn psetex(&self, key: &[u8], milliseconds: i64, value: &[u8]) -> Result<()> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].psetex(key, milliseconds, value)
    }

    pub fn setnx(&self, key: &[u8], value: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].setnx(key, value)
    }

    pub fn setbit(&self, key: &[u8], offset: i64, value: i64) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].setbit(key, offset, value)
    }

    pub fn getbit(&self, key: &[u8], offset: i64) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].getbit(key, offset)
    }

    pub fn bitcount(&self, key: &[u8], start: Option<i64>, end: Option<i64>) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].bitcount(key, start, end)
    }

    pub fn bitpos(
        &self,
        key: &[u8],
        bit: i64,
        start: Option<i64>,
        end: Option<i64>,
        is_bit_mode: bool,
    ) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].bitpos(key, bit, start, end, is_bit_mode)
    }

    pub fn bitop(&self, operation: &str, dest_key: &[u8], src_keys: &[&[u8]]) -> Result<i64> {
        let slot_id = key_to_slot_id(dest_key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        // Ensure all source keys map to the same slot as the destination key
        for key in src_keys {
            let src_slot_id = key_to_slot_id(key);
            if self.slot_indexer.get_instance_id(src_slot_id) != instance_id {
                return Err(Error::RedisErr {
                    message: "CROSSSLOT Keys in request don't hash to the same slot".into(),
                    location: Default::default(),
                });
            }
        }

        self.insts[instance_id].bitop(operation, dest_key, src_keys)
    }

    pub fn getset(&self, key: &[u8], value: &[u8]) -> Result<Option<String>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].getset(key, value)
    }

    pub fn incr_decr_float(&self, key: &[u8], incr: f64) -> Result<f64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].incr_decr_float(key, incr)
    }

    // Hash Commands Implementation

    pub fn hset(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hset(key, field, value)
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<String>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hget(key, field)
    }

    pub fn hdel(&self, key: &[u8], fields: &[Vec<u8>]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hdel(key, fields)
    }

    pub fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hexists(key, field)
    }

    pub fn hlen(&self, key: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hlen(key)
    }

    pub fn hkeys(&self, key: &[u8]) -> Result<Vec<String>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hkeys(key)
    }

    pub fn hgetall(&self, key: &[u8]) -> Result<Vec<(String, String)>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hgetall(key)
    }

    pub fn hvals(&self, key: &[u8]) -> Result<Vec<String>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hvals(key)
    }

    pub fn hmget(&self, key: &[u8], fields: &[Vec<u8>]) -> Result<Vec<Option<String>>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hmget(key, fields)
    }

    pub fn hmset(&self, key: &[u8], field_values: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hmset(key, field_values)
    }

    pub fn hsetnx(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hsetnx(key, field, value)
    }

    pub fn hincrby(&self, key: &[u8], field: &[u8], increment: i64) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hincrby(key, field, increment)
    }

    pub fn hincrbyfloat(&self, key: &[u8], field: &[u8], increment: f64) -> Result<f64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hincrbyfloat(key, field, increment)
    }

    pub fn hstrlen(&self, key: &[u8], field: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].hstrlen(key, field)
    }

    // List Commands Implementation

    pub fn lpush(&self, key: &[u8], values: &[Vec<u8>]) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].lpush(key, values)
    }

    pub fn rpush(&self, key: &[u8], values: &[Vec<u8>]) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].rpush(key, values)
    }

    pub fn lpop(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].lpop(key, count)
    }

    pub fn rpop(&self, key: &[u8], count: Option<usize>) -> Result<Option<Vec<Vec<u8>>>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].rpop(key, count)
    }

    pub fn llen(&self, key: &[u8]) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].llen(key)
    }

    pub fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Vec<u8>>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].lindex(key, index)
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].lrange(key, start, stop)
    }

    pub fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> Result<()> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].lset(key, index, value)
    }

    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> Result<()> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].ltrim(key, start, stop)
    }

    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> Result<i64> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].lrem(key, count, value)
    }

    // TTL Commands Implementation

    /// Get time to live for a key in seconds
    pub fn ttl(&self, key: &[u8]) -> Result<i64> {
        let key_str = String::from_utf8_lossy(key);

        // First check if key exists
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        // Check if key exists in storage
        match self.insts[instance_id].key_exists_live(key) {
            Ok(false) => return Ok(-2), // Key doesn't exist
            Err(e) => return Err(e),    // Propagate storage errors
            Ok(true) => {}              // Key exists, continue
        }

        if let Some(expiration_manager) = &self.expiration_manager {
            match expiration_manager.get_ttl_seconds(&key_str) {
                Some(-2) => Ok(-2), // Key expired (treat as doesn't exist)
                Some(-1) => Ok(-1), // Key exists but has no expiration
                Some(ttl) => Ok(ttl), // Positive TTL value
                None => Ok(-1), // Shouldn't happen with new API, key has no expiration
            }
        } else {
            Ok(-1) // No expiration manager, key has no expiration
        }
    }

    /// Get time to live for a key in milliseconds
    pub fn pttl(&self, key: &[u8]) -> Result<i64> {
        let key_str = String::from_utf8_lossy(key);

        // First check if key exists
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        // Check if key exists in storage
        match self.insts[instance_id].key_exists_live(key) {
            Ok(false) => return Ok(-2), // Key doesn't exist
            Err(e) => return Err(e),    // Propagate storage errors
            Ok(true) => {}              // Key exists, continue
        }

        if let Some(expiration_manager) = &self.expiration_manager {
            match expiration_manager.get_ttl_milliseconds(&key_str) {
                Some(-2) => Ok(-2), // Key expired (treat as doesn't exist)
                Some(-1) => Ok(-1), // Key exists but has no expiration
                Some(ttl) => Ok(ttl), // Positive TTL value
                None => Ok(-1), // Shouldn't happen with new API, key has no expiration
            }
        } else {
            Ok(-1) // No expiration manager, key has no expiration
        }
    }

    /// Set a timeout on key in seconds
    pub fn expire(&self, key: &[u8], seconds: i64) -> Result<bool> {
        if seconds <= 0 {
            return Ok(false);
        }

        let key_str = String::from_utf8_lossy(key);

        // First check if key exists
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        // Check if key exists in storage
        match self.insts[instance_id].key_exists_live(key) {
            Ok(false) => return Ok(false), // Key doesn't exist
            Err(_) => return Ok(false),    // Error means key doesn't exist
            Ok(true) => {}                 // Key exists, continue
        }

        if let Some(expiration_manager) = &self.expiration_manager {
            let expire_time = crate::expiration_manager::ExpirationManager::seconds_to_expire_time(seconds)?;
            expiration_manager.set_expiration(&key_str, expire_time);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Set a timeout on key in milliseconds
    pub fn pexpire(&self, key: &[u8], milliseconds: i64) -> Result<bool> {
        if milliseconds <= 0 {
            return Ok(false);
        }

        let key_str = String::from_utf8_lossy(key);

        // First check if key exists
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        // Check if key exists in storage
        match self.insts[instance_id].key_exists_live(key) {
            Ok(false) => return Ok(false), // Key doesn't exist
            Err(_) => return Ok(false),    // Error means key doesn't exist
            Ok(true) => {}                 // Key exists, continue
        }

        if let Some(expiration_manager) = &self.expiration_manager {
            let expire_time = crate::expiration_manager::ExpirationManager::milliseconds_to_expire_time(milliseconds)?;
            expiration_manager.set_expiration(&key_str, expire_time);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Set the expiration for a key as a UNIX timestamp in seconds
    pub fn expireat(&self, key: &[u8], timestamp: i64) -> Result<bool> {
        let key_str = String::from_utf8_lossy(key);

        // First check if key exists
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        // Check if key exists in storage
        match self.insts[instance_id].key_exists_live(key) {
            Ok(false) => return Ok(false), // Key doesn't exist
            Err(_) => return Ok(false),    // Error means key doesn't exist
            Ok(true) => {}                 // Key exists, continue
        }

        if let Some(expiration_manager) = &self.expiration_manager {
            let expire_time = crate::expiration_manager::ExpirationManager::unix_seconds_to_expire_time(timestamp)?;
            expiration_manager.set_expiration(&key_str, expire_time);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Set the expiration for a key as a UNIX timestamp in milliseconds
    pub fn pexpireat(&self, key: &[u8], timestamp: i64) -> Result<bool> {
        let key_str = String::from_utf8_lossy(key);

        // First check if key exists
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        // Check if key exists in storage
        match self.insts[instance_id].key_exists_live(key) {
            Ok(false) => return Ok(false), // Key doesn't exist
            Err(_) => return Ok(false),    // Error means key doesn't exist
            Ok(true) => {}                 // Key exists, continue
        }

        if let Some(expiration_manager) = &self.expiration_manager {
            let expire_time = crate::expiration_manager::ExpirationManager::unix_milliseconds_to_expire_time(timestamp)?;
            expiration_manager.set_expiration(&key_str, expire_time);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Remove the expiration from a key
    pub fn persist(&self, key: &[u8]) -> Result<bool> {
        let key_str = String::from_utf8_lossy(key);

        // First check if key exists
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        // Check if key exists in storage
        match self.insts[instance_id].key_exists_live(key) {
            Ok(false) => return Ok(false), // Key doesn't exist
            Err(_) => return Ok(false),    // Error means key doesn't exist
            Ok(true) => {}                 // Key exists, continue
        }

        if let Some(expiration_manager) = &self.expiration_manager {
            Ok(expiration_manager.remove_expiration(&key_str))
        } else {
            Ok(false)
        }
    }

    /// Check if one or more keys exist
    pub fn exists(&self, keys: &[Vec<u8>]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        let mut count = 0;
        for key in keys {
            let slot_id = key_to_slot_id(key);
            let instance_id = self.slot_indexer.get_instance_id(slot_id);

            match self.insts[instance_id].key_exists_live(key) {
                Ok(true) => count += 1,
                Ok(false) => {} // Key doesn't exist
                Err(e) => {
                    // Log error but continue counting other keys
                    log::warn!("Error checking key existence: {:?}", e);
                }
            }
        }

        Ok(count)
    }

    /// Get the data type of a key
    pub fn key_type(&self, key: &[u8]) -> Result<String> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);

        match self.insts[instance_id].get_key_type(key) {
            Ok(data_type) => Ok(crate::base_value_format::data_type_to_string(data_type).to_string()),
            Err(_) => Ok("none".to_string()), // Key doesn't exist
        }
    }

    /// Delete one or more keys
    pub fn del(&self, keys: &[Vec<u8>]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        let mut deleted_count = 0;
        for key in keys {
            let slot_id = key_to_slot_id(key);
            let instance_id = self.slot_indexer.get_instance_id(slot_id);

            // Try to delete the key - this will handle all data types
            match self.insts[instance_id].del_key(key) {
                Ok(true) => deleted_count += 1,
                Ok(false) => {} // Key doesn't exist
                Err(e) => {
                    // Log error but continue deleting other keys
                    log::warn!("Error deleting key: {:?}", e);
                }
            }
        }

        Ok(deleted_count)
    }

    /// Find all keys matching the given pattern
    pub fn keys(&self, pattern: &str) -> Result<Vec<String>> {
        let mut all_keys = Vec::new();

        for inst in &self.insts {
            // Continue with other instances on error
            if let Ok(keys) = inst.scan_keys(pattern) {
                all_keys.extend(keys);
            }
        }

        Ok(all_keys)
    }

    /// Remove all keys from the current database
    pub fn flushdb(&self) -> Result<()> {
        for inst in &self.insts {
            inst.flush_db()?;
        }
        Ok(())
    }

    /// Remove all keys from all databases
    pub fn flushall(&self) -> Result<()> {
        for inst in &self.insts {
            inst.flush_all()?;
        }
        Ok(())
    }

    /// Return a random key from the database
    pub fn randomkey(&self) -> Result<Option<String>> {
        // Try each instance until we find a key
        for inst in &self.insts {
            match inst.random_key() {
                Ok(Some(key)) => return Ok(Some(key)),
                Ok(None) | Err(_) => continue,
            }
        }
        Ok(None) // No keys found in any instance
    }

    // // Atomically sets key to value and returns the old value stored at key
    // // Returns an error when key exists but does not hold a string value.
    // pub fn get_set(&self, key: &[u8], value: &[u8], old_value: &mut String) -> Status {
    //     // Implementation of get and set key-value logic
    //     Ok(())
    // }

    // // Sets or clears the bit at offset in the string value stored at key
    // pub fn set_bit(&self, key: &[u8], offset: i64, value: i32, ret: &mut i32) -> Status {
    //     // Implementation of set bit logic
    //     Ok(())
    // }

    // // Returns the bit value at offset in the string value stored at key
    // pub fn get_bit(&self, key: &[u8], offset: i64, ret: &mut i32) -> Status {
    //     // Implementation of get bit logic
    //     Ok(())
    // }

    // // Sets the given keys to their respective values
    // // MSET replaces existing values with new values
    // pub fn mset(&self, kvs: &[KeyValue]) -> Status {
    //     // Implementation of batch set key-value logic
    //     Ok(())
    // }

    // // Returns the values of all specified keys. For every key
    // // that does not hold a string value or does not exist, the
    // // special value nil is returned
    // pub fn mget(&self, keys: &[String], vss: &mut Vec<ValueStatus>) -> Status {
    //     // Implementation of batch get key-value logic
    //     Ok(())
    // }

    // // Returns the values of all specified keyswithTTL. For every key
    // // that does not hold a string value or does not exist, the
    // // special value nil is returned
    // pub fn mget_with_ttl(&self, keys: &[String], vss: &mut Vec<ValueStatus>) -> Status {
    //     // Implementation of batch get key-value with TTL logic
    //     Ok(())
    // }

    // // Set key to hold string value if key does not exist
    // // return 1 if the key was set
    // // return 0 if the key was not set
    // pub fn setnx(&self, key: &[u8], value: &[u8], ret: &mut i32, ttl: i64) -> Status {
    //     // Implementation of set key-value if not exists logic
    //     Ok(())
    // }

    // // Sets the given keys to their respective values.
    // // MSETNX will not perform any operation at all even
    // // if just a single key already exists.
    // pub fn msetnx(&self, kvs: &[KeyValue], ret: &mut i32) -> Status {
    //     // Implementation of batch set key-value if not exists logic
    //     Ok(())
    // }

    // // Hashes Commands Implementation

    // // Sets field in the hash stored at key to value. If key does not exist, a new
    // // key holding a hash is created. If field already exists in the hash, it is
    // // overwritten.
    // pub fn hset(&self, key: &[u8], field: &[u8], value: &[u8], res: &mut i32) -> Status {
    //     // Implementation of set hash field logic
    //     Ok(())
    // }

    // // Returns the value associated with field in the hash stored at key.
    // // the value associated with field, or nil when field is not present in the
    // // hash or key does not exist.
    // pub fn hget(&self, key: &[u8], field: &[u8], value: &mut String) -> Status {
    //     // Implementation of get hash field logic
    //     Ok(())
    // }

    // // Sets the specified fields to their respective values in the hash stored at
    // // key. This command overwrites any specified fields already existing in the
    // // hash. If key does not exist, a new key holding a hash is created.
    // pub fn hmset(&self, key: &[u8], fvs: &[FieldValue]) -> Status {
    //     // Implementation of batch set hash fields logic
    //     Ok(())
    // }

    // // Returns the values associated with the specified fields in the hash stored
    // // at key.
    // // For every field that does not exist in the hash, a nil value is returned.
    // // Because a non-existing keys are treated as empty hashes, running HMGET
    // // against a non-existing key will return a list of nil values.
    // pub fn hmget(&self, key: &[u8], fields: &[String], vss: &mut Vec<ValueStatus>) -> Status {
    //     // Implementation of batch get hash fields logic
    //     Ok(())
    // }

    // // Returns all fields and values of the hash stored at key. In the returned
    // // value, every field name is followed by its value, so the length of the
    // // reply is twice the size of the hash.
    // pub fn hgetall(&self, key: &[u8], fvs: &mut Vec<FieldValue>) -> Status {
    //     // Implementation of get all hash fields and values logic
    //     Ok(())
    // }

    // pub fn hgetall_with_ttl(&self, key: &[u8], fvs: &mut Vec<FieldValue>, ttl: &mut i64) -> Status {
    //     // Implementation of get all hash fields and values with TTL logic
    //     Ok(())
    // }

    // // Sets Commands Implementation

    // Add the specified members to the set stored at key. Specified members that
    // are already a member of this set are ignored. If key does not exist, a new
    // set is created before adding the specified members.
    pub fn sadd(&self, key: &[u8], members: &[&[u8]]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sadd(key, members)
    }

    // Returns all the members of the set value stored at key.
    pub fn smembers(&self, key: &[u8]) -> Result<Vec<String>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].smembers(key)
    }

    // Returns the set cardinality (number of elements) of the set stored at key.
    pub fn scard(&self, key: &[u8]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].scard(key)
    }

    // Check if member is a member of the set stored at key.
    pub fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sismember(key, member)
    }

    // Get random members from a set.
    pub fn srandmember(&self, key: &[u8], count: Option<i32>) -> Result<Vec<String>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].srandmember(key, count)
    }

    // Remove one or more members from a set.
    pub fn srem(&self, key: &[u8], members: &[&[u8]]) -> Result<i32> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].srem(key, members)
    }

    // Remove and return random members from a set.
    pub fn spop(&self, key: &[u8], count: Option<i32>) -> Result<Vec<String>> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].spop(key, count)
    }

    // Move member from source set to destination set.
    pub fn smove(&self, source: &[u8], destination: &[u8], member: &[u8]) -> Result<bool> {
        // For multi-key operations, we need to ensure both keys are on the same instance
        let source_slot_id = key_to_slot_id(source);
        let dest_slot_id = key_to_slot_id(destination);

        if source_slot_id != dest_slot_id {
            return InvalidArgumentSnafu {
                message: "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
            }
            .fail();
        }

        let instance_id = self.slot_indexer.get_instance_id(source_slot_id);
        self.insts[instance_id].smove(source, destination, member)
    }

    // Returns the members of the set resulting from the difference between the
    // first set and all the successive sets.
    pub fn sdiff(&self, keys: &[&[u8]]) -> Result<Vec<String>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let slot_id = key_to_slot_id(keys[0]);
        for &k in keys {
            if key_to_slot_id(k) != slot_id {
                return InvalidArgumentSnafu {
                    message: "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                }
                .fail();
            }
        }
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sdiff(keys)
    }

    // Returns the members of the set resulting from the intersection of all the given sets.
    pub fn sinter(&self, keys: &[&[u8]]) -> Result<Vec<String>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let slot_id = key_to_slot_id(keys[0]);
        for &k in keys {
            if key_to_slot_id(k) != slot_id {
                return InvalidArgumentSnafu {
                    message: "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                }
                .fail();
            }
        }
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sinter(keys)
    }

    // Returns the members of the set resulting from the union of all the given sets.
    pub fn sunion(&self, keys: &[&[u8]]) -> Result<Vec<String>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let slot_id = key_to_slot_id(keys[0]);
        for &k in keys {
            if key_to_slot_id(k) != slot_id {
                return InvalidArgumentSnafu {
                    message: "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                }
                .fail();
            }
        }
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sunion(keys)
    }

    // Store the difference between the first set and all the successive sets into a destination key.
    pub fn sdiffstore(&self, destination: &[u8], keys: &[&[u8]]) -> Result<i32> {
        if keys.is_empty() {
            return Ok(0);
        }

        let slot_id = key_to_slot_id(destination);
        for &k in keys {
            if key_to_slot_id(k) != slot_id {
                return InvalidArgumentSnafu {
                    message: "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                }
                .fail();
            }
        }
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sdiffstore(destination, keys)
    }

    // Store the intersection of all the given sets into a destination key.
    pub fn sinterstore(&self, destination: &[u8], keys: &[&[u8]]) -> Result<i32> {
        if keys.is_empty() {
            return Ok(0);
        }

        let slot_id = key_to_slot_id(destination);
        for &k in keys {
            if key_to_slot_id(k) != slot_id {
                return InvalidArgumentSnafu {
                    message: "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                }
                .fail();
            }
        }
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sinterstore(destination, keys)
    }

    // Store the union of all the given sets into a destination key.
    pub fn sunionstore(&self, destination: &[u8], keys: &[&[u8]]) -> Result<i32> {
        if keys.is_empty() {
            return Ok(0);
        }

        let slot_id = key_to_slot_id(destination);
        for &k in keys {
            if key_to_slot_id(k) != slot_id {
                return InvalidArgumentSnafu {
                    message: "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                }
                .fail();
            }
        }
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sunionstore(destination, keys)
    }

    // Scan set members with cursor-based iteration.
    pub fn sscan(
        &self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<(u64, Vec<String>)> {
        let slot_id = key_to_slot_id(key);
        let instance_id = self.slot_indexer.get_instance_id(slot_id);
        self.insts[instance_id].sscan(key, cursor, pattern, count)
    }

    // // Lists Commands Implementation

    // // Insert all the specified values at the head of the list stored at key. If
    // // key does not exist, it is created as empty list before performing the push
    // // operations.
    // pub fn lpush(&self, key: &[u8], values: &[String], ret: &mut u64) -> Status {
    //     // Implementation of left push list logic
    //     Ok(())
    // }

    // // Insert all the specified values at the tail of the list stored at key. If
    // // key does not exist, it is created as empty list before performing the push
    // // operation.
    // pub fn rpush(&self, key: &[u8], values: &[String], ret: &mut u64) -> Status {
    //     // Implementation of right push list logic
    //     Ok(())
    // }

    // // Returns the specified elements of the list stored at key. The offsets start
    // // and stop are zero-based indexes, with 0 being the first element of the list
    // // (the head of the list), 1 being the next element and so on.
    // pub fn lrange(&self, key: &[u8], start: i64, stop: i64, ret: &mut Vec<String>) -> Status {
    //     // Implementation of get list range logic
    //     Ok(())
    // }

    // // Zsets Commands Implementation

    // // Adds all the specified members with the specified scores to the sorted set
    // // stored at key. It is possible to specify multiple score / member pairs. If
    // // a specified member is already a member of the sorted set, the score is
    // // updated and the element reinserted at the right position to ensure the
    // // correct ordering.
    // pub fn zadd(&self, key: &[u8], score_members: &[ScoreMember], ret: &mut i32) -> Status {
    //     // Implementation of add sorted set member logic
    //     Ok(())
    // }

    // // Returns the sorted set cardinality (number of elements) of the sorted set
    // // stored at key.
    // pub fn zcard(&self, key: &[u8], ret: &mut i32) -> Status {
    //     // Implementation of get sorted set cardinality logic
    //     Ok(())
    // }

    // // Keys Commands Implementation

    // // Set a timeout on key
    // // return -1 operation exception errors happen in database
    // // return >=0 success
    // pub fn expire(&self, key: &[u8], ttl: i64) -> i32 {
    //     // Implementation of set key expiration time logic
    //     0
    // }

    // // Removes the specified keys
    // // return -1 operation exception errors happen in database
    // // return >=0 the number of keys that were removed
    // pub fn del(&self, keys: &[String]) -> i64 {
    //     // Implementation of delete key logic
    //     0
    // }

    // // Admin Commands Implementation

    // pub fn compact(&self, type_: &DataType, sync: bool) -> Status {
    //     // Implementation of compaction logic
    //     Ok(())
    // }

    // pub fn compact_range(&self, type_: &DataType, start: &str, end: &str, sync: bool) -> Status {
    //     // Implementation of range compaction logic
    //     Ok(())
    // }

    // pub fn do_compact_range(&self, type_: &DataType, start: &str, end: &str) -> Status {
    //     // Implementation of execute range compaction logic
    //     Ok(())
    // }

    // pub fn do_compact_specific_key(&self, type_: &DataType, key: &str) -> Status {
    //     // Implementation of execute specific key compaction logic
    //     Ok(())
    // }

    // pub fn set_max_cache_statistic_keys(&self, max_cache_statistic_keys: u32) -> Status {
    //     // Implementation of set maximum cache statistic keys logic
    //     Ok(())
    // }

    // pub fn set_small_compaction_threshold(&self, small_compaction_threshold: u32) -> Status {
    //     // Implementation of set small compaction threshold logic
    //     Ok(())
    // }

    // pub fn set_small_compaction_duration_threshold(&self, small_compaction_duration_threshold: u32) -> Status {
    //     // Implementation of set small compaction duration threshold logic
    //     Ok(())
    // }

    // // HyperLogLog Implementation

    // // Adds all the element arguments to the HyperLogLog data structure stored
    // // at the variable name specified as first argument.
    // pub fn pf_add(&self, key: &[u8], values: &[String], update: &mut bool) -> Status {
    //     // Implementation of add HyperLogLog logic
    //     Ok(())
    // }

    // // When called with a single key, returns the approximated cardinality
    // // computed by the HyperLogLog data structure stored at the specified
    // // variable, which is 0 if the variable does not exist.
    // pub fn pf_count(&self, keys: &[String], result: &mut i64) -> Status {
    //     // Implementation of calculate HyperLogLog cardinality logic
    //     Ok(())
    // }

    // // Merge multiple HyperLogLog values into an unique value that will
    // // approximate the cardinality of the union of the observed Sets of the source
    // // HyperLogLog structures.
    // pub fn pf_merge(&self, keys: &[String], value_to_dest: &mut String) -> Status {
    //     // Implementation of merge HyperLogLog logic
    //     Ok(())
    // }

    // Other helper methods

    // pub fn create_checkpoint(&self, checkpoint_path: &str) -> Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Status> + Send>>> {
    //     // Implementation of create checkpoint logic
    //     Vec::new()
    // }

    // pub fn create_checkpoint_internal(&self, checkpoint_path: &str, db_index: i32) -> Status {
    //     // Implementation of internal create checkpoint logic
    //     Ok(())
    // }

    // pub fn load_checkpoint(&self, checkpoint_path: &str, db_path: &str) -> Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Status> + Send>>> {
    //     // Implementation of load checkpoint logic
    //     Vec::new()
    // }

    // pub fn load_checkpoint_internal(&self, dump_path: &str, db_path: &str, index: i32) -> Status {
    //     // Implementation of internal load checkpoint logic
    //     Ok(())
    // }

    // pub fn on_binlog_write(&self, log: crate::storage::Binlog, log_idx: crate::storage::LogIndex) -> Status {
    //     // Implementation of write binlog logic
    //     Ok(())
    // }

    // pub fn get_db_by_index(&self, index: i32) -> Option<&rocksdb::DB> {
    //     // Implementation of get DB by index logic
    //     None
    // }

    // pub fn set_options(&self, option_type: &crate::storage::OptionType, options: &HashMap<String, String>) -> Status {
    //     // Implementation of set options logic
    //     Ok(())
    // }

    // pub fn get_rocksdb_info(&self) -> String {
    //     // Implementation of get RocksDB information logic
    //     String::new()
    // }
}
