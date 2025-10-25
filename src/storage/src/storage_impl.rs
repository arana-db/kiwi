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

use crate::error::{Error, Result};
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

        // Sort keys to prevent deadlock
        let mut sorted_keys: Vec<&Vec<u8>> = kvs.iter().map(|(key, _)| key).collect();
        sorted_keys.sort();

        // Acquire locks on all keys to ensure atomicity
        let _locks: Vec<_> = sorted_keys
            .iter()
            .map(|key| {
                let key_str = String::from_utf8_lossy(key).to_string();
                self.lock_mgr.lock(&key_str)
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
}
