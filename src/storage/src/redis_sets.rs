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

//! Redis sets operations implementation
//! This module provides set operations for Redis storage

use rocksdb::{Direction, IteratorMode, ReadOptions, WriteBatch, WriteOptions};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::format::{DataType, InternalValue, ParsedInternalValue};
use crate::lock::ScopeRecordLock;
use crate::redis::Redis;
use crate::types::KeyVersion;
use crate::{Result, StorageError};

impl Redis {
    /// Scan sets key number
    pub fn scan_sets_key_num(&self, key_info: &mut crate::types::KeyInfo) -> Result<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        let mut keys = 0;
        let mut expires = 0;
        let mut ttl_sum = 0;
        let mut invalid_keys = 0;

        // Get current time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

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
            IteratorMode::Start,
        );

        for (_, value) in iter {
            // Parse the meta value
            let parsed_value = ParsedInternalValue::new(&value);

            // Check if it's a sets type
            if parsed_value.data_type() != DataType::Sets {
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

    /// Add one or more members to a set
    pub fn sadd(&self, key: &[u8], members: &[&[u8]], ret: &mut i32) -> Result<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        // Filter duplicate members
        let mut unique = HashSet::new();
        let mut filtered_members = Vec::new();

        for &member in members {
            let member_str = String::from_utf8_lossy(member).to_string();
            if !unique.contains(&member_str) {
                unique.insert(member_str);
                filtered_members.push(member);
            }
        }

        if filtered_members.is_empty() {
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
                if parsed_meta.data_type() != DataType::Sets {
                    if parsed_meta.is_expired(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    ) {
                        // Treat as not found if expired
                        let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
                        sets_meta.set_size(filtered_members.len() as u64);
                        version = sets_meta.update_version();

                        batch.put(key, &sets_meta.encode());

                        // Add all members
                        for &member in &filtered_members {
                            let member_key = self.encode_sets_member_key(key, version, member);
                            let empty_value = InternalValue::new(DataType::None, &[]).encode();
                            batch.put_cf(
                                self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                                &member_key,
                                &empty_value,
                            );
                        }

                        *ret = filtered_members.len() as i32;
                    } else {
                        return Err(StorageError::InvalidFormat(format!(
                            "Wrong type for key: {}",
                            String::from_utf8_lossy(key)
                        )));
                    }
                } else if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) || parsed_meta.size() == 0
                {
                    // Initialize meta value if expired or empty
                    version = parsed_meta.update_version();
                    parsed_meta.set_size(filtered_members.len() as u64);

                    batch.put(key, &parsed_meta.encode());

                    // Add all members
                    for &member in &filtered_members {
                        let member_key = self.encode_sets_member_key(key, version, member);
                        let empty_value = InternalValue::new(DataType::None, &[]).encode();
                        batch.put_cf(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            &member_key,
                            &empty_value,
                        );
                    }

                    *ret = filtered_members.len() as i32;
                } else {
                    // Add new members to existing set
                    let mut count = 0;
                    version = parsed_meta.version();

                    for &member in &filtered_members {
                        let member_key = self.encode_sets_member_key(key, version, member);

                        // Check if member already exists
                        match db.get_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            &member_key,
                            &read_options,
                        )? {
                            Some(_) => {
                                // Member already exists, skip
                            }
                            None => {
                                // Add new member
                                count += 1;
                                let empty_value = InternalValue::new(DataType::None, &[]).encode();
                                batch.put_cf(
                                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                                    &member_key,
                                    &empty_value,
                                );
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
            }
            None => {
                // Create new set
                let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
                sets_meta.set_size(filtered_members.len() as u64);
                version = sets_meta.update_version();

                batch.put(key, &sets_meta.encode());

                // Add all members
                for &member in &filtered_members {
                    let member_key = self.encode_sets_member_key(key, version, member);
                    let empty_value = InternalValue::new(DataType::None, &[]).encode();
                    batch.put_cf(
                        self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                        &member_key,
                        &empty_value,
                    );
                }

                *ret = filtered_members.len() as i32;
            }
        }

        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;

        Ok(())
    }

    /// Get the number of members in a set
    pub fn scard(&self, key: &[u8], ret: &mut i32) -> Result<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        *ret = 0;

        // Try to get the meta value
        let read_options = ReadOptions::default();
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                // Parse the meta value
                let parsed_meta = ParsedInternalValue::new(&meta_value);

                // Check if it's the right type
                if parsed_meta.data_type() != DataType::Sets {
                    return Err(StorageError::InvalidFormat(format!(
                        "Wrong type for key: {}",
                        String::from_utf8_lossy(key)
                    )));
                }

                // Check if expired
                if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) {
                    return Err(StorageError::KeyNotFound("Stale".to_string()));
                }

                // Get the count
                *ret = parsed_meta.size() as i32;

                if *ret == 0 {
                    return Err(StorageError::KeyNotFound("Deleted".to_string()));
                }
            }
            None => {
                // Key not found
            }
        }

        Ok(())
    }

    /// Returns the members of the set resulting from the difference between the first set and all the successive sets
    pub fn sdiff(&self, keys: &[&[u8]], members: &mut Vec<String>) -> Result<()> {
        if keys.is_empty() {
            return Err(StorageError::InvalidFormat(
                "SDiff invalid parameter, no keys".to_string(),
            ));
        }

        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        // Create snapshot and read options
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        // Store valid sets (key, version)
        let mut valid_sets = Vec::new();

        // Process all keys except the first one
        for &key in &keys[1..] {
            match db.get_opt(key, &read_options)? {
                Some(meta_value) => {
                    let parsed_meta = ParsedInternalValue::new(&meta_value);

                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::Sets {
                        return Err(StorageError::InvalidFormat(format!(
                            "Wrong type for key: {}",
                            String::from_utf8_lossy(key)
                        )));
                    }

                    // Skip if expired
                    if parsed_meta.is_expired(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    ) {
                        continue;
                    }

                    // Add to valid sets
                    valid_sets.push((key, parsed_meta.version()));
                }
                None => {
                    // Key not found, skip
                }
            }
        }

        // Process the first key
        match db.get_opt(keys[0], &read_options)? {
            Some(meta_value) => {
                let parsed_meta = ParsedInternalValue::new(&meta_value);

                // Check if it's the right type
                if parsed_meta.data_type() != DataType::Sets {
                    return Err(StorageError::InvalidFormat(format!(
                        "Wrong type for key: {}",
                        String::from_utf8_lossy(keys[0])
                    )));
                }

                // Skip if expired
                if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) {
                    return Ok(());
                }

                // Get the version
                let version = parsed_meta.version();

                // Create prefix for iteration
                let prefix = self.encode_sets_member_prefix(keys[0], version);

                // Iterate through all members of the first set
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                    read_options.clone(),
                    IteratorMode::From(&prefix, Direction::Forward),
                );

                for (key_bytes, _) in iter {
                    // Check if key starts with prefix
                    if !key_bytes.starts_with(&prefix) {
                        break;
                    }

                    // Extract member from key
                    let member = self.decode_sets_member_from_key(&key_bytes);

                    // Check if member exists in any other set
                    let mut found = false;
                    for &(other_key, other_version) in &valid_sets {
                        let member_key =
                            self.encode_sets_member_key(other_key, other_version, &member);

                        match db.get_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            &member_key,
                            &read_options,
                        )? {
                            Some(_) => {
                                found = true;
                                break;
                            }
                            None => {
                                // Not found in this set
                            }
                        }
                    }

                    // Add to result if not found in any other set
                    if !found {
                        members.push(String::from_utf8_lossy(&member).to_string());
                    }
                }
            }
            None => {
                // First key not found, return empty result
            }
        }

        Ok(())
    }

    /// Helper method to encode sets member key
    fn encode_sets_member_key(&self, key: &[u8], version: u64, member: &[u8]) -> Vec<u8> {
        // In a real implementation, this would encode the key in the format expected by the C++ code
        // For simplicity, we'll use a basic format here
        let mut result = Vec::with_capacity(key.len() + 8 + member.len() + 2);
        result.extend_from_slice(key);
        result.push(0); // separator

        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());

        result.push(0); // separator
        result.extend_from_slice(member);

        result
    }

    /// Helper method to encode sets member prefix for iteration
    fn encode_sets_member_prefix(&self, key: &[u8], version: u64) -> Vec<u8> {
        let mut result = Vec::with_capacity(key.len() + 9);
        result.extend_from_slice(key);
        result.push(0); // separator

        // Add version (8 bytes)
        result.extend_from_slice(&version.to_be_bytes());

        result.push(0); // separator

        result
    }

    /// Helper method to decode member from sets member key
    fn decode_sets_member_from_key(&self, key: &[u8]) -> Vec<u8> {
        // Find the second separator
        let mut separator_count = 0;
        let mut pos = 0;

        for (i, &byte) in key.iter().enumerate() {
            if byte == 0 {
                separator_count += 1;
                if separator_count == 2 {
                    pos = i + 1;
                    break;
                }
            }
        }

        if pos > 0 && pos < key.len() {
            key[pos..].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Store the difference between the first set and all the successive sets into a destination key
    pub fn sdiffstore(
        &self,
        destination: &[u8],
        keys: &[&[u8]],
        value_to_dest: &mut Vec<String>,
        ret: &mut i32,
    ) -> Result<()> {
        if keys.is_empty() {
            return Err(StorageError::InvalidFormat(
                "SDiffstore invalid parameter, no keys".to_string(),
            ));
        }

        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        // Create lock for the destination key
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), destination);

        // Create snapshot and read options
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        // Store valid sets (key, version)
        let mut valid_sets = Vec::new();
        let mut members = Vec::new();

        // Process all keys except the first one
        for &key in &keys[1..] {
            match db.get_opt(key, &read_options)? {
                Some(meta_value) => {
                    let parsed_meta = ParsedInternalValue::new(&meta_value);

                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::Sets {
                        return Err(StorageError::InvalidFormat(format!(
                            "Wrong type for key: {}",
                            String::from_utf8_lossy(key)
                        )));
                    }

                    // Skip if expired
                    if parsed_meta.is_expired(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    ) {
                        continue;
                    }

                    // Add to valid sets
                    valid_sets.push(KeyVersion {
                        key: key.to_vec(),
                        version: parsed_meta.version(),
                    });
                }
                None => {
                    // Key not found, skip
                }
            }
        }

        // Process the first key
        match db.get_opt(keys[0], &read_options)? {
            Some(meta_value) => {
                let parsed_meta = ParsedInternalValue::new(&meta_value);

                // Check if it's the right type
                if parsed_meta.data_type() != DataType::Sets {
                    return Err(StorageError::InvalidFormat(format!(
                        "Wrong type for key: {}",
                        String::from_utf8_lossy(keys[0])
                    )));
                }

                // Skip if expired
                if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) {
                    // Empty result
                } else {
                    // Get the version
                    let version = parsed_meta.version();

                    // Create prefix for iteration
                    let prefix = self.encode_sets_member_prefix(keys[0], version);

                    // Iterate through all members of the first set
                    let iter = db.iterator_cf_opt(
                        self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                        read_options.clone(),
                        IteratorMode::From(&prefix, Direction::Forward),
                    );

                    for (key_bytes, _) in iter {
                        // Check if key starts with prefix
                        if !key_bytes.starts_with(&prefix) {
                            break;
                        }

                        // Extract member from key
                        let member = self.decode_sets_member_from_key(&key_bytes);

                        // Check if member exists in any other set
                        let mut found = false;
                        for key_version in &valid_sets {
                            let member_key = self.encode_sets_member_key(
                                &key_version.key,
                                key_version.version,
                                &member,
                            );

                            match db.get_cf_opt(
                                self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                                &member_key,
                                &read_options,
                            )? {
                                Some(_) => {
                                    found = true;
                                    break;
                                }
                                None => {
                                    // Not found in this set
                                }
                            }
                        }

                        // Add to result if not found in any other set
                        if !found {
                            let member_str = String::from_utf8_lossy(&member).to_string();
                            members.push(member_str.clone());
                            value_to_dest.push(member_str);
                        }
                    }
                }
            }
            None => {
                // First key not found, empty result
            }
        }

        // Store the result in the destination key
        let mut batch = WriteBatch::default();
        let mut version = 0;
        let mut statistic = 0;

        // Check if destination key exists
        match db.get_opt(destination, &read_options)? {
            Some(meta_value) => {
                if ParsedInternalValue::new(&meta_value).data_type() == DataType::Sets {
                    let mut parsed_meta = ParsedInternalValue::new(&meta_value);
                    statistic = parsed_meta.size();
                    version = parsed_meta.update_version();
                    parsed_meta.set_size(members.len() as u64);
                    batch.put(destination, &parsed_meta.encode());
                } else {
                    // Create new set
                    let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
                    sets_meta.set_size(members.len() as u64);
                    version = sets_meta.update_version();
                    batch.put(destination, &sets_meta.encode());
                }
            }
            None => {
                // Create new set
                let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
                sets_meta.set_size(members.len() as u64);
                version = sets_meta.update_version();
                batch.put(destination, &sets_meta.encode());
            }
        }

        // Add all members to the destination set
        for member_str in &members {
            let member_key =
                self.encode_sets_member_key(destination, version, member_str.as_bytes());
            let empty_value = InternalValue::new(DataType::None, &[]).encode();
            batch.put_cf(
                self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                &member_key,
                &empty_value,
            );
        }

        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;

        // Update statistics
        self.update_specific_key_statistics(
            DataType::Sets,
            &String::from_utf8_lossy(destination).to_string(),
            statistic,
        )?;

        // Set return value
        *ret = members.len() as i32;

        Ok(())
    }

    /// Returns the members of the set resulting from the intersection of all the given sets
    pub fn sinter(&self, keys: &[&[u8]], members: &mut Vec<String>) -> Result<()> {
        if keys.is_empty() {
            return Err(StorageError::InvalidFormat(
                "SInter invalid parameter, no keys".to_string(),
            ));
        }

        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        // Create snapshot and read options
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        // Store valid sets (key, version)
        let mut valid_sets = Vec::new();

        // Process all keys except the first one
        for &key in &keys[1..] {
            match db.get_opt(key, &read_options)? {
                Some(meta_value) => {
                    let parsed_meta = ParsedInternalValue::new(&meta_value);

                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::Sets {
                        return Err(StorageError::InvalidFormat(format!(
                            "Wrong type for key: {}",
                            String::from_utf8_lossy(key)
                        )));
                    }

                    // Return empty result if any key is expired or empty
                    if parsed_meta.is_expired(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    ) {
                        return Ok(());
                    }

                    // Add to valid sets
                    valid_sets.push(KeyVersion {
                        key: key.to_vec(),
                        version: parsed_meta.version(),
                    });
                }
                None => {
                    // If any key doesn't exist, return empty result
                    return Ok(());
                }
            }
        }

        // Process the first key
        match db.get_opt(keys[0], &read_options)? {
            Some(meta_value) => {
                let parsed_meta = ParsedInternalValue::new(&meta_value);

                // Check if it's the right type
                if parsed_meta.data_type() != DataType::Sets {
                    return Err(StorageError::InvalidFormat(format!(
                        "Wrong type for key: {}",
                        String::from_utf8_lossy(keys[0])
                    )));
                }

                // Return empty result if the first key is expired
                if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) {
                    return Ok(());
                }

                // Get the version
                let version = parsed_meta.version();

                // Create prefix for iteration
                let prefix = self.encode_sets_member_prefix(keys[0], version);

                // Iterate through all members of the first set
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                    read_options.clone(),
                    IteratorMode::From(&prefix, Direction::Forward),
                );

                for (key_bytes, _) in iter {
                    // Check if key starts with prefix
                    if !key_bytes.starts_with(&prefix) {
                        break;
                    }

                    // Extract member from key
                    let member = self.decode_sets_member_from_key(&key_bytes);

                    // Check if member exists in all other sets
                    let mut reliable = true;
                    for key_version in &valid_sets {
                        let member_key = self.encode_sets_member_key(
                            &key_version.key,
                            key_version.version,
                            &member,
                        );

                        match db.get_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            &member_key,
                            &read_options,
                        )? {
                            Some(_) => {
                                // Member exists in this set, continue checking
                            }
                            None => {
                                // Member doesn't exist in this set, not in intersection
                                reliable = false;
                                break;
                            }
                        }
                    }

                    // Add to result if found in all sets
                    if reliable {
                        members.push(String::from_utf8_lossy(&member).to_string());
                    }
                }
            }
            None => {
                // First key not found, return empty result
            }
        }

        Ok(())
    }

    /// Store the intersection of the sets in a new set at destination
    pub fn sinterstore(
        &self,
        destination: &[u8],
        keys: &[&[u8]],
        value_to_dest: &mut Vec<String>,
        ret: &mut i32,
    ) -> Result<()> {
        if keys.is_empty() {
            return Err(StorageError::InvalidFormat(
                "SInterstore invalid parameter, no keys".to_string(),
            ));
        }

        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        // Create lock for the destination key
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), destination);

        // Create snapshot and read options
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        // Store valid sets (key, version)
        let mut valid_sets = Vec::new();
        let mut have_invalid_sets = false;
        let mut members = Vec::new();

        // Process all keys except the first one
        for &key in &keys[1..] {
            match db.get_opt(key, &read_options)? {
                Some(meta_value) => {
                    let parsed_meta = ParsedInternalValue::new(&meta_value);

                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::Sets {
                        return Err(StorageError::InvalidFormat(format!(
                            "Wrong type for key: {}",
                            String::from_utf8_lossy(key)
                        )));
                    }

                    // If any key is expired or empty, we have invalid sets
                    if parsed_meta.is_expired(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    ) {
                        have_invalid_sets = true;
                        break;
                    }

                    // Add to valid sets
                    valid_sets.push(KeyVersion {
                        key: key.to_vec(),
                        version: parsed_meta.version(),
                    });
                }
                None => {
                    // If any key doesn't exist, we have invalid sets
                    have_invalid_sets = true;
                    break;
                }
            }
        }

        // If we don't have invalid sets, process the first key
        if !have_invalid_sets {
            match db.get_opt(keys[0], &read_options)? {
                Some(meta_value) => {
                    let parsed_meta = ParsedInternalValue::new(&meta_value);

                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::Sets {
                        return Err(StorageError::InvalidFormat(format!(
                            "Wrong type for key: {}",
                            String::from_utf8_lossy(keys[0])
                        )));
                    }

                    // If the first key is expired, we have invalid sets
                    if parsed_meta.is_expired(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    ) {
                        have_invalid_sets = true;
                    } else {
                        // Get the version
                        let version = parsed_meta.version();

                        // Create prefix for iteration
                        let prefix = self.encode_sets_member_prefix(keys[0], version);

                        // Iterate through all members of the first set
                        let iter = db.iterator_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            read_options.clone(),
                            IteratorMode::From(&prefix, Direction::Forward),
                        );

                        for (key_bytes, _) in iter {
                            // Check if key starts with prefix
                            if !key_bytes.starts_with(&prefix) {
                                break;
                            }

                            // Extract member from key
                            let member = self.decode_sets_member_from_key(&key_bytes);

                            // Check if member exists in all other sets
                            let mut reliable = true;
                            for key_version in &valid_sets {
                                let member_key = self.encode_sets_member_key(
                                    &key_version.key,
                                    key_version.version,
                                    &member,
                                );

                                match db.get_cf_opt(
                                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                                    &member_key,
                                    &read_options,
                                )? {
                                    Some(_) => {
                                        // Member exists in this set, continue checking
                                    }
                                    None => {
                                        // Member doesn't exist in this set, not in intersection
                                        reliable = false;
                                        break;
                                    }
                                }
                            }

                            // Add to result if found in all sets
                            if reliable {
                                let member_str = String::from_utf8_lossy(&member).to_string();
                                members.push(member_str.clone());
                                value_to_dest.push(member_str);
                            }
                        }
                    }
                }
                None => {
                    // First key not found, empty result
                }
            }
        }

        // Store the result in the destination key
        let mut batch = WriteBatch::default();
        let mut version = 0;
        let mut statistic = 0;

        // Check if destination key exists
        match db.get_opt(destination, &read_options)? {
            Some(meta_value) => {
                if ParsedInternalValue::new(&meta_value).data_type() == DataType::Sets {
                    let mut parsed_meta = ParsedInternalValue::new(&meta_value);
                    statistic = parsed_meta.size();
                    version = parsed_meta.update_version();
                    parsed_meta.set_size(members.len() as u64);
                    batch.put(destination, &parsed_meta.encode());
                } else {
                    // Create new set
                    let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
                    sets_meta.set_size(members.len() as u64);
                    version = sets_meta.update_version();
                    batch.put(destination, &sets_meta.encode());
                }
            }
            None => {
                // Create new set
                let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
                sets_meta.set_size(members.len() as u64);
                version = sets_meta.update_version();
                batch.put(destination, &sets_meta.encode());
            }
        }

        // Add all members to the destination set
        for member_str in &members {
            let member_key =
                self.encode_sets_member_key(destination, version, member_str.as_bytes());
            let empty_value = InternalValue::new(DataType::None, &[]).encode();
            batch.put_cf(
                self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                &member_key,
                &empty_value,
            );
        }

        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;

        // Update statistics
        self.update_specific_key_statistics(
            DataType::Sets,
            &String::from_utf8_lossy(destination).to_string(),
            statistic,
        )?;

        // Set return value
        *ret = members.len() as i32;

        Ok(())
    }

    /// Check if member is a member of the set stored at key
    pub fn sismember(&self, key: &[u8], member: &[u8], ret: &mut i32) -> Result<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        *ret = 0;

        // Create read options
        let read_options = ReadOptions::default();

        // Get the meta value
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                let parsed_meta = ParsedInternalValue::new(&meta_value);

                // Check if it's the right type
                if parsed_meta.data_type() != DataType::Sets {
                    return Err(StorageError::InvalidFormat(format!(
                        "Wrong type for key: {}",
                        String::from_utf8_lossy(key)
                    )));
                }

                // Check if expired
                if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) {
                    return Ok(());
                }

                // Get the version
                let version = parsed_meta.version();

                // Create member key
                let member_key = self.encode_sets_member_key(key, version, member);

                // Check if member exists
                match db.get_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                    &member_key,
                    &read_options,
                )? {
                    Some(_) => {
                        *ret = 1;
                    }
                    None => {
                        *ret = 0;
                    }
                }
            }
            None => {
                // Key not found
                *ret = 0;
            }
        }

        Ok(())
    }

    /// Get all the members in a set
    pub fn smembers(&self, key: &[u8], members: &mut Vec<String>) -> Result<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        // Create read options
        let read_options = ReadOptions::default();

        // Get the meta value
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                let parsed_meta = ParsedInternalValue::new(&meta_value);

                // Check if it's the right type
                if parsed_meta.data_type() != DataType::Sets {
                    return Err(StorageError::InvalidFormat(format!(
                        "Wrong type for key: {}",
                        String::from_utf8_lossy(key)
                    )));
                }

                // Check if expired
                if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) {
                    return Ok(());
                }

                // Get the version
                let version = parsed_meta.version();

                // Create prefix for iteration
                let prefix = self.encode_sets_member_prefix(key, version);

                // Iterate through all members
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                    read_options.clone(),
                    IteratorMode::From(&prefix, Direction::Forward),
                );

                for (key_bytes, _) in iter {
                    // Check if key starts with prefix
                    if !key_bytes.starts_with(&prefix) {
                        break;
                    }

                    // Extract member from key
                    let member = self.decode_sets_member_from_key(&key_bytes);
                    members.push(String::from_utf8_lossy(&member).to_string());
                }
            }
            None => {
                // Key not found, return empty result
            }
        }

        Ok(())
    }

    /// Get all the members in a set with TTL
    pub fn smembers_with_ttl(
        &self,
        key: &[u8],
        members: &mut Vec<String>,
        ttl: &mut i64,
    ) -> Result<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        // Create read options
        let read_options = ReadOptions::default();

        // Get the meta value
        match db.get_opt(key, &read_options)? {
            Some(meta_value) => {
                let parsed_meta = ParsedInternalValue::new(&meta_value);

                // Check if it's the right type
                if parsed_meta.data_type() != DataType::Sets {
                    return Err(StorageError::InvalidFormat(format!(
                        "Wrong type for key: {}",
                        String::from_utf8_lossy(key)
                    )));
                }

                // Check if expired
                if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) {
                    return Err(StorageError::KeyNotFound("Stale".to_string()));
                }

                // Calculate TTL
                let etime = parsed_meta.etime();
                if etime == 0 {
                    *ttl = -1; // No expiration
                } else {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    *ttl = if etime > now {
                        (etime - now) as i64
                    } else {
                        -2
                    };
                }

                // Get the version
                let version = parsed_meta.version();

                // Create prefix for iteration
                let prefix = self.encode_sets_member_prefix(key, version);

                // Iterate through all members
                let iter = db.iterator_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                    read_options.clone(),
                    IteratorMode::From(&prefix, Direction::Forward),
                );

                for (key_bytes, _) in iter {
                    // Check if key starts with prefix
                    if !key_bytes.starts_with(&prefix) {
                        break;
                    }

                    // Extract member from key
                    let member = self.decode_sets_member_from_key(&key_bytes);
                    members.push(String::from_utf8_lossy(&member).to_string());
                }
            }
            None => {
                return Err(StorageError::KeyNotFound(
                    String::from_utf8_lossy(key).to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Move member from the set at source to the set at destination
    pub fn smove(
        &self,
        source: &[u8],
        destination: &[u8],
        member: &[u8],
        ret: &mut i32,
    ) -> Result<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;

        *ret = 0;

        // Create batch for atomic operations
        let mut batch = WriteBatch::default();

        // Create locks for both keys
        let keys = vec![source.to_vec(), destination.to_vec()];
        let _locks = self.lock_mgr.as_ref().multi_lock(&keys);

        let mut version = 0;
        let mut statistic = 0;

        // Check source key
        match db.get_opt(source, &self.default_read_options)? {
            Some(meta_value) => {
                let mut parsed_meta = ParsedInternalValue::new(&meta_value);

                // Check if it's the right type
                if parsed_meta.data_type() != DataType::Sets {
                    return Err(StorageError::InvalidFormat(format!(
                        "Wrong type for key: {}",
                        String::from_utf8_lossy(source)
                    )));
                }

                // Check if expired
                if parsed_meta.is_expired(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                ) {
                    return Ok(());
                }

                // Get the version
                version = parsed_meta.version();

                // Create member key
                let member_key = self.encode_sets_member_key(source, version, member);

                // Check if member exists in source
                match db.get_cf_opt(
                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                    &member_key,
                    &self.default_read_options,
                )? {
                    Some(_) => {
                        // Member exists, remove it from source
                        *ret = 1;

                        // Update source meta value
                        parsed_meta.set_size(parsed_meta.size() - 1);
                        batch.put(source, &parsed_meta.encode());

                        // Delete member from source
                        batch.delete_cf(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            &member_key,
                        );
                        statistic += 1;
                    }
                    None => {
                        // Member doesn't exist in source
                        return Ok(());
                    }
                }
            }
            None => {
                // Source key doesn't exist
                return Ok(());
            }
        }

        // If member was found in source, add it to destination
        if *ret == 1 {
            // Check destination key
            match db.get_opt(destination, &self.default_read_options)? {
                Some(meta_value) => {
                    let mut parsed_meta = ParsedInternalValue::new(&meta_value);

                    // Check if it's the right type
                    if parsed_meta.data_type() != DataType::Sets {
                        if parsed_meta.is_expired(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        ) {
                            // Create new set if expired
                            let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
                            sets_meta.set_size(1);
                            version = sets_meta.update_version();
                            batch.put(destination, &sets_meta.encode());

                            // Add member to destination
                            let member_key =
                                self.encode_sets_member_key(destination, version, member);
                            let empty_value = InternalValue::new(DataType::None, &[]).encode();
                            batch.put_cf(
                                self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                                &member_key,
                                &empty_value,
                            );
                        } else {
                            return Err(StorageError::InvalidFormat(format!(
                                "Wrong type for key: {}",
                                String::from_utf8_lossy(destination)
                            )));
                        }
                    } else if parsed_meta.is_expired(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    ) || parsed_meta.size() == 0
                    {
                        // Initialize meta value if expired or empty
                        version = parsed_meta.update_version();
                        parsed_meta.set_size(1);
                        batch.put(destination, &parsed_meta.encode());

                        // Add member to destination
                        let member_key = self.encode_sets_member_key(destination, version, member);
                        let empty_value = InternalValue::new(DataType::None, &[]).encode();
                        batch.put_cf(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            &member_key,
                            &empty_value,
                        );
                    } else {
                        // Check if member already exists in destination
                        version = parsed_meta.version();
                        let member_key = self.encode_sets_member_key(destination, version, member);

                        match db.get_cf_opt(
                            self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                            &member_key,
                            &self.default_read_options,
                        )? {
                            Some(_) => {
                                // Member already exists, nothing to do
                            }
                            None => {
                                // Add member to destination
                                parsed_meta.set_size(parsed_meta.size() + 1);
                                batch.put(destination, &parsed_meta.encode());

                                let empty_value = InternalValue::new(DataType::None, &[]).encode();
                                batch.put_cf(
                                    self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                                    &member_key,
                                    &empty_value,
                                );
                            }
                        }
                    }
                }
                None => {
                    // Create new set
                    let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
                    sets_meta.set_size(1);
                    version = sets_meta.update_version();
                    batch.put(destination, &sets_meta.encode());

                    // Add member to destination
                    let member_key = self.encode_sets_member_key(destination, version, member);
                    let empty_value = InternalValue::new(DataType::None, &[]).encode();
                    batch.put_cf(
                        self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
                        &member_key,
                        &empty_value,
                    );
                }
            }
        }

        // Write batch to DB
        db.write_opt(batch, &self.default_write_options)?;

        // Update statistics
        if statistic > 0 {
            self.update_specific_key_statistics(
                DataType::Sets,
                &String::from_utf8_lossy(source).to_string(),
                statistic,
            )?;
        }

        Ok(())
    }
}
