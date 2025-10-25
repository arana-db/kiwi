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

//! Redis sets operations implementation
//! This module provides set operations for Redis storage

use std::collections::HashSet;

use bytes::{BufMut, Bytes};
use kstd::lock_mgr::ScopeRecordLock;
use rocksdb::{Direction, IteratorMode, ReadOptions, WriteBatch};
use snafu::{OptionExt, ResultExt};

use crate::{
    ColumnFamilyIndex, Redis, Result,
    base_data_value_format::BaseDataValue,
    base_key_format::BaseMetaKey,
    base_meta_value_format::{BaseMetaValue, ParsedSetsMetaValue},
    base_value_format::DataType,
    error::{InvalidArgumentSnafu, KeyNotFoundSnafu, OptionNoneSnafu, RocksSnafu},
    member_data_key_format::MemberDataKey,
    storage_define::{PREFIX_RESERVE_LENGTH, SUFFIX_RESERVE_LENGTH, encode_user_key},
};

impl Redis {
    // /// Scan sets key number
    // pub fn scan_sets_key_num(&self, key_info: &mut crate::types::KeyInfo) -> Result<()> {
    //     let db = self
    //         .db
    //         .as_ref()
    //         .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
    //
    //     let mut keys = 0;
    //     let mut expires = 0;
    //     let mut ttl_sum = 0;
    //     let mut invalid_keys = 0;
    //
    //     // Get current time
    //     let now = SystemTime::now()
    //         .duration_since(UNIX_EPOCH)
    //         .unwrap()
    //         .as_secs();
    //
    //     // Create iterator options
    //     let mut iterator_options = ReadOptions::default();
    //     iterator_options.fill_cache(false);
    //
    //     // Create snapshot
    //     let snapshot = db.snapshot();
    //     iterator_options.set_snapshot(&snapshot);
    //
    //     // Iterate through all keys in meta column family
    //     let iter = db.iterator_cf_opt(
    //         self.get_handle(crate::redis::ColumnFamilyIndex::MetaCF),
    //         iterator_options,
    //         IteratorMode::Start,
    //     );
    //
    //     for (_, value) in iter {
    //         // Parse the meta value
    //         let parsed_value = ParsedInternalValue::new(&value);
    //
    //         // Check if it's a sets type
    //         if parsed_value.data_type() != DataType::Sets {
    //             continue;
    //         }
    //
    //         // Check if it's stale or empty
    //         if parsed_value.is_expired(now) || parsed_value.size() == 0 {
    //             invalid_keys += 1;
    //         } else {
    //             keys += 1;
    //
    //             // Check if it has expiration
    //             let etime = parsed_value.etime();
    //             if etime > 0 {
    //                 expires += 1;
    //                 ttl_sum += etime - now;
    //             }
    //         }
    //     }
    //
    //     // Set key info
    //     key_info.keys = keys;
    //     key_info.expires = expires;
    //     key_info.avg_ttl = if expires > 0 { ttl_sum / expires } else { 0 };
    //     key_info.invalid_keys = invalid_keys;
    //
    //     Ok(())
    // }

    /// Add one or more members to a set
    pub fn sadd(&self, key: &[u8], members: &[&[u8]]) -> Result<i32> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Filter duplicate members
        let mut unique = HashSet::new();
        let mut filtered_members = Vec::new();

        // Deduplicate members
        for &member in members {
            let member_str = String::from_utf8_lossy(member).to_string();
            if !unique.contains(&member_str) {
                unique.insert(member_str);
                filtered_members.push(member);
            }
        }

        if filtered_members.is_empty() {
            return Ok(0);
        }

        // Create lock for the key
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        // Try to get the existing set meta value
        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;
        let mut batch = WriteBatch::default();
        let base_meta_key = BaseMetaKey::new(key).encode()?;
        let meta_get = db.get_cf(&cf, &base_meta_key).context(RocksSnafu)?;
        let (mut set_meta_value, version, is_new_set) = match meta_get {
            Some(val) => {
                // Type check
                self.check_type(&val, DataType::Set)?;
                let set_meta_value = ParsedSetsMetaValue::new(&val[..])?;
                // Check if expired
                if set_meta_value.is_stale() {
                    // Create new set if expired
                    let count_bytes = 0u64.to_le_bytes().to_vec();
                    let mut new_meta = BaseMetaValue::new(Bytes::from(count_bytes));
                    new_meta.inner.data_type = DataType::Set;
                    let encoded = new_meta.encode();
                    let mut new_set_meta = ParsedSetsMetaValue::new(encoded)?;
                    let version = new_set_meta.initial_meta_value();
                    (new_set_meta, version, true)
                } else {
                    let version = set_meta_value.version();
                    (set_meta_value, version, false)
                }
            }
            None => {
                // Create new set
                let count_bytes = 0u64.to_le_bytes().to_vec();
                let mut new_meta = BaseMetaValue::new(Bytes::from(count_bytes));
                new_meta.inner.data_type = DataType::Set;
                let encoded = new_meta.encode();
                let mut new_set_meta = ParsedSetsMetaValue::new(encoded)?;
                let version = new_set_meta.initial_meta_value();
                (new_set_meta, version, true)
            }
        };

        // Filter out existing members if not a new set
        let mut members_to_add = Vec::new();
        if is_new_set {
            members_to_add = filtered_members;
        } else {
            for &member in &filtered_members {
                let member_key = MemberDataKey::new(key, version, member);
                let key_encoded = member_key.encode()?;
                let exists = db
                    .get_cf(&cf_data, &key_encoded)
                    .context(RocksSnafu)?
                    .is_some();
                if !exists {
                    members_to_add.push(member);
                }
            }
        }

        if members_to_add.is_empty() {
            return Ok(0);
        }

        // check add member size
        if !set_meta_value.check_set_count(members_to_add.len()) {
            return InvalidArgumentSnafu {
                message: "set size overflow".to_string(),
            }
            .fail();
        }

        let add_count = members_to_add.len() as u64;
        if !set_meta_value.check_modify_count(add_count) {
            return InvalidArgumentSnafu {
                message: "set size overflow".to_string(),
            }
            .fail();
        }
        set_meta_value.modify_count(add_count);

        batch.put_cf(&cf, base_meta_key, set_meta_value.encoded());

        for member in &members_to_add {
            let set_member_key = MemberDataKey::new(key, version, member);
            let iter_value = BaseDataValue::new("");
            let key_encoded = set_member_key.encode()?;
            let val_encoded = iter_value.encode();
            batch.put_cf(&cf_data, key_encoded.as_ref(), val_encoded.as_ref());
        }

        let added = members_to_add.len() as i32;
        // Write batch to DB
        db.write_opt(batch, &self.write_options)
            .context(RocksSnafu)?;

        Ok(added)
    }

    /// Get the number of members in a set
    pub fn scard(&self, key: &[u8]) -> Result<i32> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;

        match db.get_cf(&cf, &base_meta_key).context(RocksSnafu)? {
            Some(val) => {
                // Type check
                self.check_type(&val, DataType::Set)?;
                let set_meta = ParsedSetsMetaValue::new(&val[..])?;
                // Validity check (not expired and count > 0)
                if !set_meta.is_valid() {
                    return KeyNotFoundSnafu {
                        key: String::from_utf8_lossy(key).to_string(),
                    }
                    .fail();
                }
                Ok(set_meta.count() as i32)
            }
            None => KeyNotFoundSnafu {
                key: String::from_utf8_lossy(key).to_string(),
            }
            .fail(),
        }
    }

    /// Get all the members in a set
    pub fn smembers(&self, key: &[u8]) -> Result<Vec<String>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Read meta
        let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;
        let Some(val) = meta_val else {
            // Redis SMEMBERS returns empty array for non-existent keys
            return Ok(Vec::new());
        };

        // Type check
        self.check_type(&val, DataType::Set)?;

        let set_meta = ParsedSetsMetaValue::new(&val[..])?;
        // Validity check (not expired and count > 0)
        if !set_meta.is_valid() {
            // Redis SMEMBERS returns empty array for expired/invalid keys
            return Ok(Vec::new());
        }
        let version = set_meta.version();

        // Build prefix: reserve(8) + version(8) + encoded(key)
        let mut prefix = bytes::BytesMut::with_capacity(PREFIX_RESERVE_LENGTH + 8 + key.len() * 2);
        prefix.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        prefix.put_u64(version);
        encode_user_key(&bytes::Bytes::copy_from_slice(key), &mut prefix)?;

        // Iterate from prefix and collect members until prefix no longer matches
        let iter = db.iterator_cf_opt(
            &cf_data,
            ReadOptions::default(),
            IteratorMode::From(&prefix, Direction::Forward),
        );
        let mut members = Vec::new();
        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            if !raw_key.starts_with(&prefix) {
                break;
            }
            // member = raw_key[prefix..len - SUFFIX_RESERVE_LENGTH]
            if raw_key.len() >= prefix.len() + SUFFIX_RESERVE_LENGTH {
                let member_slice = &raw_key[prefix.len()..raw_key.len() - SUFFIX_RESERVE_LENGTH];
                members.push(String::from_utf8_lossy(member_slice).to_string());
            }
        }

        Ok(members)
    }

    /// Check if member is a member of the set stored at key
    pub fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Read meta
        let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;
        let Some(val) = meta_val else {
            // Redis SISMEMBER returns 0 for non-existent keys
            return Ok(false);
        };

        // Type check
        self.check_type(&val, DataType::Set)?;

        let set_meta = ParsedSetsMetaValue::new(&val[..])?;
        // Validity check (not expired and count > 0)
        if !set_meta.is_valid() {
            // Redis SISMEMBER returns 0 for expired/invalid keys
            return Ok(false);
        }

        let version = set_meta.version();

        // Build member key: reserve(8) + version(8) + encoded(key) + member + reserve(8)
        let mut member_key = bytes::BytesMut::with_capacity(
            PREFIX_RESERVE_LENGTH + 8 + key.len() * 2 + member.len() + SUFFIX_RESERVE_LENGTH,
        );
        member_key.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        member_key.put_u64(version);
        encode_user_key(&bytes::Bytes::copy_from_slice(key), &mut member_key)?;
        member_key.extend_from_slice(member);
        member_key.extend_from_slice(&[0u8; SUFFIX_RESERVE_LENGTH]);

        // Check if member exists
        let exists = db
            .get_cf(&cf_data, &member_key)
            .context(RocksSnafu)?
            .is_some();

        Ok(exists)
    }

    /// Get random members from a set
    pub fn srandmember(&self, key: &[u8], count: Option<i32>) -> Result<Vec<String>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Read meta
        let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;
        let Some(val) = meta_val else {
            // Redis SRANDMEMBER returns empty array for non-existent keys
            return Ok(Vec::new());
        };

        // Type check
        self.check_type(&val, DataType::Set)?;

        let set_meta = ParsedSetsMetaValue::new(&val[..])?;
        // Validity check (not expired and count > 0)
        if !set_meta.is_valid() {
            // Redis SRANDMEMBER returns empty array for expired/invalid keys
            return Ok(Vec::new());
        }

        let version = set_meta.version();
        let set_size = set_meta.count() as usize;

        if set_size == 0 {
            return Ok(Vec::new());
        }

        // Build prefix: reserve(8) + version(8) + encoded(key)
        let mut prefix = bytes::BytesMut::with_capacity(PREFIX_RESERVE_LENGTH + 8 + key.len() * 2);
        prefix.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        prefix.put_u64(version);
        encode_user_key(&bytes::Bytes::copy_from_slice(key), &mut prefix)?;

        // Collect all members first
        let iter = db.iterator_cf_opt(
            &cf_data,
            ReadOptions::default(),
            IteratorMode::From(&prefix, Direction::Forward),
        );
        let mut all_members = Vec::new();
        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            if !raw_key.starts_with(&prefix) {
                break;
            }
            // member = raw_key[prefix..len - SUFFIX_RESERVE_LENGTH]
            if raw_key.len() >= prefix.len() + SUFFIX_RESERVE_LENGTH {
                let member_slice = &raw_key[prefix.len()..raw_key.len() - SUFFIX_RESERVE_LENGTH];
                all_members.push(String::from_utf8_lossy(member_slice).to_string());
            }
        }

        if all_members.is_empty() {
            return Ok(Vec::new());
        }

        match count {
            None => {
                // Return single random member
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let index = rng.gen_range(0..all_members.len());
                Ok(vec![all_members[index].clone()])
            }
            Some(count_val) => {
                if count_val == 0 {
                    return Ok(Vec::new());
                }

                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();

                if count_val > 0 {
                    // Positive count: return unique members (no duplicates)
                    let count_usize = count_val as usize;
                    if count_usize >= all_members.len() {
                        // Return all members in random order
                        all_members.shuffle(&mut rng);
                        Ok(all_members)
                    } else {
                        // Return random sample without replacement
                        let sample = all_members
                            .choose_multiple(&mut rng, count_usize)
                            .cloned()
                            .collect();
                        Ok(sample)
                    }
                } else {
                    // Negative count: allow duplicates
                    let count_usize = (-count_val) as usize;
                    let mut result = Vec::with_capacity(count_usize);
                    for _ in 0..count_usize {
                        let member = all_members.choose(&mut rng).unwrap();
                        result.push(member.clone());
                    }
                    Ok(result)
                }
            }
        }
    }

    /// Remove one or more members from a set
    pub fn srem(&self, key: &[u8], members: &[&[u8]]) -> Result<i32> {
        if members.is_empty() {
            return Ok(0);
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Lock the key for atomic read-modify-write
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Read meta
        let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;
        let Some(val) = meta_val else {
            // Redis SREM returns 0 for non-existent keys
            return Ok(0);
        };

        // Type check
        self.check_type(&val, DataType::Set)?;

        let mut set_meta = ParsedSetsMetaValue::new(&val[..])?;
        // Validity check (not expired and count > 0)
        if !set_meta.is_valid() {
            // Redis SREM returns 0 for expired/invalid keys
            return Ok(0);
        }

        let version = set_meta.version();
        let mut removed_count = 0i32;
        let mut batch = WriteBatch::default();

        // Remove each member
        for member in members {
            // Build member key: reserve(8) + version(8) + encoded(key) + member + reserve(8)
            let mut member_key = bytes::BytesMut::with_capacity(
                PREFIX_RESERVE_LENGTH + 8 + key.len() * 2 + member.len() + SUFFIX_RESERVE_LENGTH,
            );
            member_key.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
            member_key.put_u64(version);
            encode_user_key(&bytes::Bytes::copy_from_slice(key), &mut member_key)?;
            member_key.extend_from_slice(member);
            member_key.extend_from_slice(&[0u8; SUFFIX_RESERVE_LENGTH]);

            // Check if member exists before removing
            if db
                .get_cf(&cf_data, &member_key)
                .context(RocksSnafu)?
                .is_some()
            {
                batch.delete_cf(&cf_data, &member_key);
                removed_count += 1;
            }
        }

        if removed_count > 0 {
            // Update set metadata
            let new_count = set_meta.count() - removed_count as u64;
            if new_count == 0 {
                // Remove the entire set if no members left
                batch.delete_cf(&cf_meta, &base_meta_key);
            } else {
                // Update the count in metadata
                set_meta.set_count(new_count);
                batch.put_cf(&cf_meta, &base_meta_key, set_meta.encoded());
            }

            // Write the batch
            db.write(batch).context(RocksSnafu)?;
        }

        Ok(removed_count)
    }

    /// Remove and return random members from a set
    pub fn spop(&self, key: &[u8], count: Option<i32>) -> Result<Vec<String>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Lock the key for atomic read-modify-write
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Read meta
        let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;
        let Some(val) = meta_val else {
            // Redis SPOP returns empty array for non-existent keys
            return Ok(Vec::new());
        };

        // Type check
        self.check_type(&val, DataType::Set)?;

        let mut set_meta = ParsedSetsMetaValue::new(&val[..])?;
        // Validity check (not expired and count > 0)
        if !set_meta.is_valid() {
            // Redis SPOP returns empty array for expired/invalid keys
            return Ok(Vec::new());
        }

        let version = set_meta.version();
        let set_size = set_meta.count() as usize;

        if set_size == 0 {
            return Ok(Vec::new());
        }

        // Build prefix: reserve(8) + version(8) + encoded(key)
        let mut prefix = bytes::BytesMut::with_capacity(PREFIX_RESERVE_LENGTH + 8 + key.len() * 2);
        prefix.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        prefix.put_u64(version);
        encode_user_key(&bytes::Bytes::copy_from_slice(key), &mut prefix)?;

        // Collect all members first
        let iter = db.iterator_cf_opt(
            &cf_data,
            ReadOptions::default(),
            IteratorMode::From(&prefix, Direction::Forward),
        );
        let mut all_members = Vec::new();
        let mut all_member_keys = Vec::new();
        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            if !raw_key.starts_with(&prefix) {
                break;
            }
            // member = raw_key[prefix..len - SUFFIX_RESERVE_LENGTH]
            if raw_key.len() >= prefix.len() + SUFFIX_RESERVE_LENGTH {
                let member_slice = &raw_key[prefix.len()..raw_key.len() - SUFFIX_RESERVE_LENGTH];
                all_members.push(String::from_utf8_lossy(member_slice).to_string());
                all_member_keys.push(raw_key.to_vec());
            }
        }

        if all_members.is_empty() {
            return Ok(Vec::new());
        }

        // Select members to pop
        let members_to_pop = match count {
            None => {
                // Return single random member
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let index = rng.gen_range(0..all_members.len());
                vec![(all_members[index].clone(), all_member_keys[index].clone())]
            }
            Some(count_val) => {
                if count_val <= 0 {
                    return Ok(Vec::new());
                }

                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();

                let count_usize = count_val as usize;
                if count_usize >= all_members.len() {
                    // Return all members
                    all_members.into_iter().zip(all_member_keys).collect()
                } else {
                    // Return random sample without replacement
                    let indices: Vec<usize> = (0..all_members.len()).collect();
                    let selected_indices = indices.choose_multiple(&mut rng, count_usize);
                    selected_indices
                        .map(|&i| (all_members[i].clone(), all_member_keys[i].clone()))
                        .collect()
                }
            }
        };

        if members_to_pop.is_empty() {
            return Ok(Vec::new());
        }

        // Remove selected members
        let mut batch = WriteBatch::default();
        let removed_count = members_to_pop.len() as u64;

        for (_, member_key) in &members_to_pop {
            batch.delete_cf(&cf_data, member_key);
        }

        // Update set metadata
        let new_count = set_meta.count() - removed_count;
        if new_count == 0 {
            // Remove the entire set if no members left
            batch.delete_cf(&cf_meta, &base_meta_key);
        } else {
            // Update the count in metadata
            set_meta.set_count(new_count);
            batch.put_cf(&cf_meta, &base_meta_key, set_meta.encoded());
        }

        // Write the batch
        db.write(batch).context(RocksSnafu)?;

        // Return the popped members
        Ok(members_to_pop
            .into_iter()
            .map(|(member, _)| member)
            .collect())
    }

    /// Move member from source set to destination set
    pub fn smove(&self, source: &[u8], destination: &[u8], member: &[u8]) -> Result<bool> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let source_meta_key = BaseMetaKey::new(source).encode()?;
        let dest_meta_key = BaseMetaKey::new(destination).encode()?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Lock both keys to ensure atomicity (lock in consistent order to avoid deadlock)
        let (first_key, second_key) = if source <= destination {
            (source, destination)
        } else {
            (destination, source)
        };

        let first_key_str = String::from_utf8_lossy(first_key).to_string();
        let _lock1 = ScopeRecordLock::new(self.lock_mgr.as_ref(), &first_key_str);
        let _lock2 = if first_key != second_key {
            let second_key_str = String::from_utf8_lossy(second_key).to_string();
            Some(ScopeRecordLock::new(
                self.lock_mgr.as_ref(),
                &second_key_str,
            ))
        } else {
            None
        };

        // Read source meta
        let source_meta_val = db.get_cf(&cf_meta, &source_meta_key).context(RocksSnafu)?;
        let Some(source_val) = source_meta_val else {
            // Source set doesn't exist
            return Ok(false);
        };

        // Type check for source
        self.check_type(&source_val, DataType::Set)?;

        let mut source_meta = ParsedSetsMetaValue::new(&source_val[..])?;
        if !source_meta.is_valid() {
            // Source set is expired/invalid
            return Ok(false);
        }

        let source_version = source_meta.version();

        // Build source member key
        let mut source_member_key = bytes::BytesMut::with_capacity(
            PREFIX_RESERVE_LENGTH + 8 + source.len() * 2 + member.len() + SUFFIX_RESERVE_LENGTH,
        );
        source_member_key.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        source_member_key.put_u64(source_version);
        encode_user_key(
            &bytes::Bytes::copy_from_slice(source),
            &mut source_member_key,
        )?;
        source_member_key.extend_from_slice(member);
        source_member_key.extend_from_slice(&[0u8; SUFFIX_RESERVE_LENGTH]);

        // Check if member exists in source
        if db
            .get_cf(&cf_data, &source_member_key)
            .context(RocksSnafu)?
            .is_none()
        {
            // Member doesn't exist in source
            return Ok(false);
        }

        // Handle destination set
        let dest_meta_val = db.get_cf(&cf_meta, &dest_meta_key).context(RocksSnafu)?;
        let (mut dest_meta, dest_version, dest_exists) = if let Some(dest_val) = dest_meta_val {
            // Type check for destination
            self.check_type(&dest_val, DataType::Set)?;

            let mut meta = ParsedSetsMetaValue::new(&dest_val[..])?;
            let version = if meta.is_valid() {
                meta.version()
            } else {
                // Destination set is expired, create new version
                meta.initial_meta_value()
            };
            (meta, version, true)
        } else {
            // Destination set doesn't exist, create new one
            let count_bytes = 0u64.to_le_bytes();
            let mut new_meta = BaseMetaValue::new(Bytes::from(count_bytes.to_vec()));
            new_meta.inner.data_type = DataType::Set;
            let encoded = new_meta.encode();
            let mut meta = ParsedSetsMetaValue::new(encoded)?;
            let version = meta.initial_meta_value();
            (meta, version, false)
        };

        // Build destination member key
        let mut dest_member_key = bytes::BytesMut::with_capacity(
            PREFIX_RESERVE_LENGTH
                + 8
                + destination.len() * 2
                + member.len()
                + SUFFIX_RESERVE_LENGTH,
        );
        dest_member_key.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        dest_member_key.put_u64(dest_version);
        encode_user_key(
            &bytes::Bytes::copy_from_slice(destination),
            &mut dest_member_key,
        )?;
        dest_member_key.extend_from_slice(member);
        dest_member_key.extend_from_slice(&[0u8; SUFFIX_RESERVE_LENGTH]);

        // Check if member already exists in destination
        let member_exists_in_dest = db
            .get_cf(&cf_data, &dest_member_key)
            .context(RocksSnafu)?
            .is_some();

        let mut batch = WriteBatch::default();

        // Remove from source
        batch.delete_cf(&cf_data, &source_member_key);
        let new_source_count = source_meta.count() - 1;
        if new_source_count == 0 {
            // Remove source set if empty
            batch.delete_cf(&cf_meta, &source_meta_key);
        } else {
            // Update source count
            source_meta.set_count(new_source_count);
            batch.put_cf(&cf_meta, &source_meta_key, source_meta.encoded());
        }

        // Add to destination (if not already there)
        if !member_exists_in_dest {
            let iter_value = BaseDataValue::new("");
            batch.put_cf(&cf_data, &dest_member_key, iter_value.encode().as_ref());

            // Update destination count
            if dest_exists && dest_meta.is_valid() {
                dest_meta.set_count(dest_meta.count() + 1);
            } else {
                dest_meta.set_count(1);
            }
            batch.put_cf(&cf_meta, &dest_meta_key, dest_meta.encoded());
        }

        // Write the batch
        db.write(batch).context(RocksSnafu)?;

        Ok(true)
    }

    // /// Get all the members in a set with TTL
    // pub fn smembers_with_ttl(
    //     &self,
    //     key: &[u8],
    //     members: &mut Vec<String>,
    //     ttl: &mut i64,
    // ) -> Result<()> {
    //     let db = self
    //         .db
    //         .as_ref()
    //         .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
    //
    //     // Create read options
    //     let read_options = ReadOptions::default();
    //
    //     // Get the meta value
    //     match db.get_opt(key, &read_options)? {
    //         Some(meta_value) => {
    //             let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //             // Check if it's the right type
    //             if parsed_meta.data_type() != DataType::Sets {
    //                 return Err(StorageError::InvalidFormat(format!(
    //                     "Wrong type for key: {}",
    //                     String::from_utf8_lossy(key)
    //                 )));
    //             }
    //
    //             // Check if expired
    //             if parsed_meta.is_expired(
    //                 SystemTime::now()
    //                     .duration_since(UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs(),
    //             ) {
    //                 return Err(StorageError::KeyNotFound("Stale".to_string()));
    //             }
    //
    //             // Calculate TTL
    //             let etime = parsed_meta.etime();
    //             if etime == 0 {
    //                 *ttl = -1; // No expiration
    //             } else {
    //                 let now = SystemTime::now()
    //                     .duration_since(UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs();
    //                 *ttl = if etime > now {
    //                     (etime - now) as i64
    //                 } else {
    //                     -2
    //                 };
    //             }
    //
    //             // Get the version
    //             let version = parsed_meta.version();
    //
    //             // Create prefix for iteration
    //             let prefix = self.encode_sets_member_prefix(key, version);
    //
    //             // Iterate through all members
    //             let iter = db.iterator_cf_opt(
    //                 self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                 read_options.clone(),
    //                 IteratorMode::From(&prefix, Direction::Forward),
    //             );
    //
    //             for (key_bytes, _) in iter {
    //                 // Check if key starts with prefix
    //                 if !key_bytes.starts_with(&prefix) {
    //                     break;
    //                 }
    //
    //                 // Extract member from key
    //                 let member = self.decode_sets_member_from_key(&key_bytes);
    //                 members.push(String::from_utf8_lossy(&member).to_string());
    //             }
    //         }
    //         None => {
    //             return Err(StorageError::KeyNotFound(
    //                 String::from_utf8_lossy(key).to_string(),
    //             ));
    //         }
    //     }
    //
    //     Ok(())
    // }

    // /// Returns the members of the set resulting from the difference between the first set and all the successive sets
    // pub fn sdiff(&self, keys: &[&[u8]], members: &mut Vec<String>) -> Result<()> {
    //     if keys.is_empty() {
    //         return Err(StorageError::InvalidFormat(
    //             "SDiff invalid parameter, no keys".to_string(),
    //         ));
    //     }
    //
    //     let db = self
    //         .db
    //         .as_ref()
    //         .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
    //
    //     // Create snapshot and read options
    //     let snapshot = db.snapshot();
    //     let mut read_options = ReadOptions::default();
    //     read_options.set_snapshot(&snapshot);
    //
    //     // Store valid sets (key, version)
    //     let mut valid_sets = Vec::new();
    //
    //     // Process all keys except the first one
    //     for &key in &keys[1..] {
    //         match db.get_opt(key, &read_options)? {
    //             Some(meta_value) => {
    //                 let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //                 // Check if it's the right type
    //                 if parsed_meta.data_type() != DataType::Sets {
    //                     return Err(StorageError::InvalidFormat(format!(
    //                         "Wrong type for key: {}",
    //                         String::from_utf8_lossy(key)
    //                     )));
    //                 }
    //
    //                 // Skip if expired
    //                 if parsed_meta.is_expired(
    //                     SystemTime::now()
    //                         .duration_since(UNIX_EPOCH)
    //                         .unwrap()
    //                         .as_secs(),
    //                 ) {
    //                     continue;
    //                 }
    //
    //                 // Add to valid sets
    //                 valid_sets.push((key, parsed_meta.version()));
    //             }
    //             None => {
    //                 // Key not found, skip
    //             }
    //         }
    //     }
    //
    //     // Process the first key
    //     match db.get_opt(keys[0], &read_options)? {
    //         Some(meta_value) => {
    //             let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //             // Check if it's the right type
    //             if parsed_meta.data_type() != DataType::Sets {
    //                 return Err(StorageError::InvalidFormat(format!(
    //                     "Wrong type for key: {}",
    //                     String::from_utf8_lossy(keys[0])
    //                 )));
    //             }
    //
    //             // Skip if expired
    //             if parsed_meta.is_expired(
    //                 SystemTime::now()
    //                     .duration_since(UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs(),
    //             ) {
    //                 return Ok(());
    //             }
    //
    //             // Get the version
    //             let version = parsed_meta.version();
    //
    //             // Create prefix for iteration
    //             let prefix = self.encode_sets_member_prefix(keys[0], version);
    //
    //             // Iterate through all members of the first set
    //             let iter = db.iterator_cf_opt(
    //                 self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                 read_options.clone(),
    //                 IteratorMode::From(&prefix, Direction::Forward),
    //             );
    //
    //             for (key_bytes, _) in iter {
    //                 // Check if key starts with prefix
    //                 if !key_bytes.starts_with(&prefix) {
    //                     break;
    //                 }
    //
    //                 // Extract member from key
    //                 let member = self.decode_sets_member_from_key(&key_bytes);
    //
    //                 // Check if member exists in any other set
    //                 let mut found = false;
    //                 for &(other_key, other_version) in &valid_sets {
    //                     let member_key =
    //                         self.encode_sets_member_key(other_key, other_version, &member);
    //
    //                     match db.get_cf_opt(
    //                         self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                         &member_key,
    //                         &read_options,
    //                     )? {
    //                         Some(_) => {
    //                             found = true;
    //                             break;
    //                         }
    //                         None => {
    //                             // Not found in this set
    //                         }
    //                     }
    //                 }
    //
    //                 // Add to result if not found in any other set
    //                 if !found {
    //                     members.push(String::from_utf8_lossy(&member).to_string());
    //                 }
    //             }
    //         }
    //         None => {
    //             // First key not found, return empty result
    //         }
    //     }
    //
    //     Ok(())
    // }

    // /// Helper method to encode sets member key
    // fn encode_sets_member_key(&self, key: &[u8], version: u64, member: &[u8]) -> Vec<u8> {
    //     // In a real implementation, this would encode the key in the format expected by the C++ code
    //     // For simplicity, we'll use a basic format here
    //     let mut result = Vec::with_capacity(key.len() + 8 + member.len() + 2);
    //     result.extend_from_slice(key);
    //     result.push(0); // separator
    //
    //     // Add version (8 bytes)
    //     result.extend_from_slice(&version.to_be_bytes());
    //
    //     result.push(0); // separator
    //     result.extend_from_slice(member);
    //
    //     result
    // }

    // /// Helper method to encode sets member prefix for iteration
    // fn encode_sets_member_prefix(&self, key: &[u8], version: u64) -> Vec<u8> {
    //     let mut result = Vec::with_capacity(key.len() + 9);
    //     result.extend_from_slice(key);
    //     result.push(0); // separator
    //
    //     // Add version (8 bytes)
    //     result.extend_from_slice(&version.to_be_bytes());
    //
    //     result.push(0); // separator
    //
    //     result
    // }

    // /// Helper method to decode member from sets member key
    // fn decode_sets_member_from_key(&self, key: &[u8]) -> Vec<u8> {
    //     // Find the second separator
    //     let mut separator_count = 0;
    //     let mut pos = 0;
    //
    //     for (i, &byte) in key.iter().enumerate() {
    //         if byte == 0 {
    //             separator_count += 1;
    //             if separator_count == 2 {
    //                 pos = i + 1;
    //                 break;
    //             }
    //         }
    //     }
    //
    //     if pos > 0 && pos < key.len() {
    //         key[pos..].to_vec()
    //     } else {
    //         Vec::new()
    //     }
    // }

    // /// Store the difference between the first set and all the successive sets into a destination key
    // pub fn sdiffstore(
    //     &self,
    //     destination: &[u8],
    //     keys: &[&[u8]],
    //     value_to_dest: &mut Vec<String>,
    //     ret: &mut i32,
    // ) -> Result<()> {
    //     if keys.is_empty() {
    //         return Err(StorageError::InvalidFormat(
    //             "SDiffstore invalid parameter, no keys".to_string(),
    //         ));
    //     }
    //
    //     let db = self
    //         .db
    //         .as_ref()
    //         .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
    //
    //     // Create lock for the destination key
    //     let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), destination);
    //
    //     // Create snapshot and read options
    //     let snapshot = db.snapshot();
    //     let mut read_options = ReadOptions::default();
    //     read_options.set_snapshot(&snapshot);
    //
    //     // Store valid sets (key, version)
    //     let mut valid_sets = Vec::new();
    //     let mut members = Vec::new();
    //
    //     // Process all keys except the first one
    //     for &key in &keys[1..] {
    //         match db.get_opt(key, &read_options)? {
    //             Some(meta_value) => {
    //                 let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //                 // Check if it's the right type
    //                 if parsed_meta.data_type() != DataType::Sets {
    //                     return Err(StorageError::InvalidFormat(format!(
    //                         "Wrong type for key: {}",
    //                         String::from_utf8_lossy(key)
    //                     )));
    //                 }
    //
    //                 // Skip if expired
    //                 if parsed_meta.is_expired(
    //                     SystemTime::now()
    //                         .duration_since(UNIX_EPOCH)
    //                         .unwrap()
    //                         .as_secs(),
    //                 ) {
    //                     continue;
    //                 }
    //
    //                 // Add to valid sets
    //                 valid_sets.push(KeyVersion {
    //                     key: key.to_vec(),
    //                     version: parsed_meta.version(),
    //                 });
    //             }
    //             None => {
    //                 // Key not found, skip
    //             }
    //         }
    //     }
    //
    //     // Process the first key
    //     match db.get_opt(keys[0], &read_options)? {
    //         Some(meta_value) => {
    //             let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //             // Check if it's the right type
    //             if parsed_meta.data_type() != DataType::Sets {
    //                 return Err(StorageError::InvalidFormat(format!(
    //                     "Wrong type for key: {}",
    //                     String::from_utf8_lossy(keys[0])
    //                 )));
    //             }
    //
    //             // Skip if expired
    //             if parsed_meta.is_expired(
    //                 SystemTime::now()
    //                     .duration_since(UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs(),
    //             ) {
    //                 // Empty result
    //             } else {
    //                 // Get the version
    //                 let version = parsed_meta.version();
    //
    //                 // Create prefix for iteration
    //                 let prefix = self.encode_sets_member_prefix(keys[0], version);
    //
    //                 // Iterate through all members of the first set
    //                 let iter = db.iterator_cf_opt(
    //                     self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                     read_options.clone(),
    //                     IteratorMode::From(&prefix, Direction::Forward),
    //                 );
    //
    //                 for (key_bytes, _) in iter {
    //                     // Check if key starts with prefix
    //                     if !key_bytes.starts_with(&prefix) {
    //                         break;
    //                     }
    //
    //                     // Extract member from key
    //                     let member = self.decode_sets_member_from_key(&key_bytes);
    //
    //                     // Check if member exists in any other set
    //                     let mut found = false;
    //                     for key_version in &valid_sets {
    //                         let member_key = self.encode_sets_member_key(
    //                             &key_version.key,
    //                             key_version.version,
    //                             &member,
    //                         );
    //
    //                         match db.get_cf_opt(
    //                             self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                             &member_key,
    //                             &read_options,
    //                         )? {
    //                             Some(_) => {
    //                                 found = true;
    //                                 break;
    //                             }
    //                             None => {
    //                                 // Not found in this set
    //                             }
    //                         }
    //                     }
    //
    //                     // Add to result if not found in any other set
    //                     if !found {
    //                         let member_str = String::from_utf8_lossy(&member).to_string();
    //                         members.push(member_str.clone());
    //                         value_to_dest.push(member_str);
    //                     }
    //                 }
    //             }
    //         }
    //         None => {
    //             // First key not found, empty result
    //         }
    //     }
    //
    //     // Store the result in the destination key
    //     let mut batch = WriteBatch::default();
    //     let mut version = 0;
    //     let mut statistic = 0;
    //
    //     // Check if destination key exists
    //     match db.get_opt(destination, &read_options)? {
    //         Some(meta_value) => {
    //             if ParsedInternalValue::new(&meta_value).data_type() == DataType::Sets {
    //                 let mut parsed_meta = ParsedInternalValue::new(&meta_value);
    //                 statistic = parsed_meta.size();
    //                 version = parsed_meta.update_version();
    //                 parsed_meta.set_size(members.len() as u64);
    //                 batch.put(destination, &parsed_meta.encode());
    //             } else {
    //                 // Create new set
    //                 let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
    //                 sets_meta.set_size(members.len() as u64);
    //                 version = sets_meta.update_version();
    //                 batch.put(destination, &sets_meta.encode());
    //             }
    //         }
    //         None => {
    //             // Create new set
    //             let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
    //             sets_meta.set_size(members.len() as u64);
    //             version = sets_meta.update_version();
    //             batch.put(destination, &sets_meta.encode());
    //         }
    //     }
    //
    //     // Add all members to the destination set
    //     for member_str in &members {
    //         let member_key =
    //             self.encode_sets_member_key(destination, version, member_str.as_bytes());
    //         let empty_value = InternalValue::new(DataType::None, &[]).encode();
    //         batch.put_cf(
    //             self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //             &member_key,
    //             &empty_value,
    //         );
    //     }
    //
    //     // Write batch to DB
    //     db.write_opt(batch, &self.default_write_options)?;
    //
    //     // Update statistics
    //     self.update_specific_key_statistics(
    //         DataType::Sets,
    //         &String::from_utf8_lossy(destination).to_string(),
    //         statistic,
    //     )?;
    //
    //     // Set return value
    //     *ret = members.len() as i32;
    //
    //     Ok(())
    // }
    //
    // /// Returns the members of the set resulting from the intersection of all the given sets
    // pub fn sinter(&self, keys: &[&[u8]], members: &mut Vec<String>) -> Result<()> {
    //     if keys.is_empty() {
    //         return Err(StorageError::InvalidFormat(
    //             "SInter invalid parameter, no keys".to_string(),
    //         ));
    //     }
    //
    //     let db = self
    //         .db
    //         .as_ref()
    //         .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
    //
    //     // Create snapshot and read options
    //     let snapshot = db.snapshot();
    //     let mut read_options = ReadOptions::default();
    //     read_options.set_snapshot(&snapshot);
    //
    //     // Store valid sets (key, version)
    //     let mut valid_sets = Vec::new();
    //
    //     // Process all keys except the first one
    //     for &key in &keys[1..] {
    //         match db.get_opt(key, &read_options)? {
    //             Some(meta_value) => {
    //                 let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //                 // Check if it's the right type
    //                 if parsed_meta.data_type() != DataType::Sets {
    //                     return Err(StorageError::InvalidFormat(format!(
    //                         "Wrong type for key: {}",
    //                         String::from_utf8_lossy(key)
    //                     )));
    //                 }
    //
    //                 // Return empty result if any key is expired or empty
    //                 if parsed_meta.is_expired(
    //                     SystemTime::now()
    //                         .duration_since(UNIX_EPOCH)
    //                         .unwrap()
    //                         .as_secs(),
    //                 ) {
    //                     return Ok(());
    //                 }
    //
    //                 // Add to valid sets
    //                 valid_sets.push(KeyVersion {
    //                     key: key.to_vec(),
    //                     version: parsed_meta.version(),
    //                 });
    //             }
    //             None => {
    //                 // If any key doesn't exist, return empty result
    //                 return Ok(());
    //             }
    //         }
    //     }
    //
    //     // Process the first key
    //     match db.get_opt(keys[0], &read_options)? {
    //         Some(meta_value) => {
    //             let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //             // Check if it's the right type
    //             if parsed_meta.data_type() != DataType::Sets {
    //                 return Err(StorageError::InvalidFormat(format!(
    //                     "Wrong type for key: {}",
    //                     String::from_utf8_lossy(keys[0])
    //                 )));
    //             }
    //
    //             // Return empty result if the first key is expired
    //             if parsed_meta.is_expired(
    //                 SystemTime::now()
    //                     .duration_since(UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs(),
    //             ) {
    //                 return Ok(());
    //             }
    //
    //             // Get the version
    //             let version = parsed_meta.version();
    //
    //             // Create prefix for iteration
    //             let prefix = self.encode_sets_member_prefix(keys[0], version);
    //
    //             // Iterate through all members of the first set
    //             let iter = db.iterator_cf_opt(
    //                 self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                 read_options.clone(),
    //                 IteratorMode::From(&prefix, Direction::Forward),
    //             );
    //
    //             for (key_bytes, _) in iter {
    //                 // Check if key starts with prefix
    //                 if !key_bytes.starts_with(&prefix) {
    //                     break;
    //                 }
    //
    //                 // Extract member from key
    //                 let member = self.decode_sets_member_from_key(&key_bytes);
    //
    //                 // Check if member exists in all other sets
    //                 let mut reliable = true;
    //                 for key_version in &valid_sets {
    //                     let member_key = self.encode_sets_member_key(
    //                         &key_version.key,
    //                         key_version.version,
    //                         &member,
    //                     );
    //
    //                     match db.get_cf_opt(
    //                         self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                         &member_key,
    //                         &read_options,
    //                     )? {
    //                         Some(_) => {
    //                             // Member exists in this set, continue checking
    //                         }
    //                         None => {
    //                             // Member doesn't exist in this set, not in intersection
    //                             reliable = false;
    //                             break;
    //                         }
    //                     }
    //                 }
    //
    //                 // Add to result if found in all sets
    //                 if reliable {
    //                     members.push(String::from_utf8_lossy(&member).to_string());
    //                 }
    //             }
    //         }
    //         None => {
    //             // First key not found, return empty result
    //         }
    //     }
    //
    //     Ok(())
    // }

    // /// Store the intersection of the sets in a new set at destination
    // pub fn sinterstore(
    //     &self,
    //     destination: &[u8],
    //     keys: &[&[u8]],
    //     value_to_dest: &mut Vec<String>,
    //     ret: &mut i32,
    // ) -> Result<()> {
    //     if keys.is_empty() {
    //         return Err(StorageError::InvalidFormat(
    //             "SInterstore invalid parameter, no keys".to_string(),
    //         ));
    //     }
    //
    //     let db = self
    //         .db
    //         .as_ref()
    //         .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
    //
    //     // Create lock for the destination key
    //     let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), destination);
    //
    //     // Create snapshot and read options
    //     let snapshot = db.snapshot();
    //     let mut read_options = ReadOptions::default();
    //     read_options.set_snapshot(&snapshot);
    //
    //     // Store valid sets (key, version)
    //     let mut valid_sets = Vec::new();
    //     let mut have_invalid_sets = false;
    //     let mut members = Vec::new();
    //
    //     // Process all keys except the first one
    //     for &key in &keys[1..] {
    //         match db.get_opt(key, &read_options)? {
    //             Some(meta_value) => {
    //                 let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //                 // Check if it's the right type
    //                 if parsed_meta.data_type() != DataType::Sets {
    //                     return Err(StorageError::InvalidFormat(format!(
    //                         "Wrong type for key: {}",
    //                         String::from_utf8_lossy(key)
    //                     )));
    //                 }
    //
    //                 // If any key is expired or empty, we have invalid sets
    //                 if parsed_meta.is_expired(
    //                     SystemTime::now()
    //                         .duration_since(UNIX_EPOCH)
    //                         .unwrap()
    //                         .as_secs(),
    //                 ) {
    //                     have_invalid_sets = true;
    //                     break;
    //                 }
    //
    //                 // Add to valid sets
    //                 valid_sets.push(KeyVersion {
    //                     key: key.to_vec(),
    //                     version: parsed_meta.version(),
    //                 });
    //             }
    //             None => {
    //                 // If any key doesn't exist, we have invalid sets
    //                 have_invalid_sets = true;
    //                 break;
    //             }
    //         }
    //     }
    //
    //     // If we don't have invalid sets, process the first key
    //     if !have_invalid_sets {
    //         match db.get_opt(keys[0], &read_options)? {
    //             Some(meta_value) => {
    //                 let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //                 // Check if it's the right type
    //                 if parsed_meta.data_type() != DataType::Sets {
    //                     return Err(StorageError::InvalidFormat(format!(
    //                         "Wrong type for key: {}",
    //                         String::from_utf8_lossy(keys[0])
    //                     )));
    //                 }
    //
    //                 // If the first key is expired, we have invalid sets
    //                 if parsed_meta.is_expired(
    //                     SystemTime::now()
    //                         .duration_since(UNIX_EPOCH)
    //                         .unwrap()
    //                         .as_secs(),
    //                 ) {
    //                     have_invalid_sets = true;
    //                 } else {
    //                     // Get the version
    //                     let version = parsed_meta.version();
    //
    //                     // Create prefix for iteration
    //                     let prefix = self.encode_sets_member_prefix(keys[0], version);
    //
    //                     // Iterate through all members of the first set
    //                     let iter = db.iterator_cf_opt(
    //                         self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                         read_options.clone(),
    //                         IteratorMode::From(&prefix, Direction::Forward),
    //                     );
    //
    //                     for (key_bytes, _) in iter {
    //                         // Check if key starts with prefix
    //                         if !key_bytes.starts_with(&prefix) {
    //                             break;
    //                         }
    //
    //                         // Extract member from key
    //                         let member = self.decode_sets_member_from_key(&key_bytes);
    //
    //                         // Check if member exists in all other sets
    //                         let mut reliable = true;
    //                         for key_version in &valid_sets {
    //                             let member_key = self.encode_sets_member_key(
    //                                 &key_version.key,
    //                                 key_version.version,
    //                                 &member,
    //                             );
    //
    //                             match db.get_cf_opt(
    //                                 self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                                 &member_key,
    //                                 &read_options,
    //                             )? {
    //                                 Some(_) => {
    //                                     // Member exists in this set, continue checking
    //                                 }
    //                                 None => {
    //                                     // Member doesn't exist in this set, not in intersection
    //                                     reliable = false;
    //                                     break;
    //                                 }
    //                             }
    //                         }
    //
    //                         // Add to result if found in all sets
    //                         if reliable {
    //                             let member_str = String::from_utf8_lossy(&member).to_string();
    //                             members.push(member_str.clone());
    //                             value_to_dest.push(member_str);
    //                         }
    //                     }
    //                 }
    //             }
    //             None => {
    //                 // First key not found, empty result
    //             }
    //         }
    //     }
    //
    //     // Store the result in the destination key
    //     let mut batch = WriteBatch::default();
    //     let mut version = 0;
    //     let mut statistic = 0;
    //
    //     // Check if destination key exists
    //     match db.get_opt(destination, &read_options)? {
    //         Some(meta_value) => {
    //             if ParsedInternalValue::new(&meta_value).data_type() == DataType::Sets {
    //                 let mut parsed_meta = ParsedInternalValue::new(&meta_value);
    //                 statistic = parsed_meta.size();
    //                 version = parsed_meta.update_version();
    //                 parsed_meta.set_size(members.len() as u64);
    //                 batch.put(destination, &parsed_meta.encode());
    //             } else {
    //                 // Create new set
    //                 let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
    //                 sets_meta.set_size(members.len() as u64);
    //                 version = sets_meta.update_version();
    //                 batch.put(destination, &sets_meta.encode());
    //             }
    //         }
    //         None => {
    //             // Create new set
    //             let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
    //             sets_meta.set_size(members.len() as u64);
    //             version = sets_meta.update_version();
    //             batch.put(destination, &sets_meta.encode());
    //         }
    //     }
    //
    //     // Add all members to the destination set
    //     for member_str in &members {
    //         let member_key =
    //             self.encode_sets_member_key(destination, version, member_str.as_bytes());
    //         let empty_value = InternalValue::new(DataType::None, &[]).encode();
    //         batch.put_cf(
    //             self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //             &member_key,
    //             &empty_value,
    //         );
    //     }
    //
    //     // Write batch to DB
    //     db.write_opt(batch, &self.default_write_options)?;
    //
    //     // Update statistics
    //     self.update_specific_key_statistics(
    //         DataType::Sets,
    //         &String::from_utf8_lossy(destination).to_string(),
    //         statistic,
    //     )?;
    //
    //     // Set return value
    //     *ret = members.len() as i32;
    //
    //     Ok(())
    // }

    // /// Check if member is a member of the set stored at key
    // pub fn sismember(&self, key: &[u8], member: &[u8], ret: &mut i32) -> Result<()> {
    //     let db = self
    //         .db
    //         .as_ref()
    //         .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
    //
    //     *ret = 0;
    //
    //     // Create read options
    //     let read_options = ReadOptions::default();
    //
    //     // Get the meta value
    //     match db.get_opt(key, &read_options)? {
    //         Some(meta_value) => {
    //             let parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //             // Check if it's the right type
    //             if parsed_meta.data_type() != DataType::Sets {
    //                 return Err(StorageError::InvalidFormat(format!(
    //                     "Wrong type for key: {}",
    //                     String::from_utf8_lossy(key)
    //                 )));
    //             }
    //
    //             // Check if expired
    //             if parsed_meta.is_expired(
    //                 SystemTime::now()
    //                     .duration_since(UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs(),
    //             ) {
    //                 return Ok(());
    //             }
    //
    //             // Get the version
    //             let version = parsed_meta.version();
    //
    //             // Create member key
    //             let member_key = self.encode_sets_member_key(key, version, member);
    //
    //             // Check if member exists
    //             match db.get_cf_opt(
    //                 self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                 &member_key,
    //                 &read_options,
    //             )? {
    //                 Some(_) => {
    //                     *ret = 1;
    //                 }
    //                 None => {
    //                     *ret = 0;
    //                 }
    //             }
    //         }
    //         None => {
    //             // Key not found
    //             *ret = 0;
    //         }
    //     }
    //
    //     Ok(())
    // }

    // /// Move member from the set at source to the set at destination
    // pub fn smove(
    //     &self,
    //     source: &[u8],
    //     destination: &[u8],
    //     member: &[u8],
    //     ret: &mut i32,
    // ) -> Result<()> {
    //     let db = self
    //         .db
    //         .as_ref()
    //         .ok_or_else(|| StorageError::InvalidFormat("DB not initialized".to_string()))?;
    //
    //     *ret = 0;
    //
    //     // Create batch for atomic operations
    //     let mut batch = WriteBatch::default();
    //
    //     // Create locks for both keys
    //     let keys = vec![source.to_vec(), destination.to_vec()];
    //     let _locks = self.lock_mgr.as_ref().multi_lock(&keys);
    //
    //     let mut version = 0;
    //     let mut statistic = 0;
    //
    //     // Check source key
    //     match db.get_opt(source, &self.default_read_options)? {
    //         Some(meta_value) => {
    //             let mut parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //             // Check if it's the right type
    //             if parsed_meta.data_type() != DataType::Sets {
    //                 return Err(StorageError::InvalidFormat(format!(
    //                     "Wrong type for key: {}",
    //                     String::from_utf8_lossy(source)
    //                 )));
    //             }
    //
    //             // Check if expired
    //             if parsed_meta.is_expired(
    //                 SystemTime::now()
    //                     .duration_since(UNIX_EPOCH)
    //                     .unwrap()
    //                     .as_secs(),
    //             ) {
    //                 return Ok(());
    //             }
    //
    //             // Get the version
    //             version = parsed_meta.version();
    //
    //             // Create member key
    //             let member_key = self.encode_sets_member_key(source, version, member);
    //
    //             // Check if member exists in source
    //             match db.get_cf_opt(
    //                 self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                 &member_key,
    //                 &self.default_read_options,
    //             )? {
    //                 Some(_) => {
    //                     // Member exists, remove it from source
    //                     *ret = 1;
    //
    //                     // Update source meta value
    //                     parsed_meta.set_size(parsed_meta.size() - 1);
    //                     batch.put(source, &parsed_meta.encode());
    //
    //                     // Delete member from source
    //                     batch.delete_cf(
    //                         self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                         &member_key,
    //                     );
    //                     statistic += 1;
    //                 }
    //                 None => {
    //                     // Member doesn't exist in source
    //                     return Ok(());
    //                 }
    //             }
    //         }
    //         None => {
    //             // Source key doesn't exist
    //             return Ok(());
    //         }
    //     }
    //
    //     // If member was found in source, add it to destination
    //     if *ret == 1 {
    //         // Check destination key
    //         match db.get_opt(destination, &self.default_read_options)? {
    //             Some(meta_value) => {
    //                 let mut parsed_meta = ParsedInternalValue::new(&meta_value);
    //
    //                 // Check if it's the right type
    //                 if parsed_meta.data_type() != DataType::Sets {
    //                     if parsed_meta.is_expired(
    //                         SystemTime::now()
    //                             .duration_since(UNIX_EPOCH)
    //                             .unwrap()
    //                             .as_secs(),
    //                     ) {
    //                         // Create new set if expired
    //                         let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
    //                         sets_meta.set_size(1);
    //                         version = sets_meta.update_version();
    //                         batch.put(destination, &sets_meta.encode());
    //
    //                         // Add member to destination
    //                         let member_key =
    //                             self.encode_sets_member_key(destination, version, member);
    //                         let empty_value = InternalValue::new(DataType::None, &[]).encode();
    //                         batch.put_cf(
    //                             self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                             &member_key,
    //                             &empty_value,
    //                         );
    //                     } else {
    //                         return Err(StorageError::InvalidFormat(format!(
    //                             "Wrong type for key: {}",
    //                             String::from_utf8_lossy(destination)
    //                         )));
    //                     }
    //                 } else if parsed_meta.is_expired(
    //                     SystemTime::now()
    //                         .duration_since(UNIX_EPOCH)
    //                         .unwrap()
    //                         .as_secs(),
    //                 ) || parsed_meta.size() == 0
    //                 {
    //                     // Initialize meta value if expired or empty
    //                     version = parsed_meta.update_version();
    //                     parsed_meta.set_size(1);
    //                     batch.put(destination, &parsed_meta.encode());
    //
    //                     // Add member to destination
    //                     let member_key = self.encode_sets_member_key(destination, version, member);
    //                     let empty_value = InternalValue::new(DataType::None, &[]).encode();
    //                     batch.put_cf(
    //                         self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                         &member_key,
    //                         &empty_value,
    //                     );
    //                 } else {
    //                     // Check if member already exists in destination
    //                     version = parsed_meta.version();
    //                     let member_key = self.encode_sets_member_key(destination, version, member);
    //
    //                     match db.get_cf_opt(
    //                         self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                         &member_key,
    //                         &self.default_read_options,
    //                     )? {
    //                         Some(_) => {
    //                             // Member already exists, nothing to do
    //                         }
    //                         None => {
    //                             // Add member to destination
    //                             parsed_meta.set_size(parsed_meta.size() + 1);
    //                             batch.put(destination, &parsed_meta.encode());
    //
    //                             let empty_value = InternalValue::new(DataType::None, &[]).encode();
    //                             batch.put_cf(
    //                                 self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                                 &member_key,
    //                                 &empty_value,
    //                             );
    //                         }
    //                     }
    //                 }
    //             }
    //             None => {
    //                 // Create new set
    //                 let mut sets_meta = InternalValue::new(DataType::Sets, &[]);
    //                 sets_meta.set_size(1);
    //                 version = sets_meta.update_version();
    //                 batch.put(destination, &sets_meta.encode());
    //
    //                 // Add member to destination
    //                 let member_key = self.encode_sets_member_key(destination, version, member);
    //                 let empty_value = InternalValue::new(DataType::None, &[]).encode();
    //                 batch.put_cf(
    //                     self.get_handle(crate::redis::ColumnFamilyIndex::SetsDataCF),
    //                     &member_key,
    //                     &empty_value,
    //                 );
    //             }
    //         }
    //     }
    //
    //     // Write batch to DB
    //     db.write_opt(batch, &self.default_write_options)?;
    //
    //     // Update statistics
    //     if statistic > 0 {
    //         self.update_specific_key_statistics(
    //             DataType::Sets,
    //             &String::from_utf8_lossy(source).to_string(),
    //             statistic,
    //         )?;
    //     }
    //
    //     Ok(())
    // }

    /// Returns the members of the set resulting from the difference between the first set and all the successive sets
    pub fn sdiff(&self, keys: &[&[u8]]) -> Result<Vec<String>> {
        if keys.is_empty() {
            return InvalidArgumentSnafu {
                message: "SDIFF requires at least one key".to_string(),
            }
            .fail();
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let mut result = Vec::new();

        // Get the first set's metadata
        let first_key = keys[0];
        let base_meta_key = BaseMetaKey::new(first_key).encode()?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Read meta for first key
        let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;
        let Some(val) = meta_val else {
            // First key not found, return empty result
            return Ok(result);
        };

        // Type check
        self.check_type(&val, DataType::Set)?;

        let set_meta = ParsedSetsMetaValue::new(&val[..])?;
        // Validity check (not expired and count > 0)
        if !set_meta.is_valid() {
            // Return empty result for expired/invalid keys
            return Ok(result);
        }

        let version = set_meta.version();

        // Build prefix: reserve(8) + version(8) + encoded(key)
        let mut prefix =
            bytes::BytesMut::with_capacity(PREFIX_RESERVE_LENGTH + 8 + first_key.len() * 2);
        prefix.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        prefix.put_u64(version);
        encode_user_key(&bytes::Bytes::copy_from_slice(first_key), &mut prefix)?;

        // Iterate through all members of the first set
        let iter = db.iterator_cf_opt(
            &cf_data,
            ReadOptions::default(),
            IteratorMode::From(&prefix, Direction::Forward),
        );

        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            if !raw_key.starts_with(&prefix) {
                break;
            }

            // Extract member from key: member = raw_key[prefix..len - SUFFIX_RESERVE_LENGTH]
            if raw_key.len() >= prefix.len() + SUFFIX_RESERVE_LENGTH {
                let member_slice = &raw_key[prefix.len()..raw_key.len() - SUFFIX_RESERVE_LENGTH];

                // Check if member exists in any other set
                let mut found_in_other = false;
                for &other_key in &keys[1..] {
                    if self.sismember(other_key, member_slice)? {
                        found_in_other = true;
                        break;
                    }
                }

                // Add to result if not found in any other set
                if !found_in_other {
                    result.push(String::from_utf8_lossy(member_slice).to_string());
                }
            }
        }

        Ok(result)
    }

    /// Returns the members of the set resulting from the intersection of all the given sets
    pub fn sinter(&self, keys: &[&[u8]]) -> Result<Vec<String>> {
        if keys.is_empty() {
            return InvalidArgumentSnafu {
                message: "SINTER requires at least one key".to_string(),
            }
            .fail();
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let mut result = Vec::new();

        // If only one key, return all its members (intersection with itself)
        if keys.len() == 1 {
            return self.smembers(keys[0]);
        }

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let _cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Find the smallest set to optimize intersection
        let mut smallest_key = keys[0];
        let mut smallest_size = i32::MAX;

        for &key in keys {
            let base_meta_key = BaseMetaKey::new(key).encode()?;
            let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;

            if meta_val.is_none() {
                // If any set doesn't exist, intersection is empty
                return Ok(result);
            }

            let val = meta_val.unwrap();
            // Type check
            self.check_type(&val, DataType::Set)?;

            let set_meta = ParsedSetsMetaValue::new(&val[..])?;
            if !set_meta.is_valid() {
                // If any set is expired/invalid, intersection is empty
                return Ok(result);
            }

            let count = set_meta.count() as i32;
            if count < smallest_size {
                smallest_size = count;
                smallest_key = key;
            }
        }

        // If smallest set is empty, intersection is empty
        if smallest_size == 0 {
            return Ok(result);
        }

        // Get members of the smallest set and check if they exist in all other sets
        let smallest_members = self.smembers(smallest_key)?;

        for member in smallest_members {
            let member_bytes = member.as_bytes();
            let mut exists_in_all = true;

            // Check if member exists in all other sets
            for &key in keys {
                if key == smallest_key {
                    continue; // Skip the smallest set itself
                }

                if !self.sismember(key, member_bytes)? {
                    exists_in_all = false;
                    break;
                }
            }

            if exists_in_all {
                result.push(member);
            }
        }

        Ok(result)
    }

    /// Returns the members of the set resulting from the union of all the given sets
    pub fn sunion(&self, keys: &[&[u8]]) -> Result<Vec<String>> {
        if keys.is_empty() {
            return InvalidArgumentSnafu {
                message: "SUNION requires at least one key".to_string(),
            }
            .fail();
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;

        // Use HashSet to ensure uniqueness
        let mut union_set = std::collections::HashSet::new();

        for &key in keys {
            let base_meta_key = BaseMetaKey::new(key).encode()?;
            let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;

            if let Some(val) = meta_val {
                // Type check
                self.check_type(&val, DataType::Set)?;

                let set_meta = ParsedSetsMetaValue::new(&val[..])?;
                if set_meta.is_valid() {
                    // Get all members of this set and add to union
                    let members = self.smembers(key)?;
                    for member in members {
                        union_set.insert(member);
                    }
                }
            }
            // If key doesn't exist, treat as empty set (no members to add)
        }

        // Convert HashSet to Vec
        let result: Vec<String> = union_set.into_iter().collect();
        Ok(result)
    }

    /// Store the difference between the first set and all the successive sets into a destination key
    pub fn sdiffstore(&self, destination: &[u8], keys: &[&[u8]]) -> Result<i32> {
        if keys.is_empty() {
            return InvalidArgumentSnafu {
                message: "SDIFFSTORE requires at least one source key".to_string(),
            }
            .fail();
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Calculate the difference
        let diff_members = self.sdiff(keys)?;

        // Clear the destination key first (if it exists)
        let dest_base_meta_key = BaseMetaKey::new(destination).encode()?;
        let mut batch = WriteBatch::default();

        // Delete existing destination set metadata and data
        let meta_val = db
            .get_cf(&cf_meta, &dest_base_meta_key)
            .context(RocksSnafu)?;
        if let Some(val) = meta_val {
            if val.first().copied() == Some(DataType::Set as u8) {
                let set_meta = ParsedSetsMetaValue::new(&val[..])?;
                if set_meta.is_valid() {
                    let version = set_meta.version();

                    // Delete all existing members
                    let mut prefix = bytes::BytesMut::with_capacity(
                        PREFIX_RESERVE_LENGTH + 8 + destination.len() * 2,
                    );
                    prefix.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
                    prefix.put_u64(version);
                    encode_user_key(&bytes::Bytes::copy_from_slice(destination), &mut prefix)?;

                    let iter = db.iterator_cf_opt(
                        &cf_data,
                        ReadOptions::default(),
                        IteratorMode::From(&prefix, Direction::Forward),
                    );

                    for item in iter {
                        let (raw_key, _) = item.context(RocksSnafu)?;
                        if !raw_key.starts_with(&prefix) {
                            break;
                        }
                        batch.delete_cf(&cf_data, &raw_key);
                    }
                }
            }
            // Delete the metadata
            batch.delete_cf(&cf_meta, &dest_base_meta_key);
        }

        // If there are no members in the difference, just clear and return 0
        if diff_members.is_empty() {
            db.write(batch).context(RocksSnafu)?;
            return Ok(0);
        }

        // Add all difference members to the destination set
        let member_refs: Vec<&[u8]> = diff_members.iter().map(|s| s.as_bytes()).collect();

        // Write the batch to clear destination first
        db.write(batch).context(RocksSnafu)?;

        // Now add the new members
        let added = self.sadd(destination, &member_refs)?;

        Ok(added)
    }

    /// Store the intersection of all the given sets into a destination key
    pub fn sinterstore(&self, destination: &[u8], keys: &[&[u8]]) -> Result<i32> {
        if keys.is_empty() {
            return InvalidArgumentSnafu {
                message: "SINTERSTORE requires at least one source key".to_string(),
            }
            .fail();
        }

        // Calculate the intersection
        let inter_members = self.sinter(keys)?;

        // Clear and store the result (reuse the same pattern as sdiffstore)
        self.clear_and_store_set(destination, &inter_members)
    }

    /// Store the union of all the given sets into a destination key
    pub fn sunionstore(&self, destination: &[u8], keys: &[&[u8]]) -> Result<i32> {
        if keys.is_empty() {
            return InvalidArgumentSnafu {
                message: "SUNIONSTORE requires at least one source key".to_string(),
            }
            .fail();
        }

        // Calculate the union
        let union_members = self.sunion(keys)?;

        // Clear and store the result
        self.clear_and_store_set(destination, &union_members)
    }

    /// Helper method to clear a destination set and store new members
    fn clear_and_store_set(&self, destination: &[u8], members: &[String]) -> Result<i32> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Lock the key for atomic read-modify-write
        let key_str = String::from_utf8_lossy(destination).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Clear the destination key first (if it exists)
        let dest_base_meta_key = BaseMetaKey::new(destination).encode()?;
        let mut batch = WriteBatch::default();

        // Delete existing destination set metadata and data
        let meta_val = db
            .get_cf(&cf_meta, &dest_base_meta_key)
            .context(RocksSnafu)?;
        if let Some(val) = meta_val {
            if val.first().copied() == Some(DataType::Set as u8) {
                let set_meta = ParsedSetsMetaValue::new(&val[..])?;
                if set_meta.is_valid() {
                    let version = set_meta.version();

                    // Delete all existing members
                    let mut prefix = bytes::BytesMut::with_capacity(
                        PREFIX_RESERVE_LENGTH + 8 + destination.len() * 2,
                    );
                    prefix.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
                    prefix.put_u64(version);
                    encode_user_key(&bytes::Bytes::copy_from_slice(destination), &mut prefix)?;

                    let iter = db.iterator_cf_opt(
                        &cf_data,
                        ReadOptions::default(),
                        IteratorMode::From(&prefix, Direction::Forward),
                    );

                    for item in iter {
                        let (raw_key, _) = item.context(RocksSnafu)?;
                        if !raw_key.starts_with(&prefix) {
                            break;
                        }
                        batch.delete_cf(&cf_data, &raw_key);
                    }
                }
            }
            // Delete the metadata
            batch.delete_cf(&cf_meta, &dest_base_meta_key);
        }

        // If there are no members, just clear and return 0
        if members.is_empty() {
            db.write(batch).context(RocksSnafu)?;
            return Ok(0);
        }

        // Write the batch to clear destination first
        db.write(batch).context(RocksSnafu)?;

        // Now add the new members
        let member_refs: Vec<&[u8]> = members.iter().map(|s| s.as_bytes()).collect();
        let added = self.sadd(destination, &member_refs)?;

        Ok(added)
    }

    /// Scan set members with cursor-based iteration
    pub fn sscan(
        &self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<(u64, Vec<String>)> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let base_meta_key = BaseMetaKey::new(key).encode()?;

        let cf_meta = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "cf is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::SetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Read meta
        let meta_val = db.get_cf(&cf_meta, &base_meta_key).context(RocksSnafu)?;
        let Some(val) = meta_val else {
            // Key not found, return empty result with cursor 0
            return Ok((0, Vec::new()));
        };

        // Type check
        self.check_type(&val, DataType::Set)?;

        let set_meta = ParsedSetsMetaValue::new(&val[..])?;
        // Validity check (not expired and count > 0)
        if !set_meta.is_valid() {
            // Return empty result with cursor 0 for expired/invalid keys
            return Ok((0, Vec::new()));
        }

        let version = set_meta.version();
        let _set_size = set_meta.count() as usize;

        // Build prefix: reserve(8) + version(8) + encoded(key)
        let mut prefix = bytes::BytesMut::with_capacity(PREFIX_RESERVE_LENGTH + 8 + key.len() * 2);
        prefix.extend_from_slice(&[0u8; PREFIX_RESERVE_LENGTH]);
        prefix.put_u64(version);
        encode_user_key(&bytes::Bytes::copy_from_slice(key), &mut prefix)?;

        // Collect all members first
        let iter = db.iterator_cf_opt(
            &cf_data,
            ReadOptions::default(),
            IteratorMode::From(&prefix, Direction::Forward),
        );
        let mut all_members = Vec::new();
        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            if !raw_key.starts_with(&prefix) {
                break;
            }
            // member = raw_key[prefix..len - SUFFIX_RESERVE_LENGTH]
            if raw_key.len() >= prefix.len() + SUFFIX_RESERVE_LENGTH {
                let member_slice = &raw_key[prefix.len()..raw_key.len() - SUFFIX_RESERVE_LENGTH];
                all_members.push(String::from_utf8_lossy(member_slice).to_string());
            }
        }

        // Sort members for consistent iteration order
        all_members.sort();

        // Apply cursor-based pagination
        let start_index = cursor as usize;
        let default_count = 10; // Default count like Redis
        let scan_count = count.unwrap_or(default_count);

        let mut result_members = Vec::new();
        let mut next_cursor = 0u64;

        if start_index < all_members.len() {
            let end_index = std::cmp::min(start_index + scan_count, all_members.len());

            for member in &all_members[start_index..end_index] {
                // Apply pattern matching if specified
                if let Some(pat) = pattern {
                    if !glob_match(pat, member) {
                        continue;
                    }
                }
                result_members.push(member.clone());
            }

            // Set next cursor
            if end_index < all_members.len() {
                next_cursor = end_index as u64;
            } else {
                next_cursor = 0; // End of iteration
            }
        }

        Ok((next_cursor, result_members))
    }
}

/// Simple glob pattern matching
/// Supports * (match any sequence) and ? (match single character)
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    fn match_recursive(pattern: &[char], text: &[char], p_idx: usize, t_idx: usize) -> bool {
        // End of pattern
        if p_idx >= pattern.len() {
            return t_idx >= text.len();
        }

        // End of text but pattern remains
        if t_idx >= text.len() {
            // Check if remaining pattern is all '*'
            return pattern[p_idx..].iter().all(|&c| c == '*');
        }

        match pattern[p_idx] {
            '*' => {
                // Try matching zero or more characters
                for i in t_idx..=text.len() {
                    if match_recursive(pattern, text, p_idx + 1, i) {
                        return true;
                    }
                }
                false
            }
            '?' => {
                // Match exactly one character
                match_recursive(pattern, text, p_idx + 1, t_idx + 1)
            }
            c => {
                // Exact character match
                if text[t_idx] == c {
                    match_recursive(pattern, text, p_idx + 1, t_idx + 1)
                } else {
                    false
                }
            }
        }
    }

    match_recursive(&pattern_chars, &text_chars, 0, 0)
}

#[cfg(test)]
mod glob_tests {
    use super::glob_match;

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
    }

    #[test]
    fn test_glob_match_wildcard() {
        assert!(glob_match("h*", "hello"));
        assert!(glob_match("h*", "h"));
        assert!(glob_match("*llo", "hello"));
        assert!(glob_match("h*o", "hello"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("h*", "world"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
        assert!(!glob_match("h?llo", "helllo"));
    }

    #[test]
    fn test_glob_match_combined() {
        assert!(glob_match("h*l?o", "hello"));
        assert!(glob_match("h*l?o", "hallo"));
        assert!(glob_match("*l?o", "hello"));
        assert!(!glob_match("h*l?o", "world"));
    }
}
