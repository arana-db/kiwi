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

//! Redis sorted sets operations implementation
//! This module provides sorted set operations for Redis storage

use crate::base_data_value_format::{BaseDataValue, ParsedBaseDataValue};
use crate::base_meta_value_format::{ParsedZSetsMetaValue, ZSetsMetaValue};
use crate::error::Error::RedisErr;
use crate::error::{OptionNoneSnafu, RocksSnafu};
use crate::redis::Redis;
use crate::{BaseMetaKey, ColumnFamilyIndex, DataType, Result};
use kstd::lock_mgr::ScopeRecordLock;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use snafu::OptionExt;
use snafu::ResultExt;
use std::collections::HashSet;

use crate::member_data_key_format::MemberDataKey;
use crate::zset_score_key_format::{ParsedZSetsScoreKey, ScoreMember, ZSetsScoreKey};

impl Redis {
    /// Add one or more members to a sorted set, or update its score if it already exists
    pub fn zadd(&self, key: &[u8], score_members: &[ScoreMember], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Remove duplicate members in score_members
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

        // Get column family handle for data reads
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::ZsetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Acquire lock for the key
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        let mut statistic = 0u32;
        *ret = 0;

        // Get existing zset meta
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        // ZSet exists, update it
        if !base_meta_val.is_empty() {
            // Check type
            self.check_type(&base_meta_val, DataType::ZSet)?;

            // Parse existing meta
            let mut parsed_zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;

            // Get version and validity
            let version;
            let valid;
            if parsed_zset_meta.is_valid() {
                valid = true;
                version = parsed_zset_meta.version();
            } else {
                valid = false;
                version = parsed_zset_meta.initial_meta_value();
            }

            // Prepare batch write
            let mut count = 0u64;
            let mut batch = self.create_batch()?;
            for sm in &filtered_score_members {
                let mut not_found = true;
                let member_key = MemberDataKey::new(key, version, &sm.member).encode()?;
                if valid {
                    // Check if member exists
                    let existing_member_val = db
                        .get_cf_opt(&cf_data, &member_key, &self.read_options)
                        .context(RocksSnafu)?
                        .unwrap_or_else(Vec::new);
                    if !existing_member_val.is_empty() {
                        // Member exists, check score
                        let mut parsed_base_data_val =
                            ParsedBaseDataValue::new(&existing_member_val[..])?;
                        parsed_base_data_val.strip_suffix();
                        not_found = false;
                        match String::from_utf8_lossy(&parsed_base_data_val.user_value()).parse() {
                            Ok(existing_score) => {
                                if (existing_score, sm.score).1.abs() < f64::EPSILON {
                                    // Score is the same, skip
                                    continue;
                                } else {
                                    // Score is different, delete old score key
                                    let old_score_key = ZSetsScoreKey::new(
                                        key,
                                        version,
                                        existing_score,
                                        &sm.member,
                                    )
                                    .encode()?;
                                    batch
                                        .delete(ColumnFamilyIndex::ZsetsScoreCF, &old_score_key)?;
                                    statistic += 1;
                                }
                            }
                            Err(_) => {
                                return Err(RedisErr {
                                    message: "invalid score format".to_string(),
                                    location: Default::default(),
                                });
                            }
                        }
                    }
                }

                // Insert new member and score
                let score_key = ZSetsScoreKey::new(key, version, sm.score, &sm.member).encode()?;
                let member_value = BaseDataValue::new(format!("{}", sm.score));
                let score_value = BaseDataValue::new("");
                batch.put(
                    ColumnFamilyIndex::ZsetsDataCF,
                    &member_key,
                    &member_value.encode(),
                )?;
                batch.put(
                    ColumnFamilyIndex::ZsetsScoreCF,
                    &score_key,
                    &score_value.encode(),
                )?;
                if not_found {
                    count += 1;
                }
                continue;
            }

            // Update meta
            if !parsed_zset_meta.check_modify_count(count) {
                return Err(RedisErr {
                    message: "zset size overflow".to_string(),
                    location: Default::default(),
                });
            }
            parsed_zset_meta.modify_count(count);
            batch.put(
                ColumnFamilyIndex::MetaCF,
                &base_meta_key.encode()?,
                parsed_zset_meta.encoded(),
            )?;
            batch.commit()?;
            *ret = count as i32;
        } else {
            // ZSet does not exist, create new one
            let mut zset_meta = ZSetsMetaValue::new(bytes::Bytes::copy_from_slice(
                &(filtered_score_members.len() as u64).to_le_bytes(),
            ));
            zset_meta.inner.data_type = DataType::ZSet;
            let version = zset_meta.update_version();
            let mut batch = self.create_batch()?;
            // Add meta value
            batch.put(
                ColumnFamilyIndex::MetaCF,
                &base_meta_key.encode()?,
                &zset_meta.encode(),
            )?;
            // Add all members
            for sm in &filtered_score_members {
                let member_key = MemberDataKey::new(key, version, &sm.member).encode()?;
                let score_key = ZSetsScoreKey::new(key, version, sm.score, &sm.member).encode()?;
                let member_value = BaseDataValue::new(format!("{}", sm.score));
                let score_value = BaseDataValue::new("");
                batch.put(
                    ColumnFamilyIndex::ZsetsDataCF,
                    &member_key,
                    &member_value.encode(),
                )?;
                batch.put(
                    ColumnFamilyIndex::ZsetsScoreCF,
                    &score_key,
                    &score_value.encode(),
                )?;
            }
            batch.commit()?;
            *ret = filtered_score_members.len() as i32;
        }

        // Update statistics
        self.update_specific_key_statistics(DataType::ZSet, &key_str, statistic as u64)?;

        Ok(())
    }

    /// Get the number of members in a sorted set
    pub fn zcard(&self, key: &[u8], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        *ret = 0;
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);
        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        *ret = zset_meta.count() as i32;
        Ok(())
    }

    /// Count the number of members in a sorted set with scores within the given values
    pub fn zcount(&self, key: &[u8], min_score: f64, max_score: f64, ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        *ret = 0;
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);
        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let version = zset_meta.version();
        let min_score_key = ZSetsScoreKey::new(key, version, min_score, &[]).encode_seek_key()?;

        let mut count = 0;
        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            // Check key and version match
            if key != score_key.key() || version != score_key.version() {
                break;
            }

            // Check score range
            if score_key.score() > max_score {
                break;
            }

            count += 1;
        }
        *ret = count;

        Ok(())
    }

    /// Increment the score of a member in a sorted set
    pub fn zincrby(
        &self,
        key: &[u8],
        increment: f64,
        member: &[u8],
        ret: &mut Vec<u8>,
    ) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Get column family handle for data reads
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::ZsetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Acquire lock for the key
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        // Get existing zset meta
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        let mut new_score = increment;

        if !base_meta_val.is_empty() {
            // ZSet exists, check if member exists
            self.check_type(&base_meta_val, DataType::ZSet)?;

            let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
            if !zset_meta.is_valid() {
                // ZSet is invalid, treat as if it doesn't exist
                let score_member = ScoreMember::new(new_score, member.to_vec());
                let mut count = 0;
                std::mem::drop(_lock);
                self.zadd(key, &[score_member], &mut count)?;
                *ret = format!("{}", new_score).into_bytes();
                return Ok(());
            }

            let version = zset_meta.version();
            let member_key = MemberDataKey::new(key, version, member).encode()?;

            // Check if member exists
            let existing_member_val = db
                .get_cf_opt(&cf_data, &member_key, &self.read_options)
                .context(RocksSnafu)?
                .unwrap_or_else(Vec::new);

            if !existing_member_val.is_empty() {
                // Member exists, get current score and increment it
                let mut parsed_base_data_val = ParsedBaseDataValue::new(&existing_member_val[..])?;
                parsed_base_data_val.strip_suffix();

                match String::from_utf8_lossy(&parsed_base_data_val.user_value()).parse::<f64>() {
                    Ok(current_score) => {
                        new_score = current_score + increment;

                        // Check for valid float (not NaN or infinite)
                        if new_score.is_nan() || new_score.is_infinite() {
                            return Err(RedisErr {
                                message: "ERR increment would produce NaN or Infinity".to_string(),
                                location: Default::default(),
                            });
                        }

                        // Update the member with new score
                        let mut batch = self.create_batch()?;

                        // Delete old score key
                        let old_score_key =
                            ZSetsScoreKey::new(key, version, current_score, member).encode()?;
                        batch.delete(ColumnFamilyIndex::ZsetsScoreCF, &old_score_key)?;

                        // Add new score key and update member value
                        let new_score_key = crate::zset_score_key_format::ZSetsScoreKey::new(
                            key, version, new_score, member,
                        )
                        .encode()?;
                        let member_value = BaseDataValue::new(format!("{}", new_score));
                        let score_value = BaseDataValue::new("");

                        batch.put(
                            ColumnFamilyIndex::ZsetsDataCF,
                            &member_key,
                            &member_value.encode(),
                        )?;
                        batch.put(
                            ColumnFamilyIndex::ZsetsScoreCF,
                            &new_score_key,
                            &score_value.encode(),
                        )?;

                        batch.commit()?;
                    }
                    Err(_) => {
                        return Err(RedisErr {
                            message: "invalid score format".to_string(),
                            location: Default::default(),
                        });
                    }
                }
            } else {
                // Member doesn't exist, add it with the increment as the score
                let score_member = ScoreMember::new(new_score, member.to_vec());
                let mut count = 0;
                std::mem::drop(_lock);
                self.zadd(key, &[score_member], &mut count)?;
            }
        } else {
            // ZSet doesn't exist, create it with the member and increment as score
            let score_member = ScoreMember::new(new_score, member.to_vec());
            let mut count = 0;
            std::mem::drop(_lock);
            self.zadd(key, &[score_member], &mut count)?;
        }

        *ret = format!("{}", new_score).into_bytes();
        Ok(())
    }

    /// Get the score associated with the given member in a sorted set
    pub fn zscore(&self, key: &[u8], member: &[u8], ret: &mut Option<Vec<u8>>) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::ZsetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        *ret = None;

        // Get existing zset meta
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let version = zset_meta.version();
        let member_key = MemberDataKey::new(key, version, member).encode()?;

        // Check if member exists
        let existing_member_val = db
            .get_cf_opt(&cf_data, &member_key, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if !existing_member_val.is_empty() {
            // Member exists, get its score
            let mut parsed_base_data_val = ParsedBaseDataValue::new(&existing_member_val[..])?;
            parsed_base_data_val.strip_suffix();
            *ret = Some(parsed_base_data_val.user_value().to_vec());
        }

        Ok(())
    }

    /// Scan members and scores in a sorted set
    pub fn zscan(
        &self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<(u64, Vec<(String, String)>)> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        // Get existing zset meta
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok((0, Vec::new()));
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok((0, Vec::new()));
        }

        let version = zset_meta.version();
        let scan_count = count.unwrap_or(10);
        let mut results = Vec::new();
        let mut scanned = 0u64;
        let mut next_cursor = 0u64;

        // Create prefix for this zset
        let prefix = {
            let mut prefix = Vec::with_capacity(key.len() + 9);
            prefix.extend_from_slice(key);
            prefix.push(0);
            prefix.extend_from_slice(&version.to_be_bytes());
            prefix.push(0);
            prefix
        };

        // Start iteration from cursor position
        let start_key = if cursor == 0 {
            prefix.clone()
        } else {
            // For simplicity, we'll start from the beginning and skip to cursor
            // In a production implementation, you'd want to encode the cursor position
            prefix.clone()
        };

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&start_key, Direction::Forward),
        );

        for (i, item) in iter.enumerate() {
            let (raw_key, _) = item.context(RocksSnafu)?;

            // Parse the score key
            let score_key = match ParsedZSetsScoreKey::new(&raw_key) {
                Ok(sk) => sk,
                Err(_) => break, // Invalid key format, stop iteration
            };

            // Check if this key belongs to our zset
            if key != score_key.key() || version != score_key.version() {
                break;
            }

            // Skip items until we reach the cursor position
            if (i as u64) < cursor {
                continue;
            }

            // Extract member and score
            let member = String::from_utf8_lossy(score_key.member()).to_string();
            let score_str = format!("{}", score_key.score());

            // Apply pattern matching if specified
            if let Some(pat) = pattern {
                if !glob_match(&member, pat) {
                    continue;
                }
            }

            results.push((member, score_str));
            scanned += 1;

            if results.len() >= scan_count {
                next_cursor = (i as u64) + 1;
                break;
            }
        }

        // If we've scanned all items, set cursor to 0
        if scanned < scan_count as u64 {
            next_cursor = 0;
        }

        Ok((next_cursor, results))
    }

    /// Get the rank of a member in a sorted set (0-based index)
    pub fn zrank(&self, key: &[u8], member: &[u8], ret: &mut Option<i64>) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::ZsetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        *ret = None;

        // Get existing zset meta
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let version = zset_meta.version();

        // First, check if the member exists and get its score
        let member_key = MemberDataKey::new(key, version, member).encode()?;
        let existing_member_val = db
            .get_cf_opt(&cf_data, &member_key, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if existing_member_val.is_empty() {
            return Ok(());
        }

        // Get the member's score
        let mut parsed_base_data_val = ParsedBaseDataValue::new(&existing_member_val[..])?;
        parsed_base_data_val.strip_suffix();
        let member_score =
            match String::from_utf8_lossy(&parsed_base_data_val.user_value()).parse::<f64>() {
                Ok(s) => s,
                Err(_) => {
                    return Err(RedisErr {
                        message: "invalid score format".to_string(),
                        location: Default::default(),
                    });
                }
            };

        // Iterate through score keys to find the rank
        let min_score_key =
            ZSetsScoreKey::new(key, version, f64::NEG_INFINITY, &[]).encode_seek_key()?;
        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        for (rank, item) in iter.enumerate() {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            // Check key and version match
            if key != score_key.key() || version != score_key.version() {
                break;
            }

            // Check if this is our member
            if score_key.score() == member_score && score_key.member() == member {
                *ret = Some(rank as i64);
                return Ok(());
            }
        }

        Ok(())
    }

    /// Get the rank of a member in a sorted set, with scores ordered from high to low (0-based index)
    pub fn zrevrank(&self, key: &[u8], member: &[u8], ret: &mut Option<i64>) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::ZsetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        *ret = None;

        // Get existing zset meta
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let version = zset_meta.version();

        // First, check if the member exists and get its score
        let member_key = MemberDataKey::new(key, version, member).encode()?;
        let existing_member_val = db
            .get_cf_opt(&cf_data, &member_key, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if existing_member_val.is_empty() {
            return Ok(());
        }

        // Get the member's score
        let mut parsed_base_data_val = ParsedBaseDataValue::new(&existing_member_val[..])?;
        parsed_base_data_val.strip_suffix();
        let member_score =
            match String::from_utf8_lossy(&parsed_base_data_val.user_value()).parse::<f64>() {
                Ok(s) => s,
                Err(_) => {
                    return Err(RedisErr {
                        message: "invalid score format".to_string(),
                        location: Default::default(),
                    });
                }
            };

        // Iterate through score keys in reverse order to find the rank
        let max_score_key =
            ZSetsScoreKey::new(key, version, f64::INFINITY, &[]).encode_seek_key()?;
        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&max_score_key, Direction::Reverse),
        );

        for (rank, item) in iter.enumerate() {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            // Check key and version match
            if key != score_key.key() || version != score_key.version() {
                break;
            }

            // Check if this is our member
            if score_key.score() == member_score && score_key.member() == member {
                *ret = Some(rank as i64);
                return Ok(());
            }
        }

        Ok(())
    }

    /// Remove one or more members from a sorted set
    pub fn zrem(&self, key: &[u8], members: &[Vec<u8>], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Get column family handle for data reads
        let cf_data =
            self.get_cf_handle(ColumnFamilyIndex::ZsetsDataCF)
                .context(OptionNoneSnafu {
                    message: "cf data is not initialized".to_string(),
                })?;

        // Acquire lock for the key
        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        *ret = 0;

        // Get existing zset meta
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let mut zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let version = zset_meta.version();
        let mut batch = self.create_batch()?;
        let mut deleted_count = 0u64;

        for member in members {
            let member_key = MemberDataKey::new(key, version, member).encode()?;

            // Check if member exists
            let existing_member_val = db
                .get_cf_opt(&cf_data, &member_key, &self.read_options)
                .context(RocksSnafu)?
                .unwrap_or_else(Vec::new);

            if !existing_member_val.is_empty() {
                // Get the member's score
                let mut parsed_base_data_val = ParsedBaseDataValue::new(&existing_member_val[..])?;
                parsed_base_data_val.strip_suffix();

                match String::from_utf8_lossy(&parsed_base_data_val.user_value()).parse::<f64>() {
                    Ok(score) => {
                        // Delete score key
                        let score_key = ZSetsScoreKey::new(key, version, score, member).encode()?;
                        batch.delete(ColumnFamilyIndex::ZsetsScoreCF, &score_key)?;

                        // Delete member key
                        batch.delete(ColumnFamilyIndex::ZsetsDataCF, &member_key)?;

                        deleted_count += 1;
                    }
                    Err(_) => {
                        return Err(RedisErr {
                            message: "invalid score format".to_string(),
                            location: Default::default(),
                        });
                    }
                }
            }
        }

        if deleted_count > 0 {
            // Update meta
            let new_count = zset_meta.count() - deleted_count;
            if new_count == 0 {
                // Remove the entire zset if no members left
                batch.delete(ColumnFamilyIndex::MetaCF, &base_meta_key.encode()?)?;
            } else {
                zset_meta.set_count(new_count);
                batch.put(
                    ColumnFamilyIndex::MetaCF,
                    &base_meta_key.encode()?,
                    zset_meta.encoded(),
                )?;
            }
            batch.commit()?;

            // Update statistics
            self.update_specific_key_statistics(DataType::ZSet, &key_str, deleted_count)?;
        }

        *ret = deleted_count as i32;
        Ok(())
    }

    /// Count the members in a sorted set within the given lexicographical range
    pub fn zlexcount(&self, key: &[u8], min: &[u8], max: &[u8], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        *ret = 0;
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);
        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        // Parse min and max range specifiers
        let (min_member, min_exclusive) = parse_lex_range(min)?;
        let (max_member, max_exclusive) = parse_lex_range(max)?;

        let version = zset_meta.version();
        let min_score_key =
            ZSetsScoreKey::new(key, version, f64::NEG_INFINITY, &[]).encode_seek_key()?;

        let mut count = 0;
        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            // Check key and version match
            if key != score_key.key() || version != score_key.version() {
                break;
            }

            let member = score_key.member();

            // Check if we've passed the max boundary - break early for optimization
            if let Some(ref max_m) = max_member {
                if member > max_m.as_slice() {
                    break;
                }
            }

            // Check if member is within the lexicographical range
            if is_in_lex_range(
                member,
                &min_member,
                min_exclusive,
                &max_member,
                max_exclusive,
            ) {
                count += 1;
            }
        }
        *ret = count;

        Ok(())
    }

    /// Return a range of members in a sorted set, by index
    pub fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
        ret: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        *ret = Vec::new();

        // Get existing zset meta
        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let count = zset_meta.count() as i64;
        if count == 0 {
            return Ok(());
        }

        // Convert negative indices to positive
        let start_idx = if start < 0 {
            (count + start).max(0)
        } else {
            start
        };

        let stop_idx = if stop < 0 {
            (count + stop).max(0)
        } else {
            stop
        };

        // If start > stop or start >= count, return empty
        if start_idx > stop_idx || start_idx >= count {
            return Ok(());
        }

        // Clamp stop to valid range
        let stop_idx = stop_idx.min(count - 1);

        let version = zset_meta.version();
        let min_score_key =
            ZSetsScoreKey::new(key, version, f64::NEG_INFINITY, &[]).encode_seek_key()?;

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        let mut current_idx = 0i64;
        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            // Check key and version match
            if key != score_key.key() || version != score_key.version() {
                break;
            }

            // Check if we're in the range
            if current_idx >= start_idx && current_idx <= stop_idx {
                ret.push(score_key.member().to_vec());
                if with_scores {
                    ret.push(format!("{}", score_key.score()).into_bytes());
                }
            }

            current_idx += 1;

            // Stop if we've passed the stop index
            if current_idx > stop_idx {
                break;
            }
        }

        Ok(())
    }

    /// Compute the intersection of multiple sorted sets and store the result
    pub fn zinterstore(
        &self,
        destination: &[u8],
        keys: &[Vec<u8>],
        weights: &[f64],
        aggregate: &str,
        ret: &mut i32,
    ) -> Result<()> {
        *ret = self.zset_store_operation(destination, keys, weights, aggregate, true)?;
        Ok(())
    }

    /// Compute the union of multiple sorted sets and store the result
    pub fn zunionstore(
        &self,
        destination: &[u8],
        keys: &[Vec<u8>],
        weights: &[f64],
        aggregate: &str,
        ret: &mut i32,
    ) -> Result<()> {
        *ret = self.zset_store_operation(destination, keys, weights, aggregate, false)?;
        Ok(())
    }

    /// Internal helper for ZINTERSTORE and ZUNIONSTORE
    fn zset_store_operation(
        &self,
        destination: &[u8],
        keys: &[Vec<u8>],
        weights: &[f64],
        aggregate: &str,
        is_inter: bool,
    ) -> Result<i32> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        if keys.is_empty() {
            return Ok(0);
        }

        // Validate weights length
        if !weights.is_empty() && weights.len() != keys.len() {
            return Err(RedisErr {
                message: "ERR syntax error".to_string(),
                location: Default::default(),
            });
        }

        // Use default weights if not provided
        let default_weights: Vec<f64> = vec![1.0; keys.len()];
        let weights = if weights.is_empty() {
            &default_weights
        } else {
            weights
        };

        // Collect all members and their scores from each zset
        use std::collections::HashMap;
        let mut member_scores: HashMap<Vec<u8>, Vec<Option<f64>>> = HashMap::new();

        for (idx, key) in keys.iter().enumerate() {
            let base_meta_key = BaseMetaKey::new(key);
            let base_meta_val = db
                .get_opt(&base_meta_key.encode()?, &self.read_options)
                .context(RocksSnafu)?
                .unwrap_or_else(Vec::new);

            if base_meta_val.is_empty() {
                // Key doesn't exist
                if is_inter {
                    // For intersection, if any key is missing, result is empty
                    // Delete destination by setting count to 0 (handled below)
                    return Ok(0);
                }
                // For union, mark all members from this key as None
                continue;
            }

            // Check type
            self.check_type(&base_meta_val, DataType::ZSet)?;

            let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
            if !zset_meta.is_valid() {
                if is_inter {
                    // For intersection, if any key is invalid, result is empty
                    return Ok(0);
                }
                continue;
            }

            let version = zset_meta.version();
            let min_score_key =
                ZSetsScoreKey::new(key, version, f64::NEG_INFINITY, &[]).encode_seek_key()?;

            let iter = db.iterator_cf_opt(
                &cf_score,
                ReadOptions::default(),
                IteratorMode::From(&min_score_key, Direction::Forward),
            );

            for item in iter {
                let (raw_key, _) = item.context(RocksSnafu)?;
                let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

                if key.as_slice() != score_key.key() || version != score_key.version() {
                    break;
                }

                let member = score_key.member().to_vec();
                let score = score_key.score() * weights[idx];

                let entry = member_scores
                    .entry(member)
                    .or_insert_with(|| vec![None; keys.len()]);
                entry[idx] = Some(score);
            }
        }

        // Filter members based on operation type and compute final scores
        let mut result_members: Vec<ScoreMember> = Vec::new();

        for (member, scores) in member_scores {
            if is_inter {
                // For intersection, all keys must have the member
                if scores.iter().all(|s| s.is_some()) {
                    let final_score = aggregate_scores(&scores, aggregate)?;
                    result_members.push(ScoreMember::new(final_score, member));
                }
            } else {
                // For union, at least one key must have the member
                if scores.iter().any(|s| s.is_some()) {
                    let final_score = aggregate_scores(&scores, aggregate)?;
                    result_members.push(ScoreMember::new(final_score, member));
                }
            }
        }

        // Store the result in destination
        {
            let dest_str = String::from_utf8_lossy(destination).to_string();
            let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &dest_str);

            // Get destination meta to check if it exists
            let dest_meta_key = BaseMetaKey::new(destination);
            let dest_meta_val = db
                .get_opt(&dest_meta_key.encode()?, &self.read_options)
                .context(RocksSnafu)?
                .unwrap_or_else(Vec::new);

            // Delete destination if it exists
            if !dest_meta_val.is_empty() {
                self.check_type(&dest_meta_val, DataType::ZSet)?;
                let dest_meta = ParsedZSetsMetaValue::new(&dest_meta_val[..])?;
                if dest_meta.is_valid() {
                    // Delete all members from destination
                    let dest_version = dest_meta.version();

                    let mut batch = self.create_batch()?;

                    // Delete all score keys
                    let min_score_key =
                        ZSetsScoreKey::new(destination, dest_version, f64::NEG_INFINITY, &[])
                            .encode_seek_key()?;
                    let iter = db.iterator_cf_opt(
                        &cf_score,
                        ReadOptions::default(),
                        IteratorMode::From(&min_score_key, Direction::Forward),
                    );

                    for item in iter {
                        let (raw_key, _) = item.context(RocksSnafu)?;
                        let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

                        if destination != score_key.key() || dest_version != score_key.version() {
                            break;
                        }

                        // Delete score key and member key
                        batch.delete(ColumnFamilyIndex::ZsetsScoreCF, &raw_key)?;
                        let member_key =
                            MemberDataKey::new(destination, dest_version, score_key.member())
                                .encode()?;
                        batch.delete(ColumnFamilyIndex::ZsetsDataCF, &member_key)?;
                    }

                    // Delete meta key
                    batch.delete(ColumnFamilyIndex::MetaCF, &dest_meta_key.encode()?)?;
                    batch.commit()?;
                }
            }
            // Lock is released here when _lock goes out of scope
        }

        // Add all result members (lock is released, so zadd can acquire it)
        if !result_members.is_empty() {
            let mut add_count = 0;
            self.zadd(destination, &result_members, &mut add_count)?;
            Ok(result_members.len() as i32)
        } else {
            Ok(0)
        }
    }

    /// Return a range of members in a sorted set, by lexicographical range
    pub fn zrangebylex(
        &self,
        key: &[u8],
        min: &[u8],
        max: &[u8],
        offset: Option<i64>,
        count: Option<i64>,
        ret: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        *ret = Vec::new();

        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        // Parse min and max range specifiers
        let (min_member, min_exclusive) = parse_lex_range(min)?;
        let (max_member, max_exclusive) = parse_lex_range(max)?;

        let version = zset_meta.version();
        let min_score_key =
            ZSetsScoreKey::new(key, version, f64::NEG_INFINITY, &[]).encode_seek_key()?;

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        let offset = offset.unwrap_or(0).max(0);
        let count = count.unwrap_or(i64::MAX);
        let mut skipped = 0i64;
        let mut collected = 0i64;

        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            if key != score_key.key() || version != score_key.version() {
                break;
            }

            let member = score_key.member();

            // Check if we've passed the max boundary
            if let Some(ref max_m) = max_member {
                if member > max_m.as_slice() {
                    break;
                }
            }

            if is_in_lex_range(
                member,
                &min_member,
                min_exclusive,
                &max_member,
                max_exclusive,
            ) {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if collected >= count {
                    break;
                }

                ret.push(member.to_vec());
                collected += 1;
            }
        }

        Ok(())
    }

    /// Return a range of members in a sorted set, by score
    pub fn zrangebyscore(
        &self,
        key: &[u8],
        min_score: f64,
        max_score: f64,
        with_scores: bool,
        offset: Option<i64>,
        count: Option<i64>,
        ret: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        *ret = Vec::new();

        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let version = zset_meta.version();
        let min_score_key = ZSetsScoreKey::new(key, version, min_score, &[]).encode_seek_key()?;

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        let offset = offset.unwrap_or(0).max(0);
        let count = count.unwrap_or(i64::MAX);
        let mut skipped = 0i64;
        let mut collected = 0i64;

        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            if key != score_key.key() || version != score_key.version() {
                break;
            }

            let score = score_key.score();
            if score > max_score {
                break;
            }

            if score >= min_score {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if collected >= count {
                    break;
                }

                ret.push(score_key.member().to_vec());
                if with_scores {
                    ret.push(format!("{}", score).into_bytes());
                }
                collected += 1;
            }
        }

        Ok(())
    }

    /// Remove all members in a sorted set within the given lexicographical range
    pub fn zremrangebylex(&self, key: &[u8], min: &[u8], max: &[u8], ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Get column family handle for score iterations
        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        *ret = 0;

        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let mut zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let (min_member, min_exclusive) = parse_lex_range(min)?;
        let (max_member, max_exclusive) = parse_lex_range(max)?;

        let version = zset_meta.version();
        let min_score_key =
            ZSetsScoreKey::new(key, version, f64::NEG_INFINITY, &[]).encode_seek_key()?;

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        let mut to_delete = Vec::new();
        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            if key != score_key.key() || version != score_key.version() {
                break;
            }

            let member = score_key.member();

            if let Some(ref max_m) = max_member {
                if member > max_m.as_slice() {
                    break;
                }
            }

            if is_in_lex_range(
                member,
                &min_member,
                min_exclusive,
                &max_member,
                max_exclusive,
            ) {
                to_delete.push((member.to_vec(), score_key.score()));
            }
        }

        if to_delete.is_empty() {
            return Ok(());
        }

        let mut batch = self.create_batch()?;
        for (member, score) in &to_delete {
            let member_key = MemberDataKey::new(key, version, member).encode()?;
            let score_key = ZSetsScoreKey::new(key, version, *score, member).encode()?;
            batch.delete(ColumnFamilyIndex::ZsetsDataCF, &member_key)?;
            batch.delete(ColumnFamilyIndex::ZsetsScoreCF, &score_key)?;
        }

        let deleted_count = to_delete.len() as u64;
        let new_count = zset_meta.count().saturating_sub(deleted_count);

        if new_count == 0 {
            batch.delete(ColumnFamilyIndex::MetaCF, &base_meta_key.encode()?)?;
        } else {
            zset_meta.set_count(new_count);
            batch.put(
                ColumnFamilyIndex::MetaCF,
                &base_meta_key.encode()?,
                zset_meta.encoded(),
            )?;
        }

        batch.commit()?;
        *ret = deleted_count as i32;

        self.update_specific_key_statistics(DataType::ZSet, &key_str, deleted_count)?;

        Ok(())
    }

    /// Remove all members in a sorted set within the given indexes
    pub fn zremrangebyrank(&self, key: &[u8], start: i64, stop: i64, ret: &mut i32) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        *ret = 0;

        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let mut zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let count = zset_meta.count() as i64;
        if count == 0 {
            return Ok(());
        }

        let start_idx = if start < 0 {
            (count + start).max(0)
        } else {
            start
        };

        let stop_idx = if stop < 0 {
            (count + stop).max(0)
        } else {
            stop
        };

        if start_idx > stop_idx || start_idx >= count {
            return Ok(());
        }

        let stop_idx = stop_idx.min(count - 1);

        let version = zset_meta.version();
        let min_score_key =
            ZSetsScoreKey::new(key, version, f64::NEG_INFINITY, &[]).encode_seek_key()?;

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        let mut to_delete = Vec::new();
        let mut current_idx = 0i64;

        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            if key != score_key.key() || version != score_key.version() {
                break;
            }

            if current_idx >= start_idx && current_idx <= stop_idx {
                to_delete.push((score_key.member().to_vec(), score_key.score()));
            }

            current_idx += 1;

            if current_idx > stop_idx {
                break;
            }
        }

        if to_delete.is_empty() {
            return Ok(());
        }

        let mut batch = self.create_batch()?;
        for (member, score) in &to_delete {
            let member_key = MemberDataKey::new(key, version, member).encode()?;
            let score_key = ZSetsScoreKey::new(key, version, *score, member).encode()?;
            batch.delete(ColumnFamilyIndex::ZsetsDataCF, &member_key)?;
            batch.delete(ColumnFamilyIndex::ZsetsScoreCF, &score_key)?;
        }

        let deleted_count = to_delete.len() as u64;
        let new_count = zset_meta.count().saturating_sub(deleted_count);

        if new_count == 0 {
            batch.delete(ColumnFamilyIndex::MetaCF, &base_meta_key.encode()?)?;
        } else {
            zset_meta.set_count(new_count);
            batch.put(
                ColumnFamilyIndex::MetaCF,
                &base_meta_key.encode()?,
                zset_meta.encoded(),
            )?;
        }

        batch.commit()?;
        *ret = deleted_count as i32;

        self.update_specific_key_statistics(DataType::ZSet, &key_str, deleted_count)?;

        Ok(())
    }

    /// Remove all members in a sorted set within the given scores
    pub fn zremrangebyscore(
        &self,
        key: &[u8],
        min_score: f64,
        max_score: f64,
        ret: &mut i32,
    ) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        // Get column family handle for score iterations
        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        let key_str = String::from_utf8_lossy(key).to_string();
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &key_str);

        *ret = 0;

        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let mut zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let version = zset_meta.version();
        let min_score_key = ZSetsScoreKey::new(key, version, min_score, &[]).encode_seek_key()?;

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&min_score_key, Direction::Forward),
        );

        let mut to_delete = Vec::new();
        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            if key != score_key.key() || version != score_key.version() {
                break;
            }

            let score = score_key.score();
            if score > max_score {
                break;
            }

            if score >= min_score {
                to_delete.push((score_key.member().to_vec(), score));
            }
        }

        if to_delete.is_empty() {
            return Ok(());
        }

        let mut batch = self.create_batch()?;
        for (member, score) in &to_delete {
            let member_key = MemberDataKey::new(key, version, member).encode()?;
            let score_key = ZSetsScoreKey::new(key, version, *score, member).encode()?;
            batch.delete(ColumnFamilyIndex::ZsetsDataCF, &member_key)?;
            batch.delete(ColumnFamilyIndex::ZsetsScoreCF, &score_key)?;
        }

        let deleted_count = to_delete.len() as u64;
        let new_count = zset_meta.count().saturating_sub(deleted_count);

        if new_count == 0 {
            batch.delete(ColumnFamilyIndex::MetaCF, &base_meta_key.encode()?)?;
        } else {
            zset_meta.set_count(new_count);
            batch.put(
                ColumnFamilyIndex::MetaCF,
                &base_meta_key.encode()?,
                zset_meta.encoded(),
            )?;
        }

        batch.commit()?;
        *ret = deleted_count as i32;

        self.update_specific_key_statistics(DataType::ZSet, &key_str, deleted_count)?;

        Ok(())
    }

    /// Return a range of members in a sorted set, by index, with scores ordered from high to low
    pub fn zrevrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
        ret: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        *ret = Vec::new();

        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let count = zset_meta.count() as i64;
        if count == 0 {
            return Ok(());
        }

        let start_idx = if start < 0 {
            (count + start).max(0)
        } else {
            start
        };

        let stop_idx = if stop < 0 {
            (count + stop).max(0)
        } else {
            stop
        };

        if start_idx > stop_idx || start_idx >= count {
            return Ok(());
        }

        let stop_idx = stop_idx.min(count - 1);

        let version = zset_meta.version();
        let max_score_key =
            ZSetsScoreKey::new(key, version, f64::INFINITY, &[]).encode_seek_key()?;

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&max_score_key, Direction::Reverse),
        );

        let mut current_idx = 0i64;
        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            if key != score_key.key() || version != score_key.version() {
                break;
            }

            if current_idx >= start_idx && current_idx <= stop_idx {
                ret.push(score_key.member().to_vec());
                if with_scores {
                    ret.push(format!("{}", score_key.score()).into_bytes());
                }
            }

            current_idx += 1;

            if current_idx > stop_idx {
                break;
            }
        }

        Ok(())
    }

    /// Return a range of members in a sorted set, by score, with scores ordered from high to low
    pub fn zrevrangebyscore(
        &self,
        key: &[u8],
        max_score: f64,
        min_score: f64,
        with_scores: bool,
        offset: Option<i64>,
        count: Option<i64>,
        ret: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;

        let cf_score = self
            .get_cf_handle(ColumnFamilyIndex::ZsetsScoreCF)
            .context(OptionNoneSnafu {
                message: "cf score is not initialized".to_string(),
            })?;

        *ret = Vec::new();

        let base_meta_key = BaseMetaKey::new(key);
        let base_meta_val = db
            .get_opt(&base_meta_key.encode()?, &self.read_options)
            .context(RocksSnafu)?
            .unwrap_or_else(Vec::new);

        if base_meta_val.is_empty() {
            return Ok(());
        }

        self.check_type(&base_meta_val, DataType::ZSet)?;

        let zset_meta = ParsedZSetsMetaValue::new(&base_meta_val[..])?;
        if !zset_meta.is_valid() {
            return Ok(());
        }

        let version = zset_meta.version();
        // Start from a position slightly above max_score to ensure we include max_score
        let start_key = ZSetsScoreKey::new(key, version, f64::INFINITY, &[]).encode_seek_key()?;

        let iter = db.iterator_cf_opt(
            &cf_score,
            ReadOptions::default(),
            IteratorMode::From(&start_key, Direction::Reverse),
        );

        let offset = offset.unwrap_or(0).max(0);
        let count = count.unwrap_or(i64::MAX);
        let mut skipped = 0i64;
        let mut collected = 0i64;

        for item in iter {
            let (raw_key, _) = item.context(RocksSnafu)?;
            let score_key = ParsedZSetsScoreKey::new(&raw_key)?;

            if key != score_key.key() || version != score_key.version() {
                break;
            }

            let score = score_key.score();
            if score < min_score {
                break;
            }

            if score >= min_score && score <= max_score {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if collected >= count {
                    break;
                }

                ret.push(score_key.member().to_vec());
                if with_scores {
                    ret.push(format!("{}", score).into_bytes());
                }
                collected += 1;
            }
        }

        Ok(())
    }
}

/// Aggregate scores based on the specified method
fn aggregate_scores(scores: &[Option<f64>], aggregate: &str) -> Result<f64> {
    match aggregate.to_uppercase().as_str() {
        "SUM" => {
            let sum: f64 = scores.iter().filter_map(|&s| s).sum();
            Ok(sum)
        }
        "MIN" => {
            let min = scores
                .iter()
                .filter_map(|&s| s)
                .fold(f64::INFINITY, f64::min);
            if min.is_infinite() { Ok(0.0) } else { Ok(min) }
        }
        "MAX" => {
            let max = scores
                .iter()
                .filter_map(|&s| s)
                .fold(f64::NEG_INFINITY, f64::max);
            if max.is_infinite() { Ok(0.0) } else { Ok(max) }
        }
        _ => Err(RedisErr {
            message: "ERR syntax error".to_string(),
            location: Default::default(),
        }),
    }
}

/// Simple glob pattern matching for ZSCAN
fn glob_match(text: &str, pattern: &str) -> bool {
    let mut text_chars = text.chars().peekable();
    let mut pattern_chars = pattern.chars().peekable();

    loop {
        match (text_chars.peek(), pattern_chars.peek()) {
            (None, None) => return true,
            (Some(_), None) => return false,
            (None, Some('*')) => {
                pattern_chars.next();
                continue;
            }
            (None, Some(_)) => return false,
            (Some(_t), Some('*')) => {
                pattern_chars.next();
                if pattern_chars.peek().is_none() {
                    return true;
                }
                // Try matching the rest of the pattern at each position
                let remaining_text: String = text_chars.collect();
                let remaining_pattern: String = pattern_chars.collect();
                for i in 0..=remaining_text.len() {
                    if glob_match(&remaining_text[i..], &remaining_pattern) {
                        return true;
                    }
                }
                return false;
            }
            (Some(_t), Some('?')) => {
                text_chars.next();
                pattern_chars.next();
            }
            (Some(t), Some(p)) => {
                if t == p {
                    text_chars.next();
                    pattern_chars.next();
                } else {
                    return false;
                }
            }
        }
    }
}

/// Parse lexicographical range specifier
/// Returns (member, is_exclusive)
/// - "-" means negative infinity (None, false)
/// - "+" means positive infinity (None, false)
/// - "[member" means inclusive (Some(member), false)
/// - "(member" means exclusive (Some(member), true)
fn parse_lex_range(range: &[u8]) -> Result<(Option<Vec<u8>>, bool)> {
    if range.is_empty() {
        return Err(RedisErr {
            message: "ERR min or max not valid string range item".to_string(),
            location: Default::default(),
        });
    }

    match range[0] {
        b'-' if range.len() == 1 => Ok((None, false)), // negative infinity
        b'+' if range.len() == 1 => Ok((None, false)), // positive infinity
        b'[' if range.len() > 1 => Ok((Some(range[1..].to_vec()), false)), // inclusive
        b'(' if range.len() > 1 => Ok((Some(range[1..].to_vec()), true)), // exclusive
        _ => Err(RedisErr {
            message: "ERR min or max not valid string range item".to_string(),
            location: Default::default(),
        }),
    }
}

/// Check if a member is within the lexicographical range
fn is_in_lex_range(
    member: &[u8],
    min_member: &Option<Vec<u8>>,
    min_exclusive: bool,
    max_member: &Option<Vec<u8>>,
    max_exclusive: bool,
) -> bool {
    // Check min boundary
    if let Some(ref min) = min_member {
        match member.cmp(min.as_slice()) {
            std::cmp::Ordering::Less => return false,
            std::cmp::Ordering::Equal if min_exclusive => return false,
            _ => {}
        }
    }

    // Check max boundary
    if let Some(ref max) = max_member {
        match member.cmp(max.as_slice()) {
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal if max_exclusive => return false,
            _ => {}
        }
    }

    true
}
