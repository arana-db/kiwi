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

//! Cursor-based keyspace iteration backing the Redis `SCAN` command.
//!
//! A `SCAN` cursor is a single `u64`. Kiwi shards the keyspace across several
//! independent RocksDB instances, so the cursor packs two fields: the high 8
//! bits select the instance and the low 56 bits are a raw-entry offset into
//! that instance's `MetaCF`. Instances are scanned in a fixed order, so a full
//! sweep visits every live key exactly once when the keyspace is not mutated
//! concurrently — the same coverage guarantee the existing HSCAN/SSCAN/ZSCAN
//! cursors provide.

use snafu::{OptionExt, ResultExt};

use crate::{
    ColumnFamilyIndex, DataType, Redis, Result,
    error::{InvalidFormatSnafu, OptionNoneSnafu, RocksSnafu},
    format_base_key::ParsedBaseKey,
    format_base_meta_value::ParsedBaseMetaValue,
    format_list_meta_value::ParsedListsMetaValue,
    format_strings_value::ParsedStringsValue,
    redis_sets::glob_match_bytes,
    storage::Storage,
};

/// Bits of the cursor reserved for the per-instance raw offset. The remaining
/// high bits select the instance, which comfortably covers the handful of
/// RocksDB instances a deployment uses.
const INSTANCE_SHIFT: u32 = 56;
const OFFSET_MASK: u64 = (1u64 << INSTANCE_SHIFT) - 1;

/// One instance's contribution to a single `SCAN` step.
struct ScanPage {
    /// Live user keys matching the type and pattern filters.
    keys: Vec<Vec<u8>>,
    /// Raw `MetaCF` entries consumed this step (matched or not); advances the cursor.
    scanned: u64,
    /// Whether this instance's `MetaCF` was fully consumed.
    exhausted: bool,
}

/// Map a `SCAN TYPE` argument to a stored data type. An unrecognized type maps
/// to [`DataType::None`], which no stored key ever has, so the filter simply
/// matches nothing (as Redis does for an unknown `TYPE`).
fn parse_scan_type(name: &[u8]) -> DataType {
    match name.to_ascii_lowercase().as_slice() {
        b"string" => DataType::String,
        b"hash" => DataType::Hash,
        b"set" => DataType::Set,
        b"list" => DataType::List,
        b"zset" => DataType::ZSet,
        _ => DataType::None,
    }
}

impl Redis {
    /// Scan up to `limit` raw `MetaCF` entries starting `offset` entries in,
    /// returning the live keys among them that pass the type and pattern
    /// filters, how many raw entries were consumed, and whether the instance
    /// was fully drained.
    fn scan_meta(
        &self,
        offset: u64,
        limit: usize,
        type_filter: Option<DataType>,
        pattern: &[u8],
    ) -> Result<ScanPage> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let meta_cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "MetaCF is not initialized".to_string(),
            })?;

        let mut keys = Vec::new();
        let mut scanned = 0u64;
        let mut exhausted = true;

        let mut iter = db
            .iterator_cf(&meta_cf, rocksdb::IteratorMode::Start)
            .skip(offset as usize);

        loop {
            if scanned >= limit as u64 {
                // Stopped on the work budget, not the end of the instance.
                exhausted = false;
                break;
            }
            let Some(item) = iter.next() else {
                break;
            };
            let (key_bytes, value_bytes) = item.context(RocksSnafu)?;
            scanned += 1;

            let base_key = ParsedBaseKey::new(&key_bytes[..])?;
            let type_byte = value_bytes.first().copied().context(InvalidFormatSnafu {
                message: "empty MetaCF value".to_string(),
            })?;
            let data_type = DataType::try_from(type_byte)?;
            let is_live = match data_type {
                DataType::String => !ParsedStringsValue::new(&value_bytes[..])?.is_stale(),
                DataType::List => {
                    let meta = ParsedListsMetaValue::new(&value_bytes[..])?;
                    !meta.is_stale() && meta.count() > 0
                }
                DataType::Hash | DataType::Set | DataType::ZSet => {
                    let meta = ParsedBaseMetaValue::new(&value_bytes[..])?;
                    !meta.is_stale() && meta.count() > 0
                }
                DataType::None | DataType::All => {
                    return InvalidFormatSnafu {
                        message: format!("invalid MetaCF data type: {type_byte}"),
                    }
                    .fail();
                }
            };

            if !is_live {
                continue;
            }
            if let Some(wanted) = type_filter {
                if data_type != wanted {
                    continue;
                }
            }
            if pattern != b"*" && !glob_match_bytes(pattern, base_key.key()) {
                continue;
            }
            keys.push(base_key.key().to_vec());
        }

        Ok(ScanPage {
            keys,
            scanned,
            exhausted,
        })
    }
}

impl Storage {
    /// Incrementally iterate the keyspace for the Redis `SCAN` command.
    ///
    /// `cursor` is `0` to start (and is `0` again once iteration completes).
    /// `count` bounds the number of `MetaCF` entries examined per call (a hint,
    /// as in Redis). `type_filter`, when set, restricts results to keys of that
    /// type name; `pattern` is a glob applied to the key (`b"*"` matches all).
    /// Returns the next cursor and the keys found in this step.
    pub fn scan(
        &self,
        cursor: u64,
        count: usize,
        type_filter: Option<&[u8]>,
        pattern: &[u8],
    ) -> Result<(u64, Vec<Vec<u8>>)> {
        let type_filter = type_filter.map(parse_scan_type);
        let mut remaining = count.max(1);
        let mut inst_idx = (cursor >> INSTANCE_SHIFT) as usize;
        let mut offset = cursor & OFFSET_MASK;
        let mut out = Vec::new();

        while inst_idx < self.insts.len() {
            let page = self.insts[inst_idx].scan_meta(offset, remaining, type_filter, pattern)?;
            out.extend(page.keys);
            remaining -= page.scanned as usize;

            if page.exhausted {
                // Move to the next instance from its start; spend any leftover
                // budget there within this same call.
                inst_idx += 1;
                offset = 0;
                if remaining == 0 {
                    break;
                }
            } else {
                // Budget spent partway through this instance; resume here later.
                offset += page.scanned;
                break;
            }
        }

        if inst_idx >= self.insts.len() {
            // Every instance drained: signal completion with cursor 0.
            return Ok((0, out));
        }
        let next_cursor = ((inst_idx as u64) << INSTANCE_SHIFT) | offset;
        Ok((next_cursor, out))
    }
}
