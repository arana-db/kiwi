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
//! Redis cursors are opaque. Kiwi maps each non-zero cursor to the RocksDB
//! instance and the next unconsumed physical `MetaCF` key. Resuming with a
//! RocksDB seek keeps every page O(COUNT) and avoids ordinal offsets shifting
//! when keys are deleted between calls.
//!
//! Cursor state is process-local and bounded. Unknown or evicted non-zero
//! cursors fail explicitly instead of silently restarting or truncating a scan.

use std::sync::atomic::{AtomicU64, Ordering};

use rocksdb::{Direction, IteratorMode};
use snafu::{OptionExt, ResultExt};

use crate::{
    ColumnFamilyIndex, DataType, Redis, Result,
    error::{Error, InvalidFormatSnafu, OptionNoneSnafu, RocksSnafu},
    format_base_key::ParsedBaseKey,
    format_base_meta_value::ParsedBaseMetaValue,
    format_list_meta_value::ParsedListsMetaValue,
    format_strings_value::ParsedStringsValue,
    redis_sets::glob_match_bytes,
    storage::Storage,
};

static NEXT_SCAN_CURSOR: AtomicU64 = AtomicU64::new(1);
pub(crate) const SCAN_CURSOR_STATE_CAPACITY: usize = 5000;

#[derive(Clone, Debug)]
pub(crate) struct ScanCursorState {
    instance_index: usize,
    next_key: Option<Vec<u8>>,
}

fn allocate_scan_cursor() -> Result<u64> {
    NEXT_SCAN_CURSOR
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
            next.checked_add(1)
        })
        .map_err(|_| Error::System {
            message: "SCAN cursor space exhausted".to_string(),
            location: snafu::location!(),
        })
}

/// One instance's contribution to a single `SCAN` step.
struct ScanPage {
    /// Live user keys matching the type and pattern filters.
    keys: Vec<Vec<u8>>,
    /// Raw `MetaCF` entries consumed this step (matched or not).
    scanned: usize,
    /// First raw key not consumed by this page, if this instance has more data.
    next_key: Option<Vec<u8>>,
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
        b"vectorset" => DataType::VectorSet,
        _ => DataType::None,
    }
}

impl Redis {
    /// Scan up to `limit` raw `MetaCF` entries starting at `start_key`,
    /// returning the live keys among them that pass the type and pattern
    /// filters, how many raw entries were consumed, and the first unconsumed
    /// physical key.
    fn scan_meta(
        &self,
        start_key: Option<&[u8]>,
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
        let mut scanned = 0usize;
        let iterator_mode = match start_key {
            Some(key) => IteratorMode::From(key, Direction::Forward),
            None => IteratorMode::Start,
        };
        let mut iter = db.iterator_cf(&meta_cf, iterator_mode);

        while scanned < limit {
            let Some(item) = iter.next() else {
                return Ok(ScanPage {
                    keys,
                    scanned,
                    next_key: None,
                });
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
                DataType::Hash | DataType::Set | DataType::ZSet | DataType::VectorSet => {
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
            if let Some(wanted) = type_filter
                && data_type != wanted
            {
                continue;
            }
            if pattern != b"*" && !glob_match_bytes(pattern, base_key.key()) {
                continue;
            }
            keys.push(base_key.key().to_vec());
        }

        let next_key = match iter.next() {
            Some(item) => {
                let (key_bytes, _) = item.context(RocksSnafu)?;
                Some(key_bytes.to_vec())
            }
            None => None,
        };

        Ok(ScanPage {
            keys,
            scanned,
            next_key,
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
        let (mut inst_idx, mut next_key) = if cursor == 0 {
            (0, None)
        } else {
            self.scan_cursor_states
                .get(&cursor)
                .map(|entry| {
                    let state = entry.value();
                    (state.instance_index, state.next_key.clone())
                })
                .ok_or_else(|| Error::RedisErr {
                    message: "ERR invalid cursor".to_string(),
                    location: snafu::location!(),
                })?
        };
        let mut out = Vec::new();

        while inst_idx < self.insts.len() {
            let page = self.insts[inst_idx].scan_meta(
                next_key.as_deref(),
                remaining,
                type_filter,
                pattern,
            )?;
            out.extend(page.keys);
            remaining -= page.scanned;

            if let Some(unconsumed_key) = page.next_key {
                next_key = Some(unconsumed_key);
                break;
            } else {
                // Move to the next instance from its start; spend any leftover
                // budget there within this same call.
                inst_idx += 1;
                next_key = None;
                if remaining == 0 {
                    break;
                }
            }
        }

        if inst_idx >= self.insts.len() {
            // Every instance drained: signal completion with cursor 0.
            return Ok((0, out));
        }
        let next_cursor = allocate_scan_cursor()?;
        self.scan_cursor_states.insert(
            next_cursor,
            ScanCursorState {
                instance_index: inst_idx,
                next_key,
            },
        );
        Ok((next_cursor, out))
    }
}
