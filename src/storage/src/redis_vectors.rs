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

use kstd::lock_mgr::ScopeRecordLock;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use snafu::{OptionExt, ResultExt};

use crate::{
    CanonicalVector, ColumnFamilyIndex, DataType, Redis, Result, TypeCheckState, VectorHit,
    VectorQuery, VectorSearchEngine, VectorSearchOptions,
    error::{
        InvalidArgumentSnafu, InvalidFormatSnafu, KeyNotFoundSnafu, OptionNoneSnafu, RocksSnafu,
    },
    format_base_key::BaseMetaKey,
    format_member_data_key::MemberDataKey,
    format_vector::{VectorDataValue, VectorMeta},
    storage_define::SUFFIX_RESERVE_LENGTH,
};

impl Redis {
    /// Decode raw meta bytes into a `VectorMeta` without liveness filtering:
    /// stale or emptied sets are still returned so callers can inspect their
    /// version. Returns an error when the key holds another live data type.
    fn decode_vector_meta(&self, value: &[u8]) -> Result<Option<VectorMeta>> {
        if value.is_empty() {
            return Ok(None);
        }

        if value[0] == DataType::VectorSet as u8 {
            return VectorMeta::decode(value).map(Some);
        }

        match self.check_type_state(value, DataType::VectorSet)? {
            TypeCheckState::Missing | TypeCheckState::Stale => Ok(None),
            TypeCheckState::Match => VectorMeta::decode(value).map(Some),
        }
    }

    /// Decode raw meta bytes into a live `VectorMeta`, treating stale or
    /// emptied sets as absent.
    fn parse_vector_meta(&self, value: &[u8]) -> Result<Option<VectorMeta>> {
        Ok(self
            .decode_vector_meta(value)?
            .filter(|meta| !meta.is_stale() && meta.count() != 0))
    }

    pub fn vadd(&self, key: &[u8], element: &[u8], vector: &CanonicalVector) -> Result<bool> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let meta_cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "MetaCF is not initialized".to_string(),
            })?;
        let vector_cf = self
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .context(OptionNoneSnafu {
                message: "VectorDataCF is not initialized".to_string(),
            })?;

        let lock_key = String::from_utf8_lossy(key);
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &lock_key);
        let meta_key = BaseMetaKey::new(key).encode()?;
        let stored_raw = db.get_cf(&meta_cf, &meta_key).context(RocksSnafu)?;
        let stored_meta = match stored_raw.as_deref() {
            Some(value) => self.decode_vector_meta(value)?,
            None => None,
        };
        // Keep the previous version even for stale or emptied sets so a
        // recreated set always gets a fresh, monotonically increasing generation.
        let previous_generation = stored_meta.as_ref().map_or(0, VectorMeta::version);
        let live_meta = stored_meta.filter(|meta| !meta.is_stale() && meta.count() != 0);

        let is_new_set = live_meta.is_none();
        let mut meta = match live_meta {
            Some(meta) => {
                if meta.dimension() != vector.dimension() {
                    return InvalidArgumentSnafu {
                        message: format!(
                            "vector dimension mismatch: expected {}, got {}",
                            meta.dimension(),
                            vector.dimension()
                        ),
                    }
                    .fail();
                }
                meta
            }
            None => VectorMeta::new_after(
                1,
                vector.dimension(),
                vector.quantization(),
                previous_generation,
            ),
        };

        let member_key = MemberDataKey::new(key, meta.version(), element).encode()?;
        let inserted = if is_new_set {
            true
        } else {
            db.get_cf(&vector_cf, &member_key)
                .context(RocksSnafu)?
                .is_none()
        };
        if inserted && !is_new_set {
            let count = meta.count().checked_add(1);
            let Some(count) = count else {
                return InvalidArgumentSnafu {
                    message: "vector set size overflow".to_string(),
                }
                .fail();
            };
            meta.set_count(count);
        }

        // Quantization is a per-set property: store the member in the set's
        // quantization regardless of the form the client supplied.
        let vector = vector.to_quantized(meta.quantization())?;
        let member_value = VectorDataValue::from_canonical(&vector).encode();
        let meta_value = meta.encode();
        let mut batch = self.create_batch()?;
        batch.put(ColumnFamilyIndex::VectorDataCF, &member_key, &member_value)?;
        batch.put(ColumnFamilyIndex::MetaCF, &meta_key, &meta_value)?;
        batch.commit()?;
        Ok(inserted)
    }

    pub fn vrem(&self, key: &[u8], element: &[u8]) -> Result<bool> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let vector_cf = self
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .context(OptionNoneSnafu {
                message: "VectorDataCF is not initialized".to_string(),
            })?;

        let lock_key = String::from_utf8_lossy(key);
        let _lock = ScopeRecordLock::new(self.lock_mgr.as_ref(), &lock_key);
        let Some(mut meta) = self.read_vector_meta(key)? else {
            return Ok(false);
        };

        let member_key = MemberDataKey::new(key, meta.version(), element).encode()?;
        if db
            .get_cf(&vector_cf, &member_key)
            .context(RocksSnafu)?
            .is_none()
        {
            return Ok(false);
        }

        let meta_key = BaseMetaKey::new(key).encode()?;
        let mut batch = self.create_batch()?;
        batch.delete(ColumnFamilyIndex::VectorDataCF, &member_key)?;
        if meta.count() > 1 {
            meta.set_count(meta.count() - 1);
            let meta_value = meta.encode();
            batch.put(ColumnFamilyIndex::MetaCF, &meta_key, &meta_value)?;
        } else {
            batch.delete(ColumnFamilyIndex::MetaCF, &meta_key)?;
        }
        batch.commit()?;
        Ok(true)
    }

    pub fn vcard(&self, key: &[u8]) -> Result<u64> {
        Ok(self.read_vector_meta(key)?.map_or(0, |meta| meta.count()))
    }

    pub fn vdim(&self, key: &[u8]) -> Result<u32> {
        match self.read_vector_meta(key)? {
            Some(meta) => Ok(meta.dimension()),
            None => KeyNotFoundSnafu {
                key: String::from_utf8_lossy(key).to_string(),
            }
            .fail(),
        }
    }

    pub fn vemb(&self, key: &[u8], element: &[u8]) -> Result<Option<Vec<f64>>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let vector_cf = self
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .context(OptionNoneSnafu {
                message: "VectorDataCF is not initialized".to_string(),
            })?;
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let Some(meta) = self.read_vector_meta_opt(key, Some(&read_options))? else {
            return Ok(None);
        };
        let member_key = MemberDataKey::new(key, meta.version(), element).encode()?;
        let Some(value_raw) = db
            .get_cf_opt(&vector_cf, &member_key, &read_options)
            .context(RocksSnafu)?
        else {
            return Ok(None);
        };
        let value = VectorDataValue::decode(&value_raw)?;
        if value.dimension() != meta.dimension() {
            return InvalidFormatSnafu {
                message: format!(
                    "vector member dimension {} does not match meta dimension {}",
                    value.dimension(),
                    meta.dimension()
                ),
            }
            .fail();
        }
        Ok(Some(value.canonical().restore()))
    }

    pub fn vismember(&self, key: &[u8], element: &[u8]) -> Result<bool> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let vector_cf = self
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .context(OptionNoneSnafu {
                message: "VectorDataCF is not initialized".to_string(),
            })?;
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let Some(meta) = self.read_vector_meta_opt(key, Some(&read_options))? else {
            return Ok(false);
        };
        let member_key = MemberDataKey::new(key, meta.version(), element).encode()?;
        Ok(db
            .get_cf_opt(&vector_cf, &member_key, &read_options)
            .context(RocksSnafu)?
            .is_some())
    }

    pub fn vsim(
        &self,
        key: &[u8],
        query: VectorQuery,
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorHit>> {
        if options.count == 0 {
            return InvalidArgumentSnafu {
                message: "vector search count must be greater than zero".to_string(),
            }
            .fail();
        }

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let vector_cf = self
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .context(OptionNoneSnafu {
                message: "VectorDataCF is not initialized".to_string(),
            })?;
        let snapshot = db.snapshot();
        let mut point_read_options = ReadOptions::default();
        point_read_options.set_snapshot(&snapshot);

        let Some(meta) = self.read_vector_meta_opt(key, Some(&point_read_options))? else {
            return Ok(Vec::new());
        };

        let query_vector = match query {
            VectorQuery::Element(element) => {
                let query_key = MemberDataKey::new(key, meta.version(), &element).encode()?;
                let Some(query_raw) = db
                    .get_cf_opt(&vector_cf, &query_key, &point_read_options)
                    .context(RocksSnafu)?
                else {
                    return KeyNotFoundSnafu {
                        key: String::from_utf8_lossy(&element).to_string(),
                    }
                    .fail();
                };
                VectorDataValue::decode(&query_raw)?.canonical().clone()
            }
            VectorQuery::Vector(vector) => vector,
        };
        if query_vector.dimension() != meta.dimension() {
            return InvalidArgumentSnafu {
                message: format!(
                    "vector dimension mismatch: expected {}, got {}",
                    meta.dimension(),
                    query_vector.dimension()
                ),
            }
            .fail();
        }
        // Score the query in the set's quantization so it is comparable to
        // the stored members.
        let query_vector = query_vector.to_quantized(meta.quantization())?;

        let prefix = MemberDataKey::new(key, meta.version(), b"").encode_seek_key()?;
        let mut scan_options = ReadOptions::default();
        scan_options.set_snapshot(&snapshot);
        let iterator = db.iterator_cf_opt(
            &vector_cf,
            scan_options,
            IteratorMode::From(&prefix, Direction::Forward),
        );
        let engine = VectorSearchEngine::Flat;
        let candidates = iterator
            .take_while(|result| match result {
                Ok((encoded_key, _)) => encoded_key.starts_with(&prefix),
                Err(_) => true,
            })
            .map(|entry| {
                let (encoded_key, encoded_value) = entry.context(RocksSnafu)?;
                if encoded_key.len() < prefix.len() + SUFFIX_RESERVE_LENGTH {
                    return InvalidFormatSnafu {
                        message: "vector member key is shorter than its generation prefix"
                            .to_string(),
                    }
                    .fail();
                }

                let element_end = encoded_key.len() - SUFFIX_RESERVE_LENGTH;
                let element = encoded_key[prefix.len()..element_end].to_vec();
                let value = VectorDataValue::decode(&encoded_value)?;
                if value.dimension() != meta.dimension() {
                    return InvalidFormatSnafu {
                        message: format!(
                            "vector member dimension {} does not match meta dimension {}",
                            value.dimension(),
                            meta.dimension()
                        ),
                    }
                    .fail();
                }
                Ok((element, value.canonical().clone()))
            });

        engine.search(&query_vector, &meta.metric(), options.count, candidates)
    }

    fn read_vector_meta_opt(
        &self,
        key: &[u8],
        read_options: Option<&ReadOptions>,
    ) -> Result<Option<VectorMeta>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let meta_cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "MetaCF is not initialized".to_string(),
            })?;
        let meta_key = BaseMetaKey::new(key).encode()?;
        let value = match read_options {
            Some(opts) => db.get_cf_opt(&meta_cf, &meta_key, opts),
            None => db.get_cf(&meta_cf, &meta_key),
        }
        .context(RocksSnafu)?;
        match value {
            Some(value) => self.parse_vector_meta(&value),
            None => Ok(None),
        }
    }

    fn read_vector_meta(&self, key: &[u8]) -> Result<Option<VectorMeta>> {
        self.read_vector_meta_opt(key, None)
    }
}
