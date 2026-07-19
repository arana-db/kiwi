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
use rocksdb::ReadOptions;
use snafu::{OptionExt, ResultExt};

use crate::{
    CanonicalVector, ColumnFamilyIndex, DataType, Redis, Result, TypeCheckState,
    error::{
        InvalidArgumentSnafu, InvalidFormatSnafu, KeyNotFoundSnafu, OptionNoneSnafu, RedisErrSnafu,
        RocksSnafu,
    },
    format_base_key::BaseMetaKey,
    format_member_data_key::MemberDataKey,
    format_vector::{VectorDataValue, VectorMeta},
};

impl Redis {
    pub fn is_cluster_mode(&self) -> bool {
        self.append_log_fn.get().is_some()
    }

    fn ensure_vector_standalone(&self) -> Result<()> {
        if self.is_cluster_mode() {
            return RedisErrSnafu {
                message: "ERR Vector Set is not supported in cluster mode".to_string(),
            }
            .fail();
        }
        Ok(())
    }

    fn parse_vector_meta(&self, value: &[u8]) -> Result<Option<VectorMeta>> {
        if value.is_empty() {
            return Ok(None);
        }

        if value[0] == DataType::VectorSet as u8 {
            let meta = VectorMeta::decode(value)?;
            return Ok((!meta.is_stale() && meta.count() != 0).then_some(meta));
        }

        match self.check_type_state(value, DataType::VectorSet)? {
            TypeCheckState::Missing | TypeCheckState::Stale => Ok(None),
            TypeCheckState::Match => VectorMeta::decode(value).map(Some),
        }
    }

    pub fn vadd(&self, key: &[u8], element: &[u8], vector: &CanonicalVector) -> Result<bool> {
        self.ensure_vector_standalone()?;
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
        let stored_meta = db.get_cf(&meta_cf, &meta_key).context(RocksSnafu)?;
        let live_meta = stored_meta
            .as_deref()
            .map(|value| self.parse_vector_meta(value))
            .transpose()?
            .flatten();

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
            None => VectorMeta::new(1, vector.dimension()),
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

        let member_value = VectorDataValue::from_canonical(vector).encode();
        let meta_value = meta.encode();
        let mut batch = self.create_batch()?;
        batch.put(ColumnFamilyIndex::VectorDataCF, &member_key, &member_value)?;
        batch.put(ColumnFamilyIndex::MetaCF, &meta_key, &meta_value)?;
        batch.commit()?;
        Ok(inserted)
    }

    pub fn vrem(&self, key: &[u8], element: &[u8]) -> Result<bool> {
        self.ensure_vector_standalone()?;
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
        let Some(meta_raw) = db.get_cf(&meta_cf, &meta_key).context(RocksSnafu)? else {
            return Ok(false);
        };
        let Some(mut meta) = self.parse_vector_meta(&meta_raw)? else {
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
        self.ensure_vector_standalone()?;
        Ok(self.read_vector_meta(key)?.map_or(0, |meta| meta.count()))
    }

    pub fn vdim(&self, key: &[u8]) -> Result<u32> {
        self.ensure_vector_standalone()?;
        match self.read_vector_meta(key)? {
            Some(meta) => Ok(meta.dimension()),
            None => KeyNotFoundSnafu {
                key: String::from_utf8_lossy(key).to_string(),
            }
            .fail(),
        }
    }

    pub fn vemb(&self, key: &[u8], element: &[u8]) -> Result<Option<Vec<f64>>> {
        self.ensure_vector_standalone()?;
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
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let meta_key = BaseMetaKey::new(key).encode()?;
        let Some(meta_raw) = db
            .get_cf_opt(&meta_cf, &meta_key, &read_options)
            .context(RocksSnafu)?
        else {
            return Ok(None);
        };
        let Some(meta) = self.parse_vector_meta(&meta_raw)? else {
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
        self.ensure_vector_standalone()?;
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
        let snapshot = db.snapshot();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);

        let meta_key = BaseMetaKey::new(key).encode()?;
        let Some(meta_raw) = db
            .get_cf_opt(&meta_cf, &meta_key, &read_options)
            .context(RocksSnafu)?
        else {
            return Ok(false);
        };
        let Some(meta) = self.parse_vector_meta(&meta_raw)? else {
            return Ok(false);
        };
        let member_key = MemberDataKey::new(key, meta.version(), element).encode()?;
        Ok(db
            .get_cf_opt(&vector_cf, &member_key, &read_options)
            .context(RocksSnafu)?
            .is_some())
    }

    fn read_vector_meta(&self, key: &[u8]) -> Result<Option<VectorMeta>> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let meta_cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "MetaCF is not initialized".to_string(),
            })?;
        let meta_key = BaseMetaKey::new(key).encode()?;
        match db.get_cf(&meta_cf, &meta_key).context(RocksSnafu)? {
            Some(value) => self.parse_vector_meta(&value),
            None => Ok(None),
        }
    }
}
