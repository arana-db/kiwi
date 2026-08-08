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

//! Key-scoped, single-snapshot execution for one VSIM command.

use std::time::{Duration, Instant};

use kstd::lock_mgr::ScopeRecordLock;
use rocksdb::{ReadOptions, Snapshot};
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{
    InvalidArgumentSnafu, InvalidFormatSnafu, KeyNotFoundSnafu, OptionNoneSnafu, Result, RocksSnafu,
};
use crate::format_vector::{VectorDataValue, VectorMeta};
use crate::{
    CanonicalVector, ColumnFamilyIndex, FlatQueryCancel, Redis, VectorHit, VectorQuery,
    VectorSearchOptions,
};

const MAX_FLAT_QUERY_TIMEOUT: Duration = Duration::from_secs(3600);

struct PreparedVectorQuery {
    dimension: u32,
    element: Option<Vec<u8>>,
    element_query: Option<CanonicalVector>,
}

/// One VSIM command's key lock, logical clock, RocksDB snapshot and prepared
/// ELE query. Dropping the session releases the key for VADD/VREM/DEL.
pub struct PreparedVsimSession<'a> {
    pub(crate) redis: &'a Redis,
    pub(crate) key: Vec<u8>,
    pub(crate) meta: VectorMeta,
    pub(crate) snapshot: Snapshot<'a>,
    pub(crate) deadline: Instant,
    logical_now_micros: u64,
    prepared: PreparedVectorQuery,
    _key_guard: ScopeRecordLock<'a>,
}

impl<'a> PreparedVsimSession<'a> {
    pub(crate) fn prepare(
        redis: &'a Redis,
        key: &[u8],
        element: Option<&[u8]>,
    ) -> Result<Option<Self>> {
        let lock_key = String::from_utf8_lossy(key);
        let key_guard = ScopeRecordLock::new(redis.lock_mgr.as_ref(), &lock_key);
        let timeout = Duration::from_millis(redis.storage.vector.flat_query_timeout_ms)
            .min(MAX_FLAT_QUERY_TIMEOUT);
        let deadline = Instant::now() + timeout;
        let db = redis.db().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let vector_cf = redis
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .context(OptionNoneSnafu {
                message: "VectorDataCF is not initialized".to_string(),
            })?;
        let snapshot = db.snapshot();
        // Capture time after pinning the DB sequence: the snapshot can still
        // represent every key that is live at this logical instant, while
        // later expiry/compaction cannot change the session's decision.
        let logical_now_micros = redis.vector_fault_hooks.logical_now_micros();
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);
        let Some(meta) = redis.read_vector_meta_opt_at(key, &read_options, logical_now_micros)?
        else {
            return Ok(None);
        };
        let element_query = if let Some(element) = element {
            let member_key = redis.vector_member_key(key, meta.version(), element)?;
            let value_raw = db
                .get_cf_opt(&vector_cf, &member_key, &read_options)
                .context(RocksSnafu)?
                .context(KeyNotFoundSnafu {
                    key: String::from_utf8_lossy(element).to_string(),
                })?;
            let value = VectorDataValue::decode(&value_raw)?;
            ensure!(
                value.dimension() == meta.dimension(),
                InvalidFormatSnafu {
                    message: format!(
                        "vector member dimension {} does not match meta dimension {}",
                        value.dimension(),
                        meta.dimension()
                    )
                }
            );
            Some(value.canonical().clone())
        } else {
            None
        };
        let dimension = meta.dimension();

        Ok(Some(Self {
            redis,
            key: key.to_vec(),
            meta,
            snapshot,
            deadline,
            logical_now_micros,
            prepared: PreparedVectorQuery {
                dimension,
                element: element.map(<[u8]>::to_vec),
                element_query,
            },
            _key_guard: key_guard,
        }))
    }

    /// Dimension captured from the same meta snapshot used by [`Self::search`].
    pub fn dimension(&self) -> u32 {
        self.prepared.dimension
    }

    /// Logical clock captured after acquiring the key lock and pinning the DB snapshot.
    pub fn logical_now_micros(&self) -> u64 {
        self.logical_now_micros
    }

    /// Execute the query while consuming and therefore bounding the lifetime
    /// of the session's key guard.
    pub fn search(
        self,
        query: VectorQuery,
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorHit>> {
        self.search_with_cancel(query, options, &FlatQueryCancel::default())
    }

    /// Execute with cooperative cancellation while retaining the key guard
    /// through every exit path.
    pub fn search_with_cancel(
        self,
        query: VectorQuery,
        options: VectorSearchOptions,
        cancel: &FlatQueryCancel,
    ) -> Result<Vec<VectorHit>> {
        let query_vector = match query {
            VectorQuery::Vector(vector) => vector,
            VectorQuery::Element(element) => {
                ensure!(
                    self.prepared.element.as_deref() == Some(element.as_slice()),
                    InvalidArgumentSnafu {
                        message: "VSIM ELE query does not match the prepared session".to_string()
                    }
                );
                self.prepared.element_query.clone().ok_or_else(|| {
                    InvalidFormatSnafu {
                        message: "prepared VSIM ELE query is missing its vector".to_string(),
                    }
                    .build()
                })?
            }
        };
        self.redis
            .vsim_session_search(&self, query_vector, options, cancel)
    }
}
