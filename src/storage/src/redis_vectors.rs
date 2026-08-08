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

use std::time::Instant;

use kstd::lock_mgr::ScopeRecordLock;
use rocksdb::{IteratorMode, ReadOptions};
use snafu::{OptionExt, ResultExt};

use crate::{
    CanonicalVector, ColumnFamilyIndex, DataType, PreparedVsimSession, Redis, Result,
    TypeCheckState, VectorHit, VectorQuery, VectorSearchEngine, VectorSearchMode,
    VectorSearchOptions,
    error::{
        BatchSnafu, InvalidArgumentSnafu, InvalidFormatSnafu, KeyNotFoundSnafu, OptionNoneSnafu,
        RedisErrSnafu, RocksSnafu, SystemSnafu, VectorFlatQueryTimeoutSnafu,
    },
    format_base_key::BaseMetaKey,
    format_vector::{VectorDataValue, VectorMeta},
    format_vector_member_key::{ParsedVectorMemberDataKey, VectorMemberDataKey},
    vector_flat::{FlatQueryCancel, FlatScanGuard},
    vector_mutation::{
        VectorSetApplyError, VectorSetApplyResult, VectorSetBusinessError, VectorSetMutationV1,
    },
};

impl Redis {
    /// Build the V1 member key for `element` under the set's current
    /// generation (stored in `meta.version`).
    pub(crate) fn vector_member_key(
        &self,
        key: &[u8],
        generation: u64,
        element: &[u8],
    ) -> Result<Vec<u8>> {
        VectorMemberDataKey {
            key,
            storage_incarnation: self.storage_incarnation()?,
            generation_sequence: generation,
            element,
        }
        .encode_full()
    }

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

    fn decode_vector_meta_at(
        &self,
        value: &[u8],
        logical_now_micros: u64,
    ) -> Result<Option<VectorMeta>> {
        if value.is_empty() {
            return Ok(None);
        }

        if value[0] == DataType::VectorSet as u8 {
            return VectorMeta::decode(value).map(Some);
        }

        match self.check_type_state_at(value, DataType::VectorSet, logical_now_micros)? {
            TypeCheckState::Missing | TypeCheckState::Stale => Ok(None),
            TypeCheckState::Match => VectorMeta::decode(value).map(Some),
        }
    }

    fn parse_vector_meta_at(
        &self,
        value: &[u8],
        logical_now_micros: u64,
    ) -> Result<Option<VectorMeta>> {
        Ok(self
            .decode_vector_meta_at(value, logical_now_micros)?
            .filter(|meta| !meta.is_stale_at(logical_now_micros) && meta.count() != 0))
    }

    pub fn vadd(&self, key: &[u8], element: &[u8], vector: &CanonicalVector) -> Result<bool> {
        // Resource limits apply before any write so standalone and cluster
        // modes reject oversized input identically.
        let vector_config = &self.storage.vector;
        if vector.dimension() > vector_config.max_dimension {
            return RedisErrSnafu {
                message: "ERR vector dimension exceeds max_dimension".to_string(),
            }
            .fail();
        }
        if element.len() > vector_config.max_element_bytes {
            return RedisErrSnafu {
                message: "ERR vector element exceeds max_element_bytes".to_string(),
            }
            .fail();
        }
        // The client supplies either an FP32 blob or VALUES floats; both are
        // `dimension * 4` bytes on the wire.
        if u64::from(vector.dimension()) * size_of::<f32>() as u64
            > vector_config.max_vector_bytes as u64
        {
            return RedisErrSnafu {
                message: "ERR vector exceeds max_vector_bytes".to_string(),
            }
            .fail();
        }

        let mutation = VectorSetMutationV1::add_from_canonical(element, vector)?;
        let result = self.apply_vector_set_mutation(key, &mutation, None)?;
        Ok(matches!(result, VectorSetApplyResult::Added))
    }

    pub fn vrem(&self, key: &[u8], element: &[u8]) -> Result<bool> {
        let mutation = VectorSetMutationV1::Remove {
            element: element.to_vec(),
        };
        let result = self.apply_vector_set_mutation(key, &mutation, None)?;
        Ok(matches!(result, VectorSetApplyResult::Removed))
    }

    /// Single decision point for vector-set mutations.
    ///
    /// Applies a logical mutation to `key`: reads the current meta, decides
    /// the outcome (create / add / update / remove / miss), and commits the
    /// derived state (member record, count, data_revision, generation on
    /// create) in one atomic batch. Standalone VADD/VREM are thin wrappers
    /// around this entry; the Raft state machine will replay logical
    /// mutations through it in log order so every replica derives identical
    /// state.
    ///
    /// `create_generation` is the generation assigned when the mutation
    /// creates the set (cluster mode: the creating Raft log index); `None`
    /// falls back to `allocate_vector_generation`.
    ///
    /// Business rejections (WRONGTYPE, dimension mismatch) are returned as
    /// deterministic `VectorSetApplyError::Business` errors; everything else
    /// is a fatal storage error.
    pub fn apply_vector_set_mutation(
        &self,
        key: &[u8],
        mutation: &VectorSetMutationV1,
        create_generation: Option<u64>,
    ) -> std::result::Result<VectorSetApplyResult, VectorSetApplyError> {
        match mutation {
            VectorSetMutationV1::Add { element, .. } => {
                let vector = mutation.canonical_vector()?.context(OptionNoneSnafu {
                    message: "add mutation carries no vector".to_string(),
                })?;
                self.apply_vector_add(key, element, vector, create_generation)
            }
            VectorSetMutationV1::Remove { element } => self.apply_vector_remove(key, element),
        }
    }

    fn apply_vector_add(
        &self,
        key: &[u8],
        element: &[u8],
        vector: CanonicalVector,
        create_generation: Option<u64>,
    ) -> std::result::Result<VectorSetApplyResult, VectorSetApplyError> {
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
        let meta_key = BaseMetaKey::new(key).encode()?;
        let live_meta = self.read_live_vector_meta_for_apply(&meta_key)?;

        let is_new_set = live_meta.is_none();
        let mut meta = match live_meta {
            Some(meta) => {
                if meta.dimension() != vector.dimension() {
                    return Err(VectorSetApplyError::Business(
                        VectorSetBusinessError::DimensionMismatch {
                            expected: meta.dimension(),
                            got: vector.dimension(),
                        },
                    ));
                }
                meta
            }
            // Creating (or recreating after expiry/deletion) allocates a fresh
            // generation sequence from the persistent generator, so stale
            // members of a previous lifecycle never collide with the new set.
            None => {
                let generation = match create_generation {
                    Some(generation) => generation,
                    None => self.allocate_vector_generation()?,
                };
                VectorMeta::new(1, vector.dimension(), vector.quantization(), generation)
            }
        };

        let member_key = self.vector_member_key(key, meta.version(), element)?;
        let inserted = if is_new_set {
            true
        } else {
            db.get_cf(&vector_cf, &member_key)
                .context(RocksSnafu)?
                .is_none()
        };
        if inserted && !is_new_set {
            let Some(count) = meta.count().checked_add(1) else {
                return Err(VectorSetApplyError::Storage(
                    InvalidArgumentSnafu {
                        message: "vector set size overflow".to_string(),
                    }
                    .build(),
                ));
            };
            meta.set_count(count);
        }

        // Quantization is a per-set property: store the member in the set's
        // quantization regardless of the form the client supplied.
        let vector = vector.to_quantized(meta.quantization())?;
        let member_value = VectorDataValue::from_canonical(&vector).encode();
        if !is_new_set {
            meta.bump_data_revision();
        }
        let meta_value = meta.encode();
        let mut batch = self.create_batch()?;
        batch.put(ColumnFamilyIndex::VectorDataCF, &member_key, &member_value)?;
        batch.put(ColumnFamilyIndex::MetaCF, &meta_key, &meta_value)?;
        if self.vector_fault_hooks.fail_batch_commit() {
            // Dropping the uncommitted batch leaves meta and member
            // untouched, exactly like a failed commit: the mutation is
            // all-or-nothing.
            return Err(VectorSetApplyError::Storage(
                BatchSnafu {
                    message: "injected fault: vector batch commit failed".to_string(),
                }
                .build(),
            ));
        }
        batch.commit()?;
        Ok(if inserted {
            VectorSetApplyResult::Added
        } else {
            VectorSetApplyResult::Updated
        })
    }

    fn apply_vector_remove(
        &self,
        key: &[u8],
        element: &[u8],
    ) -> std::result::Result<VectorSetApplyResult, VectorSetApplyError> {
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
        let meta_key = BaseMetaKey::new(key).encode()?;
        let Some(mut meta) = self.read_live_vector_meta_for_apply(&meta_key)? else {
            return Ok(VectorSetApplyResult::RemoveMissed);
        };

        let member_key = self.vector_member_key(key, meta.version(), element)?;
        if db
            .get_cf(&vector_cf, &member_key)
            .context(RocksSnafu)?
            .is_none()
        {
            return Ok(VectorSetApplyResult::RemoveMissed);
        }

        let mut batch = self.create_batch()?;
        batch.delete(ColumnFamilyIndex::VectorDataCF, &member_key)?;
        if meta.count() > 1 {
            meta.set_count(meta.count() - 1);
            meta.bump_data_revision();
            let meta_value = meta.encode();
            batch.put(ColumnFamilyIndex::MetaCF, &meta_key, &meta_value)?;
        } else {
            batch.delete(ColumnFamilyIndex::MetaCF, &meta_key)?;
        }
        if self.vector_fault_hooks.fail_batch_commit() {
            // Dropping the uncommitted batch leaves meta and member
            // untouched, exactly like a failed commit: the mutation is
            // all-or-nothing.
            return Err(VectorSetApplyError::Storage(
                BatchSnafu {
                    message: "injected fault: vector batch commit failed".to_string(),
                }
                .build(),
            ));
        }
        batch.commit()?;
        Ok(VectorSetApplyResult::Removed)
    }

    /// Read the live vector meta for `meta_key` during mutation apply.
    ///
    /// A live non-vector value is classified as the WRONGTYPE business error
    /// so a Raft apply loop can treat it as deterministic; decode failures of
    /// vector metadata indicate corruption and stay fatal storage errors.
    fn read_live_vector_meta_for_apply(
        &self,
        meta_key: &[u8],
    ) -> std::result::Result<Option<VectorMeta>, VectorSetApplyError> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let meta_cf = self
            .get_cf_handle(ColumnFamilyIndex::MetaCF)
            .context(OptionNoneSnafu {
                message: "MetaCF is not initialized".to_string(),
            })?;
        if self.vector_fault_hooks.fail_meta_read() {
            return Err(VectorSetApplyError::Storage(
                SystemSnafu {
                    message: "injected fault: vector meta read failed".to_string(),
                }
                .build(),
            ));
        }
        let stored_raw = db.get_cf(&meta_cf, meta_key).context(RocksSnafu)?;
        let Some(value) = stored_raw.as_deref() else {
            return Ok(None);
        };
        if value.is_empty() {
            return Ok(None);
        }
        if value[0] != DataType::VectorSet as u8 {
            // Mirrors `decode_vector_meta`: stale non-vector metadata counts
            // as absent, a live non-vector value is WRONGTYPE.
            return match self.is_stale(value) {
                Ok(true) => Ok(None),
                Ok(false) => Err(VectorSetApplyError::Business(
                    VectorSetBusinessError::WrongType,
                )),
                Err(error) => Err(VectorSetApplyError::Storage(error)),
            };
        }
        let meta = VectorMeta::decode(value)?;
        Ok((!meta.is_stale() && meta.count() != 0).then_some(meta))
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

    /// O(1) per-set metadata for VINFO; `None` when the key is missing (or
    /// stale/emptied), WRONGTYPE when it holds another live data type.
    pub fn vinfo(&self, key: &[u8]) -> Result<Option<crate::VectorInfo>> {
        Ok(self.read_vector_meta(key)?.map(|meta| crate::VectorInfo {
            dimension: meta.dimension(),
            size: meta.count(),
            generation: meta.version(),
        }))
    }

    pub fn prepare_vsim_session(
        &self,
        key: &[u8],
        element: Option<&[u8]>,
    ) -> Result<Option<PreparedVsimSession<'_>>> {
        PreparedVsimSession::prepare(self, key, element)
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
        let member_key = self.vector_member_key(key, meta.version(), element)?;
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
        let member_key = self.vector_member_key(key, meta.version(), element)?;
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
        self.vsim_with_cancel(key, query, options, &FlatQueryCancel::default())
    }

    /// `vsim` with an explicit cancellation token. The token is checked
    /// cooperatively during the scan; no trigger source is wired into the
    /// command path yet (client disconnects are follow-up work).
    pub fn vsim_with_cancel(
        &self,
        key: &[u8],
        query: VectorQuery,
        options: VectorSearchOptions,
        cancel: &FlatQueryCancel,
    ) -> Result<Vec<VectorHit>> {
        self.validate_vsim_options(options)?;
        let element = match &query {
            VectorQuery::Element(element) => Some(element.as_slice()),
            VectorQuery::Vector(_) => None,
        };
        let Some(session) = self.prepare_vsim_session(key, element)? else {
            return Ok(Vec::new());
        };
        session.search_with_cancel(query, options, cancel)
    }

    fn validate_vsim_options(&self, options: VectorSearchOptions) -> Result<()> {
        let vector_config = &self.storage.vector;
        if options.count == 0 {
            return InvalidArgumentSnafu {
                message: "vector search count must be greater than zero".to_string(),
            }
            .fail();
        }
        if options.count > vector_config.max_k {
            return RedisErrSnafu {
                message: "ERR vector search count exceeds max_k".to_string(),
            }
            .fail();
        }
        Ok(())
    }

    pub(crate) fn vsim_session_search(
        &self,
        session: &PreparedVsimSession<'_>,
        query_vector: CanonicalVector,
        options: VectorSearchOptions,
        cancel: &FlatQueryCancel,
    ) -> Result<Vec<VectorHit>> {
        self.validate_vsim_options(options)?;
        let Some(_permit) = self.flat_query_gate.acquire(session.deadline) else {
            self.vector_metrics.record_capacity_rejected();
            return VectorFlatQueryTimeoutSnafu.fail();
        };
        self.vector_metrics.record_query_started();
        let started = Instant::now();
        let result = self.vsim_session_scan(session, query_vector, options, cancel);
        self.vector_metrics
            .record_query_finished(started.elapsed(), result.as_ref().err());
        result
    }

    fn vsim_session_scan(
        &self,
        session: &PreparedVsimSession<'_>,
        query_vector: CanonicalVector,
        options: VectorSearchOptions,
        cancel: &FlatQueryCancel,
    ) -> Result<Vec<VectorHit>> {
        let vector_config = &self.storage.vector;
        let mut scan_guard = FlatScanGuard::new(vector_config, session.deadline, cancel);
        scan_guard.check_signals()?;

        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        let vector_cf = self
            .get_cf_handle(ColumnFamilyIndex::VectorDataCF)
            .context(OptionNoneSnafu {
                message: "VectorDataCF is not initialized".to_string(),
            })?;
        if query_vector.dimension() != session.meta.dimension() {
            return InvalidArgumentSnafu {
                message: format!(
                    "vector dimension mismatch: expected {}, got {}",
                    session.meta.dimension(),
                    query_vector.dimension()
                ),
            }
            .fail();
        }
        // Score the query in the set's quantization so it is comparable to
        // the stored members.
        let query_vector = query_vector.to_quantized(session.meta.quantization())?;
        self.vector_fault_hooks.block_vsim_scan_if_armed();

        // Scan exactly this set's generation range: an inclusive lower bound
        // at the (key, incarnation, generation) prefix and its exclusive
        // successor as the upper bound. The starts_with check stays as a
        // defensive guard.
        let prefix = VectorMemberDataKey::encode_prefix(
            &session.key,
            self.storage_incarnation()?,
            session.meta.version(),
        )?;
        let mut scan_options = ReadOptions::default();
        scan_options.set_snapshot(&session.snapshot);
        scan_options.set_iterate_lower_bound(prefix.clone());
        if let Some(upper_bound) = VectorMemberDataKey::prefix_upper_bound(&prefix) {
            scan_options.set_iterate_upper_bound(upper_bound);
        }
        let iterator = db.iterator_cf_opt(&vector_cf, scan_options, IteratorMode::Start);
        // Phase 1: both modes run the exhaustive FLAT scan. TRUTH is wired
        // through explicitly so the mode selects the engine once an
        // approximate index exists.
        let engine = match options.mode {
            VectorSearchMode::Approximate => VectorSearchEngine::Flat,
            VectorSearchMode::Truth => VectorSearchEngine::Flat,
        };
        let candidates = iterator
            .take_while(|result| match result {
                Ok((encoded_key, _)) => encoded_key.starts_with(&prefix),
                Err(_) => true,
            })
            .map(|entry| {
                let (encoded_key, encoded_value) = entry.context(RocksSnafu)?;
                if self.vector_fault_hooks.fail_member_read() {
                    return SystemSnafu {
                        message: "injected fault: vector member read failed".to_string(),
                    }
                    .fail();
                }
                scan_guard.record(encoded_key.len(), encoded_value.len())?;
                let element = ParsedVectorMemberDataKey::decode(&encoded_key)?
                    .element()
                    .to_vec();
                let value = VectorDataValue::decode(&encoded_value)?;
                if value.dimension() != session.meta.dimension() {
                    return InvalidFormatSnafu {
                        message: format!(
                            "vector member dimension {} does not match meta dimension {}",
                            value.dimension(),
                            session.meta.dimension()
                        ),
                    }
                    .fail();
                }
                Ok((element, value.canonical().clone()))
            });

        engine.search(
            &query_vector,
            &session.meta.metric(),
            options.count,
            candidates,
        )
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
        if self.vector_fault_hooks.fail_meta_read() {
            return SystemSnafu {
                message: "injected fault: vector meta read failed".to_string(),
            }
            .fail();
        }
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

    pub(crate) fn read_vector_meta_opt_at(
        &self,
        key: &[u8],
        read_options: &ReadOptions,
        logical_now_micros: u64,
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
        if self.vector_fault_hooks.fail_meta_read() {
            return SystemSnafu {
                message: "injected fault: vector meta read failed".to_string(),
            }
            .fail();
        }
        match db
            .get_cf_opt(&meta_cf, &meta_key, read_options)
            .context(RocksSnafu)?
        {
            Some(value) => self.parse_vector_meta_at(&value, logical_now_micros),
            None => Ok(None),
        }
    }

    fn read_vector_meta(&self, key: &[u8]) -> Result<Option<VectorMeta>> {
        self.read_vector_meta_opt(key, None)
    }

    /// Validate every persisted VectorSet meta and member in this instance.
    pub fn validate_vector_consistency(&self) -> Result<crate::VectorConsistencyReport> {
        let db = self.db.as_ref().context(OptionNoneSnafu {
            message: "db is not initialized".to_string(),
        })?;
        crate::vector_consistency::validate_vector_consistency_db(
            db,
            self.storage_incarnation()?,
            "open Redis instance",
        )
    }
}
