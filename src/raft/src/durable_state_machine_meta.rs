// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::result_large_err)]

use std::sync::Arc;

#[cfg(test)]
use std::path::PathBuf;

use conf::raft_type::KiwiNode;
use openraft::{LogId, StorageError, StoredMembership};
use rocksdb::{BoundColumnFamily, DB, WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};

use crate::log_store_rocksdb::{
    SM_META_CF, cf_not_found_read, deserialize, io_read_err, io_write_err, serialize,
};

pub const SM_META_FORMAT_VERSION: u16 = 1;

const SM_META_KEY: &[u8] = b"state_machine_meta";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DurableStateMachineMeta {
    pub format_version: u16,
    pub last_applied: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, KiwiNode>,
}

impl DurableStateMachineMeta {
    pub fn new(
        last_applied: Option<LogId<u64>>,
        last_membership: StoredMembership<u64, KiwiNode>,
    ) -> Self {
        Self {
            format_version: SM_META_FORMAT_VERSION,
            last_applied,
            last_membership,
        }
    }
}

#[cfg(test)]
mod test_hooks {
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::LazyLock;

    use parking_lot::Mutex;

    pub static SM_META_SAVE_FAILURES: LazyLock<Mutex<HashSet<PathBuf>>> =
        LazyLock::new(|| Mutex::new(HashSet::new()));
}

#[cfg(test)]
pub struct SmMetaSaveFailureGuard {
    db_path: PathBuf,
}

#[cfg(test)]
impl Drop for SmMetaSaveFailureGuard {
    fn drop(&mut self) {
        test_hooks::SM_META_SAVE_FAILURES
            .lock()
            .remove(&self.db_path);
    }
}

/// note(guozhihao-224) Inject one failure into the next save_meta for the DB.
#[cfg(test)]
#[doc(hidden)]
#[must_use]
pub fn fail_next_sm_meta_save(db: &DB) -> SmMetaSaveFailureGuard {
    let db_path = db.path().to_path_buf();
    assert!(
        test_hooks::SM_META_SAVE_FAILURES
            .lock()
            .insert(db_path.clone()),
        "sm_meta save failure already registered for {}",
        db_path.display()
    );
    SmMetaSaveFailureGuard { db_path }
}

pub struct DurableStateMachineStore {
    db: Arc<DB>,
}

impl DurableStateMachineStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    #[cfg(test)]
    pub(crate) fn db(&self) -> Arc<DB> {
        Arc::clone(&self.db)
    }

    fn cf(&self) -> Result<Arc<BoundColumnFamily<'_>>, StorageError<u64>> {
        self.db
            .cf_handle(SM_META_CF)
            .ok_or_else(|| cf_not_found_read(SM_META_CF))
    }

    /// note(guozhihao-224) Synced WAL write; only called after the business
    /// mutation for the same entries has committed.
    pub fn save_meta(&self, meta: &DurableStateMachineMeta) -> Result<(), StorageError<u64>> {
        #[cfg(test)]
        if test_hooks::SM_META_SAVE_FAILURES
            .lock()
            .remove(&self.db.path().to_path_buf())
        {
            return Err(io_write_err(format!(
                "injected sm_meta save failure for {}",
                self.db.path().display()
            )));
        }

        let cf = self.cf()?;
        let bytes = serialize(meta)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf, SM_META_KEY, &bytes);

        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db.write_opt(&batch, &opts).map_err(io_write_err)
    }

    pub fn load_meta(&self) -> Result<Option<DurableStateMachineMeta>, StorageError<u64>> {
        let cf = self.cf()?;
        match self.db.get_cf(&cf, SM_META_KEY).map_err(io_read_err)? {
            None => Ok(None),
            Some(bytes) => deserialize(&bytes).map(Some),
        }
    }

    /// note(guozhihao-224) Reject unknown format versions; None when nothing
    /// has been persisted yet (first start).
    pub fn validate(&self) -> Result<Option<DurableStateMachineMeta>, StorageError<u64>> {
        match self.load_meta()? {
            None => Ok(None),
            Some(meta) => {
                if meta.format_version != SM_META_FORMAT_VERSION {
                    return Err(io_read_err(format!(
                        "unsupported state machine metadata format version {}, expected {}",
                        meta.format_version, SM_META_FORMAT_VERSION
                    )));
                }
                Ok(Some(meta))
            }
        }
    }
}
