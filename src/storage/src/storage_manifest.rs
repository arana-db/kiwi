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

//! Per-instance storage manifest: the durable home of `storage_incarnation`
//! and the standalone-mode generation sequence generator.
//!
//! Each `Redis` (RocksDB) instance owns one manifest file living next to the
//! database files in the instance's data directory. A plain file (rather than
//! a reserved MetaCF key) is used so the manifest never interferes with
//! MetaCF scans, compaction filters, or FLUSHDB, and so it can be carried
//! into Raft snapshot checkpoints with a simple copy.
//!
//! - `storage_incarnation` is generated once when an empty database directory
//!   is first opened and never changes afterwards. It distinguishes data
//!   written by different storages (or by a rebuilt data directory) inside
//!   vector member keys.
//! - `next_generation` backs the monotonic generation sequence generator. The
//!   incremented value is persisted *before* a generation is handed out, so a
//!   restarted instance never reuses an allocated generation.
//!
//! A missing manifest on a non-empty database means the data predates this
//! mechanism (or the file was lost); opening then fails instead of silently
//! reinterpreting existing data.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rand::Rng;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};

use crate::durable_fs::sync_parent_directory;
use crate::error::{InvalidFormatSnafu, IoSnafu, Result};

pub const STORAGE_MANIFEST_FILE: &str = "__kiwi_storage_manifest";
const STORAGE_MANIFEST_VERSION: u32 = 1;
const FIRST_GENERATION: u64 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct ManifestFile {
    version: u32,
    storage_incarnation: u64,
    next_generation: u64,
}

pub(crate) struct StorageManifest {
    path: PathBuf,
    storage_incarnation: u64,
    next_generation: Mutex<u64>,
}

impl StorageManifest {
    /// Load the manifest for the instance stored in `db_dir`, creating one
    /// when the database is empty. `db_has_entries` must report whether any
    /// column family of the already-open database contains at least one key.
    pub(crate) fn open(db_dir: &Path, db_has_entries: bool) -> Result<Self> {
        let path = db_dir.join(STORAGE_MANIFEST_FILE);
        if path.exists() {
            return Self::read(&path);
        }

        ensure!(
            !db_has_entries,
            InvalidFormatSnafu {
                message: format!(
                    "storage manifest {} is missing but the database is not empty; \
                     refusing to reinterpret existing data",
                    path.display()
                )
            }
        );

        let storage_incarnation = rand::thread_rng().r#gen::<u64>().max(1);
        let manifest = Self {
            path,
            storage_incarnation,
            next_generation: Mutex::new(FIRST_GENERATION),
        };
        manifest.persist(FIRST_GENERATION)?;
        Ok(manifest)
    }

    pub(crate) fn storage_incarnation(&self) -> u64 {
        self.storage_incarnation
    }

    /// Allocate the next generation sequence. The incremented counter is
    /// persisted before the generation is returned, so allocations survive
    /// restarts and are never reused.
    pub(crate) fn allocate_generation(&self) -> Result<u64> {
        let mut next_generation = self
            .next_generation
            .lock()
            .expect("storage manifest mutex should not be poisoned");
        let generation = *next_generation;
        let successor = generation.checked_add(1).context(InvalidFormatSnafu {
            message: "generation sequence exhausted".to_string(),
        })?;
        self.persist(successor)?;
        *next_generation = successor;
        Ok(generation)
    }

    /// Copy the manifest file into `dir` (used when exporting a checkpoint so
    /// the snapshot carries the storage identity with the data).
    pub(crate) fn copy_to(&self, dir: &Path) -> Result<()> {
        let target = dir.join(STORAGE_MANIFEST_FILE);
        fs::copy(&self.path, &target).context(IoSnafu)?;
        fs::File::open(&target)
            .context(IoSnafu)?
            .sync_all()
            .context(IoSnafu)?;
        Ok(())
    }

    fn read(path: &Path) -> Result<Self> {
        let bytes = fs::read(path).context(IoSnafu)?;
        let file: ManifestFile = serde_json::from_slice(&bytes).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("invalid storage manifest {}: {error}", path.display()),
            }
            .build()
        })?;
        ensure!(
            file.version == STORAGE_MANIFEST_VERSION,
            InvalidFormatSnafu {
                message: format!(
                    "unsupported storage manifest version {} in {}",
                    file.version,
                    path.display()
                )
            }
        );
        ensure!(
            file.storage_incarnation != 0 && file.next_generation >= FIRST_GENERATION,
            InvalidFormatSnafu {
                message: format!(
                    "corrupt storage manifest {}: incarnation {}, next generation {}",
                    path.display(),
                    file.storage_incarnation,
                    file.next_generation
                )
            }
        );
        Ok(Self {
            path: path.to_path_buf(),
            storage_incarnation: file.storage_incarnation,
            next_generation: Mutex::new(file.next_generation),
        })
    }

    /// Atomically persist `next_generation` via write-temp-sync-rename, then
    /// sync the directory so the rename reaches stable storage.
    fn persist(&self, next_generation: u64) -> Result<()> {
        let file = ManifestFile {
            version: STORAGE_MANIFEST_VERSION,
            storage_incarnation: self.storage_incarnation,
            next_generation,
        };
        let json = serde_json::to_vec(&file).map_err(|error| {
            InvalidFormatSnafu {
                message: format!("failed to serialize storage manifest: {error}"),
            }
            .build()
        })?;

        let temp_path = self.path.with_extension("tmp");
        {
            let mut temp = fs::File::create(&temp_path).context(IoSnafu)?;
            temp.write_all(&json).context(IoSnafu)?;
            temp.sync_all().context(IoSnafu)?;
        }
        fs::rename(&temp_path, &self.path).context(IoSnafu)?;
        sync_parent_directory(&self.path).context(IoSnafu)?;
        Ok(())
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_is_created_for_empty_db_and_survives_reopen() {
        let dir = tempfile::tempdir().expect("temp dir");

        let created = StorageManifest::open(dir.path(), false).expect("create manifest");
        let incarnation = created.storage_incarnation();
        assert_ne!(incarnation, 0);
        assert!(dir.path().join(STORAGE_MANIFEST_FILE).exists());

        let reopened = StorageManifest::open(dir.path(), true).expect("reopen manifest");
        assert_eq!(reopened.storage_incarnation(), incarnation);
    }

    #[test]
    fn allocations_are_monotonic_and_persisted() {
        let dir = tempfile::tempdir().expect("temp dir");

        let manifest = StorageManifest::open(dir.path(), false).expect("create manifest");
        let first = manifest.allocate_generation().expect("allocate first");
        let second = manifest.allocate_generation().expect("allocate second");
        assert_eq!(first, FIRST_GENERATION);
        assert_eq!(second, FIRST_GENERATION + 1);

        // A reopen must never reuse an allocated generation.
        let reopened = StorageManifest::open(dir.path(), false).expect("reopen manifest");
        let third = reopened
            .allocate_generation()
            .expect("allocate after reopen");
        assert!(third > second);
    }

    #[test]
    fn missing_manifest_on_non_empty_db_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert!(StorageManifest::open(dir.path(), true).is_err());
    }

    #[test]
    fn corrupt_manifest_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        fs::write(dir.path().join(STORAGE_MANIFEST_FILE), b"not json").expect("write");
        assert!(StorageManifest::open(dir.path(), false).is_err());
    }
}
