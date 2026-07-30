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
// WITHOUT WARRANTIES OR CONDITIONS OF KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Request-local storage-layer instrumentation.
//!
//! These types were originally declared in `runtime::message` (see
//! <https://github.com/arana-db/kiwi/issues/312>). They live in the `client`
//! crate so that `storage`, `runtime` and `cmd` can share them without
//! creating a dependency cycle (`client` is a leaf crate that everything else
//! already depends on).
//!
//! The real collector ([`RealStorageStatsCollector`]) is installed once per
//! storage request via the [`STORAGE_STATS_COLLECTOR`] task-local and read by
//! the storage engine at the point where reads, writes and deletes actually
//! happen, so the byte/key counters reflect true I/O rather than values
//! inferred from Redis command arguments.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use serde::{Deserialize, Serialize};

/// Statistics about storage operations for monitoring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageStats {
    /// Number of keys read during the operation
    pub keys_read: u64,
    /// Number of keys written during the operation
    pub keys_written: u64,
    /// Number of keys deleted during the operation
    pub keys_deleted: u64,
    /// Size of data read in bytes (key bytes + value bytes)
    pub bytes_read: u64,
    /// Size of data written in bytes (key bytes + value bytes)
    pub bytes_written: u64,
    /// Size of data removed by delete operations in bytes
    pub bytes_deleted: u64,
    /// Whether the operation hit a cache (e.g. block/moka cache)
    pub cache_hit: bool,
    /// RocksDB compaction level accessed
    pub compaction_level: Option<u32>,
}

/// Request-local collector for storage-layer instrumentation.
///
/// Implementations must record the *actual* byte sizes measured by the storage
/// layer, not sizes inferred from Redis command arguments.
pub trait StorageStatsCollector: Send + Sync {
    /// Record a storage read. `key_bytes` and `value_bytes` are measured by the
    /// storage layer.
    fn record_read(&self, key_bytes: u64, value_bytes: u64);

    /// Record a storage write. `key_bytes` and `value_bytes` are measured after
    /// the storage layer accepts the mutation.
    fn record_write(&self, key_bytes: u64, value_bytes: u64);

    /// Record a storage delete. `key_bytes` is measured by the storage layer.
    fn record_delete(&self, key_bytes: u64);

    /// Return the accumulated statistics for the request.
    fn finish(&self) -> StorageStats;
}

/// Placeholder collector used until storage-layer instrumentation is wired in.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopStorageStatsCollector;

impl StorageStatsCollector for NoopStorageStatsCollector {
    fn record_read(&self, _key_bytes: u64, _value_bytes: u64) {}

    fn record_write(&self, _key_bytes: u64, _value_bytes: u64) {}

    fn record_delete(&self, _key_bytes: u64) {}

    fn finish(&self) -> StorageStats {
        StorageStats::default()
    }
}

/// A real per-request collector backed by lock-free atomic counters.
///
/// Safe to share across the (single) task that executes one storage request:
/// every field is an atomic and `finish` snapshots the accumulated values.
pub struct RealStorageStatsCollector {
    keys_read: AtomicU64,
    keys_written: AtomicU64,
    keys_deleted: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    bytes_deleted: AtomicU64,
    cache_hit: AtomicBool,
    compaction_level: StdMutex<Option<u32>>,
}

impl Default for RealStorageStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl RealStorageStatsCollector {
    /// Create an empty collector ready to accumulate one request's stats.
    pub fn new() -> Self {
        Self {
            keys_read: AtomicU64::new(0),
            keys_written: AtomicU64::new(0),
            keys_deleted: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_deleted: AtomicU64::new(0),
            cache_hit: AtomicBool::new(false),
            compaction_level: StdMutex::new(None),
        }
    }

    /// Mark that this request hit a cache (e.g. block/moka cache).
    pub fn mark_cache_hit(&self) {
        self.cache_hit.store(true, Ordering::Relaxed);
    }

    /// Record the RocksDB compaction level that served this request.
    pub fn set_compaction_level(&self, level: u32) {
        *self.compaction_level.lock().expect("compaction_level poisoned") = Some(level);
    }
}

impl StorageStatsCollector for RealStorageStatsCollector {
    fn record_read(&self, key_bytes: u64, value_bytes: u64) {
        self.keys_read.fetch_add(1, Ordering::Relaxed);
        self.bytes_read
            .fetch_add(key_bytes.saturating_add(value_bytes), Ordering::Relaxed);
    }

    fn record_write(&self, key_bytes: u64, value_bytes: u64) {
        self.keys_written.fetch_add(1, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(key_bytes.saturating_add(value_bytes), Ordering::Relaxed);
    }

    fn record_delete(&self, key_bytes: u64) {
        self.keys_deleted.fetch_add(1, Ordering::Relaxed);
        self.bytes_deleted.fetch_add(key_bytes, Ordering::Relaxed);
    }

    fn finish(&self) -> StorageStats {
        StorageStats {
            keys_read: self.keys_read.load(Ordering::Relaxed),
            keys_written: self.keys_written.load(Ordering::Relaxed),
            keys_deleted: self.keys_deleted.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_deleted: self.bytes_deleted.load(Ordering::Relaxed),
            cache_hit: self.cache_hit.load(Ordering::Relaxed),
            compaction_level: *self.compaction_level.lock().expect("compaction_level poisoned"),
        }
    }
}

tokio::task_local! {
    /// Per-request storage stats collector, scoped for the duration of a single
    /// storage command execution. The storage layer reads it via
    /// [`try_collector`].
    pub static STORAGE_STATS_COLLECTOR: Arc<dyn StorageStatsCollector + Send + Sync>;
}

/// Returns the active request-local collector, if one was installed for the
/// current async task.
///
/// Returns `None` when no collector is scoped (e.g. unit tests that call the
/// storage engine directly, or background storage tasks), so callers can record
/// unconditionally without special-casing those paths.
pub fn try_collector() -> Option<Arc<dyn StorageStatsCollector + Send + Sync>> {
    STORAGE_STATS_COLLECTOR.try_with(|c| c.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_collector_accumulates_reads_writes_deletes() {
        let c = RealStorageStatsCollector::new();
        c.record_read(3, 5);
        c.record_write(3, 7);
        c.record_read(2, 4);
        c.record_delete(3);

        let stats = c.finish();
        assert_eq!(stats.keys_read, 2);
        assert_eq!(stats.keys_written, 1);
        assert_eq!(stats.keys_deleted, 1);
        assert_eq!(stats.bytes_read, (3 + 5) + (2 + 4));
        assert_eq!(stats.bytes_written, 3 + 7);
        assert_eq!(stats.bytes_deleted, 3);
        assert!(!stats.cache_hit);
    }

    #[test]
    fn noop_yields_empty_stats() {
        assert_eq!(NoopStorageStatsCollector.finish(), StorageStats::default());
    }

    #[test]
    fn scoped_collector_is_visible_to_try_collector() {
        // Mirrors how `storage_server` installs the collector and how the
        // storage facade reads it via `try_collector` — without needing a real
        // RocksDB instance.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let collector: Arc<dyn StorageStatsCollector + Send + Sync> =
                Arc::new(RealStorageStatsCollector::new());
            STORAGE_STATS_COLLECTOR
                .scope(Arc::clone(&collector), async {
                    if let Some(c) = try_collector() {
                        c.record_write(3, 5);
                        c.record_read(2, 4);
                    } else {
                        panic!("try_collector() must return the scoped collector");
                    }
                })
                .await;
            let stats = collector.finish();
            assert_eq!(stats.keys_written, 1);
            assert_eq!(stats.bytes_written, 8);
            assert_eq!(stats.keys_read, 1);
            assert_eq!(stats.bytes_read, 6);
        });
    }
}
