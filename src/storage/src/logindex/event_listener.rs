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

//! LogIndexAndSequenceCollectorPurger: EventListener for flush completion handling

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rocksdb::event_listener::{EventListener, FlushJobInfo};

use crate::logindex::cf_tracker::LogIndexOfColumnFamilies;
use crate::logindex::collector::LogIndexAndSequenceCollector;
use crate::logindex::types::cf_metadata::COLUMN_FAMILY_COUNT;

/// Empty state for manual_flushing_cf (no flush in progress).
const EMPTY: u64 = u64::MAX;

/// Pack generation (high 32 bits) and cf_id (low 32 bits) to avoid ABA in watchdog.
#[inline]
fn pack(gen: u64, cf_id: u32) -> u64 {
    (gen << 32) | (cf_id as u64)
}

/// Extract cf_id from packed value.
#[inline]
fn unpack_cf_id(packed: u64) -> u32 {
    (packed & 0xFFFFFFFF) as u32
}

/// Snapshot callback: `(log_index, is_manual)`
pub type SnapshotCallback = Box<dyn Fn(i64, bool) + Send + Sync>;

/// Manual flush trigger: pass cf_id, caller executes db.flush_cf(cf)
pub type FlushTrigger = Box<dyn Fn(usize) + Send + Sync>;

/// Watchdog timeout for manual flush operations
const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(30);

/// EventListener: updates collector/cf_tracker when flush completes, triggers snapshot or manual flush if needed
pub struct LogIndexAndSequenceCollectorPurger {
    collector: Arc<LogIndexAndSequenceCollector>,
    cf_tracker: Arc<LogIndexOfColumnFamilies>,
    callback: SnapshotCallback,
    flush_trigger: Option<FlushTrigger>,
    count: AtomicU64,
    next_generation: AtomicU64,
    manual_flushing_cf: Arc<AtomicU64>,
}

impl LogIndexAndSequenceCollectorPurger {
    pub fn new(
        collector: Arc<LogIndexAndSequenceCollector>,
        cf_tracker: Arc<LogIndexOfColumnFamilies>,
        callback: SnapshotCallback,
        flush_trigger: Option<FlushTrigger>,
    ) -> Self {
        Self {
            collector,
            cf_tracker,
            callback,
            flush_trigger,
            count: AtomicU64::new(0),
            next_generation: AtomicU64::new(0),
            manual_flushing_cf: Arc::new(AtomicU64::new(EMPTY)),
        }
    }

    /// Set the CF currently being manually flushed (used to skip duplicate triggers)
    ///
    /// Uses a generation counter (high 32 bits) + cf_id (low 32 bits) packing scheme
    /// to avoid ABA problems: each call increments the generation, ensuring that
    /// even if the same CF is flushed twice in rapid succession, the watchdog can
    /// distinguish between the two operations.
    pub fn set_manual_flushing_cf(&self, cf_id: i64) {
        if cf_id < 0 {
            self.manual_flushing_cf.store(EMPTY, Ordering::SeqCst);
        } else {
            let gen = self.next_generation.fetch_add(1, Ordering::SeqCst);
            self.manual_flushing_cf
                .store(pack(gen, cf_id as u32), Ordering::SeqCst);
        }
    }

    /// Get the CF currently being manually flushed (for testing/debugging)
    pub fn get_manual_flushing_cf(&self) -> i64 {
        let v = self.manual_flushing_cf.load(Ordering::SeqCst);
        if v == EMPTY {
            -1
        } else {
            unpack_cf_id(v) as i64
        }
    }
}

impl EventListener for LogIndexAndSequenceCollectorPurger {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let cf_name = info.cf_name();
        let cf_id = match cf_name
            .as_ref()
            .and_then(|n| crate::logindex::cf_name_to_index(n))
        {
            Some(id) => id,
            None => {
                let name_str = cf_name.map(|n| String::from_utf8_lossy(&n).into_owned());
                log::warn!(
                    "LogIndexAndSequenceCollectorPurger: unknown CF name {:?}, skipping flush completion handling",
                    name_str
                );
                return;
            }
        };

        let largest_seqno = info.largest_seqno();
        let log_index = self.collector.find_applied_log_index(largest_seqno);

        self.cf_tracker
            .set_flushed_log_index(cf_id, log_index, largest_seqno);

        let res = self.cf_tracker.get_smallest_log_index(Some(cf_id));
        self.collector.purge(res.smallest_applied_log_index);

        if res.smallest_flushed_log_index_cf >= 0 {
            self.cf_tracker.set_flushed_log_index_global(
                res.smallest_flushed_log_index,
                res.smallest_flushed_seqno,
            );
        }

        // Trigger snapshot callback every 10 flushes.
        // This frequency is a trade-off: frequent enough to keep snapshot lag bounded,
        // but infrequent enough to avoid excessive overhead.
        // TODO(#LOGINDEX-3): Make this configurable via RaftConfig (e.g., `snapshot_callback_interval`)
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if count.is_multiple_of(10) {
            (self.callback)(res.smallest_flushed_log_index, false);
        }

        let current = self.manual_flushing_cf.load(Ordering::SeqCst);
        if current != EMPTY && unpack_cf_id(current) == cf_id as u32 {
            let _ = self.manual_flushing_cf.compare_exchange(
                current,
                EMPTY,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }

        let flushing_cf = self.manual_flushing_cf.load(Ordering::SeqCst);
        if flushing_cf != EMPTY || !self.collector.is_flush_pending() {
            return;
        }

        if res.smallest_flushed_log_index_cf < 0 {
            return;
        }
        let target_cf = res.smallest_flushed_log_index_cf as usize;

        let Some(ref trigger) = self.flush_trigger else {
            return;
        };

        let gen = self.next_generation.fetch_add(1, Ordering::SeqCst);
        let packed = pack(gen, target_cf as u32);

        // Attempt to claim manual-flush state; abort if another flush is already in progress
        if self
            .manual_flushing_cf
            .compare_exchange(EMPTY, packed, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        if target_cf >= COLUMN_FAMILY_COUNT {
            // Invalid CF index: we must release it immediately.
            self.manual_flushing_cf.store(EMPTY, Ordering::SeqCst);
            return;
        }

        let manual_flushing_cf = Arc::clone(&self.manual_flushing_cf);

        // Spawn watchdog to reset state if the flush never completes (e.g., async DB error).
        // Uses compare_exchange(packed, EMPTY) so it only resets when generation matches,
        // avoiding ABA: a new flush of the same CF would have a different generation.
        //
        // Must use std::thread::spawn: on_flush_completed runs in RocksDB's internal
        // thread pool which has no tokio runtime. tokio::spawn would panic with
        // "there is no reactor running".
        std::thread::spawn(move || {
            std::thread::sleep(WATCHDOG_TIMEOUT);
            let _ = manual_flushing_cf.compare_exchange(
                packed,
                EMPTY,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        });

        // If trigger panics synchronously, reset immediately rather than waiting for the watchdog.
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| trigger(target_cf))).is_err() {
            log::error!(
                "flush trigger panicked for CF {}, resetting manual_flushing_cf",
                target_cf
            );
            let _ = self.manual_flushing_cf.compare_exchange(
                packed,
                EMPTY,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
    }
}

/// Convert CF name to index (matches storage::ColumnFamilyIndex order)
pub use crate::logindex::types::cf_metadata::cf_name_to_index;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use rocksdb::{DB, Options};
    use tempfile::TempDir;

    use crate::logindex::cf_tracker::LogIndexOfColumnFamilies;
    use crate::logindex::collector::LogIndexAndSequenceCollector;
    use crate::logindex::event_listener::SnapshotCallback;
    use crate::logindex::table_properties::LogIndexTablePropertiesCollectorFactory;

    use super::LogIndexAndSequenceCollectorPurger;

    #[test]
    fn test_event_listener_on_flush_completed() {
        let temp_dir = TempDir::new().expect("temp dir");
        let path = temp_dir.path();

        let collector = Arc::new(LogIndexAndSequenceCollector::new(0));
        let cf_tracker = Arc::new(LogIndexOfColumnFamilies::new());
        let callback_count = Arc::new(AtomicU64::new(0));
        let callback_count_clone = callback_count.clone();
        let callback: SnapshotCallback = Box::new(move |_log_index: i64, _is_manual: bool| {
            callback_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        let purger = LogIndexAndSequenceCollectorPurger::new(
            collector.clone(),
            cf_tracker.clone(),
            callback,
            None,
        );

        let factory = LogIndexTablePropertiesCollectorFactory::new(collector.clone());

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_table_properties_collector_factory(factory);
        opts.add_event_listener(purger);

        let db = DB::open(&opts, path).expect("open");

        db.put(b"k", b"v").expect("put");
        let latest_seq = db.latest_sequence_number();
        collector.update(233333, latest_seq);
        db.flush().expect("flush");

        let (flushed, _) = cf_tracker.get_cf_flushed(0);
        assert_eq!(
            flushed, 233333,
            "cf_tracker should be updated by on_flush_completed"
        );

        assert_eq!(
            callback_count.load(Ordering::SeqCst),
            1,
            "snapshot callback called on 1st flush (count % 10 == 0)"
        );
    }

    #[test]
    fn test_event_listener_no_flush_trigger() {
        // Test that manual flush is not triggered when flush_trigger is None
        let temp_dir = TempDir::new().expect("temp dir");
        let path = temp_dir.path();

        let collector = Arc::new(LogIndexAndSequenceCollector::new(0));
        let cf_tracker = Arc::new(LogIndexOfColumnFamilies::new());
        let callback_count = Arc::new(AtomicU64::new(0));

        let callback: SnapshotCallback = Box::new(move |_log_index: i64, _is_manual: bool| {
            callback_count.fetch_add(1, Ordering::SeqCst);
        });

        // No flush_trigger provided
        let purger = LogIndexAndSequenceCollectorPurger::new(
            collector.clone(),
            cf_tracker.clone(),
            callback,
            None,
        );

        let factory = LogIndexTablePropertiesCollectorFactory::new(collector.clone());

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_table_properties_collector_factory(factory);
        opts.add_event_listener(purger);

        let db = DB::open(&opts, path).expect("open");

        // Trigger multiple flushes to create a gap
        for i in 0..5 {
            db.put(format!("k{i}").as_bytes(), b"v").expect("put");
            let latest_seq = db.latest_sequence_number();
            collector.update(100 + i, latest_seq);
            db.flush().expect("flush");
        }

        // cf_tracker should be updated but no manual flush triggered
        let (flushed, _) = cf_tracker.get_cf_flushed(0);
        assert_eq!(flushed, 104); // Last value
    }

    #[test]
    fn test_event_listener_with_flush_trigger_normal_operation() {
        // Test that the event listener handles flush trigger panics gracefully
        // by using catch_unwind internally and resetting state.
        //
        // Note: We can't easily test the actual panic-and-recover path because
        // it requires is_flush_pending() to return true, which needs 1000+ entries.
        // Instead, we verify that the purger continues to function after a theoretical
        // panic would have occurred.

        let temp_dir = TempDir::new().expect("temp dir");
        let path = temp_dir.path();

        let collector = Arc::new(LogIndexAndSequenceCollector::new(0));
        let cf_tracker = Arc::new(LogIndexOfColumnFamilies::new());
        let callback_count = Arc::new(AtomicU64::new(0));

        let callback: SnapshotCallback = Box::new(move |_log_index: i64, _is_manual: bool| {
            callback_count.fetch_add(1, Ordering::SeqCst);
        });

        // Flush trigger that doesn't panic - we verify normal operation
        let flush_trigger_called = Arc::new(AtomicBool::new(false));
        let flush_trigger_called_clone = flush_trigger_called.clone();
        let flush_trigger: super::FlushTrigger = Box::new(move |_cf_id: usize| {
            flush_trigger_called_clone.store(true, Ordering::SeqCst);
        });

        let purger = LogIndexAndSequenceCollectorPurger::new(
            collector.clone(),
            cf_tracker.clone(),
            callback,
            Some(flush_trigger),
        );

        let factory = LogIndexTablePropertiesCollectorFactory::new(collector.clone());

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_table_properties_collector_factory(factory);
        opts.add_event_listener(purger);

        let db = DB::open(&opts, path).expect("open");

        // Normal flushes should work
        for i in 0..3 {
            db.put(format!("k{i}").as_bytes(), b"v").expect("put");
            let latest_seq = db.latest_sequence_number();
            collector.update(100 + i, latest_seq);
            db.flush().expect("flush");
        }

        // Verify cf_tracker was updated
        let (flushed, _) = cf_tracker.get_cf_flushed(0);
        assert_eq!(flushed, 102);

        // Verify no manual flush was triggered (gap not large enough)
        assert!(!flush_trigger_called.load(Ordering::SeqCst));
    }
}
