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
// See the License for the language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use rocksdb::event_listener::{EventListener, FlushJobInfo};

use crate::cf_tracker::LogIndexOfColumnFamilies;
use crate::collector::LogIndexAndSequenceCollector;
use crate::{COLUMN_FAMILY_COUNT, cf_name_to_index};

/// Snapshot callback: `(log_index, is_manual)`
pub type SnapshotCallback = Box<dyn Fn(i64, bool) + Send + Sync>;

/// Manual flush trigger: pass cf_id, caller executes db.flush_cf(cf)
pub type FlushTrigger = Box<dyn Fn(usize) + Send + Sync>;

/// EventListener: updates collector/cf_tracker when flush completes, triggers snapshot or manual flush if needed
pub struct LogIndexAndSequenceCollectorPurger {
    collector: std::sync::Arc<LogIndexAndSequenceCollector>,
    cf_tracker: std::sync::Arc<LogIndexOfColumnFamilies>,
    callback: SnapshotCallback,
    flush_trigger: Option<FlushTrigger>,
    count: AtomicU64,
    manual_flushing_cf: AtomicI64,
}

impl LogIndexAndSequenceCollectorPurger {
    pub fn new(
        collector: std::sync::Arc<LogIndexAndSequenceCollector>,
        cf_tracker: std::sync::Arc<LogIndexOfColumnFamilies>,
        callback: SnapshotCallback,
        flush_trigger: Option<FlushTrigger>,
    ) -> Self {
        Self {
            collector,
            cf_tracker,
            callback,
            flush_trigger,
            count: AtomicU64::new(0),
            manual_flushing_cf: AtomicI64::new(-1),
        }
    }

    /// Set the CF currently being manually flushed (used to skip duplicate triggers)
    pub fn set_manual_flushing_cf(&self, cf_id: i64) {
        self.manual_flushing_cf.store(cf_id, Ordering::SeqCst);
    }
}

impl EventListener for LogIndexAndSequenceCollectorPurger {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let cf_name = info.cf_name();
        let cf_id = match cf_name.as_ref().and_then(|n| cf_name_to_index(n)) {
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

        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if count.is_multiple_of(10) {
            (self.callback)(res.smallest_flushed_log_index, false);
        }

        if cf_id as i64 == self.manual_flushing_cf.load(Ordering::SeqCst) {
            self.manual_flushing_cf.store(-1, Ordering::SeqCst);
        }

        let flushing_cf = self.manual_flushing_cf.load(Ordering::SeqCst);
        if flushing_cf != -1 || !self.collector.is_flush_pending() {
            return;
        }

        if res.smallest_flushed_log_index_cf < 0 {
            return;
        }
        let target_cf = res.smallest_flushed_log_index_cf as usize;

        let Some(ref trigger) = self.flush_trigger else {
            return;
        };

        // Attempt to claim manual-flush state; abort if another flush is already in progress
        if self
            .manual_flushing_cf
            .compare_exchange(-1, target_cf as i64, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        if target_cf < COLUMN_FAMILY_COUNT {
            trigger(target_cf);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use rocksdb::{DB, Options};
    use tempfile::TempDir;

    use crate::cf_tracker::LogIndexOfColumnFamilies;
    use crate::collector::LogIndexAndSequenceCollector;
    use crate::table_properties::LogIndexTablePropertiesCollectorFactory;

    use super::LogIndexAndSequenceCollectorPurger;

    #[test]
    fn test_event_listener_on_flush_completed() {
        let temp_dir = TempDir::new().expect("temp dir");
        let path = temp_dir.path();

        let collector = std::sync::Arc::new(LogIndexAndSequenceCollector::new(0));
        let cf_tracker = std::sync::Arc::new(LogIndexOfColumnFamilies::new());
        let callback_count = std::sync::Arc::new(AtomicU64::new(0));
        let callback_count_clone = callback_count.clone();
        let callback = Box::new(move |_log_index: i64, _is_manual: bool| {
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
}
