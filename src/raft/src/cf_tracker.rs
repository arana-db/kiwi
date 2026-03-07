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

use std::collections::BTreeSet;

use parking_lot::RwLock;

use crate::COLUMN_FAMILY_COUNT;
use crate::db_access::{DbCfAccess, Result};
use crate::table_properties::get_largest_log_index_from_collection;
use crate::types::{LogIndex, LogIndexSeqnoPair, SequenceNumber};

/// Each CF's applied (latest in memtable) and flushed (latest in SST)
struct LogIndexPair {
    applied_index: LogIndexSeqnoPair,
    flushed_index: LogIndexSeqnoPair,
}

impl Default for LogIndexPair {
    fn default() -> Self {
        Self {
            applied_index: LogIndexSeqnoPair::new(0, 0),
            flushed_index: LogIndexSeqnoPair::new(0, 0),
        }
    }
}

/// Return result of GetSmallestLogIndex
#[derive(Debug, Clone)]
pub struct SmallestIndexRes {
    pub smallest_applied_log_index_cf: i32,
    pub smallest_applied_log_index: LogIndex,
    pub smallest_flushed_log_index_cf: i32,
    pub smallest_flushed_log_index: LogIndex,
    pub smallest_flushed_seqno: SequenceNumber,
}

impl Default for SmallestIndexRes {
    fn default() -> Self {
        Self {
            smallest_applied_log_index_cf: -1,
            smallest_applied_log_index: i64::MAX,
            smallest_flushed_log_index_cf: -1,
            smallest_flushed_log_index: i64::MAX,
            smallest_flushed_seqno: u64::MAX,
        }
    }
}

/// Tracks log index state for all Column Families
pub struct LogIndexOfColumnFamilies {
    cf: RwLock<[LogIndexPair; COLUMN_FAMILY_COUNT]>,
    last_flush_index: LogIndexSeqnoPair,
}

impl Default for LogIndexOfColumnFamilies {
    fn default() -> Self {
        Self {
            cf: RwLock::new(std::array::from_fn(|_| LogIndexPair::default())),
            last_flush_index: LogIndexSeqnoPair::new(0, 0),
        }
    }
}

impl LogIndexOfColumnFamilies {
    fn is_valid_cf_id(cf_id: usize) -> bool {
        cf_id < COLUMN_FAMILY_COUNT
    }

    pub fn new() -> Self {
        Self::default()
    }

    /// Restore applied/flushed state from each CF's SST
    pub fn init<D: DbCfAccess>(&self, db: &D) -> Result<()> {
        for i in 0..COLUMN_FAMILY_COUNT {
            let collection = db.get_properties_of_all_tables_cf(i)?;
            if let Some(pair) = get_largest_log_index_from_collection(&collection) {
                let log_index = pair.applied_log_index();
                let seqno = pair.seqno();
                let cf = self.cf.write();
                cf[i].applied_index.set(log_index, seqno);
                cf[i].flushed_index.set(log_index, seqno);
            }
        }
        Ok(())
    }

    /// Get smallest applied/flushed log index (used for Purge, SetFlushedLogIndexGlobal)
    ///
    /// `flush_cf`: index of CF currently being flushed, `None` means none
    pub fn get_smallest_log_index(&self, flush_cf: Option<usize>) -> SmallestIndexRes {
        let mut res = SmallestIndexRes::default();
        let cf = self.cf.read();

        for i in 0..COLUMN_FAMILY_COUNT {
            let skip = flush_cf
                .is_some_and(|fc| i != fc && cf[i].flushed_index.ge_seqno(&cf[i].applied_index));
            if skip {
                continue;
            }

            let applied = cf[i].applied_index.log_index();
            let flushed = cf[i].flushed_index.log_index();
            let flushed_seqno = cf[i].flushed_index.seqno();

            if applied < res.smallest_applied_log_index {
                res.smallest_applied_log_index = applied;
                res.smallest_applied_log_index_cf = i as i32;
            }
            if flushed < res.smallest_flushed_log_index {
                res.smallest_flushed_log_index = flushed;
                res.smallest_flushed_seqno = flushed_seqno;
                res.smallest_flushed_log_index_cf = i as i32;
            }
        }
        res
    }

    pub fn set_flushed_log_index(&self, cf_id: usize, log_index: LogIndex, seqno: SequenceNumber) {
        if !Self::is_valid_cf_id(cf_id) {
            return;
        }
        let cf = self.cf.write();
        let li = cf[cf_id].flushed_index.log_index();
        let seq = cf[cf_id].flushed_index.seqno();
        cf[cf_id]
            .flushed_index
            .set(log_index.max(li), seqno.max(seq));
    }

    pub fn set_flushed_log_index_global(&self, log_index: LogIndex, seqno: SequenceNumber) {
        self.set_last_flush_index(log_index, seqno);
        let cf = self.cf.write();
        for i in 0..COLUMN_FAMILY_COUNT {
            if cf[i].flushed_index.le_seqno(&self.last_flush_index) {
                let flush_li = cf[i]
                    .flushed_index
                    .log_index()
                    .max(self.last_flush_index.log_index());
                let flush_seq = cf[i]
                    .flushed_index
                    .seqno()
                    .max(self.last_flush_index.seqno());
                cf[i].flushed_index.set(flush_li, flush_seq);
            }
        }
    }

    /// Whether cur_log_index has been applied (less than applied)
    pub fn is_applied(&self, cf_id: usize, cur_log_index: LogIndex) -> bool {
        if !Self::is_valid_cf_id(cf_id) {
            return false;
        }
        cur_log_index < self.cf.read()[cf_id].applied_index.log_index()
    }

    /// Update applied_index on write; if flushed==applied, also update flushed
    pub fn update(&self, cf_id: usize, cur_log_index: LogIndex, cur_seqno: SequenceNumber) {
        if !Self::is_valid_cf_id(cf_id) {
            return;
        }
        let cf = self.cf.write();
        if cf[cf_id].flushed_index.le_seqno(&self.last_flush_index)
            && cf[cf_id].flushed_index.eq_seqno(&cf[cf_id].applied_index)
        {
            let flush_li = cf[cf_id]
                .flushed_index
                .log_index()
                .max(self.last_flush_index.log_index());
            let flush_seq = cf[cf_id]
                .flushed_index
                .seqno()
                .max(self.last_flush_index.seqno());
            cf[cf_id].flushed_index.set(flush_li, flush_seq);
        }
        cf[cf_id].applied_index.set(cur_log_index, cur_seqno);
    }

    /// Whether there is a CF that needs flush (gap > 0)
    pub fn is_pending_flush(&self) -> bool {
        self.get_pending_flush_gap() > 0
    }

    /// max - min of all applied/flushed log indexes
    pub fn get_pending_flush_gap(&self) -> u64 {
        let cf = self.cf.read();
        let mut s: BTreeSet<LogIndex> = BTreeSet::new();
        for i in 0..COLUMN_FAMILY_COUNT {
            s.insert(cf[i].applied_index.log_index());
            s.insert(cf[i].flushed_index.log_index());
        }
        if s.is_empty() {
            return 0;
        }
        if s.len() == 1 {
            return 0;
        }
        let first = *s.first().unwrap();
        let last = *s.last().unwrap();
        last.saturating_sub(first) as u64
    }

    pub fn set_last_flush_index(&self, log_index: LogIndex, seqno: SequenceNumber) {
        let li = self.last_flush_index.log_index().max(log_index);
        let seq = self.last_flush_index.seqno().max(seqno);
        self.last_flush_index.set(li, seq);
    }

    pub fn get_last_flush_index(&self) -> LogIndexSeqnoPair {
        self.last_flush_index.clone()
    }

    /// Get state of specified CF (for testing or debugging)
    #[allow(dead_code)]
    pub fn get_cf_applied(&self, cf_id: usize) -> (LogIndex, SequenceNumber) {
        if !Self::is_valid_cf_id(cf_id) {
            return (0, 0);
        }
        let cf = self.cf.read();
        (
            cf[cf_id].applied_index.log_index(),
            cf[cf_id].applied_index.seqno(),
        )
    }

    #[allow(dead_code)]
    pub fn get_cf_flushed(&self, cf_id: usize) -> (LogIndex, SequenceNumber) {
        if !Self::is_valid_cf_id(cf_id) {
            return (0, 0);
        }
        let cf = self.cf.read();
        (
            cf[cf_id].flushed_index.log_index(),
            cf[cf_id].flushed_index.seqno(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CF_NAMES;
    use crate::LogIndexAndSequenceCollector;
    use crate::db_access::DbCfAccess;
    use rocksdb::{ColumnFamilyDescriptor, DB, Options};
    use std::sync::Arc;
    use tempfile::TempDir;

    struct MultiCfDbAccess<'a> {
        db: &'a DB,
    }

    impl DbCfAccess for MultiCfDbAccess<'_> {
        fn get_properties_of_all_tables_cf(
            &self,
            cf_id: usize,
        ) -> Result<rocksdb::table_properties::TablePropertiesCollection> {
            if cf_id < CF_NAMES.len() {
                let cf = self.db.cf_handle(CF_NAMES[cf_id]).expect("cf handle");
                self.db.get_properties_of_all_tables_cf(&cf)
            } else {
                self.db.get_properties_of_all_tables()
            }
        }
    }

    #[test]
    fn test_init_from_db() {
        let temp_dir = TempDir::new().expect("temp dir");
        let path = temp_dir.path();

        let collector = Arc::new(LogIndexAndSequenceCollector::new(0));
        let factory = crate::table_properties::LogIndexTablePropertiesCollectorFactory::new(
            collector.clone(),
        );

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_table_properties_collector_factory(factory);

        let cfs: Vec<ColumnFamilyDescriptor> = CF_NAMES
            .iter()
            .map(|n| ColumnFamilyDescriptor::new(*n, opts.clone()))
            .collect();

        let db = DB::open_cf_descriptors(&opts, path, cfs).expect("open");

        db.put(b"k", b"v").expect("put");
        db.flush().expect("flush");

        let latest_seq = db.latest_sequence_number();
        collector.update(233333, latest_seq);
        db.put(b"k2", b"v2").expect("put"); // new data to trigger second flush to generate new SST
        db.flush().expect("flush");

        let access = MultiCfDbAccess { db: &db };
        let cf_tracker = LogIndexOfColumnFamilies::new();
        cf_tracker.init(&access).expect("init");

        let (applied, _) = cf_tracker.get_cf_applied(0);
        let (flushed, _) = cf_tracker.get_cf_flushed(0);
        assert_eq!(applied, 233333);
        assert_eq!(flushed, 233333);
    }

    #[test]
    fn test_cf_tracker_update_and_get_smallest() {
        let cf = LogIndexOfColumnFamilies::new();
        for i in 0..COLUMN_FAMILY_COUNT {
            cf.update(i, 100 + i as i64, 1000 + i as u64);
        }
        cf.update(1, 50, 500);

        let res = cf.get_smallest_log_index(None);
        assert_eq!(res.smallest_applied_log_index, 50);
        assert_eq!(res.smallest_applied_log_index_cf, 1);
    }

    #[test]
    fn test_set_flushed_log_index() {
        let cf = LogIndexOfColumnFamilies::new();
        cf.update(0, 100, 1000);
        cf.set_flushed_log_index(0, 80, 800);
        let (applied, _) = cf.get_cf_applied(0);
        let (flushed, _) = cf.get_cf_flushed(0);
        assert_eq!(applied, 100);
        assert_eq!(flushed, 80); // max(0, 80) = 80
    }

    #[test]
    fn test_get_pending_flush_gap() {
        let cf = LogIndexOfColumnFamilies::new();
        cf.update(0, 10, 100);
        cf.update(1, 50, 500);
        cf.update(2, 30, 300);
        let gap = cf.get_pending_flush_gap();
        // set = {applied 10,50,30} ∪ {flushed 0,0,0} = {0,10,30,50}, gap = 50
        assert_eq!(gap, 50);
    }

    #[test]
    fn test_is_pending_flush() {
        let cf = LogIndexOfColumnFamilies::new();
        assert!(!cf.is_pending_flush()); // all 0, gap=0
        cf.update(0, 10, 100);
        assert!(cf.is_pending_flush()); // applied 10 vs flushed 0, gap 10
    }
}
