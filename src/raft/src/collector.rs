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

use crate::types::{LogIndex, LogIndexAndSequencePair, SequenceNumber};
use parking_lot::RwLock;
use std::collections::VecDeque;
const DEFAULT_MAX_GAP: u64 = 1000;

/// Maintains an ordered (LogIndex, SequenceNumber) mapping queue
///
/// - Update: append with step_length_mask sampling
/// - FindAppliedLogIndex: binary search for log index corresponding to seqno
/// - Purge: clean up outdated entries after flush
pub struct LogIndexAndSequenceCollector {
    /// When step_length_bit=0, mask=0, i.e., full sampling; (1 << bit) - 1
    step_length_mask: u64,
    list: RwLock<VecDeque<LogIndexAndSequencePair>>,
    max_gap: u64,
}

impl LogIndexAndSequenceCollector {
    /// Create Collector
    ///
    /// When step_length_bit=0, full sampling; when >0, sample at 2^bit intervals to save memory
    pub fn new(step_length_bit: u8) -> Self {
        let step_length_mask = match step_length_bit {
            0 => 0,
            1..=63 => (1u64 << step_length_bit) - 1,
            _ => u64::MAX,
        };
        Self {
            step_length_mask,
            list: RwLock::new(VecDeque::new()),
            max_gap: DEFAULT_MAX_GAP,
        }
    }

    /// Binary search for log index corresponding to seqno (including seqno or earlier)
    ///
    /// seqno=0 (e.g., during compaction) returns 0
    pub fn find_applied_log_index(&self, seqno: SequenceNumber) -> LogIndex {
        if seqno == 0 {
            return 0;
        }
        let list = self.list.read();
        if list.is_empty() || seqno < list.front().unwrap().seqno() {
            return 0;
        }
        if seqno >= list.back().unwrap().seqno() {
            return list.back().unwrap().applied_log_index();
        }
        // partition_point: the first position where predicate is false
        // i.e., the index of the first element where seqno() > seqno
        let idx = list.partition_point(|p| p.seqno() <= seqno);
        list.get(idx.saturating_sub(1))
            .map(|p| p.applied_log_index())
            .unwrap_or(0)
    }

    /// Append new mapping (with step_length_mask sampling)
    ///
    /// Only append when (log_index & step_length_mask) == 0
    ///
    /// Note: LogIndex is i64 but expected to be non-negative in practice.
    /// Negative values will be skipped as they cast to large u64 values.
    pub fn update(
        &self,
        smallest_applied_log_index: LogIndex,
        smallest_flush_seqno: SequenceNumber,
    ) {
        if smallest_applied_log_index < 0 {
            return;
        }
        if (smallest_applied_log_index as u64 & self.step_length_mask) != 0 {
            return;
        }
        let mut list = self.list.write();
        list.push_back(LogIndexAndSequencePair::new(
            smallest_applied_log_index,
            smallest_flush_seqno,
        ));
    }

    /// Clean up outdated mappings after flush
    ///
    /// Strategy: keep at least one element <= smallest_applied_log_index to ensure correct lookup next flush.
    /// When the second element also satisfies the condition, the first can be safely removed.
    pub fn purge(&self, smallest_applied_log_index: LogIndex) {
        let mut list = self.list.write();
        // Keep at least one element <= smallest_applied_log_index
        // When list[1] <= threshold, list[0] can be safely removed
        while list.len() >= 2 && list[1].applied_log_index() <= smallest_applied_log_index {
            list.pop_front();
        }
    }

    /// Whether manual flush is needed (queue length >= max_gap)
    pub fn is_flush_pending(&self) -> bool {
        self.size() >= self.max_gap
    }

    /// Queue length
    pub fn size(&self) -> u64 {
        self.list.read().len() as u64
    }

    /// Set max_gap (for testing or tuning)
    #[allow(dead_code)]
    pub fn set_max_gap(&mut self, gap: u64) {
        self.max_gap = gap;
    }
}

impl Default for LogIndexAndSequenceCollector {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_applied_log_index_empty() {
        let c = LogIndexAndSequenceCollector::new(0);
        assert_eq!(c.find_applied_log_index(0), 0);
        assert_eq!(c.find_applied_log_index(100), 0);
    }

    #[test]
    fn test_find_applied_log_index_basic() {
        let c = LogIndexAndSequenceCollector::new(0);
        c.update(10, 100);
        c.update(20, 200);
        c.update(30, 300);

        assert_eq!(c.find_applied_log_index(0), 0);
        assert_eq!(c.find_applied_log_index(50), 0);
        assert_eq!(c.find_applied_log_index(100), 10);
        assert_eq!(c.find_applied_log_index(150), 10);
        assert_eq!(c.find_applied_log_index(200), 20);
        assert_eq!(c.find_applied_log_index(250), 20);
        assert_eq!(c.find_applied_log_index(300), 30);
        assert_eq!(c.find_applied_log_index(400), 30);
    }

    #[test]
    fn test_update_step_length_mask() {
        let c = LogIndexAndSequenceCollector::new(2); // mask = 3
        c.update(0, 100); // 0 & 3 == 0, add
        c.update(1, 101); // 1 & 3 != 0, skip
        c.update(4, 104); // 4 & 3 == 0, add
        assert_eq!(c.size(), 2);
        assert_eq!(c.find_applied_log_index(100), 0);
        assert_eq!(c.find_applied_log_index(104), 4);
    }

    #[test]
    fn test_purge() {
        let c = LogIndexAndSequenceCollector::new(0);
        c.update(10, 100);
        c.update(20, 200);
        c.update(30, 300);
        c.update(40, 400);

        // delete 10, keep 20,30,40 (second=20<=25 delete 10; second=30>25 stop)
        c.purge(25);
        assert_eq!(c.size(), 3);
        assert_eq!(c.find_applied_log_index(300), 30);
        assert_eq!(c.find_applied_log_index(250), 20);
    }

    #[test]
    fn test_is_flush_pending() {
        let mut c = LogIndexAndSequenceCollector::new(0);
        c.set_max_gap(3);
        assert!(!c.is_flush_pending());
        c.update(1, 1);
        c.update(2, 2);
        assert!(!c.is_flush_pending());
        c.update(3, 3);
        assert!(c.is_flush_pending());
    }
}
