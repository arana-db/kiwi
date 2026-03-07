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

/// Raft log index
pub type LogIndex = i64;

pub type SequenceNumber = u64;

const ORDERING: Ordering = Ordering::SeqCst;

/// Value object: stores the binding relationship between (LogIndex, SequenceNumber)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogIndexAndSequencePair {
    applied_log_index: LogIndex,
    seqno: SequenceNumber,
}

impl LogIndexAndSequencePair {
    pub fn new(applied_log_index: LogIndex, seqno: SequenceNumber) -> Self {
        Self {
            applied_log_index,
            seqno,
        }
    }

    pub fn applied_log_index(&self) -> LogIndex {
        self.applied_log_index
    }

    pub fn seqno(&self) -> SequenceNumber {
        self.seqno
    }

    pub fn set_applied_log_index(&mut self, v: LogIndex) {
        self.applied_log_index = v;
    }

    pub fn set_seqno(&mut self, v: SequenceNumber) {
        self.seqno = v;
    }
}

/// Atomic (log_index, seqno), supports comparison based on seqno
#[derive(Debug, Default)]
pub struct LogIndexSeqnoPair {
    log_index: AtomicI64,
    seqno: AtomicU64,
}

impl LogIndexSeqnoPair {
    pub fn new(log_index: LogIndex, seqno: SequenceNumber) -> Self {
        Self {
            log_index: AtomicI64::new(log_index),
            seqno: AtomicU64::new(seqno),
        }
    }

    pub fn log_index(&self) -> LogIndex {
        self.log_index.load(ORDERING)
    }

    pub fn seqno(&self) -> SequenceNumber {
        self.seqno.load(ORDERING)
    }

    pub fn set(&self, log_index: LogIndex, seqno: SequenceNumber) {
        self.log_index.store(log_index, ORDERING);
        self.seqno.store(seqno, ORDERING);
    }

    /// Compare based on seqno
    pub fn eq_seqno(&self, other: &Self) -> bool {
        self.seqno.load(ORDERING) == other.seqno.load(ORDERING)
    }

    pub fn le_seqno(&self, other: &Self) -> bool {
        self.seqno.load(ORDERING) <= other.seqno.load(ORDERING)
    }

    pub fn ge_seqno(&self, other: &Self) -> bool {
        self.seqno.load(ORDERING) >= other.seqno.load(ORDERING)
    }

    pub fn lt_seqno(&self, other: &Self) -> bool {
        self.seqno.load(ORDERING) < other.seqno.load(ORDERING)
    }
}

impl Clone for LogIndexSeqnoPair {
    fn clone(&self) -> Self {
        Self::new(self.log_index(), self.seqno())
    }
}

impl PartialEq for LogIndexSeqnoPair {
    fn eq(&self, other: &Self) -> bool {
        self.eq_seqno(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_index_and_sequence_pair() {
        let mut p = LogIndexAndSequencePair::new(233333, 5);
        assert_eq!(p.applied_log_index(), 233333);
        assert_eq!(p.seqno(), 5);

        p.set_applied_log_index(100);
        p.set_seqno(10);
        assert_eq!(p.applied_log_index(), 100);
        assert_eq!(p.seqno(), 10);
    }

    #[test]
    fn test_log_index_seqno_pair() {
        let p = LogIndexSeqnoPair::new(100, 50);
        assert_eq!(p.log_index(), 100);
        assert_eq!(p.seqno(), 50);

        p.set(200, 60);
        assert_eq!(p.log_index(), 200);
        assert_eq!(p.seqno(), 60);

        let q = LogIndexSeqnoPair::new(0, 70);
        assert!(p.lt_seqno(&q));
        assert!(p.le_seqno(&q));
        assert!(!p.ge_seqno(&q));
        assert!(p != q);

        let r = LogIndexSeqnoPair::new(0, 60);
        assert!(p.eq_seqno(&r));
        assert!(p == r);
    }
}
