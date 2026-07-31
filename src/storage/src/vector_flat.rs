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

//! Runtime governance for FLAT (brute-force) vector similarity queries:
//! a concurrency gate, a cooperative cancellation token, and a scan guard
//! enforcing the deadline and scan budget.
//!
//! The pieces are synchronous by design: storage commands execute as
//! blocking code inside storage-runtime tasks and `Redis::vsim` is a plain
//! synchronous function, so a blocking semaphore matches every call site
//! without requiring an async context.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use conf::vector_config::VectorConfig;

use crate::error::{
    Result, VectorFlatQueryCancelledSnafu, VectorFlatQueryTimeoutSnafu,
    VectorFlatScanBudgetExceededSnafu,
};

/// Cooperative cancellation signal for an in-flight FLAT query.
///
/// Cheap to clone; the default token is never cancelled. No trigger source
/// is wired into the call chain yet (client-disconnect propagation is
/// follow-up work); callers that have one share a token and trip it via
/// [`FlatQueryCancel::cancel`].
#[derive(Debug, Clone, Default)]
pub struct FlatQueryCancel {
    flag: Arc<AtomicBool>,
}

impl FlatQueryCancel {
    /// Trip the signal; the owning query aborts at its next check.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct FlatQueryGateInner {
    available: Mutex<usize>,
    released: Condvar,
}

/// Counting semaphore bounding the number of concurrent FLAT scans.
#[derive(Debug)]
pub struct FlatQueryGate {
    inner: Arc<FlatQueryGateInner>,
}

/// RAII slot in a [`FlatQueryGate`]; dropping it returns the slot.
#[derive(Debug)]
pub struct FlatQueryPermit {
    inner: Arc<FlatQueryGateInner>,
}

impl Drop for FlatQueryPermit {
    fn drop(&mut self) {
        let mut available = self
            .inner
            .available
            .lock()
            .expect("flat query gate mutex poisoned");
        *available += 1;
        drop(available);
        self.inner.released.notify_one();
    }
}

impl FlatQueryGate {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(FlatQueryGateInner {
                available: Mutex::new(capacity.max(1)),
                released: Condvar::new(),
            }),
        }
    }

    /// Block until a slot is free or `deadline` passes; `None` on timeout,
    /// so queue wait counts against the caller's deadline.
    pub fn acquire(&self, deadline: Instant) -> Option<FlatQueryPermit> {
        let mut available = self
            .inner
            .available
            .lock()
            .expect("flat query gate mutex poisoned");
        loop {
            if *available > 0 {
                *available -= 1;
                return Some(FlatQueryPermit {
                    inner: Arc::clone(&self.inner),
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let (guard, _timeout) = self
                .inner
                .released
                .wait_timeout(available, deadline - now)
                .expect("flat query gate mutex poisoned");
            available = guard;
        }
    }

    /// Number of currently free slots.
    pub fn available_permits(&self) -> usize {
        *self
            .inner
            .available
            .lock()
            .expect("flat query gate mutex poisoned")
    }
}

/// Cooperative guard consulted while a FLAT scan streams candidates: the
/// scan budget is enforced on every entry, the deadline and cancellation
/// signal every `flat_cancel_check_interval` entries.
pub struct FlatScanGuard<'a> {
    deadline: Instant,
    cancel: &'a FlatQueryCancel,
    check_interval: u64,
    max_entries: u64,
    max_bytes: u64,
    entries: u64,
    bytes: u64,
}

impl<'a> FlatScanGuard<'a> {
    pub fn new(config: &VectorConfig, deadline: Instant, cancel: &'a FlatQueryCancel) -> Self {
        Self {
            deadline,
            cancel,
            check_interval: (config.flat_cancel_check_interval as u64).max(1),
            max_entries: config.flat_scan_max_entries,
            max_bytes: config.flat_scan_max_bytes,
            entries: 0,
            bytes: 0,
        }
    }

    /// Account one scanned entry (raw key/value bytes) and enforce the
    /// budget, deadline and cancellation policy.
    pub fn record(&mut self, key_bytes: usize, value_bytes: usize) -> Result<()> {
        self.entries += 1;
        self.bytes += (key_bytes + value_bytes) as u64;
        if self.entries > self.max_entries || self.bytes > self.max_bytes {
            return VectorFlatScanBudgetExceededSnafu.fail();
        }
        if self.entries.is_multiple_of(self.check_interval) {
            self.check_signals()?;
        }
        Ok(())
    }

    /// Check the deadline and cancellation signal without accounting an
    /// entry (used before the scan starts, right after gate acquisition).
    pub fn check_signals(&self) -> Result<()> {
        if self.cancel.is_cancelled() {
            return VectorFlatQueryCancelledSnafu.fail();
        }
        if Instant::now() >= self.deadline {
            return VectorFlatQueryTimeoutSnafu.fail();
        }
        Ok(())
    }
}
