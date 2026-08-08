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

//! Fault injection hooks for the vector set storage path.
//!
//! The runtime-level `FaultInjectionConfig` only describes network delay and
//! message drop semantics for the dual-runtime message channel, so it cannot
//! express RocksDB-layer failures inside storage. These hooks close that gap
//! for the vector set path: meta reads, member reads during a FLAT scan, and
//! the atomic meta+member WriteBatch commit.
//!
//! The hooks are compiled in unconditionally (instead of behind a cargo
//! feature) because the storage crate's own integration tests under
//! `src/storage/tests/` link against the library without `cfg(test)` and
//! without self-enabled features. The cost in production is three relaxed
//! atomic loads per vector operation with all flags off. Flags are meant to
//! be flipped by tests only; nothing in the server startup path sets them.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use chrono::Utc;

#[derive(Debug, Default)]
struct VectorVsimTestGateState {
    entered: bool,
    released: bool,
}

/// Deterministic integration-test barrier entered by VSIM while its key
/// session and RocksDB snapshot are both live.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct VectorVsimTestGate {
    state: Mutex<VectorVsimTestGateState>,
    changed: Condvar,
}

impl VectorVsimTestGate {
    /// Wait until a VSIM scan reaches the barrier.
    pub fn wait_until_entered(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.entered {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (next_state, timed_out) = self
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next_state;
            if timed_out.timed_out() && !state.entered {
                return false;
            }
        }
        true
    }

    /// Release the blocked VSIM scan.
    pub fn release(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.released = true;
        drop(state);
        self.changed.notify_all();
    }

    fn enter_and_wait(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

/// Switchboard for vector storage fault injection. One instance lives on
/// each `Redis`; tests arm and disarm flags between operations.
#[derive(Debug)]
pub struct VectorFaultHooks {
    fail_meta_read: AtomicBool,
    fail_member_read: AtomicBool,
    fail_batch_commit: AtomicBool,
    logical_now_micros_override: AtomicU64,
    vsim_scan_gate: Mutex<Option<Weak<VectorVsimTestGate>>>,
}

impl Default for VectorFaultHooks {
    fn default() -> Self {
        Self {
            fail_meta_read: AtomicBool::new(false),
            fail_member_read: AtomicBool::new(false),
            fail_batch_commit: AtomicBool::new(false),
            logical_now_micros_override: AtomicU64::new(0),
            vsim_scan_gate: Mutex::new(None),
        }
    }
}

impl VectorFaultHooks {
    /// Whether vector meta reads (MetaCF point lookups) should fail.
    pub fn fail_meta_read(&self) -> bool {
        self.fail_meta_read.load(Ordering::Relaxed)
    }

    /// Whether member reads during a FLAT scan should fail.
    pub fn fail_member_read(&self) -> bool {
        self.fail_member_read.load(Ordering::Relaxed)
    }

    /// Whether the meta+member WriteBatch commit should fail.
    pub fn fail_batch_commit(&self) -> bool {
        self.fail_batch_commit.load(Ordering::Relaxed)
    }

    /// Arm/disarm meta read failure (test-only).
    pub fn set_fail_meta_read(&self, enabled: bool) {
        self.fail_meta_read.store(enabled, Ordering::Relaxed);
    }

    /// Arm/disarm member read failure (test-only).
    pub fn set_fail_member_read(&self, enabled: bool) {
        self.fail_member_read.store(enabled, Ordering::Relaxed);
    }

    /// Arm/disarm batch commit failure (test-only).
    pub fn set_fail_batch_commit(&self, enabled: bool) {
        self.fail_batch_commit.store(enabled, Ordering::Relaxed);
    }

    /// Override the logical timestamp captured by new VSIM sessions.
    #[doc(hidden)]
    pub fn set_logical_now_micros_override(&self, logical_now_micros: Option<u64>) {
        self.logical_now_micros_override
            .store(logical_now_micros.unwrap_or(0), Ordering::Relaxed);
    }

    /// Install or remove the deterministic VSIM scan barrier.
    #[doc(hidden)]
    pub fn set_vsim_scan_gate(&self, gate: Option<Arc<VectorVsimTestGate>>) {
        let mut installed = self
            .vsim_scan_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *installed = gate.map(|gate| Arc::downgrade(&gate));
    }

    pub(crate) fn logical_now_micros(&self) -> u64 {
        let overridden = self.logical_now_micros_override.load(Ordering::Relaxed);
        if overridden != 0 {
            overridden
        } else {
            Utc::now().timestamp_micros() as u64
        }
    }

    pub(crate) fn block_vsim_scan_if_armed(&self) {
        let gate = self
            .vsim_scan_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(gate) = gate {
            gate.enter_and_wait();
        }
    }

    /// Disarm every flag (test-only).
    pub fn reset(&self) {
        self.set_fail_meta_read(false);
        self.set_fail_member_read(false);
        self.set_fail_batch_commit(false);
        self.set_logical_now_micros_override(None);
        self.set_vsim_scan_gate(None);
    }
}
