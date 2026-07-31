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

use std::sync::atomic::{AtomicBool, Ordering};

/// Switchboard for vector storage fault injection. One instance lives on
/// each `Redis`; tests arm and disarm flags between operations.
#[derive(Debug, Default)]
pub struct VectorFaultHooks {
    fail_meta_read: AtomicBool,
    fail_member_read: AtomicBool,
    fail_batch_commit: AtomicBool,
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

    /// Disarm every flag (test-only).
    pub fn reset(&self) {
        self.set_fail_meta_read(false);
        self.set_fail_member_read(false);
        self.set_fail_batch_commit(false);
    }
}
