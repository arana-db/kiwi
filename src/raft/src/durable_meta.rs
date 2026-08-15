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

//! Durable state machine metadata for crash-safe recovery.
//!
//! Persists the applied frontier (`last_applied`) and membership so that
//! after restart the state machine can recover its position without relying
//! on snapshot metadata alone or falling back to log index 0.

use openraft::{LogId, StoredMembership};
use serde::{Deserialize, Serialize};

use conf::raft_type::KiwiNode;

/// Current format version. Bump when the struct layout changes.
const DURABLE_META_VERSION: u32 = 1;

/// Persisted state machine metadata.
///
/// Written to the Raft log store's `state_cf` after each successful apply
/// and after snapshot install. On startup, loaded and cross-validated against
/// the snapshot metadata and log store boundaries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DurableStateMachineMeta {
    /// Format version for forward/backward compatibility.
    pub version: u32,
    /// Last log entry that was durably applied to storage.
    pub last_applied: Option<LogId<u64>>,
    /// Last membership configuration observed during apply.
    pub last_membership: StoredMembership<u64, KiwiNode>,
}

impl DurableStateMachineMeta {
    /// Create a new meta snapshot from the current state machine state.
    pub fn new(
        last_applied: Option<LogId<u64>>,
        last_membership: StoredMembership<u64, KiwiNode>,
    ) -> Self {
        Self {
            version: DURABLE_META_VERSION,
            last_applied,
            last_membership,
        }
    }

    /// Validate the metadata after deserialization.
    ///
    /// Returns `Err` with a human-readable description if the metadata is
    /// corrupt or from an unsupported future version. The caller must treat
    /// the node as unhealthy and refuse to provide strong-consistency service.
    pub fn validate(&self) -> Result<(), String> {
        if self.version == 0 || self.version > DURABLE_META_VERSION {
            return Err(format!(
                "unsupported durable meta version {} (expected 1..={})",
                self.version, DURABLE_META_VERSION,
            ));
        }
        Ok(())
    }
}
