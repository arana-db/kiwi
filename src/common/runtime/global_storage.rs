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

//! Global Storage wrapper using ArcSwap for hot-swapping Storage during snapshot installation.
//!
//! This module provides `GlobalStorage` which wraps `ArcSwap<Storage>`. All components
//! that need access to Storage should use `GlobalStorage::load()` to get the current
//! instance. During snapshot installation, `swap()` atomically switches to the new Storage.

use std::sync::Arc;

use arc_swap::ArcSwap;
use storage::storage::Storage;

/// Global storage wrapper that enables hot-swapping Storage during snapshot installation.
///
/// Uses `ArcSwap` internally to provide atomic swap operations. All holders
/// get the current Storage instance via `load()`, which is a cheap atomic read (~1-2ns).
pub struct GlobalStorage {
    inner: ArcSwap<Storage>,
}

impl GlobalStorage {
    /// Create a new GlobalStorage with the initial Storage instance.
    pub fn new(storage: Storage) -> Self {
        Self {
            inner: ArcSwap::from(Arc::new(storage)),
        }
    }

    /// Create from an existing Arc<Storage>.
    pub fn from_arc(storage: Arc<Storage>) -> Self {
        Self {
            inner: ArcSwap::from(storage),
        }
    }

    /// Get the current Storage instance.
    /// This is a cheap atomic read (~1-2ns overhead).
    pub fn load(&self) -> Arc<Storage> {
        self.inner.load_full()
    }

    /// Swap to a new Storage instance atomically.
    /// Used during snapshot installation to switch to restored data.
    pub fn swap(&self, new_storage: Arc<Storage>) {
        self.inner.swap(new_storage);
    }

    /// Get a reference to the underlying ArcSwap for direct access.
    /// Used by KiwiStateMachine which needs ArcSwap for install_snapshot.
    pub fn arc_swap(&self) -> &ArcSwap<Storage> {
        &self.inner
    }

    /// Get db_instance_num from current Storage.
    pub fn db_instance_num(&self) -> usize {
        self.load().db_instance_num
    }

    /// Get db_id from current Storage.
    pub fn db_id(&self) -> usize {
        self.load().db_id
    }
}

impl Clone for GlobalStorage {
    fn clone(&self) -> Self {
        Self {
            inner: ArcSwap::from(self.load()),
        }
    }
}