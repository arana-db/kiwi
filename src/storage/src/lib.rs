//  Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

/// Re-export storage error types for external use
pub mod error;

pub use error::{Result, StorageError};

/// Storage engine options
#[derive(Debug, Clone)]
pub struct StorageOptions {
    /// Path to the storage directory
    pub path: String,
    /// Whether to create the storage directory if it doesn't exist
    pub create_if_missing: bool,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            path: String::from("storage"),
            create_if_missing: true,
        }
    }
}