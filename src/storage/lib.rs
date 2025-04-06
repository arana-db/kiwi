//  Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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