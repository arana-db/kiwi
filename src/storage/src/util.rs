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

//! Utility functions and data structures for the storage engine

pub fn unique_test_db_path() -> std::path::PathBuf {
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos();

    let thread_id = format!("{:?}", thread::current().id());
    let unique_name = format!(
        "kiwi-test-db-{}-{}",
        timestamp,
        thread_id.replace("ThreadId(", "").replace(")", "")
    );

    tempfile::tempdir()
        .expect("Failed to create temp dir")
        .path()
        .join(unique_name)
}

/// Safe cleanup function that handles Windows file locking issues
pub fn safe_cleanup_test_db(path: &std::path::Path) {
    if !path.exists() {
        return;
    }

    // Try multiple times with delays to handle Windows file locking
    for attempt in 0..5 {
        match std::fs::remove_dir_all(path) {
            Ok(_) => return,
            Err(e) => {
                if attempt == 4 {
                    // Last attempt failed, log the error but don't panic
                    eprintln!(
                        "Warning: Failed to cleanup test database at {:?}: {}",
                        path, e
                    );
                    return;
                }
                // Wait a bit before retrying
                std::thread::sleep(std::time::Duration::from_millis(100 * (attempt + 1)));
            }
        }
    }
}
