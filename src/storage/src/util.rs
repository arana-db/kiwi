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

use std::fs;
use std::io;
use std::path::Path;

/// TODO: remove allow dead code
#[allow(dead_code)]
pub fn is_dir<P: AsRef<Path>>(path: P) -> io::Result<bool> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.is_dir())
}

/// Creates a directory and all its parent directories with the specified mode.
/// This corresponds to the 'mkpath' functionality.
/// TODO: remove allow dead code
#[allow(dead_code)]
pub fn mkdir_with_path<P: AsRef<Path>>(path: P, _mode: u32) -> io::Result<()> {
    // Use the fs::create_dir_all method to create the directory path.
    // It does not handle mode settings, so additional steps are required to set modes.
    fs::create_dir_all(&path)?;

    // Optionally, we can set the mode using 'chmod' if the platform supports it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(_mode))?;
    }

    Ok(())
}

/// TODO: remove allow dead code
#[allow(dead_code)]
pub fn delete_dir<P: AsRef<Path>>(dirname: P) -> io::Result<()> {
    let path = dirname.as_ref();

    // Open the directory.
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();

        // Skip '.' and '..'
        if let Some(name) = entry.file_name().to_str() {
            if name == "." || name == ".." {
                continue;
            }
        }

        // Check if the path is a directory or a file.
        if is_dir(&entry_path)? {
            // It's a directory, recurse into it.
            delete_dir(&entry_path)?;
        } else {
            // It's a file, remove it.
            fs::remove_file(&entry_path)?;
        }
    }

    // Finally, remove the main directory.
    fs::remove_dir(path)?;

    Ok(())
}

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

// requires:
// 1. pattern's length >= 2
// 2. tail character is '*'
// 3. other position's charactor cannot be *, ?, [,]
pub fn is_tail_wildcard(pattern: &str) -> bool {
    let bytes = pattern.as_bytes();
    let len = bytes.len();

    if len < 2 || bytes[len - 1] != b'*' {
        return false;
    }

    for &c in &bytes[..len - 1] {
        if c == b'*' || c == b'?' || c == b'[' || c == b']' {
            return false;
        }
    }

    true
}
