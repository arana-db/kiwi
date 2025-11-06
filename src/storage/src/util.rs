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
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    let thread_id = std::thread::current().id();

    std::env::temp_dir().join(format!(
        "kiwi-test-db-{}-{:?}-{}",
        pid, thread_id, timestamp
    ))
}
