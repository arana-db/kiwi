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

use std::io;
use std::path::Path;

#[cfg(unix)]
use std::fs::File;

#[cfg(windows)]
use std::fs::OpenOptions;

/// Flush a directory so its file-name changes reach stable storage.
#[cfg(unix)]
pub fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Flush a directory so its file-name changes reach stable storage.
#[cfg(windows)]
pub fn sync_directory(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;

    // FlushFileBuffers requires GENERIC_WRITE. FILE_FLAG_BACKUP_SEMANTICS is
    // required for CreateFileW to return a directory handle.
    OpenOptions::new()
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?
        .sync_all()
}

#[cfg(not(any(unix, windows)))]
pub fn sync_directory(path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "durable directory synchronization is unsupported on this platform: {}",
            path.display()
        ),
    ))
}

/// Flush the directory containing `path`.
pub fn sync_parent_directory(path: &Path) -> io::Result<()> {
    sync_directory(path.parent().unwrap_or_else(|| Path::new(".")))
}

#[cfg(test)]
mod tests {
    use super::sync_directory;

    #[test]
    fn sync_directory_accepts_a_real_directory() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        sync_directory(directory.path()).expect("real directory should be synchronizable");
    }
}
