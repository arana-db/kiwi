//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
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

//! Utility functions and data structures for the storage engine

use std::fs;
use std::io;
use std::path::Path;

pub fn is_dir<P: AsRef<Path>>(path: P) -> io::Result<bool> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.is_dir())
}

/// Creates a directory and all its parent directories with the specified mode.
/// This corresponds to the 'mkpath' functionality.
pub fn mkdir_with_path<P: AsRef<Path>>(path: P, mode: u32) -> io::Result<()> {
    // Use the fs::create_dir_all method to create the directory path.
    // It does not handle mode settings, so additional steps are required to set modes.
    fs::create_dir_all(&path)?;

    // Optionally, we can set the mode using 'chmod' if the platform supports it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(mode))?;
    }

    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    #[test]
    fn test_is_dir() {
        let dir_path = "test_dir";
        fs::create_dir(dir_path).unwrap();
        let result = is_dir(dir_path);
        assert!(result.is_ok());
        assert!(result.unwrap());

        fs::remove_dir(dir_path).unwrap();
    }

    #[test]
    fn test_mkdir_with_path() {
        let dir_path = "nested/test/dir";
        let mode = 0o755; // Unix specific mode (read, write, exec for owner; read, exec for others)
        let result = mkdir_with_path(dir_path, mode);
        assert!(result.is_ok());
        // Check if the directory exists and the permissions are set correctly
        let metadata = fs::metadata(dir_path).unwrap();
        assert!(metadata.permissions().mode() & 0o777 == mode);
        // Clean up
        fs::remove_dir_all("nested").unwrap();
    }

    #[test]
    fn test_delete_dir() {
        let dir_path = "test_delete_dir";
        fs::create_dir_all(format!("{}/subdir", dir_path)).unwrap();
        fs::File::create(format!("{}/subdir/file.txt", dir_path))
            .unwrap()
            .write_all(b"data")
            .unwrap();
        assert!(Path::new(&format!("{}/subdir", dir_path)).exists());
        assert!(Path::new(&format!("{}/subdir/file.txt", dir_path)).exists());
        let result = delete_dir(dir_path);
        assert!(result.is_ok());

        assert!(!Path::new(dir_path).exists());
    }
}
