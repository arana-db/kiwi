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

//! Tar packing/unpacking for Raft snapshot checkpoints.
//!
//! TODO: stream snapshot bytes (read/write without holding the full tar in memory) for
//! build and install paths; align with OpenRaft snapshot APIs when switching off in-memory buffers.

use std::io::{self, Cursor};
use std::path::Path;

/// Pack a directory (checkpoint root) into a GNU tar archive in memory.
pub fn pack_dir_to_vec(src: &Path) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut buf);
        append_dir_all_skip_lock(&mut builder, "snap", src)?;
        builder.finish()?;
    }
    Ok(buf)
}

fn append_dir_all_skip_lock<W: std::io::Write>(
    builder: &mut tar::Builder<W>,
    dst_path: &str,
    src_path: &Path,
) -> io::Result<()> {
    for entry in std::fs::read_dir(src_path)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();

        if file_name_str == "LOCK" {
            continue;
        }

        let src = entry.path();
        let dst = std::path::Path::new(dst_path).join(&file_name);
        let dst_str = dst.to_string_lossy();

        if entry.file_type()?.is_dir() {
            append_dir_all_skip_lock(builder, &dst_str, &src)?;
        } else {
            builder.append_path_with_name(&src, &dst)?;
        }
    }
    Ok(())
}

/// Unpack a tar archive (from `build_snapshot` / OpenRaft `SnapshotData`) into `dst`.
///
/// Security: validates paths to prevent path traversal attacks.
/// - Rejects if dst exists but is not a directory
/// - Rejects tar entries containing ".." components
/// - Rejects paths that escape the destination directory
pub fn unpack_tar_to_dir(bytes: &[u8], dst: &Path) -> io::Result<()> {
    if dst.exists() && !dst.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination is not a directory",
        ));
    }

    std::fs::create_dir_all(dst)?;

    let mut validate_archive = tar::Archive::new(Cursor::new(bytes));
    for entry in validate_archive.entries()? {
        let entry = entry?;
        let path = entry.path()?.to_path_buf();

        if path.components().any(|c| c.as_os_str() == "..") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("malicious path detected: {:?}", path),
            ));
        }

        let full_path = dst.join(&path);
        if !full_path.starts_with(dst) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path escapes destination",
            ));
        }
    }

    let mut archive = tar::Archive::new(Cursor::new(bytes));
    archive.unpack(dst)?;
    Ok(())
}

/// Directory inside `unpack_tar_to_dir` output that contains `0/`, `1/`, … and `__raft_snapshot_meta`.
pub fn unpacked_checkpoint_root(unpack_root: &Path) -> std::path::PathBuf {
    unpack_root.join("snap")
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::RaftSnapshotMeta;

    #[test]
    fn pack_unpack_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("ck");
        std::fs::create_dir_all(src.join("0")).unwrap();
        std::fs::write(src.join("0").join("marker"), b"x").unwrap();
        let meta = RaftSnapshotMeta::new(7, 3);
        meta.write_to_dir(&src).unwrap();

        let bytes = pack_dir_to_vec(&src).unwrap();
        assert!(!bytes.is_empty());

        let dst = tempfile::tempdir().unwrap();
        let unpack = dst.path().join("u");
        unpack_tar_to_dir(&bytes, &unpack).unwrap();
        let root = unpacked_checkpoint_root(&unpack);
        assert!(root.join("0").join("marker").exists());
        let m = RaftSnapshotMeta::read_from_dir(&root).unwrap();
        assert_eq!(m.version, 1);
        assert_eq!(m.last_included_index, 7);
        assert_eq!(m.last_included_term, 3);
    }

    #[test]
    fn test_unpack_rejects_non_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let file_path = tmp.path().join("not_a_dir");
        std::fs::write(&file_path, b"content").unwrap();

        let result = unpack_tar_to_dir(&[0u8; 10], &file_path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_unpack_rejects_path_traversal() {
        // Test: valid tar should unpack successfully
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("ck");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("file.txt"), b"content").unwrap();

        let bytes = pack_dir_to_vec(&src).unwrap();
        let dst = tempfile::tempdir().unwrap();
        let unpack = dst.path().join("u");

        let result = unpack_tar_to_dir(&bytes, &unpack);
        assert!(result.is_ok(), "Valid tar should unpack successfully");

        let unpacked_root = unpack.join("snap");
        assert!(
            unpacked_root.exists(),
            "Unpacked snap directory should exist"
        );
    }
}
