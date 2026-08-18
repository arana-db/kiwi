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

#[cfg(target_os = "linux")]
use std::ffi::OsString;
#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::io::{Read, Seek, SeekFrom};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "linux")]
use std::path::Path;

#[cfg(target_os = "linux")]
use kiwi_compat::oracle::OracleProvenance;

#[cfg(target_os = "linux")]
const MAX_PROVENANCE_BYTES: u64 = 1024 * 1024;
#[cfg(target_os = "linux")]
const MAX_EVIDENCE_BYTES: u64 = 128 * 1024 * 1024;

#[cfg(target_os = "linux")]
fn usage(program: &OsString) -> String {
    format!(
        "usage: {} PROVENANCE_FD EVIDENCE_FD EVIDENCE_FILE_NAME EXPECTED_HEAD EXPECTED_TREE",
        Path::new(program)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
    )
}

#[cfg(target_os = "linux")]
fn inherited_fd(value: OsString, label: &str) -> Result<RawFd, Box<dyn std::error::Error>> {
    let text = value
        .into_string()
        .map_err(|_| format!("{label} must be a decimal file descriptor"))?;
    let fd = text
        .parse::<RawFd>()
        .map_err(|_| format!("{label} must be a decimal file descriptor"))?;
    if fd <= libc::STDERR_FILENO {
        return Err(format!("{label} must not alias a standard stream").into());
    }
    Ok(fd)
}

#[cfg(target_os = "linux")]
fn inherited_file(fd: RawFd, label: &str) -> Result<File, Box<dyn std::error::Error>> {
    let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, libc::STDERR_FILENO + 1) };
    if duplicate < 0 {
        return Err(format!("{label} is not an open inherited file descriptor").into());
    }
    Ok(unsafe { File::from_raw_fd(duplicate) })
}

#[cfg(target_os = "linux")]
fn read_bounded(
    file: &mut File,
    label: &str,
    limit: u64,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let initial = file.metadata()?;
    if !initial.is_file() {
        return Err(format!("{label} is not a regular file").into());
    }
    if initial.nlink() != 0 {
        return Err(format!("{label} must be an anonymous sealed file").into());
    }
    let required_seals =
        libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    let initial_seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if initial_seals < 0 || initial_seals & required_seals != required_seals {
        return Err(format!("{label} is missing required write seals").into());
    }
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.by_ref().take(limit + 1).read_to_end(&mut bytes)?;
    let length = u64::try_from(bytes.len())?;
    if length > limit {
        return Err(format!("{label} exceeds the {limit}-byte limit").into());
    }
    let final_metadata = file.metadata()?;
    if !final_metadata.is_file() || final_metadata.len() != initial.len() || length != initial.len()
    {
        return Err(format!("{label} changed while it was read").into());
    }
    if (
        final_metadata.dev(),
        final_metadata.ino(),
        final_metadata.mode(),
        final_metadata.nlink(),
    ) != (
        initial.dev(),
        initial.ino(),
        initial.mode(),
        initial.nlink(),
    ) {
        return Err(format!("{label} identity changed while it was read").into());
    }
    let final_seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if final_seals < 0 || final_seals & required_seals != required_seals {
        return Err(format!("{label} seals changed while it was read").into());
    }
    Ok(bytes)
}

#[cfg(target_os = "linux")]
fn next_utf8(
    arguments: &mut impl Iterator<Item = OsString>,
    usage: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    arguments
        .next()
        .ok_or_else(|| usage.to_string())?
        .into_string()
        .map_err(|_| "expected Head and tree must be valid UTF-8".into())
}

#[cfg(target_os = "linux")]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os();
    let program = arguments.next().unwrap_or_default();
    let usage = usage(&program);
    let provenance_fd = inherited_fd(
        arguments.next().ok_or_else(|| usage.clone())?,
        "provenance FD",
    )?;
    let evidence_fd = inherited_fd(
        arguments.next().ok_or_else(|| usage.clone())?,
        "evidence FD",
    )?;
    if provenance_fd == evidence_fd {
        return Err("provenance and evidence FDs must be distinct".into());
    }
    let evidence_file_name = arguments
        .next()
        .ok_or_else(|| usage.clone())?
        .into_string()
        .map_err(|_| "evidence file name must be valid UTF-8")?;
    let evidence_name_path = Path::new(&evidence_file_name);
    if evidence_name_path
        .file_name()
        .and_then(|name| name.to_str())
        != Some(&evidence_file_name)
        || evidence_name_path.components().count() != 1
        || evidence_file_name == "."
        || evidence_file_name == ".."
    {
        return Err("evidence file name must be a single canonical basename".into());
    }
    let expected_head = next_utf8(&mut arguments, &usage)?;
    let expected_tree = next_utf8(&mut arguments, &usage)?;
    if arguments.next().is_some() {
        return Err(usage.into());
    }

    // The controller passes sealed memfd snapshots. Duplicate them before
    // constructing Files so arbitrary numeric argv cannot violate FD ownership.
    let mut provenance_file = inherited_file(provenance_fd, "provenance FD")?;
    let mut evidence_file = inherited_file(evidence_fd, "evidence FD")?;
    let provenance_bytes = read_bounded(
        &mut provenance_file,
        "inherited provenance",
        MAX_PROVENANCE_BYTES,
    )?;
    let provenance_source = std::str::from_utf8(&provenance_bytes)?;
    let provenance = OracleProvenance::from_json(provenance_source)?;
    let evidence = read_bounded(&mut evidence_file, "inherited evidence", MAX_EVIDENCE_BYTES)?;
    provenance.verify_external_bindings(
        &expected_head,
        &expected_tree,
        &evidence_file_name,
        &evidence,
    )?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    Err("Oracle publication binding verification requires inherited Unix file descriptors".into())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Oracle publication binding verification: {error}");
        std::process::exit(1);
    }
}
