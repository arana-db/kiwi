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

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const SOURCE_INPUTS: &[&str] = &[
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    ".cargo",
    "src",
    "tools/runtime-baseline/Cargo.toml",
    "tools/runtime-baseline/build.rs",
    "tools/runtime-baseline/build_support.rs",
    "tools/runtime-baseline/src",
    "tools/runtime-baseline/tests",
];
const SOURCE_EXCLUSIONS: &[&str] = &[
    ":(exclude,glob)**/.git",
    ":(exclude,glob)**/.git/**",
    ":(exclude,glob)**/target",
    ":(exclude,glob)**/target/**",
    ":(exclude,glob)**/results",
    ":(exclude,glob)**/results/**",
];

pub struct BuildIdentity {
    pub compiled_git_sha: String,
    pub source_dirty: bool,
    pub rerun_paths: BTreeSet<PathBuf>,
}

#[allow(dead_code)]
pub struct GitMetadata {
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    pub rerun_paths: BTreeSet<PathBuf>,
}

impl BuildIdentity {
    pub fn collect(source_root: &Path, explicit_git_sha: Option<&str>) -> Result<Self, String> {
        let metadata = GitMetadata::discover(source_root)?;
        let git_sha = match explicit_git_sha {
            Some(git_sha) => git_sha.to_owned(),
            None => metadata
                .git_output(source_root, &["rev-parse", "HEAD"])?
                .trim()
                .to_owned(),
        };
        validate_git_sha(&git_sha, "KIWI_BASELINE_BUILD_GIT_SHA")?;

        let mut status_arguments = vec!["status", "--porcelain", "--untracked-files=all", "--"];
        status_arguments.extend(SOURCE_INPUTS);
        status_arguments.extend(SOURCE_EXCLUSIONS);
        let source_dirty = !metadata
            .git_output(source_root, &status_arguments)?
            .trim()
            .is_empty();
        let source_paths = source_rerun_paths(source_root, &metadata)?;
        let mut rerun_paths = metadata.rerun_paths;
        rerun_paths.extend(source_paths);

        Ok(Self {
            compiled_git_sha: git_sha.to_ascii_lowercase(),
            source_dirty,
            rerun_paths,
        })
    }
}

impl GitMetadata {
    pub fn discover(source_root: &Path) -> Result<Self, String> {
        let dot_git = source_root.join(".git");
        let git_dir = if dot_git.is_dir() {
            accessible_path(&dot_git, source_root)
        } else {
            let contents = fs::read_to_string(&dot_git)
                .map_err(|error| format!("cannot read {}: {error}", dot_git.display()))?;
            git_dir_from_gitfile(&dot_git, source_root, &contents)?
        };
        let commondir = git_dir.join("commondir");
        let common_dir = if commondir.is_file() {
            let contents = fs::read_to_string(&commondir)
                .map_err(|error| format!("cannot read {}: {error}", commondir.display()))?;
            let relative = contents.trim();
            if relative.is_empty() {
                return Err(format!("{} is empty", commondir.display()));
            }
            accessible_path(Path::new(relative), &git_dir)
        } else {
            git_dir.clone()
        };
        let head = git_dir.join("HEAD");
        let index = git_dir.join("index");
        let packed_refs = common_dir.join("packed-refs");
        let mut rerun_paths =
            BTreeSet::from([dot_git.clone(), head.clone(), index, commondir, packed_refs]);

        if let Some(reference) = symbolic_head_reference(&head)? {
            let reference = accessible_path(&reference, &common_dir);
            if let Some(parent) = reference.parent() {
                rerun_paths.insert(parent.to_path_buf());
            }
            rerun_paths.insert(reference);
        }

        Ok(Self {
            git_dir,
            common_dir,
            rerun_paths,
        })
    }

    fn git_output(&self, source_root: &Path, args: &[&str]) -> Result<String, String> {
        let output = run_git(source_root, self, args)?;
        String::from_utf8(output.stdout)
            .map_err(|error| format!("git output is not UTF-8: {error}"))
    }
}

pub fn git_dir_from_gitfile(
    dot_git: &Path,
    source_root: &Path,
    contents: &str,
) -> Result<PathBuf, String> {
    let git_dir = contents
        .trim()
        .strip_prefix("gitdir:")
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| format!("{} is not a valid Git gitfile", dot_git.display()))?;
    Ok(accessible_path(Path::new(git_dir), source_root))
}

pub fn accessible_path(path: &Path, relative_to: &Path) -> PathBuf {
    let raw = path.as_os_str().to_string_lossy();
    let resolved = if let Some(converted) = windows_path_to_wsl(&raw) {
        converted
    } else if path.is_absolute() {
        path.to_path_buf()
    } else {
        relative_to.join(path)
    };
    let accessible = resolved.canonicalize().unwrap_or(resolved);
    strip_windows_verbatim_prefix(accessible)
}

fn symbolic_head_reference(head: &Path) -> Result<Option<PathBuf>, String> {
    let contents = fs::read_to_string(head)
        .map_err(|error| format!("cannot read {}: {error}", head.display()))?;
    Ok(contents
        .trim()
        .strip_prefix("ref: ")
        .map(|reference| PathBuf::from(reference.trim())))
}

fn source_rerun_paths(
    source_root: &Path,
    metadata: &GitMetadata,
) -> Result<BTreeSet<PathBuf>, String> {
    let mut paths = SOURCE_INPUTS
        .iter()
        .map(|path| source_root.join(path))
        .collect::<BTreeSet<_>>();

    for command in [
        ["ls-files", "-z"].as_slice(),
        ["ls-files", "--others", "--exclude-standard", "-z"].as_slice(),
    ] {
        let mut arguments = command.to_vec();
        arguments.push("--");
        arguments.extend(SOURCE_INPUTS);
        arguments.extend(SOURCE_EXCLUSIONS);
        let output = metadata.git_output(source_root, &arguments)?;
        for path in output.split('\0').filter(|path| !path.is_empty()) {
            paths.insert(source_root.join(path));
        }
    }

    Ok(paths)
}

fn validate_git_sha(git_sha: &str, source: &str) -> Result<(), String> {
    if git_sha.len() != 40 || !git_sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!(
            "{source} must be a 40-character hexadecimal Git SHA"
        ));
    }
    Ok(())
}

fn run_git(source_root: &Path, metadata: &GitMetadata, args: &[&str]) -> Result<Output, String> {
    for candidate in git_candidates() {
        match Command::new(candidate)
            .arg(format!("--git-dir={}", metadata.git_dir.display()))
            .arg(format!("--work-tree={}", source_root.display()))
            .args(args)
            .current_dir(source_root)
            .output()
        {
            Ok(output) if output.status.success() => return Ok(output),
            Ok(_) | Err(_) => continue,
        }
    }

    Err(format!(
        "git {} failed in {}",
        args.join(" "),
        source_root.display()
    ))
}

#[cfg(unix)]
fn git_candidates() -> &'static [&'static str] {
    &["git", "git.exe"]
}

#[cfg(not(unix))]
fn git_candidates() -> &'static [&'static str] {
    &["git"]
}

#[cfg(unix)]
fn windows_path_to_wsl(raw: &str) -> Option<PathBuf> {
    let bytes = raw.as_bytes();
    if bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/')
    {
        let drive = (bytes[0] as char).to_ascii_lowercase();
        let suffix = raw[3..].replace('\\', "/");
        return Some(PathBuf::from(format!("/mnt/{drive}/{suffix}")));
    }

    None
}

#[cfg(not(unix))]
fn windows_path_to_wsl(_: &str) -> Option<PathBuf> {
    None
}

#[cfg(windows)]
fn strip_windows_verbatim_prefix(path: PathBuf) -> PathBuf {
    let raw = path.to_string_lossy();
    if let Some(path) = raw.strip_prefix(r"\\?\UNC\") {
        return PathBuf::from(format!(r"\\{path}"));
    }
    if let Some(path) = raw.strip_prefix(r"\\?\") {
        return PathBuf::from(path);
    }
    drop(raw);
    path
}

#[cfg(not(windows))]
fn strip_windows_verbatim_prefix(path: PathBuf) -> PathBuf {
    path
}
