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

#[path = "../build_support.rs"]
mod build_support;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use build_support::{BuildIdentity, GitMetadata};
use runtime_baseline::schema::Publishability;
use tempfile::TempDir;

#[cfg(unix)]
#[test]
fn parses_a_windows_gitfile_path_for_wsl() {
    let repository_root = Path::new("/repo");
    let dot_git = repository_root.join(".git");
    let git_dir = build_support::git_dir_from_gitfile(
        &dot_git,
        repository_root,
        "gitdir: D:\\repo\\.git\\worktrees\\baseline\n",
    )
    .expect("Windows gitfile path");

    assert_eq!(
        git_dir,
        PathBuf::from("/mnt/d/repo/.git/worktrees/baseline")
    );
}

#[cfg(windows)]
#[test]
fn accessible_git_paths_do_not_use_the_windows_verbatim_prefix() {
    let current_dir = std::env::current_dir().expect("current directory");
    let accessible = build_support::accessible_path(&current_dir.join(".git"), &current_dir);

    assert!(!accessible.to_string_lossy().starts_with(r"\\?\"));
}

#[test]
fn discovers_linked_worktree_metadata_from_gitfile_and_commondir() {
    let sandbox = TempDir::new().expect("temporary linked-worktree metadata");
    let common_dir = sandbox.path().join("repository/.git");
    let git_dir = common_dir.join("worktrees/baseline");
    let worktree = sandbox.path().join("linked-worktree");
    let branch_ref = common_dir.join("refs/heads/baseline");
    fs::create_dir_all(&git_dir).expect("worktree Git directory");
    fs::create_dir_all(branch_ref.parent().expect("branch ref parent"))
        .expect("branch ref directory");
    fs::create_dir(&worktree).expect("linked worktree directory");
    fs::write(
        worktree.join(".git"),
        format!("gitdir: {}\n", git_dir.display()),
    )
    .expect("linked worktree gitfile");
    fs::write(git_dir.join("commondir"), "../..\n").expect("commondir");
    fs::write(git_dir.join("HEAD"), "ref: refs/heads/baseline\n").expect("HEAD");
    fs::write(git_dir.join("index"), []).expect("index");
    fs::write(&branch_ref, "0".repeat(40)).expect("branch ref");
    fs::write(common_dir.join("packed-refs"), "# pack-refs with: peeled\n").expect("packed refs");

    let metadata = GitMetadata::discover(&worktree).expect("linked worktree metadata");
    assert_eq!(metadata.git_dir, git_dir);
    assert_eq!(metadata.common_dir, common_dir);
    for path in [
        worktree.join(".git"),
        git_dir.join("HEAD"),
        git_dir.join("index"),
        git_dir.join("commondir"),
        branch_ref.clone(),
        branch_ref
            .parent()
            .expect("branch ref parent")
            .to_path_buf(),
        common_dir.join("packed-refs"),
    ] {
        assert!(
            metadata.rerun_paths.contains(&path),
            "missing {}",
            path.display()
        );
    }
}

#[test]
fn identity_ignores_generated_target_and_results_files() {
    let repo = initialized_repository();
    let clean = BuildIdentity::collect(repo.path(), None).expect("clean identity");
    assert_eq!(clean.compiled_git_sha.len(), 40);
    assert!(!clean.source_dirty);
    assert!(clean.rerun_paths.contains(&repo.path().join("src")));
    assert!(clean.rerun_paths.contains(&repo.path().join(".cargo")));

    for excluded in ["src/results/output.json", "src/target/artifact"] {
        let path = repo.path().join(excluded);
        fs::create_dir_all(path.parent().expect("excluded path parent"))
            .expect("excluded directory");
        fs::write(&path, "generated\n").expect("excluded generated file");
    }
    let generated_outputs =
        BuildIdentity::collect(repo.path(), None).expect("identity with generated outputs");
    assert!(!generated_outputs.source_dirty);
    assert!(
        generated_outputs
            .rerun_paths
            .iter()
            .all(|path| !path.ends_with("output.json") && !path.ends_with("artifact"))
    );
}

#[test]
fn identity_marks_a_tracked_source_change_dirty() {
    let repo = initialized_repository();
    fs::write(
        repo.path().join("src/lib.rs"),
        "pub const TRACKED: bool = true;\n",
    )
    .expect("tracked source edit");
    let tracked_dirty = BuildIdentity::collect(repo.path(), None).expect("tracked dirty identity");
    assert!(tracked_dirty.source_dirty);
    assert_eq!(
        Publishability::from_source_dirty(tracked_dirty.source_dirty),
        Publishability::NonPublishable {
            reasons: vec!["dirty_source_tree".to_string()],
        }
    );
}

#[test]
fn identity_marks_an_untracked_source_change_dirty_and_watches_it() {
    let repo = initialized_repository();
    let untracked_source = repo.path().join("src/new_source.rs");
    fs::write(&untracked_source, "pub const UNTRACKED: bool = true;\n")
        .expect("untracked source edit");
    let untracked_dirty =
        BuildIdentity::collect(repo.path(), None).expect("untracked dirty identity");
    assert!(untracked_dirty.source_dirty);
    assert!(untracked_dirty.rerun_paths.contains(&untracked_source));
    assert!(
        untracked_dirty
            .rerun_paths
            .contains(&repo.path().join("src"))
    );
}

#[test]
fn cargo_rebuilds_the_real_build_script_after_a_tracked_source_edit() {
    let fixture = TempDir::new().expect("temporary Cargo fixture");
    create_cargo_fixture(fixture.path());
    generate_fixture_lockfile(fixture.path());
    git(fixture.path(), &["init"]);
    git(
        fixture.path(),
        &["config", "user.email", "runtime-baseline@example.test"],
    );
    git(
        fixture.path(),
        &["config", "user.name", "Runtime Baseline Test"],
    );
    git(fixture.path(), &["add", "."]);
    git(fixture.path(), &["commit", "-m", "clean fixture"]);

    let clean = run_fixture_identity(fixture.path());
    assert!(!clean.source_dirty);
    assert!(clean.reasons.is_empty());
    fs::write(
        fixture.path().join("src/marker.rs"),
        "pub const MARKER: bool = true;\n",
    )
    .expect("tracked source edit");
    let dirty = run_fixture_identity(fixture.path());
    assert!(dirty.source_dirty);
    assert_eq!(dirty.reasons, ["dirty_source_tree"]);
}

#[test]
fn cargo_rebuilds_the_real_build_script_when_a_linked_worktree_branch_ref_moves() {
    let repository = TempDir::new().expect("temporary Cargo repository");
    create_cargo_fixture(repository.path());
    generate_fixture_lockfile(repository.path());
    git(repository.path(), &["init"]);
    git(
        repository.path(),
        &["config", "user.email", "runtime-baseline@example.test"],
    );
    git(
        repository.path(),
        &["config", "user.name", "Runtime Baseline Test"],
    );
    git(repository.path(), &["add", "."]);
    git(repository.path(), &["commit", "-m", "fixture source"]);
    let first_sha = git_stdout(repository.path(), &["rev-parse", "HEAD"]);
    git(
        repository.path(),
        &["commit", "--allow-empty", "-m", "second identity"],
    );
    let second_sha = git_stdout(repository.path(), &["rev-parse", "HEAD"]);
    assert_ne!(first_sha, second_sha);

    let worktree = repository.path().join("linked-worktree");
    git(
        repository.path(),
        &[
            "worktree",
            "add",
            "-b",
            "baseline",
            path_as_str(&worktree),
            &first_sha,
        ],
    );
    let first_identity = run_fixture_identity(&worktree);
    assert_eq!(first_identity.compiled_git_sha, first_sha);
    assert!(!first_identity.source_dirty);

    let branch_ref = repository.path().join(".git/refs/heads/baseline");
    let before = fs::read_to_string(&branch_ref).expect("branch ref before update");
    git(
        repository.path(),
        &["update-ref", "refs/heads/baseline", &second_sha],
    );
    let after = fs::read_to_string(&branch_ref).expect("branch ref after update");
    assert_ne!(before, after);

    let second_identity = run_fixture_identity(&worktree);
    assert_eq!(second_identity.compiled_git_sha, second_sha);
    assert!(!second_identity.source_dirty);
}

fn initialized_repository() -> TempDir {
    let repo = TempDir::new().expect("temporary repository");
    git(repo.path(), &["init"]);
    git(
        repo.path(),
        &["config", "user.email", "runtime-baseline@example.test"],
    );
    git(
        repo.path(),
        &["config", "user.name", "Runtime Baseline Test"],
    );
    fs::create_dir(repo.path().join("src")).expect("source directory");
    fs::write(
        repo.path().join("src/lib.rs"),
        "pub const CLEAN: bool = true;\n",
    )
    .expect("tracked source");
    git(repo.path(), &["add", "src/lib.rs"]);
    git(repo.path(), &["commit", "-m", "initial source"]);
    repo
}

fn git(directory: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(directory)
        .output()
        .expect("git command starts");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn path_as_str(path: &Path) -> &str {
    path.to_str().expect("temporary path is UTF-8")
}

fn create_cargo_fixture(root: &Path) {
    fs::create_dir_all(root.join("src")).expect("fixture source directory");
    fs::write(
        root.join("src/marker.rs"),
        "pub const MARKER: bool = false;\n",
    )
    .expect("fixture tracked source");
    fs::write(root.join(".gitignore"), "target/\n").expect("fixture Git ignore");
    let fixture_manifest = r#"[workspace]
resolver = "2"
members = ["tools/runtime-baseline"]

[workspace.package]
version = "0.1.0"
description = "runtime baseline fixture"
repository = "https://example.test/runtime-baseline"
edition = "2021"
rust-version = "__RUST_VERSION__"

[workspace.dependencies]
anyhow = "1.0"
clap = { version = "4.6", features = ["derive"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tempfile = "3.27"

[workspace.lints.clippy]
dbg_macro = "warn"
implicit_clone = "warn"

[workspace.lints.rust]
unknown_lints = "deny"
"#
    .replace("__RUST_VERSION__", env!("CARGO_PKG_RUST_VERSION"));
    fs::write(root.join("Cargo.toml"), fixture_manifest).expect("fixture Cargo manifest");

    let source_crate = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture_crate = root.join("tools/runtime-baseline");
    fs::create_dir_all(fixture_crate.join("src/bin")).expect("fixture crate directory");
    for file in ["Cargo.toml", "build.rs", "build_support.rs"] {
        fs::copy(source_crate.join(file), fixture_crate.join(file)).expect("fixture build input");
    }
    for file in ["cli.rs", "lib.rs", "main.rs", "schema.rs", "startup.rs"] {
        fs::copy(
            source_crate.join("src").join(file),
            fixture_crate.join("src").join(file),
        )
        .expect("fixture crate source");
    }
    fs::write(
        fixture_crate.join("src/bin/identity.rs"),
        r#"use runtime_baseline::schema::{Publishability, source_dirty};

#[derive(serde::Serialize)]
struct Identity<'a> {
    compiled_git_sha: &'a str,
    source_dirty: bool,
    reasons: Vec<String>,
}

fn main() {
    let source_dirty = source_dirty();
    let reasons = match Publishability::from_source_dirty(source_dirty) {
        Publishability::Publishable => Vec::new(),
        Publishability::NonPublishable { reasons } => reasons,
    };
    println!(
        "{}",
        serde_json::to_string(&Identity {
            compiled_git_sha: runtime_baseline::schema::COMPILED_GIT_SHA,
            source_dirty,
            reasons,
        })
        .expect("identity JSON")
    );
}
"#,
    )
    .expect("fixture identity binary");
}

#[derive(serde::Deserialize)]
struct FixtureIdentity {
    compiled_git_sha: String,
    source_dirty: bool,
    reasons: Vec<String>,
}

fn run_fixture_identity(root: &Path) -> FixtureIdentity {
    let output = fixture_cargo(root)
        .args([
            "run",
            "-p",
            "runtime-baseline",
            "--bin",
            "identity",
            "--quiet",
        ])
        .env("CARGO_TARGET_DIR", root.join("target"))
        .output()
        .expect("fixture cargo starts");
    assert!(
        output.status.success(),
        "fixture cargo failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("fixture identity output is JSON")
}

fn generate_fixture_lockfile(root: &Path) {
    let output = fixture_cargo(root)
        .arg("generate-lockfile")
        .output()
        .expect("fixture lockfile generation starts");
    assert!(
        output.status.success(),
        "fixture lockfile generation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fixture_cargo(root: &Path) -> Command {
    let mut command = Command::new("cargo");
    command
        .current_dir(root)
        .env_remove("RUSTFLAGS")
        .env_remove("RUSTDOCFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    command
}

fn git_stdout(directory: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(directory)
        .output()
        .expect("git command starts");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git stdout is UTF-8")
        .trim()
        .to_owned()
}
