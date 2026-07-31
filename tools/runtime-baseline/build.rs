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

use std::env;
use std::path::{Path, PathBuf};

#[path = "build_support.rs"]
mod build_support;

use build_support::BuildIdentity;

const BUILD_GIT_SHA_ENV: &str = "KIWI_BASELINE_BUILD_GIT_SHA";

fn main() {
    if let Err(error) = configure_build_identity() {
        panic!("failed to configure runtime baseline build identity: {error}");
    }
}

fn configure_build_identity() -> Result<(), String> {
    println!("cargo:rerun-if-env-changed={BUILD_GIT_SHA_ENV}");

    let source_root = source_root()?;
    let expected_git_sha = match env::var(BUILD_GIT_SHA_ENV) {
        Ok(git_sha) => Some(git_sha),
        Err(env::VarError::NotPresent) => None,
        Err(error) => return Err(format!("cannot read {BUILD_GIT_SHA_ENV}: {error}")),
    };
    let identity = BuildIdentity::collect(&source_root, expected_git_sha.as_deref())?;
    for path in identity.rerun_paths {
        println!("cargo:rerun-if-changed={}", path.display());
    }

    println!(
        "cargo:rustc-env=KIWI_BASELINE_COMPILED_GIT_SHA={}",
        identity.compiled_git_sha
    );
    println!(
        "cargo:rustc-env=KIWI_BASELINE_SOURCE_DIRTY={}",
        identity.source_dirty
    );
    Ok(())
}

fn source_root() -> Result<PathBuf, String> {
    let manifest_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR").map_err(|error| format!("CARGO_MANIFEST_DIR: {error}"))?,
    );
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            format!(
                "cannot locate workspace root from {}",
                manifest_dir.display()
            )
        })
}
