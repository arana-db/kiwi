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

use std::io::Write;
use std::path::Path;

#[cfg(unix)]
use std::fs;

use anyhow::{Context, Result, bail};
use tempfile::NamedTempFile;

use crate::cli::validate_git_sha;
use crate::schema::{COMPILED_GIT_SHA, StartupEvent};

/// Reject an identity mismatch before the future harness binds listeners.
pub fn ensure_expected_git_sha(expected_git_sha: Option<&str>) -> Result<()> {
    let Some(expected_git_sha) = expected_git_sha else {
        return Ok(());
    };
    validate_git_sha("expected-git-sha", expected_git_sha)?;
    if !expected_git_sha.eq_ignore_ascii_case(COMPILED_GIT_SHA) {
        bail!("expected Git SHA does not match this binary's compiled identity");
    }
    Ok(())
}

/// Publish a complete startup event by atomically renaming a synced sibling file.
pub fn write_startup_event_atomically(output: &Path, event: &StartupEvent) -> Result<()> {
    let parent = output
        .parent()
        .context("startup event output must have a parent directory")?;
    if !parent.is_dir() {
        bail!(
            "startup event parent directory does not exist: {}",
            parent.display()
        );
    }
    if output.exists() {
        // This is only a friendly fast path; persist_noclobber below enforces no-clobber atomically.
        bail!("startup event output already exists: {}", output.display());
    }

    let mut temporary = NamedTempFile::new_in(parent)
        .with_context(|| format!("cannot create startup event beside {}", output.display()))?;
    serde_json::to_writer(temporary.as_file_mut(), event)
        .context("cannot serialize startup event JSON")?;
    temporary
        .as_file_mut()
        .flush()
        .context("cannot flush temporary startup event")?;
    temporary
        .as_file()
        .sync_all()
        .context("cannot sync temporary startup event")?;
    temporary
        .persist_noclobber(output)
        .map_err(|error| error.error)
        .with_context(|| {
            format!(
                "cannot atomically publish startup event at {}",
                output.display()
            )
        })?;
    sync_parent_directory(parent)?;
    Ok(())
}

fn sync_parent_directory(parent: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(parent)
            .with_context(|| format!("cannot open startup event directory {}", parent.display()))?
            .sync_all()
            .with_context(|| format!("cannot sync startup event directory {}", parent.display()))?;
    }

    #[cfg(not(unix))]
    {
        let _ = parent;
    }

    Ok(())
}
