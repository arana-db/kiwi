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

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{ArgAction, Parser};

/// Parameters accepted by the benchmark-only target.
#[derive(Clone, Debug, Parser)]
#[command(name = "kiwi-runtime-baseline")]
pub struct ServerArgs {
    #[arg(long, default_value = "127.0.0.1:0")]
    pub listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:0")]
    pub control_listen: SocketAddr,
    #[arg(long)]
    pub data_dir: PathBuf,
    #[arg(long)]
    pub startup_event: PathBuf,
    #[arg(long)]
    pub metrics_output: PathBuf,
    #[arg(long)]
    pub expected_git_sha: Option<String>,
    #[arg(long, default_value_t = 1)]
    pub network_threads: usize,
    #[arg(long, default_value_t = 1)]
    pub storage_threads: usize,
    #[arg(long, default_value_t = 1024)]
    pub channel_capacity: usize,
    #[arg(long, default_value_t = 30_000)]
    pub request_timeout_ms: u64,
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    pub batching: bool,
    #[arg(long, default_value_t = 100)]
    pub batch_size: usize,
    #[arg(long, default_value_t = 10)]
    pub batch_timeout_ms: u64,
    #[arg(
        long,
        default_value = "off",
        action = ArgAction::Set,
        value_parser = parse_on_off
    )]
    pub instrumentation: bool,
    #[arg(long, default_value_t = 8192)]
    pub instrumentation_sample_capacity: usize,
}

fn parse_on_off(value: &str) -> std::result::Result<bool, String> {
    match value {
        "on" => Ok(true),
        "off" => Ok(false),
        _ => Err("must be one of: on, off".to_owned()),
    }
}

impl ServerArgs {
    /// Reject unsafe target parameters before opening a listener or data store.
    pub fn validate(&self) -> Result<()> {
        validate_loopback("listen", self.listen)?;
        validate_loopback("control-listen", self.control_listen)?;
        validate_absolute_path("data-dir", &self.data_dir)?;
        let normalized_data_dir = normalize_data_dir(&self.data_dir)?;
        let normalized_startup_event = normalize_output_path("startup-event", &self.startup_event)?;
        let normalized_metrics_output =
            normalize_output_path("metrics-output", &self.metrics_output)?;

        if output_paths_may_alias(&normalized_startup_event, &normalized_metrics_output) {
            bail!("startup-event and metrics-output must be distinct paths");
        }
        if normalized_startup_event.starts_with(&normalized_data_dir)
            || normalized_metrics_output.starts_with(&normalized_data_dir)
        {
            bail!("output paths must not be located under data-dir");
        }
        if normalized_data_dir.join("CURRENT").exists() {
            bail!("data-dir already contains RocksDB CURRENT");
        }
        if self.data_dir.exists() && !self.data_dir.is_dir() {
            bail!("data-dir must be a directory when it exists");
        }
        if let Some(expected_git_sha) = &self.expected_git_sha {
            validate_git_sha("expected-git-sha", expected_git_sha)?;
        }

        validate_nonzero("network-threads", self.network_threads)?;
        validate_nonzero("storage-threads", self.storage_threads)?;
        validate_nonzero("channel-capacity", self.channel_capacity)?;
        validate_nonzero("request-timeout-ms", self.request_timeout_ms)?;
        validate_nonzero("batch-size", self.batch_size)?;
        validate_nonzero("batch-timeout-ms", self.batch_timeout_ms)?;
        validate_nonzero(
            "instrumentation-sample-capacity",
            self.instrumentation_sample_capacity,
        )?;

        Ok(())
    }
}

fn output_paths_may_alias(first: &Path, second: &Path) -> bool {
    if first == second {
        return true;
    }

    #[cfg(any(windows, target_os = "macos"))]
    {
        first.parent() == second.parent()
            && first
                .file_name()
                .zip(second.file_name())
                .is_some_and(|(first, second)| {
                    first
                        .as_encoded_bytes()
                        .eq_ignore_ascii_case(second.as_encoded_bytes())
                })
    }

    #[cfg(not(any(windows, target_os = "macos")))]
    {
        false
    }
}

fn validate_loopback(name: &str, address: SocketAddr) -> Result<()> {
    if !address.ip().is_loopback() {
        bail!("{name} must use a loopback address");
    }
    Ok(())
}

fn validate_absolute_path(name: &str, path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("{name} must be an absolute path");
    }
    Ok(())
}

fn normalize_output_path(name: &str, path: &Path) -> Result<PathBuf> {
    validate_absolute_path(name, path)?;
    let parent = path
        .parent()
        .context("absolute output path must have a parent directory")?;
    if !parent.is_dir() {
        bail!(
            "{name} parent directory does not exist: {}",
            parent.display()
        );
    }
    let file_name = path
        .file_name()
        .with_context(|| format!("{name} must name an output file"))?;
    #[cfg(any(windows, target_os = "macos"))]
    if !file_name.as_encoded_bytes().is_ascii() {
        bail!("{name} file name must use ASCII characters on this platform");
    }
    let parent = parent
        .canonicalize()
        .with_context(|| format!("cannot canonicalize {name} parent {}", parent.display()))?;
    Ok(parent.join(file_name))
}

fn normalize_data_dir(path: &Path) -> Result<PathBuf> {
    let mut existing = path.to_path_buf();
    let mut missing = Vec::new();

    loop {
        match existing.canonicalize() {
            Ok(mut normalized) => {
                for component in missing.into_iter().rev() {
                    match component {
                        MissingComponent::Normal(component) => normalized.push(component),
                        MissingComponent::Parent => {
                            normalized.pop();
                        }
                    }
                }
                return Ok(normalized);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = existing
                    .components()
                    .next_back()
                    .context("data-dir must contain an existing absolute ancestor")?;
                match component {
                    Component::Normal(component) => {
                        missing.push(MissingComponent::Normal(component.to_os_string()));
                    }
                    Component::ParentDir => missing.push(MissingComponent::Parent),
                    Component::CurDir => {}
                    Component::Prefix(_) | Component::RootDir => {
                        return Err(error).context("cannot canonicalize data-dir");
                    }
                }
                if !existing.pop() {
                    return Err(error).context("cannot canonicalize data-dir");
                }
            }
            Err(error) => return Err(error).context("cannot canonicalize data-dir"),
        }
    }
}

enum MissingComponent {
    Normal(OsString),
    Parent,
}

fn validate_nonzero<T>(name: &str, value: T) -> Result<()>
where
    T: TryInto<u128>,
    T::Error: std::fmt::Debug,
{
    if value.try_into().expect("integer converts to u128") == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(())
}

pub fn validate_git_sha(name: &str, git_sha: &str) -> Result<()> {
    if git_sha.len() != 40 || !git_sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{name} must be a 40-character hexadecimal Git SHA");
    }
    Ok(())
}
