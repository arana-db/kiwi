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

use std::fs;
use std::net::SocketAddr;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;

use runtime_baseline::schema::{
    COMPILED_GIT_SHA, Publishability, STARTUP_SCHEMA_VERSION, StartupEvent, source_dirty,
};
use runtime_baseline::startup::{ensure_expected_git_sha, write_startup_event_atomically};
use tempfile::TempDir;

#[test]
fn startup_event_is_complete_atomic_and_uses_compiled_identity() {
    let dir = TempDir::new().expect("temp directory");
    let data_dir = dir.path().join("data");
    let output_dir = dir.path().join("events");
    let output = output_dir.join("startup.json");
    fs::create_dir(&data_dir).expect("data directory");
    fs::create_dir(&output_dir).expect("output directory");

    let redis_addr = "127.0.0.1:41001"
        .parse::<SocketAddr>()
        .expect("redis loopback address");
    let control_addr = "127.0.0.1:41002"
        .parse::<SocketAddr>()
        .expect("control loopback address");
    let event =
        StartupEvent::new(4242, redis_addr, control_addr, &data_dir).expect("valid startup event");

    write_startup_event_atomically(&output, &event).expect("atomic startup event");

    let parsed: StartupEvent = serde_json::from_slice(
        &fs::read(&output).expect("startup event is present after atomic rename"),
    )
    .expect("startup event is complete JSON");
    assert_eq!(parsed.schema_version, STARTUP_SCHEMA_VERSION);
    assert_eq!(parsed.pid, 4242);
    assert_eq!(parsed.redis_addr, redis_addr);
    assert_eq!(parsed.control_addr, control_addr);
    assert_eq!(
        parsed.data_dir,
        fs::canonicalize(&data_dir).expect("canonical data dir")
    );
    assert_eq!(parsed.git_sha, COMPILED_GIT_SHA);
    assert_eq!(parsed.publishability, event.publishability);

    let output_names = fs::read_dir(&output_dir)
        .expect("output directory")
        .map(|entry| entry.expect("directory entry").file_name())
        .collect::<Vec<_>>();
    assert_eq!(output_names, vec!["startup.json"]);
}

#[test]
fn existing_startup_event_is_not_overwritten() {
    let dir = TempDir::new().expect("temp directory");
    let data_dir = dir.path().join("data");
    let output_dir = dir.path().join("events");
    let output = output_dir.join("startup.json");
    fs::create_dir(&data_dir).expect("data directory");
    fs::create_dir(&output_dir).expect("output directory");
    fs::write(&output, b"existing startup event\n").expect("existing startup event");
    let event = startup_event(&data_dir, 4242);

    write_startup_event_atomically(&output, &event).expect_err("must not replace existing output");

    assert_eq!(
        fs::read(&output).expect("existing output remains readable"),
        b"existing startup event\n"
    );
    assert_eq!(directory_entry_names(&output_dir), ["startup.json"]);
}

#[test]
fn concurrent_startup_event_writers_publish_at_most_one_complete_file() {
    let dir = TempDir::new().expect("temp directory");
    let data_dir = dir.path().join("data");
    let output_dir = dir.path().join("events");
    let output = output_dir.join("startup.json");
    fs::create_dir(&data_dir).expect("data directory");
    fs::create_dir(&output_dir).expect("output directory");
    let events = [4242, 4343]
        .into_iter()
        .enumerate()
        .map(|(index, pid)| {
            let mut event = startup_event(&data_dir, pid);
            event.publishability = Publishability::NonPublishable {
                reasons: vec![(if index == 0 { "a" } else { "b" }).repeat(4 * 1024 * 1024)],
            };
            Arc::new(event)
        })
        .collect::<Vec<_>>();
    let output = Arc::new(output);
    let barrier = Arc::new(Barrier::new(2));

    let writers = events
        .iter()
        .map(|event| {
            let event = Arc::clone(event);
            let output = Arc::clone(&output);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                write_startup_event_atomically(&output, &event)
            })
        })
        .collect::<Vec<_>>();
    let results = writers
        .into_iter()
        .map(|writer| writer.join().expect("startup writer does not panic"))
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let parsed: StartupEvent = serde_json::from_slice(
        &fs::read(output.as_ref()).expect("published startup event is present"),
    )
    .expect("published startup event is complete JSON");
    assert!(events.iter().any(|event| &parsed == event.as_ref()));
    assert_eq!(directory_entry_names(&output_dir), ["startup.json"]);
}

#[test]
fn publishability_reflects_embedded_source_cleanliness() {
    let dir = TempDir::new().expect("temp directory");
    let data_dir = dir.path().join("data");
    fs::create_dir(&data_dir).expect("data directory");
    let event = StartupEvent::new(
        1,
        "127.0.0.1:0".parse().expect("redis loopback address"),
        "127.0.0.1:0".parse().expect("control loopback address"),
        &data_dir,
    )
    .expect("startup event");

    if source_dirty() {
        assert_eq!(
            event.publishability,
            Publishability::NonPublishable {
                reasons: vec!["dirty_source_tree".to_string()],
            }
        );
    } else {
        assert_eq!(event.publishability, Publishability::Publishable);
    }
}

#[test]
fn expected_sha_mismatch_fails_without_echoing_the_caller_value() {
    let caller_value = "ffffffffffffffffffffffffffffffffffffffff";
    assert_ne!(COMPILED_GIT_SHA, caller_value);

    let error = ensure_expected_git_sha(Some(caller_value)).expect_err("must reject mismatch");
    assert!(!error.to_string().contains(caller_value));
}

#[test]
fn binary_rejects_mismatched_identity_before_reaching_the_harness_exit() {
    let dir = TempDir::new().expect("temp directory");
    let data_dir = dir.path().join("data");
    let startup_event = dir.path().join("startup.json");
    let metrics_output = dir.path().join("metrics.json");
    let caller_sha = "f".repeat(40);
    assert_ne!(caller_sha, COMPILED_GIT_SHA);

    let mismatch = run_binary(&data_dir, &startup_event, &metrics_output, &caller_sha);
    assert!(!mismatch.status.success());
    let mismatch_stderr = String::from_utf8(mismatch.stderr).expect("UTF-8 stderr");
    assert!(
        mismatch_stderr.contains("expected Git SHA does not match this binary's compiled identity")
    );
    assert!(!mismatch_stderr.contains(&caller_sha));
    assert!(!mismatch_stderr.contains("harness not initialized"));
    assert!(!startup_event.exists());
    assert!(!metrics_output.exists());
}

#[test]
fn binary_with_matching_identity_reaches_the_harness_exit_without_outputs() {
    let dir = TempDir::new().expect("temp directory");
    let data_dir = dir.path().join("data");
    let startup_event = dir.path().join("startup.json");
    let metrics_output = dir.path().join("metrics.json");
    let matching = run_binary(&data_dir, &startup_event, &metrics_output, COMPILED_GIT_SHA);
    assert!(!matching.status.success());
    let matching_stderr = String::from_utf8(matching.stderr).expect("UTF-8 stderr");
    assert!(matching_stderr.contains("harness not initialized"));
    assert!(!startup_event.exists());
    assert!(!metrics_output.exists());
}

fn run_binary(
    data_dir: &std::path::Path,
    startup_event: &std::path::Path,
    metrics_output: &std::path::Path,
    expected_sha: &str,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_kiwi-runtime-baseline"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--control-listen",
            "127.0.0.1:0",
            "--data-dir",
            data_dir.to_str().expect("temporary path is UTF-8"),
            "--startup-event",
            startup_event.to_str().expect("temporary path is UTF-8"),
            "--metrics-output",
            metrics_output.to_str().expect("temporary path is UTF-8"),
            "--expected-git-sha",
            expected_sha,
            "--network-threads",
            "2",
            "--storage-threads",
            "3",
            "--channel-capacity",
            "4",
            "--request-timeout-ms",
            "5",
            "--batching=false",
            "--batch-size",
            "6",
            "--batch-timeout-ms",
            "7",
            "--instrumentation",
            "off",
            "--instrumentation-sample-capacity",
            "8",
        ])
        .output()
        .expect("runtime baseline binary starts")
}

fn startup_event(data_dir: &std::path::Path, pid: u32) -> StartupEvent {
    StartupEvent::new(
        pid,
        "127.0.0.1:41001"
            .parse::<SocketAddr>()
            .expect("redis loopback address"),
        "127.0.0.1:41002"
            .parse::<SocketAddr>()
            .expect("control loopback address"),
        data_dir,
    )
    .expect("valid startup event")
}

fn directory_entry_names(directory: &std::path::Path) -> Vec<String> {
    let mut names = fs::read_dir(directory)
        .expect("output directory")
        .map(|entry| {
            entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}
