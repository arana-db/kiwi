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
use std::path::Path;

#[cfg(windows)]
use std::process::Command;

use clap::Parser;
use runtime_baseline::cli::ServerArgs;
use tempfile::TempDir;

fn absolute_path(dir: &TempDir, name: &str) -> std::path::PathBuf {
    dir.path().join(name)
}

fn valid_args(dir: &TempDir) -> ServerArgs {
    ServerArgs {
        listen: "127.0.0.1:0"
            .parse::<SocketAddr>()
            .expect("valid listen address"),
        control_listen: "127.0.0.1:0"
            .parse::<SocketAddr>()
            .expect("valid control address"),
        data_dir: absolute_path(dir, "data"),
        startup_event: absolute_path(dir, "startup.json"),
        metrics_output: absolute_path(dir, "metrics.json"),
        expected_git_sha: None,
        network_threads: 1,
        storage_threads: 1,
        channel_capacity: 1,
        request_timeout_ms: 1,
        batching: false,
        batch_size: 1,
        batch_timeout_ms: 1,
        instrumentation: false,
        instrumentation_sample_capacity: 1,
    }
}

#[test]
fn rejects_relative_data_directory() {
    let dir = TempDir::new().expect("temp directory");
    let mut args = valid_args(&dir);
    args.data_dir = Path::new("relative-data").to_path_buf();

    assert!(args.validate().is_err());
}

#[test]
fn rejects_non_loopback_listeners() {
    let dir = TempDir::new().expect("temp directory");
    let mut args = valid_args(&dir);
    args.listen = "0.0.0.0:0".parse().expect("valid socket address");

    assert!(args.validate().is_err());

    args.listen = "127.0.0.1:0".parse().expect("valid socket address");
    args.control_listen = "192.168.1.10:0".parse().expect("valid socket address");

    assert!(args.validate().is_err());
}

#[test]
fn rejects_existing_rocksdb_data_directory() {
    let dir = TempDir::new().expect("temp directory");
    let args = valid_args(&dir);
    std::fs::create_dir(&args.data_dir).expect("data directory");
    std::fs::write(args.data_dir.join("CURRENT"), b"MANIFEST-000001\n").expect("rocksdb marker");

    assert!(args.validate().is_err());
}

#[test]
fn rejects_any_existing_current_marker() {
    let dir = TempDir::new().expect("temp directory");
    let args = valid_args(&dir);
    std::fs::create_dir(&args.data_dir).expect("data directory");
    std::fs::create_dir(args.data_dir.join("CURRENT")).expect("rocksdb marker directory");

    assert!(args.validate().is_err());
}

#[test]
fn rejects_relative_event_and_metrics_paths() {
    let dir = TempDir::new().expect("temp directory");
    let mut args = valid_args(&dir);
    args.startup_event = Path::new("startup.json").to_path_buf();

    assert!(args.validate().is_err());

    args.startup_event = absolute_path(&dir, "startup.json");
    args.metrics_output = Path::new("metrics.json").to_path_buf();

    assert!(args.validate().is_err());
}

#[test]
fn rejects_output_paths_that_normalize_to_the_same_target() {
    let dir = TempDir::new().expect("temp directory");
    let alias = dir.path().join("alias");
    fs::create_dir(&alias).expect("alias directory");
    let mut args = valid_args(&dir);
    args.startup_event = alias.join("..").join("shared.json");
    args.metrics_output = dir.path().join("shared.json");

    let error = args
        .validate()
        .expect_err("aliases must resolve to one target");
    assert!(error.to_string().contains("must be distinct"));
}

#[test]
fn rejects_an_output_parent_alias_that_resolves_inside_the_data_directory() {
    let dir = TempDir::new().expect("temp directory");
    let mut args = valid_args(&dir);
    fs::create_dir(&args.data_dir).expect("data directory");
    let alias = dir.path().join("data-alias");
    create_directory_alias(&args.data_dir, &alias);
    args.startup_event = alias.join("startup.json");

    let error = args
        .validate()
        .expect_err("output aliases must not enter data-dir");
    assert!(
        error
            .to_string()
            .contains("must not be located under data-dir")
    );
}

#[test]
fn normalizes_a_missing_data_directory_from_its_existing_aliased_ancestor() {
    let dir = TempDir::new().expect("temp directory");
    let actual_parent = dir.path().join("actual-parent");
    fs::create_dir(&actual_parent).expect("actual parent directory");
    let alias = dir.path().join("parent-alias");
    create_directory_alias(&actual_parent, &alias);
    let mut args = valid_args(&dir);
    args.data_dir = alias.join("future-data");
    args.startup_event = actual_parent.join("future-data");
    assert!(!args.data_dir.exists());

    let error = args
        .validate()
        .expect_err("missing data-dir aliases must normalize from their existing ancestor");
    assert!(
        error
            .to_string()
            .contains("must not be located under data-dir")
    );
}

#[test]
fn parses_every_planned_argument() {
    let dir = TempDir::new().expect("temp directory");
    let arguments = valid_command_line(&dir);
    let parsed = ServerArgs::try_parse_from(&arguments).expect("all planned arguments parse");
    assert_eq!(
        parsed.listen,
        "127.0.0.1:0".parse().expect("listen address")
    );
    assert_eq!(
        parsed.control_listen,
        "127.0.0.1:0".parse().expect("control address")
    );
    assert_eq!(parsed.data_dir, absolute_path(&dir, "parsed-data"));
    assert_eq!(
        parsed.startup_event,
        absolute_path(&dir, "parsed-startup.json")
    );
    assert_eq!(
        parsed.metrics_output,
        absolute_path(&dir, "parsed-metrics.json")
    );
    assert_eq!(
        parsed.expected_git_sha.as_deref(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    assert_eq!(parsed.network_threads, 2);
    assert_eq!(parsed.storage_threads, 3);
    assert_eq!(parsed.channel_capacity, 4);
    assert_eq!(parsed.request_timeout_ms, 5);
    assert!(parsed.batching);
    assert_eq!(parsed.batch_size, 6);
    assert_eq!(parsed.batch_timeout_ms, 7);
    assert!(parsed.instrumentation);
    assert_eq!(parsed.instrumentation_sample_capacity, 8);
    assert!(parsed.validate().is_ok());
}

#[test]
fn rejects_zero_for_each_numeric_parameter() {
    let dir = TempDir::new().expect("temp directory");
    for flag in [
        "--network-threads",
        "--storage-threads",
        "--channel-capacity",
        "--request-timeout-ms",
        "--batch-size",
        "--batch-timeout-ms",
        "--instrumentation-sample-capacity",
    ] {
        let mut zero_arguments = valid_command_line(&dir);
        let value_index = zero_arguments
            .iter()
            .position(|argument| argument == flag)
            .expect("planned option is present")
            + 1;
        zero_arguments[value_index] = "0".to_string();
        let parsed = ServerArgs::try_parse_from(&zero_arguments).expect("zero still parses");
        assert!(parsed.validate().is_err(), "{flag} must reject zero");
    }
}

#[test]
fn instrumentation_uses_the_documented_on_off_values() {
    let dir = TempDir::new().expect("temp directory");

    let enabled =
        ServerArgs::try_parse_from(valid_command_line(&dir)).expect("instrumentation=on parses");
    assert!(enabled.instrumentation);

    let mut disabled_arguments = valid_command_line(&dir);
    replace_option_value(&mut disabled_arguments, "--instrumentation", "off");
    let disabled =
        ServerArgs::try_parse_from(disabled_arguments).expect("instrumentation=off parses");
    assert!(!disabled.instrumentation);

    let mut boolean_arguments = valid_command_line(&dir);
    replace_option_value(&mut boolean_arguments, "--instrumentation", "true");
    assert!(ServerArgs::try_parse_from(boolean_arguments).is_err());
}

#[test]
fn rejects_invalid_expected_git_sha_from_the_real_argument_parser() {
    let dir = TempDir::new().expect("temp directory");
    for git_sha in ["abc".to_string(), "g".repeat(40), "a".repeat(41)] {
        let mut arguments = valid_command_line(&dir);
        let value_index = arguments
            .iter()
            .position(|argument| argument == "--expected-git-sha")
            .expect("expected SHA is present")
            + 1;
        arguments[value_index] = git_sha;
        let parsed = ServerArgs::try_parse_from(&arguments).expect("invalid SHA syntax parses");
        assert!(parsed.validate().is_err());
    }
}

fn valid_command_line(dir: &TempDir) -> Vec<String> {
    vec![
        "kiwi-runtime-baseline".to_string(),
        "--listen".to_string(),
        "127.0.0.1:0".to_string(),
        "--control-listen".to_string(),
        "127.0.0.1:0".to_string(),
        "--data-dir".to_string(),
        absolute_path(dir, "parsed-data").display().to_string(),
        "--startup-event".to_string(),
        absolute_path(dir, "parsed-startup.json")
            .display()
            .to_string(),
        "--metrics-output".to_string(),
        absolute_path(dir, "parsed-metrics.json")
            .display()
            .to_string(),
        "--expected-git-sha".to_string(),
        "a".repeat(40),
        "--network-threads".to_string(),
        "2".to_string(),
        "--storage-threads".to_string(),
        "3".to_string(),
        "--channel-capacity".to_string(),
        "4".to_string(),
        "--request-timeout-ms".to_string(),
        "5".to_string(),
        "--batching=true".to_string(),
        "--batch-size".to_string(),
        "6".to_string(),
        "--batch-timeout-ms".to_string(),
        "7".to_string(),
        "--instrumentation".to_string(),
        "on".to_string(),
        "--instrumentation-sample-capacity".to_string(),
        "8".to_string(),
    ]
}

fn replace_option_value(arguments: &mut [String], flag: &str, value: &str) {
    let value_index = arguments
        .iter()
        .position(|argument| argument == flag)
        .expect("planned option is present")
        + 1;
    arguments[value_index] = value.to_string();
}

#[cfg(unix)]
fn create_directory_alias(target: &Path, alias: &Path) {
    std::os::unix::fs::symlink(target, alias).expect("directory symlink");
}

#[cfg(windows)]
fn create_directory_alias(target: &Path, alias: &Path) {
    let output = Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(alias)
        .arg(target)
        .output()
        .expect("junction command starts");
    assert!(
        output.status.success(),
        "cannot create directory junction: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
