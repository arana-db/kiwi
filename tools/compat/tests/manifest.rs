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

use kiwi_compat::manifest::{
    Classification, CompatibilityManifest, Mode, Protocol, REDIS_COMMIT, REDIS_TAG,
};

const VALID_MANIFEST: &str = r#"
schema: kiwi-redis-compat/v1
profile: redis_8_8_1_core
redis:
  tag: 8.8.1
  commit: 77b6c308396c9700672390a210143a8496fb4b10
commands:
  - command: get
    classification: known_difference
    modes:
      standalone_cache_off: required
      raft_single_group_cache_off: deferred
    protocols:
      - resp2
      - resp3
    owner: cmd-string
"#;

#[test]
fn rejects_a_manifest_with_an_incorrect_redis_commit() {
    let yaml = VALID_MANIFEST.replace(REDIS_COMMIT, "0000000000000000000000000000000000000000");
    assert_error_contains(&yaml, REDIS_COMMIT);
    assert_error_contains(&yaml, REDIS_TAG);
}

#[test]
fn rejects_a_manifest_with_an_incorrect_redis_tag() {
    let yaml = VALID_MANIFEST.replace("tag: 8.8.1", "tag: 8.8.0");
    assert_error_contains(&yaml, REDIS_TAG);
    assert_error_contains(&yaml, REDIS_COMMIT);
}

#[test]
fn rejects_a_manifest_with_an_incorrect_schema() {
    let yaml = VALID_MANIFEST.replace("kiwi-redis-compat/v1", "kiwi-redis-compat/v2");
    assert_error_contains(&yaml, "kiwi-redis-compat/v1");
}

#[test]
fn rejects_an_unknown_top_level_field() {
    let yaml = VALID_MANIFEST.replace(
        "profile: redis_8_8_1_core",
        "profile: redis_8_8_1_core\nunexpected: true",
    );
    assert_error_contains(&yaml, "unknown field");
}

#[test]
fn rejects_an_unknown_nested_field() {
    let yaml = VALID_MANIFEST.replace(
        "    owner: cmd-string",
        "    owner: cmd-string\n    extra: true",
    );
    assert_error_contains(&yaml, "unknown field");
}

#[test]
fn rejects_an_unknown_redis_field() {
    let yaml = VALID_MANIFEST.replace(
        "  commit: 77b6c308396c9700672390a210143a8496fb4b10",
        "  commit: 77b6c308396c9700672390a210143a8496fb4b10\n  source: upstream",
    );
    assert_error_contains(&yaml, "unknown field");
}

#[test]
fn canonicalizes_command_names_with_ascii_uppercase() {
    let manifest = parse_valid(VALID_MANIFEST);
    assert_eq!(first_command(&manifest).command(), "GET");
}

#[test]
fn accepts_a_printable_ascii_command_with_a_dot() {
    let yaml = VALID_MANIFEST.replace("command: get", "command: module.command");
    let manifest = parse_valid(&yaml);
    assert_eq!(first_command(&manifest).command(), "MODULE.COMMAND");
}

#[test]
fn rejects_an_empty_command_token() {
    let yaml = VALID_MANIFEST.replace("command: get", "command: ''");
    assert_error_contains(&yaml, "command");
}

#[test]
fn rejects_command_tokens_with_leading_or_trailing_whitespace() {
    for command in ["' GET'", "'GET '"] {
        let yaml = VALID_MANIFEST.replace("command: get", &format!("command: {command}"));
        assert_error_contains(&yaml, "command");
    }
}

#[test]
fn rejects_a_command_token_with_an_ascii_control_character() {
    let yaml = VALID_MANIFEST.replace("command: get", "command: \"GET\\u0001\"");
    assert_error_contains(&yaml, "command");
}

#[test]
fn rejects_a_non_ascii_command_without_unicode_uppercasing_it() {
    let yaml = VALID_MANIFEST.replace("command: get", "command: 'gét'");
    assert_error_contains(&yaml, "gét");
}

#[test]
fn rejects_duplicate_commands_after_ascii_canonicalization() {
    let duplicate = r#"
  - command: GET
    classification: required
    modes:
      standalone_cache_off: required
    protocols: [resp2]
    owner: cmd-string
"#;
    let yaml = VALID_MANIFEST.replace(
        "    owner: cmd-string\n",
        &format!("    owner: cmd-string\n{duplicate}"),
    );
    assert_error_contains(&yaml, "duplicates");
    assert_error_contains(&yaml, "GET");
}

#[test]
fn rejects_empty_modes() {
    let yaml = VALID_MANIFEST.replace(
        "    modes:\n      standalone_cache_off: required\n      raft_single_group_cache_off: deferred",
        "    modes: {}",
    );
    assert_error_contains(&yaml, "modes");
}

#[test]
fn rejects_an_unknown_mode() {
    let yaml = VALID_MANIFEST.replace("standalone_cache_off", "experimental_cache_off");
    assert_error_contains(&yaml, "experimental_cache_off");
}

#[test]
fn exposes_the_standalone_cache_off_mode_through_a_closed_getter() {
    let manifest = parse_valid(VALID_MANIFEST);
    assert_eq!(
        first_command(&manifest)
            .modes()
            .get(&Mode::StandaloneCacheOff),
        Some(&Classification::Required)
    );
}

#[test]
fn exposes_the_raft_single_group_cache_off_mode_through_a_closed_getter() {
    let manifest = parse_valid(VALID_MANIFEST);
    assert_eq!(
        first_command(&manifest)
            .modes()
            .get(&Mode::RaftSingleGroupCacheOff),
        Some(&Classification::Deferred)
    );
}

#[test]
fn rejects_a_blank_mode() {
    let yaml = VALID_MANIFEST.replace("standalone_cache_off", "'   '");
    assert_error_contains(&yaml, "   ");
}

#[test]
fn rejects_empty_protocols() {
    let yaml = VALID_MANIFEST.replace(
        "    protocols:\n      - resp2\n      - resp3",
        "    protocols: []",
    );
    assert_error_contains(&yaml, "protocols");
}

#[test]
fn rejects_an_empty_owner_after_trimming() {
    let yaml = VALID_MANIFEST.replace("owner: cmd-string", "owner: '   '");
    assert_error_contains(&yaml, "owner");
}

#[test]
fn rejects_an_unsupported_classification() {
    let yaml = VALID_MANIFEST.replace(
        "classification: known_difference",
        "classification: partial",
    );
    assert_error_contains(&yaml, "classification");
}

#[test]
fn rejects_an_unsupported_mode_classification() {
    let yaml = VALID_MANIFEST.replace(
        "standalone_cache_off: required",
        "standalone_cache_off: partial",
    );
    assert_error_contains(&yaml, "partial");
}

#[test]
fn rejects_an_unsupported_protocol() {
    let yaml = VALID_MANIFEST.replace("      - resp3", "      - resp9");
    assert_error_contains(&yaml, "resp9");
}

#[test]
fn loads_a_valid_manifest_through_read_only_getters() {
    let manifest = parse_valid(VALID_MANIFEST);
    let command = first_command(&manifest);

    assert_eq!(manifest.schema(), "kiwi-redis-compat/v1");
    assert_eq!(manifest.profile(), "redis_8_8_1_core");
    assert_eq!(manifest.redis().tag(), REDIS_TAG);
    assert_eq!(manifest.redis().commit(), REDIS_COMMIT);
    assert_eq!(manifest.commands().len(), 1);
    assert_eq!(command.classification(), Classification::KnownDifference);
    assert_eq!(command.protocols(), &[Protocol::Resp2, Protocol::Resp3]);
    assert_eq!(command.owner(), "cmd-string");
}

#[test]
fn loads_the_repository_redis_8_8_1_manifest() {
    let yaml = include_str!("../../../tests/compat/redis-8.8.1/manifest.yaml");
    let manifest = parse_valid(yaml);

    assert_eq!(manifest.schema(), "kiwi-redis-compat/v1");
    assert_eq!(manifest.profile(), "redis_8_8_1_core");
    assert_eq!(manifest.redis().tag(), REDIS_TAG);
    assert_eq!(manifest.redis().commit(), REDIS_COMMIT);
    assert!(manifest.commands().is_empty());
}

fn parse_valid(yaml: &str) -> CompatibilityManifest {
    match CompatibilityManifest::from_yaml(yaml) {
        Ok(manifest) => manifest,
        Err(error) => panic!("valid manifest must load: {error}"),
    }
}

fn first_command(manifest: &CompatibilityManifest) -> &kiwi_compat::manifest::CommandContract {
    match manifest.commands().first() {
        Some(command) => command,
        None => panic!("manifest must contain a command"),
    }
}

fn assert_error_contains(yaml: &str, expected: &str) {
    let error = match CompatibilityManifest::from_yaml(yaml) {
        Ok(_) => panic!("manifest must be rejected"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(
        message.contains(expected),
        "error {message:?} did not contain {expected:?}"
    );
}
