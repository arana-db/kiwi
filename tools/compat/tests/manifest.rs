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
    ArgumentSemantics, Classification, CompatibilityManifest, ErrorSemantics, Mode, Profile,
    Protocol, REDIS_COMMIT, REDIS_TAG, ReplySchema, TestEvidence, TtlSemantics,
};

const VALID_MANIFEST: &str = r#"
schema: kiwi-redis-compat/v1
profile: redis_8_8_1_standalone_cache_off
redis:
  tag: 8.8.1
  commit: 77b6c308396c9700672390a210143a8496fb4b10
commands:
  - command: get
    classification: required
    modes:
      standalone_cache_off: required
      raft_single_group_cache_off: deferred
    protocols:
      - resp2
      - resp3
    arguments: exact
    reply_schema: exact
    errors: exact-prefix
    ttl_semantics: applicable
    tests: [wire-differential, final-state]
    known_differences: []
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
        "profile: redis_8_8_1_standalone_cache_off",
        "profile: redis_8_8_1_standalone_cache_off\nunexpected: true",
    );
    assert_error_contains(&yaml, "unknown field");
}

#[test]
fn rejects_empty_or_unknown_profiles() {
    for profile in ["''", "redis_8_8_1_typo"] {
        let yaml = VALID_MANIFEST.replace(
            "profile: redis_8_8_1_standalone_cache_off",
            &format!("profile: {profile}"),
        );
        assert_error_contains(&yaml, "profile");
    }
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
    arguments: exact
    reply_schema: exact
    errors: exact-prefix
    ttl_semantics: applicable
    tests: [wire-differential]
    known_differences: []
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
fn rejects_duplicate_modes_before_they_can_override_required_or_known_difference() {
    for first_classification in ["required", "known_difference"] {
        let yaml = VALID_MANIFEST
            .replace("classification: required", "classification: deferred")
            .replace(
                "      standalone_cache_off: required\n      raft_single_group_cache_off: deferred",
                &format!(
                    "      standalone_cache_off: {first_classification}\n      standalone_cache_off: deferred\n      raft_single_group_cache_off: deferred"
                ),
            )
            .replace(
                "tests: [wire-differential, final-state]",
                "tests: []",
            );
        assert_error_contains(&yaml, "duplicate");
    }
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
fn rejects_empty_test_evidence_for_a_required_command() {
    let yaml = VALID_MANIFEST.replace("tests: [wire-differential, final-state]", "tests: []");
    assert_error_contains(&yaml, "tests");
}

#[test]
fn rejects_empty_test_evidence_when_only_a_mode_is_required() {
    let yaml = VALID_MANIFEST
        .replace("classification: required", "classification: deferred")
        .replace("tests: [wire-differential, final-state]", "tests: []");
    assert_error_contains(&yaml, "tests");
}

#[test]
fn accepts_test_evidence_when_only_a_mode_is_required() {
    let yaml = VALID_MANIFEST.replace("classification: required", "classification: deferred");
    let manifest = parse_valid(&yaml);

    assert_eq!(
        first_command(&manifest).classification(),
        Classification::Deferred
    );
    assert_eq!(
        first_command(&manifest)
            .modes()
            .get(&Mode::StandaloneCacheOff),
        Some(&Classification::Required)
    );
}

#[test]
fn rejects_an_empty_owner_after_trimming() {
    let yaml = VALID_MANIFEST.replace("owner: cmd-string", "owner: '   '");
    assert_error_contains(&yaml, "owner");
}

#[test]
fn rejects_an_unsupported_classification() {
    let yaml = VALID_MANIFEST.replace("classification: required", "classification: partial");
    assert_error_contains(&yaml, "classification");
}

#[test]
fn rejects_a_known_difference_without_governance_metadata() {
    let yaml = VALID_MANIFEST.replace(
        "classification: required",
        "classification: known_difference",
    );
    assert_error_contains(&yaml, "known_differences");
}

#[test]
fn rejects_missing_governance_when_only_a_mode_has_a_known_difference() {
    let yaml = VALID_MANIFEST
        .replace("classification: required", "classification: deferred")
        .replace(
            "standalone_cache_off: required",
            "standalone_cache_off: known_difference",
        );
    assert_error_contains(&yaml, "known_differences");
}

#[test]
fn loads_governance_when_only_a_mode_has_a_known_difference() {
    let yaml = governed_known_difference_manifest()
        .replace(
            "classification: known_difference",
            "classification: deferred",
        )
        .replace(
            "standalone_cache_off: required",
            "standalone_cache_off: known_difference",
        )
        .replace("tests: [wire-differential, final-state]", "tests: []");
    let manifest = parse_valid(&yaml);
    let command = first_command(&manifest);

    assert_eq!(command.classification(), Classification::Deferred);
    assert_eq!(
        command.modes().get(&Mode::StandaloneCacheOff),
        Some(&Classification::KnownDifference)
    );
    assert_eq!(command.known_differences().len(), 1);
}

#[test]
fn rejects_known_difference_metadata_without_a_matching_classification() {
    let yaml = VALID_MANIFEST.replace(
        "known_differences: []",
        "known_differences:\n      - owner: cmd-string\n        issue: https://github.com/arana-db/kiwi/issues/999\n        reason: Redis behavior is not implemented yet\n        remove_when: wire differential and final-state evidence pass\n        introduced: 2026-08-05\n        affected: standalone_cache_off; resp2/resp3\n        last_verified_ref: redis-source:77b6c308396c9700672390a210143a8496fb4b10",
    );
    assert_error_contains(&yaml, "known_differences");
}

#[test]
fn loads_governed_known_difference_metadata() {
    let yaml = governed_known_difference_manifest();
    let manifest = parse_valid(&yaml);
    let differences = first_command(&manifest).known_differences();

    assert_eq!(differences.len(), 1);
    assert_eq!(differences[0].owner(), "cmd-string");
    assert_eq!(
        differences[0].issue(),
        "https://github.com/arana-db/kiwi/issues/999"
    );
    assert_eq!(
        differences[0].reason(),
        "Redis behavior is not implemented yet"
    );
    assert_eq!(
        differences[0].remove_when(),
        "wire differential and final-state evidence pass"
    );
    assert_eq!(differences[0].introduced(), "2026-08-05");
    assert_eq!(
        differences[0].affected(),
        "standalone_cache_off; resp2/resp3"
    );
    assert_eq!(
        differences[0].last_verified_ref(),
        "redis-source:77b6c308396c9700672390a210143a8496fb4b10"
    );
}

#[test]
fn rejects_blank_known_difference_governance_fields() {
    for (field, original, replacement) in [
        ("owner", "      - owner: cmd-string", "      - owner: '   '"),
        (
            "issue",
            "        issue: https://github.com/arana-db/kiwi/issues/999",
            "        issue: '   '",
        ),
        (
            "reason",
            "        reason: Redis behavior is not implemented yet",
            "        reason: '   '",
        ),
        (
            "remove_when",
            "        remove_when: wire differential and final-state evidence pass",
            "        remove_when: '   '",
        ),
        (
            "introduced",
            "        introduced: 2026-08-05",
            "        introduced: '   '",
        ),
        (
            "affected",
            "        affected: standalone_cache_off; resp2/resp3",
            "        affected: '   '",
        ),
        (
            "last_verified_ref",
            "        last_verified_ref: redis-source:77b6c308396c9700672390a210143a8496fb4b10",
            "        last_verified_ref: '   '",
        ),
    ] {
        let yaml = governed_known_difference_manifest().replace(original, replacement);
        assert_error_contains(&yaml, field);
    }
}

#[test]
fn rejects_known_difference_governance_fields_when_missing() {
    for (field, line) in [
        ("introduced", "        introduced: 2026-08-05\n"),
        (
            "affected",
            "        affected: standalone_cache_off; resp2/resp3\n",
        ),
        (
            "last_verified_ref",
            "        last_verified_ref: redis-source:77b6c308396c9700672390a210143a8496fb4b10\n",
        ),
    ] {
        let yaml = governed_known_difference_manifest().replace(line, "");
        assert_error_contains(&yaml, field);
    }
}

#[test]
fn rejects_a_known_difference_with_an_invalid_introduced_date() {
    let yaml = governed_known_difference_manifest()
        .replace("introduced: 2026-08-05", "introduced: 2026-02-30");
    assert_error_contains(&yaml, "introduced");
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
fn loads_the_authoritative_command_contract_fields() {
    let manifest = parse_valid(VALID_MANIFEST);
    let command = first_command(&manifest);

    assert_eq!(command.arguments(), ArgumentSemantics::Exact);
    assert_eq!(command.reply_schema(), ReplySchema::Exact);
    assert_eq!(command.errors(), ErrorSemantics::ExactPrefix);
    assert_eq!(command.ttl_semantics(), TtlSemantics::Applicable);
    assert_eq!(
        command.tests(),
        &[TestEvidence::WireDifferential, TestEvidence::FinalState]
    );
    assert!(command.known_differences().is_empty());
}

#[test]
fn rejects_missing_authoritative_command_contract_fields() {
    for (field, line) in [
        ("arguments", "    arguments: exact\n"),
        ("reply_schema", "    reply_schema: exact\n"),
        ("errors", "    errors: exact-prefix\n"),
        ("ttl_semantics", "    ttl_semantics: applicable\n"),
        ("tests", "    tests: [wire-differential, final-state]\n"),
        ("known_differences", "    known_differences: []\n"),
    ] {
        let yaml = VALID_MANIFEST.replace(line, "");
        assert_error_contains(&yaml, field);
    }
}

#[test]
fn rejects_unsupported_authoritative_command_contract_values() {
    for (field, supported, unsupported) in [
        ("arguments", "arguments: exact", "arguments: normalized"),
        (
            "reply_schema",
            "reply_schema: exact",
            "reply_schema: approximate",
        ),
        ("errors", "errors: exact-prefix", "errors: ignored"),
        (
            "ttl_semantics",
            "ttl_semantics: applicable",
            "ttl_semantics: unspecified",
        ),
        (
            "tests",
            "tests: [wire-differential, final-state]",
            "tests: [manual-only]",
        ),
    ] {
        let yaml = VALID_MANIFEST.replace(supported, unsupported);
        assert_error_contains(&yaml, field);
    }
}

#[test]
fn loads_a_valid_manifest_through_read_only_getters() {
    let manifest = parse_valid(VALID_MANIFEST);
    let command = first_command(&manifest);

    assert_eq!(manifest.schema(), "kiwi-redis-compat/v1");
    assert_eq!(manifest.profile(), Profile::Redis881StandaloneCacheOff);
    assert_eq!(manifest.redis().tag(), REDIS_TAG);
    assert_eq!(manifest.redis().commit(), REDIS_COMMIT);
    assert_eq!(manifest.commands().len(), 1);
    assert_eq!(command.classification(), Classification::Required);
    assert_eq!(command.protocols(), &[Protocol::Resp2, Protocol::Resp3]);
    assert_eq!(command.owner(), "cmd-string");
}

#[test]
fn loads_the_repository_redis_8_8_1_manifest() {
    let yaml = include_str!("../../../tests/compat/redis-8.8.1/manifest.yaml");
    let manifest = parse_valid(yaml);

    assert_eq!(manifest.schema(), "kiwi-redis-compat/v1");
    assert_eq!(manifest.profile(), Profile::Redis881StandaloneCacheOff);
    assert_eq!(manifest.redis().tag(), REDIS_TAG);
    assert_eq!(manifest.redis().commit(), REDIS_COMMIT);
    assert!(!manifest.commands().is_empty());
    for command in manifest.commands() {
        assert_eq!(command.command(), command.command().to_ascii_uppercase());
        assert!(!command.protocols().is_empty());
        assert!(!command.owner().is_empty());
    }
}

#[test]
fn repository_vector_contract_is_explicitly_governed_while_frozen() {
    let manifest = parse_valid(include_str!(
        "../../../tests/compat/redis-8.8.1/manifest.yaml"
    ));
    let vector_commands = manifest
        .commands()
        .iter()
        .filter(|command| command.command().starts_with('V'))
        .collect::<Vec<_>>();

    for command in &vector_commands {
        assert!(
            !command.tests().contains(&TestEvidence::WireDifferential),
            "{} must not claim wire-differential evidence while VectorSet is frozen",
            command.command()
        );
    }

    for name in ["VADD", "VEMB", "VSIM"] {
        let command = vector_commands
            .iter()
            .find(|command| command.command() == name)
            .unwrap_or_else(|| panic!("{name} must be registered"));
        assert_eq!(command.classification(), Classification::KnownDifference);
        assert_eq!(
            command.modes().get(&Mode::StandaloneCacheOff),
            Some(&Classification::KnownDifference)
        );
        assert!(command.tests().contains(&TestEvidence::FinalState));
        assert!(command.known_differences().iter().all(|difference| {
            !difference.owner().is_empty()
                && !difference.issue().is_empty()
                && !difference.reason().is_empty()
                && !difference.remove_when().is_empty()
                && !difference.introduced().is_empty()
                && !difference.affected().is_empty()
                && !difference.last_verified_ref().is_empty()
        }));
    }

    let command = |name| {
        vector_commands
            .iter()
            .find(|command| command.command() == name)
            .unwrap_or_else(|| panic!("{name} must be registered"))
    };
    for term in [
        "omitted", "default", "Q8", "explicit", "BIN", "REDUCE", "CAS", "EF", "SETATTR", "M",
    ] {
        assert!(
            command("VADD")
                .known_differences()
                .iter()
                .any(|difference| {
                    format!("{} {}", difference.reason(), difference.remove_when()).contains(term)
                }),
            "VADD governance must mention {term}"
        );
    }
    assert!(
        command("VEMB")
            .known_differences()
            .iter()
            .any(|difference| {
                let declared = format!("{} {}", difference.reason(), difference.remove_when())
                    .to_ascii_lowercase();
                ["f32", "precision", "large dynamic ranges"]
                    .iter()
                    .all(|term| declared.contains(term))
            }),
        "VEMB governance must describe the f32 precision risk"
    );
    assert!(
        command("VEMB")
            .known_differences()
            .iter()
            .any(|difference| difference.reason().contains("RAW")),
        "VEMB governance must mention RAW"
    );
    for term in [
        "WITHATTRIBS",
        "EPSILON",
        "EF",
        "FILTER",
        "FILTER-EF",
        "NOTHREAD",
        "max_k",
        "entry",
        "byte",
        "deadline",
    ] {
        assert!(
            command("VSIM")
                .known_differences()
                .iter()
                .any(|difference| {
                    difference.reason().contains(term) || difference.remove_when().contains(term)
                }),
            "VSIM governance must mention {term}"
        );
    }
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

fn governed_known_difference_manifest() -> String {
    VALID_MANIFEST
        .replace(
            "classification: required",
            "classification: known_difference",
        )
        .replace(
            "known_differences: []",
            "known_differences:\n      - owner: cmd-string\n        issue: https://github.com/arana-db/kiwi/issues/999\n        reason: Redis behavior is not implemented yet\n        remove_when: wire differential and final-state evidence pass\n        introduced: 2026-08-05\n        affected: standalone_cache_off; resp2/resp3\n        last_verified_ref: redis-source:77b6c308396c9700672390a210143a8496fb4b10",
        )
}
