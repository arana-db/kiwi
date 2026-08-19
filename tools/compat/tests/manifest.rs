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
    Protocol, REDIS_COMMIT, REDIS_TAG, ReplySchema, RequiredVectorJobs, TestEvidence, TtlSemantics,
};
use std::collections::BTreeSet;

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
    for (field, line, replacement) in [
        ("owner", "      - owner: cmd-string\n", "      -\n"),
        (
            "issue",
            "        issue: https://github.com/arana-db/kiwi/issues/999\n",
            "",
        ),
        (
            "reason",
            "        reason: Redis behavior is not implemented yet\n",
            "",
        ),
        (
            "remove_when",
            "        remove_when: wire differential and final-state evidence pass\n",
            "",
        ),
        ("introduced", "        introduced: 2026-08-05\n", ""),
        (
            "affected",
            "        affected: standalone_cache_off; resp2/resp3\n",
            "",
        ),
        (
            "last_verified_ref",
            "        last_verified_ref: redis-source:77b6c308396c9700672390a210143a8496fb4b10\n",
            "",
        ),
    ] {
        let yaml = governed_known_difference_manifest().replace(line, replacement);
        assert_error_contains(&yaml, field);
    }
}

#[test]
fn repository_vector_operational_limits_are_explicitly_governed() {
    const ISSUE_418: &str = "https://github.com/arana-db/kiwi/issues/418";
    const ISSUE_421: &str = "https://github.com/arana-db/kiwi/issues/421";
    const OPERATIONAL_LIMIT_PREFIX: &str = "Operational-limit difference:";

    fn validate_operational_limits(yaml: &str) -> Result<(), String> {
        let manifest = parse_valid(yaml);
        let expected = [
            (
                "VADD",
                &[
                    "max_dimension",
                    "max_vector_bytes",
                    "max_element_bytes",
                    "raw",
                ][..],
            ),
            (
                "VSIM",
                &["max_dimension", "max_vector_bytes", "max_element_bytes"][..],
            ),
            ("VEMB", &["max_element_bytes"][..]),
            ("VREM", &["max_element_bytes"][..]),
            ("VISMEMBER", &["max_element_bytes"][..]),
        ];
        let expected_commands = expected
            .iter()
            .map(|(name, _)| *name)
            .collect::<BTreeSet<_>>();
        let mut observed_commands = BTreeSet::new();

        for command in manifest.commands() {
            let differences = command
                .known_differences()
                .iter()
                .filter(|difference| difference.reason().starts_with(OPERATIONAL_LIMIT_PREFIX))
                .collect::<Vec<_>>();
            if differences.len() > 1 {
                return Err(format!(
                    "{} must register exactly one operational-limit difference",
                    command.command()
                ));
            }
            let Some(difference) = differences.first() else {
                continue;
            };

            let name = command.command();
            observed_commands.insert(name);
            let Some((_, reason_terms)) = expected.iter().find(|(expected, _)| *expected == name)
            else {
                return Err(format!("unexpected operational-limit command {name}"));
            };
            if command.classification() != Classification::KnownDifference
                || command.modes().get(&Mode::StandaloneCacheOff)
                    != Some(&Classification::KnownDifference)
            {
                return Err(format!("{name} must remain a known difference"));
            }
            if difference.issue() != ISSUE_418 {
                return Err(format!(
                    "{name} operational-limit difference must be owned by Issue #418"
                ));
            }
            if difference.owner() != "cmd-vector"
                || difference.affected() != "standalone_cache_off; resp2/resp3"
                || difference.last_verified_ref() != format!("redis-source:{REDIS_COMMIT}")
            {
                return Err(format!(
                    "{name} operational-limit governance metadata must remain intact"
                ));
            }
            let reason = difference.reason().to_ascii_lowercase();
            for term in *reason_terms {
                if !reason.contains(term) {
                    return Err(format!(
                        "{name} operational-limit reason must mention {term}"
                    ));
                }
            }
            let removal = difference.remove_when().to_ascii_lowercase();
            for term in ["raw", "resp2", "resp3", "boundary"] {
                if !removal.contains(term) {
                    return Err(format!(
                        "{name} operational-limit removal condition must mention {term}"
                    ));
                }
            }
        }

        if observed_commands != expected_commands {
            return Err(format!(
                "operational-limit commands must be exactly {expected_commands:?}, found {observed_commands:?}"
            ));
        }
        if yaml.contains(ISSUE_421) {
            return Err("the compatibility manifest must not retain Issue #421 ownership".into());
        }
        Ok(())
    }

    let yaml = include_str!("../../../tests/compat/redis-8.8.1/manifest.yaml");
    validate_operational_limits(yaml).unwrap();

    let reason_index = yaml
        .find("reason: \"Operational-limit difference:")
        .expect("the repository manifest must contain an operational-limit difference");
    let issue_index = yaml[..reason_index]
        .rfind(ISSUE_418)
        .expect("the first operational-limit difference must be owned by Issue #418");
    let mut reverted = yaml.to_owned();
    reverted.replace_range(issue_index..issue_index + ISSUE_418.len(), ISSUE_421);
    assert!(
        validate_operational_limits(&reverted).is_err(),
        "restoring one operational-limit owner to Issue #421 must fail"
    );
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
fn repository_vector_contract_has_required_wire_differential_evidence() {
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
            command.tests().contains(&TestEvidence::WireDifferential),
            "{} must be owned by the required wire-differential gate",
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
    let vinfo_governance = command("VINFO")
        .known_differences()
        .iter()
        .map(|difference| difference.reason())
        .collect::<Vec<_>>()
        .join(" ");
    for field in ["hnsw-m", "max-level", "vset-uid", "hnsw-max-node-uid"] {
        assert!(
            vinfo_governance.contains(field),
            "VINFO raw-schema payload allowance must be explicitly governed for {field}"
        );
    }
    for invariant in [
        "field token",
        "order",
        "container",
        "pair count",
        "frame types",
    ] {
        assert!(
            vinfo_governance.contains(invariant),
            "VINFO raw-schema invariant must remain exact for {invariant}"
        );
    }
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

#[test]
fn repository_required_vector_job_matches_manifest_and_exact_pytest_collection() {
    let manifest = parse_valid(include_str!(
        "../../../tests/compat/redis-8.8.1/manifest.yaml"
    ));
    let registry = RequiredVectorJobs::from_yaml(include_str!(
        "../../../tests/compat/redis-8.8.1/vector-required-jobs.yaml"
    ))
    .expect("required Vector job registry must load");

    assert_eq!(registry.job_id(), "trusted-vector-differential");
    assert_eq!(
        registry.test_module(),
        "tests/python/test_vector_set_differential.py"
    );
    assert_eq!(registry.pytest_marker(), "raw_vector_protocol");
    assert_eq!(registry.protocols(), &[Protocol::Resp2, Protocol::Resp3]);
    assert_eq!(registry.manifest_profile(), manifest.profile());
    assert_eq!(registry.fast_job_owner(), registry.job_id());
    assert_eq!(
        registry.fast_job_deselect_marker(),
        registry.pytest_marker()
    );
    assert_eq!(
        registry.expected_item_count(),
        registry.expected_node_ids().len()
    );
    assert!(registry.expected_item_count() > 0);
    assert_eq!(
        registry.final_state_applicability().len(),
        registry.expected_item_count(),
        "every required pytest node must own final-state applicability",
    );
    assert_eq!(
        registry
            .final_state_applicability()
            .keys()
            .collect::<BTreeSet<_>>(),
        registry.expected_node_ids().iter().collect::<BTreeSet<_>>(),
        "final-state applicability must exactly cover the required collection",
    );
    for node_id in registry.expected_node_ids() {
        let applicability = registry
            .final_state_applicability()
            .get(node_id)
            .expect("required node must own final-state applicability");
        if node_id.contains("test_raw_comparator_rejects_")
            || node_id.contains("test_vinfo_raw_schema_")
            || node_id.contains("test_raw_cleanup_")
            || node_id.contains("test_raw_endpoint_separation_")
        {
            assert_eq!(applicability.applicability(), "not-applicable");
            assert_eq!(applicability.reason(), Some("comparator"));
            assert_eq!(applicability.state_profile(), None);
            assert_eq!(applicability.observation_profile(), None);
        } else if node_id.contains("test_vinfo_raw_parser_") {
            assert_eq!(applicability.applicability(), "not-applicable");
            assert_eq!(applicability.reason(), Some("parser"));
            assert_eq!(applicability.state_profile(), None);
            assert_eq!(applicability.observation_profile(), None);
        } else {
            assert_eq!(applicability.applicability(), "server-backed");
            assert_eq!(applicability.reason(), None);
            assert!(applicability.state_profile().is_some());
            assert_eq!(
                applicability.observation_profile(),
                Some("complete-vector-state-v1")
            );
        }
    }
    for protocol in ["resp2", "resp3"] {
        let node_id = format!(
            "tests/python/test_vector_set_differential.py::test_repeated_vadd_and_vsim_options_match[{protocol}]"
        );
        assert_eq!(
            registry
                .final_state_applicability()
                .get(&node_id)
                .expect("repeated VADD node must own final-state applicability")
                .state_profile(),
            Some("typed-main-two-member-vector")
        );
    }

    let manifest_commands = manifest
        .commands()
        .iter()
        .filter(|command| {
            command.command().starts_with('V')
                && (matches!(
                    command.classification(),
                    Classification::Required | Classification::KnownDifference
                ) || matches!(
                    command.modes().get(&Mode::StandaloneCacheOff),
                    Some(Classification::Required | Classification::KnownDifference)
                ))
        })
        .map(|command| command.command().to_string())
        .collect::<BTreeSet<_>>();
    let registry_commands = registry.commands().iter().cloned().collect::<BTreeSet<_>>();
    assert_eq!(registry_commands, manifest_commands);
    assert_eq!(
        registry
            .raw_cases()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        registry_commands,
        "every claimed required command must own observed raw RESP cases",
    );
    for (command, raw_cases) in registry.raw_cases() {
        let expected_case_count = if command == "VINFO" { 2 } else { 1 };
        assert_eq!(
            raw_cases.len(),
            expected_case_count,
            "{command} raw-case ownership drifted"
        );
        assert!(raw_cases.iter().all(|raw_case| {
            raw_case
                .node_ids()
                .iter()
                .all(|node_id| registry.expected_node_ids().contains(node_id))
        }));
        assert!(raw_cases.iter().all(|raw_case| {
            raw_case
                .request_base64_by_node()
                .keys()
                .collect::<BTreeSet<_>>()
                == raw_case.node_ids().iter().collect::<BTreeSet<_>>()
                && raw_case
                    .request_base64_by_node()
                    .values()
                    .all(|request| !request.is_empty())
        }));
    }
    let vinfo_cases = registry
        .raw_cases()
        .get("VINFO")
        .expect("required registry must contain VINFO raw cases");
    assert!(vinfo_cases.iter().any(|raw_case| {
        raw_case.case_id() == "missing-key" && raw_case.evidence_kind() == "exact-frame"
    }));
    assert!(vinfo_cases.iter().any(|raw_case| {
        raw_case.case_id() == "populated" && raw_case.evidence_kind() == "raw-schema"
    }));

    let node_ids = registry.expected_node_ids().iter().collect::<BTreeSet<_>>();
    assert_eq!(node_ids.len(), registry.expected_item_count());
    assert!(
        registry.expected_node_ids().iter().all(|node_id| {
            node_id.starts_with("tests/python/test_vector_set_differential.py::")
        })
    );
}

#[test]
fn required_vector_registry_rejects_a_command_without_raw_case_ownership() {
    let yaml = include_str!("../../../tests/compat/redis-8.8.1/vector-required-jobs.yaml")
        .replace("\r\n", "\n");
    let start = yaml
        .find("      VCARD:\n")
        .expect("registry fixture must contain VCARD");
    let end = yaml[start..]
        .find("      VDIM:\n")
        .expect("registry fixture must contain VDIM after VCARD")
        + start;
    let without_vcard = format!("{}{}", &yaml[..start], &yaml[end..]);
    assert_ne!(
        without_vcard, yaml,
        "registry fixture must contain the VCARD raw case"
    );
    assert!(RequiredVectorJobs::from_yaml(&without_vcard).is_err());
}

#[test]
fn required_vector_registry_requires_populated_vinfo_raw_schema_evidence() {
    let yaml = include_str!("../../../tests/compat/redis-8.8.1/vector-required-jobs.yaml")
        .replace("\r\n", "\n");
    let start = yaml
        .find("        - case_id: populated\n")
        .expect("registry fixture must contain populated VINFO");
    let end = yaml[start..]
        .find("      VISMEMBER:\n")
        .expect("registry fixture must contain VISMEMBER after populated VINFO")
        + start;
    let without_populated = format!("{}{}", &yaml[..start], &yaml[end..]);
    assert_ne!(
        without_populated, yaml,
        "registry fixture must own populated VINFO"
    );
    assert!(RequiredVectorJobs::from_yaml(&without_populated).is_err());
}

#[test]
fn required_vector_registry_requires_one_exact_request_per_raw_case_node() {
    let yaml = include_str!("../../../tests/compat/redis-8.8.1/vector-required-jobs.yaml")
        .replace("\r\n", "\n");
    let request = "            \"tests/python/test_vector_set_differential.py::test_zero_vector_values_raw_differential[resp2]\": KjgNCiQ0DQpWQUREDQokMjQNCnRlc3RfdmRpZmY6cmF3OnAyOnZhbHVlcw0KJDYNClZBTFVFUw0KJDENCjINCiQxDQowDQokMQ0KMA0KJDQNCnplcm8NCiQ3DQpOT1FVQU5UDQo=\n";
    let valid_base64 = request
        .trim()
        .split_once(": ")
        .expect("request fixture must contain a YAML value")
        .1;
    let missing = yaml.replacen(request, "", 1);
    assert_ne!(
        missing, yaml,
        "registry fixture must contain the VADD request"
    );
    assert!(RequiredVectorJobs::from_yaml(&missing).is_err());

    let extra = yaml.replacen(
        "          request_base64_by_node:\n",
        "          request_base64_by_node:\n            \"tests/python/test_vector_set_differential.py::test_unregistered_node[resp2]\": KjENCiQ0DQpWQUREDQo=\n",
        1,
    );
    assert_ne!(
        extra, yaml,
        "registry fixture must contain raw request ownership"
    );
    assert!(RequiredVectorJobs::from_yaml(&extra).is_err());

    let invalid_base64 = yaml.replacen(valid_base64, "not_base64!", 1);
    assert_ne!(
        invalid_base64, yaml,
        "registry fixture must contain the canonical VADD request"
    );
    assert!(RequiredVectorJobs::from_yaml(&invalid_base64).is_err());

    let unpadded_base64 = valid_base64.trim_end_matches('=');
    assert_ne!(
        unpadded_base64, valid_base64,
        "request fixture must exercise Base64 padding"
    );
    let noncanonical_base64 = yaml.replacen(valid_base64, unpadded_base64, 1);
    assert_ne!(
        noncanonical_base64, yaml,
        "registry fixture must contain the padded VADD request"
    );
    assert!(RequiredVectorJobs::from_yaml(&noncanonical_base64).is_err());
}

#[test]
fn required_vector_registry_rejects_node_count_and_identity_drift() {
    let yaml = include_str!("../../../tests/compat/redis-8.8.1/vector-required-jobs.yaml");
    let count_drift = yaml.replace("expected_item_count: 40", "expected_item_count: 39");
    assert_ne!(count_drift, yaml, "registry fixture must contain count 40");
    assert!(RequiredVectorJobs::from_yaml(&count_drift).is_err());

    let identity_drift = yaml.replace(
        "test_raw_cleanup_requires_a_nonnegative_integer_frame",
        "test_unregistered_collection_node",
    );
    let registry = RequiredVectorJobs::from_yaml(&identity_drift)
        .expect("structurally valid node drift must still parse");
    assert_ne!(
        registry.expected_node_ids(),
        RequiredVectorJobs::from_yaml(yaml)
            .expect("repository registry must parse")
            .expected_node_ids()
    );
}

#[test]
fn required_vector_registry_rejects_final_state_ownership_drift() {
    let yaml = include_str!("../../../tests/compat/redis-8.8.1/vector-required-jobs.yaml")
        .replace("\r\n", "\n");

    let missing = yaml.replacen(
        "      \"tests/python/test_vector_set_differential.py::test_raw_comparator_rejects_equal_typed_values_with_different_frames\":\n        applicability: not-applicable\n        reason: comparator\n",
        "",
        1,
    );
    assert_ne!(
        missing, yaml,
        "registry fixture must name every final-state node"
    );
    assert!(RequiredVectorJobs::from_yaml(&missing).is_err());

    let extra = yaml.replacen(
        "    expected_node_ids:\n",
        "      tests/python/test_vector_set_differential.py::test_unregistered_node:\n        applicability: server-backed\n    expected_node_ids:\n",
        1,
    );
    assert_ne!(
        extra, yaml,
        "registry fixture must contain final-state mapping"
    );
    assert!(RequiredVectorJobs::from_yaml(&extra).is_err());

    let unknown_field = yaml.replacen(
        "        applicability: server-backed\n",
        "        applicability: server-backed\n        clock_tolerance_ms: 1000\n",
        1,
    );
    assert_ne!(
        unknown_field, yaml,
        "registry fixture must contain server-backed nodes"
    );
    assert!(RequiredVectorJobs::from_yaml(&unknown_field).is_err());

    let self_declared_skip = yaml.replacen(
        "        applicability: server-backed\n",
        "        applicability: not-applicable\n        reason: test-declared\n",
        1,
    );
    assert_ne!(
        self_declared_skip, yaml,
        "registry fixture must contain server-backed nodes"
    );
    assert!(RequiredVectorJobs::from_yaml(&self_declared_skip).is_err());
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
