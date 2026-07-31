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

//! 单 case 阈值检查器：把一次基线测量与 [`crate::thresholds`] 中的冻结策略逐项对比。
//!
//! `Pass` 只表示该 case 满足阈值；完整 run 的 case 守恒、重复轮次、CV 和来源身份必须
//! 由后续 controller verifier 组合后才能产生发布结论。阈值仍为占位值时策略不可用。

use std::{collections::BTreeSet, path::Path};

use anyhow::Context as _;
use serde::Deserialize;

use crate::thresholds::ThresholdPolicy;

/// 面向外部结果生产者的单 case JSON Schema。
pub const OUTCOME_SCHEMA: &str = include_str!("../schema/outcome.schema.json");

/// 单次基线场景的测量结果（由控制器在固定 Linux/WSL 主机上采集后写出）。
///
/// 本类型只描述阈值检查输入，不代表完整 baseline run 的发布合同。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    pub case_id: String,
    pub qps: f64,
    pub p99_ms: f64,
    pub max_latency_ms: f64,
    pub error_rate: f64,
    pub queued_plus_running_max: usize,
    pub cpu_change_ratio: f64,
    pub rss_change_ratio: f64,
    pub threads_delta: i64,
    pub case: CaseMetrics,
}

/// 每类 case 必须提供的专属指标。
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CaseMetrics {
    NormalLoad {
        anchor_qps: f64,
        anchor_p99_ms: f64,
        anchor_max_latency_ms: f64,
    },
    #[serde(rename = "overload_120pct")]
    Overload120pct {
        error_kinds: Vec<String>,
    },
    StorageGate {
        pause_ms: u64,
        resume_drain_ms: u64,
    },
    Shutdown {
        drain_ms: u64,
    },
    SlowStorage {
        new_conn_ms: u64,
        ping_ms: u64,
    },
}

/// 单 case 阈值检查结果；`Pass` 不代表完整 baseline run 可发布。
#[derive(Debug, PartialEq, Eq)]
pub enum ThresholdCheck {
    Pass,
    PolicyUnavailable(Vec<String>),
    Fail(Vec<String>),
}

/// 使用编译期冻结策略检查单个 case。
pub fn verify_outcome(outcome: &Outcome) -> ThresholdCheck {
    let input_reasons = validate_outcome(outcome);
    if !input_reasons.is_empty() {
        return ThresholdCheck::Fail(input_reasons);
    }

    match ThresholdPolicy::from_frozen_constants() {
        Ok(policy) => verify_outcome_with_policy(outcome, &policy),
        Err(reasons) => ThresholdCheck::PolicyUnavailable(reasons),
    }
}

/// 使用显式策略检查单个 case，供测试和后续 controller 组合使用。
pub fn verify_outcome_with_policy(outcome: &Outcome, policy: &ThresholdPolicy) -> ThresholdCheck {
    let input_reasons = validate_outcome(outcome);
    if !input_reasons.is_empty() {
        return ThresholdCheck::Fail(input_reasons);
    }

    if let Err(reasons) = policy.validate() {
        return ThresholdCheck::PolicyUnavailable(reasons);
    }

    let mut reasons = Vec::new();
    if outcome.queued_plus_running_max > policy.max_queued_plus_running {
        reasons.push(format!(
            "queued plus running {} exceeds {}",
            outcome.queued_plus_running_max, policy.max_queued_plus_running
        ));
    }
    if outcome.cpu_change_ratio.abs() > policy.cpu_allowed_change_ratio {
        reasons.push(format!(
            "CPU change {} exceeds +/-{}",
            outcome.cpu_change_ratio, policy.cpu_allowed_change_ratio
        ));
    }
    if outcome.rss_change_ratio.abs() > policy.rss_allowed_change_ratio {
        reasons.push(format!(
            "RSS change {} exceeds +/-{}",
            outcome.rss_change_ratio, policy.rss_allowed_change_ratio
        ));
    }
    if outcome.threads_delta.unsigned_abs() > policy.threads_allowed_delta as u64 {
        reasons.push(format!(
            "thread delta {} exceeds +/-{}",
            outcome.threads_delta, policy.threads_allowed_delta
        ));
    }

    match &outcome.case {
        CaseMetrics::NormalLoad {
            anchor_qps,
            anchor_p99_ms,
            anchor_max_latency_ms,
        } => {
            let throughput_regression = (anchor_qps - outcome.qps) / anchor_qps;
            if throughput_regression > policy.normal_load_throughput_max_regression_ratio {
                reasons.push(format!(
                    "throughput regression {throughput_regression} exceeds {}",
                    policy.normal_load_throughput_max_regression_ratio
                ));
            }
            let p99_change = (outcome.p99_ms - anchor_p99_ms) / anchor_p99_ms;
            if p99_change > policy.p99_max_latency_allowed_change_ratio {
                reasons.push(format!(
                    "p99 change {p99_change} exceeds {}",
                    policy.p99_max_latency_allowed_change_ratio
                ));
            }
            let max_latency_change =
                (outcome.max_latency_ms - anchor_max_latency_ms) / anchor_max_latency_ms;
            if max_latency_change > policy.max_latency_allowed_change_ratio {
                reasons.push(format!(
                    "max latency change {max_latency_change} exceeds {}",
                    policy.max_latency_allowed_change_ratio
                ));
            }
            if outcome.error_rate > policy.normal_load_max_error_rate {
                reasons.push(format!(
                    "normal error rate {} exceeds {}",
                    outcome.error_rate, policy.normal_load_max_error_rate
                ));
            }
        }
        CaseMetrics::Overload120pct { error_kinds } => {
            if outcome.error_rate > policy.overload_120pct_max_error_rate {
                reasons.push(format!(
                    "overload error rate {} exceeds {}",
                    outcome.error_rate, policy.overload_120pct_max_error_rate
                ));
            }
            for kind in error_kinds {
                if !policy
                    .overload_120pct_allowed_error_kinds
                    .iter()
                    .any(|allowed| allowed == kind)
                {
                    reasons.push(format!("error kind {kind:?} is not allowed under overload"));
                }
            }
        }
        CaseMetrics::StorageGate {
            pause_ms,
            resume_drain_ms,
        } => {
            if *pause_ms > policy.storage_gate_pause_max_ms {
                reasons.push(format!(
                    "storage gate pause {pause_ms} ms exceeds {} ms",
                    policy.storage_gate_pause_max_ms
                ));
            }
            if *resume_drain_ms > policy.storage_gate_resume_drain_max_ms {
                reasons.push(format!(
                    "storage gate resume drain {resume_drain_ms} ms exceeds {} ms",
                    policy.storage_gate_resume_drain_max_ms
                ));
            }
        }
        CaseMetrics::Shutdown { drain_ms } => {
            if *drain_ms > policy.shutdown_drain_max_ms {
                reasons.push(format!(
                    "shutdown drain {drain_ms} ms exceeds {} ms",
                    policy.shutdown_drain_max_ms
                ));
            }
        }
        CaseMetrics::SlowStorage {
            new_conn_ms,
            ping_ms,
        } => {
            if *new_conn_ms > policy.slow_storage_new_conn_max_ms {
                reasons.push(format!(
                    "slow storage new connection {new_conn_ms} ms exceeds {} ms",
                    policy.slow_storage_new_conn_max_ms
                ));
            }
            if *ping_ms > policy.slow_storage_ping_max_ms {
                reasons.push(format!(
                    "slow storage PING {ping_ms} ms exceeds {} ms",
                    policy.slow_storage_ping_max_ms
                ));
            }
        }
    }

    if reasons.is_empty() {
        ThresholdCheck::Pass
    } else {
        ThresholdCheck::Fail(reasons)
    }
}

fn validate_outcome(outcome: &Outcome) -> Vec<String> {
    let mut reasons = Vec::new();
    if outcome.case_id.trim().is_empty() {
        reasons.push("case_id must not be empty".to_string());
    }
    validate_nonnegative("qps", outcome.qps, &mut reasons);
    validate_nonnegative("p99_ms", outcome.p99_ms, &mut reasons);
    validate_nonnegative("max_latency_ms", outcome.max_latency_ms, &mut reasons);
    if outcome.max_latency_ms < outcome.p99_ms {
        reasons.push("max_latency_ms must be greater than or equal to p99_ms".to_string());
    }
    if !outcome.error_rate.is_finite() || !(0.0..=1.0).contains(&outcome.error_rate) {
        reasons.push("error_rate must be finite and within [0, 1]".to_string());
    }
    validate_change_ratio("cpu_change_ratio", outcome.cpu_change_ratio, &mut reasons);
    validate_change_ratio("rss_change_ratio", outcome.rss_change_ratio, &mut reasons);

    match &outcome.case {
        CaseMetrics::NormalLoad {
            anchor_qps,
            anchor_p99_ms,
            anchor_max_latency_ms,
        } => {
            validate_positive("anchor_qps", *anchor_qps, &mut reasons);
            validate_positive("anchor_p99_ms", *anchor_p99_ms, &mut reasons);
            validate_positive(
                "anchor_max_latency_ms",
                *anchor_max_latency_ms,
                &mut reasons,
            );
            if anchor_max_latency_ms < anchor_p99_ms {
                reasons.push(
                    "anchor_max_latency_ms must be greater than or equal to anchor_p99_ms"
                        .to_string(),
                );
            }
        }
        CaseMetrics::Overload120pct { error_kinds } => {
            if outcome.error_rate > 0.0 && error_kinds.is_empty() {
                reasons
                    .push("overload error_kinds must be present when errors occurred".to_string());
            }
            let mut unique = BTreeSet::new();
            for kind in error_kinds {
                let normalized = kind.trim();
                if normalized.is_empty() {
                    reasons
                        .push("overload error_kinds must not contain an empty value".to_string());
                } else if normalized != kind {
                    reasons.push(
                        "overload error_kinds must not contain leading or trailing whitespace"
                            .to_string(),
                    );
                } else if !unique.insert(normalized) {
                    reasons.push(format!("duplicate overload error kind: {normalized}"));
                }
            }
        }
        CaseMetrics::StorageGate { .. }
        | CaseMetrics::Shutdown { .. }
        | CaseMetrics::SlowStorage { .. } => {}
    }

    reasons
}

fn validate_nonnegative(name: &str, value: f64, reasons: &mut Vec<String>) {
    if !value.is_finite() || value < 0.0 {
        reasons.push(format!("{name} must be finite and non-negative"));
    }
}

fn validate_positive(name: &str, value: f64, reasons: &mut Vec<String>) {
    if !value.is_finite() || value <= 0.0 {
        reasons.push(format!("{name} must be finite and greater than zero"));
    }
}

fn validate_change_ratio(name: &str, value: f64, reasons: &mut Vec<String>) {
    if !value.is_finite() || value < -1.0 {
        reasons.push(format!("{name} must be finite and no less than -1"));
    }
}

/// 从文件读取 [`Outcome`] 并验证。
pub fn verify_outcome_path(path: &Path) -> anyhow::Result<ThresholdCheck> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read outcome file {}", path.display()))?;
    let outcome: Outcome = serde_json::from_str(&raw)
        .with_context(|| format!("parse outcome json {}", path.display()))?;
    Ok(verify_outcome(&outcome))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    use crate::thresholds::ThresholdPolicy;

    fn frozen_policy() -> ThresholdPolicy {
        ThresholdPolicy {
            normal_load_throughput_max_regression_ratio: 0.10,
            p99_max_latency_allowed_change_ratio: 0.10,
            max_latency_allowed_change_ratio: 0.20,
            normal_load_max_error_rate: 0.01,
            overload_120pct_max_error_rate: 0.10,
            overload_120pct_allowed_error_kinds: vec!["command_error".to_string()],
            max_queued_plus_running: 100,
            storage_gate_pause_max_ms: 10,
            storage_gate_resume_drain_max_ms: 20,
            shutdown_drain_max_ms: 30,
            slow_storage_new_conn_max_ms: 50,
            slow_storage_ping_max_ms: 10,
            cpu_allowed_change_ratio: 0.20,
            rss_allowed_change_ratio: 0.20,
            threads_allowed_delta: 2,
        }
    }

    fn common(case: Value) -> Value {
        json!({
            "case_id": "normal-get-c16-p16",
            "qps": 90.0,
            "p99_ms": 11.0,
            "max_latency_ms": 12.0,
            "error_rate": 0.01,
            "queued_plus_running_max": 100,
            "cpu_change_ratio": 0.20,
            "rss_change_ratio": -0.20,
            "threads_delta": -2,
            "case": case
        })
    }

    fn normal_value() -> Value {
        common(json!({
            "kind": "normal_load",
            "anchor_qps": 100.0,
            "anchor_p99_ms": 10.0,
            "anchor_max_latency_ms": 10.0
        }))
    }

    fn parse(value: Value) -> Outcome {
        serde_json::from_value(value).expect("valid threshold outcome")
    }

    fn assert_failed(value: Value, reason: &str) {
        let outcome = parse(value);
        let ThresholdCheck::Fail(reasons) = verify_outcome_with_policy(&outcome, &frozen_policy())
        else {
            panic!("outcome must fail threshold checking");
        };
        assert!(
            reasons.iter().any(|candidate| candidate.contains(reason)),
            "missing reason {reason:?} in {reasons:?}"
        );
    }

    #[test]
    fn placeholder_thresholds_make_policy_unavailable() {
        assert!(matches!(
            verify_outcome(&parse(normal_value())),
            ThresholdCheck::PolicyUnavailable(_)
        ));
    }

    #[test]
    fn invalid_input_fails_before_policy_availability() {
        let mut value = normal_value();
        value["case_id"] = json!("  ");

        let ThresholdCheck::Fail(reasons) = verify_outcome(&parse(value)) else {
            panic!("invalid input must fail even while frozen policy is unavailable");
        };
        assert!(
            reasons.iter().any(|reason| reason.contains("case_id")),
            "unexpected input reasons: {reasons:?}"
        );
    }

    #[test]
    fn normal_load_accepts_exact_threshold_boundaries() {
        assert_eq!(
            verify_outcome_with_policy(&parse(normal_value()), &frozen_policy()),
            ThresholdCheck::Pass
        );
    }

    #[test]
    fn normal_load_checks_every_common_and_relative_threshold() {
        let cases = [
            ("qps", json!(89.0), "throughput regression"),
            ("p99_ms", json!(11.1), "p99 change"),
            ("max_latency_ms", json!(12.1), "max latency change"),
            ("error_rate", json!(0.011), "normal error rate"),
            ("queued_plus_running_max", json!(101), "queued plus running"),
            ("cpu_change_ratio", json!(0.21), "CPU change"),
            ("rss_change_ratio", json!(-0.21), "RSS change"),
            ("threads_delta", json!(-3), "thread delta"),
        ];
        for (field, replacement, reason) in cases {
            let mut value = normal_value();
            value[field] = replacement;
            assert_failed(value, reason);
        }
    }

    #[test]
    fn case_specific_metrics_accept_exact_threshold_boundaries() {
        let passing = [
            common(json!({
                "kind": "overload_120pct",
                "error_kinds": ["command_error"]
            })),
            common(json!({
                "kind": "storage_gate",
                "pause_ms": 10,
                "resume_drain_ms": 20
            })),
            common(json!({ "kind": "shutdown", "drain_ms": 30 })),
            common(json!({
                "kind": "slow_storage",
                "new_conn_ms": 50,
                "ping_ms": 10
            })),
        ];
        for value in passing {
            assert_eq!(
                verify_outcome_with_policy(&parse(value), &frozen_policy()),
                ThresholdCheck::Pass
            );
        }
    }

    #[test]
    fn overload_checks_error_rate_threshold() {
        let mut overload_rate = common(json!({
            "kind": "overload_120pct",
            "error_kinds": ["command_error"]
        }));
        overload_rate["error_rate"] = json!(0.101);
        assert_failed(overload_rate, "overload error rate");
    }

    #[test]
    fn overload_rejects_error_kind_outside_policy() {
        assert_failed(
            common(json!({
                "kind": "overload_120pct",
                "error_kinds": ["timeout"]
            })),
            "error kind",
        );
    }

    #[test]
    fn storage_gate_checks_each_duration_threshold() {
        assert_failed(
            common(json!({
                "kind": "storage_gate",
                "pause_ms": 11,
                "resume_drain_ms": 20
            })),
            "storage gate pause",
        );
        assert_failed(
            common(json!({
                "kind": "storage_gate",
                "pause_ms": 10,
                "resume_drain_ms": 21
            })),
            "storage gate resume drain",
        );
    }

    #[test]
    fn shutdown_checks_drain_threshold() {
        assert_failed(
            common(json!({ "kind": "shutdown", "drain_ms": 31 })),
            "shutdown drain",
        );
    }

    #[test]
    fn slow_storage_checks_each_latency_threshold() {
        assert_failed(
            common(json!({
                "kind": "slow_storage",
                "new_conn_ms": 51,
                "ping_ms": 10
            })),
            "slow storage new connection",
        );
        assert_failed(
            common(json!({
                "kind": "slow_storage",
                "new_conn_ms": 50,
                "ping_ms": 11
            })),
            "slow storage PING",
        );
    }

    #[test]
    fn invalid_policy_is_unavailable() {
        let mut policy = frozen_policy();
        policy.overload_120pct_allowed_error_kinds = vec![" command_error".to_string()];

        let ThresholdCheck::PolicyUnavailable(reasons) =
            verify_outcome_with_policy(&parse(normal_value()), &policy)
        else {
            panic!("invalid policy must be unavailable");
        };
        assert!(
            reasons
                .iter()
                .any(|reason| reason.contains("leading or trailing whitespace")),
            "unexpected policy reasons: {reasons:?}"
        );
    }

    #[test]
    fn invalid_or_incomplete_input_fails_closed() {
        let mut empty_id = normal_value();
        empty_id["case_id"] = json!("  ");
        assert_failed(empty_id, "case_id");

        let mut negative_qps = normal_value();
        negative_qps["qps"] = json!(-1.0);
        assert_failed(negative_qps, "qps");

        let mut invalid_rate = normal_value();
        invalid_rate["error_rate"] = json!(1.1);
        assert_failed(invalid_rate, "error_rate");

        let mut unknown = normal_value();
        unknown["unexpected"] = json!(true);
        assert!(serde_json::from_value::<Outcome>(unknown).is_err());

        let missing_case_metric = common(json!({ "kind": "shutdown" }));
        assert!(serde_json::from_value::<Outcome>(missing_case_metric).is_err());

        let unknown_case = common(json!({ "kind": "not-a-case" }));
        assert!(serde_json::from_value::<Outcome>(unknown_case).is_err());

        assert_failed(
            common(json!({
                "kind": "overload_120pct",
                "error_kinds": [" command_error"]
            })),
            "leading or trailing whitespace",
        );
    }

    #[test]
    fn schema_is_strict_2020_12_threshold_contract() {
        let schema: Value = serde_json::from_str(OUTCOME_SCHEMA).expect("schema is JSON");
        assert_eq!(
            schema["$schema"],
            json!("https://json-schema.org/draft/2020-12/schema")
        );
        assert_eq!(schema["additionalProperties"], json!(false));

        let required = schema["required"]
            .as_array()
            .expect("top-level required fields");
        for field in [
            "case_id",
            "qps",
            "p99_ms",
            "max_latency_ms",
            "error_rate",
            "queued_plus_running_max",
            "cpu_change_ratio",
            "rss_change_ratio",
            "threads_delta",
            "case",
        ] {
            assert!(required.contains(&json!(field)), "missing required {field}");
        }

        let variants = schema["properties"]["case"]["oneOf"]
            .as_array()
            .expect("case variants");
        let mut kinds = variants
            .iter()
            .map(|variant| {
                assert_eq!(variant["additionalProperties"], json!(false));
                variant["properties"]["kind"]["const"]
                    .as_str()
                    .expect("case kind")
            })
            .collect::<Vec<_>>();
        kinds.sort_unstable();
        assert_eq!(
            kinds,
            [
                "normal_load",
                "overload_120pct",
                "shutdown",
                "slow_storage",
                "storage_gate",
            ]
        );
    }

    #[test]
    fn verify_outcome_path_rejects_missing_file() -> anyhow::Result<()> {
        let p = Path::new("/nonexistent/outcome.json");
        let res = verify_outcome_path(p);
        assert!(res.is_err(), "缺失文件应返回错误");
        Ok(())
    }
}
