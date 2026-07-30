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

//! Outcome 比对器：把单次基线测量结果与 [`crate::thresholds`] 中冻结的阈值逐一对比，
//! 判定该 case 是否 `Publishable` / `NonPublishable` / `Fail`。
//!
//! 在阈值尚未回填（仍为占位哨兵值）时，任何 case 都判为 `NonPublishable`——
//! 这是「不伪造数字」的硬保证：占位值绝不能被当成通过阈值。

use std::path::Path;

use anyhow::Context as _;
use serde::Deserialize;

use crate::thresholds;

/// 单次基线场景的测量结果（由控制器在固定 Linux/WSL 主机上采集后写出）。
///
/// 字段允许前向兼容（不强制 `deny_unknown_fields`），未来扩展指标时旧 verifier 仍可解析。
#[derive(Debug, Deserialize)]
pub struct Outcome {
    pub case_id: String,
    pub qps: f64,
    pub p99_ms: f64,
    pub max_latency_ms: f64,
    pub error_rate: f64,
    #[serde(default)]
    pub overload_120pct_error_rate: Option<f64>,
    pub queued_plus_running_max: usize,
    #[serde(default)]
    pub storage_gate_pause_ms: Option<u64>,
    #[serde(default)]
    pub storage_gate_resume_drain_ms: Option<u64>,
    #[serde(default)]
    pub shutdown_drain_ms: Option<u64>,
    #[serde(default)]
    pub slow_storage_new_conn_ms: Option<u64>,
    #[serde(default)]
    pub slow_storage_ping_ms: Option<u64>,
    #[serde(default)]
    pub cpu_change_ratio: Option<f64>,
    #[serde(default)]
    pub rss_change_ratio: Option<f64>,
    #[serde(default)]
    pub threads_delta: Option<i64>,
}

/// 单个 case 的验证结论。
#[derive(Debug, PartialEq, Eq)]
pub enum VerifyStatus {
    /// 阈值已全部回填且本 case 满足全部阈值。
    Publishable,
    /// 阈值尚未回填（仍是占位哨兵值），本 case 不可发布。
    NonPublishable,
    /// 阈值已回填，但本 case 超出某项阈值。
    Fail(String),
}

/// 把占位哨兵值归一为 `None`：只有真正回填的阈值才参与判定。
fn frozen_u64(v: u64) -> Option<u64> {
    if v == u64::MAX {
        None
    } else {
        Some(v)
    }
}

/// 把占位哨兵值归一为 `None`：只有真正回填的阈值才参与判定。
fn frozen_usize(v: usize) -> Option<usize> {
    if v == usize::MAX {
        None
    } else {
        Some(v)
    }
}

/// 把占位哨兵值（NaN）归一为 `None`：只有真正回填的阈值才参与判定。
fn frozen_f64(v: f64) -> Option<f64> {
    if v.is_nan() {
        None
    } else {
        Some(v)
    }
}

/// 对比单个 [`Outcome`] 与冻结阈值。
pub fn verify_outcome(o: &Outcome) -> VerifyStatus {
    if !thresholds::all_frozen() {
        return VerifyStatus::NonPublishable;
    }
    // TODO(#351): 真实比对方式需结合 anchor case（吞吐/延迟的相对变化）与 offered-load 维度；
    // 此处先给出与绝对值阈值的可扩展骨架，回填真实阈值后补充相对变化计算。
    // 由于 `all_frozen()` 已为真，下方 `frozen_*` 都返回 `Some`，比较的是真实阈值。
    if let Some(limit) = frozen_f64(thresholds::NORMAL_LOAD_MAX_ERROR_RATE)
        && o.error_rate > limit
    {
        return VerifyStatus::Fail(format!(
            "{}: error_rate {} > NORMAL_LOAD_MAX_ERROR_RATE {}",
            o.case_id, o.error_rate, limit
        ));
    }
    if let Some(limit) = frozen_usize(thresholds::MAX_QUEUED_PLUS_RUNNING)
        && o.queued_plus_running_max > limit
    {
        return VerifyStatus::Fail(format!(
            "{}: queued_plus_running_max {} > MAX_QUEUED_PLUS_RUNNING {}",
            o.case_id, o.queued_plus_running_max, limit
        ));
    }
    if let (Some(limit), Some(v)) = (
        frozen_f64(thresholds::OVERLOAD_120PCT_MAX_ERROR_RATE),
        o.overload_120pct_error_rate,
    ) && v > limit
    {
        return VerifyStatus::Fail(format!(
            "{}: overload_120pct_error_rate {} > OVERLOAD_120PCT_MAX_ERROR_RATE {}",
            o.case_id, v, limit
        ));
    }
    if let (Some(limit), Some(v)) = (
        frozen_u64(thresholds::STORAGE_GATE_PAUSE_MAX_MS),
        o.storage_gate_pause_ms,
    ) && v > limit
    {
        return VerifyStatus::Fail(format!(
            "{}: storage_gate_pause_ms {} > STORAGE_GATE_PAUSE_MAX_MS {}",
            o.case_id, v, limit
        ));
    }
    if let (Some(limit), Some(v)) = (frozen_u64(thresholds::SHUTDOWN_DRAIN_MAX_MS), o.shutdown_drain_ms)
        && v > limit
    {
        return VerifyStatus::Fail(format!(
            "{}: shutdown_drain_ms {} > SHUTDOWN_DRAIN_MAX_MS {}",
            o.case_id, v, limit
        ));
    }
    VerifyStatus::Publishable
}

/// 从文件读取 [`Outcome`] 并验证。
pub fn verify_outcome_path(path: &Path) -> anyhow::Result<VerifyStatus> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read outcome file {}", path.display()))?;
    let outcome: Outcome =
        serde_json::from_str(&raw).with_context(|| format!("parse outcome json {}", path.display()))?;
    Ok(verify_outcome(&outcome))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Outcome {
        Outcome {
            case_id: "smoke-get-c1-p1-batch-off".to_string(),
            qps: 120_000.0,
            p99_ms: 3.2,
            max_latency_ms: 9.5,
            error_rate: 0.0,
            overload_120pct_error_rate: Some(0.0),
            queued_plus_running_max: 64,
            storage_gate_pause_ms: Some(5),
            storage_gate_resume_drain_ms: Some(8),
            shutdown_drain_ms: Some(12),
            slow_storage_new_conn_ms: Some(20),
            slow_storage_ping_ms: Some(2),
            cpu_change_ratio: Some(0.01),
            rss_change_ratio: Some(0.02),
            threads_delta: Some(0),
        }
    }

    #[test]
    fn placeholder_thresholds_make_every_case_non_publishable() -> anyhow::Result<()> {
        // 硬保证：在阈值回填前，任何真实数值都不得被判 Publishable。
        let status = verify_outcome(&sample());
        assert_eq!(
            status,
            VerifyStatus::NonPublishable,
            "占位阈值必须判 NonPublishable"
        );
        Ok(())
    }

    #[test]
    fn verify_outcome_path_rejects_missing_file() -> anyhow::Result<()> {
        let p = Path::new("/nonexistent/outcome.json");
        let res = verify_outcome_path(p);
        assert!(res.is_err(), "缺失文件应返回错误");
        Ok(())
    }
}
