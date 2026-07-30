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

//! 冻结基线阈值（#351 量化验收）。
//!
//! 本模块当前为「方法论冻结 + 占位」。真实数值只能在固定 Linux/WSL 主机上，
//! 按版本化的 `cases.yaml` 跑完整测量矩阵（性能 case 重复 5 次、lifecycle case 重复
//! 3 次、CV<=5%）后回填。
//!
//! **严禁在本文件硬编码猜测值。** 设计规格明确禁止用 Windows / GitHub hosted
//! runner 的数字冻结阈值——完整矩阵未来也只能在固定 Linux 环境执行。
//! 任一常量仍处于占位哨兵值（`f64::NAN` / `usize::MAX` / `u64::MAX` / `i64::MAX`）
//! 时，`all_frozen()` 返回 false，checker 会据此返回 `PolicyUnavailable`，从而杜绝
//! 「用占位值假装通过」。

use std::collections::BTreeSet;

/// 正常负载吞吐允许回退比例（相对 anchor case，例如 0.10 = 允许回退 10%）。
pub const NORMAL_LOAD_THROUGHPUT_MAX_REGRESSION_RATIO: f64 = f64::NAN; // TODO(#351): 固定 Linux 实测回填

/// P99 与 max latency 允许变化（相对 anchor，比例）。
pub const P99_MAX_LATENCY_ALLOWED_CHANGE_RATIO: f64 = f64::NAN; // TODO(#351)
pub const MAX_LATENCY_ALLOWED_CHANGE_RATIO: f64 = f64::NAN; // TODO(#351)

/// 正常负载错误率上限（小数，例如 0.001 = 0.1%）。
pub const NORMAL_LOAD_MAX_ERROR_RATE: f64 = f64::NAN; // TODO(#351)

/// 120% offered load 下允许的错误率上限与允许的错误类别。
pub const OVERLOAD_120PCT_MAX_ERROR_RATE: f64 = f64::NAN; // TODO(#351)
pub const OVERLOAD_120PCT_ALLOWED_ERROR_KINDS: &[&str] = &[]; // TODO(#351): 实测确认

/// queued + running 最大允许值（“最大 pending 阈值”，与 HealthThresholds.max_pending_requests 不同）。
pub const MAX_QUEUED_PLUS_RUNNING: usize = usize::MAX; // TODO(#351): 固定 Linux 实测回填

/// storage-gate pause/drain 与 shutdown 最大耗时（毫秒）。
pub const STORAGE_GATE_PAUSE_MAX_MS: u64 = u64::MAX; // TODO(#351)
pub const STORAGE_GATE_RESUME_DRAIN_MAX_MS: u64 = u64::MAX; // TODO(#351)
pub const SHUTDOWN_DRAIN_MAX_MS: u64 = u64::MAX; // TODO(#351)

/// 慢存储时新连接建立与 PING 的最大延迟（毫秒）。
pub const SLOW_STORAGE_NEW_CONN_MAX_MS: u64 = u64::MAX; // TODO(#351)
pub const SLOW_STORAGE_PING_MAX_MS: u64 = u64::MAX; // TODO(#351)

/// 资源允许变化（绝对值或比例，按实测约定）。
pub const CPU_ALLOWED_CHANGE_RATIO: f64 = f64::NAN; // TODO(#351)
pub const RSS_ALLOWED_CHANGE_RATIO: f64 = f64::NAN; // TODO(#351)
pub const THREADS_ALLOWED_DELTA: i64 = i64::MAX; // TODO(#351)

/// 一组已经冻结且通过范围校验的单 case 阈值。
#[derive(Debug, Clone, PartialEq)]
pub struct ThresholdPolicy {
    pub normal_load_throughput_max_regression_ratio: f64,
    pub p99_max_latency_allowed_change_ratio: f64,
    pub max_latency_allowed_change_ratio: f64,
    pub normal_load_max_error_rate: f64,
    pub overload_120pct_max_error_rate: f64,
    pub overload_120pct_allowed_error_kinds: Vec<String>,
    pub max_queued_plus_running: usize,
    pub storage_gate_pause_max_ms: u64,
    pub storage_gate_resume_drain_max_ms: u64,
    pub shutdown_drain_max_ms: u64,
    pub slow_storage_new_conn_max_ms: u64,
    pub slow_storage_ping_max_ms: u64,
    pub cpu_allowed_change_ratio: f64,
    pub rss_allowed_change_ratio: f64,
    pub threads_allowed_delta: i64,
}

impl ThresholdPolicy {
    /// 从编译期常量构造策略；占位值或无效范围都会阻止策略可用。
    pub fn from_frozen_constants() -> Result<Self, Vec<String>> {
        let policy = Self {
            normal_load_throughput_max_regression_ratio:
                NORMAL_LOAD_THROUGHPUT_MAX_REGRESSION_RATIO,
            p99_max_latency_allowed_change_ratio: P99_MAX_LATENCY_ALLOWED_CHANGE_RATIO,
            max_latency_allowed_change_ratio: MAX_LATENCY_ALLOWED_CHANGE_RATIO,
            normal_load_max_error_rate: NORMAL_LOAD_MAX_ERROR_RATE,
            overload_120pct_max_error_rate: OVERLOAD_120PCT_MAX_ERROR_RATE,
            overload_120pct_allowed_error_kinds: OVERLOAD_120PCT_ALLOWED_ERROR_KINDS
                .iter()
                .map(|kind| (*kind).to_string())
                .collect(),
            max_queued_plus_running: MAX_QUEUED_PLUS_RUNNING,
            storage_gate_pause_max_ms: STORAGE_GATE_PAUSE_MAX_MS,
            storage_gate_resume_drain_max_ms: STORAGE_GATE_RESUME_DRAIN_MAX_MS,
            shutdown_drain_max_ms: SHUTDOWN_DRAIN_MAX_MS,
            slow_storage_new_conn_max_ms: SLOW_STORAGE_NEW_CONN_MAX_MS,
            slow_storage_ping_max_ms: SLOW_STORAGE_PING_MAX_MS,
            cpu_allowed_change_ratio: CPU_ALLOWED_CHANGE_RATIO,
            rss_allowed_change_ratio: RSS_ALLOWED_CHANGE_RATIO,
            threads_allowed_delta: THREADS_ALLOWED_DELTA,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// 拒绝哨兵、非有限数、负阈值、非法错误率和未冻结的错误类别。
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut reasons = Vec::new();
        validate_ratio(
            "normal_load_throughput_max_regression_ratio",
            self.normal_load_throughput_max_regression_ratio,
            Some(1.0),
            &mut reasons,
        );
        validate_ratio(
            "p99_max_latency_allowed_change_ratio",
            self.p99_max_latency_allowed_change_ratio,
            None,
            &mut reasons,
        );
        validate_ratio(
            "max_latency_allowed_change_ratio",
            self.max_latency_allowed_change_ratio,
            None,
            &mut reasons,
        );
        validate_ratio(
            "normal_load_max_error_rate",
            self.normal_load_max_error_rate,
            Some(1.0),
            &mut reasons,
        );
        validate_ratio(
            "overload_120pct_max_error_rate",
            self.overload_120pct_max_error_rate,
            Some(1.0),
            &mut reasons,
        );
        validate_ratio(
            "cpu_allowed_change_ratio",
            self.cpu_allowed_change_ratio,
            None,
            &mut reasons,
        );
        validate_ratio(
            "rss_allowed_change_ratio",
            self.rss_allowed_change_ratio,
            None,
            &mut reasons,
        );

        if self.max_queued_plus_running == usize::MAX {
            reasons.push("max_queued_plus_running is still a placeholder".to_string());
        }
        for (name, value) in [
            ("storage_gate_pause_max_ms", self.storage_gate_pause_max_ms),
            (
                "storage_gate_resume_drain_max_ms",
                self.storage_gate_resume_drain_max_ms,
            ),
            ("shutdown_drain_max_ms", self.shutdown_drain_max_ms),
            (
                "slow_storage_new_conn_max_ms",
                self.slow_storage_new_conn_max_ms,
            ),
            ("slow_storage_ping_max_ms", self.slow_storage_ping_max_ms),
        ] {
            if value == u64::MAX {
                reasons.push(format!("{name} is still a placeholder"));
            }
        }
        if self.threads_allowed_delta < 0 || self.threads_allowed_delta == i64::MAX {
            reasons.push("threads_allowed_delta must be frozen and non-negative".to_string());
        }

        if self.overload_120pct_allowed_error_kinds.is_empty() {
            reasons.push("overload_120pct_allowed_error_kinds is not frozen".to_string());
        } else {
            let mut unique = BTreeSet::new();
            for kind in &self.overload_120pct_allowed_error_kinds {
                let normalized = kind.trim();
                if normalized.is_empty() {
                    reasons
                        .push("overload error kinds must not contain an empty value".to_string());
                } else if normalized != kind {
                    reasons.push(
                        "overload error kinds must not contain leading or trailing whitespace"
                            .to_string(),
                    );
                } else if !unique.insert(normalized) {
                    reasons.push(format!("duplicate overload error kind: {normalized}"));
                }
            }
        }

        if reasons.is_empty() {
            Ok(())
        } else {
            Err(reasons)
        }
    }
}

fn validate_ratio(name: &str, value: f64, maximum: Option<f64>, reasons: &mut Vec<String>) {
    if !value.is_finite() || value < 0.0 {
        reasons.push(format!("{name} must be finite and non-negative"));
    } else if let Some(limit) = maximum.filter(|limit| value > *limit) {
        reasons.push(format!("{name} must not exceed {limit}"));
    }
}

/// 所有阈值均已回填（非占位）才返回 true；任一仍为占位哨兵值则 false。
///
/// checker 依赖此函数判断「当前是否具备可比较的真实阈值」；只要还有占位值，
/// 策略就不可用，防止用哨兵值假装通过。
pub fn all_frozen() -> bool {
    ThresholdPolicy::from_frozen_constants().is_ok()
}
