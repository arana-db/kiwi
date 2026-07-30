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
//! 按版本化的 `cases.yaml` 跑完整测量矩阵（每 case 重复 5 次、CV<=5%）后回填。
//!
//! **严禁在本文件硬编码猜测值。** 设计规格明确禁止用 Windows / GitHub hosted
//! runner 的数字冻结阈值——本仓库的 CI 也只应在固定 Linux 环境执行完整矩阵。
//! 任一常量仍处于占位哨兵值（`f64::NAN` / `usize::MAX` / `u64::MAX` / `i64::MAX`）
//! 时，`all_frozen()` 返回 false，verifier 会据此判该 case 为 `NonPublishable`，
//! 从而杜绝「用占位值假装通过」。

#![allow(dead_code)]

/// 正常负载吞吐允许回退比例（相对 anchor case，例如 0.10 = 允许回退 10%）。
pub const NORMAL_LOAD_THROUGHPUT_MAX_REGRESSION_RATIO: f64 = f64::NAN; // TODO(#351): 固定 Linux 实测回填

/// P99 与 max latency 允许变化（相对 anchor，比例）。
pub const P99_MAX_LATENCY_ALLOWED_CHANGE_RATIO: f64 = f64::NAN; // TODO(#351)
pub const MAX_LATENCY_ALLOWED_CHANGE_RATIO: f64 = f64::NAN; // TODO(#351)

/// 正常负载错误率上限（小数，例如 0.001 = 0.1%）。
pub const NORMAL_LOAD_MAX_ERROR_RATE: f64 = f64::NAN; // TODO(#351)

/// 120% offered load 下允许的错误率上限与允许的错误类别。
pub const OVERLOAD_120PCT_MAX_ERROR_RATE: f64 = f64::NAN; // TODO(#351)
pub const OVERLOAD_120PCT_ALLOWED_ERROR_KINDS: &[&str] = &["command_error"]; // TODO(#351): 实测确认

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

/// 所有阈值均已回填（非占位）才返回 true；任一仍为占位哨兵值则 false。
///
/// verifier 依赖此函数判断「当前是否具备可发布的真实基线」——只要还有占位值，
/// 任何 case 都应判为 `NonPublishable`，防止用哨兵值假装通过。
pub fn all_frozen() -> bool {
    MAX_QUEUED_PLUS_RUNNING != usize::MAX
        && STORAGE_GATE_PAUSE_MAX_MS != u64::MAX
        && STORAGE_GATE_RESUME_DRAIN_MAX_MS != u64::MAX
        && SHUTDOWN_DRAIN_MAX_MS != u64::MAX
        && SLOW_STORAGE_NEW_CONN_MAX_MS != u64::MAX
        && SLOW_STORAGE_PING_MAX_MS != u64::MAX
        && THREADS_ALLOWED_DELTA != i64::MAX
        && !NORMAL_LOAD_THROUGHPUT_MAX_REGRESSION_RATIO.is_nan()
        && !P99_MAX_LATENCY_ALLOWED_CHANGE_RATIO.is_nan()
        && !MAX_LATENCY_ALLOWED_CHANGE_RATIO.is_nan()
        && !NORMAL_LOAD_MAX_ERROR_RATE.is_nan()
        && !OVERLOAD_120PCT_MAX_ERROR_RATE.is_nan()
        && !CPU_ALLOWED_CHANGE_RATIO.is_nan()
        && !RSS_ALLOWED_CHANGE_RATIO.is_nan()
}
