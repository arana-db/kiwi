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

//! Counters for FLAT vector query execution, surfaced through INFO VECTOR.
//!
//! One `VectorMetrics` lives on each `Redis` instance next to its FLAT query
//! gate; `Storage::vector_metrics` aggregates the per-instance snapshots.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::Error;

#[derive(Debug, Default)]
pub struct VectorMetrics {
    flat_queries_total: AtomicU64,
    flat_query_timeouts_total: AtomicU64,
    flat_query_errors_total: AtomicU64,
    capacity_rejected_total: AtomicU64,
    flat_query_duration_micros_total: AtomicU64,
    flat_query_duration_count: AtomicU64,
}

/// Point-in-time copy of the vector counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorMetricsSnapshot {
    pub flat_queries_total: u64,
    pub flat_query_timeouts_total: u64,
    pub flat_query_errors_total: u64,
    pub capacity_rejected_total: u64,
    pub flat_query_duration_micros_total: u64,
    pub flat_query_duration_count: u64,
}

impl VectorMetricsSnapshot {
    pub fn add(&mut self, other: &Self) {
        self.flat_queries_total += other.flat_queries_total;
        self.flat_query_timeouts_total += other.flat_query_timeouts_total;
        self.flat_query_errors_total += other.flat_query_errors_total;
        self.capacity_rejected_total += other.capacity_rejected_total;
        self.flat_query_duration_micros_total += other.flat_query_duration_micros_total;
        self.flat_query_duration_count += other.flat_query_duration_count;
    }
}

impl VectorMetrics {
    /// A query passed the gate and entered the scan path.
    pub fn record_query_started(&self) {
        self.flat_queries_total.fetch_add(1, Ordering::Relaxed);
    }

    /// The gate could not hand out a permit before the caller's deadline.
    pub fn record_capacity_rejected(&self) {
        self.capacity_rejected_total.fetch_add(1, Ordering::Relaxed);
    }

    /// A started query finished (successfully or not); `error` classifies
    /// the abort path: scan-deadline timeouts are counted separately from
    /// budget, cancellation and other storage errors.
    pub fn record_query_finished(&self, elapsed: Duration, error: Option<&Error>) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.flat_query_duration_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        self.flat_query_duration_count
            .fetch_add(1, Ordering::Relaxed);
        match error {
            None => {}
            Some(Error::VectorFlatQueryTimeout { .. }) => {
                self.flat_query_timeouts_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(_) => {
                self.flat_query_errors_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> VectorMetricsSnapshot {
        VectorMetricsSnapshot {
            flat_queries_total: self.flat_queries_total.load(Ordering::Relaxed),
            flat_query_timeouts_total: self.flat_query_timeouts_total.load(Ordering::Relaxed),
            flat_query_errors_total: self.flat_query_errors_total.load(Ordering::Relaxed),
            capacity_rejected_total: self.capacity_rejected_total.load(Ordering::Relaxed),
            flat_query_duration_micros_total: self
                .flat_query_duration_micros_total
                .load(Ordering::Relaxed),
            flat_query_duration_count: self.flat_query_duration_count.load(Ordering::Relaxed),
        }
    }
}
