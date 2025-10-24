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

use std::cmp::Eq;
use std::cmp::PartialEq;
use std::hash::{Hash, Hasher};
use std::{collections::VecDeque, time::Duration, time::Instant};

use crate::base_value_format::DataType;

type StatisticsCallback = Box<dyn FnOnce(DataType, String, Duration) + Send>;

#[derive(Debug, Clone)]
pub struct KeyStatistics {
    window_size: usize,
    durations: VecDeque<u64>,
    modify_count: u64,
}

impl PartialEq for KeyStatistics {
    fn eq(&self, other: &Self) -> bool {
        self.window_size == other.window_size
            && self.durations == other.durations
            && self.modify_count == other.modify_count
    }
}

impl Eq for KeyStatistics {}

impl Hash for KeyStatistics {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.window_size.hash(state);
        for d in &self.durations {
            d.hash(state);
        }
        self.modify_count.hash(state);
    }
}

impl KeyStatistics {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size: window_size + 2,
            durations: VecDeque::new(),
            modify_count: 0,
        }
    }

    pub fn add_duration(&mut self, duration: u64) {
        self.durations.push_back(duration);
        while self.durations.len() > self.window_size {
            self.durations.pop_front();
        }
    }

    pub fn avg_duration(&self) -> u64 {
        if self.durations.len() < self.window_size {
            return 0;
        }

        if self.durations.is_empty() {
            return 0;
        }

        let mut min = self.durations[0];
        let mut max = self.durations[0];
        let mut sum = 0u64;

        for &duration in &self.durations {
            if duration < min {
                min = duration;
            }
            if duration > max {
                max = duration;
            }
            sum += duration;
        }

        if self.durations.len() <= 2 {
            return 0;
        }

        (sum - max - min) / (self.durations.len() - 2) as u64
    }

    pub fn add_modify_count(&mut self, count: u64) {
        self.modify_count += count;
    }

    pub fn modify_count(&self) -> u64 {
        self.modify_count
    }
}

impl Default for KeyStatistics {
    fn default() -> Self {
        Self::new(10)
    }
}

#[allow(dead_code)]
pub struct KeyStatisticsDurationGuard {
    dtype: DataType,
    key: String,
    start_time: std::time::Instant,
    callback: Option<StatisticsCallback>,
}

#[allow(dead_code)]
impl KeyStatisticsDurationGuard {
    pub fn new<F>(dtype: DataType, key: String, callback: F) -> Self
    where
        F: FnOnce(DataType, String, Duration) + Send + 'static,
    {
        Self {
            callback: Some(Box::new(callback)),
            key,
            start_time: Instant::now(),
            dtype,
        }
    }
}

impl Drop for KeyStatisticsDurationGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        if let Some(callback) = self.callback.take() {
            callback(self.dtype, self.key.clone(), duration);
        }
    }
}
