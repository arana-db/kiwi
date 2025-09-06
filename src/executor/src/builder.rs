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

use crate::CmdExecutor;

pub struct CmdExecutorBuilder {
    worker_count: usize,
    channel_size: usize,
}

impl CmdExecutorBuilder {
    pub fn new() -> Self {
        Self {
            worker_count: num_cpus::get(),
            channel_size: 1000,
        }
    }

    pub fn worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    pub fn channel_size(mut self, channel_size: usize) -> Self {
        self.channel_size = channel_size;
        self
    }

    pub fn build(self) -> CmdExecutor {
        CmdExecutor::new(self.worker_count, self.channel_size)
    }
}

impl Default for CmdExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}
