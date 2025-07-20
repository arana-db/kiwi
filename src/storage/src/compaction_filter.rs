/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



use rocksdb::compaction_filter::{CompactionFilter, Decision};
use std::ffi::CStr;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage::base_key_format::ParsedBaseKey;

struct MetaCompactionFilter {}

impl CompactionFilter for MetaCompactionFilter {
    fn filter(&mut self, level: u32, key: &[u8], value: &[u8]) -> Decision {
        let cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let parsed = ParsedBaseKey::new(key);
    }

    fn name(&self) -> &CStr {}
}