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

//! RedisForRaft wrapper type
//!
//! This module defines the RedisForRaft wrapper WITHOUT implementing any traits.
//! The trait implementation is in a separate module to avoid method resolution conflicts.

use crate::Redis;
use std::sync::Arc;

/// Wrapper struct that provides Redis operations for Raft integration
/// 
/// This wrapper exposes Redis methods with `raft_` prefix to avoid
/// conflicts with the RedisOperations trait methods.
pub struct RedisForRaft {
    redis: Arc<Redis>,
}

impl RedisForRaft {
    pub fn new(redis: Arc<Redis>) -> Self {
        Self { redis }
    }
    
    pub fn inner(&self) -> &Arc<Redis> {
        &self.redis
    }
    
    /// Get binary value for a key
    pub fn raft_get_binary(&self, key: &[u8]) -> crate::Result<Vec<u8>> {
        self.redis.as_ref().get_binary(key)
    }
    
    /// Set a key-value pair
    pub fn raft_set(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        self.redis.as_ref().set(key, value)
    }
    
    /// Delete multiple keys
    pub fn raft_del(&self, keys: &[&[u8]]) -> crate::Result<i32> {
        let mut deleted = 0;
        for key in keys {
            if self.redis.as_ref().del_key(key)? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }
    
    /// Set multiple key-value pairs
    pub fn raft_mset(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> crate::Result<()> {
        self.redis.as_ref().mset(pairs)
    }
}
