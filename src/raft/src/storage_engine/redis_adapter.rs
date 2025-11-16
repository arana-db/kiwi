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

//! Adapter to connect storage::Redis to RedisStorage trait

use super::redis_storage_engine::RedisStorage;
use std::sync::Arc;

/// Adapter that implements RedisStorage trait for storage::Redis
///
/// This uses dynamic dispatch to avoid direct type dependency on storage::Redis
pub struct RedisStorageAdapter {
    // Store as trait object to avoid direct dependency
    get_fn: Box<
        dyn Fn(&[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
    >,
    set_fn: Box<
        dyn Fn(&[u8], &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
    >,
    del_fn: Box<
        dyn Fn(&[&[u8]]) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
    >,
    mset_fn: Box<
        dyn Fn(&[(Vec<u8>, Vec<u8>)]) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync,
    >,
}

impl RedisStorageAdapter {
    /// Create a new adapter with function closures
    ///
    /// This allows us to avoid direct dependency on storage::Redis type
    pub fn new<R>(redis: Arc<R>) -> Self
    where
        R: 'static + Send + Sync,
        R: RedisOperations,
    {
        let redis_get = redis.clone();
        let redis_set = redis.clone();
        let redis_del = redis.clone();
        let redis_mset = redis;

        Self {
            get_fn: Box::new(move |key| redis_get.raft_get_binary(key)),
            set_fn: Box::new(move |key, value| redis_set.raft_set(key, value)),
            del_fn: Box::new(move |keys| redis_del.raft_del(keys)),
            mset_fn: Box::new(move |pairs| redis_mset.raft_mset(pairs)),
        }
    }
}

/// Trait for Redis operations to avoid direct dependency
///
/// Note: Method names are prefixed with `raft_` to avoid conflicts with
/// Redis's inherent methods (e.g., `raft_del` instead of `del`).
/// This is necessary because Rust's method resolution prioritizes trait
/// methods over inherent methods when implementing a trait.
pub trait RedisOperations: Send + Sync {
    fn raft_get_binary(
        &self,
        key: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
    fn raft_set(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn raft_del(&self, keys: &[&[u8]]) -> Result<i32, Box<dyn std::error::Error + Send + Sync>>;
    fn raft_mset(
        &self,
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

impl RedisStorage for RedisStorageAdapter {
    fn get_binary(&self, key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        (self.get_fn)(key)
    }

    fn set(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        (self.set_fn)(key, value)
    }

    fn del(&self, keys: &[&[u8]]) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
        (self.del_fn)(keys)
    }

    fn mset(
        &self,
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        (self.mset_fn)(pairs)
    }
}
