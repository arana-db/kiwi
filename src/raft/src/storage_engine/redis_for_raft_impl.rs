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

//! RedisOperations trait implementation for RedisForRaft
//!
//! This implementation is in the raft crate to avoid method name resolution
//! conflicts that would occur if it were in the storage crate.

// This file is intentionally empty for now.
// The RedisOperations trait will be implemented using the RedisStorageAdapter
// which uses function pointers to avoid method name conflicts.
//
// See src/raft/src/storage_engine/redis_adapter.rs for the actual implementation.
