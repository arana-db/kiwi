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

// gRPC 服务模块 - 模块化实现
//
// 该模块包含所有 gRPC 服务的实现，按服务类型分为三个子模块：
// - core: RaftCoreService (Vote, AppendEntries, StreamAppend, InstallSnapshot)
// - admin: RaftAdminService (Initialize, AddLearner, ChangeMembership, RemoveNode)
// - client: RaftClientService + RaftMetricsService (Write, Read, Metrics, Leader, Members)

pub mod core;
pub mod admin;
pub mod client;

// 导出服务创建器，便于 main.rs 使用
pub use core::create_core_service;
pub use admin::create_admin_service;
pub use client::{create_client_service, create_metrics_service};
