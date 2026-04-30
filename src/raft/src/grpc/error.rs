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

//! gRPC 统一错误处理模块
//!
//! 提供 gRPC 层的统一错误类型定义，便于错误追踪和日志记录。

use thiserror::Error;

/// gRPC 层统一错误类型
#[derive(Debug, Error)]
pub enum GrpcError {
    /// 连接失败
    #[error("connection failed: {0}")]
    ConnectionFailed(#[from] tonic::transport::Error),

    /// 连接超时
    #[error("connection timeout: {0}")]
    Timeout(String),

    /// RPC 调用失败
    #[error("RPC failed: {0}")]
    RpcFailed(String),

    /// 服务端返回错误
    #[error("server error: {0}")]
    ServerError(String),

    /// 请求参数无效
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// 序列化/反序列化错误
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// 节点不可用
    #[error("node unavailable: {0}")]
    NodeUnavailable(String),

    /// 未实现的功能
    #[error("not implemented: {0}")]
    NotImplemented(String),
}

impl GrpcError {
    /// 将 GrpcError 转换为 tonic::Status
    pub fn to_status(&self) -> tonic::Status {
        use tonic::Status;
        match self {
            GrpcError::ConnectionFailed(e) => {
                Status::unavailable(format!("connection failed: {}", e))
            }
            GrpcError::Timeout(e) => Status::deadline_exceeded(format!("timeout: {}", e)),
            GrpcError::RpcFailed(e) => Status::internal(format!("RPC failed: {}", e)),
            GrpcError::ServerError(e) => Status::unknown(format!("server error: {}", e)),
            GrpcError::InvalidArgument(e) => Status::invalid_argument(e),
            GrpcError::SerializationError(e) => {
                Status::invalid_argument(format!("serialization error: {}", e))
            }
            GrpcError::NodeUnavailable(e) => Status::unavailable(e),
            GrpcError::NotImplemented(e) => Status::unimplemented(e),
        }
    }
}

/// gRPC 客户端错误类型（用于客户端调用）
#[derive(Debug, Error)]
pub enum GrpcClientError {
    /// 连接失败
    #[error("failed to connect to {target}: {source}")]
    Connect {
        /// 目标地址
        target: String,
        /// 错误原因
        #[source]
        source: tonic::transport::Error,
    },

    /// RPC 调用超时
    #[error("RPC timeout calling {target}: {message}")]
    Timeout {
        /// 目标地址
        target: String,
        /// 超时信息
        message: String,
    },

    /// RPC 调用失败
    #[error("RPC failed calling {target}: {message}")]
    RpcFailed {
        /// 目标地址
        target: String,
        /// 错误信息
        message: String,
    },

    /// 服务端返回错误
    #[error("server error from {target}: {code} - {message}")]
    ServerError {
        /// 目标地址
        target: String,
        /// gRPC 状态码
        code: i32,
        /// 错误信息
        message: String,
    },
}

impl GrpcClientError {
    /// 从 tonic::Status 创建客户端错误
    pub fn from_status(target: String, status: &tonic::Status) -> Self {
        let code = status.code() as i32;
        let message = status.message().to_string();
        GrpcClientError::ServerError {
            target,
            code,
            message,
        }
    }
}
