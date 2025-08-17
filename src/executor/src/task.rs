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

use tokio::sync::oneshot;
use resp::{RespData, RespError};

/// A task that represents a command execution
pub struct CmdExecutionTask {
    /// Receiver for the task result
    result: oneshot::Receiver<Result<RespData, RespError>>,
}

impl CmdExecutionTask {
    /// Creates a new command execution task
    pub fn new(result: oneshot::Receiver<Result<RespData, RespError>>) -> Self {
        Self { result }
    }

    /// Waits for the task to complete and returns the result
    pub async fn await_result(self) -> Result<RespData, RespError> {
        self.result
            .await
            .map_err(|_| RespError::UnknownError("Task channel closed".to_string()))?
    }

    /// Checks if the task has completed
    pub fn is_completed(&self) -> bool {
        self.result.is_closed()
    }
}
