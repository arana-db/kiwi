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

//! Async RESP parser for dual runtime architecture
//! 
//! This module provides an enhanced RESP parser that supports async storage
//! operations, request pipelining, and improved error handling.

use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use log::{debug, warn};
use resp::{Parse, RespData, RespParseResult, RespVersion, RespParse};

/// Enhanced RESP parser for async storage operations
pub struct AsyncRespParser {
    /// Underlying RESP parser
    parser: RespParse,
    /// Buffer for incomplete data
    buffer: BytesMut,
    /// Queue of parsed commands ready for processing
    command_queue: VecDeque<RespData>,
    /// Whether we're in pipelining mode
    pipelining_mode: bool,
    /// Maximum commands to buffer for pipelining
    max_pipeline_commands: usize,
}

impl AsyncRespParser {
    /// Create a new async RESP parser
    pub fn new(version: RespVersion) -> Self {
        Self {
            parser: RespParse::new(version),
            buffer: BytesMut::new(),
            command_queue: VecDeque::new(),
            pipelining_mode: false,
            max_pipeline_commands: 100,
        }
    }

    /// Create a new parser with custom pipeline settings
    pub fn with_pipeline_config(version: RespVersion, max_pipeline_commands: usize) -> Self {
        Self {
            parser: RespParse::new(version),
            buffer: BytesMut::new(),
            command_queue: VecDeque::new(),
            pipelining_mode: false,
            max_pipeline_commands,
        }
    }

    /// Parse incoming data and return available commands
    pub fn parse_data(&mut self, data: Bytes) -> Result<Vec<RespData>, String> {
        let mut commands = Vec::new();
        
        // Add new data to buffer
        self.buffer.extend_from_slice(&data);
        
        // Parse as many complete commands as possible
        loop {
            let parse_result = self.parser.parse(self.buffer.split().freeze());
            
            match parse_result {
                RespParseResult::Complete(command_data) => {
                    debug!("Parsed complete command: {:?}", command_data);
                    commands.push(command_data);
                    
                    // Enable pipelining mode if we have multiple commands
                    if commands.len() > 1 {
                        self.pipelining_mode = true;
                    }
                    
                    // Prevent excessive buffering
                    if commands.len() >= self.max_pipeline_commands {
                        warn!("Maximum pipeline commands reached, processing batch");
                        break;
                    }
                }
                RespParseResult::Incomplete => {
                    debug!("Incomplete RESP data, need more bytes");
                    break;
                }
                RespParseResult::Error(e) => {
                    return Err(format!("RESP parsing error: {:?}", e));
                }
            }
        }
        
        Ok(commands)
    }

    /// Check if parser is in pipelining mode
    pub fn is_pipelining(&self) -> bool {
        self.pipelining_mode
    }

    /// Get the current protocol version
    pub fn version(&self) -> RespVersion {
        self.parser.version()
    }

    /// Reset the parser state
    pub fn reset(&mut self) {
        self.parser.reset();
        self.buffer.clear();
        self.command_queue.clear();
        self.pipelining_mode = false;
    }

    /// Get the number of buffered bytes
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Check if we should flush the current batch
    pub fn should_flush(&self) -> bool {
        !self.command_queue.is_empty() || self.buffered_bytes() == 0
    }
}

/// Command batch for efficient processing
#[derive(Debug, Clone)]
pub struct CommandBatch {
    /// Commands in this batch
    pub commands: Vec<RespData>,
    /// Whether this batch supports parallel execution
    pub parallel_safe: bool,
    /// Priority level for this batch
    pub priority: BatchPriority,
}

/// Priority levels for command batches
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BatchPriority {
    /// Low priority background operations
    Low,
    /// Normal priority operations
    Normal,
    /// High priority operations
    High,
    /// Critical operations that must be processed immediately
    Critical,
}

impl CommandBatch {
    /// Create a new command batch
    pub fn new(commands: Vec<RespData>) -> Self {
        let parallel_safe = Self::check_parallel_safety(&commands);
        let priority = Self::determine_priority(&commands);
        
        Self {
            commands,
            parallel_safe,
            priority,
        }
    }

    /// Check if commands in batch can be executed in parallel
    fn check_parallel_safety(commands: &[RespData]) -> bool {
        // For now, assume all commands need sequential execution
        // TODO: Implement read-only command detection for parallel execution
        commands.len() == 1
    }

    /// Determine the priority of the batch based on commands
    fn determine_priority(commands: &[RespData]) -> BatchPriority {
        let mut max_priority = BatchPriority::Normal;
        
        for command in commands {
            if let RespData::Array(Some(params)) = command {
                if let Some(RespData::BulkString(Some(cmd_name))) = params.first() {
                    let cmd_str = String::from_utf8_lossy(cmd_name).to_lowercase();
                    let cmd_priority = match cmd_str.as_str() {
                        // Critical commands
                        "auth" | "hello" | "quit" => BatchPriority::Critical,
                        // High priority commands
                        "ping" | "echo" => BatchPriority::High,
                        // Low priority commands
                        "info" | "keys" | "scan" => BatchPriority::Low,
                        // Normal priority for everything else
                        _ => BatchPriority::Normal,
                    };
                    
                    if cmd_priority > max_priority {
                        max_priority = cmd_priority;
                    }
                }
            }
        }
        
        max_priority
    }

    /// Get the number of commands in this batch
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resp::RespVersion;

    #[test]
    fn test_async_resp_parser_creation() {
        let parser = AsyncRespParser::new(RespVersion::RESP2);
        assert_eq!(parser.version(), RespVersion::RESP2);
        assert!(!parser.is_pipelining());
        assert_eq!(parser.buffered_bytes(), 0);
    }

    #[test]
    fn test_command_batch_priority() {
        let ping_cmd = RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from("PING"))),
        ]));
        
        let batch = CommandBatch::new(vec![ping_cmd]);
        assert_eq!(batch.priority, BatchPriority::High);
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_command_batch_parallel_safety() {
        let get_cmd = RespData::Array(Some(vec![
            RespData::BulkString(Some(Bytes::from("GET"))),
            RespData::BulkString(Some(Bytes::from("key1"))),
        ]));
        
        let batch = CommandBatch::new(vec![get_cmd]);
        assert!(batch.parallel_safe); // Single command is parallel safe
    }
}