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

//! Integration tests for network layer with Raft awareness

use net::raft_network_handle::{is_read_command, is_write_command, ClusterMode};

#[test]
fn test_command_classification_integration() {
    // Test write commands
    let write_commands = vec![
        "SET", "DEL", "INCR", "DECR", "LPUSH", "RPUSH", 
        "SADD", "ZADD", "HSET", "EXPIRE", "MSET"
    ];
    
    for cmd in write_commands {
        assert!(is_write_command(cmd), "Command {} should be classified as write", cmd);
        assert!(!is_read_command(cmd), "Command {} should not be classified as read", cmd);
    }
    
    // Test read commands
    let read_commands = vec![
        "GET", "MGET", "EXISTS", "TTL", "LRANGE", "LLEN",
        "SMEMBERS", "ZRANGE", "HGET", "HGETALL", "PING"
    ];
    
    for cmd in read_commands {
        assert!(is_read_command(cmd), "Command {} should be classified as read", cmd);
        assert!(!is_write_command(cmd), "Command {} should not be classified as write", cmd);
    }
}

#[test]
fn test_cluster_mode_values() {
    let single = ClusterMode::Single;
    let cluster = ClusterMode::Cluster;
    
    assert_ne!(single, cluster);
    assert_eq!(single, ClusterMode::Single);
    assert_eq!(cluster, ClusterMode::Cluster);
}

#[test]
fn test_case_insensitive_command_classification() {
    // Test that command classification is case-insensitive
    assert!(is_write_command("SET"));
    assert!(is_write_command("set"));
    assert!(is_write_command("Set"));
    assert!(is_write_command("SeT"));
    
    assert!(is_read_command("GET"));
    assert!(is_read_command("get"));
    assert!(is_read_command("Get"));
    assert!(is_read_command("GeT"));
}

#[test]
fn test_comprehensive_redis_commands() {
    // String commands
    assert!(is_write_command("APPEND"));
    assert!(is_write_command("SETRANGE"));
    assert!(is_read_command("STRLEN"));
    assert!(is_read_command("GETRANGE"));
    
    // List commands
    assert!(is_write_command("LPOP"));
    assert!(is_write_command("RPOP"));
    assert!(is_write_command("LSET"));
    assert!(is_read_command("LINDEX"));
    
    // Set commands
    assert!(is_write_command("SREM"));
    assert!(is_write_command("SPOP"));
    assert!(is_read_command("SCARD"));
    assert!(is_read_command("SISMEMBER"));
    
    // Sorted set commands
    assert!(is_write_command("ZREM"));
    assert!(is_write_command("ZINCRBY"));
    assert!(is_read_command("ZSCORE"));
    assert!(is_read_command("ZRANK"));
    
    // Hash commands
    assert!(is_write_command("HDEL"));
    assert!(is_write_command("HINCRBY"));
    assert!(is_read_command("HEXISTS"));
    assert!(is_read_command("HLEN"));
    
    // Key commands
    assert!(is_write_command("EXPIRE"));
    assert!(is_write_command("PERSIST"));
    assert!(is_read_command("TYPE"));
    assert!(is_read_command("PTTL"));
}
