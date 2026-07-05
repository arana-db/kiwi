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
pub mod config;
pub mod de_func;
pub mod error;
pub mod raft_type;
pub mod runtime_config;

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use validator::Validate;

    use super::*;
    use crate::config::Config;

    #[test]
    fn test_config_parsing() {
        use std::io::Write;

        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(config_file, "port 7379").unwrap();
        writeln!(config_file, "memory 10M").unwrap();
        writeln!(config_file, "rocksdb-max-subcompactions 2").unwrap();
        writeln!(config_file, "rocksdb-ttl-second 604800").unwrap();
        writeln!(config_file, "rocksdb-periodic-second 259200").unwrap();

        let config = config::Config::load(config_file.path().to_str().unwrap());
        assert!(
            config.is_ok(),
            "Config loading failed: {:?}",
            config.as_ref().err()
        );

        let config = config.unwrap();

        assert_eq!(7379, config.port);
        assert_eq!(10 * 1024 * 1024, config.memory);

        assert_eq!(2, config.rocksdb_max_subcompactions);
        assert_eq!(4, config.rocksdb_max_background_jobs);
        assert_eq!(2, config.rocksdb_max_write_buffer_number);
        assert_eq!(2, config.rocksdb_min_write_buffer_number_to_merge);
        assert_eq!(67108864, config.rocksdb_write_buffer_size);
        assert_eq!(4, config.rocksdb_level0_file_num_compaction_trigger);
        assert_eq!(7, config.rocksdb_num_levels);
        assert!(!config.rocksdb_enable_pipelined_write);
        assert_eq!(20, config.rocksdb_level0_slowdown_writes_trigger);
        assert_eq!(36, config.rocksdb_level0_stop_writes_trigger);
        assert_eq!(604800, config.rocksdb_ttl_second);
        assert_eq!(259200, config.rocksdb_periodic_second);
        assert!(config.rocksdb_level_compaction_dynamic_level_bytes);
        assert_eq!(10000, config.rocksdb_max_open_files);
        assert_eq!(64 << 20, config.rocksdb_target_file_size_base);

        assert_eq!(5000, config.small_compaction_threshold);
        assert_eq!(10000, config.small_compaction_duration_threshold);

        assert_eq!(50, config.timeout);
        assert_eq!("./kiwi_data/logs", config.log_dir);
        assert_eq!("./kiwi_data/db", config.data_dir);
        assert!(!config.redis_compatible_mode);
        assert_eq!(3, config.db_instance_num);

        assert!(
            config.validate().is_ok(),
            "Config validation failed: {:?}",
            config.validate().err()
        );
    }

    #[test]
    fn test_validate_port_range() {
        let mut invalid_config = Config {
            binding: "127.0.0.1".to_string(),
            port: 999,
            timeout: 100,
            redis_compatible_mode: false,
            log_dir: "".to_string(),
            data_dir: "./kiwi_data/db".to_string(),
            memory: 1024,
            rocksdb_max_subcompactions: 0,
            rocksdb_max_background_jobs: 4,
            rocksdb_max_write_buffer_number: 2,
            rocksdb_min_write_buffer_number_to_merge: 2,
            rocksdb_write_buffer_size: 64 << 20,
            rocksdb_level0_file_num_compaction_trigger: 4,
            rocksdb_num_levels: 7,
            rocksdb_enable_pipelined_write: false,
            rocksdb_level0_slowdown_writes_trigger: 20,
            rocksdb_level0_stop_writes_trigger: 36,
            rocksdb_ttl_second: 30 * 24 * 60 * 60,
            rocksdb_periodic_second: 30 * 24 * 60 * 60,
            rocksdb_level_compaction_dynamic_level_bytes: true,
            rocksdb_max_open_files: 10000,
            rocksdb_target_file_size_base: 64 << 20,
            rocksdb_compression_type: config::CompressionType::Lz4,
            db_instance_num: 3,
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,
            requirepass: None,
            raft: None,
            runtime: runtime_config::RuntimeConfig::default(),
        };
        assert!(invalid_config.validate().is_err());

        invalid_config.port = 8080;
        assert!(invalid_config.validate().is_ok());
    }

    #[test]
    fn test_data_dir_default() {
        let config = Config::default();
        assert_eq!("./kiwi_data/db", config.data_dir);
    }

    #[test]
    fn test_data_dir_from_config_file() {
        use std::io::Write;

        let filename = format!("kiwi_test_data_dir_{}.conf", std::process::id());
        let tmp = std::env::temp_dir().join(filename);
        let config_path = tmp.to_str().unwrap();
        let mut f = std::fs::File::create(config_path).unwrap();
        writeln!(f, "port 7379").unwrap();
        writeln!(f, "data-dir /data/kiwi/db").unwrap();
        drop(f);

        let config = Config::load(config_path).unwrap();
        assert_eq!("/data/kiwi/db", config.data_dir);

        let _ = std::fs::remove_file(config_path);
    }

    #[test]
    fn test_redis_style_runtime_validation_is_applied() {
        use std::io::Write;

        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(config_file, "port 7379").unwrap();
        writeln!(config_file, "runtime-network-threads 0").unwrap();

        let loaded = Config::load(config_file.path().to_str().unwrap());
        assert!(
            loaded.is_err(),
            "runtime validation should reject zero threads"
        );
    }

    #[test]
    fn test_redis_style_runtime_skipped_fields_keep_defaults() {
        use std::io::Write;

        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(config_file, "port 7379").unwrap();
        writeln!(config_file, "memory 10M").unwrap();

        let loaded = Config::load(config_file.path().to_str().unwrap()).unwrap();
        assert_eq!(Duration::from_secs(30), loaded.runtime.request_timeout);
        assert_eq!(Duration::from_millis(10), loaded.runtime.batch_timeout);
        assert_eq!(
            Duration::from_secs(1),
            loaded.runtime.scaling.evaluation_interval
        );
        assert_eq!(
            Duration::from_millis(100),
            loaded.runtime.raft_metrics.collection_interval
        );
        assert_eq!(
            Duration::from_secs(3600),
            loaded.runtime.raft_metrics.retention_period
        );
        assert_eq!(
            Duration::from_millis(100),
            loaded.runtime.fault_injection.default_network_delay
        );
        assert_eq!(0.1, loaded.runtime.fault_injection.default_drop_rate);
    }

    #[test]
    fn test_redis_style_runtime_config_parsing() {
        use std::io::Write;

        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(config_file, "port 7379").unwrap();
        writeln!(config_file, "runtime-network-threads 3").unwrap();
        writeln!(config_file, "runtime-storage_threads 5").unwrap();
        writeln!(config_file, "runtime-channel-buffer-size 1024").unwrap();
        writeln!(config_file, "runtime-batch-size 11").unwrap();
        writeln!(config_file, "runtime-scaling-enabled yes").unwrap();
        writeln!(config_file, "runtime-scaling-min-network-threads 1").unwrap();
        writeln!(config_file, "runtime-scaling-max-network-threads 4").unwrap();
        writeln!(config_file, "runtime-scaling-min-storage-threads 2").unwrap();
        writeln!(config_file, "runtime-scaling-max-storage-threads 8").unwrap();
        writeln!(config_file, "runtime-scaling-scale-up-threshold 70").unwrap();
        writeln!(config_file, "runtime-scaling-scale-down-threshold 30").unwrap();
        writeln!(config_file, "runtime-scaling-scale-increment 2").unwrap();
        writeln!(config_file, "runtime-priority-enabled yes").unwrap();
        writeln!(config_file, "runtime-priority-high-priority-weight 6").unwrap();
        writeln!(config_file, "runtime-priority-normal-priority-weight 3").unwrap();
        writeln!(config_file, "runtime-priority-low-priority-weight 1").unwrap();
        writeln!(
            config_file,
            "runtime-priority-max-queue-size-per-priority 2048"
        )
        .unwrap();
        writeln!(config_file, "runtime-raft-metrics-enabled no").unwrap();
        writeln!(
            config_file,
            "runtime-raft-metrics-track-replication-latency no"
        )
        .unwrap();
        writeln!(config_file, "runtime-raft-metrics-track-election-events no").unwrap();
        writeln!(config_file, "runtime-fault-injection-enabled yes").unwrap();
        writeln!(config_file, "runtime-fault-injection-log-events no").unwrap();
        writeln!(config_file, "raft-node-id 1").unwrap();
        writeln!(config_file, "raft-addr 127.0.0.1:8501").unwrap();
        writeln!(config_file, "raft-resp-addr 127.0.0.1:7379").unwrap();
        writeln!(config_file, "raft-data-dir /tmp/kiwi/raft").unwrap();
        writeln!(config_file, "raft-heartbeat-interval-ms 200").unwrap();
        writeln!(config_file, "raft-election-timeout-min-ms 500").unwrap();
        writeln!(config_file, "raft-election-timeout-max-ms 1500").unwrap();

        let loaded = Config::load(config_file.path().to_str().unwrap()).unwrap();

        assert_eq!(3, loaded.runtime.network_threads);
        assert_eq!(5, loaded.runtime.storage_threads);
        assert_eq!(1024, loaded.runtime.channel_buffer_size);
        assert_eq!(11, loaded.runtime.batch_size);
        assert!(loaded.runtime.scaling.enabled);
        assert_eq!(4, loaded.runtime.scaling.max_network_threads);
        assert!(loaded.runtime.priority.enabled);
        assert_eq!(6, loaded.runtime.priority.high_priority_weight);
        assert!(!loaded.runtime.raft_metrics.enabled);
        assert!(!loaded.runtime.raft_metrics.track_replication_latency);
        assert!(loaded.runtime.fault_injection.enabled);
        assert!(!loaded.runtime.fault_injection.log_events);
        let raft = loaded.raft.unwrap();
        assert_eq!(Some(200), raft.heartbeat_interval_ms);
        assert_eq!(Some(500), raft.election_timeout_min_ms);
        assert_eq!(Some(1500), raft.election_timeout_max_ms);
    }

    #[test]
    fn test_sample_config_round_trip() {
        use std::io::Write;

        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "{}", Config::sample_config()).unwrap();
        assert!(
            Config::load(f.path().to_str().unwrap()).is_ok(),
            "sample config should be reloadable"
        );
    }

    #[test]
    fn test_full_sample_config_round_trip() {
        use std::io::Write;

        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "{}", Config::full_sample_config()).unwrap();
        let result = Config::load(f.path().to_str().unwrap());
        assert!(
            result.is_ok(),
            "full sample config should be reloadable: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_partial_raft_config_requires_node_id() {
        use std::io::Write;

        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(config_file, "port 7379").unwrap();
        writeln!(config_file, "raft-addr 127.0.0.1:8081").unwrap();

        let loaded = Config::load(config_file.path().to_str().unwrap());
        assert!(
            loaded.is_err(),
            "raft-* without raft-node-id should be rejected"
        );
    }
}
