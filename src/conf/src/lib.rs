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

#[cfg(test)]
mod tests {
    use validator::Validate;

    use super::*;
    use crate::config::Config;

    #[test]
    fn test_config_parsing() {
        let config = config::Config::load("./kiwi.conf");
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
        assert_eq!("/data/kiwi_rs/logs", config.log_dir);
        assert_eq!("./db", config.db_dir);
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
        let mut invalid_config = Config::default();
        invalid_config.port = 999;
        assert!(invalid_config.validate().is_err());

        invalid_config.port = 8080;
        assert!(invalid_config.validate().is_ok());
    }

    #[test]
    fn test_db_dir_default() {
        let config = Config::default();
        assert_eq!("./db", config.db_dir);
    }

    #[test]
    fn test_db_dir_from_config_file() {
        use std::io::Write;

        let tmp = std::env::temp_dir().join(format!(
            "kiwi_test_db_dir_{}.toml",
            std::process::id()
        ));
        let config_path = tmp.to_str().unwrap();
        let mut f = std::fs::File::create(config_path).unwrap();
        writeln!(f, "port = 7379").unwrap();
        writeln!(f, "db-dir = \"/data/kiwi/db\"").unwrap();
        drop(f);

        let config = Config::load(config_path).unwrap();
        assert_eq!("/data/kiwi/db", config.db_dir);

        let _ = std::fs::remove_file(config_path);
    }

    #[test]
    fn test_toml_raft_section() {
        use std::io::Write;

        let tmp = std::env::temp_dir().join(format!(
            "kiwi_test_raft_{}.toml",
            std::process::id()
        ));
        let config_path = tmp.to_str().unwrap();
        let mut f = std::fs::File::create(config_path).unwrap();
        writeln!(f, "port = 7379").unwrap();
        writeln!(f, "[raft]").unwrap();
        writeln!(f, "node-id = 1").unwrap();
        writeln!(f, "raft-addr = \"127.0.0.1:8081\"").unwrap();
        writeln!(f, "resp-addr = \"127.0.0.1:6379\"").unwrap();
        writeln!(f, "data-dir = \"/tmp/kiwi/raft\"").unwrap();
        drop(f);

        let config = Config::load(config_path).unwrap();
        assert!(config.raft.is_some());
        let raft = config.raft.unwrap();
        assert_eq!(1, raft.node_id);
        assert_eq!("127.0.0.1:8081", raft.raft_addr);
        assert_eq!("127.0.0.1:6379", raft.resp_addr);
        assert_eq!("/tmp/kiwi/raft", raft.data_dir);
        assert!(!raft.use_memory_log_store);

        let _ = std::fs::remove_file(config_path);
    }

    #[test]
    fn test_toml_memory_string() {
        use std::io::Write;

        let tmp = std::env::temp_dir().join(format!(
            "kiwi_test_memory_{}.toml",
            std::process::id()
        ));
        let config_path = tmp.to_str().unwrap();
        let mut f = std::fs::File::create(config_path).unwrap();
        writeln!(f, "port = 7379").unwrap();
        writeln!(f, "memory = \"2GB\"").unwrap();
        drop(f);

        let config = Config::load(config_path).unwrap();
        assert_eq!(2 * 1024 * 1024 * 1024, config.memory);

        let _ = std::fs::remove_file(config_path);
    }

    #[test]
    fn test_toml_memory_integer() {
        use std::io::Write;

        let tmp = std::env::temp_dir().join(format!(
            "kiwi_test_memory_int_{}.toml",
            std::process::id()
        ));
        let config_path = tmp.to_str().unwrap();
        let mut f = std::fs::File::create(config_path).unwrap();
        writeln!(f, "port = 7379").unwrap();
        writeln!(f, "memory = 536870912").unwrap(); // 512MB as raw bytes
        drop(f);

        let config = Config::load(config_path).unwrap();
        assert_eq!(536870912, config.memory);

        let _ = std::fs::remove_file(config_path);
    }

    #[test]
    fn test_minimal_toml() {
        use std::io::Write;

        let tmp = std::env::temp_dir().join(format!(
            "kiwi_test_minimal_{}.toml",
            std::process::id()
        ));
        let config_path = tmp.to_str().unwrap();
        let mut f = std::fs::File::create(config_path).unwrap();
        writeln!(f, "# minimal config - everything else uses defaults").unwrap();
        drop(f);

        let config = Config::load(config_path).unwrap();
        assert_eq!(7379, config.port);
        assert_eq!("127.0.0.1", config.binding);
        assert!(config.raft.is_none());

        let _ = std::fs::remove_file(config_path);
    }
}
