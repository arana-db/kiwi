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
pub mod config;
pub mod de_func;
pub mod error;

#[cfg(test)]
mod tests {
    use super::*;
    use config::Config;
    use validator::Validate;

    #[test]
    fn test_config_parsing() {
        let config = config::Config::load("./config.ini");
        assert!(
            config.is_ok(),
            "Config loading failed: {:?}",
            config.err().unwrap()
        );

        let config = config.unwrap();

        assert_eq!(1430, config.port);
        assert_eq!(10 * 1024 * 1024, config.memory);

        assert_eq!(2, config.rocksdb_max_subcompactions);
        assert_eq!(4, config.rocksdb_max_background_jobs);
        assert_eq!(2, config.rocksdb_max_write_buffer_number);
        assert_eq!(2, config.rocksdb_min_write_buffer_number_to_merge);
        assert_eq!(67108864, config.rocksdb_write_buffer_size);
        assert_eq!(4, config.rocksdb_level0_file_num_compaction_trigger);
        assert_eq!(7, config.rocksdb_num_levels);
        assert_eq!(false, config.rocksdb_enable_pipelined_write); // no = false
        assert_eq!(20, config.rocksdb_level0_slowdown_writes_trigger);
        assert_eq!(36, config.rocksdb_level0_stop_writes_trigger);
        assert_eq!(604800, config.rocksdb_ttl_second);
        assert_eq!(259200, config.rocksdb_periodic_second);
        assert_eq!(true, config.rocksdb_level_compaction_dynamic_level_bytes);
        assert_eq!(10000, config.rocksdb_max_open_files);
        assert_eq!(64 << 20, config.rocksdb_target_file_size_base);

        assert_eq!(5000, config.small_compaction_threshold);
        assert_eq!(10000, config.small_compaction_duration_threshold);

        assert_eq!(50, config.timeout);
        assert_eq!("/data/kiwi_rs/logs", config.log_dir);
        assert_eq!(false, config.redis_compatible_mode);
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
            port: 999,
            timeout: 100,
            redis_compatible_mode: false,
            log_dir: "".to_string(),
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
            db_instance_num: 3,
            small_compaction_threshold: 5000,
            small_compaction_duration_threshold: 10000,
        };
        assert_eq!(false, invalid_config.validate().is_ok());

        invalid_config.port = 8080;
        assert_eq!(true, invalid_config.validate().is_ok());
    }
}
