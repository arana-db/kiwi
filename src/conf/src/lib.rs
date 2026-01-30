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
    use config::Config;
    use validator::Validate;

    use super::*;

    #[test]
    fn test_config_parsing() {
        let config = config::Config::load("./kiwi.conf");
        assert!(
            config.is_ok(),
            "Config loading failed: {:?}",
            config.err().unwrap()
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
        assert!(!config.redis_compatible_mode);
        assert_eq!(3, config.db_instance_num);

        assert!(
            config.validate().is_ok(),
            "Config validation failed: {:?}",
            config.validate().err()
        );
    }
}
