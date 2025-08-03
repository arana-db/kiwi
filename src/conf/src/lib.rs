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
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(50, config.timeout);
    }

    #[test]
    fn test_validate_port_range() {
        let mut invalid_config = Config {
            port: 999,
            timeout: 100,
            redis_compatible_mode: false,
            log_dir: "".to_string(),
            memory: 1024,
        };
        assert_eq!(false, invalid_config.validate().is_ok());

        invalid_config.port = 8080;
        assert_eq!(true, invalid_config.validate().is_ok());
    }
}
