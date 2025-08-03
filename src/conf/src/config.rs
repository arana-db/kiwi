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
use crate::de_func::{deserialize_bool_from_yes_no, deserialize_memory};
use crate::error::Error;
use serde::Deserialize;
use serde_ini;
use snafu::ResultExt;
use validator::Validate;

//config struct define
#[derive(Debug, Deserialize, Validate)]
#[serde(default)]
pub struct Config {
    #[validate(range(min = 1024, max = 65535))]
    pub port: u16,

    #[validate(range(min = 1, max = 1000))]
    pub timeout: u32,

    pub log_dir: String,

    #[serde(deserialize_with = "deserialize_memory")]
    pub memory: u64,

    #[serde(deserialize_with = "deserialize_bool_from_yes_no")]
    pub redis_compatible_mode: bool,
}

//set default value for config
impl Default for Config {
    fn default() -> Self {
        Self {
            port: 8080,
            timeout: 50,
            memory: 1024 * 1024 * 1024,
            log_dir: "/data/kiwi_rs/logs".to_string(),
            redis_compatible_mode: false,
        }
    }
}

impl Config {
    //load config from file
    pub fn load(path: &str) -> Result<Self, Error> {
        let content =
            std::fs::read_to_string(path).context(crate::error::ConfigFileSnafu { path })?;

        let config: Config =
            serde_ini::from_str(&content).context(crate::error::InvalidConfigSnafu {})?;

        config
            .validate()
            .map_err(|e| Error::ValidConfigFail { source: e })?;

        Ok(config)
    }
}
