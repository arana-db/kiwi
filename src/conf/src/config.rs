use crate::de_func::{deserialize_bool_from_yes_no, deserialize_memory};
use serde::Deserialize;
use serde_ini;
use snafu::ResultExt;
use validator::Validate;
use crate::error::Error;

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
        let content = std::fs::read_to_string(path).context(crate::error::ConfigFileSnafu { path: path })?;

        let config: Config = serde_ini::from_str(&content).context(crate::error::InvalidConfigSnafu {})?;

        config.validate().map_err(|e| Error::ValidConfigFail { source: e })?;

        Ok(config)
    }
}
