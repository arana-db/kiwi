use std::collections::HashMap;
use std::fs;
use std::path::Path;
use snafu::Snafu;
use std::path::PathBuf;

// error define
#[derive(Debug, Snafu)]
enum ParseConfigError {
    #[snafu(display("read config file fail {}: {}", path.display(), source))]
    ReadFileErr {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("parse config key {} fail", key))]
    ParseConfItemErr { key: String },
}


// Redis config define
#[derive(Debug, Clone,Default)]
pub struct RedisConfig {
    port: u16,
    host: String,
}

impl RedisConfig {
    // read config file and parse file content

pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ParseConfigError> {
    let path_buf = path.as_ref().to_path_buf(); // 转换为拥有所有权的 PathBuf
    let content = fs::read_to_string(&path_buf) // 使用 &path_buf 借用
        .map_err(|e| ParseConfigError::ReadFileErr { path: path_buf, source: e })?; // 错误中使用 path_buf 的所有权
    Self::parse(&content)
}


    //pase content of file content
    pub fn parse(content: &str) -> Result<Self, ParseConfigError> {
        let mut line_number = 0;

        //default value
        let mut port: u16 = 6379;
        let mut host: String = "127.0.0.1".to_string();

        for line in content.lines() {
            line_number += 1;
            let line = line.trim();

            //skip for comment
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            //split by whitespace in line
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            let key = parts[0].to_lowercase(); // ignore case
            let values = parts[1..].iter().map(|s| s.to_string()).collect::<Vec<_>>();

            //get config value in config
            let value = match key.as_str() {
                "port" => {
                    if values.len() == 1 {
                        if let Ok(num) = values[0].parse::<u16>() {
                            port = num;
                        } else {
                            return Err(ParseConfigError::ParseConfItemErr {key: key.to_string()});
                        }
                    } else {
                        return Err(ParseConfigError::ParseConfItemErr {key: key.to_string()});
                    }
                }
                "host" => {
                    if values.len() == 1 {
                        host = values[0].to_string();
                    } else {
                        return Err(ParseConfigError::ParseConfItemErr {key: key.to_string()});
                    }
                }
                // 默认处理：整数或字符串
                _ => {
                    continue
                }
            };
        }

        Ok(RedisConfig {
            port,
            host,
        })
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_CONFIG: &str = r#"
        # Redis config
        port 6379
        bind 127.0.0.1 ::1
    "#;

    #[test]
    fn test_parse_config() {
        let config:RedisConfig = RedisConfig::parse(SAMPLE_CONFIG).expect("parse config err");

        // 测试基本类型解析
        assert_eq!(config.port, 6379);
        assert_eq!(config.host,"127.0.0.1".to_string());

    }
}