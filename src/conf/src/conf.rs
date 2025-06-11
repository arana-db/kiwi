use std::collections::HashMap;
use std::fs;
use std::path::Path;


/// Redis config define
#[derive(Debug, Clone,Default)]
pub struct RedisConfig {
    port: u16,
    host: String,
}

impl RedisConfig {
    // read config file and parse file content
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let content = fs::read_to_string(path)
            .map_err(|e| format!("读取配置文件失败: {}", e))?;
        Self::parse(&content)
    }

    //pase content of file content
    pub fn parse(content: &str) -> Result<Self, String> {
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
                            return Err("port参数转换失败".to_string());
                        }
                    } else {
                        return Err("port参数只需要1个值".to_string());
                    }
                }
                "host" => {
                    if values.len() == 1 {
                        host = values[0].to_string();
                    } else {
                        return Err("host参数只需要1个值".to_string());
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
        let config:RedisConfig = RedisConfig::parse(SAMPLE_CONFIG).expect("解析失败");

        // 测试基本类型解析
        assert_eq!(config.port, 6379);
        assert_eq!(config.host,"127.0.0.1".to_string());

    }
}