mod config;

use std::fs;
use serde_ini;
use serde::{Deserialize, Deserializer,de};

#[derive(Debug, Deserialize)]
struct Owner {

}
#[derive(Debug, Deserialize)]
struct Database {
    server: String,
    #[validate(range(min = 1024, max = 65535))]
    port: u16,
    file: String,
    name: String,
    organization: String,
}
#[derive(Debug, Deserialize)]
struct Config {
    server: String,
    port: u16,
    file: String,
    name: String,
    #[serde(deserialize_with = "deserialize_bool_from_yes_no")]
    short: bool,
    organization: String,
}

fn deserialize_bool_from_yes_no<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    // 将字符串转换为小写，然后匹配
    match s.to_lowercase().as_str() {
        "yes" | "true" | "1" | "on" => Ok(true),
        "no" | "false" | "0" | "off" => Ok(false),
        _ => Err(de::Error::custom("expected one of: yes, no, true, false, 1, 0, on, off")),
    }
}

fn main() {
    let content = fs::read_to_string("config.ini").expect("无法读取文件");
    let config: Config = serde_ini::from_str(&content).expect("无法解析INI文件");
    println!("{:#?}", config);
}