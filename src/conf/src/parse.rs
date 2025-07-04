use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;

#[derive(Debug)]
pub struct ConfigParser {
    data: HashMap<String, Vec<String>>,
}

impl ConfigParser {
    pub fn new() -> Self {
        ConfigParser {
            data: HashMap::new(),
        }
    }

    pub fn load<P: AsRef<Path>>(&mut self, path: P) -> Result<(), String> {
        let file = File::open(path.as_ref()).map_err(|e| e.to_string())?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.map_err(|e| e.to_string())?;
            let line = line.trim().to_string();

            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once(|c: char| c.is_whitespace()) {
                let key = key.to_lowercase();
                let value = value.trim().to_string();
                self.data.entry(key).or_default().push(value);
            }
        }

        Ok(())
    }

    pub fn get_data<T>(&self, key: &str, default: T) -> T
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        self.data
            .get(key)
            .and_then(|v| v.first())
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    pub fn get_map(&self) -> &HashMap<String, Vec<String>> {
        &self.data
    }

    #[cfg(test)]
    pub fn print(&self) {
        for (k, v) in &self.data {
            println!("{}: {:?}", k, v);
        }
    }
}
