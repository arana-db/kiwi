use crate::error::MemoryParseError;
use serde::{de, Deserialize, Deserializer};
pub fn deserialize_bool_from_yes_no<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    match s.to_lowercase().as_str() {
        "yes" | "true" | "1" | "on" => Ok(true),
        "no" | "false" | "0" | "off" => Ok(false),
        _ => Err(de::Error::custom(
            "expected one of: yes, no, true, false, 1, 0, on, off",
        )),
    }
}

pub fn deserialize_memory<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    match parse_memory(s.as_str()) {
        Ok(num) => Ok(num),
        Err(e) => Err(de::Error::custom(format!("{}", e))),
    }
}

pub fn parse_memory(input: &str) -> Result<u64, MemoryParseError> {
    let cleaned_input = input.trim().replace(',', "").to_uppercase();

    let num_end = cleaned_input
        .rfind(|c: char| c.is_ascii_digit() || c == '.')
        .map(|pos| pos + 1)
        .unwrap_or(0);

    let (num_str, unit_str) = cleaned_input.split_at(num_end);
    let unit_str = unit_str.trim();

    if num_str.is_empty() {
        return Err(MemoryParseError::InvalidFormat {
            raw: input.to_string(),
        });
    }

    let num_value: u64 = num_str
        .parse()
        .map_err(|e| MemoryParseError::InvalidNumber { source: e })?;

    let multiplier: u64 = match unit_str {
        "" | "B" => 1,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024u64.pow(2),
        "G" | "GB" => 1024u64.pow(3),
        "T" | "TB" => 1024u64.pow(4),
        _ => {
            return Err(MemoryParseError::UnknownUnit {
                unit: unit_str.to_string(),
            })
        }
    };

    let bytes = num_value * multiplier;
    Ok(bytes)
}
