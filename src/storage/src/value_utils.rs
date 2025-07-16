// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::{Error, Result};

/// 简化的数据类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    None = 0,
    String = 1,
    Hash = 2,
    List = 3,
    Set = 4,
    ZSet = 5,
}

impl DataType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(DataType::None),
            1 => Some(DataType::String),
            2 => Some(DataType::Hash),
            3 => Some(DataType::List),
            4 => Some(DataType::Set),
            5 => Some(DataType::ZSet),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// 零拷贝值处理工具
pub struct ValueUtils;

impl ValueUtils {
    /// 格式化值（包含类型和TTL信息）- 零拷贝版本
    pub fn format_value_bytes(value: &[u8], ttl: Option<u64>, dtype: DataType) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(9 + value.len());

        // 添加数据类型标记
        result.push(dtype as u8);

        // 添加TTL信息
        let ttl_value = ttl.unwrap_or(0);
        result.extend_from_slice(&ttl_value.to_le_bytes());

        // 添加值数据
        result.extend_from_slice(value);

        Ok(result)
    }

    /// 解析值 - 零拷贝版本，返回字节切片
    pub fn parse_value_bytes(data: &[u8]) -> Result<(&[u8], Option<u64>, DataType)> {
        if data.len() < 9 {
            return Err(Error::InvalidFormat {
                message: "Data too short for value format".to_string(),
            });
        }

        // 解析数据类型
        let dtype = DataType::from_u8(data[0]).ok_or_else(|| Error::InvalidFormat {
            message: format!("Invalid data type: {}", data[0]),
        })?;

        // 解析TTL
        let ttl_bytes = &data[1..9];
        let ttl = u64::from_le_bytes(ttl_bytes.try_into().unwrap());
        let ttl = if ttl > 0 { Some(ttl) } else { None };

        // 解析值（字节切片，避免字符串转换）
        let value_bytes = &data[9..];

        Ok((value_bytes, ttl, dtype))
    }

    /// 格式化字符串值（向后兼容）
    pub fn format_value_string(value: &str, ttl: Option<u64>, dtype: DataType) -> Result<Vec<u8>> {
        Self::format_value_bytes(value.as_bytes(), ttl, dtype)
    }

    /// 解析字符串值（向后兼容）
    pub fn parse_value_string(data: &[u8]) -> Result<(String, Option<u64>, DataType)> {
        let (value_bytes, ttl, dtype) = Self::parse_value_bytes(data)?;

        // 转换为字符串（仅在需要时）
        let value = String::from_utf8_lossy(value_bytes).to_string();

        Ok((value, ttl, dtype))
    }

    /// 检查值是否过期
    pub fn is_expired(ttl: Option<u64>) -> bool {
        if let Some(expire_time) = ttl {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            current_time >= expire_time
        } else {
            false
        }
    }

    /// 计算剩余TTL
    pub fn calculate_ttl(expire_time: Option<u64>) -> i64 {
        if let Some(expire_time) = expire_time {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if current_time >= expire_time {
                0
            } else {
                (expire_time - current_time) as i64
            }
        } else {
            -1 // 无过期时间
        }
    }
}

/// 字节切片包装器，用于零拷贝操作
#[derive(Debug, Clone)]
pub struct ByteSlice<'a> {
    data: &'a [u8],
    ttl: Option<u64>,
    dtype: DataType,
}

impl<'a> ByteSlice<'a> {
    pub fn new(data: &'a [u8], ttl: Option<u64>, dtype: DataType) -> Self {
        Self { data, ttl, dtype }
    }

    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    pub fn ttl(&self) -> Option<u64> {
        self.ttl
    }

    pub fn dtype(&self) -> DataType {
        self.dtype
    }

    pub fn is_expired(&self) -> bool {
        ValueUtils::is_expired(self.ttl)
    }

    pub fn to_string(&self) -> String {
        String::from_utf8_lossy(self.data).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_and_parse_value_bytes() {
        let test_data = b"hello world";
        let ttl = Some(1234567890);
        let dtype = DataType::String;

        // 格式化
        let formatted = ValueUtils::format_value_bytes(test_data, ttl, dtype).unwrap();

        // 解析
        let (parsed_data, parsed_ttl, parsed_dtype) =
            ValueUtils::parse_value_bytes(&formatted).unwrap();

        assert_eq!(parsed_data, test_data);
        assert_eq!(parsed_ttl, ttl);
        assert_eq!(parsed_dtype, dtype);
    }

    #[test]
    fn test_byte_slice() {
        let data = b"test data";
        let ttl = Some(1000);
        let dtype = DataType::String;

        let slice = ByteSlice::new(data, ttl, dtype);

        assert_eq!(slice.data(), data);
        assert_eq!(slice.ttl(), ttl);
        assert_eq!(slice.dtype(), dtype);
        assert_eq!(slice.to_string(), "test data");
    }

    #[test]
    fn test_ttl_calculation() {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 测试未来时间
        let future_ttl = Some(current_time + 100);
        assert!(!ValueUtils::is_expired(future_ttl));

        // 测试过去时间
        let past_ttl = Some(current_time - 100);
        assert!(ValueUtils::is_expired(past_ttl));

        // 测试无TTL
        assert!(!ValueUtils::is_expired(None));
    }
}
