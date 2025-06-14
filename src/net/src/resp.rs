use std::fmt;

#[derive(Debug)]
pub enum ParseError {
    InvalidFormat,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::InvalidFormat => write!(f, "Invalid RESP format"),
        }
    }
}

pub trait Protocol: Send + Sync {
    fn push_bulk_string(&mut self, p0: String);
    fn serialize(&self) -> Vec<u8>;
    fn parse(&mut self, v: &[u8]) -> Result<bool, ParseError>;
}

pub struct RespProtocol {
    args: Vec<Vec<u8>>,
    buffer: Vec<u8>,
    response: Vec<u8>,
}

impl RespProtocol {
    pub fn new() -> RespProtocol {
        RespProtocol {
            args: Vec::new(),
            buffer: Vec::new(),
            response: Vec::new(),
        }
    }

    pub fn take_args(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.args)
    }
}

impl Protocol for RespProtocol {
    fn push_bulk_string(&mut self, p0: String) {
        self.response.extend_from_slice(p0.as_bytes());
    }
    fn serialize(&self) -> Vec<u8> {
        let mut resp = Vec::<u8>::new();
        resp.push(b'$');
        resp.extend_from_slice(self.response.len().to_string().as_bytes());
        resp.push(b'\r');
        resp.push(b'\n');
        resp.extend_from_slice(&self.response);
        resp.push(b'\r');
        resp.push(b'\n');
        resp
    }
    fn parse(&mut self, v: &[u8]) -> Result<bool, ParseError> {
        self.buffer.extend_from_slice(v);  // 累积新数据
        let mut pos = 0;
        let buf = &self.buffer;

        // 1. 检查是否是数组类型（*开头）
        if buf.get(pos) != Some(&b'*') {
            return Err(ParseError::InvalidFormat);
        }
        pos += 1;

        // 2. 解析数组元素数量（*<count>\r\n）
        let count_end = match buf[pos..].iter().position(|&b| b == b'\r') {
            Some(i) => pos + i,
            None => return Ok(false),  // 数据不完整
        };
        if count_end + 1 >= buf.len() || buf[count_end + 1] != b'\n' {
            return Err(ParseError::InvalidFormat);  // 格式错误
        }
        let count_str = &buf[pos..count_end];

        let count = std::str::from_utf8(count_str).unwrap().parse::<usize>()
            .map_err(|_| ParseError::InvalidFormat)
            .expect("TODO: panic message");
        // 显式指定usize类型解决类型推断问题
        pos = count_end + 2;  // 移动到\r\n之后

        // 3. 解析每个批量字符串元素（$<len>\r\n<data>\r\n）
        let mut parsed_args = Vec::with_capacity(count);
        for _ in 0..count {
            // 检查是否是批量字符串（$开头）
            if buf.get(pos) != Some(&b'$') {
                return Ok(false);
            }
            pos += 1;

            // 解析字符串长度（$<len>\r\n）
            let len_end = match buf[pos..].iter().position(|&b| b == b'\r') {
                Some(i) => pos + i,
                None => return Ok(false),  // 数据不完整,
            };
            if len_end + 1 >= buf.len() || buf[len_end + 1] != b'\n' {
                return Ok(false);
            }
            let len_str = &buf[pos..len_end];
            // 显式指定usize类型解决类型推断问题
            let len = std::str::from_utf8(len_str)
                .unwrap().parse::<usize>()
                .expect("TODO: panic message");
            pos = len_end + 2;  // 移动到\r\n之后

            // 检查数据部分是否完整（<data>\r\n）
            if pos + len + 2 > buf.len() {
                return Ok(false);  // 数据不足
            }
            if buf[pos + len] != b'\r' || buf[pos + len + 1] != b'\n' {
                return Err(ParseError::InvalidFormat);  // 格式错误
            }

            // 提取有效数据
            parsed_args.push(buf[pos..pos + len].to_vec());
            pos += len + 2;  // 移动到当前元素末尾
        }

        // 4. 更新状态
        self.args = parsed_args;
        self.buffer = self.buffer.drain(pos..).collect();  // 保留未解析数据
        Ok(true)
    }
}

