use std::collections::VecDeque;

use bytes::Bytes;

use crate::{
    error::{RespError, RespResult},
    traits::Decoder,
    types::{RespData, RespVersion},
};

#[derive(Default)]
pub struct Resp3Decoder {
    out: VecDeque<RespResult<RespData>>,
    buf: bytes::BytesMut,
}

impl Decoder for Resp3Decoder {
    fn push(&mut self, data: Bytes) {
        // keep empty to avoid unused import warning
        self.buf.extend_from_slice(&data);

        loop {
            if self.buf.is_empty() {
                break;
            }
            match self.buf[0] {
                b'_' => {
                    if self.buf.len() < 3 {
                        break;
                    }
                    if &self.buf[..3] == b"_\r\n" {
                        let _ = self.buf.split_to(3);
                        self.out.push_back(Ok(RespData::Null));
                        continue;
                    } else {
                        self.out
                            .push_back(Err(RespError::ParseError("invalid RESP3 null".into())));
                        break;
                    }
                }
                b'#' => {
                    if self.buf.len() < 4 {
                        break;
                    }
                    // format: #t\r\n or #f\r\n
                    if &self.buf[..4] == b"#t\r\n" {
                        let _ = self.buf.split_to(4);
                        self.out.push_back(Ok(RespData::Boolean(true)));
                        continue;
                    }
                    if &self.buf[..4] == b"#f\r\n" {
                        let _ = self.buf.split_to(4);
                        self.out.push_back(Ok(RespData::Boolean(false)));
                        continue;
                    }
                    self.out
                        .push_back(Err(RespError::ParseError("invalid RESP3 boolean".into())));
                    break;
                }
                b',' => {
                    // format: ,<double>\r\n
                    if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                        let line_len = pos + 1;
                        let line = &self.buf[..line_len];
                        if line.len() < 3 || line[line.len() - 2] != b'\r' {
                            break;
                        }
                        // strip prefix ',' and CRLF
                        let chunk = self.buf.split_to(line_len);
                        let body = &chunk[1..chunk.len() - 2];
                        if let Ok(s) = std::str::from_utf8(body) {
                            let sl = s.to_ascii_lowercase();
                            let val = if sl == "inf" {
                                Some(f64::INFINITY)
                            } else if sl == "-inf" {
                                Some(f64::NEG_INFINITY)
                            } else if sl == "nan" {
                                Some(f64::NAN)
                            } else {
                                s.parse::<f64>().ok()
                            };
                            match val {
                                Some(v) => {
                                    self.out.push_back(Ok(RespData::Double(v)));
                                    continue;
                                }
                                None => {
                                    self.out.push_back(Err(RespError::ParseError(
                                        "invalid RESP3 double".into(),
                                    )));
                                    break;
                                }
                            }
                        } else {
                            self.out.push_back(Err(RespError::ParseError(
                                "invalid RESP3 double".into(),
                            )));
                            break;
                        }
                    } else {
                        break;
                    }
                }
                b'!' => {
                    // Bulk error: !<len>\r\n<data>\r\n
                    if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                        if nl < 3 || self.buf[nl - 1] != b'\r' {
                            break;
                        }
                        let len_bytes = &self.buf[1..nl - 1];
                        let len = match std::str::from_utf8(len_bytes)
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            Some(v) => v,
                            None => {
                                self.out.push_back(Err(RespError::ParseError(
                                    "invalid bulk error len".into(),
                                )));
                                break;
                            }
                        };
                        let need = nl + 1 + len + 2;
                        if self.buf.len() < need {
                            break;
                        }
                        let chunk = self.buf.split_to(need);
                        if &chunk[nl + 1 + len..need] != b"\r\n" {
                            self.out.push_back(Err(RespError::ParseError(
                                "invalid bulk error ending".into(),
                            )));
                            break;
                        }
                        let data = bytes::Bytes::copy_from_slice(&chunk[nl + 1..nl + 1 + len]);
                        self.out.push_back(Ok(RespData::BulkError(data)));
                        continue;
                    } else {
                        break;
                    }
                }
                b'=' => {
                    // Verbatim string: =<len>\r\n<fmt>:<data>\r\n, fmt is 3 bytes
                    if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                        if nl < 3 || self.buf[nl - 1] != b'\r' {
                            break;
                        }
                        let len_bytes = &self.buf[1..nl - 1];
                        let len = match std::str::from_utf8(len_bytes)
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            Some(v) => v,
                            None => {
                                self.out.push_back(Err(RespError::ParseError(
                                    "invalid verbatim len".into(),
                                )));
                                break;
                            }
                        };
                        let need = nl + 1 + len + 2;
                        if self.buf.len() < need {
                            break;
                        }
                        let chunk = self.buf.split_to(need);
                        let content = &chunk[nl + 1..nl + 1 + len];
                        if content.len() < 4 || content[3] != b':' {
                            self.out.push_back(Err(RespError::ParseError(
                                "invalid verbatim header".into(),
                            )));
                            break;
                        }
                        let mut fmt = [0u8; 3];
                        fmt.copy_from_slice(&content[0..3]);
                        let data = bytes::Bytes::copy_from_slice(&content[4..]);
                        if &chunk[nl + 1 + len..need] != b"\r\n" {
                            self.out.push_back(Err(RespError::ParseError(
                                "invalid verbatim ending".into(),
                            )));
                            break;
                        }
                        self.out
                            .push_back(Ok(RespData::VerbatimString { format: fmt, data }));
                        continue;
                    } else {
                        break;
                    }
                }
                b'(' => {
                    // Big number: (<string>)\r\n, we treat contents as string until CRLF
                    if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                        let line_len = pos + 1;
                        let chunk = self.buf.split_to(line_len);
                        if chunk.len() < 3 || chunk[chunk.len() - 2] != b'\r' {
                            break;
                        }
                        let body = &chunk[1..chunk.len() - 2];
                        match std::str::from_utf8(body) {
                            Ok(s) => {
                                self.out.push_back(Ok(RespData::BigNumber(s.to_string())));
                                continue;
                            }
                            Err(_) => {
                                self.out.push_back(Err(RespError::ParseError(
                                    "invalid big number".into(),
                                )));
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                }
                b'%' => {
                    // Map: %<len>\r\n<k1><v1>... ; we only support nested scalars implemented so far
                    if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                        if nl < 3 || self.buf[nl - 1] != b'\r' {
                            break;
                        }
                        let len_bytes = &self.buf[1..nl - 1];
                        let pairs = match std::str::from_utf8(len_bytes)
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            Some(v) => v,
                            None => {
                                self.out.push_back(Err(RespError::ParseError(
                                    "invalid map len".into(),
                                )));
                                break;
                            }
                        };
                        let _ = self.buf.split_to(nl + 1);
                        let mut items = Vec::with_capacity(pairs);
                        for _ in 0..pairs {
                            // parse key
                            let k = if self.buf.is_empty() {
                                break;
                            } else {
                                match self.buf[0] {
                                    b'_' => {
                                        if self.buf.len() < 3 {
                                            break;
                                        }
                                        let _ = self.buf.split_to(3);
                                        RespData::Null
                                    }
                                    b'#' => {
                                        if self.buf.len() < 4 {
                                            break;
                                        }
                                        let is_true = self.buf[1] == b't';
                                        let _ = self.buf.split_to(4);
                                        if is_true {
                                            RespData::Boolean(true)
                                        } else {
                                            RespData::Boolean(false)
                                        }
                                    }
                                    b',' => {
                                        if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                                            let line = self.buf.split_to(pos + 1);
                                            if line.len() < 3 || line[line.len() - 2] != b'\r' {
                                                break;
                                            }
                                            let s =
                                                std::str::from_utf8(&line[1..line.len() - 2]).ok();
                                            let val = s.and_then(|s| {
                                                let sl = s.to_ascii_lowercase();
                                                if sl == "inf" {
                                                    Some(f64::INFINITY)
                                                } else if sl == "-inf" {
                                                    Some(f64::NEG_INFINITY)
                                                } else if sl == "nan" {
                                                    Some(f64::NAN)
                                                } else {
                                                    s.parse::<f64>().ok()
                                                }
                                            });
                                            match val {
                                                Some(v) => RespData::Double(v),
                                                None => {
                                                    self.out.push_back(Err(RespError::ParseError(
                                                        "invalid double".into(),
                                                    )));
                                                    break;
                                                }
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                    b'!' => {
                                        if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                                            if nl < 3 || self.buf[nl - 1] != b'\r' {
                                                break;
                                            }
                                            let len = std::str::from_utf8(&self.buf[1..nl - 1])
                                                .ok()
                                                .and_then(|s| s.parse::<usize>().ok());
                                            let len = match len {
                                                Some(v) => v,
                                                None => {
                                                    self.out.push_back(Err(RespError::ParseError(
                                                        "invalid bulk error len".into(),
                                                    )));
                                                    break;
                                                }
                                            };
                                            let need = nl + 1 + len + 2;
                                            if self.buf.len() < need {
                                                break;
                                            }
                                            let chunk = self.buf.split_to(need);
                                            if &chunk[nl + 1 + len..need] != b"\r\n" {
                                                self.out.push_back(Err(RespError::ParseError(
                                                    "invalid bulk error ending".into(),
                                                )));
                                                break;
                                            }
                                            RespData::BulkError(bytes::Bytes::copy_from_slice(
                                                &chunk[nl + 1..nl + 1 + len],
                                            ))
                                        } else {
                                            break;
                                        }
                                    }
                                    _ => break,
                                }
                            };
                            // parse value
                            let v = if self.buf.is_empty() {
                                break;
                            } else {
                                match self.buf[0] {
                                    b'_' => {
                                        if self.buf.len() < 3 {
                                            break;
                                        }
                                        let _ = self.buf.split_to(3);
                                        RespData::Null
                                    }
                                    b'#' => {
                                        if self.buf.len() < 4 {
                                            break;
                                        }
                                        let is_true = self.buf[1] == b't';
                                        let _ = self.buf.split_to(4);
                                        if is_true {
                                            RespData::Boolean(true)
                                        } else {
                                            RespData::Boolean(false)
                                        }
                                    }
                                    b',' => {
                                        if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                                            let line = self.buf.split_to(pos + 1);
                                            if line.len() < 3 || line[line.len() - 2] != b'\r' {
                                                break;
                                            }
                                            let s =
                                                std::str::from_utf8(&line[1..line.len() - 2]).ok();
                                            let val = s.and_then(|s| {
                                                let sl = s.to_ascii_lowercase();
                                                if sl == "inf" {
                                                    Some(f64::INFINITY)
                                                } else if sl == "-inf" {
                                                    Some(f64::NEG_INFINITY)
                                                } else if sl == "nan" {
                                                    Some(f64::NAN)
                                                } else {
                                                    s.parse::<f64>().ok()
                                                }
                                            });
                                            match val {
                                                Some(v) => RespData::Double(v),
                                                None => {
                                                    self.out.push_back(Err(RespError::ParseError(
                                                        "invalid double".into(),
                                                    )));
                                                    break;
                                                }
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                    b'!' => {
                                        if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                                            if nl < 3 || self.buf[nl - 1] != b'\r' {
                                                break;
                                            }
                                            let len = std::str::from_utf8(&self.buf[1..nl - 1])
                                                .ok()
                                                .and_then(|s| s.parse::<usize>().ok());
                                            let len = match len {
                                                Some(v) => v,
                                                None => {
                                                    self.out.push_back(Err(RespError::ParseError(
                                                        "invalid bulk error len".into(),
                                                    )));
                                                    break;
                                                }
                                            };
                                            let need = nl + 1 + len + 2;
                                            if self.buf.len() < need {
                                                break;
                                            }
                                            let chunk = self.buf.split_to(need);
                                            if &chunk[nl + 1 + len..need] != b"\r\n" {
                                                self.out.push_back(Err(RespError::ParseError(
                                                    "invalid bulk error ending".into(),
                                                )));
                                                break;
                                            }
                                            RespData::BulkError(bytes::Bytes::copy_from_slice(
                                                &chunk[nl + 1..nl + 1 + len],
                                            ))
                                        } else {
                                            break;
                                        }
                                    }
                                    _ => break,
                                }
                            };
                            items.push((k, v));
                        }
                        self.out.push_back(Ok(RespData::Map(items)));
                        continue;
                    } else {
                        break;
                    }
                }
                b'~' => {
                    // Set: ~<len>\r\n<item>...
                    if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                        if nl < 3 || self.buf[nl - 1] != b'\r' {
                            break;
                        }
                        let len_bytes = &self.buf[1..nl - 1];
                        let count = match std::str::from_utf8(len_bytes)
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            Some(v) => v,
                            None => {
                                self.out.push_back(Err(RespError::ParseError(
                                    "invalid set len".into(),
                                )));
                                break;
                            }
                        };
                        let _ = self.buf.split_to(nl + 1);
                        let mut items = Vec::with_capacity(count);
                        for _ in 0..count {
                            if self.buf.is_empty() {
                                break;
                            }
                            let val = match self.buf[0] {
                                b'_' => {
                                    if self.buf.len() < 3 {
                                        break;
                                    }
                                    let _ = self.buf.split_to(3);
                                    RespData::Null
                                }
                                b'#' => {
                                    if self.buf.len() < 4 {
                                        break;
                                    }
                                    let is_true = self.buf[1] == b't';
                                    let _ = self.buf.split_to(4);
                                    if is_true {
                                        RespData::Boolean(true)
                                    } else {
                                        RespData::Boolean(false)
                                    }
                                }
                                b',' => {
                                    if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                                        let line = self.buf.split_to(pos + 1);
                                        if line.len() < 3 || line[line.len() - 2] != b'\r' {
                                            break;
                                        }
                                        let s = std::str::from_utf8(&line[1..line.len() - 2]).ok();
                                        let val = s.and_then(|s| {
                                            let sl = s.to_ascii_lowercase();
                                            if sl == "inf" {
                                                Some(f64::INFINITY)
                                            } else if sl == "-inf" {
                                                Some(f64::NEG_INFINITY)
                                            } else if sl == "nan" {
                                                Some(f64::NAN)
                                            } else {
                                                s.parse::<f64>().ok()
                                            }
                                        });
                                        match val {
                                            Some(v) => RespData::Double(v),
                                            None => {
                                                self.out.push_back(Err(RespError::ParseError(
                                                    "invalid double".into(),
                                                )));
                                                break;
                                            }
                                        }
                                    } else {
                                        break;
                                    }
                                }
                                b'!' => {
                                    if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                                        if nl < 3 || self.buf[nl - 1] != b'\r' {
                                            break;
                                        }
                                        let len = std::str::from_utf8(&self.buf[1..nl - 1])
                                            .ok()
                                            .and_then(|s| s.parse::<usize>().ok());
                                        let len = match len {
                                            Some(v) => v,
                                            None => {
                                                self.out.push_back(Err(RespError::ParseError(
                                                    "invalid bulk error len".into(),
                                                )));
                                                break;
                                            }
                                        };
                                        let need = nl + 1 + len + 2;
                                        if self.buf.len() < need {
                                            break;
                                        }
                                        let chunk = self.buf.split_to(need);
                                        if &chunk[nl + 1 + len..need] != b"\r\n" {
                                            self.out.push_back(Err(RespError::ParseError(
                                                "invalid bulk error ending".into(),
                                            )));
                                            break;
                                        }
                                        RespData::BulkError(bytes::Bytes::copy_from_slice(
                                            &chunk[nl + 1..nl + 1 + len],
                                        ))
                                    } else {
                                        break;
                                    }
                                }
                                _ => break,
                            };
                            items.push(val);
                        }
                        self.out.push_back(Ok(RespData::Set(items)));
                        continue;
                    } else {
                        break;
                    }
                }
                b'>' => {
                    // Push: >len\r\n<item>...
                    if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                        if nl < 3 || self.buf[nl - 1] != b'\r' {
                            break;
                        }
                        let len_bytes = &self.buf[1..nl - 1];
                        let count = match std::str::from_utf8(len_bytes)
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                        {
                            Some(v) => v,
                            None => {
                                self.out.push_back(Err(RespError::ParseError(
                                    "invalid push len".into(),
                                )));
                                break;
                            }
                        };
                        let _ = self.buf.split_to(nl + 1);
                        let mut items = Vec::with_capacity(count);
                        for _ in 0..count {
                            if self.buf.is_empty() {
                                break;
                            }
                            let val = match self.buf[0] {
                                b'_' => {
                                    if self.buf.len() < 3 {
                                        break;
                                    }
                                    let _ = self.buf.split_to(3);
                                    RespData::Null
                                }
                                b'#' => {
                                    if self.buf.len() < 4 {
                                        break;
                                    }
                                    let is_true = self.buf[1] == b't';
                                    let _ = self.buf.split_to(4);
                                    if is_true {
                                        RespData::Boolean(true)
                                    } else {
                                        RespData::Boolean(false)
                                    }
                                }
                                b',' => {
                                    if let Some(pos) = memchr::memchr(b'\n', &self.buf) {
                                        let line = self.buf.split_to(pos + 1);
                                        if line.len() < 3 || line[line.len() - 2] != b'\r' {
                                            break;
                                        }
                                        let s = std::str::from_utf8(&line[1..line.len() - 2]).ok();
                                        let val = s.and_then(|s| {
                                            let sl = s.to_ascii_lowercase();
                                            if sl == "inf" {
                                                Some(f64::INFINITY)
                                            } else if sl == "-inf" {
                                                Some(f64::NEG_INFINITY)
                                            } else if sl == "nan" {
                                                Some(f64::NAN)
                                            } else {
                                                s.parse::<f64>().ok()
                                            }
                                        });
                                        match val {
                                            Some(v) => RespData::Double(v),
                                            None => {
                                                self.out.push_back(Err(RespError::ParseError(
                                                    "invalid double".into(),
                                                )));
                                                break;
                                            }
                                        }
                                    } else {
                                        break;
                                    }
                                }
                                b'!' => {
                                    if let Some(nl) = memchr::memchr(b'\n', &self.buf) {
                                        if nl < 3 || self.buf[nl - 1] != b'\r' {
                                            break;
                                        }
                                        let len = std::str::from_utf8(&self.buf[1..nl - 1])
                                            .ok()
                                            .and_then(|s| s.parse::<usize>().ok());
                                        let len = match len {
                                            Some(v) => v,
                                            None => {
                                                self.out.push_back(Err(RespError::ParseError(
                                                    "invalid bulk error len".into(),
                                                )));
                                                break;
                                            }
                                        };
                                        let need = nl + 1 + len + 2;
                                        if self.buf.len() < need {
                                            break;
                                        }
                                        let chunk = self.buf.split_to(need);
                                        if &chunk[nl + 1 + len..need] != b"\r\n" {
                                            self.out.push_back(Err(RespError::ParseError(
                                                "invalid bulk error ending".into(),
                                            )));
                                            break;
                                        }
                                        RespData::BulkError(bytes::Bytes::copy_from_slice(
                                            &chunk[nl + 1..nl + 1 + len],
                                        ))
                                    } else {
                                        break;
                                    }
                                }
                                _ => break,
                            };
                            items.push(val);
                        }
                        self.out.push_back(Ok(RespData::Push(items)));
                        continue;
                    } else {
                        break;
                    }
                }
                _ => {
                    break;
                }
            }
        }
    }

    fn next(&mut self) -> Option<RespResult<RespData>> {
        self.out.pop_front()
    }

    fn reset(&mut self) {
        self.out.clear();
        self.buf.clear();
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP3
    }
}
