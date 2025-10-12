use bytes::{Bytes, BytesMut};

use crate::{
    error::{RespError, RespResult},
    traits::Encoder,
    types::{RespData, RespVersion},
};

#[derive(Default)]
pub struct Resp3Encoder;

impl Encoder for Resp3Encoder {
    fn encode_one(&mut self, data: &RespData) -> RespResult<Bytes> {
        let mut buf = BytesMut::new();
        self.encode_into(data, &mut buf)?;
        Ok(buf.freeze())
    }

    fn encode_into(&mut self, data: &RespData, out: &mut BytesMut) -> RespResult<()> {
        match data {
            RespData::Null => {
                out.extend_from_slice(b"_\r\n");
            }
            RespData::Boolean(true) => {
                out.extend_from_slice(b"#t\r\n");
            }
            RespData::Boolean(false) => {
                out.extend_from_slice(b"#f\r\n");
            }
            RespData::Double(v) => {
                out.extend_from_slice(b",");
                if v.is_infinite() {
                    if v.is_sign_positive() {
                        out.extend_from_slice(b"inf");
                    } else {
                        out.extend_from_slice(b"-inf");
                    }
                } else if v.is_nan() {
                    out.extend_from_slice(b"nan");
                } else {
                    use core::fmt::Write as _;
                    let mut s = String::new();
                    let _ = write!(&mut s, "{}", v);
                    out.extend_from_slice(s.as_bytes());
                }
                out.extend_from_slice(b"\r\n");
            }
            RespData::BulkError(b) => {
                use core::fmt::Write as _;
                let len = b.len();
                let _ = write!(out, "!{}\r\n", len);
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            RespData::VerbatimString { format, data } => {
                use core::fmt::Write as _;
                // fmt: exactly 3 bytes
                let payload_len = 3 + 1 + data.len();
                let _ = write!(out, "={}\r\n", payload_len);
                out.extend_from_slice(format);
                out.extend_from_slice(b":");
                out.extend_from_slice(data);
                out.extend_from_slice(b"\r\n");
            }
            RespData::BigNumber(s) => {
                out.extend_from_slice(b"(");
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespData::Map(entries) => {
                use core::fmt::Write as _;
                let _ = write!(out, "%{}\r\n", entries.len());
                for (k, v) in entries {
                    self.encode_into(k, out)?;
                    self.encode_into(v, out)?;
                }
            }
            RespData::Set(items) => {
                use core::fmt::Write as _;
                let _ = write!(out, "~{}\r\n", items.len());
                for it in items {
                    self.encode_into(it, out)?;
                }
            }
            RespData::Push(items) => {
                use core::fmt::Write as _;
                let _ = write!(out, ">{}\r\n", items.len());
                for it in items {
                    self.encode_into(it, out)?;
                }
            }
            _ => return Err(RespError::UnsupportedType),
        }
        Ok(())
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP3
    }
}
