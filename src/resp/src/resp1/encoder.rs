use bytes::{Bytes, BytesMut};

use crate::{
    compat::{BooleanMode, DoubleMode, DownlevelPolicy, MapMode},
    encode::{RespEncode, RespEncoder},
    error::RespResult,
    traits::Encoder,
    types::{RespData, RespVersion},
};

#[derive(Default)]
pub struct Resp1Encoder {
    inner: RespEncoder,
    policy: DownlevelPolicy,
}

impl Resp1Encoder {
    pub fn new() -> Self {
        Self {
            inner: RespEncoder::new(RespVersion::RESP1),
            policy: DownlevelPolicy::default(),
        }
    }

    pub fn with_policy(policy: DownlevelPolicy) -> Self {
        Self {
            inner: RespEncoder::new(RespVersion::RESP1),
            policy,
        }
    }
}

impl Encoder for Resp1Encoder {
    fn encode_one(&mut self, data: &RespData) -> RespResult<Bytes> {
        self.inner.clear();
        self.encode_downleveled(data);
        Ok(self.inner.get_response())
    }

    fn encode_into(&mut self, data: &RespData, out: &mut BytesMut) -> RespResult<()> {
        let bytes = self.encode_one(data)?;
        out.extend_from_slice(&bytes);
        Ok(())
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP1
    }
}

impl Resp1Encoder {
    fn encode_downleveled(&mut self, data: &RespData) {
        match data {
            // RESP1 uses inline/simple/integer/bulk/array semantics basically consistent
            RespData::SimpleString(_)
            | RespData::Error(_)
            | RespData::Integer(_)
            | RespData::BulkString(_)
            | RespData::Array(_)
            | RespData::Inline(_) => {
                self.inner.encode_resp_data(data);
            }
            // Downlevel mapping strategy consistent with RESP2
            RespData::Null => {
                self.inner.set_line_string("$-1");
            }
            RespData::Boolean(b) => {
                match self.policy.boolean_mode {
                    BooleanMode::Integer => self.inner.append_integer(if *b { 1 } else { 0 }),
                    BooleanMode::SimpleString => {
                        if *b {
                            self.inner.append_simple_string("OK")
                        } else {
                            self.inner.append_simple_string("ERR")
                        }
                    }
                };
            }
            RespData::Double(v) => {
                if let DoubleMode::IntegerIfWhole = self.policy.double_mode {
                    if v.fract() == 0.0 && v.is_finite() {
                        self.inner.append_integer(*v as i64);
                        return;
                    }
                }
                self.inner.append_string(&format!("{}", v));
            }
            crate::types::RespData::BulkError(msg) => {
                self.inner
                    .append_string_raw(&format!("-{}\r\n", String::from_utf8_lossy(msg)));
            }
            crate::types::RespData::VerbatimString { data, .. } => {
                self.inner.append_bulk_string(data);
            }
            crate::types::RespData::BigNumber(s) => {
                self.inner.append_string(s);
            }
            crate::types::RespData::Map(entries) => match self.policy.map_mode {
                MapMode::FlatArray => {
                    self.inner.append_array_len((entries.len() * 2) as i64);
                    for (k, v) in entries {
                        self.encode_downleveled(k);
                        self.encode_downleveled(v);
                    }
                }
                MapMode::ArrayOfPairs => {
                    self.inner.append_array_len(entries.len() as i64);
                    for (k, v) in entries {
                        self.inner.append_array_len(2);
                        self.encode_downleveled(k);
                        self.encode_downleveled(v);
                    }
                }
            },
            crate::types::RespData::Set(items) | crate::types::RespData::Push(items) => {
                self.inner.append_array_len(items.len() as i64);
                for it in items {
                    self.encode_downleveled(it);
                }
            }
        }
    }
}
