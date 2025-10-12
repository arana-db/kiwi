use std::collections::VecDeque;

use bytes::Bytes;

use crate::{
    error::RespResult,
    parse::{Parse, RespParse, RespParseResult},
    traits::Decoder,
    types::{RespData, RespVersion},
};

#[derive(Default)]
pub struct Resp1Decoder {
    inner: RespParse,
    out: VecDeque<RespResult<RespData>>,
}

impl Resp1Decoder {
    pub fn new() -> Self {
        Self {
            inner: RespParse::new(RespVersion::RESP1),
            out: VecDeque::new(),
        }
    }
}

impl Decoder for Resp1Decoder {
    fn push(&mut self, data: Bytes) {
        let mut res = self.inner.parse(data);
        loop {
            match res {
                RespParseResult::Complete(d) => self.out.push_back(Ok(d)),
                RespParseResult::Error(e) => self.out.push_back(Err(e)),
                RespParseResult::Incomplete => break,
            }
            res = self.inner.parse(Bytes::new());
        }
    }

    fn next(&mut self) -> Option<RespResult<RespData>> {
        self.out.pop_front()
    }

    fn reset(&mut self) {
        self.inner.reset();
        self.out.clear();
    }

    fn version(&self) -> RespVersion {
        RespVersion::RESP1
    }
}
