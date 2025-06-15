//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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
        // extend from slice, avoid copying data
        self.buffer.extend_from_slice(v);
        let mut pos = 0;
        let buf = &self.buffer;

        // phase 1: check if the buffer is start with '*'
        if buf.get(pos) != Some(&b'*') {
            return Err(ParseError::InvalidFormat);
        }
        pos += 1;

        // phase 2: parse the array element count (*<count>\r\n)
        let count_end = match buf[pos..].iter().position(|&b| b == b'\r') {
            Some(i) => pos + i,
            None => return Ok(false), // data not complete, continue to read
        };
        if count_end + 1 >= buf.len() || buf[count_end + 1] != b'\n' {
            return Err(ParseError::InvalidFormat); // invalid format, return error
        }
        let count_str = &buf[pos..count_end];

        let count = std::str::from_utf8(count_str)
            .unwrap()
            .parse::<usize>()
            .map_err(|_| ParseError::InvalidFormat)
            .expect("TODO: panic message");
        pos = count_end + 2; // move cursor to the next element

        // phase 3: parse each bulk string element ($<len>\r\n<data>\r\n)
        let mut parsed_args = Vec::with_capacity(count);
        for _ in 0..count {
            // check if the current element is a bulk string ($<len>\r\n<data>\r\n)
            if buf.get(pos) != Some(&b'$') {
                return Ok(false);
            }
            pos += 1;

            // parse the length of the string ($<len>\r\n)
            let len_end = match buf[pos..].iter().position(|&b| b == b'\r') {
                Some(i) => pos + i,
                None => return Ok(false), // data not complete, continue to read
            };
            if len_end + 1 >= buf.len() || buf[len_end + 1] != b'\n' {
                return Ok(false);
            }
            let len_str = &buf[pos..len_end];
            let len = std::str::from_utf8(len_str)
                .unwrap()
                .parse::<usize>()
                .expect("TODO: panic message");
            pos = len_end + 2; // move cursor to the next element

            if pos + len + 2 > buf.len() {
                return Ok(false); // data not complete, continue to read
            }
            if buf[pos + len] != b'\r' || buf[pos + len + 1] != b'\n' {
                return Err(ParseError::InvalidFormat); // invalid format, return error
            }

            // push the data to the parsed_args
            parsed_args.push(buf[pos..pos + len].to_vec());
            pos += len + 2; // move cursor to the next element
        }

        // phase 4: move the parsed data to the args, and clear the buffer
        self.args = parsed_args;
        self.buffer = self.buffer.drain(pos..).collect();
        Ok(true)
    }
}
