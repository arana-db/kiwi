// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScanOptions {
    pub(crate) cursor: u64,
    pub(crate) pattern: Option<Vec<u8>>,
    pub(crate) count: Option<usize>,
}

pub(crate) fn parse_scan_options(
    cursor: &[u8],
    options: &[Vec<u8>],
) -> Result<ScanOptions, &'static str> {
    let cursor = std::str::from_utf8(cursor)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or("ERR invalid cursor")?;

    let mut pattern = None;
    let mut count = None;
    let mut index = 0;

    while index < options.len() {
        if options[index].eq_ignore_ascii_case(b"MATCH") {
            let value = options.get(index + 1).ok_or("ERR syntax error")?;
            pattern = Some(value.clone());
            index += 2;
        } else if options[index].eq_ignore_ascii_case(b"COUNT") {
            let value = options.get(index + 1).ok_or("ERR syntax error")?;
            count = std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|count| *count > 0)
                .ok_or("ERR value is not an integer or out of range")?
                .into();
            index += 2;
        } else {
            return Err("ERR syntax error");
        }
    }

    Ok(ScanOptions {
        cursor,
        pattern,
        count,
    })
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::{ScanOptions, parse_scan_options};

    fn args(values: &[&[u8]]) -> Vec<Vec<u8>> {
        values.iter().map(|value| value.to_vec()).collect()
    }

    #[test]
    fn parses_cursor_without_optional_arguments() {
        assert_eq!(
            parse_scan_options(b"42", &[]).unwrap(),
            ScanOptions {
                cursor: 42,
                pattern: None,
                count: None,
            }
        );
    }

    #[test]
    fn rejects_invalid_cursor_with_expected_error() {
        for cursor in [b"not-a-number".as_slice(), b"-1", b"\xff"] {
            assert_eq!(parse_scan_options(cursor, &[]), Err("ERR invalid cursor"));
        }
    }

    #[test]
    fn parses_case_insensitive_options_and_preserves_binary_pattern() {
        let options = args(&[b"mAtCh", b"\xff?", b"cOuNt", b"25"]);

        assert_eq!(
            parse_scan_options(b"7", &options).unwrap(),
            ScanOptions {
                cursor: 7,
                pattern: Some(b"\xff?".to_vec()),
                count: Some(25),
            }
        );
    }

    #[test]
    fn repeated_options_use_the_last_value() {
        let options = args(&[
            b"MATCH", b"first", b"COUNT", b"10", b"match", b"second", b"count", b"20",
        ]);

        assert_eq!(
            parse_scan_options(b"0", &options).unwrap(),
            ScanOptions {
                cursor: 0,
                pattern: Some(b"second".to_vec()),
                count: Some(20),
            }
        );
    }

    #[test]
    fn rejects_missing_option_values_and_unknown_options_as_syntax_errors() {
        for options in [
            args(&[b"MATCH"]),
            args(&[b"COUNT"]),
            args(&[b"UNKNOWN", b"value"]),
        ] {
            assert_eq!(parse_scan_options(b"0", &options), Err("ERR syntax error"));
        }
    }

    #[test]
    fn rejects_invalid_count_with_expected_error() {
        for count in [
            b"\xff".as_slice(),
            b"not-a-number",
            b"0",
            b"-1",
            b"184467440737095516160",
        ] {
            let options = args(&[b"COUNT", count]);
            assert_eq!(
                parse_scan_options(b"0", &options),
                Err("ERR value is not an integer or out of range")
            );
        }
    }
}
