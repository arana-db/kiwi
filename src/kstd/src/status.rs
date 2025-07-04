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
pub struct Status {
    code: Code,
    message: String,
}

/// TODO: remove allow dead code.
#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum Code {
    Ok,
    Timeout,
    Busy,
}

/// TODO: remove allow dead code.
#[allow(dead_code)]
impl Status {
    // Create a success status.
    pub fn ok() -> Self {
        Self::create_status(Code::Ok, None)
    }

    pub fn timeout(msg: &str) -> Self {
        Self::create_status(Code::Timeout, Some(msg))
    }

    pub fn busy(msg: &str) -> Self {
        Self::create_status(Code::Busy, Some(msg))
    }

    fn create_status(code: Code, msg: Option<&str>) -> Self {
        let message = msg.unwrap_or("").to_string();
        Status { code, message }
    }

    pub fn is_ok(&self) -> bool {
        self.code == Code::Ok
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_ok() {
        let status = Status::ok();
        assert_eq!(status.code, Code::Ok);
        assert_eq!(status.message, "");
    }

    #[test]
    fn test_status_timeout() {
        let status = Status::timeout("Connection timeout");
        assert_eq!(status.code, Code::Timeout);
        assert_eq!(status.message, "Connection timeout");
    }

    #[test]
    fn test_empty_timeout_message() {
        let status = Status::timeout("");
        assert_eq!(status.code, Code::Timeout);
        assert_eq!(status.message, "");
    }

    #[test]
    fn test_to_string() {
        let status = Status::timeout("Operation timeout");
        assert_eq!(status.to_string(), "Timeout: Operation timeout");
    }

    #[test]
    fn test_display_trait() {
        let status = Status::ok();
        assert_eq!(format!("{}", status), "Ok: ");

        let status = Status::timeout("Request timeout");
        assert_eq!(format!("{}", status), "Timeout: Request timeout");
    }

    #[test]
    fn test_debug_trait() {
        let status = Status::ok();
        assert!(format!("{:?}", status).contains("Ok"));

        let status = Status::timeout("Network timeout");
        assert!(format!("{:?}", status).contains("Timeout"));
        assert!(format!("{:?}", status).contains("Network timeout"));
    }
}
