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
        // 直接在这里实现格式化，不再调用 to_string 方法
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
