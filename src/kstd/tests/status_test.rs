use crate::kstd::status::{Code, Status};

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
