use snafu::Snafu;
use std::path::PathBuf;

#[derive(Debug, Snafu)]
pub enum ParseConfigError {
    #[snafu(display("read config file fail {}: {}", path.display(), source))]
    ReadFileErr {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("parse config key {} fail", key))]
    ParseConfItemErr {
        key: String,
    },
    GetConfItemFail {
        type_error: TypeError,
    },
}

// 自定义错误类型
#[derive(Debug, PartialEq)]
pub enum TypeError {
    KeyNotFound,
    TypeMismatch,
}
