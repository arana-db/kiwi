pub mod command;
pub mod encode;
pub mod error;
pub mod parse;
pub mod types;

pub use command::{Command, CommandType, RespCommand};
pub use encode::{CmdRes, RespEncode};
pub use error::{RespError, RespResult};
pub use parse::{Parse, RespParse, RespParseResult};
pub use types::{RespData, RespType, RespVersion};

pub const CRLF: &str = "\r\n";