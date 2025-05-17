/// Usage:
///    ```rust
///    use bytes::Bytes;
///    use kiwi_rs::resp::{CmdRes, Parse, RespEncode, RespParse, RespParseResult, RespResult, RespVersion};
///    use kiwi_rs::resp::encode::RespEncoder;
///
///    let mut parser = RespParse::new(RespVersion::RESP2);
///    let mut encoder = RespEncoder::new(RespVersion::RESP2);
///
///    let chunks = vec![
///       "*5\r\n$5\r\nhmget",
///       "\r\n$5\r\nfruit\r\n$5\r\n",
///       "apple\r\n$6\r\nbanana\r\n",
///       "$10\r\nwatermelon\r\n"
///   ];
///
///   for chunk in chunks {
///       println!("parsing: {}", chunk);
///       match parser.parse(Bytes::from(chunk)) {
///           RespParseResult::Complete(data) => {
///               println!("complete cmd: {:?}", data);
///
///                while let Some(cmd_res) = parser.next_command() {
///                    match cmd_res {
///                        Ok(cmd) => {
///                            println!("parsed cmd: {:?}", cmd);
///                        },
///                        Err(e) => {
///                            println!("cmd err: {:?}", e);
///                        }
///                    }
///                }
///            },
///            RespParseResult::Incomplete => {
///                println!("incomplete...");
///            },
///            RespParseResult::Error(err) => {
///                println!("parse err: {}", err);
///                encoder.clear().set_res(CmdRes::ErrOther, &format!("{}", err));
///                return;
///            },
///        }
///    }
///
///    println!("full cmd:");
///    while let Some(cmd_res) = parser.next_command() {
///        match cmd_res {
///            Ok(cmd) => {
///                println!("cmd: {:?}", cmd);
///            },
///            Err(e) => {
///                println!("err: {:?}", e);
///            }
///        }
///    }
///    ```
///
pub mod command;
pub mod encode;
pub mod error;
pub mod parse;
mod tests;
pub mod types;

pub use command::{Command, CommandType, RespCommand};
pub use encode::{CmdRes, RespEncode};
pub use error::{RespError, RespResult};
pub use parse::{Parse, RespParse, RespParseResult};
pub use types::{RespData, RespType, RespVersion};

pub const CRLF: &str = "\r\n";
