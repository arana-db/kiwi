use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum RespError {
    #[error("Invalid RESP data: {0}")]
    InvalidData(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Incomplete data")]
    Incomplete,

    #[error("Invalid integer: {0}")]
    InvalidInteger(String),

    #[error("Invalid bulk string length: {0}")]
    InvalidBulkStringLength(String),

    #[error("Invalid array length: {0}")]
    InvalidArrayLength(String),

    #[error("Unsupported RESP type")]
    UnsupportedType,

    #[error("Unknown command: {0}")]
    UnknownCommand(String),

    #[error("Unknown subcommand: {0}")]
    UnknownSubCommand(String),

    #[error("Syntax error: {0}")]
    SyntaxError(String),

    #[error("Wrong number of arguments: {0}")]
    WrongNumberOfArguments(String),

    #[error("Unknown error: {0}")]
    UnknownError(String),
}

pub type RespResult<T> = Result<T, RespError>;