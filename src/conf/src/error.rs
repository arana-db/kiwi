use serde_ini::de::Error as serdeErr;
use snafu::Snafu;
use std::io;
use std::num::ParseIntError;
use std::path::PathBuf;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Could not read file {}: {}", path.display(), source))]
    ConfigFile { source: io::Error, path: PathBuf },

    #[snafu(display("Invalid configuration: {}", source))]
    InvalidConfig { source: serdeErr },

    #[snafu(display("validate fail: {}",source))]
    ValidConfigFail{ source: validator::ValidationErrors},

    #[snafu(display("Invalid memory: {}", source))]
    MemoryParse { source: MemoryParseError },
}

#[derive(Debug, Snafu)]
pub enum MemoryParseError {
    #[snafu(display("invalid data: {}", source))]
    InvalidNumber { source: ParseIntError },

    #[snafu(display("invalid memory uint: '{}'. support: B, K, M, G, T (ignore letter case)", unit))]
    UnknownUnit { unit: String },

    #[snafu(display("wrong format: '{}'. correct example: 256MB, 1.5GB, 512K", raw))]
    InvalidFormat { raw: String },

    #[snafu(display("out of range: '{}'. max : 18.44EB (2^64 bytes)", raw))]
    OutOfRange { raw: String },
}
