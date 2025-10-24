use std::sync::Arc;

use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

/// BITPOS key bit [start] [end] [BYTE | BIT]
///
/// Return the position of the first bit set to 1 or 0 in a string.
/// The position is returned, thinking of the string as an array of bits from left to right,
/// where the first byte's most significant bit is at position 0, the second byte's most significant bit is at position 8, and so forth.
#[derive(Clone, Default)]
pub struct BitposCmd {
    meta: CmdMeta,
}

impl BitposCmd {
    pub fn new() -> Self {
        Self {
            meta: CmdMeta {
                name: "bitpos".to_string(),
                arity: -3, // BITPOS key bit [start] [end] [BYTE | BIT]
                flags: CmdFlags::READONLY,
                acl_category: AclCategory::STRING | AclCategory::READ,
                ..Default::default()
            },
        }
    }
}

impl Cmd for BitposCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        let argv = client.argv();
        
        // Check if the number of arguments is valid
        if argv.len() < 3 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'bitpos' command"
                    .to_string()
                    .into(),
            ));
            return false;
        }

        // Use the key for routing in distributed setup
        let key = argv[1].clone();
        client.set_key(&key);

        true
    }

    /// Process the BITPOS command
    ///
    /// # Arguments
    /// * `client` - The client that sent the command
    /// * `storage` - The storage instance
    ///
    /// # Command Format
    /// BITPOS key bit [start] [end] [BYTE | BIT]
    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let key = client.key();

        // Validate minimum number of arguments
        if argv.len() < 3 {
            client.set_reply(RespData::Error(
                "ERR wrong number of arguments for 'bitpos' command"
                    .to_string()
                    .into(),
            ));
            return;
        }

        // Parse bit argument (must be 0 or 1)
        let bit: i64 = match String::from_utf8_lossy(&argv[2]).parse() {
            Ok(val) if val == 0 || val == 1 => val,
            _ => {
                client.set_reply(RespData::Error(
                    "ERR The bit argument must be 1 or 0".to_string().into(),
                ));
                return;
            }
        };

        // Parse optional arguments
        let mut start: Option<i64> = None;
        let mut end: Option<i64> = None;
        let mut is_bit_mode = false; // Default to byte mode

        let mut i = 3;
        while i < argv.len() {
            let arg = String::from_utf8_lossy(&argv[i]);
            match arg.to_uppercase().as_str() {
                "BIT" => {
                    is_bit_mode = true;
                    i += 1;
                }
                "BYTE" => {
                    is_bit_mode = false;
                    i += 1;
                }
                _ => {
                    // Try to parse as start or end position
                    match arg.parse::<i64>() {
                        Ok(val) => {
                            if start.is_none() {
                                start = Some(val);
                            } else if end.is_none() {
                                end = Some(val);
                            } else {
                                // Too many numeric arguments
                                client.set_reply(RespData::Error(
                                    "ERR syntax error".to_string().into(),
                                ));
                                return;
                            }
                            i += 1;
                        }
                        Err(_) => {
                            client.set_reply(RespData::Error(
                                "ERR value is not an integer or out of range"
                                    .to_string()
                                    .into(),
                            ));
                            return;
                        }
                    }
                }
            }
        }

        match storage.bitpos(&key, bit, start, end, is_bit_mode) {
            Ok(position) => {
                client.set_reply(RespData::Integer(position));
            }
            Err(e) => match e {
                storage::error::Error::RedisErr { ref message, .. }
                    if message.starts_with("WRONGTYPE") =>
                {
                    // RedisErr already contains the formatted message
                    client.set_reply(RespData::Error(message.clone().into()));
                }
                _ => {
                    client.set_reply(RespData::Error(format!("ERR {e}").into()));
                }
            },
        }
    }
}