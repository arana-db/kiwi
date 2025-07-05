use bytes::Bytes;
use std::fmt;
use std::str::FromStr;

use crate::error::RespError;
use crate::types::RespData;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    // Keys
    Del,
    Exists,
    Expire,
    ExpireAt,
    Keys,
    Persist,
    PExpire,
    PExpireAt,
    PTtl,
    Rename,
    RenameNx,
    Scan,
    Touch,
    Ttl,
    Type,
    Unlink,

    // Strings
    Append,
    BitCount,
    BitOp,
    BitPos,
    Decr,
    DecrBy,
    Get,
    GetBit,
    GetRange,
    GetSet,
    Incr,
    IncrBy,
    IncrByFloat,
    MGet,
    MSet,
    MSetNx,
    PSetEx,
    Set,
    SetBit,
    SetEx,
    SetNx,
    SetRange,
    StrLen,

    // Lists
    BLPop,
    BRPop,
    BRPopLPush,
    LIndex,
    LInsert,
    LLen,
    LPop,
    LPush,
    LPushX,
    LRange,
    LRem,
    LSet,
    LTrim,
    RPop,
    RPopLPush,
    RPush,
    RPushX,

    // Sets
    SAdd,
    SCard,
    SDiff,
    SDiffStore,
    SInter,
    SInterStore,
    SIsMember,
    SMembers,
    SMove,
    SPop,
    SRandMember,
    SRem,
    SScan,
    SUnion,
    SUnionStore,

    // Sorted Sets
    ZAdd,
    ZCard,
    ZCount,
    ZIncrBy,
    ZInterStore,
    ZLexCount,
    ZRange,
    ZRangeByLex,
    ZRangeByScore,
    ZRank,
    ZRem,
    ZRemRangeByLex,
    ZRemRangeByRank,
    ZRemRangeByScore,
    ZRevRange,
    ZRevRangeByLex,
    ZRevRangeByScore,
    ZRevRank,
    ZScan,
    ZScore,
    ZUnionStore,

    // Hashes
    HDel,
    HExists,
    HGet,
    HGetAll,
    HIncrBy,
    HIncrByFloat,
    HKeys,
    HLen,
    HMGet,
    HMSet,
    HScan,
    HSet,
    HSetNx,
    HStrLen,
    HVals,

    // Server
    Auth,
    Echo,
    FlushAll,
    FlushDb,
    Info,
    Ping,
    Select,

    // Transactions
    Discard,
    Exec,
    Multi,
    Unwatch,
    Watch,

    // Pub/Sub
    PSubscribe,
    Publish,
    PUnsubscribe,
    Subscribe,
    Unsubscribe,

    // Connection
    Quit,

    // Unknown command
    Unknown,
}

impl FromStr for CommandType {
    type Err = RespError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            // Keys
            "DEL" => Ok(CommandType::Del),
            "EXISTS" => Ok(CommandType::Exists),
            "EXPIRE" => Ok(CommandType::Expire),
            "EXPIREAT" => Ok(CommandType::ExpireAt),
            "KEYS" => Ok(CommandType::Keys),
            "PERSIST" => Ok(CommandType::Persist),
            "PEXPIRE" => Ok(CommandType::PExpire),
            "PEXPIREAT" => Ok(CommandType::PExpireAt),
            "PTTL" => Ok(CommandType::PTtl),
            "RENAME" => Ok(CommandType::Rename),
            "RENAMENX" => Ok(CommandType::RenameNx),
            "SCAN" => Ok(CommandType::Scan),
            "TOUCH" => Ok(CommandType::Touch),
            "TTL" => Ok(CommandType::Ttl),
            "TYPE" => Ok(CommandType::Type),
            "UNLINK" => Ok(CommandType::Unlink),

            // Strings
            "APPEND" => Ok(CommandType::Append),
            "BITCOUNT" => Ok(CommandType::BitCount),
            "BITOP" => Ok(CommandType::BitOp),
            "BITPOS" => Ok(CommandType::BitPos),
            "DECR" => Ok(CommandType::Decr),
            "DECRBY" => Ok(CommandType::DecrBy),
            "GET" => Ok(CommandType::Get),
            "GETBIT" => Ok(CommandType::GetBit),
            "GETRANGE" => Ok(CommandType::GetRange),
            "GETSET" => Ok(CommandType::GetSet),
            "INCR" => Ok(CommandType::Incr),
            "INCRBY" => Ok(CommandType::IncrBy),
            "INCRBYFLOAT" => Ok(CommandType::IncrByFloat),
            "MGET" => Ok(CommandType::MGet),
            "MSET" => Ok(CommandType::MSet),
            "MSETNX" => Ok(CommandType::MSetNx),
            "PSETEX" => Ok(CommandType::PSetEx),
            "SET" => Ok(CommandType::Set),
            "SETBIT" => Ok(CommandType::SetBit),
            "SETEX" => Ok(CommandType::SetEx),
            "SETNX" => Ok(CommandType::SetNx),
            "SETRANGE" => Ok(CommandType::SetRange),
            "STRLEN" => Ok(CommandType::StrLen),

            // Lists
            "BLPOP" => Ok(CommandType::BLPop),
            "BRPOP" => Ok(CommandType::BRPop),
            "BRPOPLPUSH" => Ok(CommandType::BRPopLPush),
            "LINDEX" => Ok(CommandType::LIndex),
            "LINSERT" => Ok(CommandType::LInsert),
            "LLEN" => Ok(CommandType::LLen),
            "LPOP" => Ok(CommandType::LPop),
            "LPUSH" => Ok(CommandType::LPush),
            "LPUSHX" => Ok(CommandType::LPushX),
            "LRANGE" => Ok(CommandType::LRange),
            "LREM" => Ok(CommandType::LRem),
            "LSET" => Ok(CommandType::LSet),
            "LTRIM" => Ok(CommandType::LTrim),
            "RPOP" => Ok(CommandType::RPop),
            "RPOPLPUSH" => Ok(CommandType::RPopLPush),
            "RPUSH" => Ok(CommandType::RPush),
            "RPUSHX" => Ok(CommandType::RPushX),

            // Sets
            "SADD" => Ok(CommandType::SAdd),
            "SCARD" => Ok(CommandType::SCard),
            "SDIFF" => Ok(CommandType::SDiff),
            "SDIFFSTORE" => Ok(CommandType::SDiffStore),
            "SINTER" => Ok(CommandType::SInter),
            "SINTERSTORE" => Ok(CommandType::SInterStore),
            "SISMEMBER" => Ok(CommandType::SIsMember),
            "SMEMBERS" => Ok(CommandType::SMembers),
            "SMOVE" => Ok(CommandType::SMove),
            "SPOP" => Ok(CommandType::SPop),
            "SRANDMEMBER" => Ok(CommandType::SRandMember),
            "SREM" => Ok(CommandType::SRem),
            "SSCAN" => Ok(CommandType::SScan),
            "SUNION" => Ok(CommandType::SUnion),
            "SUNIONSTORE" => Ok(CommandType::SUnionStore),

            // Sorted Sets
            "ZADD" => Ok(CommandType::ZAdd),
            "ZCARD" => Ok(CommandType::ZCard),
            "ZCOUNT" => Ok(CommandType::ZCount),
            "ZINCRBY" => Ok(CommandType::ZIncrBy),
            "ZINTERSTORE" => Ok(CommandType::ZInterStore),
            "ZLEXCOUNT" => Ok(CommandType::ZLexCount),
            "ZRANGE" => Ok(CommandType::ZRange),
            "ZRANGEBYLEX" => Ok(CommandType::ZRangeByLex),
            "ZRANGEBYSCORE" => Ok(CommandType::ZRangeByScore),
            "ZRANK" => Ok(CommandType::ZRank),
            "ZREM" => Ok(CommandType::ZRem),
            "ZREMRANGEBYLEX" => Ok(CommandType::ZRemRangeByLex),
            "ZREMRANGEBYRANK" => Ok(CommandType::ZRemRangeByRank),
            "ZREMRANGEBYSCORE" => Ok(CommandType::ZRemRangeByScore),
            "ZREVRANGE" => Ok(CommandType::ZRevRange),
            "ZREVRANGEBYLEX" => Ok(CommandType::ZRevRangeByLex),
            "ZREVRANGEBYSCORE" => Ok(CommandType::ZRevRangeByScore),
            "ZREVRANK" => Ok(CommandType::ZRevRank),
            "ZSCAN" => Ok(CommandType::ZScan),
            "ZSCORE" => Ok(CommandType::ZScore),
            "ZUNIONSTORE" => Ok(CommandType::ZUnionStore),

            // Hashes
            "HDEL" => Ok(CommandType::HDel),
            "HEXISTS" => Ok(CommandType::HExists),
            "HGET" => Ok(CommandType::HGet),
            "HGETALL" => Ok(CommandType::HGetAll),
            "HINCRBY" => Ok(CommandType::HIncrBy),
            "HINCRBYFLOAT" => Ok(CommandType::HIncrByFloat),
            "HKEYS" => Ok(CommandType::HKeys),
            "HLEN" => Ok(CommandType::HLen),
            "HMGET" => Ok(CommandType::HMGet),
            "HMSET" => Ok(CommandType::HMSet),
            "HSCAN" => Ok(CommandType::HScan),
            "HSET" => Ok(CommandType::HSet),
            "HSETNX" => Ok(CommandType::HSetNx),
            "HSTRLEN" => Ok(CommandType::HStrLen),
            "HVALS" => Ok(CommandType::HVals),

            // Server
            "AUTH" => Ok(CommandType::Auth),
            "ECHO" => Ok(CommandType::Echo),
            "FLUSHALL" => Ok(CommandType::FlushAll),
            "FLUSHDB" => Ok(CommandType::FlushDb),
            "INFO" => Ok(CommandType::Info),
            "PING" => Ok(CommandType::Ping),
            "SELECT" => Ok(CommandType::Select),

            // Transactions
            "DISCARD" => Ok(CommandType::Discard),
            "EXEC" => Ok(CommandType::Exec),
            "MULTI" => Ok(CommandType::Multi),
            "UNWATCH" => Ok(CommandType::Unwatch),
            "WATCH" => Ok(CommandType::Watch),

            // Pub/Sub
            "PSUBSCRIBE" => Ok(CommandType::PSubscribe),
            "PUBLISH" => Ok(CommandType::Publish),
            "PUNSUBSCRIBE" => Ok(CommandType::PUnsubscribe),
            "SUBSCRIBE" => Ok(CommandType::Subscribe),
            "UNSUBSCRIBE" => Ok(CommandType::Unsubscribe),

            // Connection
            "QUIT" => Ok(CommandType::Quit),

            // Unknown command
            _ => Ok(CommandType::Unknown),
        }
    }
}

impl fmt::Display for CommandType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Keys
            CommandType::Del => write!(f, "DEL"),
            CommandType::Exists => write!(f, "EXISTS"),
            CommandType::Expire => write!(f, "EXPIRE"),
            CommandType::ExpireAt => write!(f, "EXPIREAT"),
            CommandType::Keys => write!(f, "KEYS"),
            CommandType::Persist => write!(f, "PERSIST"),
            CommandType::PExpire => write!(f, "PEXPIRE"),
            CommandType::PExpireAt => write!(f, "PEXPIREAT"),
            CommandType::PTtl => write!(f, "PTTL"),
            CommandType::Rename => write!(f, "RENAME"),
            CommandType::RenameNx => write!(f, "RENAMENX"),
            CommandType::Scan => write!(f, "SCAN"),
            CommandType::Touch => write!(f, "TOUCH"),
            CommandType::Ttl => write!(f, "TTL"),
            CommandType::Type => write!(f, "TYPE"),
            CommandType::Unlink => write!(f, "UNLINK"),

            // Strings
            CommandType::Append => write!(f, "APPEND"),
            CommandType::BitCount => write!(f, "BITCOUNT"),
            CommandType::BitOp => write!(f, "BITOP"),
            CommandType::BitPos => write!(f, "BITPOS"),
            CommandType::Decr => write!(f, "DECR"),
            CommandType::DecrBy => write!(f, "DECRBY"),
            CommandType::Get => write!(f, "GET"),
            CommandType::GetBit => write!(f, "GETBIT"),
            CommandType::GetRange => write!(f, "GETRANGE"),
            CommandType::GetSet => write!(f, "GETSET"),
            CommandType::Incr => write!(f, "INCR"),
            CommandType::IncrBy => write!(f, "INCRBY"),
            CommandType::IncrByFloat => write!(f, "INCRBYFLOAT"),
            CommandType::MGet => write!(f, "MGET"),
            CommandType::MSet => write!(f, "MSET"),
            CommandType::MSetNx => write!(f, "MSETNX"),
            CommandType::PSetEx => write!(f, "PSETEX"),
            CommandType::Set => write!(f, "SET"),
            CommandType::SetBit => write!(f, "SETBIT"),
            CommandType::SetEx => write!(f, "SETEX"),
            CommandType::SetNx => write!(f, "SETNX"),
            CommandType::SetRange => write!(f, "SETRANGE"),
            CommandType::StrLen => write!(f, "STRLEN"),

            // Lists
            CommandType::BLPop => write!(f, "BLPOP"),
            CommandType::BRPop => write!(f, "BRPOP"),
            CommandType::BRPopLPush => write!(f, "BRPOPLPUSH"),
            CommandType::LIndex => write!(f, "LINDEX"),
            CommandType::LInsert => write!(f, "LINSERT"),
            CommandType::LLen => write!(f, "LLEN"),
            CommandType::LPop => write!(f, "LPOP"),
            CommandType::LPush => write!(f, "LPUSH"),
            CommandType::LPushX => write!(f, "LPUSHX"),
            CommandType::LRange => write!(f, "LRANGE"),
            CommandType::LRem => write!(f, "LREM"),
            CommandType::LSet => write!(f, "LSET"),
            CommandType::LTrim => write!(f, "LTRIM"),
            CommandType::RPop => write!(f, "RPOP"),
            CommandType::RPopLPush => write!(f, "RPOPLPUSH"),
            CommandType::RPush => write!(f, "RPUSH"),
            CommandType::RPushX => write!(f, "RPUSHX"),

            // Sets
            CommandType::SAdd => write!(f, "SADD"),
            CommandType::SCard => write!(f, "SCARD"),
            CommandType::SDiff => write!(f, "SDIFF"),
            CommandType::SDiffStore => write!(f, "SDIFFSTORE"),
            CommandType::SInter => write!(f, "SINTER"),
            CommandType::SInterStore => write!(f, "SINTERSTORE"),
            CommandType::SIsMember => write!(f, "SISMEMBER"),
            CommandType::SMembers => write!(f, "SMEMBERS"),
            CommandType::SMove => write!(f, "SMOVE"),
            CommandType::SPop => write!(f, "SPOP"),
            CommandType::SRandMember => write!(f, "SRANDMEMBER"),
            CommandType::SRem => write!(f, "SREM"),
            CommandType::SScan => write!(f, "SSCAN"),
            CommandType::SUnion => write!(f, "SUNION"),
            CommandType::SUnionStore => write!(f, "SUNIONSTORE"),

            // Sorted Sets
            CommandType::ZAdd => write!(f, "ZADD"),
            CommandType::ZCard => write!(f, "ZCARD"),
            CommandType::ZCount => write!(f, "ZCOUNT"),
            CommandType::ZIncrBy => write!(f, "ZINCRBY"),
            CommandType::ZInterStore => write!(f, "ZINTERSTORE"),
            CommandType::ZLexCount => write!(f, "ZLEXCOUNT"),
            CommandType::ZRange => write!(f, "ZRANGE"),
            CommandType::ZRangeByLex => write!(f, "ZRANGEBYLEX"),
            CommandType::ZRangeByScore => write!(f, "ZRANGEBYSCORE"),
            CommandType::ZRank => write!(f, "ZRANK"),
            CommandType::ZRem => write!(f, "ZREM"),
            CommandType::ZRemRangeByLex => write!(f, "ZREMRANGEBYLEX"),
            CommandType::ZRemRangeByRank => write!(f, "ZREMRANGEBYRANK"),
            CommandType::ZRemRangeByScore => write!(f, "ZREMRANGEBYSCORE"),
            CommandType::ZRevRange => write!(f, "ZREVRANGE"),
            CommandType::ZRevRangeByLex => write!(f, "ZREVRANGEBYLEX"),
            CommandType::ZRevRangeByScore => write!(f, "ZREVRANGEBYSCORE"),
            CommandType::ZRevRank => write!(f, "ZREVRANK"),
            CommandType::ZScan => write!(f, "ZSCAN"),
            CommandType::ZScore => write!(f, "ZSCORE"),
            CommandType::ZUnionStore => write!(f, "ZUNIONSTORE"),

            // Hashes
            CommandType::HDel => write!(f, "HDEL"),
            CommandType::HExists => write!(f, "HEXISTS"),
            CommandType::HGet => write!(f, "HGET"),
            CommandType::HGetAll => write!(f, "HGETALL"),
            CommandType::HIncrBy => write!(f, "HINCRBY"),
            CommandType::HIncrByFloat => write!(f, "HINCRBYFLOAT"),
            CommandType::HKeys => write!(f, "HKEYS"),
            CommandType::HLen => write!(f, "HLEN"),
            CommandType::HMGet => write!(f, "HMGET"),
            CommandType::HMSet => write!(f, "HMSET"),
            CommandType::HScan => write!(f, "HSCAN"),
            CommandType::HSet => write!(f, "HSET"),
            CommandType::HSetNx => write!(f, "HSETNX"),
            CommandType::HStrLen => write!(f, "HSTRLEN"),
            CommandType::HVals => write!(f, "HVALS"),

            // Server
            CommandType::Auth => write!(f, "AUTH"),
            CommandType::Echo => write!(f, "ECHO"),
            CommandType::FlushAll => write!(f, "FLUSHALL"),
            CommandType::FlushDb => write!(f, "FLUSHDB"),
            CommandType::Info => write!(f, "INFO"),
            CommandType::Ping => write!(f, "PING"),
            CommandType::Select => write!(f, "SELECT"),

            // Transactions
            CommandType::Discard => write!(f, "DISCARD"),
            CommandType::Exec => write!(f, "EXEC"),
            CommandType::Multi => write!(f, "MULTI"),
            CommandType::Unwatch => write!(f, "UNWATCH"),
            CommandType::Watch => write!(f, "WATCH"),

            // Pub/Sub
            CommandType::PSubscribe => write!(f, "PSUBSCRIBE"),
            CommandType::Publish => write!(f, "PUBLISH"),
            CommandType::PUnsubscribe => write!(f, "PUNSUBSCRIBE"),
            CommandType::Subscribe => write!(f, "SUBSCRIBE"),
            CommandType::Unsubscribe => write!(f, "UNSUBSCRIBE"),

            // Connection
            CommandType::Quit => write!(f, "QUIT"),

            // Unknown command
            CommandType::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RespCommand {
    pub command_type: CommandType,

    pub args: Vec<Bytes>,

    pub is_pipeline: bool,
}

impl RespCommand {
    pub fn new(command_type: CommandType, args: Vec<Bytes>, is_pipeline: bool) -> Self {
        Self {
            command_type,
            args,
            is_pipeline,
        }
    }

    pub fn name(&self) -> String {
        self.command_type.to_string()
    }

    pub fn arg(&self, index: usize) -> Option<&Bytes> {
        self.args.get(index)
    }

    pub fn arg_count(&self) -> usize {
        self.args.len()
    }

    pub fn arg_string(&self, index: usize) -> Option<String> {
        self.arg(index)
            .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok())
    }
}

pub trait Command {
    fn to_command(&self) -> Result<RespCommand, RespError>;
}

impl Command for RespData {
    fn to_command(&self) -> Result<RespCommand, RespError> {
        match self {
            RespData::Array(Some(array)) if !array.is_empty() => {
                let command_name = array[0].as_string().ok_or_else(|| {
                    RespError::InvalidData("Command name must be a string".to_string())
                })?;

                let command_type =
                    CommandType::from_str(&command_name).unwrap_or(CommandType::Unknown);

                let args = array
                    .iter()
                    .skip(1)
                    .map(|data| {
                        data.as_bytes().ok_or_else(|| {
                            RespError::InvalidData(
                                "Command argument must be convertible to bytes".to_string(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(RespCommand::new(command_type, args, false))
            }
            RespData::Inline(parts) if !parts.is_empty() => {
                let command_name = std::str::from_utf8(&parts[0]).map_err(|_| {
                    RespError::InvalidData("Command name must be a valid UTF-8 string".to_string())
                })?;

                let command_type =
                    CommandType::from_str(command_name).unwrap_or(CommandType::Unknown);

                let args = parts.iter().skip(1).cloned().collect();

                Ok(RespCommand::new(command_type, args, false))
            }
            _ => Err(RespError::InvalidData("Invalid command format".to_string())),
        }
    }
}