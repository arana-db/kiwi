// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use crate::Cmd;
use crate::auth::RequirepassProvider;

pub type CmdTable = HashMap<String, Arc<dyn Cmd>>;

#[macro_export]
macro_rules! register_cmd {
    ($cmd_table:expr, $($cmd_struct:ty),+ $(,)?) => {
        $(
            {
                let cmd = <$cmd_struct>::new();
                let cmd_name = cmd.meta().name.clone();
                let arc_cmd = Arc::new(cmd);
                $cmd_table.insert(cmd_name, arc_cmd);
            }
        )+
    };
}

#[macro_export]
macro_rules! register_group_cmd {
    ($cmd_table:expr, $($constructor:path),+ $(,)?) => {
        $(
            {
                let group_cmd = $constructor();
                let cmd_name = group_cmd.name().to_lowercase();
                $cmd_table.insert(cmd_name, Arc::new(group_cmd));
            }
        )+
    };
}

pub fn create_command_table(requirepass_provider: RequirepassProvider) -> CmdTable {
    let mut cmd_table: CmdTable = HashMap::new();

    register_cmd!(
        cmd_table,
        // String commands
        crate::append::AppendCmd,
        crate::set::SetCmd,
        crate::get::GetCmd,
        crate::incr::IncrCmd,
        crate::incrby::IncrbyCmd,
        crate::incrbyfloat::IncrbyFloatCmd,
        crate::decr::DecrCmd,
        crate::decrby::DecrbyCmd,
        crate::strlen::StrlenCmd,
        crate::getrange::GetrangeCmd,
        crate::setrange::SetrangeCmd,
        crate::setex::SetexCmd,
        crate::psetex::PsetexCmd,
        crate::setnx::SetnxCmd,
        crate::getset::GetsetCmd,
        crate::mget::MgetCmd,
        crate::mset::MsetCmd,
        crate::msetnx::MsetnxCmd,
        crate::setbit::SetbitCmd,
        crate::getbit::GetbitCmd,
        crate::bitcount::BitcountCmd,
        crate::bitpos::BitposCmd,
        crate::bitop::BitopCmd,
        // Keyspace and TTL commands
        crate::del::DelCmd,
        crate::exists::ExistsCmd,
        crate::expire::ExpireCmd,
        crate::expireat::ExpireatCmd,
        crate::pexpire::PexpireCmd,
        crate::pexpireat::PexpireatCmd,
        crate::ttl::TtlCmd,
        crate::pttl::PttlCmd,
        crate::persist::PersistCmd,
        crate::type_cmd::TypeCmd,
        crate::keys::KeysCmd,
        crate::randomkey::RandomkeyCmd,
        crate::flushdb::FlushdbCmd,
        crate::flushall::FlushallCmd,
        // Hash commands
        crate::hset::HSetCmd,
        crate::hget::HGetCmd,
        crate::hdel::HDelCmd,
        crate::hexists::HExistsCmd,
        crate::hlen::HLenCmd,
        crate::hkeys::HKeysCmd,
        crate::hmset::HMSetCmd,
        crate::hmget::HMGetCmd,
        crate::hgetall::HGetAllCmd,
        crate::hvals::HValsCmd,
        crate::hincrby::HIncrByCmd,
        crate::hincrbyfloat::HIncrByFloatCmd,
        crate::hsetnx::HSetNXCmd,
        crate::hstrlen::HStrLenCmd,
        crate::hscan::HScanCmd,
        // List commands
        crate::list::LPushCmd,
        crate::list::RPushCmd,
        crate::list::LPopCmd,
        crate::list::RPopCmd,
        crate::list::LLenCmd,
        crate::list::LIndexCmd,
        crate::list::LRangeCmd,
        crate::list::LSetCmd,
        crate::list::LTrimCmd,
        crate::list::LRemCmd,
        crate::list::LPushxCmd,
        crate::list::RPushxCmd,
        crate::list::LInsertCmd,
        crate::list::RPoplpushCmd,
        // Admin commands
        crate::admin::InfoCmd,
        crate::admin::ConfigCmd,
        // Set commands
        crate::sadd::SaddCmd,
        crate::scard::ScardCmd,
        crate::sdiff::SdiffCmd,
        crate::sdiffstore::SdiffstoreCmd,
        crate::sinter::SinterCmd,
        crate::sinterstore::SinterstoreCmd,
        crate::sismember::SismemberCmd,
        crate::smembers::SmembersCmd,
        crate::smove::SmoveCmd,
        crate::spop::SpopCmd,
        crate::srandmember::SrandmemberCmd,
        crate::srem::SremCmd,
        crate::sscan::SscanCmd,
        crate::sunion::SunionCmd,
        crate::sunionstore::SunionstoreCmd,
        // ZSet commands
        crate::zadd::ZaddCmd,
        crate::zcard::ZcardCmd,
        crate::zcount::ZcountCmd,
        crate::zincrby::ZincrbyCmd,
        crate::zinterstore::ZinterstoreCmd,
        crate::zlexcount::ZlexcountCmd,
        crate::zrange::ZrangeCmd,
        crate::zrangebylex::ZrangebylexCmd,
        crate::zrangebyscore::ZrangebyscoreCmd,
        crate::zrank::ZrankCmd,
        crate::zrem::ZremCmd,
        crate::zremrangebylex::ZremrangebylexCmd,
        crate::zremrangebyrank::ZremrangebyrankCmd,
        crate::zremrangebyscore::ZremrangebyscoreCmd,
        crate::zrevrange::ZrevrangeCmd,
        crate::zrevrangebyscore::ZrevrangebyscoreCmd,
        crate::zrevrank::ZrevrankCmd,
        crate::zscan::ZscanCmd,
        crate::zscore::ZscoreCmd,
        crate::zunionstore::ZunionstoreCmd,
        // connection commands
        crate::hello::HelloCmd,
        crate::ping::PingCmd,
    );

    // AuthCmd with requirepass provider
    {
        let auth_cmd = crate::auth::AuthCmd::new(requirepass_provider);
        let cmd_name = auth_cmd.meta().name.clone();
        cmd_table.insert(cmd_name, Arc::new(auth_cmd));
    }

    register_group_cmd!(
        cmd_table,
        crate::group_client::new_client_group_cmd,
        // TODO: add more group commands...
    );

    cmd_table
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use client::{Client, StreamTrait};
    use resp::RespData;
    use storage::storage::Storage;

    use super::create_command_table;

    struct TestStream;

    #[async_trait::async_trait]
    impl StreamTrait for TestStream {
        async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }

        async fn write(&mut self, _data: &[u8]) -> Result<usize, std::io::Error> {
            Ok(0)
        }
    }

    #[test]
    fn hello_command_returns_resp3_handshake() {
        let table = create_command_table(Arc::new(|| None));
        let command = table.get("hello").expect("HELLO should be registered");
        let client = Client::new(Box::new(TestStream));
        client.set_cmd_name(b"hello");
        client.set_argv(&[b"hello".to_vec(), b"3".to_vec()]);

        command.execute(&client, Arc::new(Storage::new(1, 0)));

        assert_eq!(
            client.take_reply(),
            RespData::Map(vec![
                (
                    RespData::BulkString(Some(Bytes::from("server"))),
                    RespData::BulkString(Some(Bytes::from("kiwi"))),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("version"))),
                    RespData::BulkString(Some(Bytes::from("1.0.0"))),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("proto"))),
                    RespData::Integer(3),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("id"))),
                    RespData::Integer(1),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("mode"))),
                    RespData::BulkString(Some(Bytes::from("standalone"))),
                ),
                (
                    RespData::BulkString(Some(Bytes::from("role"))),
                    RespData::BulkString(Some(Bytes::from("master"))),
                ),
            ])
        );
    }
}
