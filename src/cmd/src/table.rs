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

pub fn create_command_table() -> CmdTable {
    let mut cmd_table: CmdTable = HashMap::new();

    register_cmd!(
        cmd_table,
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
        // Admin commands
        crate::admin::InfoCmd,
        crate::admin::ConfigCmd,
        // TODO: add more commands...
    );

    register_group_cmd!(
        cmd_table,
        crate::group_client::new_client_group_cmd,
        crate::cluster::new_cluster_group_cmd,
        crate::cluster::new_raft_group_cmd,
        // TODO: add more group commands...
    );

    cmd_table
}
