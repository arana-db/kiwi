/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::cmd::Cmd;
use std::collections::HashMap;

pub type CmdTable = HashMap<String, Box<dyn Cmd>>;

#[macro_export]
macro_rules! register_cmd {
    ($cmd_table:expr, $($cmd_struct:ty),+ $(,)?) => {
        $(
            {
                let cmd = <$cmd_struct>::new();
                let cmd_name = cmd.meta().name.clone();
                let boxed_cmd = Box::new(cmd);
                $cmd_table.insert(cmd_name, boxed_cmd);
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
                $cmd_table.insert(cmd_name, Box::new(group_cmd));
            }
        )+
    };
}

pub fn create_command_table() -> CmdTable {
    let mut cmd_table: CmdTable = HashMap::new();

    register_cmd!(
        cmd_table,
        crate::cmd_set::SetCmd,
        crate::cmd_get::GetCmd,
        // TODO: add more commands...
    );

    register_group_cmd!(
        cmd_table,
        crate::cmd_group_client::new_client_group_cmd,
        // TODO: add more group commands...
    );

    cmd_table
}
