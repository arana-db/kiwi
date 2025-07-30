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

use crate::base_cmd::BaseCmd;
use crate::cmd_group_client;
use crate::cmd_kv;
use std::collections::HashMap;

pub type CommandTable = HashMap<String, Box<dyn BaseCmd>>;

pub fn create_command_table() -> CommandTable {
    let mut cmd_table: CommandTable = HashMap::new();

    // TODO: use macro.

    // set command
    let set_cmd = Box::new(cmd_kv::SetCmd::new());
    cmd_table.insert("set".to_string(), set_cmd);

    // get command
    let get_cmd = Box::new(cmd_kv::GetCmd::new());
    cmd_table.insert("get".to_string(), get_cmd);

    // client group command
    let client_group_cmd = cmd_group_client::new_client_cmd();
    cmd_table.insert("client".to_string(), Box::new(client_group_cmd));

    cmd_table
}
