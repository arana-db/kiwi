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

use std::sync::Arc;

use client::Client;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

use super::{MissingError, integer_reply, storage_error_reply};

crate::define_vector_command!(
    VCardCmd,
    "vcard",
    2, // VCARD key
    CmdFlags::READONLY | CmdFlags::FAST,
    AclCategory::KEYSPACE | AclCategory::READ
);

impl Cmd for VCardCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, client: &Client) -> bool {
        super::set_command_key(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let reply = match storage.vcard(&client.key()) {
            Ok(count) => integer_reply(count),
            Err(error) => storage_error_reply(error, MissingError::Key),
        };
        client.set_reply(reply);
    }
}
