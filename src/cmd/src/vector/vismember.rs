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

use bytes::Bytes;
use client::Client;
use resp::RespData;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta, impl_cmd_clone_box, impl_cmd_meta};

use super::admission::{VectorAdmissionLimits, admit_vector_request};
use super::{MissingError, error_reply, storage_error_reply};

crate::define_vector_command!(
    VIsMemberCmd,
    "vismember",
    3, // VISMEMBER key element
    CmdFlags::READONLY | CmdFlags::FAST,
    AclCategory::KEYSPACE | AclCategory::READ
);

impl Cmd for VIsMemberCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn admit_network_request(
        &self,
        argv: &[Bytes],
        limits: VectorAdmissionLimits,
    ) -> Result<(), RespData> {
        admit_vector_request(argv, limits).map_err(|error| error_reply(error.as_str()))
    }

    fn do_initial(&self, client: &Client) -> bool {
        super::set_command_key(client)
    }

    fn do_cmd(&self, client: &Client, storage: Arc<Storage>) {
        let argv = client.argv();
        let reply = match storage.vismember(&client.key(), &argv[2]) {
            Ok(is_member) => RespData::Boolean(is_member),
            Err(error) => storage_error_reply(error, MissingError::Key),
        };
        client.set_reply(reply);
    }
}
