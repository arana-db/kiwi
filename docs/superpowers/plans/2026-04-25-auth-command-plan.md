# AUTH 命令实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Redis 兼容的 `AUTH <password>` 命令，支持单密码认证，未认证客户端受限访问

**架构：** 在 Config 中添加 `requirepass`，Client 添加认证状态，命令执行入口添加 `NO_AUTH` flag 白名单检查，新增 `AuthCmd` 实现密码验证

**技术栈：** Rust, tokio, bitflags

---

## 文件总览

| 操作 | 文件 | 职责 |
|------|------|------|
| 修改 | `conf/src/config.rs` | 新增 `requirepass: Option<String>` |
| 修改 | `client/src/lib.rs` | 新增 `authenticated` 状态 + 访问方法 |
| 新增 | `cmd/src/auth.rs` | `AuthCmd` 实现 |
| 修改 | `cmd/src/lib.rs` | 新增 `pub mod auth` |
| 修改 | `cmd/src/table.rs` | 注册 `AuthCmd` |
| 修改 | `net/src/handle.rs` | `handle_command` 添加认证检查 |
| 修改 | `net/src/network_handle.rs` | `process_command_batch` 添加认证检查 |

---

### Task 1: Config 新增 requirepass

**Files:**
- Modify: `conf/src/config.rs`

- [ ] **Step 1.1: 添加 requirepass 字段到 Config 结构体**

在 `Config` 结构体中 `raft: Option<RaftClusterConfig>` 之前添加：

```rust
// conf/src/config.rs — Config 结构体中新增字段
pub requirepass: Option<String>,
```

- [ ] **Step 1.2: 添加 requirepass 默认值**

在 `impl Default for Config` 的 `Default::default()` 中，`raft: None` 之前添加：

```rust
requirepass: None,
```

- [ ] **Step 1.3: 添加配置解析**

在 `Config::load` 的 `for (key, value) in config_map` match 中，`"db-path" =>` 分支之后、`_ =>` 之前添加：

```rust
"requirepass" => {
    config.requirepass = Some(value);
}
```

- [ ] **Step 1.4: 编译验证**

Run: `cargo check 2>&1`
Expected: 编译通过，无错误

- [ ] **Step 1.5: 提交**

```bash
git add conf/src/config.rs
git commit -m "feat(config): add requirepass field for authentication"
```

---

### Task 2: Client 添加认证状态

**Files:**
- Modify: `client/src/lib.rs`

- [ ] **Step 2.1: 在 ClientContext 添加 authenticated 字段**

修改 `ClientContext` 结构体，在 `reply` 字段之后添加：

```rust
// client/src/lib.rs — ClientContext 结构体
authenticated: bool,
```

- [ ] **Step 2.2: 修改 Client::new 初始化 authenticated**

将 `Client::new` 中 `ClientContext` 的初始化修改为：

```rust
// client/src/lib.rs — Client::new 中 ClientContext 初始化
ctx: parking_lot::Mutex::new(ClientContext {
    argv: Vec::default(),
    name: Arc::new(Vec::default()),
    cmd_name: Arc::new(Vec::default()),
    key: Vec::default(),
    reply: RespData::default(),
    authenticated: true,  // 默认已认证；是否禁用由外部通过 set_authenticated 控制
}),
```

说明：默认 `authenticated: true`（即无密码时直接可用），如果配置了 `requirepass`，server 层会在连接建立后调用 `set_authenticated(false)`。

- [ ] **Step 2.3: 添加 is_authenticated 方法**

在 `impl Client` 中 `take_reply` 方法之后添加：

```rust
// client/src/lib.rs — impl Client 中新增
pub fn is_authenticated(&self) -> bool {
    let ctx = self.ctx.lock();
    ctx.authenticated
}

pub fn set_authenticated(&self, val: bool) {
    let mut ctx = self.ctx.lock();
    ctx.authenticated = val;
}
```

- [ ] **Step 2.4: 编译验证**

Run: `cargo check 2>&1`
Expected: 编译通过

- [ ] **Step 2.5: 提交**

```bash
git add client/src/lib.rs
git commit -m "feat(client): add authenticated state for AUTH command"
```

---

### Task 3: 实现 AuthCmd

**Files:**
- Create: `cmd/src/auth.rs`
- Modify: `cmd/src/lib.rs`
- Modify: `cmd/src/table.rs`

- [ ] **Step 3.1: 创建 auth.rs**

```rust
// cmd/src/auth.rs
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
use conf::config::Config;
use resp::RespData;
use storage::storage::Storage;

use crate::{AclCategory, Cmd, CmdFlags, CmdMeta};
use crate::{impl_cmd_clone_box, impl_cmd_meta};

#[derive(Clone, Default)]
pub struct AuthCmd {
    meta: CmdMeta,
    config: Arc<Config>,
}

impl AuthCmd {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            meta: CmdMeta {
                name: "auth".to_string(),
                arity: -2,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION,
                ..Default::default()
            },
            config,
        }
    }
}

impl Cmd for AuthCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        let argv = client.argv();
        match argv.len() {
            2 => {
                let password = String::from_utf8_lossy(&argv[1]);
                if let Some(ref requirepass) = self.config.requirepass {
                    if password.as_ref() == requirepass {
                        client.set_authenticated(true);
                        client.set_reply(RespData::SimpleString("OK".into()));
                    } else {
                        client.set_reply(RespData::Error(
                            "WRONGPASS invalid username-password pair or user is disabled.".into(),
                        ));
                    }
                } else {
                    client.set_reply(RespData::Error(
                        "ERR AUTH called without any password configured".into(),
                    ));
                }
            }
            3 => {
                // AUTH <user> <pass> — ACL 形式，预留但不实现
                client.set_reply(RespData::Error(
                    "ERR ACL authentication is not supported".into(),
                ));
            }
            _ => {
                client.set_reply(RespData::Error(
                    "ERR wrong number of arguments for 'auth' command".into(),
                ));
            }
        }
    }
}
```

- [ ] **Step 3.2: 在 lib.rs 中导出 auth 模块**

在 `cmd/src/lib.rs` 的模块声明区域（`pub mod admin;` 之后）添加：

```rust
pub mod auth;
```

- [ ] **Step 3.3: 在 table.rs 中注册 AuthCmd**

在 `cmd/src/table.rs` 的 `create_command_table` 函数中，`crate::ping::PingCmd` 之后添加。由于 `AuthCmd` 需要 `Arc<Config>` 参数，不能用 `register_cmd!` 宏，需要手动注册：

```rust
// cmd/src/table.rs — create_command_table 函数中，ping 注册之后
{
    use conf::config::Config;
    let config = Arc::new(Config::default());
    let auth_cmd = crate::auth::AuthCmd::new(config);
    let cmd_name = auth_cmd.meta().name.clone();
    cmd_table.insert(cmd_name, Arc::new(auth_cmd));
}
```

注意：这里使用 `Config::default()` 是为了让命令表可以独立创建。实际运行时，认证检查依赖于 Client 的 `authenticated` 状态，而 `AuthCmd` 中的 `config` 用于读取 `requirepass`。这意味着命令表中的 `AuthCmd` 使用的配置必须和实际 server 配置一致 — 后续需要改为注入方式（见 Task 6）。

- [ ] **Step 3.4: 编译验证**

Run: `cargo check 2>&1`
Expected: 编译通过

- [ ] **Step 3.5: 提交**

```bash
git add cmd/src/auth.rs cmd/src/lib.rs cmd/src/table.rs
git commit -m "feat(cmd): add AUTH command implementation"
```

---

### Task 4: handle.rs 添加认证检查

**Files:**
- Modify: `net/src/handle.rs`
- Modify: `cmd/src/table.rs`（改进 AuthCmd 配置注入）

- [ ] **Step 4.1: 修改 handle_command 添加认证检查**

在 `net/src/handle.rs` 的 `handle_command` 函数中，`let cmd_name = ...` 之后、`if let Some(cmd) = cmd_table.get(&cmd_name)` 之前添加：

```rust
// net/src/handle.rs — handle_command 函数中
if !client.is_authenticated() {
    let cmd_name_lower = String::from_utf8_lossy(&client.cmd_name()).to_lowercase();
    if let Some(cmd) = cmd_table.get(&cmd_name_lower) {
        if !cmd.has_flag(cmd::CmdFlags::NO_AUTH) {
            client.set_reply(RespData::Error(
                "NOAUTH Authentication required.".into(),
            ));
            return;
        }
    }
}
```

同时在文件头部添加 imports（已有 `use cmd::table::CmdTable;`，需额外添加）：

```rust
use cmd::CmdFlags;
```

- [ ] **Step 4.2: 编译验证**

Run: `cargo check 2>&1`
Expected: 编译通过

- [ ] **Step 4.3: 提交**

```bash
git add net/src/handle.rs
git commit -m "feat(net): add auth check in handle_command"
```

---

### Task 5: network_handle.rs 添加认证检查

**Files:**
- Modify: `net/src/network_handle.rs`

- [ ] **Step 5.1: 在 process_command_batch 添加认证检查**

在 `net/src/network_handle.rs` 的 `process_command_batch` 函数中，`client.set_cmd_name(&command.cmd_name);` 和 `client.set_argv(&command.argv);` 之后、`handle_network_command(...)` 之前添加认证检查：

```rust
// net/src/network_handle.rs — process_command_batch 函数中
// Auth check: deny non-NO_AUTH commands when not authenticated
{
    let cmd_name_str = String::from_utf8_lossy(&command.cmd_name).to_lowercase();
    if !client.is_authenticated() {
        if let Some(cmd) = cmd_table.get(&cmd_name_str) {
            if !cmd.has_flag(cmd::CmdFlags::NO_AUTH) {
                client.set_reply(RespData::Error(
                    "NOAUTH Authentication required.".into(),
                ));
                // Still send the response
                let response = client.take_reply();
                let mut encoder = RespEncoder::new(RespVersion::RESP2);
                encoder.encode_resp_data(&response);
                let _ = client.write(encoder.get_response().as_ref()).await;
                continue;
            }
        }
    }
}
```

在文件头部添加 `use cmd::CmdFlags;`（文件顶部已有 `use cmd::table::CmdTable;`）。

- [ ] **Step 5.2: 编译验证**

Run: `cargo check 2>&1`
Expected: 编译通过

- [ ] **Step 5.3: 提交**

```bash
git add net/src/network_handle.rs
git commit -m "feat(net): add auth check in process_command_batch"
```

---

### Task 6: 连接初始化时禁用认证（有 requirepass 时）

**Files:**
- Modify: `net/src/network_server.rs`
- Modify: `net/src/network_handle.rs`
- Modify: `cmd/src/table.rs`
- Modify: `cmd/src/auth.rs`

- [ ] **Step 6.1: 通过 ServerFactory/NetworkServer 传递 Config 给连接处理**

当前 `NetworkServer::new` 和 `process_connection_with_storage_client` 不接收 Config。需要在调用链中传递 `requirepass`。最简单的方式是在 `NetworkServer` 和 `process_connection_with_storage_client` 中新增一个 `requirepass: Option<String>` 参数。

但更简洁的方案是：在 `Client::new()` 之后，如果 `requirepass` 为 `Some`，立即调用 `client.set_authenticated(false)`。

在 `net/src/network_server.rs` 的 `run` 方法中，`let client = Arc::new(Client::new(Box::new(stream)));` 之后，添加：

```rust
// net/src/network_server.rs — run 方法中 Client 创建之后
// AuthClient 不会被 NetworkResources 持有，需要通过闭包传递
let requirepass = self.requirepass.clone();
```

这需要 `NetworkServer` 新增 `requirepass: Option<String>` 字段。

修改 `NetworkServer` 结构体，添加字段：

```rust
// net/src/network_server.rs — NetworkServer 结构体
requirepass: Option<String>,
```

修改 `NetworkServer::new` 签名，添加 `requirepass` 参数：

```rust
// net/src/network_server.rs — NetworkServer::new
pub fn new(
    addr: Option<String>,
    storage_client: Arc<StorageClient>,
    cmd_table: Arc<CmdTable>,
    executor: Arc<CmdExecutor>,
    requirepass: Option<String>,
) -> Result<Self, Box<dyn Error>> {
    let pool_config = default_network_pool_config();

    Ok(Self {
        addr: addr.unwrap_or("127.0.0.1:7379".to_string()),
        storage_client: storage_client.clone(),
        cmd_table: cmd_table.clone(),
        executor: executor.clone(),
        connection_pool: Arc::new(ConnectionPool::new(pool_config)),
        requirepass,
    })
}
```

同样修改 `with_pool_config` 签名，添加 `requirepass` 参数。

- [ ] **Step 6.2: 在连接建立时设置 authenticated**

在 `run` 方法中 `let client = Arc::new(Client::new(Box::new(stream)));` 之后添加：

```rust
// net/src/network_server.rs — run 方法中
if self.requirepass.is_some() {
    client.set_authenticated(false);
}
```

- [ ] **Step 6.3: 修改 AuthCmd 使用共享 Config**

`AuthCmd` 需要读取 `requirepass`。当前 Task 3 中使用 `Arc<Config>`，但命令表中的 Config 是 `Config::default()`。

改进方案：`AuthCmd` 通过闭包或回调获取 `requirepass`，而非持有整个 Config。

修改 `AuthCmd`：

```rust
// cmd/src/auth.rs
use std::sync::Arc;
use std::sync::Mutex;

// ... (前面的结构体定义替换为)

type RequirepassProvider = Arc<dyn Fn() -> Option<String> + Send + Sync>;

#[derive(Clone)]
pub struct AuthCmd {
    meta: CmdMeta,
    requirepass_provider: RequirepassProvider,
}

impl Default for AuthCmd {
    fn default() -> Self {
        Self {
            meta: CmdMeta {
                name: "auth".to_string(),
                arity: -2,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION,
                ..Default::default()
            },
            requirepass_provider: Arc::new(|| None),
        }
    }
}

impl AuthCmd {
    pub fn new(provider: RequirepassProvider) -> Self {
        Self {
            meta: CmdMeta {
                name: "auth".to_string(),
                arity: -2,
                flags: CmdFlags::NO_AUTH | CmdFlags::FAST,
                acl_category: AclCategory::CONNECTION,
                ..Default::default()
            },
            requirepass_provider: provider,
        }
    }
}

impl Cmd for AuthCmd {
    impl_cmd_meta!();
    impl_cmd_clone_box!();

    fn do_initial(&self, _client: &Client) -> bool {
        true
    }

    fn do_cmd(&self, client: &Client, _storage: Arc<Storage>) {
        let argv = client.argv();
        match argv.len() {
            2 => {
                let password = String::from_utf8_lossy(&argv[1]);
                if let Some(requirepass) = (self.requirepass_provider)() {
                    if password.as_ref() == &requirepass {
                        client.set_authenticated(true);
                        client.set_reply(RespData::SimpleString("OK".into()));
                    } else {
                        client.set_reply(RespData::Error(
                            "WRONGPASS invalid username-password pair or user is disabled.".into(),
                        ));
                    }
                } else {
                    client.set_reply(RespData::Error(
                        "ERR AUTH called without any password configured".into(),
                    ));
                }
            }
            3 => {
                client.set_reply(RespData::Error(
                    "ERR ACL authentication is not supported".into(),
                ));
            }
            _ => {
                client.set_reply(RespData::Error(
                    "ERR wrong number of arguments for 'auth' command".into(),
                ));
            }
        }
    }
}
```

- [ ] **Step 6.4: 修改命令表创建**

修改 `cmd/src/table.rs` 的 `create_command_table` 函数签名和 AuthCmd 注册：

```rust
// cmd/src/table.rs — create_command_table 函数签名修改
pub fn create_command_table(requirepass_provider: crate::auth::RequirepassProvider) -> CmdTable {
    let mut cmd_table: CmdTable = HashMap::new();

    register_cmd!(
        cmd_table,
        // ... (保持原有命令不变)
        crate::ping::PingCmd,
    );

    // AuthCmd 需要 requirepass provider
    {
        let auth_cmd = crate::auth::AuthCmd::new(requirepass_provider);
        let cmd_name = auth_cmd.meta().name.clone();
        cmd_table.insert(cmd_name, Arc::new(auth_cmd));
    }

    cmd_table
}
```

在调用处（`server/src/main.rs` 或 `net` 的测试中），需要传入 provider。

在 `server/src/main.rs` 中，`create_command_table` 调用处修改。由于 main.rs 中 `config` 是局部变量，需要通过 `Arc<Config>` 持有：

```rust
// server/src/main.rs — 在 start_server 或更早处
let config = Arc::new(config); // 将 Config 包裹进 Arc
// ...
let cmd_table = Arc::new(cmd::table::create_command_table(Arc::clone(&config)));
```

需要在 `cmd/src/auth.rs` 中导出 `RequirepassProvider`：

```rust
pub type RequirepassProvider = Arc<dyn Fn() -> Option<String> + Send + Sync>;
```

- [ ] **Step 6.5: 修复测试中的 create_command_table 调用**

搜索所有 `create_command_table()` 调用，改为 `create_command_table(Arc::new(|| None))`。涉及文件：
- `net/src/network_server.rs`（测试模块）
- `net/src/network_handle.rs`（测试模块）
- 其他使用 `create_command_table` 的测试

- [ ] **Step 6.6: 编译验证**

Run: `cargo check 2>&1`
Expected: 编译通过

- [ ] **Step 6.7: 提交**

```bash
git add cmd/src/auth.rs cmd/src/table.rs net/src/network_server.rs net/src/network_handle.rs server/src/main.rs
git commit -m "feat: wire requirepass through server to AuthCmd"
```

---

### Task 7: 运行测试和验证

- [ ] **Step 7.1: 运行全部现有测试**

Run: `cargo test 2>&1`
Expected: 所有现有测试通过

- [ ] **Step 7.2: 手动验证**

启动 server 并测试 AUTH 行为：

```bash
# 无密码模式 — 所有命令可直接执行
RUST_LOG=info cargo run -- -c config.example.toml

# 有密码模式 — 需修改配置文件添加 requirepass
echo 'requirepass mypassword' >> config.example.toml
RUST_LOG=info cargo run -- -c config.example.toml
```

用 redis-cli 验证：
```bash
# 未认证时执行命令应返回 NOAUTH
redis-cli -p 7379 GET test

# 认证
redis-cli -p 7379 AUTH mypassword

# 认证后应可正常执行命令
redis-cli -p 7379 GET test
```

- [ ] **Step 7.3: 最终提交**

所有变更已完成，无需额外提交。

---

### 总结

整个实现分为 7 个 Task，每个 Task 都可独立编译验证并提交：

1. Config requirepass 字段
2. Client 认证状态
3. AuthCmd 实现
4. handle.rs 认证检查
5. network_handle.rs 认证检查
6. requirepass 传递链路（NetworkServer → Client → AuthCmd）
7. 测试验证
