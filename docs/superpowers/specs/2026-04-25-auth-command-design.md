---
name: AUTH command design
description: Redis-compatible AUTH command implementation for kiwi-server with single-password authentication and ACL extension points
---

# AUTH 命令实现设计

## 概述

在 kiwi-server 中实现 Redis 兼容的 `AUTH` 命令，支持单密码认证（`AUTH <password>`），
并在架构上为未来 ACL 用户系统预留扩展点。

## 需求

- 配置文件通过 `requirepass` 设置认证密码，默认不启用认证
- 连接初始未认证（有 requirepass 时），仅允许 `NO_AUTH` flag 命令
- AUTH 命令验证密码通过后设置 `client.authenticated = true`
- 未认证客户端执行非 `NO_AUTH` 命令时返回 `NOAUTH Authentication required.`
- `HELLO`、ACL 用户认证留到后续实现

## 架构

### 1. 配置层 (`conf/src/config.rs`)

- `Config` 新增 `requirepass: Option<String>` 字段，默认 `None`
- 配置文件解析新增 `requirepass` key
- Config 需可通过全局单例或 Arc 共享给 pipeline/executor 层

### 2. 客户端认证状态 (`client/src/lib.rs`)

- `ClientContext` 新增 `authenticated: bool` 字段
- 新增 `is_authenticated(&self) -> bool` 方法
- 新增 `set_authenticated(&self, bool)` 方法
- 构造时根据 requirepass 决定初始值：有密码 → `false`，无密码 → `true`

### 3. 认证检查 (`net/src/pipeline.rs`)

在 `pipeline.rs` 的 `execute_command` 中，命令查找前插入检查：

```
if !client.is_authenticated() && !cmd.flags.contains(NO_AUTH) {
    return RespData::Error("NOAUTH Authentication required.".into());
}
```

白名单由 `CmdFlags::NO_AUTH` 控制。

### 4. AUTH 命令 (`cmd/src/auth.rs`)

```
AuthCmd {
    meta: CmdMeta {
        name: "auth",
        arity: -2,
        flags: NO_AUTH | FAST,
        acl_category: CONNECTION,
    }
}

do_cmd(client, _storage):
    argv = client.argv()
    match argv.len():
        2:
            password = String::from_utf8_lossy(&argv[1])
            if global_config.requirepass == Some(password):
                client.set_authenticated(true)
                reply("OK")
            else:
                reply("WRONGPASS invalid username-password pair or user is disabled.")
        3:
            // AUTH <user> <pass> — 预留，暂不实现
            reply("ERR ACL authentication is not supported")
        _:
            reply("ERR wrong number of arguments for 'auth' command")
```

### 5. 配置传递

Config 通过 `Arc<Config>` 在 server 启动时传递到 pipeline，
使 `execute_command` 可读取 `requirepass` 字段。

## 新增文件

- `cmd/src/auth.rs` — AUTH 命令实现

## 修改文件

- `conf/src/config.rs` — 新增 requirepass 字段
- `client/src/lib.rs` — 新增 authenticated 状态
- `cmd/src/lib.rs` — 新增 pub mod auth
- `cmd/src/table.rs` — 注册 AuthCmd
- `net/src/pipeline.rs` — 认证前置检查
- `conf/src/de_func.rs` — 如需要支持配置解析

## 错误消息

| 场景 | 响应 |
|------|------|
| AUTH 密码错误 | `WRONGPASS invalid username-password pair or user is disabled.` |
| 未认证执行命令 | `NOAUTH Authentication required.` |
| AUTH 参数 > 2 | `ERR wrong number of arguments for 'auth' command` |
| AUTH 参数 = 3 | `ERR ACL authentication is not supported` |
