# Kiwi-rs

## 简介

Kiwi-rs 是用 Rust 实现的增强版 Redis 服务器，目标是提供高性能和持久化的键值存储。当前项目还处于早期阶段，功能正在不断扩展和完善。

## 功能特色

- 使用 RocksDB 作为后端持久化存储。
- 高度兼容 Redis 协议，支持大部分 Redis 指令。
- 支持使用 Redis 进行性能基准测试。
- 计划中的模块化支持，允许开发者自定义扩展。
- 提供高性能的请求处理能力。

## 系统要求

- 操作系统：Linux、macOS 或 FreeBSD
- Rust 编译工具链

## 安装指南

请确保已经安装了 Rust 工具链，可以使用 [rustup](https://rustup.rs/) 进行安装：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 开发计划

- 增加集群模式支持
- 扩展命令支持并优化命令执行效率
- 增强模块化扩展的功能并提供示例
- 完善开发文档和用户指南

## 贡献

欢迎对 Kiwi-RS 项目的贡献！如果你有任何建议或发现了问题，请提交 Issue 或创建 Pull Request。

## 联系我们
