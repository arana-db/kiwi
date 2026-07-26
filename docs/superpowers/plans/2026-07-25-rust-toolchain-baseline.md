# Rust 1.97.1 工具链基线实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 将 Kiwi 普通开发、CI、Docker 和发布工具链统一到 Rust 1.97.1 stable，同时保持 Rust 2021 Edition。

**架构：** `rust-toolchain.toml` 定义实际工具链，workspace `rust-version` 定义 Cargo 合同，所有 member 继承合同。普通 Actions job 直接读取工具链文件，Sanitizer 使用独立 dated nightly。

**技术栈：** Rust 1.97.1 stable、Cargo workspace、GitHub Actions、Docker Bookworm、Windows MSVC/Visual Studio C++ 构建工具、Linux/macOS 原生 C/C++ 工具链、Protoc 27.1。

---

### 任务 1：统一 Cargo workspace 和本地工具链

**文件：**
- 修改：`rust-toolchain.toml`
- 修改：`Cargo.toml`
- 修改：所有 workspace member `Cargo.toml`

- [ ] 将 `rust-toolchain.toml` 设为 `channel = "1.97.1"`、`profile = "minimal"`、`components = ["rustfmt", "clippy"]`。
- [ ] 在 `[workspace.package]` 保持 `edition = "2021"`，新增 `rust-version = "1.97.1"`。
- [ ] 所有 member 使用 `edition.workspace = true` 和 `rust-version.workspace = true`；`client`/`cmd` 去掉显式 2021，不改变语义。
- [ ] 运行 `cargo metadata --locked --format-version 1 --no-deps`，断言所有 `workspace_members` 对应 package 均为 edition 2021 / rust_version 1.97.1。
- [ ] 运行 `cargo fmt --all -- --check` 和 `cargo check --workspace --all-features --locked`。
- [ ] 只提交工具链和 manifest：`build: pin Rust 1.97.1 toolchain`。

### 任务 2：统一 CI、Sanitizer 和 Docker

**文件：**
- 修改：`.github/workflows/ci.yml`
- 修改：`.github/workflows/benchmark.yml`
- 修改：`.github/workflows/codeql.yml`
- 修改：`.github/workflows/release.yml`
- 修改：`Dockerfile`

- [ ] 普通 setup-rust-toolchain 步骤删除 `toolchain: stable`，由 action 读取根 `rust-toolchain.toml`。
- [ ] Sanitizer job 定义 `SANITIZER_TOOLCHAIN: nightly-2026-07-17`，action input 使用 `${{ env.SANITIZER_TOOLCHAIN }}`，所有 `cargo +nightly` 改为 `cargo +${SANITIZER_TOOLCHAIN}`。
- [ ] 确认 `https://static.rust-lang.org/dist/2026-07-17/channel-rust-nightly.toml` 存在。
- [ ] Docker builder 改为 `FROM rust:1.97.1-bookworm AS builder`。
- [ ] 运行 GitHub Actions YAML 语法检查，并人工审查全部 setup-rust-toolchain 和 Cargo override 行。
- [ ] 提交：`ci: align builds with pinned Rust toolchain`。

### 任务 3：更新开发者合同文档

**文件：**
- 修改：`README.md`
- 修改：`README_CN.md`
- 修改：`CONTRIBUTING.md`
- 修改：`CLAUDE.md`
- 修改：`docs/development.md`
- 创建：本设计和两个计划文档

- [ ] 记录 Rust 1.97.1 stable、Edition 2021 过渡状态、Nightly 专项边界、Protoc、Windows MSVC/Visual Studio C++ 工具链及 Linux/macOS 原生构建要求。
- [ ] 记录 `rustup show active-toolchain` 和 `rustc --version --verbose`，用于核验当前 checkout 自动选择的工具链。
- [ ] 删除生效文档中 `nightly-2025-08-20` 和浮动 stable 是标准的表述；历史 plan 证据不改写。
- [ ] 运行 `rg` 残留扫描、`git diff --check`，并提交：`docs: define the Kiwi Rust baseline`。

### 任务 4：完整验证 PR 1

- [ ] 运行 `rustup show active-toolchain` 和 `rustc --version --verbose`，确认当前 checkout 使用 Rust 1.97.1 stable。
- [ ] Windows 验证必须在 Visual Studio Developer Command Prompt 或已加载等价 VS C++ 环境的终端中执行；`rustc --version --verbose` 必须严格包含 `host: x86_64-pc-windows-msvc`，且 `where cl`（PowerShell 使用 `where.exe cl`）必须解析到可执行的 MSVC `cl.exe`。任一条件不满足时立即停止，不得把后续结果记为 Windows MSVC 验证。
- [ ] Windows MSVC + Protoc 27.1：`cargo fmt --all -- --check`、`cargo check --workspace --all-features --locked`、`cargo clippy --locked --all-features --workspace -- -D warnings -D clippy::unwrap_used`、`cargo test --workspace --all-features --locked`。
- [ ] WSL/Linux 使用独立 target 重复 check、Clippy 和 test，不与 Windows 共享 native artifacts。
- [ ] 运行 `docker build --target builder -t kiwi-rust-1.97.1-builder .`。
- [ ] 运行 `git diff --check origin/main...HEAD`并确认无 Edition 2024 语义迁移或业务源码改动。
- [ ] 保存验证证据；不 push、不创建 GitHub PR，等待用户单独授权。
