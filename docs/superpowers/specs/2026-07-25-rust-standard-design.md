# Kiwi Rust 语言标准设计

## 决策

Kiwi 的长期语言标准为 Rust 2024 Edition；当前普通开发、CI、Docker 和发布使用精确固定的 Rust 1.97.1 stable 工具链。

工具链统一和 Edition 迁移拆成两个原子 PR：

1. PR 1 将开发、CI、Docker 和发布工具链统一到 Rust 1.97.1 stable，声明 `rust-version = "1.97.1"`，但保持 Rust 2021 Edition。
2. PR 2 只负责将 Kiwi 自有 workspace 迁移到 Rust 2024 Edition，并审计编译器迁移产生的源码变更。

PR 2 基于 PR 1 的 Head；PR 1 合并前，PR 2 作为 stacked PR 审查。PR 1 合并后，PR 2 再基于最新 `main` 复核。

## 工具链合同

- `rust-toolchain.toml` 是普通开发和构建工具链的唯一真值源。
- stable channel 必须使用完整三段版本号，禁止浮动 `stable`。
- workspace `rust-version` 与批准工具链保持一致。如果未来需要承诺更低 MSRV，必须增加独立 MSRV CI 后再调整。
- 所有 workspace package 通过 `edition.workspace = true` 和 `rust-version.workspace = true` 继承合同。Cargo 对实际 workspace 范围的判定以 `cargo metadata` 为准。
- 普通 GitHub Actions job 不指定 `stable`，由 `actions-rust-lang/setup-rust-toolchain` 读取根目录工具链文件。
- Docker builder 明确使用 `rust:1.97.1-bookworm`，修改时与工具链文件在同一工具链升级 PR 中复核。
- Sanitizer 保留独立、精确日期的 nightly；它是质量工具，不定义产品语言标准。
- `Cargo.lock` 继续提交，构建和发布使用 `--locked`。

## 门禁设计

门禁使用工具本身的权威语义：

- `rustup show active-toolchain` 和 `rustc --version --verbose` 证明实际工具链。
- `cargo metadata --locked --format-version 1 --no-deps` 证明每个 workspace package 的 Edition 和 `rust_version`。
- GitHub Actions 安装步骤读取 `rust-toolchain.toml`，后续 Cargo 命令使用该 override。
- Docker 执行真实 builder 构建，而不是只做文本匹配。
- 静态残留扫描只作为人工审查证据，不能替代实际工具链、CI 或 Docker 验证。

## 开发环境合同

- `protoc` 是所有平台的必需依赖。
- Windows 使用 Rust MSVC target 和 Visual Studio C++ 构建工具。
- Linux 和 macOS 安装项目原生 RocksDB 构建所需的 C/C++ 工具链；各平台以项目开发文档和 CI 配置为准。
- 开发者可运行 `rustup show active-toolchain` 和 `rustc --version --verbose` 核验当前 checkout 实际使用的编译器。

## 验证边界

PR 1 必须完成 Rust 1.97.1 的 fmt、Clippy、workspace check/test，Windows MSVC + Protoc 27.1 原生构建，WSL/Linux 独立 target 构建测试，以及 Docker builder 构建。

PR 2 必须在相同工具链上重复全部门禁，另外执行 Edition 迁移 lint，并人工审计 `unsafe`、FFI、宏 fragment、临时值/drop 顺序和环境变量 API 的变更。

## 升级政策

- 每个 stable 发布后评估一次，通过独立 PR 显式升级。
- point release 包含安全、误编译或重大正确性修复时优先升级。
- 工具链升级 PR 不夹带业务功能、依赖大版本升级或 Edition 迁移。
- 只有 Linux、macOS、Windows 的必需 checks 均通过，才更新批准基线。

## 交付授权边界

两个 PR 的本地实现、验证和提交不自动授权 push、创建或更新 GitHub PR、merge
或关闭 Issue。每项远端写操作必须等待用户单独授权。
