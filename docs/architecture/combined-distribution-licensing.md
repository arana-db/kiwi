# Kiwi 与 Redis 派生动态库的组合发行许可设计

> 状态：发行合同设计；当前没有 Redis 派生生产二进制进入 Kiwi 发行物
> Redis source baseline：tag `8.8.1` / commit `77b6c308396c9700672390a210143a8496fb4b10`
> Redis fork：`https://github.com/arana-db/redis`
> Redis fork 许可证选择：`AGPL-3.0-only`

关联决定：`D002`、`D009`、`D010`。

主要需求：`REQ-LICENSE-001..008`、`REQ-STABILITY-001..006`、`REQ-HOT-002`、`REQ-HOT-010..012`。

## 1. 适用范围

本文定义未来官方 Kiwi 发行包携带并运行时加载 Redis 8.8.1 派生动态库时的工程合规要求。

本文不是法律意见。首次导入 Redis 派生源码、首次发布组合二进制以及许可证或装载方式发生实质变化时，必须由熟悉开源许可证的律师结合最终源码、ABI、构建和发行物复核。

当前 Cache OFF 发行物不携带 Redis 派生动态库，不因本设计自动转为组合发行物。

## 2. 许可证选择

Redis 8.8.1 顶层 `LICENSE.txt` 提供 RSALv2、SSPLv1 和 AGPLv3 三种选择。Kiwi 对 `arana-db/redis` 派生源码和动态库选择 AGPLv3 路径，并使用保守的 SPDX 表达：

```text
AGPL-3.0-only
```

选择必须在 fork、源码包、二进制包、SBOM、NOTICE 和 release provenance 中保持一致，不能在不同平台或发行渠道隐式切换。

上游依据：<https://github.com/redis/redis/blob/8.8.1/LICENSE.txt>

## 3. 三层许可证边界

### 3.1 Kiwi 自有、可独立识别源码

Kiwi 自有 Rust 源码继续按其文件头和仓库声明使用 Apache-2.0。不得为了组合发行机械删除历史版权和 Apache-2.0 notice。

### 3.2 Redis 派生源码和动态库

`arana-db/redis` 中来自或派生自 Redis 8.8.1 的源码、生成物和 native library 必须：

- 保留 Redis 上游版权和许可证文本；
- 明确选择 `AGPL-3.0-only`；
- 标识 Kiwi/Arana 的修改；
- 记录 exact upstream/downstream commit 和 patch history；
- 满足 AGPL 的源码提供及网络交互义务。

### 3.3 官方组合发行物

当 Kiwi 与 Redis 派生动态库被作为一个预期共同运行的产品发行，并由 Kiwi 在同一进程加载以提供核心能力时，官方组合发行物必须履行适用的 AGPL-3.0-only 义务，不能标记为 Apache-2.0-only。

仓库拆分、动态链接或运行时加载可以形成清晰的供应链和构建边界，但不能被当作免除组合发行义务的依据。

## 4. `arana-db/redis` fork 合同

fork 必须具备：

```text
upstream_url:        https://github.com/redis/redis
upstream_tag:        8.8.1
upstream_commit:     77b6c308396c9700672390a210143a8496fb4b10
downstream_url:      https://github.com/arana-db/redis
downstream_commit:   <exact commit>
selected_license:    AGPL-3.0-only
patch_series_hash:   <sha256>
source_archive_hash: <sha256>
```

导入和维护规则：

1. 上游基线必须能从 exact commit 重建；
2. 下游修改采用可审计提交或可重放 patch series；
3. 每个 imported/generated file 都有来源或生成规则；
4. 不删除上游许可证、版权和贡献声明；
5. 不从未审计的 Redis 派生仓库复制实现；
6. allocator、compiler、feature、platform patch 和生成工具全部进入 provenance；
7. release 不从浮动分支或开发工作区直接构建。

## 5. 发行包布局

未来组合发行包至少包含：

```text
kiwi-distribution/
  bin/kiwi
  lib/libkiwi_redis_hot_tier.so        # platform equivalent where applicable
  manifests/redis-hot-tier-pairing.json
  licenses/AGPL-3.0-only.txt
  licenses/Apache-2.0.txt
  licenses/redis-LICENSE.txt
  NOTICE
  THIRD_PARTY_NOTICES.md
  SOURCE_OFFER.md
  SBOM.spdx.json
  build-provenance.json
```

不同平台文件名可以不同，但 pairing、hash、源码和许可证义务必须一致。

## 6. Corresponding Source

每个组合发行版本必须提供与实际二进制精确匹配的完整对应源码，至少包括：

- Kiwi exact source commit；
- `arana-db/redis` exact downstream source commit；
- Redis exact upstream identity；
- 全部修改和 patch history；
- C ABI headers 和 binding generation inputs；
- 构建、链接、安装和打包脚本；
- 编译器、feature、allocator 和关键环境配置；
- 生成源文件所需脚本和输入；
- pairing manifest、SBOM、NOTICE 和许可证；
- 重建发布二进制所需的其他适用材料。

源码 URL 必须绑定 release tag、immutable archive 或 exact commit，不能只指向浮动默认分支。二进制、source archive 和 build provenance 的 hash 必须相互对账。

## 7. 网络用户源码入口

Kiwi 是网络服务器。未来组合发行物需要提供显著、稳定、与运行版本精确对应的源码入口。候选接口包括：

- `INFO server` 或 `INFO build` 中的版本、许可证和 source URL；
- `HELLO` 的可发现构建身份；
- 专用只读 `KIWI.SOURCE` 命令；
- 发行文档和启动日志中的同一 immutable source URL。

至少暴露：

```text
kiwi_version
kiwi_commit
kiwi_license
kiwi_source_url
redis_upstream_tag
redis_upstream_commit
redis_downstream_commit
redis_hot_tier_abi_version
redis_hot_tier_library_sha256
```

具体接口和显著程度必须在首次组合发行前完成法律复核。当前只冻结字段，不授权增加生产命令或修改协议输出。

## 8. Pairing 和二进制身份

组合发行物必须包含版本化 pairing manifest。Kiwi loader 在装载前校验：

- manifest schema；
- Kiwi exact version/commit；
- Redis upstream/downstream exact commit；
- ABI major/minor；
- platform/architecture；
- 动态库规范化路径和文件名；
- 动态库 SHA-256；
- source URL 和许可证标识。

装载后再通过 ABI identity API 校验编译进动态库的身份。文件 hash 和运行时 identity 任一不一致都必须拒绝加载，不能回退为加载未知库。

## 9. NOTICE、SBOM 和修改声明

`THIRD_PARTY_NOTICES.md` 至少记录：

```text
Component: Redis-derived native hot-tier library
Upstream: https://github.com/redis/redis
Upstream tag/commit: 8.8.1 / 77b6c308396c9700672390a210143a8496fb4b10
Downstream: https://github.com/arana-db/redis
Downstream commit: <exact commit>
Selected license: AGPL-3.0-only
Use: runtime-loaded Embedded Redis Hot Tier
Modifications: <immutable patch/source reference>
```

SBOM 必须覆盖 Kiwi、Redis 派生库、native dependencies、allocator 和发行包中的工具。修改声明必须足以区分上游原始代码和下游变更，但不得错误声称拥有上游版权。

## 10. CI 和 Release 门禁

首次组合发行前 required checks：

- license text/notice 存在且与选择一致；
- 上游和下游 exact commit 可验证；
- source/patch/build 可重复；
- dynamic library hash 与 pairing manifest 一致；
- ABI runtime identity 与 manifest 一致；
- source archive 能在干净环境重建目标产物；
- source URL 可访问且绑定 immutable version；
- SBOM 覆盖所有实际打包文件；
- release archive 解包后许可证和源码入口完整；
- 开源许可证律师完成最终复核；
- 无未关闭的许可证 P0/P1。

## 11. 当前阶段的禁止事项

在 `docs/quality/system-stability-gate.md` 通过且用户重新批准前：

- 不创建或修改 Redis 派生生产源码；
- 不编译或打包热层动态库；
- 不在 Kiwi 加入 loader、FFI 或运行时依赖；
- 不新增网络 source-offer 命令；
- 不把当前 Cache OFF 发行物声明为组合发行物；
- 不以本设计代替法律复核。

本文是未来合规工作的验收合同，不是实施授权。

## 12. 参考

- Redis 8.8.1 license：<https://github.com/redis/redis/blob/8.8.1/LICENSE.txt>
- GNU AGPLv3：<https://www.gnu.org/licenses/agpl-3.0.html>
- Apache Software Foundation GPL compatibility：<https://www.apache.org/licenses/GPL-compatibility.html>
- OSI AGPL-3.0：<https://opensource.org/license/agpl-3-0>
