# 固定上游与研究来源

所有“基于上游”的结论必须指向 exact tag 或 commit。浮动 `main`、`master` 和 `latest` 不能作为兼容、构建或发布证据。

## 实现与行为来源

| 项目 | 固定版本 | 用途 | 许可证边界 |
|---|---|---|---|
| Redis | [`8.8.1`](https://github.com/redis/redis/tree/77b6c308396c9700672390a210143a8496fb4b10) | 唯一普通 Redis 行为 Oracle、接口设计和未来原生热层上游来源 | Redis 8 tri-license；未来 fork 选择 AGPL-3.0-only，保留上游版权、许可证和修改记录 |
| PikiwiDB | [`b483eea`](https://github.com/OpenAtomFoundation/pikiwidb/tree/b483eeafe65c2f4e7594dae004fd328ff688f409) | Redis–RocksDB 混合存储和热层架构研究 | 只借鉴可验证架构，不直接复制来源或许可证不清的代码 |
| pikiwidb/rediscache | [`v1.0.7`](https://github.com/pikiwidb/rediscache/tree/v1.0.7) | 研究原生 Redis 内存层的历史接入方式和风险 | 不作为 Kiwi 依赖；任何实现都必须重新完成来源、许可证、ABI 和正确性审计 |
| RedisRaft | [`ade4aa8`](https://github.com/RedisLabs/redisraft/tree/ade4aa8e6aa5c3b21678a1998309825f06567d4f) | 公开 Raft 命令、错误、INFO/CONFIG 和测试场景 | RSALv2/SSPLv1；不得直接复制实现或测试，采用 clean-room 行为规范 |
| RedisLabs/raft | [`a634de6`](https://github.com/RedisLabs/raft/tree/a634de6f81e1d774ce95e3acd62aef349ce6e521) | deterministic simulator 与 Raft 不变量 | BSD-3-Clause；派生代码保留 notice |
| RedisLabs/redis-rs | [`c7c1464`](https://github.com/RedisLabs/redis-rs/tree/c7c14647dc3569af385bc06f732b4949dfde6934) | 历史客户端测试研究 | 历史 fork，不作为当前依赖 |
| redis-rs/redis-rs | [`redis-1.4.1`](https://github.com/redis-rs/redis-rs/tree/37f4cad379fd8a05d058416356640f10160a4755) | 当前 Rust 客户端兼容测试候选 | BSD-3-Clause；仅测试用途，不进入生产 server crate |

`arana-db/redis` 建立后，必须在本表新增下游 exact commit、与 Redis 8.8.1 上游的对应关系、patch 清单、构建配置和发布产物 hash。在这些证据存在前，它不是 Kiwi 构建输入；该 fork 的生产实现仍受系统稳定性门禁冻结。

## 许可证来源

| 来源 | 固定链接 | 用途 |
|---|---|---|
| Redis 8.8.1 LICENSE | [`LICENSE.txt`](https://github.com/redis/redis/blob/8.8.1/LICENSE.txt) | 核对 RSALv2、SSPLv1、AGPLv3 三选一授权及完整上游声明 |
| GNU AGPLv3 | [GNU Affero General Public License v3](https://www.gnu.org/licenses/agpl-3.0.html) | 对应源码、修改、网络交互和组合程序义务的正式文本 |
| Apache Software Foundation | [GPL compatibility](https://www.apache.org/licenses/GPL-compatibility.html) | Apache-2.0 代码进入 GPLv3 系列组合发行时的单向兼容说明 |
| Open Source Initiative | [AGPL-3.0](https://opensource.org/license/agpl-3-0) | AGPL-3.0 的 OSI 批准状态和许可证入口 |

## 本地研究镜像

本地只读研究镜像应放在 Kiwi 仓库之外、由使用者自行配置的隔离目录中；具体工作站绝对路径不得写入版本化项目真相。这些镜像不是 Kiwi 构建输入，也不能替代上游 tag、commit、许可证、patch 或 binary/source pairing 清单。
