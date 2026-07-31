# Third-Party Source and License Inventory

Kiwi-authored source remains licensed under Apache License 2.0. This file records third-party source that is planned, vendored, derived, linked, distributed, or used in compatibility tooling.

## Redis 8.8.1

- Project: Redis
- Upstream: https://github.com/redis/redis
- Tag: `8.8.1`
- Commit: `77b6c308396c9700672390a210143a8496fb4b10`
- Upstream license choices: RSALv2, SSPLv1, or AGPLv3, as recorded in the upstream `LICENSE.txt`.
- Selected license for the future `arana-db/redis` fork: `AGPL-3.0-only`.
- Current use: exact behavioral compatibility Oracle and interface-design baseline.
- Deferred use: source baseline for the Embedded Redis Hot Tier native library.
- Current repository and release state: no Redis-derived source or native library is vendored, linked, loaded, or distributed by Kiwi. The hot tier is design-only until the system stability gate passes and the user explicitly authorizes a separate implementation task.

Before Redis-derived source enters Kiwi, the importing change must add:

1. The complete selected AGPL-3.0-only license terms and upstream copyright notices.
2. An exact source manifest identifying upstream and downstream commits, every patch, and every generated file.
3. A reproducible transformation and build record, including compiler, build options, allocator assumptions, and binary hashes.
4. The versioned C ABI definition, binding-generation procedure, and runtime identity checks.
5. Corresponding source, license notices, SBOM, and a stable source-offer entry point matching each binary release.
6. A distribution review covering source, binary, container, archive, package-manager, and remote-use obligations.

An official distribution that contains the Redis-derived native library must not be described as Apache-2.0-only. Repository separation, dynamic linking, or runtime loading are engineering boundaries and do not remove the accepted combined-distribution obligations.

## RedisRaft

- Upstream: https://github.com/RedisLabs/redisraft
- Research commit: `ade4aa8e6aa5c3b21678a1998309825f06567d4f`
- License: RSALv2 or SSPLv1 according to the upstream repository.
- Kiwi use: behavioral research for the clean-room public compatibility profile only.
- Restriction: RedisRaft source and tests are not copied into Kiwi by this planning change.

## RedisLabs/raft

- Upstream: https://github.com/RedisLabs/raft
- Research commit: `a634de6f81e1d774ce95e3acd62aef349ce6e521`
- License: BSD 3-Clause.
- Kiwi use: Raft invariant and deterministic-simulation design research. Any derived code must retain the applicable notice.

## redis-rs

- Upstream: https://github.com/redis-rs/redis-rs
- Candidate test release: `redis-1.4.1`
- Commit: `37f4cad379fd8a05d058416356640f10160a4755`
- License: BSD 3-Clause.
- Kiwi use: test-only Rust client compatibility tooling; not a production server dependency.

This inventory is an engineering control and does not replace legal review.
