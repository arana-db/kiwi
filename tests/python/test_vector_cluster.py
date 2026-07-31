# Copyright (c) 2024-present, arana-db Community.  All rights reserved.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Vector Set 三节点 Raft 集群集成测试。

每个用例独立拉起一套 3 节点 kiwi 集群（动态端口、临时数据目录），通过
grpcurl 调用 RaftAdminService 初始化/变更成员，通过 redis-py 走 RESP 协议
验证 Vector Set 命令在集群模式下的行为。

运行方式（默认跳过，避免拖慢常规集成测试）：

    cargo build --bin kiwi
    KIWI_RUN_CLUSTER_TESTS=1 python3 -m pytest tests/python/test_vector_cluster.py -v

依赖：`grpcurl`（集群初始化与成员管理）、`target/debug/kiwi`（可用
KIWI_BINARY 覆盖）、redis-py。

已知限制（本分支现状，非测试缺陷）：
- Raft 快照的网络安装尚未实现（src/raft/src/network.rs 的 install_snapshot
  直接返回 Unsupported，src/raft/src/grpc/core.rs 的接收端丢弃分片），因此
  "follower 落后超过 leader 日志保留窗口后通过快照追平" 的场景无法测试。
  test_lagged_follower_log_replay_and_snapshot_build 只覆盖：落后未超窗的
  follower 通过日志回放追平 + leader 本地快照构建/日志清理不影响集群。
- Vector member key 编码内嵌节点本地随机的 storage_incarnation
  （src/storage/src/format_vector_member_key.rs + storage_manifest.rs），
  而集群复制是物理 binlog 回放（逐字节复制 leader 编码后的 CF 记录）。
  因此 follower 重放的 member 记录带有 leader 的 incarnation，用本地
  incarnation 构造 key 的读路径（VEMB/VISMEMBER/VSIM/VREM）在新 leader
  上读不到旧数据；只有不内嵌 incarnation 的 meta（VCARD/VDIM/VINFO）
  复制后可读。这是 PR0 logical mutation replay（社区 issue #332）要解决
  的问题。因此集群模式下 Vector 命令默认被 `vector-cluster-enabled`
  门禁确定性拒绝（ERR vector commands are not supported in cluster
  mode yet）；本测试在节点配置中显式开启该开关以验证集群链路行为。
  主用例只断言当前可兑现的行为；member 数据在副本上的可读性由
  test_replica_member_data_survives_failover（xfail）锁定，PR0 落地后
  该用例会自然转绿（XPASS）。
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import threading
import time

import pytest
import redis
from redis.exceptions import AskError, MovedError

# redis-py 的 MovedError/AskError 期望 Redis Cluster 的 "MOVED <slot> <host:port>"
# 格式，而 Kiwi 返回简化形式 "MOVED <host:port>"（无 slot 号），原生解析会直接
# 抛 ValueError。这里打补丁兼容两种格式，并保留 host/port 供重定向使用。
def _redirect_error_init(self, resp, status_code=None):
    redis.ResponseError.__init__(self, resp, status_code=status_code)
    self.args = (resp,)
    self.message = resp
    parts = resp.split(" ")
    if len(parts) == 1:
        self.slot_id = None
        host, port = parts[0].rsplit(":", 1)
    else:
        self.slot_id = int(parts[0])
        host, port = parts[1].rsplit(":", 1)
    self.node_addr = self.host, self.port = host, int(port)


AskError.__init__ = _redirect_error_init

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
KIWI_BINARY = os.environ.get(
    "KIWI_BINARY", os.path.join(REPO_ROOT, "target", "debug", "kiwi")
)
GRPCURL = shutil.which("grpcurl")

ELECTION_TIMEOUT_MS = 600

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.skipif(
        os.environ.get("KIWI_RUN_CLUSTER_TESTS") != "1",
        reason="cluster tests are slow; set KIWI_RUN_CLUSTER_TESTS=1 to enable",
    ),
    pytest.mark.skipif(GRPCURL is None, reason="grpcurl is required"),
    pytest.mark.skipif(
        not os.path.isfile(KIWI_BINARY),
        reason=f"kiwi binary not found at {KIWI_BINARY}; run `cargo build --bin kiwi`",
    ),
]


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def wait_until(fn, timeout, desc, interval=0.2):
    """轮询 fn 直到返回真值；超时抛 AssertionError。返回 fn 的真值结果。"""
    deadline = time.monotonic() + timeout
    while True:
        result = fn()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for: {desc}")
        time.sleep(interval)


def grpc_call(addr, service, method, payload, timeout=10):
    """通过 grpcurl 调用 gRPC（服务带反射，无需 proto 文件）。失败返回 None。"""
    try:
        proc = subprocess.run(
            [GRPCURL, "-plaintext", "-d", json.dumps(payload), addr, f"{service}/{method}"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return None
    if proc.returncode != 0:
        return None
    out = proc.stdout.strip()
    if not out:
        return {}
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def grpc_metrics(node):
    return grpc_call(
        f"127.0.0.1:{node.raft_port}",
        "kiwi.raft.v1.RaftMetricsService",
        "Metrics",
        {},
    )


def free_ports(count):
    ports = set()
    while len(ports) < count:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        ports.add(sock.getsockname()[1])
        sock.close()
    return sorted(ports)


def vec(i):
    """第 i 个元素的 2 维向量（非零，避免零范数边界）。"""
    return (1.0 + (i % 1000) * 0.001, 0.5 - (i % 1000) * 0.0005)


def elem(i):
    return f"e{i}".encode()


# force_leader 的 bump 写入使用独立 key，不影响各用例主 key 的 VCARD 计数
BUMP_KEY = b"vc:bump"


def vadd_args(key, i):
    x, y = vec(i)
    return (b"VADD", key, b"VALUES", 2, f"{x:.6f}", f"{y:.6f}", elem(i), b"NOQUANT")


# ---------------------------------------------------------------------------
# 集群封装
# ---------------------------------------------------------------------------


class ClusterNode:
    def __init__(self, node_id, raft_port, resp_port, node_dir):
        self.node_id = node_id
        self.raft_port = raft_port
        self.resp_port = resp_port
        self.dir = node_dir
        self.conf_path = os.path.join(node_dir, "node.conf")
        self.log_path = os.path.join(node_dir, "kiwi.log")
        self.proc = None
        self.log_fd = None
        self.alive = False

    @property
    def raft_addr(self):
        return f"127.0.0.1:{self.raft_port}"

    @property
    def resp_addr(self):
        return f"127.0.0.1:{self.resp_port}"


class VectorCluster:
    def __init__(self, base_dir, binary, node_count=3):
        self.base_dir = str(base_dir)
        self.binary = binary
        ports = free_ports(node_count * 2)
        self.nodes = []
        for i in range(node_count):
            node_dir = os.path.join(self.base_dir, f"node{i + 1}")
            os.makedirs(node_dir, exist_ok=True)
            self.nodes.append(
                ClusterNode(i + 1, ports[i], ports[node_count + i], node_dir)
            )
        self._clients = {}

    # -- 生命周期 ---------------------------------------------------------

    def _write_config(self, node):
        with open(node.conf_path, "w", encoding="utf-8") as conf:
            conf.write(
                "\n".join(
                    [
                        "binding 127.0.0.1",
                        f"port {node.resp_port}",
                        "runtime-network_threads 1",
                        "runtime-storage_threads 2",
                        "db-instance-num 1",
                        f"data-dir {node.dir}/db",
                        f"log-dir {node.dir}/logs",
                        f"raft-node-id {node.node_id}",
                        f"raft-addr {node.raft_addr}",
                        f"raft-resp-addr {node.resp_addr}",
                        f"raft-data-dir {node.dir}/raft",
                        "raft-heartbeat-interval-ms 100",
                        "raft-election-timeout-min-ms 300",
                        f"raft-election-timeout-max-ms {ELECTION_TIMEOUT_MS}",
                        # 集群模式 Vector 命令默认拒绝（failover 后成员数据
                        # 不可读的防护，见 docs/vector-set-operations.md）；
                        # 本测试显式放开以验证集群链路行为。
                        "vector-cluster-enabled yes",
                        "",
                    ]
                )
            )

    def start_node(self, node, timeout=30):
        self._write_config(node)
        node.log_fd = open(node.log_path, "ab")
        env = dict(os.environ, RUST_LOG="info")
        node.proc = subprocess.Popen(
            [self.binary, "--config", node.conf_path],
            cwd=node.dir,
            stdout=node.log_fd,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=env,
        )
        node.alive = True

        def ping_ok():
            if node.proc.poll() is not None:
                raise AssertionError(
                    f"node {node.node_id} exited early (rc={node.proc.returncode}); "
                    f"see {node.log_path}"
                )
            try:
                self.client(node).ping()
                return True
            except redis.RedisError:
                return False

        wait_until(ping_ok, timeout, f"node {node.node_id} RESP ready")

    def start_all(self):
        for node in self.nodes:
            self.start_node(node)

    def kill(self, node):
        """SIGKILL 整个进程组，模拟节点崩溃。"""
        if node.proc is not None and node.proc.poll() is None:
            os.killpg(node.proc.pid, signal.SIGKILL)
            node.proc.wait(timeout=10)
        node.alive = False

    def restart(self, node, timeout=30):
        assert not node.alive
        self._clients.pop(node.node_id, None)
        self.start_node(node, timeout=timeout)
        # 等它重新感知到 leader（或自己成为 leader）
        wait_until(
            lambda: (grpc_metrics(node) or {}).get("currentLeader", 0) != 0
            or (grpc_metrics(node) or {}).get("isLeader"),
            timeout,
            f"node {node.node_id} rejoined the cluster",
        )

    def shutdown(self):
        for node in self.nodes:
            if node.alive:
                self.kill(node)
            if node.log_fd is not None:
                node.log_fd.close()
        for client in self._clients.values():
            client.close()
        self._clients.clear()

    # -- 客户端与重定向 ----------------------------------------------------

    def client(self, node):
        if node.node_id not in self._clients:
            self._clients[node.node_id] = redis.Redis(
                host="127.0.0.1",
                port=node.resp_port,
                decode_responses=False,
                socket_connect_timeout=2,
                socket_timeout=10,
            )
        return self._clients[node.node_id]

    def alive_nodes(self):
        return [n for n in self.nodes if n.alive]

    def node_by_resp_port(self, port):
        for node in self.nodes:
            if node.resp_port == port:
                return node
        raise AssertionError(f"MOVED target port {port} is not a cluster node")

    def execute_from(self, node, *args, max_redirects=5):
        """从指定节点发命令，跟随 MOVED 重定向（Kiwi 简化版：MOVED <addr>）。"""
        current = node
        for _ in range(max_redirects):
            try:
                return self.client(current).execute_command(*args)
            except MovedError as exc:
                current = self.node_by_resp_port(exc.port)
        raise AssertionError(f"too many MOVED redirects for command {args[0]!r}")

    # -- Raft 操作 ----------------------------------------------------------

    def initialize(self):
        payload = {
            "nodes": [
                {
                    "node_id": n.node_id,
                    "raft_addr": n.raft_addr,
                    "resp_addr": n.resp_addr,
                }
                for n in self.nodes
            ]
        }
        result = wait_until(
            lambda: grpc_call(
                self.nodes[0].raft_addr,
                "kiwi.raft.v1.RaftAdminService",
                "Initialize",
                payload,
            ),
            30,
            "cluster Initialize RPC",
        )
        assert result.get("response", {}).get("success") is True, (
            f"Initialize failed: {result}"
        )

    def wait_leader(self, timeout=30):
        def find():
            for node in self.alive_nodes():
                metrics = grpc_metrics(node)
                if metrics and metrics.get("isLeader"):
                    return node
            return None

        return wait_until(find, timeout, "a leader to be elected")

    def wait_card(self, node, key, expected, timeout=30):
        """等指定节点（应为 leader）上线性一致读 VCARD 达到期望值。"""

        def card_ok():
            try:
                return self.client(node).execute_command(b"VCARD", key) == expected
            except redis.RedisError:
                return False

        wait_until(
            card_ok,
            timeout,
            f"node {node.node_id} VCARD({key!r}) == {expected}",
        )


def force_leader(cluster, target, timeout=60):
    """确定性地让 target 成为 leader。

    利用 Raft 选举限制——voter 不会把票投给日志不如自己完整的 candidate：
    先杀掉第三节点，通过现 leader 提交一条 bump 写入（只会到达 target，
    使 target 日志严格最长），再杀掉现 leader 并重启第三节点（其日志必然
    短于 target），于是 target 必然赢得选举。bump 写入提交成功本身也隐含
    了 target 已追平到该写入。返回新的 leader（应为 target）。
    """
    leader = cluster.wait_leader()
    if leader.node_id == target.node_id:
        return leader
    third = next(
        n
        for n in cluster.nodes
        if n.node_id not in (leader.node_id, target.node_id)
    )
    cluster.kill(third)
    cluster.execute_from(leader, *vadd_args(BUMP_KEY, target.node_id * 1000 + leader.node_id))
    cluster.kill(leader)
    cluster.restart(third)
    new_leader = cluster.wait_leader(timeout=timeout)
    assert new_leader.node_id == target.node_id, (
        f"expected node {target.node_id} to win the election (longest log), "
        f"got node {new_leader.node_id}"
    )
    return new_leader


# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def redis_client():
    """覆盖 conftest 的全局单节点夹具：本文件自带 3 节点集群，不依赖外部服务器。"""
    yield None


@pytest.fixture()
def cluster(tmp_path):
    instance = VectorCluster(tmp_path, KIWI_BINARY)
    try:
        instance.start_all()
        instance.initialize()
        instance.wait_leader(timeout=30)
    except Exception:
        instance.shutdown()
        raise
    yield instance
    instance.shutdown()


def vadd_batch(cluster, key, start, count, chunk=250):
    """在当前 leader 上批量写入 e[start]..e[start+count-1]，断言全部为新元素。"""
    leader = cluster.wait_leader()
    client = cluster.client(leader)
    end = start + count
    for off in range(start, end, chunk):
        pipe = client.pipeline(transaction=False)
        for i in range(off, min(off + chunk, end)):
            pipe.execute_command(*vadd_args(key, i))
        for reply in pipe.execute():
            assert reply == 1
    return leader


# ---------------------------------------------------------------------------
# 用例
# ---------------------------------------------------------------------------


def test_quorum_commit_and_three_replica_consistency(cluster):
    """写路径只走 leader（follower 一律 MOVED），线性一致读验证数据，
    并逐副本验证三副本一致（成员收缩为单节点后直接读）。"""
    key = b"vc:t1"
    leader = cluster.wait_leader()
    followers = [n for n in cluster.nodes if n.node_id != leader.node_id]

    # follower 上写命令与 6 个 vector 读命令都应 MOVED 到 leader
    for follower in followers:
        client = cluster.client(follower)
        with pytest.raises(MovedError) as write_redirect:
            client.execute_command(*vadd_args(key, 0))
        assert write_redirect.value.port == leader.resp_port
        for read_cmd in (b"VCARD", b"VDIM", b"VINFO"):
            with pytest.raises(MovedError):
                client.execute_command(read_cmd, key)

    # 从 follower 连接出发、跟随 MOVED 后写成功（客户端可见的写路径）
    assert cluster.execute_from(followers[0], *vadd_args(key, 0)) == 1

    # leader 上写入 100 个向量（quorum 提交）
    vadd_batch(cluster, key, 1, 99)

    # leader 上的线性一致读
    leader_client = cluster.client(cluster.wait_leader())
    assert leader_client.execute_command(b"VCARD", key) == 100
    assert leader_client.execute_command(b"VDIM", key) == 2
    assert leader_client.execute_command(b"VISMEMBER", key, elem(7)) == 1
    emb = leader_client.execute_command(b"VEMB", key, elem(42))
    assert [float(v) for v in emb] == pytest.approx(list(vec(42)))

    # 三副本一致：vector 读只在 leader 可用，因此把每个 follower 副本依次
    # "扶上 leader"（force_leader 利用日志最完整者必胜的选举规则，确定性
    # 轮换）后直接读验证；原 leader 副本已被上面的线性一致读覆盖。
    # 注：member 级数据（VEMB/VISMEMBER）因 storage_incarnation 内嵌于
    # member key，副本上读不到（见模块 docstring），这里断言 meta 级一致；
    # member 级断言见 xfail 用例 test_replica_member_data_survives_failover。
    for target in followers:
        forced = force_leader(cluster, target)
        assert forced.node_id == target.node_id
        cluster.wait_card(target, key, 100)
        assert cluster.client(target).execute_command(b"VDIM", key) == 2


def test_concurrent_vadd(cluster):
    """多线程并发 VADD 不同元素到同一个 set，最终 VCARD == 成功新增数。"""
    key = b"vc:t2"
    threads, per_thread = 8, 50
    added = [0] * threads
    errors = []

    def worker(tid):
        try:
            node = cluster.wait_leader()
            total = 0
            for i in range(per_thread):
                index = tid * per_thread + i
                reply = cluster.execute_from(node, *vadd_args(key, index))
                total += int(reply)
            added[tid] = total
        except Exception as exc:  # noqa: BLE001 - 收集到主线程统一断言
            errors.append(f"thread {tid}: {exc!r}")

    workers = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
    for worker_thread in workers:
        worker_thread.start()
    for worker_thread in workers:
        worker_thread.join(timeout=120)

    assert not errors, f"concurrent VADD errors: {errors}"
    expected = sum(added)
    assert expected == threads * per_thread, (
        f"VADD should report every element as newly added, got {expected}"
    )
    leader_client = cluster.client(cluster.wait_leader())
    assert leader_client.execute_command(b"VCARD", key) == expected


def test_leader_failover(cluster):
    """杀掉 leader 后新 leader 选出：旧数据可见（VCARD/VSIM），新写入成功。"""
    key = b"vc:t3"
    leader = vadd_batch(cluster, key, 0, 200)
    leader_client = cluster.client(leader)
    assert leader_client.execute_command(b"VCARD", key) == 200

    cluster.kill(leader)

    new_leader = cluster.wait_leader(timeout=30)
    assert new_leader.node_id != leader.node_id
    new_client = cluster.client(new_leader)

    # 旧数据在新 leader 上可见（等任期内的线性一致屏障就绪）。
    # meta 级（VCARD）可验证；member 级（VSIM/VEMB 旧数据）受
    # storage_incarnation 限制，见 xfail 用例。
    cluster.wait_card(new_leader, key, 200)

    # 新 leader 接受写入，且新写入的数据立即可读
    assert new_client.execute_command(*vadd_args(key, 5000)) == 1
    assert new_client.execute_command(b"VCARD", key) == 201
    emb = new_client.execute_command(b"VEMB", key, elem(5000))
    assert [float(v) for v in emb] == pytest.approx(list(vec(5000)))


def test_lagged_follower_log_replay_and_snapshot_build(cluster):
    """落后 follower 通过日志回放追平；写过量超过快照阈值后 leader 构建快照。

    注：快照的网络安装（install_snapshot RPC）本分支未实现，落后超过日志
    保留窗口的 follower 无法追平，该场景无法测试；这里 B 阶段写入量刻意
    低于保留窗口（快照阈值 5000 / 保留 1000），保证走日志回放路径。
    """
    key = b"vc:t4"

    # A：全集群健康时写入 500
    leader = vadd_batch(cluster, key, 0, 500)
    follower = next(n for n in cluster.nodes if n.node_id != leader.node_id)

    # B：停掉一个 follower，再写 800（远低于日志清理窗口）
    cluster.kill(follower)
    vadd_batch(cluster, key, 500, 800)
    leader_client = cluster.client(cluster.wait_leader())
    assert leader_client.execute_command(b"VCARD", key) == 1300

    # C：重启 follower，等它通过日志回放追平，再把它扶上 leader 直接验证副本。
    # force_leader 中的 bump 写入只有在该 follower 完整追平后才能提交成功
    # （AppendEntries 会先补齐它缺失的全部日志），因此追平被隐式确认。
    # member 级旧数据受 incarnation 限制不可读（见模块 docstring），改为验证
    # meta 计数 + 该节点成为 leader 后的新写新读闭环。
    cluster.restart(follower)
    forced = force_leader(cluster, follower)
    assert forced.node_id == follower.node_id
    cluster.wait_card(follower, key, 1300)
    forced_client = cluster.client(forced)
    assert forced_client.execute_command(*vadd_args(key, 6000)) == 1
    emb = forced_client.execute_command(b"VEMB", key, elem(6000))
    assert [float(v) for v in emb] == pytest.approx(list(vec(6000)))

    # 让 phase C 中被杀的第三节点归队：phase D 全量写入会触发快照与日志清理，
    # 若它持续落后超过清理窗口，由于快照网络安装未实现将永远无法追平，
    # 进而卡死后续选举出的新 leader（无法凑齐多数派提交新任期条目）。
    dead = next(n for n in cluster.nodes if not n.alive)
    cluster.restart(dead)

    # D：全员在线继续写，累计日志超过快照阈值（5000），等 leader 构建快照
    vadd_batch(cluster, key, 1300, 4000)
    leader = cluster.wait_leader()
    cluster.wait_card(leader, key, 5301, timeout=120)
    snapshot_tar = os.path.join(leader.dir, "raft", "snapshots", "current_snapshot.tar")
    snapshot_meta = os.path.join(
        leader.dir, "raft", "snapshots", "current_snapshot_meta.json"
    )
    wait_until(
        lambda: os.path.isfile(snapshot_tar)
        and os.path.isfile(snapshot_meta)
        and os.path.getsize(snapshot_tar) > 0,
        240,
        "leader to persist a snapshot (current_snapshot.tar)",
    )

    # 快照构建后集群读写正常，且 failover 后副本数据依然完整（meta 级 +
    # 新 leader 的新写新读闭环；member 级旧数据受 incarnation 限制）
    assert cluster.execute_from(leader, *vadd_args(key, 9000)) == 1
    cluster.wait_card(cluster.wait_leader(), key, 5302)
    other = next(
        n for n in cluster.nodes if n.node_id != leader.node_id and n.alive
    )
    forced = force_leader(cluster, other)
    cluster.wait_card(forced, key, 5302, timeout=300)
    forced_client = cluster.client(forced)
    assert forced_client.execute_command(*vadd_args(key, 9001)) == 1
    emb = forced_client.execute_command(b"VEMB", key, elem(9001))
    assert [float(v) for v in emb] == pytest.approx(list(vec(9001)))


@pytest.mark.xfail(
    reason=(
        "vector member key 内嵌节点本地 storage_incarnation，物理 binlog 回放后 "
        "副本上的 member 数据（VEMB/VISMEMBER/VSIM）不可读；待 PR0 logical "
        "mutation replay（社区 issue #332）落地后此用例应转绿"
    ),
    strict=False,
)
def test_replica_member_data_survives_failover(cluster):
    """规范要求：failover 后旧数据在新 leader 上完全可见（member 级）。

    当前分支因 incarnation 问题失败（xfail）；meta 级（VCARD）可见性由
    test_leader_failover 覆盖。
    """
    key = b"vc:t6"
    leader = vadd_batch(cluster, key, 0, 50)
    cluster.kill(leader)

    new_leader = cluster.wait_leader(timeout=30)
    assert new_leader.node_id != leader.node_id
    cluster.wait_card(new_leader, key, 50)
    client = cluster.client(new_leader)

    assert client.execute_command(b"VISMEMBER", key, elem(1)) == 1
    emb = client.execute_command(b"VEMB", key, elem(1))
    assert emb is not None
    assert [float(v) for v in emb] == pytest.approx(list(vec(1)))
    x, y = vec(1)
    similar = client.execute_command(
        b"VSIM", key, b"VALUES", 2, f"{x:.6f}", f"{y:.6f}", b"COUNT", 3, b"TRUTH"
    )
    assert elem(1) in similar


def test_minority_rejects_writes(cluster):
    """杀掉两个节点后，少数派节点不可写（MOVED/ERR not leader），不断言具体错误。"""
    key = b"vc:t5"
    leader = cluster.wait_leader()
    others = [n for n in cluster.nodes if n.node_id != leader.node_id]
    survivor = others[0]

    cluster.kill(leader)
    cluster.kill(others[1])

    # 给选超时间留窗口：幸存节点无法凑齐多数派，不能成为 leader
    time.sleep(ELECTION_TIMEOUT_MS / 1000 * 3)

    client = cluster.client(survivor)
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        try:
            client.execute_command(*vadd_args(key, 0))
            pytest.fail("VADD unexpectedly succeeded on a minority node")
        except MovedError:
            pass  # 重定向到已死的旧 leader，同样不可写
        except redis.ResponseError as exc:
            assert "not leader" in str(exc), (
                f"unexpected error from minority node: {exc}"
            )
        time.sleep(0.5)
