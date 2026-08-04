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
Vector Set three-node Raft cluster integration test.

Vector commands are deliberately unsupported in cluster mode until logical
Raft mutation replay is implemented. This test verifies that leader and
follower nodes reject representative vector reads and writes with the same
error before routing or a read barrier.

Run explicitly because cluster tests are slow:

    cargo build --bin kiwi
    KIWI_RUN_CLUSTER_TESTS=1 python3 -m pytest tests/python/test_vector_cluster.py -v

Requires grpcurl, redis-py, and target/debug/kiwi (override with KIWI_BINARY).
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import time

import pytest
import redis

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
KIWI_BINARY = os.environ.get(
    "KIWI_BINARY", os.path.join(REPO_ROOT, "target", "debug", "kiwi")
)
GRPCURL = shutil.which("grpcurl")
EXPECTED_ERROR = "vector commands are not supported in cluster mode yet"

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


def wait_until(fn, timeout, desc, interval=0.2):
    deadline = time.monotonic() + timeout
    while True:
        result = fn()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for: {desc}")
        time.sleep(interval)


def grpc_call(addr, service, method, payload, timeout=10):
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
    output = proc.stdout.strip()
    if not output:
        return {}
    try:
        return json.loads(output)
    except json.JSONDecodeError:
        return None


def grpc_metrics(node):
    return grpc_call(
        node.raft_addr,
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
        self.binary = binary
        ports = free_ports(node_count * 2)
        self.nodes = []
        for index in range(node_count):
            node_dir = os.path.join(str(base_dir), f"node{index + 1}")
            os.makedirs(node_dir, exist_ok=True)
            self.nodes.append(
                ClusterNode(
                    index + 1,
                    ports[index],
                    ports[node_count + index],
                    node_dir,
                )
            )
        self._clients = {}

    def _write_config(self, node):
        with open(node.conf_path, "w", encoding="utf-8") as conf:
            conf.write(
                "\n".join(
                    [
                        "binding 127.0.0.1",
                        f"port {node.resp_port}",
                        "runtime-network_threads 1",
                        "runtime-storage-threads 2",
                        "db-instance-num 1",
                        f"data-dir {node.dir}/db",
                        f"log-dir {node.dir}/logs",
                        f"raft-node-id {node.node_id}",
                        f"raft-addr {node.raft_addr}",
                        f"raft-resp-addr {node.resp_addr}",
                        f"raft-data-dir {node.dir}/raft",
                        "raft-heartbeat-interval-ms 100",
                        "raft-election-timeout-min-ms 300",
                        "raft-election-timeout-max-ms 600",
                        "",
                    ]
                )
            )

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

    def start_node(self, node, timeout=30):
        self._write_config(node)
        node.log_fd = open(node.log_path, "ab")
        node.proc = subprocess.Popen(
            [self.binary, "--config", node.conf_path],
            cwd=node.dir,
            stdout=node.log_fd,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=dict(os.environ, RUST_LOG="info"),
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

    def initialize(self):
        payload = {
            "nodes": [
                {
                    "node_id": node.node_id,
                    "raft_addr": node.raft_addr,
                    "resp_addr": node.resp_addr,
                }
                for node in self.nodes
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
        def find_leader():
            for node in self.nodes:
                metrics = grpc_metrics(node)
                if metrics and metrics.get("isLeader"):
                    return node
            return None

        return wait_until(find_leader, timeout, "a leader to be elected")

    def shutdown(self):
        for node in self.nodes:
            if node.proc is not None and node.proc.poll() is None:
                os.killpg(node.proc.pid, signal.SIGKILL)
                node.proc.wait(timeout=10)
            node.alive = False
            if node.log_fd is not None:
                node.log_fd.close()
        for client in self._clients.values():
            client.close()
        self._clients.clear()


@pytest.fixture(scope="session", autouse=True)
def redis_client():
    """Override the global single-node fixture; this module owns its cluster."""
    yield None


@pytest.fixture()
def cluster(tmp_path):
    instance = VectorCluster(tmp_path, KIWI_BINARY)
    try:
        instance.start_all()
        instance.initialize()
        instance.wait_leader()
    except Exception:
        instance.shutdown()
        raise
    yield instance
    instance.shutdown()


def test_vector_commands_are_rejected_before_cluster_routing(cluster):
    leader = cluster.wait_leader()
    follower = next(node for node in cluster.nodes if node.node_id != leader.node_id)
    commands = [
        (b"VADD", b"vectors", b"VALUES", 2, 1, 0, b"member"),
        (b"VREM", b"vectors", b"member"),
        (b"VSIM", b"vectors", b"VALUES", 2, 1, 0),
        (b"VCARD", b"vectors"),
        (b"VDIM", b"vectors"),
        (b"VEMB", b"vectors", b"member"),
        (b"VINFO", b"vectors"),
        (b"VISMEMBER", b"vectors", b"member"),
    ]

    for node in (leader, follower):
        client = cluster.client(node)
        for command in commands:
            with pytest.raises(redis.ResponseError) as exc_info:
                client.execute_command(*command)
            assert str(exc_info.value) == EXPECTED_ERROR
