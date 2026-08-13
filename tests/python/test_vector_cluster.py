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

"""Required three-node proof that Vector commands fail closed in cluster mode."""

import argparse
import json
import os
import sys
from pathlib import Path

EXPECTED_ERROR = "vector commands are not supported in cluster mode yet"
EXPECTED_CAPABILITIES = ["vector_set_storage_v1", "snapshot_schema_v2"]
COMMAND_CASES = [
    ("vadd", (b"VADD", b"vectors", b"VALUES", 2, 1, 0, b"member", b"NOQUANT")),
    ("vrem", (b"VREM", b"vectors", b"member")),
    ("vsim", (b"VSIM", b"vectors", b"VALUES", 2, 1, 0)),
    ("vcard", (b"VCARD", b"vectors")),
    ("vdim", (b"VDIM", b"vectors")),
    ("vemb", (b"VEMB", b"vectors", b"member")),
    ("vinfo", (b"VINFO", b"vectors")),
    ("vismember", (b"VISMEMBER", b"vectors", b"member")),
]
EXPECTED_NODE_IDS = {
    f"tests/python/test_vector_cluster.py::test_vector_command_is_rejected_before_cluster_routing[{role}-{name}]"
    for role in ("leader", "follower")
    for name, _ in COMMAND_CASES
}


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def validate_collection(path):
    actual = {
        line.strip()
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.startswith("tests/python/test_vector_cluster.py::")
    }
    if actual != EXPECTED_NODE_IDS:
        missing = sorted(EXPECTED_NODE_IDS - actual)
        extra = sorted(actual - EXPECTED_NODE_IDS)
        raise SystemExit(f"cluster collection drift: missing={missing}, extra={extra}")


def validate_summary(path):
    summary = _read_json(path)
    expected_count = len(EXPECTED_NODE_IDS)
    if summary.get("collected") != expected_count or summary.get("passed") != expected_count:
        raise SystemExit(f"cluster totals mismatch: {summary}")
    for name in ("failed", "skipped", "xfailed", "xpassed", "deselected"):
        if summary.get(name) != 0:
            raise SystemExit(f"cluster result is not fail-closed: {summary}")


def validate_cleanup(path):
    cleanup = _read_json(path)
    processes = cleanup.get("processes", [])
    if cleanup.get("schema") != "kiwi-vector-cluster-cleanup/v1" or len(processes) != 3:
        raise SystemExit(f"cluster cleanup evidence is incomplete: {cleanup}")
    for process in processes:
        if not process.get("term_sent") or not process.get("waited") or not process.get("process_group_gone"):
            raise SystemExit(f"cluster process cleanup failed: {process}")


def main(argv):
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate-collection")
    group.add_argument("--validate-summary")
    group.add_argument("--validate-cleanup")
    args = parser.parse_args(argv)
    if args.validate_collection:
        validate_collection(args.validate_collection)
    elif args.validate_summary:
        validate_summary(args.validate_summary)
    else:
        validate_cleanup(args.validate_cleanup)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


import signal
import socket
import subprocess
import time

import pytest
import redis

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def _required_path(name):
    value = os.environ.get(name)
    if not value:
        pytest.fail(f"required cluster input is missing: {name}", pytrace=False)
    path = os.path.abspath(value)
    if not os.path.isfile(path) or not os.access(path, os.X_OK):
        pytest.fail(f"required cluster executable is unavailable: {name}={path}", pytrace=False)
    return path


def _require_runtime_inputs():
    if os.environ.get("KIWI_RUN_CLUSTER_TESTS") != "1":
        pytest.fail("required cluster mode needs KIWI_RUN_CLUSTER_TESTS=1", pytrace=False)
    return _required_path("KIWI_BINARY"), _required_path("KIWI_GRPCURL")


def wait_until(fn, timeout, desc, interval=0.2):
    deadline = time.monotonic() + timeout
    while True:
        result = fn()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for: {desc}")
        time.sleep(interval)


def process_group_gone(pgid):
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def grpc_call(grpcurl, addr, service, method, payload, timeout=10):
    try:
        proc = subprocess.run(
            [grpcurl, "-plaintext", "-d", json.dumps(payload), addr, f"{service}/{method}"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
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


def free_ports(count):
    ports = set()
    while len(ports) < count:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            ports.add(sock.getsockname()[1])
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

    @property
    def raft_addr(self):
        return f"127.0.0.1:{self.raft_port}"

    @property
    def resp_addr(self):
        return f"127.0.0.1:{self.resp_port}"


class VectorCluster:
    def __init__(self, base_dir, binary, grpcurl, node_count=3):
        self.binary = binary
        self.grpcurl = grpcurl
        self.pid_registry = os.environ.get("KIWI_VECTOR_CLUSTER_PID_REGISTRY")
        self.cleanup_evidence = os.environ.get("KIWI_VECTOR_CLUSTER_CLEANUP")
        ports = free_ports(node_count * 2)
        self.nodes = []
        for index in range(node_count):
            node_dir = os.path.join(str(base_dir), f"node{index + 1}")
            os.makedirs(node_dir, exist_ok=True)
            self.nodes.append(
                ClusterNode(index + 1, ports[index], ports[node_count + index], node_dir)
            )
        self._clients = {}

    def _write_json(self, path, payload):
        if not path:
            return
        destination = Path(path)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, destination)

    def _publish_pid_registry(self):
        self._write_json(
            self.pid_registry,
            {
                "processes": [
                    {"node_id": node.node_id, "pid": node.proc.pid, "pgid": os.getpgid(node.proc.pid)}
                    for node in self.nodes
                    if node.proc is not None and node.proc.poll() is None
                ]
            },
        )

    def _write_config(self, node):
        with open(node.conf_path, "w", encoding="utf-8") as conf:
            conf.write(
                "\n".join(
                    [
                        "binding 127.0.0.1",
                        f"port {node.resp_port}",
                        "runtime-network-threads 1",
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
        self._publish_pid_registry()

        def ping_ok():
            if node.proc.poll() is not None:
                raise AssertionError(
                    f"node {node.node_id} exited early (rc={node.proc.returncode}); see {node.log_path}"
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
        wait_until(
            lambda: grpc_call(
                self.grpcurl,
                self.nodes[0].raft_addr,
                "kiwi.raft.v1.RaftAdminService",
                "GetNodeCapabilities",
                {},
            ),
            30,
            "cluster admin RPC readiness",
        )
        result = grpc_call(
            self.grpcurl,
            self.nodes[0].raft_addr,
            "kiwi.raft.v1.RaftAdminService",
            "Initialize",
            payload,
        )
        assert result is not None, "Initialize result is unknown; refusing to replay it"
        assert result.get("response", {}).get("success") is True, f"Initialize failed: {result}"

    def assert_node_capabilities(self):
        for node in self.nodes:
            result = grpc_call(
                self.grpcurl,
                node.raft_addr,
                "kiwi.raft.v1.RaftAdminService",
                "GetNodeCapabilities",
                {},
            )
            assert result is not None, f"GetNodeCapabilities failed for node {node.node_id}"
            assert result.get("capabilities") == EXPECTED_CAPABILITIES

    def wait_leader(self, timeout=30):
        def find_leader():
            for node in self.nodes:
                metrics = grpc_call(
                    self.grpcurl,
                    node.raft_addr,
                    "kiwi.raft.v1.RaftMetricsService",
                    "Metrics",
                    {},
                )
                if metrics and metrics.get("isLeader"):
                    return node
            return None

        return wait_until(find_leader, timeout, "a leader to be elected")

    def shutdown(self, grace_seconds=5):
        for client in self._clients.values():
            client.close()
        self._clients.clear()

        records = []
        for node in self.nodes:
            if node.proc is None:
                continue
            record = {
                "node_id": node.node_id,
                "pid": node.proc.pid,
                "pgid": node.proc.pid,
                "term_sent": False,
                "kill_sent": False,
                "waited": False,
                "process_group_gone": False,
            }
            if node.proc.poll() is None:
                os.killpg(node.proc.pid, signal.SIGTERM)
                record["term_sent"] = True
            records.append((node, record))

        deadline = time.monotonic() + grace_seconds
        for node, _ in records:
            remaining = max(0, deadline - time.monotonic())
            try:
                node.proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                pass

        for node, record in records:
            if node.proc.poll() is None:
                os.killpg(node.proc.pid, signal.SIGKILL)
                record["kill_sent"] = True
            node.proc.wait(timeout=10)
            record["waited"] = True
            record["process_group_gone"] = wait_until(
                lambda pgid=node.proc.pid: process_group_gone(pgid),
                5,
                f"process group {node.proc.pid} to disappear",
                interval=0.1,
            )
            if node.log_fd is not None:
                node.log_fd.close()

        evidence = {"schema": "kiwi-vector-cluster-cleanup/v1", "processes": [r for _, r in records]}
        self._write_json(self.cleanup_evidence, evidence)
        self._publish_pid_registry()


@pytest.fixture(scope="session", autouse=True)
def redis_client():
    """Override the global single-node fixture; this module owns its cluster."""
    yield None


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    binary, grpcurl = _require_runtime_inputs()
    instance = VectorCluster(tmp_path_factory.mktemp("vector-cluster"), binary, grpcurl)
    try:
        instance.start_all()
        instance.initialize()
        instance.assert_node_capabilities()
        instance.wait_leader()
        yield instance
    finally:
        instance.shutdown()


VECTOR_CASE_PARAMS = [
    pytest.param(role, command_name, command, id=f"{role}-{command_name}")
    for role in ("leader", "follower")
    for command_name, command in COMMAND_CASES
]


@pytest.mark.parametrize(("role", "command_name", "command"), VECTOR_CASE_PARAMS)
def test_vector_command_is_rejected_before_cluster_routing(cluster, role, command_name, command):
    leader = cluster.wait_leader()
    node = leader if role == "leader" else next(
        candidate for candidate in cluster.nodes if candidate.node_id != leader.node_id
    )
    with pytest.raises(redis.ResponseError) as exc_info:
        cluster.client(node).execute_command(*command)
    assert str(exc_info.value) == EXPECTED_ERROR, f"{role} {command_name}"
