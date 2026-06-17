#!/usr/bin/env python3
"""End-to-end notification reproducer that runs entirely on localhost.

Spawns a bridge + a single edge as subprocesses, submits one tiny job
through the ``psij`` plugin (``local`` executor), and asserts that a
terminal ``job_status`` notification reaches the BridgeClient via SSE
within a small timeout.

The point is to exercise the full Plugin -> EdgeService.send_notification
-> WS -> Bridge._broadcast_event -> SSE -> BridgeClient._listen_sse
-> registered-callback path on a single machine, with no Dragon, no
SLURM, no tunnel — so a regression in that path can be reproduced and
bisected in seconds rather than minutes.

Usage:
    # As a pytest test (skips if HOME setup is hostile to subprocess)
    pytest tests/integration/test_notifications_local.py -v -s

    # As a standalone reproducer (exit 0 = pass, 1 = fail).  Suitable
    # for ``git bisect run``:
    python tests/integration/test_notifications_local.py
"""
import logging
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


REPO_ROOT  = Path(__file__).resolve().parents[2]
BIN_BRIDGE = REPO_ROOT / 'bin' / 'radical-edge-bridge.py'
BIN_EDGE   = REPO_ROOT / 'bin' / 'radical-edge-service.py'

EDGE_NAME       = 'test-edge-local'
NOTIF_TIMEOUT_S = 30.0   # generous: covers slow PsiJ local startup


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"port {host}:{port} did not open within {timeout}s")


def _drain(stream, prefix: str) -> threading.Thread:
    """Forward a subprocess stream to our stderr with a label, line-buffered."""
    def _run():
        try:
            for line in iter(stream.readline, ''):
                if not line:
                    break
                sys.stderr.write(f"[{prefix}] {line}")
                sys.stderr.flush()
        except Exception:
            pass
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


def _make_self_signed(certdir: Path) -> tuple:
    """Generate a one-shot self-signed cert+key for the test.  openssl is
    a hard requirement on every host that runs the bridge anyway, so
    this doesn't add a new dep."""
    cert = certdir / 'cert.pem'
    key  = certdir / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.chmod(key, 0o600)
    return cert, key


def run_test() -> int:
    tmpdir = tempfile.mkdtemp(prefix='radical-edge-test-')
    cert, key = _make_self_signed(Path(tmpdir))
    cert_path, key_path = str(cert), str(key)

    port = _free_port()
    bridge_url = f'https://localhost:{port}'
    # Sanitize env: strip inherited RADICAL_* vars that might point at a
    # different bridge / cert from a previous unrelated session.
    env = {k: v for k, v in os.environ.items() if not k.startswith('RADICAL_')}
    env.update(
        RADICAL_BRIDGE_URL=bridge_url,
        RADICAL_BRIDGE_CERT=cert_path,
        RADICAL_EDGE_LOG_LEVEL='DEBUG',
    )

    bridge_proc = subprocess.Popen(
        [sys.executable, str(BIN_BRIDGE),
         '--host', 'localhost', '--port', str(port),
         '--cert', cert_path, '--key', key_path,
         '--plugins', ''],   # bridge needs no plugins for this test
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    _drain(bridge_proc.stdout, 'bridge')

    edge_proc = None
    try:
        _wait_for_port('localhost', port, timeout=10.0)

        edge_proc = subprocess.Popen(
            [sys.executable, str(BIN_EDGE),
             '--name', EDGE_NAME,
             '--url', bridge_url,
             '--plugins', 'psij',
             '--log-level', 'DEBUG'],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        _drain(edge_proc.stdout, 'edge')

        # Late imports so logging picks up the env above.
        from radical.edge.client import BridgeClient

        client = BridgeClient(url=bridge_url, cert=cert_path)
        # Make SSE listener errors visible (otherwise they're DEBUG-only).
        logging.getLogger('radical.edge').setLevel(logging.DEBUG)
        logging.getLogger('radical.edge.client').setLevel(logging.DEBUG)

        # Wait for the edge to register over WS.  Use the topology
        # callback the BridgeClient already exposes.
        edge_seen = threading.Event()
        def on_topology(edges):
            if EDGE_NAME in edges:
                edge_seen.set()
        client.register_topology_callback(on_topology)

        if not edge_seen.wait(timeout=15.0):
            # fall back to polling list_edges in case the topology
            # event was missed (e.g. SSE listener not yet warm).
            for _ in range(50):
                try:
                    if EDGE_NAME in client.list_edges():
                        edge_seen.set()
                        break
                except Exception:
                    pass
                time.sleep(0.1)
        if not edge_seen.is_set():
            print("FAIL: edge did not register on bridge", file=sys.stderr)
            return 1

        edge = client.get_edge_client(EDGE_NAME)
        psij = edge.get_plugin('psij')

        notifications = []
        terminal_seen = threading.Event()

        def on_job_status(edge_id, plugin_name, topic, data):
            notifications.append((topic, data))
            state = (data or {}).get('state') or (data or {}).get('status')
            if state in ('DONE', 'COMPLETED', 'FAILED',
                         'CANCELED', 'CANCELLED'):
                terminal_seen.set()

        psij.register_notification_callback(on_job_status, topic='job_status')

        job_spec = {
            'executable': '/bin/echo',
            'arguments' : ['notification-roundtrip-ok'],
        }
        psij.submit_job(job_spec, executor='local')

        if not terminal_seen.wait(timeout=NOTIF_TIMEOUT_S):
            print(f"FAIL: no terminal job_status within {NOTIF_TIMEOUT_S}s",
                  file=sys.stderr)
            print(f"      received {len(notifications)} notification(s):",
                  file=sys.stderr)
            for n in notifications[-10:]:
                print(f"      - {n}", file=sys.stderr)
            return 1

        print(f"PASS: terminal notification received "
              f"after {len(notifications)} update(s)", file=sys.stderr)
        return 0

    finally:
        for proc, name in ((edge_proc, 'edge'), (bridge_proc, 'bridge')):
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2)


# Pytest entry point ----------------------------------------------------------

def test_notifications_local():
    import pytest
    try:
        subprocess.run(['openssl', 'version'], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pytest.skip('openssl not available')

    rc = run_test()
    assert rc == 0, "notification path is broken — see captured output"


if __name__ == '__main__':
    sys.exit(run_test())
