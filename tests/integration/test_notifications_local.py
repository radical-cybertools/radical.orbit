#!/usr/bin/env python3
"""End-to-end notification reproducer that runs entirely on localhost.

Spawns a broker + a single endpoint as subprocesses, submits one tiny job
through the ``psij`` plugin (``local`` executor), and asserts that a
terminal ``job_status`` notification reaches the BrokerClient via SSE
within a small timeout.

The point is to exercise the full Plugin -> EndpointService.send_notification
-> WS -> Broker._broadcast_event -> SSE -> BrokerClient._listen_sse
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
BIN_BROKER = REPO_ROOT / 'bin' / 'radical-orbit-broker.py'
BIN_ENDPOINT   = REPO_ROOT / 'bin' / 'radical-orbit-endpoint.py'

ENDPOINT_NAME       = 'test-endpoint-local'
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
    a hard requirement on every host that runs the broker anyway, so
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
    tmpdir = tempfile.mkdtemp(prefix='orbit-test-')
    cert, key = _make_self_signed(Path(tmpdir))
    cert_path, key_path = str(cert), str(key)

    port = _free_port()
    broker_url = f'https://localhost:{port}'
    # Sanitize env: strip inherited RADICAL_* vars that might point at a
    # different broker / cert from a previous unrelated session.
    env = {k: v for k, v in os.environ.items() if not k.startswith('RADICAL_')}
    env.update(
        RADICAL_ORBIT_BROKER_URL=broker_url,
        RADICAL_ORBIT_BROKER_CERT=cert_path,
        RADICAL_ORBIT_LOG_LVL='DEBUG',
    )

    broker_proc = subprocess.Popen(
        [sys.executable, str(BIN_BROKER),
         '--host', 'localhost', '--port', str(port),
         '--cert', cert_path, '--key', key_path,
         '--plugins', ''],   # broker needs no plugins for this test
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    _drain(broker_proc.stdout, 'broker')

    endpoint_proc = None
    client = None
    try:
        _wait_for_port('localhost', port, timeout=10.0)

        endpoint_proc = subprocess.Popen(
            [sys.executable, str(BIN_ENDPOINT),
             '--name', ENDPOINT_NAME,
             '--url', broker_url,
             '--plugins', 'psij',
             '--log-level', 'DEBUG'],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        _drain(endpoint_proc.stdout, 'endpoint')

        # Late imports so logging picks up the env above.
        from radical.orbit import EndpointRuntime

        client = EndpointRuntime(broker_url=broker_url, cert=cert_path)
        client.start(wait=True)
        logging.getLogger('radical.orbit').setLevel(logging.DEBUG)
        logging.getLogger('radical.orbit.runtime').setLevel(logging.DEBUG)

        # Wait for the endpoint to register over WS.  Use the topology
        # callback the runtime consumer already exposes.
        endpoint_seen = threading.Event()
        def on_topology(endpoints):
            if ENDPOINT_NAME in endpoints:
                endpoint_seen.set()
        client.register_topology_callback(on_topology)

        if not endpoint_seen.wait(timeout=15.0):
            # fall back to polling the topology in case the change
            # event was missed.
            for _ in range(50):
                try:
                    if ENDPOINT_NAME in client.topology():
                        endpoint_seen.set()
                        break
                except Exception:
                    pass
                time.sleep(0.1)
        if not endpoint_seen.is_set():
            print("FAIL: endpoint did not register on broker", file=sys.stderr)
            return 1

        psij = client.get_plugin(ENDPOINT_NAME, 'psij')

        notifications = []
        terminal_seen = threading.Event()

        def on_job_status(endpoint_id, plugin_name, topic, data):
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
        try:
            if client is not None:
                client.stop()
        except Exception:
            pass
        for proc, name in ((endpoint_proc, 'endpoint'), (broker_proc, 'broker')):
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
