"""Tests for the :class:`radical.edge.bridge.Bridge` class.

The bridge logic moved from ``bin/radical-edge-bridge.py`` (module-level
globals) to a class with instance attributes — these tests construct a
``Bridge`` and poke its ``.pending`` / ``.edges`` directly.
"""

import asyncio
import os
import subprocess

import pytest

from fastapi import HTTPException
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _have_openssl() -> bool:
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


@pytest.fixture
def self_signed(tmp_path):
    """Generate a throw-away self-signed cert+key pair.  Returns
    ``(cert_path, key_path)``.  Skipped when openssl isn't available."""
    if not _have_openssl():
        pytest.skip("openssl not available")

    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True,
    )
    os.chmod(key, 0o600)
    return cert, key


@pytest.fixture
def make_bridge(self_signed, tmp_path, monkeypatch):
    """Factory fixture: returns a callable that builds a fresh Bridge.

    Resolver paths are redirected to *tmp_path* so the bridge.url write
    on startup doesn't touch the dev's real ``~/.radical/edge/``.
    """
    from radical.edge import utils
    monkeypatch.setattr(utils, 'URL_FILE', tmp_path / 'bridge.url')

    cert, key = self_signed

    def _build(**kwargs):
        from radical.edge import Bridge
        defaults = dict(cert=str(cert), key=str(key))
        defaults.update(kwargs)
        return Bridge(**defaults)

    return _build


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_bridge_disconnect_isolation(make_bridge):
    """Disconnecting one edge fails only its in-flight requests.

    Instance state (``bridge.pending`` / ``bridge.edges``) replaces what
    used to be module-level globals on the old bin script.
    """
    bridge = make_bridge()
    client = TestClient(bridge.app)

    # Pre-seed a pending request for an unrelated edge.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    fut_b = loop.create_future()
    bridge.pending["test_req_b"] = (fut_b, "edge_b")
    bridge.edges["edge_b"]        = "mock_ws"

    with client.websocket_connect("/register") as websocket:
        websocket.send_json({
            "type":      "register",
            "edge_name": "edge_a",
            "endpoint":  {"type": "radical.edge"},
        })

        fut_a = loop.create_future()
        bridge.pending["test_req_a"] = (fut_a, "edge_a")

        # Closing the WS context manager triggers the disconnect
        # cleanup path for edge_a only.

    # edge_a's pending was failed with HTTPException(503).
    assert "test_req_a" not in bridge.pending
    assert fut_a.done()
    assert isinstance(fut_a.exception(), HTTPException)

    # edge_b's pending is untouched.
    assert "test_req_b" in bridge.pending
    assert not fut_b.done()
