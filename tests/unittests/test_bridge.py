"""Tests for the :class:`radical.orbit.bridge.Bridge` class.

The bridge logic moved from ``bin/radical-orbit-bridge.py`` (module-level
globals) to a class with instance attributes — these tests construct a
``Bridge`` and poke its ``.pending`` / ``.endpoints`` directly.
"""

import asyncio
import os
import subprocess
import time

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
    on startup doesn't touch the dev's real ``~/.radical/orbit/``.
    """
    from radical.orbit import utils
    monkeypatch.setattr(utils, 'URL_FILE', tmp_path / 'bridge.url')
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'bridge.token')

    cert, key = self_signed

    def _build(**kwargs):
        from radical.orbit import Bridge
        # Default these bridge tests to auth-off; dedicated auth coverage
        # lives in test_bridge_auth.py.  Callers override with
        # ``no_auth=False`` + ``token=...``.
        defaults = dict(cert=str(cert), key=str(key), no_auth=True)
        defaults.update(kwargs)
        return Bridge(**defaults)

    return _build


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_bridge_disconnect_isolation(make_bridge):
    """Disconnecting one endpoint fails only its in-flight requests.

    Instance state (``bridge.pending`` / ``bridge.endpoints``) replaces what
    used to be module-level globals on the old bin script.
    """
    bridge = make_bridge()
    client = TestClient(bridge.app)

    # Pre-seed a pending request for an unrelated endpoint.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    fut_b = loop.create_future()
    bridge.pending["test_req_b"] = (fut_b, "endpoint_b")
    bridge.endpoint_ws["endpoint_b"]      = "mock_ws"

    with client.websocket_connect("/register") as websocket:
        websocket.send_json({
            "type":      "register",
            "endpoint_name": "endpoint_a",
            "endpoint":  {"type": "radical.orbit"},
        })

        fut_a = loop.create_future()
        bridge.pending["test_req_a"] = (fut_a, "endpoint_a")

        # Closing the WS context manager triggers the disconnect
        # cleanup path for endpoint_a only.

    # endpoint_a's pending was failed with HTTPException(503).
    assert "test_req_a" not in bridge.pending
    assert fut_a.done()
    assert isinstance(fut_a.exception(), HTTPException)

    # endpoint_b's pending is untouched.
    assert "test_req_b" in bridge.pending
    assert not fut_b.done()


# ---------------------------------------------------------------------------
# Registration / topology — regression coverage
#
# A rename once collapsed the live-socket map (``endpoint_ws``) into the
# serializable topology dict (``endpoints``).  Registering an endpoint then
# stored a WebSocket inside ``endpoints``, and the on-register topology
# broadcast crashed with "Object of type WebSocket is not JSON serializable".
# These tests exercise the register / list / disconnect path that the single
# pre-existing test did not cover.
# ---------------------------------------------------------------------------

def _wait_until(predicate, timeout=5.0, interval=0.02):
    """Poll *predicate* until true or *timeout*; returns the final bool.

    The bridge processes WebSocket frames on a background task, so the test
    thread must wait for the register side effects to land.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _register(ws, name="thinkie", plugins=("psij", "rhapsody")):
    ws.send_json({
        "type":          "register",
        "endpoint_name": name,
        "endpoint":      {"type": "radical.orbit"},
        "plugins":       {p: {} for p in plugins},
    })


def _registered(bridge, name="thinkie", n_plugins=2):
    """True once the register handler has fully populated the topology entry.

    Registration sets ``endpoint_ws`` first and fills the topology
    ``plugins`` last, with no ``await`` in between — so syncing on the
    populated ``plugins`` dict avoids observing a half-applied register from
    the test thread.
    """
    entry = bridge.endpoints["endpoints"].get(name)
    return bool(entry) and len(entry.get("plugins", {})) == n_plugins


def test_register_keeps_socket_out_of_topology(make_bridge):
    """The WebSocket lives in ``endpoint_ws``; the topology dict stays
    JSON-serializable.  Regression for the socket/topology collision."""
    import json

    bridge = make_bridge()
    client = TestClient(bridge.app)

    with client.websocket_connect("/register") as ws:
        _register(ws)

        assert _wait_until(lambda: _registered(bridge)), \
            "endpoint never registered (register handler likely crashed)"

        # the live socket is in endpoint_ws, NEVER in the topology dict
        assert "thinkie" in bridge.endpoint_ws
        assert set(bridge.endpoints.keys()) == {"bridge", "endpoints"}

        # the actual crash: the topology dict must serialize cleanly
        json.dumps(bridge.endpoints)
        assert set(bridge.endpoints["endpoints"]["thinkie"]["plugins"]) \
            == {"psij", "rhapsody"}


def test_endpoint_list_rest_serializes_after_register(make_bridge):
    """``POST /endpoint/list`` (serializes the topology) and ``GET /endpoints``
    (reads ``endpoint_ws`` for the connected flag) work after a register."""
    bridge = make_bridge()
    client = TestClient(bridge.app)

    with client.websocket_connect("/register") as ws:
        _register(ws)
        assert _wait_until(lambda: _registered(bridge))

        resp = client.post("/endpoint/list")
        assert resp.status_code == 200          # would be 500 if a WS leaked in
        topo = resp.json()["data"]
        assert "thinkie" in topo["endpoints"]

        resp = client.get("/endpoints")
        assert resp.status_code == 200
        entry = {e["name"]: e for e in resp.json()["endpoints"]}["thinkie"]
        assert entry["connected"] is True
        assert sorted(entry["plugins"]) == ["psij", "rhapsody"]


def test_disconnect_removes_endpoint_from_both_maps(make_bridge):
    """On disconnect the endpoint is dropped from both the socket map and the
    topology dict, and the topology stays serializable."""
    import json

    bridge = make_bridge()
    client = TestClient(bridge.app)

    with client.websocket_connect("/register") as ws:
        _register(ws)
        assert _wait_until(lambda: _registered(bridge))
        # leaving the context closes the socket -> disconnect cleanup

    assert _wait_until(lambda: "thinkie" not in bridge.endpoints["endpoints"])
    assert "thinkie" not in bridge.endpoint_ws
    json.dumps(bridge.endpoints)


# ---------------------------------------------------------------------------
# _strip_headers — bridge credential must not leak to endpoint plugins
# ---------------------------------------------------------------------------

def _request_with_headers(headers: dict):
    """Build a minimal Starlette ``Request`` carrying *headers*."""
    from starlette.requests import Request
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    return Request({"type": "http", "method": "GET", "path": "/",
                    "headers": raw})


def test_strip_headers_drops_auth_keeps_other_cookies():
    """The auth header and the bridge auth cookie are stripped; unrelated
    cookies and other headers are forwarded untouched."""
    from radical.orbit import Bridge, utils

    req = _request_with_headers({
        "authorization": "Bearer secret",
        "cookie": f"{utils.AUTH_COOKIE}=secret; session=abc; theme=dark",
        "content-type": "application/json",
    })
    out = {k.lower(): v for k, v in Bridge._strip_headers(req).items()}

    assert "authorization" not in out
    assert out["content-type"] == "application/json"
    assert utils.AUTH_COOKIE not in out["cookie"]
    assert "session=abc" in out["cookie"]
    assert "theme=dark"  in out["cookie"]


def test_strip_headers_drops_cookie_header_when_only_auth():
    """When the auth cookie is the sole cookie, the whole Cookie header goes."""
    from radical.orbit import Bridge, utils

    req = _request_with_headers({"cookie": f"{utils.AUTH_COOKIE}=secret"})
    out = {k.lower(): v for k, v in Bridge._strip_headers(req).items()}

    assert "cookie" not in out
