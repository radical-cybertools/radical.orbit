"""Tests for the bridge ingress auth gate (shared bearer token).

Covers the HTTP middleware (401 without a token, 200 with the bearer header,
ungated UI shell), the ``POST /auth`` cookie mint, and the ``/register`` WS
token check.  ``--no-auth`` bypass is verified too.
"""

import os
import subprocess

import pytest

from fastapi.testclient import TestClient


def _have_openssl() -> bool:
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


@pytest.fixture
def make_bridge(tmp_path, monkeypatch):
    """Factory: build a Bridge with redirected resolver paths.

    Defaults to auth ON with a fixed token; pass ``no_auth=True`` for the
    bypass case.
    """
    if not _have_openssl():
        pytest.skip("openssl not available")

    from radical.orbit import utils
    monkeypatch.setattr(utils, 'URL_FILE',   tmp_path / 'bridge.url')
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'bridge.token')
    monkeypatch.delenv(utils.ENV_TOKEN,   raising=False)
    monkeypatch.delenv(utils.ENV_NO_AUTH, raising=False)

    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True)
    os.chmod(key, 0o600)

    def _build(**kwargs):
        from radical.orbit import Bridge
        defaults = dict(cert=str(cert), key=str(key), token='s3cret',
                        no_auth=False)
        defaults.update(kwargs)
        return Bridge(**defaults)

    return _build


TOK = {'Authorization': 'Bearer s3cret'}


def test_capability_route_requires_token(make_bridge):
    client = TestClient(make_bridge().app)
    assert client.post('/endpoint/list').status_code == 401
    assert client.post('/endpoint/list', headers=TOK).status_code == 200


def test_wrong_token_rejected(make_bridge):
    client = TestClient(make_bridge().app)
    r = client.post('/endpoint/list', headers={'Authorization': 'Bearer nope'})
    assert r.status_code == 401


def test_ui_shell_and_plugins_are_ungated(make_bridge):
    client = TestClient(make_bridge().app)
    # Never 401 — the shell must load so the user can supply the token.
    assert client.get('/').status_code != 401
    assert client.get('/plugins/anything.js').status_code != 401


def test_auth_route_sets_httponly_cookie(make_bridge):
    client = TestClient(make_bridge().app)
    r = client.post('/auth', headers=TOK)
    assert r.status_code == 200
    set_cookie = r.headers.get('set-cookie', '')
    assert 'orbit_bridge_token=' in set_cookie
    assert 'HttpOnly' in set_cookie


def test_register_requires_token(make_bridge):
    bridge = make_bridge()
    client = TestClient(bridge.app)
    with client.websocket_connect('/register') as ws:
        ws.send_json({'type': 'register', 'endpoint_name': 'e1',
                      'endpoint': {}})
        msg = ws.receive_json()
        assert msg['type'] == 'error'
        assert 'token' in msg['message'].lower()


def test_register_with_token_succeeds(make_bridge):
    bridge = make_bridge()
    client = TestClient(bridge.app)
    with client.websocket_connect('/register') as ws:
        ws.send_json({'type': 'register', 'endpoint_name': 'e1',
                      'endpoint': {}, 'token': 's3cret'})
        # A successful register triggers a topology broadcast back to the
        # just-registered socket (not an error).
        msg = ws.receive_json()
        assert msg['type'] == 'topology'
        # Registered while the socket is live (cleanup runs on context exit).
        assert 'e1' in bridge.endpoints['endpoints']


def test_no_auth_bypass(make_bridge):
    client = TestClient(make_bridge(no_auth=True).app)
    # No token, still reachable.
    assert client.post('/endpoint/list').status_code == 200
    with client.websocket_connect('/register') as ws:
        ws.send_json({'type': 'register', 'endpoint_name': 'e2',
                      'endpoint': {}})
        msg = ws.receive_json()
        assert msg['type'] == 'topology'
