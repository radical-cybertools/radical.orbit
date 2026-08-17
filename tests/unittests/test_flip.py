"""Final-flip smoke tests.

Guards the flip itself: the broker-era ``bin/`` entry points now construct the
new stack (``Broker`` + ``EndpointRuntime``), and the broker's ``gateway``
module serves the Explorer shell and a broker-hosted plugin's
dynamically-registered ``ui_module`` (the ``iri.<endpoint>`` UI path — packaged
static JS wins, then the host's dynamic modules).
"""

import importlib.util
import subprocess
import sys
import threading

from importlib.machinery import SourceFileLoader
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parents[2]


def _load_bin(filename: str):
    """Load a ``bin/`` script (hyphenated, extensionless) as a module."""
    path   = REPO / 'bin' / filename
    modname = filename.replace('-', '_').replace('.py', '')
    loader = SourceFileLoader(modname, str(path))
    spec   = importlib.util.spec_from_loader(modname, loader)
    mod    = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


def _have_openssl() -> bool:
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


# ---------------------------------------------------------------------------
# bin/ entry points: argparse + object construction (no server run)
# ---------------------------------------------------------------------------

def test_broker_entrypoint_constructs_broker(monkeypatch, tmp_path):
    mod = _load_bin('radical-orbit-broker.py')
    captured = {}

    def fake_broker(**kw):
        captured.update(kw)
        return MagicMock()

    monkeypatch.setattr(mod, 'Broker', fake_broker)
    monkeypatch.setenv('RADICAL_ORBIT_LOG_FILE', str(tmp_path / 'broker.log'))
    monkeypatch.setattr(sys, 'argv',
                        ['radical-orbit-broker.py', '--no-auth', '--no-gateway',
                         '--port', '9123', '--plugins', 'sysinfo'])
    mod.main()

    assert captured['auth']    is False          # --no-auth -> auth=False
    assert captured['gateway'] is False          # --no-gateway -> gateway=False
    assert captured['port']    == 9123
    assert captured['plugins'] == 'sysinfo'


def test_broker_entrypoint_gateway_on_by_default(monkeypatch, tmp_path):
    mod = _load_bin('radical-orbit-broker.py')
    captured = {}
    monkeypatch.setattr(mod, 'Broker',
                        lambda **kw: captured.update(kw) or MagicMock())
    monkeypatch.setenv('RADICAL_ORBIT_LOG_FILE', str(tmp_path / 'broker.log'))
    monkeypatch.setattr(sys, 'argv', ['radical-orbit-broker.py', '--no-auth'])
    mod.main()
    assert captured['gateway'] is True


def test_endpoint_entrypoint_constructs_runtime(monkeypatch, tmp_path):
    mod = _load_bin('radical-orbit-endpoint.py')
    captured = {}

    def fake_runtime(**kw):
        captured.update(kw)
        return MagicMock()

    # Don't block on the shutdown event; don't install real signal handlers.
    class _ImmediateEvent:
        def set(self):                 pass
        def wait(self, timeout=None):  return True

    monkeypatch.setattr(mod, 'EndpointRuntime', fake_runtime)
    monkeypatch.setattr(mod.threading, 'Event', _ImmediateEvent)
    monkeypatch.setattr(mod.signal, 'signal', lambda *a, **k: None)
    monkeypatch.setenv('RADICAL_ORBIT_LOG_FILE', str(tmp_path / 'ep.log'))
    monkeypatch.setattr(sys, 'argv',
                        ['radical-orbit-endpoint.py', '--url', 'ws://hub:8000',
                         '--name', 'ep1', '--plugins', 'sysinfo,psij',
                         '--tunnel', 'forward', '--tunnel-via', 'login7'])
    mod.main()

    assert captured['broker_url'] == 'ws://hub:8000'
    assert captured['name']       == 'ep1'
    assert captured['plugins']    == ['sysinfo', 'psij']
    assert captured['tunnel']     == 'forward'
    assert captured['tunnel_via'] == 'login7'


# ---------------------------------------------------------------------------
# gateway: Explorer + dynamic ui_module serving (over the broker seam)
# ---------------------------------------------------------------------------

@pytest.fixture
def broker_client(tmp_path, monkeypatch):
    if not _have_openssl():
        pytest.skip("openssl not available")
    from radical.orbit import utils
    from radical.orbit.broker import Broker, BrokerTuning

    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')

    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True)

    broker = Broker(cert=str(cert), key=str(key), auth=False,
                    tuning=BrokerTuning(grace=0.05, ping_timeout=0.05))
    return broker


def test_gateway_serves_explorer_html(broker_client):
    with TestClient(broker_client.app) as client:
        r = client.get('/')
        assert r.status_code == 200
        assert 'html' in r.text.lower()


def test_gateway_serves_dynamic_ui_module(broker_client, monkeypatch):
    # A broker-hosted plugin registered at runtime (e.g. ``iri.nersc``) ships a
    # ui_module the packaged static tree does not carry — serve it from the
    # host's dynamic modules via the broker seam.
    async def _fake_modules():
        return {'iri.nersc': '/* dynamic UI */'}
    monkeypatch.setattr(broker_client, 'get_ui_modules', _fake_modules)
    with TestClient(broker_client.app) as client:
        r = client.get('/plugins/iri.nersc.js')
        assert r.status_code == 200
        assert r.text == '/* dynamic UI */'
        assert 'javascript' in r.headers['content-type']


def test_gateway_unknown_plugin_js_is_404(broker_client, monkeypatch):
    async def _no_modules():
        return {}
    monkeypatch.setattr(broker_client, 'get_ui_modules', _no_modules)
    with TestClient(broker_client.app) as client:
        assert client.get('/plugins/nope.js').status_code == 404


# ---------------------------------------------------------------------------
# gateway auth: traversal normalization (ported from the deleted broker suite)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gateway_auth_dispatch_normalizes_traversal(tmp_path, monkeypatch):
    if not _have_openssl():
        pytest.skip("openssl not available")
    from starlette.requests  import Request
    from starlette.responses import JSONResponse
    from radical.orbit import utils
    from radical.orbit.broker import Broker

    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')
    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True)

    broker = Broker(cert=str(cert), key=str(key), token='s3cret', auth=True)
    gateway = broker._gateway

    async def _passed(_req):
        return JSONResponse({'ok': True})

    def _req(path):
        return Request({'type': 'http', 'method': 'GET', 'path': path,
                        'headers': []})

    # Traversal out of /plugins -> normalized to /endpoint/list -> gated (401).
    resp = await gateway._auth_dispatch(_req('/plugins/../endpoint/list'),
                                        _passed)
    assert resp.status_code == 401

    # A genuine /plugins/ asset stays exempt (passes through without a token).
    resp = await gateway._auth_dispatch(_req('/plugins/orbit.js'), _passed)
    assert resp.status_code == 200
