"""WebSocket permessage-deflate switch: tuning > env > off (default)."""

import os
import subprocess

import pytest

from radical.orbit import utils


ENV = utils.ENV_WS_COMPRESSION


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)


# ---------------------------------------------------------------------------
# resolver
# ---------------------------------------------------------------------------

def test_resolve_default_off():
    assert utils.resolve_ws_compression() is False


@pytest.mark.parametrize('raw', ['1', 'true', 'ON', ' yes '])
def test_resolve_env_on(monkeypatch, raw):
    monkeypatch.setenv(ENV, raw)
    assert utils.resolve_ws_compression() is True


@pytest.mark.parametrize('raw', ['0', 'false', 'Off', 'no', ''])
def test_resolve_env_off(monkeypatch, raw):
    monkeypatch.setenv(ENV, raw)
    assert utils.resolve_ws_compression() is False


def test_resolve_explicit_wins_over_env(monkeypatch):
    monkeypatch.setenv(ENV, 'on')
    assert utils.resolve_ws_compression(False) is False
    monkeypatch.setenv(ENV, 'off')
    assert utils.resolve_ws_compression(True) is True


def test_resolve_env_invalid_raises(monkeypatch):
    monkeypatch.setenv(ENV, 'maybe')
    with pytest.raises(ValueError, match=ENV):
        utils.resolve_ws_compression()


# ---------------------------------------------------------------------------
# broker: uvicorn ws_per_message_deflate
# ---------------------------------------------------------------------------

@pytest.fixture
def self_signed(tmp_path):
    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    try:
        subprocess.run(
            ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
             '-keyout', str(key), '-out', str(cert),
             '-days', '1', '-subj', '/CN=localhost'],
            check=True, capture_output=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        pytest.skip("openssl not available")
    os.chmod(key, 0o600)
    return cert, key


@pytest.fixture
def broker_deflate(self_signed, tmp_path, monkeypatch):
    """Build a Broker and return its uvicorn ``ws_per_message_deflate``."""
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')
    cert, key = self_signed

    def _build(**tuning_kw):
        from radical.orbit.broker import Broker, BrokerTuning
        broker = Broker(cert=str(cert), key=str(key), auth=False,
                        tuning=BrokerTuning(**tuning_kw))
        return broker._uvicorn_kwargs()['ws_per_message_deflate']

    return _build


def test_broker_default_off(broker_deflate):
    assert broker_deflate() is False


def test_broker_env_on(broker_deflate, monkeypatch):
    monkeypatch.setenv(ENV, 'on')
    assert broker_deflate() is True


def test_broker_tuning_overrides_env(broker_deflate, monkeypatch):
    monkeypatch.setenv(ENV, 'on')
    assert broker_deflate(ws_compression=False) is False
    monkeypatch.setenv(ENV, 'off')
    assert broker_deflate(ws_compression=True) is True


# ---------------------------------------------------------------------------
# runtime: websockets.connect(compression=...)
# ---------------------------------------------------------------------------

def _connect_compression(**tuning):
    from radical.orbit.runtime import EndpointRuntime
    rt = EndpointRuntime(broker_url='http://127.0.0.1:1', token=None,
                         **tuning)
    return rt._ws_connect_kwargs(None)['compression']


def test_runtime_default_off():
    assert _connect_compression() is None


def test_runtime_env_on(monkeypatch):
    monkeypatch.setenv(ENV, 'yes')
    assert _connect_compression() == 'deflate'


def test_runtime_tuning_overrides_env(monkeypatch):
    monkeypatch.setenv(ENV, 'on')
    assert _connect_compression(ws_compression=False) is None
    monkeypatch.setenv(ENV, 'off')
    assert _connect_compression(ws_compression=True) == 'deflate'
