"""WebSocket permessage-deflate switch: modes, precedence, auto probe."""

import asyncio
import os
import subprocess

import pytest

from radical.orbit import utils


ENV     = utils.ENV_WS_COMPRESSION
ENV_RTT = utils.ENV_WS_COMPRESSION_RTT_MS


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
    monkeypatch.delenv(ENV,     raising=False)
    monkeypatch.delenv(ENV_RTT, raising=False)


# ---------------------------------------------------------------------------
# resolver
# ---------------------------------------------------------------------------

def test_resolve_default():
    assert utils.resolve_ws_compression() == 'auto'
    assert utils.resolve_ws_compression(default='on') == 'on'


@pytest.mark.parametrize('raw, mode', [
    ('1', 'on'), ('true', 'on'), ('ON', 'on'), (' yes ', 'on'),
    ('0', 'off'), ('false', 'off'), ('Off', 'off'), ('no', 'off'),
    ('auto', 'auto'), ('', 'auto')])
def test_resolve_env(monkeypatch, raw, mode):
    monkeypatch.setenv(ENV, raw)
    assert utils.resolve_ws_compression() == mode


@pytest.mark.parametrize('value, mode', [
    (True, 'on'), (False, 'off'), ('on', 'on'), ('off', 'off'),
    ('auto', 'auto')])
def test_resolve_explicit_wins_over_env(monkeypatch, value, mode):
    for env in ('on', 'off', 'auto'):
        monkeypatch.setenv(ENV, env)
        assert utils.resolve_ws_compression(value) == mode


def test_resolve_invalid_raises(monkeypatch):
    with pytest.raises(ValueError):
        utils.resolve_ws_compression('maybe')
    monkeypatch.setenv(ENV, 'maybe')
    with pytest.raises(ValueError, match=ENV):
        utils.resolve_ws_compression()


def test_resolve_rtt_ms(monkeypatch):
    assert utils.resolve_ws_compression_rtt_ms() == utils.WS_COMPRESSION_RTT_MS
    monkeypatch.setenv(ENV_RTT, '25')
    assert utils.resolve_ws_compression_rtt_ms() == 25.0
    assert utils.resolve_ws_compression_rtt_ms(3) == 3.0
    monkeypatch.setenv(ENV_RTT, 'fast')
    with pytest.raises(ValueError, match=ENV_RTT):
        utils.resolve_ws_compression_rtt_ms()


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


def test_broker_default_accepts(broker_deflate):
    assert broker_deflate() is True


@pytest.mark.parametrize('env, accepts', [
    ('off', False), ('0', False), ('on', True), ('auto', True)])
def test_broker_env(broker_deflate, monkeypatch, env, accepts):
    monkeypatch.setenv(ENV, env)
    assert broker_deflate() is accepts


def test_broker_tuning_overrides_env(broker_deflate, monkeypatch):
    monkeypatch.setenv(ENV, 'on')
    assert broker_deflate(ws_compression=False) is False
    assert broker_deflate(ws_compression='off') is False
    monkeypatch.setenv(ENV, 'off')
    assert broker_deflate(ws_compression=True) is True


# ---------------------------------------------------------------------------
# runtime: offer decision + websockets.connect kwargs
# ---------------------------------------------------------------------------

def _runtime(**tuning):
    from radical.orbit.runtime import EndpointRuntime
    return EndpointRuntime(broker_url='http://127.0.0.1:1', token=None,
                           **tuning)


def _offer(rt, probe_ms=None):
    """Run the per-connect decision with an injected probe; record calls."""
    calls = []

    async def _probe(host, port):
        calls.append((host, port))
        return probe_ms

    rt._probe_connect_ms = _probe
    return asyncio.run(rt._offer_deflate(rt._ws_url())), calls


@pytest.fixture(autouse=True)
def _no_env_proxy(monkeypatch):
    for k in ('http_proxy', 'https_proxy', 'all_proxy', 'ws_proxy',
              'wss_proxy', 'socks_proxy', 'no_proxy'):
        monkeypatch.delenv(k, raising=False)
        monkeypatch.delenv(k.upper(), raising=False)


def test_runtime_auto_via_proxy_skips_probe(monkeypatch):
    from radical.orbit import runtime as rt_mod
    if rt_mod._ws_get_proxy is None:
        pytest.skip('websockets without proxy support')
    monkeypatch.setenv('https_proxy', 'http://proxy.example:3128')
    offer, calls = _offer(_runtime(), probe_ms=0.1)
    assert offer is True
    assert calls == []
    monkeypatch.setenv('no_proxy', '127.0.0.1')
    offer, calls = _offer(_runtime(), probe_ms=0.1)
    assert offer is False
    assert calls == [('127.0.0.1', 1)]


def test_runtime_default_auto():
    assert _runtime()._ws_compression == 'auto'


def test_runtime_tuning_overrides_env(monkeypatch):
    monkeypatch.setenv(ENV, 'on')
    assert _runtime(ws_compression=False)._ws_compression == 'off'
    monkeypatch.setenv(ENV, 'off')
    assert _runtime(ws_compression=True)._ws_compression  == 'on'
    assert _runtime(ws_compression='auto')._ws_compression == 'auto'
    assert _runtime()._ws_compression == 'off'


@pytest.mark.parametrize('mode, expected', [('on', True), ('off', False)])
def test_runtime_fixed_modes_skip_probe(mode, expected):
    offer, calls = _offer(_runtime(ws_compression=mode), probe_ms=999.0)
    assert offer is expected
    assert calls == []


@pytest.mark.parametrize('probe_ms, expected', [
    (0.3, False), (9.9, False), (10.0, True), (80.0, True), (None, False)])
def test_runtime_auto_by_connect_time(probe_ms, expected):
    offer, calls = _offer(_runtime(), probe_ms=probe_ms)
    assert offer is expected
    assert calls == [('127.0.0.1', 1)]


def test_runtime_auto_threshold_tunable(monkeypatch):
    assert _offer(_runtime(ws_compression_rtt_ms=50), 20.0)[0] is False
    monkeypatch.setenv(ENV_RTT, '5')
    assert _offer(_runtime(), 6.0)[0] is True


def test_runtime_auto_probe_real_socket():
    # A real (failing) probe: nothing listens on port 1 -> None -> off.
    rt = _runtime()
    assert asyncio.run(rt._probe_connect_ms('127.0.0.1', 1)) is None
    assert asyncio.run(rt._offer_deflate(rt._ws_url())) is False


def test_runtime_connect_kwargs():
    from websockets.extensions.permessage_deflate import \
        ClientPerMessageDeflateFactory
    rt = _runtime()
    off = rt._ws_connect_kwargs(None, False)
    assert off['compression'] is None and off['extensions'] is None
    on = rt._ws_connect_kwargs(None, True)
    assert on['compression'] is None
    [ext] = on['extensions']
    assert isinstance(ext, ClientPerMessageDeflateFactory)
    assert ext.compress_settings == {'level': 1}
