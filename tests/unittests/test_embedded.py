# pylint: disable=protected-access
"""Unit tests for the embedded broker (``radical.orbit.embedded``) and the
``EndpointRuntime(embedded=True)`` wiring.

The embedded broker is a regular broker: operator-placed cert/key/token
(redirected into ``tmp_path`` here), auth on, nothing ever written under
the config dir.  The end-to-end test drives a real uvicorn server on a
daemon thread and registers the hosting runtime against it — the same
pattern as ``test_runtime.py``'s ``_RunningBroker`` harness, but through
the production code path.
"""

import os
import socket
import subprocess
import threading

import pytest

from radical.orbit import utils


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
    """Throw-away self-signed cert+key in *tmp_path* (skip w/o openssl)."""
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
def isolated(tmp_path, monkeypatch, self_signed):
    """Operator-placed cert/key/token in a tmp config dir; env cleared.

    Yields the tmp config dir.  Mirrors the read-only-config contract:
    the files exist before any broker starts, and tests assert nothing
    new appears.
    """
    cert, key = self_signed
    cfg = tmp_path / 'orbit'
    cfg.mkdir()
    (cfg / 'broker_cert.pem').write_bytes(cert.read_bytes())
    (cfg / 'broker_key.pem').write_bytes(key.read_bytes())
    os.chmod(cfg / 'broker_key.pem', 0o600)
    (cfg / 'broker.token').write_text('sekret-token\n')
    os.chmod(cfg / 'broker.token', 0o600)

    monkeypatch.setattr(utils, 'DEFAULT_DIR', cfg)
    monkeypatch.setattr(utils, 'CERT_FILE',  cfg / 'broker_cert.pem')
    monkeypatch.setattr(utils, 'KEY_FILE',   cfg / 'broker_key.pem')
    monkeypatch.setattr(utils, 'TOKEN_FILE', cfg / 'broker.token')
    for var in (utils.ENV_URL, utils.ENV_CERT, utils.ENV_KEY, utils.ENV_TOKEN):
        monkeypatch.delenv(var, raising=False)
    return cfg


@pytest.fixture
def local_embedded(monkeypatch):
    """Redirect the runtime's EmbeddedBroker to loopback + ephemeral port.

    Keeps the end-to-end tests hermetic (no fqdn DNS, no port-8000
    contention); the production default (0.0.0.0:8000) is covered by the
    port-selection unit tests.
    """
    import radical.orbit.embedded as emb

    class _LocalEB(emb.EmbeddedBroker):
        def __init__(self, **kw):
            kw.setdefault('host', '127.0.0.1')
            kw.setdefault('port', 0)
            super().__init__(**kw)

    monkeypatch.setattr(emb, 'EmbeddedBroker', _LocalEB)
    return _LocalEB


def _embedded_threads():
    return [t for t in threading.enumerate()
            if t.name == 'orbit-embedded-broker' and t.is_alive()]


# ---------------------------------------------------------------------------
# EmbeddedBroker
# ---------------------------------------------------------------------------

def test_embedded_start_stop(isolated):
    from radical.orbit.embedded import EmbeddedBroker
    eb  = EmbeddedBroker(host='127.0.0.1', port=0)
    url = eb.start()
    try:
        assert url == eb.url
        assert url.startswith('https://127.0.0.1:')
        assert not url.endswith(':0')                 # real bound port
        assert eb._server.started
        assert _embedded_threads()
    finally:
        eb.stop()
    assert not _embedded_threads()
    eb.stop()                                         # idempotent no-op


def test_embedded_default_port_fallback(isolated, monkeypatch, caplog):
    from radical.orbit.embedded import EmbeddedBroker

    # Occupy a port of our own choosing and make it the "default".
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.bind(('127.0.0.1', 0))
    blocker.listen(1)
    taken = blocker.getsockname()[1]
    monkeypatch.setattr(EmbeddedBroker, '_DEFAULT_PORT', taken)

    try:
        eb = EmbeddedBroker(host='127.0.0.1')          # port=None → fallback
        with caplog.at_level('WARNING', logger='radical.orbit.embedded'):
            url = eb.start()
        try:
            assert not url.endswith(f':{taken}')       # landed elsewhere
            assert 'in use' in caplog.text
        finally:
            eb.stop()

        # An *explicit* busy port must fail, not fall back.
        with pytest.raises(OSError):
            EmbeddedBroker(host='127.0.0.1', port=taken).start()
    finally:
        blocker.close()


def test_embedded_missing_token_fails_closed(isolated, monkeypatch):
    from radical.orbit.embedded import EmbeddedBroker
    (isolated / 'broker.token').unlink()

    eb = EmbeddedBroker(host='127.0.0.1', port=0)
    with pytest.raises(ValueError, match='ingress auth token required') as ei:
        eb.start()
    assert 'token_urlsafe' in str(ei.value)            # carries TOKEN_RECIPE
    assert not _embedded_threads()                     # nothing leaked
    assert not (isolated / 'broker.token').exists()    # nothing written


def test_embedded_server_setup_failure_closes_socket(isolated, monkeypatch):
    # A failure *after* the Broker ctor (uvicorn Config/Server/thread setup)
    # must also release the pre-bound socket, not just Broker-ctor failures.
    import uvicorn
    from radical.orbit.embedded import EmbeddedBroker

    def _boom(*a, **kw):
        raise RuntimeError('injected uvicorn setup failure')

    monkeypatch.setattr(uvicorn, 'Config', _boom)
    eb = EmbeddedBroker(host='127.0.0.1', port=0)
    with pytest.raises(RuntimeError, match='injected uvicorn setup failure'):
        eb.start()
    assert eb._sock is None                            # socket released
    assert not _embedded_threads()


def test_embedded_binds_ipv6_literal(isolated):
    from radical.orbit.embedded import EmbeddedBroker
    probe = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        probe.bind(('::1', 0))
    except OSError:
        pytest.skip("no IPv6 loopback available")
    finally:
        probe.close()

    eb = EmbeddedBroker(host='::1', port=0)
    sock = eb._bind()                                  # AF_INET6, no OSError
    try:
        assert sock.family == socket.AF_INET6
        assert sock.getsockname()[1] > 0
    finally:
        sock.close()


def test_embedded_missing_cert_fails_closed(isolated):
    from radical.orbit.embedded import EmbeddedBroker
    (isolated / 'broker_cert.pem').unlink()

    eb = EmbeddedBroker(host='127.0.0.1', port=0)
    with pytest.raises(ValueError, match='TLS cert required'):
        eb.start()
    assert not _embedded_threads()


# ---------------------------------------------------------------------------
# EndpointRuntime(embedded=True)
# ---------------------------------------------------------------------------

def test_runtime_embedded_end_to_end(isolated, local_embedded):
    from radical.orbit.runtime import EndpointRuntime

    before = sorted(os.listdir(isolated))
    rt = EndpointRuntime(embedded=True, name='embed-e2e')
    try:
        assert rt.broker_url.startswith('https://127.0.0.1:')
        assert rt.embedded_broker is not None
        assert rt._app.state.broker_url == rt.broker_url

        rt.start(wait=True, timeout=15.0)
        assert rt.wait_registered(timeout=15.0)
        assert 'embed-e2e' in rt.topology()            # self-registered
    finally:
        rt.stop()

    assert not _embedded_threads()                     # broker torn down
    assert sorted(os.listdir(isolated)) == before      # config dir untouched


def test_runtime_embedded_excludes_broker_url(isolated, local_embedded):
    from radical.orbit.runtime import EndpointRuntime
    with pytest.raises(ValueError, match='mutually exclusive'):
        EndpointRuntime(broker_url='wss://x:1', embedded=True)
    assert not _embedded_threads()


def test_runtime_embedded_ignores_env_url(isolated, local_embedded,
                                          monkeypatch):
    from radical.orbit.runtime import EndpointRuntime
    monkeypatch.setenv(utils.ENV_URL, 'https://ambient-env-host:9999')
    rt = EndpointRuntime(embedded=True)
    try:
        assert 'ambient-env-host' not in rt.broker_url
    finally:
        rt.stop()
    assert not _embedded_threads()


def test_runtime_embedded_init_failure_stops_broker(isolated, local_embedded,
                                                    monkeypatch):
    # A failure in the init tail *after* the embed must stop the embedded
    # server before the ctor exception propagates.  The first cert
    # resolution happens inside the Broker ctor; fail the second (the
    # runtime's own).
    from radical.orbit.runtime import EndpointRuntime

    real  = utils.resolve_broker_cert
    calls = {'n': 0}

    def _flaky(cli=None):
        calls['n'] += 1
        if calls['n'] >= 2:
            raise ValueError('injected post-embed failure')
        return real(cli=cli)

    monkeypatch.setattr(utils, 'resolve_broker_cert', _flaky)
    with pytest.raises(ValueError, match='injected post-embed failure'):
        EndpointRuntime(embedded=True)
    assert not _embedded_threads()
