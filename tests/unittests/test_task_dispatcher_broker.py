"""Broker-integration tests for the task dispatcher (M7 transport port).

Drives a **real** :class:`~radical.orbit.broker.Broker` (hosting the dispatcher)
under uvicorn on an ephemeral port, plus a **real** ``EndpointRuntime`` child
that serves a fake plugin route.  Exercises:

- the in-process broker caller the dispatcher uses (``call_threadsafe`` → a real
  child endpoint → response), end-to-end;
- the dispatcher being wired with the broker caller + event tap on the host;
- gateway HTTP → broker-hosted dispatcher round-trip (pre-flip item 1).

No test sleeps for more than ~1 s; liveness/backoff knobs are injected tiny.
"""

import json
import subprocess
import threading
import time

import httpx
import pytest

from radical.orbit.plugin_base import Plugin
from radical.orbit.plugin_session_base import PluginSession


# ---------------------------------------------------------------------------
# TLS material
# ---------------------------------------------------------------------------

def _have_openssl() -> bool:
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


@pytest.fixture
def self_signed(tmp_path):
    if not _have_openssl():
        pytest.skip("openssl not available")
    import os
    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True)
    os.chmod(key, 0o600)
    return cert, key


# ---------------------------------------------------------------------------
# A tiny served plugin standing in for a pilot's rhapsody/psij
# ---------------------------------------------------------------------------

class _FakePilot(Plugin):
    plugin_name = 'fake_pilot'
    version     = '0.0.1'

    def __init__(self, app):
        super().__init__(app, 'fake_pilot')
        self.add_route_get('ping', self._ping)

    async def _ping(self, request):
        return {'pong': True}


class _FakePsij(Plugin):
    """A psij-shaped served plugin: base ``register_session`` + a canned
    ``submit_tunneled/{sid}`` that echoes what crossed the wire.

    Registered under a distinct ``plugin_name`` (so the real ``PluginPSIJ``
    stays in the class registry) but mounted at instance name ``psij`` so its
    namespace is ``/psij`` — exactly what a real ``PSIJClient`` formats.
    """
    plugin_name   = 'fake_psij'
    session_class = PluginSession
    version       = '0.0.1'

    def __init__(self, app, instance_name: str = 'psij'):
        super().__init__(app, instance_name)
        self.add_route_post('submit_tunneled/{sid}', self._submit_tunneled)

    async def _submit_tunneled(self, request):
        data = await request.json()
        return {'job_id':        'j.1',
                'native_id':     'n.1',
                'echo_tunnel':   data.get('tunnel'),
                'echo_executor': data.get('executor')}


# ---------------------------------------------------------------------------
# Broker-under-uvicorn harness + runtime factory (mirrors test_gateway.py)
# ---------------------------------------------------------------------------

class _RunningBroker:
    def __init__(self, broker):
        import uvicorn
        self.broker = broker
        config = uvicorn.Config(
            broker.app, host='127.0.0.1', port=0, log_level='error',
            ws_ping_interval=20.0, ws_ping_timeout=20.0)
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self):
        self._thread.start()
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if self._server.started and self._server.servers:
                socks = self._server.servers[0].sockets
                if socks:
                    self.port = socks[0].getsockname()[1]
                    return self
            time.sleep(0.02)
        raise RuntimeError("broker server did not start")

    @property
    def url(self):
        return 'http://127.0.0.1:%d' % self.port

    def stop(self):
        self._server.should_exit = True
        self._thread.join(timeout=10.0)


@pytest.fixture
def harness(self_signed, tmp_path, monkeypatch):
    from radical.orbit import utils
    monkeypatch.setattr(utils, 'URL_FILE',   tmp_path / 'broker.url')
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')
    monkeypatch.delenv('RADICAL_ORBIT_BROKER_TOKEN', raising=False)
    # Keep the dispatcher's durable store off $HOME.
    monkeypatch.setattr(
        'radical.orbit.plugin_task_dispatcher._DEFAULT_STATE_ROOT',
        tmp_path / 'td_state')
    monkeypatch.setattr(
        'radical.orbit.plugin_task_dispatcher._DEFAULT_SCRATCH_ROOT',
        tmp_path / 'td_scratch')
    cert, key = self_signed

    servers, runtimes = [], []

    def make_broker(**kw):
        from radical.orbit.broker import Broker, BrokerTuning
        tuning = BrokerTuning(grace=2.0)
        for _k in list(kw):
            if hasattr(tuning, _k):
                setattr(tuning, _k, kw.pop(_k))
        defaults = dict(cert=str(cert), key=str(key), no_auth=True, tuning=tuning)
        defaults.update(kw)
        srv = _RunningBroker(Broker(**defaults)).start()
        servers.append(srv)
        return srv

    def make_runtime(url, wait=True, serve=None, **kw):
        from radical.orbit.runtime import EndpointRuntime
        defaults = dict(broker_url=url, token=None, ping_interval=1.0,
                        ping_timeout=3.0, backoff_start=0.05, backoff_max=0.2)
        defaults.update(kw)
        rt = EndpointRuntime(**defaults)
        runtimes.append(rt)
        for p in (serve or []):
            rt.serve(p)                    # mount before connecting
        rt.start(wait=wait, timeout=10.0)
        return rt

    yield make_broker, make_runtime

    for rt in runtimes:
        try:    rt.stop()
        except Exception:
            pass
    for srv in servers:
        try:    srv.stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def _dispatcher(srv):
    return srv.broker._plugin_host._plugins['task_dispatcher']


def test_dispatcher_wired_with_caller_and_tap(harness):
    make_broker, _ = harness
    srv = make_broker(plugins='task_dispatcher')
    td = _dispatcher(srv)
    # The dispatcher is broker-hosted and reaches endpoints via the broker
    # caller; the raw event tap is wired for child task events.
    assert td._broker_caller is srv.broker.caller
    assert td._broker_tap is not None


def test_dispatcher_calls_child_via_caller(harness):
    """End-to-end: the dispatcher's own caller reaches a real child endpoint."""
    make_broker, make_runtime = harness
    srv = make_broker(plugins='task_dispatcher')
    make_runtime(srv.url, name='ep', plugins=['fake_pilot'])

    td   = _dispatcher(srv)
    # Drive the exact seam the dispatcher's `_call` uses (call_threadsafe →
    # routing loop → child endpoint → response), from this thread.
    fut  = td._broker_caller.call_threadsafe('ep', 'GET', '/fake_pilot/ping',
                                             timeout=10.0)
    resp = fut.result(timeout=10.0)
    assert int(resp['status']) == 200
    body = resp['body']
    if isinstance(body, str):
        body = body.encode()
    assert json.loads(body)['pong'] is True


def test_dispatcher_drives_real_psij_helper_via_caller(harness):
    """End-to-end: the dispatcher builds a REAL ``PSIJClient`` wired to the
    broker caller (``CallerHTTP``) and drives its async cores against a real
    child endpoint — register + ``asubmit_tunneled`` — with the payload and
    response crossing the routing loop intact.  This is the convergence the
    proxies were replaced by: one helper implementation, caller-backed.
    """
    import asyncio

    make_broker, make_runtime = harness
    srv = make_broker(plugins='task_dispatcher')
    make_runtime(srv.url, name='ep', serve=[_FakePsij])

    td = _dispatcher(srv)

    async def _drive():
        psij = await td._get_psij_client('ep')      # real PSIJClient over caller
        assert psij is not None
        assert psij.sid                              # session registered
        return await psij.asubmit_tunneled(
            {'executable': '/bin/true'}, 'local', 'none')

    result = asyncio.run(_drive())
    assert result['job_id']        == 'j.1'
    assert result['echo_tunnel']   == 'none'         # payload shape crossed intact
    assert result['echo_executor'] == 'local'
    # The client is cached for reuse on the next dispatcher call.
    assert ('ep', 'psij', None) in td._child_clients


def test_gateway_http_to_hosted_dispatcher(harness):
    """Gateway HTTP → broker-hosted dispatcher route (pre-flip item 1)."""
    make_broker, _ = harness
    srv = make_broker(plugins='task_dispatcher')
    with httpx.Client(timeout=10.0) as c:
        r = c.get('%s/broker/task_dispatcher/pools' % srv.url)
    assert r.status_code == 200, r.text
    assert r.json()['pools'] == {}     # no sessions/pools declared yet


def test_gateway_unknown_hosted_route_404(harness):
    make_broker, _ = harness
    srv = make_broker(plugins='task_dispatcher')
    with httpx.Client(timeout=10.0) as c:
        r = c.get('%s/broker/task_dispatcher/nope' % srv.url)
    assert r.status_code == 404
