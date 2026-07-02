"""Tests for :class:`radical.orbit.runtime.EndpointRuntime` (M4).

The suite drives a **real** :class:`~radical.orbit.broker.Broker` under uvicorn
on an ephemeral port and connects **real** ``EndpointRuntime`` participants over
real WebSockets — the most faithful reproduction of production wiring.  Where a
building block is better exercised in isolation (the callback-dispatcher
drop-oldest counter, the dispatch.py move) it is unit-tested directly.

No test sleeps for more than ~2.5 s; liveness/backoff knobs are injected tiny.
"""

import asyncio
import subprocess
import threading
import time

import pytest

from radical.orbit               import PluginSession
from radical.orbit.plugin_base   import Plugin
from radical.orbit.client        import PluginClient


# ---------------------------------------------------------------------------
# TLS material (the broker requires a loadable cert/key even for plain-ws runs)
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
# A tiny served plugin: ping (session-less), echo (session), notify, block
# ---------------------------------------------------------------------------

class _EchoSession(PluginSession):
    async def echo(self, msg):
        return {'echo': msg}


class _EchoClient(PluginClient):
    def ping(self):
        resp = self._http.get(self._url('ping'))
        self._raise(resp, 'ping')
        return resp.json()

    def echo(self, msg):
        self._require_session()
        resp = self._http.post(self._url(f'echo/{self.sid}'),
                               json={'msg': msg})
        self._raise(resp, 'echo')
        return resp.json()


class _EchoPlugin(Plugin):
    plugin_name   = 'echo_rt'
    session_class = _EchoSession
    client_class  = _EchoClient
    version       = '0.0.1'

    def __init__(self, app):
        super().__init__(app, 'echo_rt')
        self.add_route_get('ping',        self._ping)
        self.add_route_post('echo/{sid}', self._echo)
        self.add_route_get('notify',      self._notify)
        self.add_route_get('slow/{ms}',   self._slow)
        self.add_route_get('block/{ms}',  self._block)

    async def _ping(self, request):
        return {'pong': True}

    async def _echo(self, request):
        sid  = request.path_params['sid']
        data = await request.json()
        return await self._forward(sid, _EchoSession.echo, data.get('msg'))

    async def _notify(self, request):
        await self.send_notification('demo', {'hello': 'world'})
        return {'sent': True}

    async def _slow(self, request):
        # An async sleep holds the concurrency slot WITHOUT stalling the work
        # loop — so admission of further requests still runs (503 fast-fail).
        await asyncio.sleep(int(request.path_params['ms']) / 1000.0)
        return {'slow': True}

    async def _block(self, request):
        # A synchronous sleep pins the WORK loop — the transport thread must
        # keep answering keepalive regardless (transport isolation).
        time.sleep(int(request.path_params['ms']) / 1000.0)
        return {'blocked': True}


class _LivenessPlugin(Plugin):
    '''Records the (name, liveness) pairs its on_topology_change observes and
    still drives the base reclaim-drain behavior (calls super()).'''
    plugin_name   = 'liveness_rt'
    session_class = PluginSession
    version       = '0.0.1'

    def __init__(self, app):
        super().__init__(app, 'liveness_rt')
        self.observed = []

    async def on_topology_change(self, participants):
        for name, info in participants.items():
            self.observed.append((name, (info or {}).get('liveness')))
        await super().on_topology_change(participants)


# ---------------------------------------------------------------------------
# Broker-under-uvicorn harness + runtime factory
# ---------------------------------------------------------------------------

class _RunningBroker:
    def __init__(self, broker, ws_ping_interval, ws_ping_timeout):
        import uvicorn
        self.broker = broker
        config = uvicorn.Config(
            broker.app, host='127.0.0.1', port=0, log_level='error',
            ws_ping_interval=ws_ping_interval,
            ws_ping_timeout=ws_ping_timeout)
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
    """Factory yielding (make_broker, make_runtime); tears everything down."""
    from radical.orbit import utils
    monkeypatch.setattr(utils, 'URL_FILE',   tmp_path / 'broker.url')
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')
    monkeypatch.delenv('RADICAL_ORBIT_BROKER_TOKEN', raising=False)
    monkeypatch.delenv('RADICAL_ORBIT_BROKER_URL',   raising=False)
    cert, key = self_signed

    servers = []
    runtimes = []

    def make_broker(ws_ping_interval=20.0, ws_ping_timeout=20.0, **kw):
        from radical.orbit.broker import Broker
        defaults = dict(cert=str(cert), key=str(key), no_auth=True, grace=2.0)
        defaults.update(kw)
        broker = Broker(**defaults)
        srv = _RunningBroker(broker, ws_ping_interval, ws_ping_timeout).start()
        servers.append(srv)
        return srv

    def make_runtime(url, wait=True, **kw):
        from radical.orbit.runtime import EndpointRuntime
        defaults = dict(broker_url=url, token=None, ping_interval=1.0,
                        ping_timeout=3.0, backoff_start=0.05, backoff_max=0.2)
        defaults.update(kw)
        rt = EndpointRuntime(**defaults)
        runtimes.append(rt)
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


def _wait(cond, timeout=5.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return False


def _wait_topology(rt, name, plugin=None, timeout=5.0):
    def _ok():
        topo = rt.topology()
        if name not in topo:
            return False
        if plugin is None:
            return True
        return plugin in (topo[name].get('plugins') or {})
    return _wait(_ok, timeout=timeout)


# ---------------------------------------------------------------------------
# register + ack + auto-name consumer
# ---------------------------------------------------------------------------

def test_register_ack_and_auto_named_consumer(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    rt = make_runtime(srv.url)                      # no name, no plugins
    assert rt.wait_registered(timeout=5.0)
    assert rt.name.startswith('consumer.')
    assert rt.resume_key                            # minted, non-empty
    assert rt._role() == 'consumer'
    assert _wait(lambda: rt.name in srv.broker.registry)


# ---------------------------------------------------------------------------
# name-in-use retry/backoff, then the name frees and the retry wins
# ---------------------------------------------------------------------------

def test_name_in_use_retries_until_name_frees(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='dup')
    assert a.wait_registered(timeout=5.0)

    b = make_runtime(srv.url, name='dup', wait=False)
    # b keeps hitting name-in-use while a holds the name.
    assert b.wait_registered(timeout=1.0) is False

    a.stop()                                        # clean close frees 'dup'
    assert b.wait_registered(timeout=5.0)
    assert _wait(lambda: 'dup' in srv.broker.registry)


# ---------------------------------------------------------------------------
# resume-key reconnect replaces a live socket
# ---------------------------------------------------------------------------

def test_resume_key_replaces_live_socket(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='r1', backoff_start=30.0, backoff_max=30.0)
    assert a.wait_registered(timeout=5.0)
    key = a.resume_key

    # A second participant with the same name + the resume key replaces A.
    b = make_runtime(srv.url, name='r1', resume_key=key)
    assert b.wait_registered(timeout=5.0)
    assert b.resume_key == key                      # broker returns same key
    assert _wait(lambda: 'r1' in srv.broker.registry)
    # A's socket is closed by the broker; with a 30 s backoff it does not
    # ping-pong back within the test window.


# ---------------------------------------------------------------------------
# serve a plugin end-to-end: A serves echo, B calls it via get_plugin()
# ---------------------------------------------------------------------------

def test_serve_and_call_plugin_end_to_end(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='epA', plugins=['echo_rt'])
    assert a.wait_registered(timeout=5.0)
    assert a._role() == 'endpoint'

    b = make_runtime(srv.url, name='epB')
    assert _wait_topology(b, 'epA', 'echo_rt', timeout=5.0)

    echo = b.get_plugin('epA', 'echo_rt')
    assert echo.ping() == {'pong': True}
    assert echo.echo('hi') == {'echo': 'hi'}

    # Also exercise the raw consumer call surface.
    resp = b.call('epA', 'GET', '/echo_rt/ping')
    assert resp.status_code == 200
    assert resp.json() == {'pong': True}


# ---------------------------------------------------------------------------
# pending-table timeout (and no leak: a later call still works)
# ---------------------------------------------------------------------------

def test_call_timeout_and_recovery(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='epA', plugins=['echo_rt'])
    b = make_runtime(srv.url, name='epB')
    assert _wait_topology(b, 'epA', 'echo_rt', timeout=5.0)

    with pytest.raises((asyncio.TimeoutError, TimeoutError)):
        b.call('epA', 'GET', '/echo_rt/slow/600', timeout=0.2)

    time.sleep(0.8)                                 # let the handler drain
    resp = b.call('epA', 'GET', '/echo_rt/ping')
    assert resp.status_code == 200                  # pending table not leaked


# ---------------------------------------------------------------------------
# 503 fast-fail at the request-concurrency cap (+ Retry-After)
# ---------------------------------------------------------------------------

def test_503_fast_fail_at_concurrency_cap(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='epA', plugins=['echo_rt'],
                     request_concurrency=1, accept_queue=0)
    b = make_runtime(srv.url, name='epB')
    assert _wait_topology(b, 'epA', 'echo_rt', timeout=5.0)

    # Occupy the single slot with a blocking request.
    def _occupy():
        b.call('epA', 'GET', '/echo_rt/slow/800', timeout=5.0)

    t = threading.Thread(target=_occupy, daemon=True)
    t.start()
    assert _wait(lambda: a._req_inflight >= 1, timeout=2.0)

    resp = b.call('epA', 'GET', '/echo_rt/ping', timeout=2.0)
    assert resp.status_code == 503
    assert 'retry-after' in {k.lower(): v for k, v in resp.headers.items()}
    t.join(timeout=5.0)


# ---------------------------------------------------------------------------
# event round-trip with auto-subscribe; callback fires on the callback thread
# ---------------------------------------------------------------------------

def test_event_roundtrip_on_callback_thread(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='epA', plugins=['echo_rt'])
    b = make_runtime(srv.url, name='epB')
    assert _wait_topology(b, 'epA', 'echo_rt', timeout=5.0)

    seen = []
    lock = threading.Lock()

    def _cb(endpoint, plugin, topic, data):
        with lock:
            seen.append((endpoint, plugin, topic, data,
                         threading.get_ident()))

    b.register_callback(endpoint_id='epA', plugin_name='echo_rt',
                        topic='demo', callback=_cb)
    time.sleep(0.2)                                 # let the subscribe land

    b.call('epA', 'GET', '/echo_rt/notify')
    assert _wait(lambda: len(seen) >= 1, timeout=5.0)

    endpoint, plugin, topic, data, ident = seen[0]
    assert (endpoint, plugin, topic) == ('epA', 'echo_rt', 'demo')
    assert data == {'hello': 'world'}
    # The user callback ran on the dedicated dispatcher thread — NOT the work
    # loop or the transport loop.
    assert ident == b._cb.ident
    assert ident != b._work_ident
    assert ident != b._transport_ident


# ---------------------------------------------------------------------------
# topology callback on connect + disconnect
# ---------------------------------------------------------------------------

def test_topology_callback_connect_and_disconnect(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    b = make_runtime(srv.url, name='epB')
    snaps = []
    lock  = threading.Lock()

    def _topo(participants):
        with lock:
            snaps.append(set(participants.keys()))

    b.register_topology_callback(_topo)

    a = make_runtime(srv.url, name='epA', plugins=['echo_rt'])
    assert _wait(lambda: any('epA' in s for s in snaps), timeout=5.0)

    a.stop()
    assert _wait(lambda: snaps and 'epA' not in snaps[-1], timeout=5.0)


# ---------------------------------------------------------------------------
# owner channel: broker-stamped src wins over a forged x-orbit-src header
# ---------------------------------------------------------------------------

def test_owner_channel_broker_stamped_not_spoofable(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='epA', plugins=['echo_rt'])
    b = make_runtime(srv.url, name='epB')
    assert _wait_topology(b, 'epA', 'echo_rt', timeout=5.0)

    # B forges x-orbit-src; the serving runtime overwrites it with the
    # broker-stamped identity 'epB' before the plugin records the owner.
    resp = b.call('epA', 'POST', '/echo_rt/register_session',
                  headers={'x-orbit-src': 'attacker'}, body=b'{}')
    assert resp.status_code == 200
    sid = resp.json()['sid']

    plugin = a._plugins['echo_rt']
    assert _wait(lambda: sid in plugin._session_policy, timeout=2.0)
    assert plugin._session_policy[sid]['owner'] == 'epB'    # not 'attacker'


# ---------------------------------------------------------------------------
# runtime wires on_topology_change to served plugins incl. synthesized 'lost';
# an owner-bound ephemeral session is reclaimed after the drain
# ---------------------------------------------------------------------------

def test_served_plugin_observes_lost_and_reclaims(harness):
    make_broker, make_runtime = harness
    # Aggressive broker keepalive + tiny grace so a hard-dropped consumer
    # reaches suspect then lost within the test window.
    srv = make_broker(ws_ping_interval=0.2, ws_ping_timeout=0.5, grace=0.5)

    a = make_runtime(srv.url, name='epA', plugins=['liveness_rt'],
                     ping_interval=0.2, ping_timeout=0.5)
    b = make_runtime(srv.url, name='epB',
                     ping_interval=0.2, ping_timeout=0.5)
    assert _wait_topology(b, 'epA', 'liveness_rt', timeout=5.0)

    plugin = a._plugins['liveness_rt']
    plugin.reclaim_drain = 0.2

    # B registers an owner-bound ephemeral session on epA's plugin.
    resp = b.call('epA', 'POST', '/liveness_rt/register_session', body=b'{}')
    sid = resp.json()['sid']
    assert _wait(lambda: sid in plugin._sessions, timeout=2.0)
    assert plugin._session_policy[sid]['owner'] == 'epB'
    assert _wait(lambda: ('epB', 'present') in plugin.observed, timeout=5.0)

    # Hard-drop B: abort the WS transport (TCP RST, no close frame) so the
    # broker sees a NON-clean drop -> suspect -> (grace) -> lost.  Mark B
    # stopping first so its transport loop does not reconnect.
    b._stopping = True

    def _abort():
        try:    b._ws.transport.abort()
        except Exception:
            pass
    b._transport_loop.call_soon_threadsafe(_abort)

    # epA's plugin observes suspect, then the runtime-synthesized 'lost'
    # (the wire carries no tombstone — the participant simply vanishes).
    assert _wait(lambda: ('epB', 'suspect') in plugin.observed, timeout=5.0)
    assert _wait(lambda: ('epB', 'lost')    in plugin.observed, timeout=5.0)

    # The owner-bound ephemeral session is reclaimed after the drain.
    assert _wait(lambda: sid not in plugin._sessions, timeout=5.0)


# ---------------------------------------------------------------------------
# RuntimePluginClient satisfies a real plugin helper (sysinfo)
# ---------------------------------------------------------------------------

def test_runtime_plugin_client_satisfies_sysinfo(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='epA', plugins=['sysinfo'])
    b = make_runtime(srv.url, name='epB')
    assert _wait_topology(b, 'epA', 'sysinfo', timeout=5.0)

    sysinfo = b.get_plugin('epA', 'sysinfo')
    assert isinstance(sysinfo.homedir(), str)       # session-less helper
    metrics = sysinfo.get_metrics()                 # session helper
    assert 'system' in metrics


# ---------------------------------------------------------------------------
# transport isolation: a blocked work loop keeps the endpoint present
# ---------------------------------------------------------------------------

def test_transport_isolation_blocked_work_loop_stays_present(harness):
    make_broker, make_runtime = harness
    # Aggressive broker keepalive: a client that failed to answer would be
    # dropped in well under a second.
    srv = make_broker(ws_ping_interval=0.2, ws_ping_timeout=0.5)

    a = make_runtime(srv.url, name='epA', plugins=['echo_rt'],
                     ping_interval=0.2, ping_timeout=0.5)
    b = make_runtime(srv.url, name='epB',
                     ping_interval=0.2, ping_timeout=0.5)
    assert _wait_topology(b, 'epA', 'echo_rt', timeout=5.0)

    # Block A's work loop for ~2.5 s.
    def _block():
        b.call('epA', 'GET', '/echo_rt/block/2500', timeout=5.0)

    t = threading.Thread(target=_block, daemon=True)
    t.start()
    assert _wait(lambda: a._req_inflight >= 1, timeout=2.0)

    # Mid-block, A must still be present (transport thread answered pings).
    time.sleep(1.2)
    assert 'epA' in srv.broker.registry
    assert srv.broker._liveness.get('epA') == 'present'
    t.join(timeout=5.0)


# ---------------------------------------------------------------------------
# callback-queue drop-oldest counter (unit)
# ---------------------------------------------------------------------------

def test_callback_dispatcher_drop_oldest_counter():
    from radical.orbit.runtime_support import CallbackDispatcher
    cb = CallbackDispatcher(maxlen=2)               # not started → nothing drains
    for _ in range(5):
        cb.submit(lambda: None)
    assert cb.dropped == 3                           # 5 submitted, 2 retained


# ---------------------------------------------------------------------------
# dispatch.py move: shared RequestShim/match_route across hosts
# ---------------------------------------------------------------------------

def test_dispatch_move_reexport():
    from radical.orbit import dispatch
    from radical.orbit import runtime as _runtime
    from radical.orbit import broker_plugin_host
    assert _runtime.RequestShim is dispatch.RequestShim
    assert broker_plugin_host.RequestShim is dispatch.RequestShim
    assert callable(dispatch.match_route)
    # A route table the shared matcher understands.
    import re
    routes = [('GET', re.compile(r'^/p/ping$'), (), 'H')]
    handler, params = dispatch.match_route(routes, 'GET', '/p/ping')
    assert handler == 'H' and params == {}
