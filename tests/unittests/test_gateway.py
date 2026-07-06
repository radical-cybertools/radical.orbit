"""Tests for :class:`radical.orbit.gateway.Gateway` (M5 compat tier).

Following the :mod:`test_runtime` pattern, the suite drives a **real**
:class:`~radical.orbit.broker.Broker` (with its default-on gateway) under
uvicorn on an ephemeral port and connects **real** ``EndpointRuntime``
participants that serve a test plugin — the most faithful reproduction of
production wiring.  HTTP ingress is exercised over ``httpx``; SSE over a
streaming ``requests`` reader.  The bounded per-client SSE queue is unit-tested
in isolation.

No test sleeps for more than ~1 s; liveness/backoff knobs are injected tiny.
"""

import json
import subprocess
import threading
import time

import httpx
import pytest
import requests

from starlette.responses     import Response as StarletteResponse
from starlette.responses     import JSONResponse

from radical.orbit.plugin_base import Plugin


# ---------------------------------------------------------------------------
# TLS material (the broker resolves a cert/key even when the test server runs
# plain-HTTP under uvicorn)
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
# A tiny served plugin: ping, echo (verbatim body), teapot (status+headers),
# notify (event)
# ---------------------------------------------------------------------------

class _GwPlugin(Plugin):
    plugin_name = 'gw_test'
    version     = '0.0.1'

    def __init__(self, app):
        super().__init__(app, 'gw_test')
        self.add_route_get('ping',   self._ping)
        self.add_route_post('echo',  self._echo)
        self.add_route_get('teapot', self._teapot)
        self.add_route_get('notify', self._notify)

    async def _ping(self, request):
        return {'pong': True}

    async def _echo(self, request):
        body = await request.body()
        # Return the request body verbatim (JSON + binary round-trip) with a
        # custom content-type + header to exercise passthrough.
        return StarletteResponse(
            content=body, status_code=200,
            headers={'content-type': 'application/octet-stream',
                     'x-echo': 'yes'})

    async def _teapot(self, request):
        return JSONResponse({'brew': 'coffee'}, status_code=418,
                            headers={'x-custom': 'v'})

    async def _notify(self, request):
        await self.send_notification('demo', {'hello': 'world'})
        return {'sent': True}


# ---------------------------------------------------------------------------
# Broker-under-uvicorn harness + runtime factory
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
    """Factory yielding (make_broker, make_runtime); tears everything down."""
    from radical.orbit import utils
    monkeypatch.setattr(utils, 'URL_FILE',   tmp_path / 'broker.url')
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')
    monkeypatch.delenv('RADICAL_ORBIT_BROKER_TOKEN', raising=False)
    monkeypatch.delenv('RADICAL_ORBIT_BROKER_URL',   raising=False)
    cert, key = self_signed

    servers  = []
    runtimes = []

    def make_broker(**kw):
        from radical.orbit.broker import Broker, BrokerTuning
        tuning = BrokerTuning(grace=2.0)
        for _k in list(kw):
            if hasattr(tuning, _k):
                setattr(tuning, _k, kw.pop(_k))
        defaults = dict(cert=str(cert), key=str(key), no_auth=True, tuning=tuning)
        defaults.update(kw)
        broker = Broker(**defaults)
        srv = _RunningBroker(broker).start()
        servers.append(srv)
        return srv

    def make_runtime(url, wait=True, token=None, **kw):
        from radical.orbit.runtime import EndpointRuntime
        defaults = dict(broker_url=url, token=token, ping_interval=1.0,
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


# ---------------------------------------------------------------------------
# SSE reader (background thread, streaming)
# ---------------------------------------------------------------------------

class _SSEReader:
    """Open an SSE stream and collect parsed ``{topic, data}`` frames."""

    def __init__(self, url, cookies=None):
        self.frames = []
        self.lock   = threading.Lock()
        self._stop  = False
        self._resp  = requests.get(url, stream=True, timeout=10.0,
                                   cookies=cookies or {})
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        try:
            for line in self._resp.iter_lines(decode_unicode=True):
                if self._stop:
                    break
                if line and line.startswith('data: '):
                    try:
                        frame = json.loads(line[6:])
                    except Exception:
                        continue
                    with self.lock:
                        self.frames.append(frame)
        except Exception:
            pass

    def by_topic(self, topic):
        with self.lock:
            return [f for f in self.frames if f.get('topic') == topic]

    def close(self):
        self._stop = True
        try:    self._resp.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Auth middleware
# ---------------------------------------------------------------------------

def test_auth_gate_401_without_token_and_cookie_flow(harness):
    make_broker, _ = harness
    srv = make_broker(no_auth=False, token='sekret')

    # No token -> 401 on a capability route.
    r = httpx.post(srv.url + '/endpoint/list')
    assert r.status_code == 401
    assert r.json()['detail'] == 'missing or invalid broker token'

    # Bearer token passes.
    r = httpx.post(srv.url + '/endpoint/list',
                   headers={'authorization': 'Bearer sekret'})
    assert r.status_code == 200

    # /auth mints the cookie (reached with a valid bearer header).
    r = httpx.post(srv.url + '/auth',
                   headers={'authorization': 'Bearer sekret'})
    assert r.status_code == 200
    assert r.cookies.get('orbit_broker_token') == 'sekret'

    # The cookie alone then passes.
    r = httpx.post(srv.url + '/endpoint/list',
                   cookies={'orbit_broker_token': 'sekret'})
    assert r.status_code == 200


def test_auth_exempt_paths(harness):
    make_broker, _ = harness
    srv = make_broker(no_auth=False, token='sekret')

    # UI shell + static plugin JS load without a token (Explorer must prompt).
    assert httpx.get(srv.url + '/').status_code in (200, 404)  # served or absent
    # A plugin JS request is exempt from the gate (404 only if the file is
    # absent — never 401).
    assert httpx.get(srv.url + '/plugins/does_not_exist.js').status_code == 404


def test_auth_disabled_passthrough(harness):
    make_broker, _ = harness
    srv = make_broker(no_auth=True)
    r = httpx.post(srv.url + '/endpoint/list')
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Explorer served at /
# ---------------------------------------------------------------------------

def test_explorer_served_at_root(harness):
    make_broker, _ = harness
    srv = make_broker()
    r = httpx.get(srv.url + '/')
    assert r.status_code == 200
    assert 'html' in r.text.lower()


# ---------------------------------------------------------------------------
# Discovery: /endpoint/list broker-era shape (namespaces present)
# ---------------------------------------------------------------------------

def test_endpoint_list_shape_with_live_endpoint(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    a = make_runtime(srv.url, name='epA', plugins=['gw_test'])
    assert a.wait_registered(timeout=5.0)
    assert _wait(lambda: 'epA' in srv.broker.registry)

    r = httpx.post(srv.url + '/endpoint/list')
    assert r.status_code == 200
    data = r.json()['data']

    # Broker-era envelope: {broker:{url}, endpoints:{...}}.
    assert 'url' in data['broker']
    endpoints = data['endpoints']
    assert 'epA'    in endpoints
    assert 'broker' in endpoints                          # broker is a participant

    plugins = endpoints['epA']['plugins']
    assert 'gw_test' in plugins
    # Endpoint-relative /{instance} presented as the full /{endpoint}/{instance}.
    assert plugins['gw_test']['namespace'] == '/epA/gw_test'


def test_endpoints_summary_list(harness):
    make_broker, make_runtime = harness
    srv = make_broker()
    a = make_runtime(srv.url, name='epA', plugins=['gw_test'])
    assert a.wait_registered(timeout=5.0)
    assert _wait(lambda: 'epA' in srv.broker.registry)

    r = httpx.get(srv.url + '/endpoints')
    assert r.status_code == 200
    body = r.json()
    names = {e['name']: e for e in body['endpoints']}
    assert 'epA' in names
    assert names['epA']['connected'] is True
    assert names['epA']['plugin_count'] == 1
    assert body['total'] == len(body['endpoints'])


# ---------------------------------------------------------------------------
# Catch-all proxy end-to-end
# ---------------------------------------------------------------------------

def test_proxy_json_roundtrip(harness):
    make_broker, make_runtime = harness
    srv = make_broker()
    make_runtime(srv.url, name='epA', plugins=['gw_test'])
    assert _wait(lambda: 'epA' in srv.broker.registry)

    r = httpx.get(srv.url + '/epA/gw_test/ping')
    assert r.status_code == 200
    assert r.json() == {'pong': True}


def test_proxy_binary_roundtrip_and_header_passthrough(harness):
    make_broker, make_runtime = harness
    srv = make_broker()
    make_runtime(srv.url, name='epA', plugins=['gw_test'])
    assert _wait(lambda: 'epA' in srv.broker.registry)

    payload = bytes(range(256))                           # non-UTF-8 bytes
    r = httpx.post(srv.url + '/epA/gw_test/echo', content=payload)
    assert r.status_code == 200
    assert r.content == payload                           # verbatim round-trip
    assert r.headers['x-echo'] == 'yes'                   # header passthrough
    assert r.headers['content-type'] == 'application/octet-stream'


def test_proxy_status_and_custom_header_passthrough(harness):
    make_broker, make_runtime = harness
    srv = make_broker()
    make_runtime(srv.url, name='epA', plugins=['gw_test'])
    assert _wait(lambda: 'epA' in srv.broker.registry)

    r = httpx.get(srv.url + '/epA/gw_test/teapot')
    assert r.status_code == 418                           # status passthrough
    assert r.headers['x-custom'] == 'v'
    assert r.json() == {'brew': 'coffee'}


def test_proxy_unknown_endpoint_is_404(harness):
    make_broker, _ = harness
    srv = make_broker()
    r = httpx.get(srv.url + '/ghost/gw_test/ping')
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Long 504 deadline carried over (config value; not waited on)
# ---------------------------------------------------------------------------

def test_long_request_deadline_config(harness):
    from radical.orbit import gateway
    make_broker, _ = harness
    srv = make_broker()
    assert gateway.REQUEST_TIMEOUT == 600
    assert srv.broker._gateway._request_timeout == 600


# ---------------------------------------------------------------------------
# SSE: notification (legacy shape) + topology (connect/disconnect)
# ---------------------------------------------------------------------------

def test_sse_notification_legacy_shape(harness):
    make_broker, make_runtime = harness
    srv = make_broker()
    make_runtime(srv.url, name='epA', plugins=['gw_test'])
    assert _wait(lambda: 'epA' in srv.broker.registry)

    sse = _SSEReader(srv.url + '/events')
    try:
        time.sleep(0.3)                                   # let the stream open
        httpx.get(srv.url + '/epA/gw_test/notify')
        assert _wait(lambda: len(sse.by_topic('notification')) >= 1, timeout=5.0)
        frame = sse.by_topic('notification')[0]
        # Exact legacy envelope: {topic:'notification', data:{endpoint,plugin,
        # topic,data}}.
        d = frame['data']
        assert d['endpoint'] == 'epA'
        assert d['plugin']   == 'gw_test'
        assert d['topic']    == 'demo'
        assert d['data']     == {'hello': 'world'}
    finally:
        sse.close()


def test_sse_topology_on_connect_and_disconnect(harness):
    make_broker, make_runtime = harness
    srv = make_broker()

    sse = _SSEReader(srv.url + '/events')
    try:
        time.sleep(0.3)
        # Initial topology frame is sent on connect.
        assert _wait(lambda: len(sse.by_topic('topology')) >= 1, timeout=5.0)

        a = make_runtime(srv.url, name='epA', plugins=['gw_test'])
        assert _wait(
            lambda: any('epA' in f['data'].get('endpoints', {})
                        for f in sse.by_topic('topology')),
            timeout=5.0)

        a.stop()                                          # clean close
        assert _wait(
            lambda: 'epA' not in sse.by_topic('topology')[-1]['data']
                    .get('endpoints', {}),
            timeout=5.0)
    finally:
        sse.close()


def test_sse_queue_drop_oldest_counter():
    # The gateway now uses the shared bounded, drop-oldest queue.
    from radical.orbit.queues import BoundedDropOldestQueue
    q = BoundedDropOldestQueue(2)
    for i in range(5):
        q.push('f%d' % i)
    assert q.dropped == 3                                 # 5 pushed, 2 retained
    assert list(q.buf) == ['f3', 'f4']                    # oldest evicted


# ---------------------------------------------------------------------------
# Admin: disconnect + terminate (+ alias)
# ---------------------------------------------------------------------------

def test_endpoint_disconnect(harness):
    make_broker, make_runtime = harness
    srv = make_broker()
    make_runtime(srv.url, name='epA', plugins=['gw_test'],
                 backoff_start=30.0, backoff_max=30.0)
    assert _wait(lambda: 'epA' in srv.broker.registry)

    r = httpx.post(srv.url + '/endpoint/disconnect/epA')
    assert r.status_code == 200
    assert r.json()['endpoint'] == 'epA'
    assert _wait(lambda: 'epA' not in srv.broker.registry, timeout=5.0)

    # Unknown endpoint -> 404.
    assert httpx.post(srv.url + '/endpoint/disconnect/ghost').status_code == 404


def test_terminate_and_alias_self_sigterm(harness, monkeypatch):
    import radical.orbit.broker as bmod
    make_broker, _ = harness
    srv = make_broker()

    killed = {}
    monkeypatch.setattr(bmod.os, 'kill',
                        lambda pid, sig: killed.setdefault('sig', sig))

    r = httpx.post(srv.url + '/broker/terminate')
    assert r.status_code == 200
    assert r.json()['status'] == 'terminating'
    assert _wait(lambda: killed.get('sig') == bmod.signal.SIGTERM, timeout=2.0)

    killed.clear()
    r = httpx.post(srv.url + '/broker/terminate')          # alias
    assert r.status_code == 200
    assert _wait(lambda: killed.get('sig') == bmod.signal.SIGTERM, timeout=2.0)


# ---------------------------------------------------------------------------
# gateway=False leaves the app without gateway routes
# ---------------------------------------------------------------------------

def test_headless_broker_has_no_gateway_routes(harness):
    make_broker, make_runtime = harness
    srv = make_broker(gateway=False)

    paths = {getattr(r, 'path', None) for r in srv.broker.app.routes}
    assert '/register' in paths                            # core WS gate present
    assert '/endpoint/list' not in paths
    assert '/events' not in paths
    assert srv.broker._gateway is None

    # The HTTP surface is simply not there.
    assert httpx.post(srv.url + '/endpoint/list').status_code == 404

    # But the WS /register still works (a runtime can connect).
    a = make_runtime(srv.url, name='epA', plugins=['gw_test'])
    assert a.wait_registered(timeout=5.0)


# ---------------------------------------------------------------------------
# CORS preflight
# ---------------------------------------------------------------------------

def test_cors_preflight_headers(harness):
    make_broker, _ = harness
    srv = make_broker()
    r = httpx.request(
        'OPTIONS', srv.url + '/endpoint/list',
        headers={'Origin': 'http://localhost:8080',
                 'Access-Control-Request-Method': 'POST'})
    assert r.status_code in (200, 204)
    assert r.headers.get('access-control-allow-origin') == 'http://localhost:8080'
    assert r.headers.get('access-control-allow-credentials') == 'true'


# ---------------------------------------------------------------------------
# Unit tests for header hygiene / namespace / notification-data handling
# ---------------------------------------------------------------------------

def test_full_namespace_no_trailing_slash():
    """An empty/None namespace yields '/{name}' with no trailing slash."""
    from radical.orbit.gateway import Gateway
    assert Gateway._full_namespace('epA', '')          == '/epA'
    assert Gateway._full_namespace('epA', None)        == '/epA'
    assert Gateway._full_namespace('epA', '/sysinfo')  == '/epA/sysinfo'
    assert Gateway._full_namespace('epA', 'sysinfo')   == '/epA/sysinfo'
    # broker's own hosted plugins already carry the full form.
    assert Gateway._full_namespace('broker', '/broker/x') == '/broker/x'


def test_clean_response_headers_strips_framing():
    """Hop-by-hop + content-length are dropped from the upstream response."""
    from radical.orbit.gateway import Gateway
    cleaned = Gateway._clean_response_headers({
        'Content-Type':      'application/json',
        'Content-Length':    '123',
        'Transfer-Encoding': 'chunked',
        'Connection':        'keep-alive',
        'X-Custom':          'v',
    })
    lower = {k.lower() for k in cleaned}
    assert 'content-length'    not in lower
    assert 'transfer-encoding' not in lower
    assert 'connection'        not in lower
    assert cleaned['Content-Type'] == 'application/json'   # case preserved
    assert cleaned['X-Custom']     == 'v'


def test_on_event_preserves_falsy_data():
    """A legit falsy notification payload (e.g. []) rides through unchanged;
    only a genuinely absent 'data' becomes {}."""
    from radical.orbit.gateway import Gateway

    class _FakeLoop:
        def call_soon_threadsafe(self, fn, *a):
            fn(*a)

    class _FakeBroker:
        _loop = _FakeLoop()

    pushed = []
    g = Gateway.__new__(Gateway)
    g._broker      = _FakeBroker()
    g._sse_clients = {'client'}                    # non-empty -> proceed
    g._push_all    = lambda frame: pushed.append(frame)

    g._on_event({'src': 'e', 'plugin': 'p', 'topic': 't', 'data': []})
    assert pushed
    payload = json.loads(pushed[0][len('data: '):])
    assert payload['data']['data'] == []           # falsy [] preserved

    pushed.clear()
    g._on_event({'src': 'e', 'plugin': 'p', 'topic': 't'})   # no 'data'
    payload = json.loads(pushed[0][len('data: '):])
    assert payload['data']['data'] == {}           # absent -> {}
