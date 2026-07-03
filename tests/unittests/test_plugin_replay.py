"""Unit + broker-integration tests for the event-replay plugin (M8).

The in-process tests drive :class:`~radical.orbit.plugin_replay.PluginReplay`
directly: they feed event dicts through the raw-tap consumer (``_on_event``) and
call the route handlers with a :class:`~radical.orbit.dispatch.RequestShim`, the
same in-process pattern ``test_task_dispatcher_broker.py`` uses.  Timers are
injected (a fake clock + direct ``_prune_cursors`` / ``_evict_aged`` calls), so
no test sleeps for age/cursor expiry.

The e2e test drives a **real** :class:`~radical.orbit.broker.Broker` hosting
``replay`` plus a **real** ``EndpointRuntime`` emitting events, and a second
runtime that connects late, registers a live callback, and drains
``replay_iter`` — exercising the replay→live splice with ``seq`` dedup.
"""

import asyncio
import json
import subprocess
import threading
import time

import pytest

from fastapi import FastAPI

from radical.orbit.dispatch      import RequestShim
from radical.orbit.plugin_base   import Plugin
from radical.orbit.plugin_replay import (PluginReplay, ReplayClient,
                                         _event_matches, _RingBuffer)


# ---------------------------------------------------------------------------
# in-process helpers
# ---------------------------------------------------------------------------

def _run(coro):
    """Run a coroutine on a fresh loop (route handlers are stateless per call)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _shim(body):
    return RequestShim({}, {}, json.dumps(body).encode())


def _make_plugin(**attrs):
    """A PluginReplay on a fresh app with a capturing fake tap."""
    taps = []
    app = FastAPI()
    app.state.is_broker   = True
    app.state.broker_tap  = lambda cb: (taps.append(cb)
                                        or (lambda: taps.remove(cb)))
    p = PluginReplay(app, 'replay')
    for k, v in attrs.items():
        setattr(p, k, v)
    p._taps = taps
    return p


def _ev(seq, session=None, topic='t', src='ep', plugin='rh', ts=None,
        data=None):
    # Default to a fresh timestamp so session buffers (age-bounded) don't
    # age-evict under the real clock; age tests pass an explicit ts + fake now.
    return {'kind': 'event', 'seq': seq, 'session': session, 'plugin': plugin,
            'topic': topic, 'src': src,
            'ts': time.time() if ts is None else ts, 'data': data or {}}


class _InProcHTTP:
    """Minimal httpx-shaped adapter routing a ReplayClient onto plugin routes."""

    def __init__(self, plugin):
        self._p = plugin

    def post(self, url, json=None, **kw):
        body = json or {}
        if   url.endswith('/fetch'):        data = _run(self._p._route_fetch(_shim(body)))
        elif url.endswith('/drop_cursor'):  data = _run(self._p._route_drop_cursor(_shim(body)))
        else:                               raise AssertionError(url)
        return _Resp(data)

    def get(self, url, **kw):
        if url.endswith('/stats'):          data = _run(self._p._route_stats(_shim({})))
        else:                               raise AssertionError(url)
        return _Resp(data)


class _Resp:
    def __init__(self, data):
        self._d          = data
        self.status_code = 200
        self.is_error    = False

    def json(self):
        return self._d

    @property
    def text(self):
        return json.dumps(self._d)


def _client(plugin):
    return ReplayClient(_InProcHTTP(plugin), '/replay',
                        endpoint_id='broker', plugin_name='replay')


# ---------------------------------------------------------------------------
# retention: session vs global split
# ---------------------------------------------------------------------------

def test_retention_session_vs_global_split():
    p = _make_plugin()
    for i in range(3):
        p._on_event(_ev(i))                       # session-less -> global ring
    p._on_event(_ev(0, session='s1'))
    p._on_event(_ev(1, session='s1'))
    p._on_event(_ev(0, session='s2'))

    assert p._global.stats()['retained'] == 3
    assert p._session_buffers['s1'].stats()['retained'] == 2
    assert p._session_buffers['s2'].stats()['retained'] == 1
    # session buffers are isolated: s2 is not in s1's bookkeeping.
    assert set(p._session_buffers) == {'s1', 's2'}


def test_non_event_and_seqless_ignored():
    p = _make_plugin()
    p._on_event({'kind': 'topology', 'seq': 0})   # not an event
    p._on_event({'kind': 'event'})                # no seq
    assert p._global.stats()['retained'] == 0


# ---------------------------------------------------------------------------
# age eviction (fake clock) + dropped counter + stats shape
# ---------------------------------------------------------------------------

def test_age_eviction_and_dropped_counter():
    clock = {'t': 1000.0}
    p = _make_plugin(session_max_age=600.0, session_max_bytes=10 ** 9)
    p._now = lambda: clock['t']

    p._on_event(_ev(0, session='s', ts=1000.0))
    p._on_event(_ev(1, session='s', ts=1000.0))
    assert p._session_buffers['s'].stats()['retained'] == 2

    # jump past the age bound and force a lazy sweep
    clock['t'] = 1000.0 + 601.0
    p._evict_aged(clock['t'])
    st = p._session_buffers['s'].stats()
    assert st['retained'] == 0
    assert st['dropped']  == 2


def test_stats_shape():
    p = _make_plugin()
    p._on_event(_ev(5))
    p._on_event(_ev(7))
    p._on_event(_ev(0, session='s'))
    st = _run(p._route_stats(_shim({})))
    assert set(st) == {'global', 'sessions', 'cursors'}
    g = st['global']
    assert set(g) == {'retained', 'bytes', 'dropped', 'lowest_seq',
                      'highest_seq'}
    assert g['lowest_seq'] == 5 and g['highest_seq'] == 7
    assert g['retained'] == 2 and g['bytes'] > 0
    assert st['sessions']['s']['retained'] == 1


# ---------------------------------------------------------------------------
# size eviction + dropped counter
# ---------------------------------------------------------------------------

def test_size_eviction_global_ring():
    p = _make_plugin()
    one = len(__import__('msgpack').packb(_ev(0), use_bin_type=True))
    p.global_max_bytes = one * 2 + 1      # rebind after construction
    p._global = _RingBuffer(p.global_max_bytes)   # honour the new bound
    for i in range(5):
        p._on_event(_ev(i))
    st = p._global.stats()
    assert st['retained'] == 2            # only the last two fit
    assert st['dropped']  == 3
    assert st['lowest_seq'] == 3 and st['highest_seq'] == 4


def test_size_eviction_per_session_isolated():
    p = _make_plugin(session_max_age=10 ** 9, session_max_bytes=len(
        __import__('msgpack').packb(_ev(0, session='s1'), use_bin_type=True)
        ) * 2 + 1)
    for i in range(4):
        p._on_event(_ev(i, session='s1'))
    for i in range(1):
        p._on_event(_ev(i, session='s2'))
    # s1 is chatty and self-evicts; s2 keeps its single event untouched.
    assert p._session_buffers['s1'].stats()['dropped']  == 2
    assert p._session_buffers['s1'].stats()['retained'] == 2
    assert p._session_buffers['s2'].stats()['dropped']  == 0
    assert p._session_buffers['s2'].stats()['retained'] == 1


# ---------------------------------------------------------------------------
# pattern filtering incl. wildcards
# ---------------------------------------------------------------------------

def test_event_matches_wildcards():
    ev = _ev(0, src='epA', plugin='psij', topic='job_status')
    assert _event_matches([], ev) is True                       # match-all
    assert _event_matches([{}], ev) is True                     # all-wildcard
    assert _event_matches([{'endpoint': 'epA'}], ev) is True
    assert _event_matches([{'endpoint': 'epB'}], ev) is False
    assert _event_matches([{'plugin': 'psij', 'topic': 'job_status'}], ev)
    assert _event_matches([{'topic': 'other'}], ev) is False
    # OR across patterns
    assert _event_matches([{'endpoint': 'epB'}, {'plugin': 'psij'}], ev)


def test_fetch_pattern_filter():
    p = _make_plugin()
    p._on_event(_ev(0, plugin='psij', topic='job_status'))
    p._on_event(_ev(1, plugin='rhapsody', topic='task_status'))
    p._on_event(_ev(2, plugin='psij', topic='job_status'))
    r = _run(p._route_fetch(_shim(
        {'cursor_id': 'c', 'patterns': [{'plugin': 'psij'}]})))
    assert [e['seq'] for e in r['events']] == [0, 2]


# ---------------------------------------------------------------------------
# fetch/ack cursor advance + at-least-once (unacked re-delivery)
# ---------------------------------------------------------------------------

def test_fetch_ack_cursor_advance_and_redelivery():
    p = _make_plugin()
    for i in range(3):
        p._on_event(_ev(i))

    a = _run(p._route_fetch(_shim({'cursor_id': 'c'})))
    assert [e['seq'] for e in a['events']] == [0, 1, 2]
    assert a['next_seq'] == 2 and a['gap'] is False

    # A second fetch WITHOUT ack re-delivers the same batch (at-least-once).
    b = _run(p._route_fetch(_shim({'cursor_id': 'c'})))
    assert [e['seq'] for e in b['events']] == [0, 1, 2]

    # ACK advances the cursor; the next fetch returns only newer events.
    c = _run(p._route_fetch(_shim({'cursor_id': 'c', 'ack_seq': 2})))
    assert c['events'] == [] and c['next_seq'] == 2

    p._on_event(_ev(3))
    d = _run(p._route_fetch(_shim({'cursor_id': 'c', 'ack_seq': 2})))
    assert [e['seq'] for e in d['events']] == [3]


def test_max_events_batches():
    p = _make_plugin()
    for i in range(5):
        p._on_event(_ev(i))
    r = _run(p._route_fetch(_shim({'cursor_id': 'c', 'max_events': 2})))
    assert [e['seq'] for e in r['events']] == [0, 1]
    assert r['next_seq'] == 1


# ---------------------------------------------------------------------------
# seq dedup end-to-end across a simulated drop mid-drain (replay_iter)
# ---------------------------------------------------------------------------

def test_replay_iter_seq_dedup_across_drop():
    p = _make_plugin()
    for i in range(5):
        p._on_event(_ev(i))
    cl = _client(p)

    # First drain aborts after two events WITHOUT ever acking (a mid-drain
    # drop): replay_iter acks a batch only on its *next* fetch, which never
    # runs once we break.
    partial = []
    for ev in cl.replay_iter('cur', max_events=2):
        partial.append(ev['seq'])
        if len(partial) == 2:
            break
    assert partial == [0, 1]

    # Fresh drain with the same cursor_id: the unacked prefix is re-delivered
    # (at-least-once); the consumer dedups by seq.
    seen = {}
    for ev in cl.replay_iter('cur', max_events=2):
        seen[ev['seq']] = seen.get(ev['seq'], 0) + 1

    # every event delivered at least once; dedup yields each exactly once
    assert set(seen) == {0, 1, 2, 3, 4}
    # the re-delivery actually happened (0/1 came back after the drop)
    assert seen[0] == 1 and seen[1] == 1        # within the second drain
    # across BOTH drains 0 and 1 were delivered twice total → dedup earns keep
    total_deliveries = len(partial) + sum(seen.values())
    assert total_deliveries == 2 + 5            # 2 dropped + 5 clean


# ---------------------------------------------------------------------------
# gap detection after eviction
# ---------------------------------------------------------------------------

def test_gap_true_after_eviction():
    # keep a bound that holds ~2 events so early ones evict but some remain
    two = len(__import__('msgpack').packb(_ev(0, session='s'),
                                          use_bin_type=True)) * 2 + 1
    p = _make_plugin(session_max_age=10 ** 9, session_max_bytes=two)
    for i in range(5):
        p._on_event(_ev(i, session='s'))
    buf = p._session_buffers['s']
    assert buf.lowest_seq == 3 and buf.stats()['dropped'] == 3

    # a fresh cursor (position -1) sits below the lowest retained seq -> gap
    r = _run(p._route_fetch(_shim({'cursor_id': 'c', 'session': 's'})))
    assert r['gap'] is True
    assert [e['seq'] for e in r['events']] == [3, 4]

    # a cursor that only wants seq > 4 sees no gap
    r2 = _run(p._route_fetch(_shim(
        {'cursor_id': 'c2', 'session': 's', 'after_seq': 4})))
    assert r2['gap'] is False and r2['events'] == []


def test_no_gap_on_contiguous_stream():
    p = _make_plugin()
    for i in range(3):
        p._on_event(_ev(i))
    r = _run(p._route_fetch(_shim({'cursor_id': 'c'})))
    assert r['gap'] is False


# ---------------------------------------------------------------------------
# cursor expiry + drop_cursor
# ---------------------------------------------------------------------------

def test_cursor_expiry_by_inactivity():
    clock = {'t': 1000.0}
    p = _make_plugin(cursor_ttl=100.0)
    p._now = lambda: clock['t']

    _run(p._route_fetch(_shim({'cursor_id': 'c'})))
    assert 'c' in p._cursors

    clock['t'] = 1000.0 + 101.0
    assert p._prune_cursors(clock['t']) == 1
    assert 'c' not in p._cursors


def test_drop_cursor():
    p = _make_plugin()
    _run(p._route_fetch(_shim({'cursor_id': 'c'})))
    r = _run(p._route_drop_cursor(_shim({'cursor_id': 'c'})))
    assert r['dropped'] is True
    assert 'c' not in p._cursors
    r2 = _run(p._route_drop_cursor(_shim({'cursor_id': 'nope'})))
    assert r2['dropped'] is False


def test_fetch_requires_cursor_id():
    from fastapi import HTTPException
    p = _make_plugin()
    with pytest.raises(HTTPException) as ei:
        _run(p._route_fetch(_shim({})))
    assert ei.value.status_code == 400


def test_fetch_unknown_session_empty_no_gap():
    p = _make_plugin()
    r = _run(p._route_fetch(_shim({'cursor_id': 'c', 'session': 'ghost'})))
    assert r['events'] == [] and r['gap'] is False


# ---------------------------------------------------------------------------
# is_enabled gating
# ---------------------------------------------------------------------------

def test_is_enabled_requires_broker_and_tap():
    app = FastAPI()
    app.state.is_broker  = True
    app.state.broker_tap = lambda cb: (lambda: None)
    assert PluginReplay.is_enabled(app) is True

    # broker host but no tap -> disabled
    app2 = FastAPI()
    app2.state.is_broker = True
    assert PluginReplay.is_enabled(app2) is False

    # not a broker host -> disabled even if a tap were present
    app3 = FastAPI()
    app3.state.broker_tap = lambda cb: (lambda: None)
    assert PluginReplay.is_enabled(app3) is False


def test_not_in_default_plugin_set():
    # Documented: replay is opt-in via --plugins, never auto-loaded.  The
    # 'default' special token must not expand to include it.
    from radical.orbit.plugin_host_base import _expand_special_tokens
    app = FastAPI()
    app.state.is_broker = True
    available = Plugin.get_plugin_names()
    assert 'replay' in available
    expanded = _expand_special_tokens(['default'], app, available)
    assert 'replay' not in expanded


# ===========================================================================
# e2e: real broker hosting replay + real runtimes (late consumer + splice)
# ===========================================================================

def _have_openssl():
    try:
        subprocess.run(['openssl', 'version'], check=True, capture_output=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


class _ReplayEmit(Plugin):
    """A served plugin that emits N session-less events per burst call."""

    plugin_name = 'replay_emit'
    version     = '0.0.1'

    def __init__(self, app):
        super().__init__(app, 'replay_emit')
        self.add_route_get('burst/{n}/{base}', self._burst)

    async def _burst(self, request):
        n    = int(request.path_params['n'])
        base = int(request.path_params['base'])
        for i in range(n):
            await self.send_notification('tick', {'n': base + i})
        return {'sent': n}


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
def harness(tmp_path, monkeypatch):
    if not _have_openssl():
        pytest.skip("openssl not available")
    import os
    from radical.orbit import utils
    monkeypatch.setattr(utils, 'URL_FILE',   tmp_path / 'broker.url')
    monkeypatch.setattr(utils, 'TOKEN_FILE', tmp_path / 'broker.token')
    monkeypatch.delenv('RADICAL_ORBIT_BROKER_TOKEN', raising=False)
    cert = tmp_path / 'cert.pem'
    key  = tmp_path / 'key.pem'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
         '-keyout', str(key), '-out', str(cert),
         '-days', '1', '-subj', '/CN=localhost'],
        check=True, capture_output=True)
    os.chmod(key, 0o600)

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


def _wait(cond, timeout=8.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return False


def test_e2e_late_consumer_replays_with_splice(harness):
    import httpx

    make_broker, make_runtime = harness
    srv = make_broker(plugins='replay')

    replay = srv.broker._plugin_host._plugins['replay']
    assert replay is not None

    # emitter endpoint emits 5 events BEFORE any consumer exists
    make_runtime(srv.url, name='epA', plugins=['replay_emit'])
    assert _wait(lambda: 'epA' in srv.broker.registry)
    r = httpx.get('%s/epA/replay_emit/burst/5/0' % srv.url, timeout=10.0)
    assert r.status_code == 200, r.text
    assert _wait(lambda: replay._global.stats()['retained'] >= 5)

    # a consumer connects LATE (missed all 5 live)
    consumer = make_runtime(srv.url, name='cons', plugins=[])
    assert _wait(lambda: 'broker' in consumer.topology()
                 and 'replay' in consumer.topology()['broker'].get('plugins', {}))

    # 1) register the LIVE callback first, keyed by the app-level id in data
    lock = threading.Lock()
    live_ns = []

    def on_live(endpoint, plugin, topic, data):
        with lock:
            live_ns.append(data['n'])

    consumer.register_callback(endpoint_id='epA', plugin_name='replay_emit',
                               callback=on_live)
    time.sleep(0.3)                                   # let subscribe land

    # 2) emit 2 MORE events — these arrive LIVE at the consumer AND land in the
    #    replay buffer (the overlap window of the splice)
    httpx.get('%s/epA/replay_emit/burst/2/5' % srv.url, timeout=10.0)
    assert _wait(lambda: set(live_ns) >= {5, 6})

    # 3) drain replay and merge; dedup across live + replay by the event key
    rc = consumer.get_plugin('broker', 'replay')
    replayed_ns  = []
    replayed_seq = []
    for ev in rc.replay_iter('cons-cursor',
                             patterns=[{'endpoint': 'epA'}]):
        replayed_ns.append(ev['data']['n'])
        replayed_seq.append(ev['seq'])

    # replay covers the whole stream (0..6), each seq exactly once
    assert replayed_seq == sorted(set(replayed_seq))
    assert set(replayed_ns) == set(range(7))

    # the splice: live delivered 5 & 6, replay ALSO delivered them → overlap.
    # After dedup by the per-event key the consumer holds each event once.
    with lock:
        all_deliveries = list(live_ns) + replayed_ns
    deduped = set(all_deliveries)
    assert deduped == set(range(7))                  # exactly-once after dedup
    assert len(all_deliveries) > len(deduped)        # overlap actually occurred
